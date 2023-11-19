use std::io::Result;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_channel::Sender;
use aws_sdk_s3::types::{ChecksumAlgorithm, ObjectPart};
use leaky_bucket::RateLimiter;
use pin_project::pin_project;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::runtime::Handle;
use tokio::task;
use tracing::{error, trace};

use crate::storage::checksum::AdditionalChecksum;
use crate::types::{ObjectChecksum, SyncStatistics};

const MINIMUM_CHUNKSIZE: usize = 5 * 1024 * 1024;

#[pin_project]
pub struct AsyncReadWithCallback<R: AsyncRead> {
    #[pin]
    inner: R,
    stats_sender: Sender<SyncStatistics>,
    bandwidth_limiter: Option<Arc<RateLimiter>>,
    additional_checksum: Option<Arc<AdditionalChecksum>>,
    object_checksum: Option<ObjectChecksum>,
    cursor: Cursor,
}

#[derive(Debug, Default)]
struct Cursor {
    pub part_number: usize,
    pub part_offset: usize,
    pub eof: bool,
}

impl<R: AsyncRead> AsyncReadWithCallback<R> {
    pub fn new(
        inner: R,
        stats_sender: Sender<SyncStatistics>,
        bandwidth_limiter: Option<Arc<RateLimiter>>,
        additional_checksum: Option<Arc<AdditionalChecksum>>,
        object_checksum: Option<ObjectChecksum>,
    ) -> Self {
        Self {
            inner,
            stats_sender,
            bandwidth_limiter,
            additional_checksum,
            object_checksum,
            cursor: Cursor::default(),
        }
    }
}

impl<R: AsyncRead> AsyncRead for AsyncReadWithCallback<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        let this = self.project();

        let before = buf.filled().len();

        let result = this.inner.poll_read(cx, buf);
        if !result.is_ready() {
            return result;
        }

        let after = buf.filled().len();

        let sync_bytes = after - before;

        if let Some(bandwidth_limiter) = this.bandwidth_limiter.clone() {
            if 0 < sync_bytes {
                task::block_in_place(move || {
                    Handle::current().block_on(async move {
                        bandwidth_limiter.acquire(sync_bytes).await;
                    });
                });
            }
        }

        if 0 < sync_bytes {
            let _ = this
                .stats_sender
                .send_blocking(SyncStatistics::SyncBytes(sync_bytes as u64));
        }

        if this.additional_checksum.is_some() && is_multipart(this.object_checksum) {
            let hasher = &mut this.additional_checksum.as_mut().unwrap();
            let hasher = Arc::get_mut(hasher).unwrap();

            return validate_checksum(
                buf,
                before,
                sync_bytes,
                hasher,
                this.cursor,
                this.object_checksum.as_ref().unwrap(),
                result,
            );
        }

        result
    }
}

fn is_multipart(object_checksum: &Option<ObjectChecksum>) -> bool {
    if object_checksum.is_none() {
        return false;
    }

    if object_checksum.as_ref().unwrap().object_parts.is_none() {
        return false;
    }

    !object_checksum
        .as_ref()
        .unwrap()
        .object_parts
        .as_ref()
        .unwrap()
        .is_empty()
}

fn validate_checksum(
    buf: &ReadBuf<'_>,
    before: usize,
    sync_bytes: usize,
    hasher: &mut AdditionalChecksum,
    cursor: &mut Cursor,
    object_checksum: &ObjectChecksum,
    result: Poll<Result<()>>,
) -> Poll<Result<()>> {
    let mut version_id = "".to_string();
    if let Some(source_version_id) = object_checksum.version_id.as_ref() {
        version_id = source_version_id.to_string();
    }
    let key = object_checksum.key.as_str();
    let part_number = cursor.part_number + 1;

    if sync_bytes == 0 {
        if !(cursor.eof) {
            // last part
            let local_checksum = hasher.finalize();
            let remote_checksum = get_checksum(
                &object_checksum.object_parts.as_ref().unwrap()
                    [object_checksum.object_parts.as_ref().unwrap().len() - 1],
                object_checksum.checksum_algorithm.as_ref().unwrap().clone(),
            )
            .unwrap();

            if local_checksum != remote_checksum {
                error!(
                    key = key,
                    version_id = version_id,
                    remote_checksum = remote_checksum,
                    local_checksum = local_checksum,
                    part_number = part_number,
                    "additional checksum mismatch."
                );

                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "additional checksum mismatch.",
                )));
            }

            let local_final_checksum = hasher.finalize_all();
            if local_final_checksum != *object_checksum.final_checksum.as_ref().unwrap() {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "additional checksum mismatch",
                )));
            }

            trace!(
                key = key,
                version_id = version_id,
                local_final_checksum = local_final_checksum,
                "additional checksum verification completed."
            );

            cursor.eof = true
        }

        return result;
    }

    if sync_bytes != 0 {
        if sync_bytes >= MINIMUM_CHUNKSIZE {
            error!(
                key = key,
                version_id = version_id,
                part_number = part_number,
                "unexpected I/O behavior."
            );

            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unexpected I/O behavior.",
            )));
        }

        let current_part_size = object_checksum.object_parts.as_ref().unwrap()[cursor.part_number]
            .size
            .unwrap() as usize;
        let remaining = current_part_size - cursor.part_offset;

        if cursor.part_offset + sync_bytes <= current_part_size {
            // same part as before
            hasher.update(&buf.filled()[before..]);
            cursor.part_offset += sync_bytes;

            if remaining == sync_bytes {
                let local_checksum = hasher.finalize();
                let remote_checksum = get_checksum(
                    &object_checksum.object_parts.as_ref().unwrap()[cursor.part_number],
                    object_checksum.checksum_algorithm.as_ref().unwrap().clone(),
                )
                .unwrap();

                if local_checksum != remote_checksum {
                    error!(
                        key = key,
                        version_id = version_id,
                        remote_checksum = remote_checksum,
                        local_checksum = local_checksum,
                        part_number = part_number,
                        "additional checksum mismatch."
                    );

                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "additional checksum mismatch.",
                    )));
                }

                if cursor.part_number == object_checksum.object_parts.as_ref().unwrap().len() - 1 {
                    let local_final_checksum = hasher.finalize_all();
                    trace!(
                        key = key,
                        version_id = version_id,
                        local_final_checksum = local_final_checksum,
                        "additional checksum verification completed."
                    );

                    cursor.eof = true;

                    if local_final_checksum != *object_checksum.final_checksum.as_ref().unwrap() {
                        return Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "additional checksum mismatch",
                        )));
                    }
                }

                cursor.part_offset = 0;
                cursor.part_number += 1;
            }
        } else {
            // move to next part
            hasher.update(&buf.filled()[before..remaining]);
            let local_checksum = hasher.finalize();
            let remote_checksum = get_checksum(
                &object_checksum.object_parts.as_ref().unwrap()[cursor.part_number],
                object_checksum.checksum_algorithm.as_ref().unwrap().clone(),
            )
            .unwrap();

            if local_checksum != remote_checksum {
                error!(
                    key = key,
                    version_id = version_id,
                    remote_checksum = remote_checksum,
                    local_checksum = local_checksum,
                    part_number = part_number,
                    "additional checksum mismatch."
                );

                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "additional checksum mismatch.",
                )));
            }

            hasher.update(&buf.filled()[remaining..]);

            cursor.part_offset = sync_bytes - remaining;
            cursor.part_number += 1;
        }
    }

    result
}

fn get_checksum(part: &ObjectPart, checksum_algorithm: ChecksumAlgorithm) -> Option<String> {
    match checksum_algorithm {
        ChecksumAlgorithm::Sha256 => part.checksum_sha256().map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Sha1 => part.checksum_sha1().map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc32 => part.checksum_crc32().map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc32C => part.checksum_crc32_c().map(|checksum| checksum.to_string()),
        _ => panic!("unknown algorithm"),
    }
}

#[cfg(test)]
#[cfg(target_family = "unix")]
mod tests {
    use std::path::PathBuf;

    use aws_sdk_s3::types::builders::ObjectPartBuilder;
    use tokio::fs::File;
    use tokio::io::AsyncReadExt;

    use super::*;

    const TEST_DATA_SIZE: usize = 5;

    const EMPTY_SHA256_BASE64_DIGEST: &str = "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=";
    const EMPTY_SHA256_BASE64_FINAL_DIGEST: &str = "Xfbg4nYTWdMKgnUFjimfzAOBU0VF9Vz0PkGYP11MlFY=-1";

    const SHA256_BASE64_FIRST_PART_DIGEST: &str = "a4ayc/80/OGda4BO/1o/V0etpOqiLx1JwB5S3beHW0s=";
    //const SHA256_BASE64_SECOND_PART_DIGEST: &str = "OAg8fukSHhdAGINWahSKpcLi1V3FO8SpSgJlF9v/PGs=";
    const SHA256_BASE64_FINAL_DIGEST: &str = "hZFcWDoaQSPadXkEHeQv0Fl6dnS5f7pQgPhW7eKJ3Lw=-2";

    pub const LARGE_FILE_PATH: &str = "./playground/large_data/9MiB";
    pub const LARGE_FILE_DIR: &str = "./playground/large_data/";
    pub const LARGE_FILE_SIZE: usize = 9 * 1024 * 1024;

    const LARGE_FILE_SHA256_BASE64_FIRST_PART_DIGEST: &str =
        "La6x82CVtEsxhBCz9Oi12Yncx7sCPRQmxJLasKMFPnQ=";
    //const LARGE_FILE_SHA256_BASE64_FIRST_PART_DIGEST: &str = "MOFJVevxNSJm3C/4Bn5oEEYH51CrudOzZYK4r5Cfy1g=";
    const LARGE_FILE_SHA256_BASE64_FINAL_DIGEST: &str =
        "zWifJvli3SaQ9LZtHxzpOjkUE9x4ovgJZ+34As/NMwc=-2";

    #[tokio::test]
    async fn callback_test() {
        init_dummy_tracing_subscriber();

        let file = File::open("test_data/5byte.dat").await.unwrap();
        let (stats_sender, stats_receiver) = async_channel::unbounded();
        let mut file_with_callback =
            AsyncReadWithCallback::new(file, stats_sender, None, None, None);

        let mut buffer = Vec::new();
        file_with_callback.read_to_end(&mut buffer).await.unwrap();

        assert!(!stats_receiver.is_empty());
        assert_eq!(buffer.len(), TEST_DATA_SIZE);
    }

    #[tokio::test]
    async fn callback_with_additional_checksum_empty_data_test() {
        init_dummy_tracing_subscriber();

        let file = File::open("test_data/empty/.gitkeep").await.unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let mut object_parts = Vec::new();
        object_parts.push(
            ObjectPartBuilder::default()
                .size(0)
                .checksum_sha256(EMPTY_SHA256_BASE64_DIGEST)
                .build(),
        );

        let object_checksum = Some(ObjectChecksum {
            key: "test".to_string(),
            version_id: None,
            checksum_algorithm: Some(ChecksumAlgorithm::Sha256),
            object_parts: Some(object_parts),
            final_checksum: Some(EMPTY_SHA256_BASE64_FINAL_DIGEST.to_string()),
        });

        let additional_checksum =
            Some(Arc::new(AdditionalChecksum::new(ChecksumAlgorithm::Sha256)));

        let mut file_with_callback = AsyncReadWithCallback::new(
            file,
            stats_sender,
            None,
            additional_checksum,
            object_checksum,
        );

        let mut buffer = Vec::new();
        file_with_callback.read_to_end(&mut buffer).await.unwrap();

        assert_eq!(buffer.len(), 0);
    }

    #[tokio::test]
    async fn callback_with_additional_checksum_error_test() {
        init_dummy_tracing_subscriber();

        let file = File::open("test_data/5byte.dat").await.unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let mut object_parts = Vec::new();
        object_parts.push(
            ObjectPartBuilder::default()
                .size(0)
                .checksum_sha256(EMPTY_SHA256_BASE64_DIGEST)
                .build(),
        );

        let object_checksum = Some(ObjectChecksum {
            key: "test".to_string(),
            version_id: None,
            checksum_algorithm: Some(ChecksumAlgorithm::Sha256),
            object_parts: Some(object_parts),
            final_checksum: Some(EMPTY_SHA256_BASE64_FINAL_DIGEST.to_string()),
        });

        let additional_checksum =
            Some(Arc::new(AdditionalChecksum::new(ChecksumAlgorithm::Sha256)));

        let mut file_with_callback = AsyncReadWithCallback::new(
            file,
            stats_sender,
            None,
            additional_checksum,
            object_checksum,
        );

        let mut buffer = Vec::new();
        assert!(file_with_callback.read_to_end(&mut buffer).await.is_err())
    }

    #[tokio::test]
    #[should_panic]
    async fn callback_with_additional_checksum_first_part_error_test() {
        init_dummy_tracing_subscriber();

        let file = File::open("test_data/5byte.dat").await.unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let mut object_parts = Vec::new();
        object_parts.push(
            ObjectPartBuilder::default()
                .size(1)
                .checksum_sha256(EMPTY_SHA256_BASE64_DIGEST)
                .build(),
        );
        object_parts.push(
            ObjectPartBuilder::default()
                .size(4)
                .checksum_sha256(EMPTY_SHA256_BASE64_DIGEST)
                .build(),
        );

        let object_checksum = Some(ObjectChecksum {
            key: "test".to_string(),
            version_id: None,
            checksum_algorithm: Some(ChecksumAlgorithm::Sha256),
            object_parts: Some(object_parts),
            final_checksum: Some(EMPTY_SHA256_BASE64_FINAL_DIGEST.to_string()),
        });

        let additional_checksum =
            Some(Arc::new(AdditionalChecksum::new(ChecksumAlgorithm::Sha256)));

        let mut file_with_callback = AsyncReadWithCallback::new(
            file,
            stats_sender,
            None,
            additional_checksum,
            object_checksum,
        );

        let mut buffer = Vec::new();
        let _ = file_with_callback.read_to_end(&mut buffer).await;
    }

    #[tokio::test]
    async fn callback_with_additional_checksum_last_part_error_test() {
        init_dummy_tracing_subscriber();

        let file = File::open("test_data/5byte.dat").await.unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let mut object_parts = Vec::new();
        object_parts.push(
            ObjectPartBuilder::default()
                .size(1)
                .checksum_sha256(SHA256_BASE64_FIRST_PART_DIGEST)
                .build(),
        );
        object_parts.push(
            ObjectPartBuilder::default()
                .size(4)
                .checksum_sha256("invalid-digest")
                .build(),
        );

        let object_checksum = Some(ObjectChecksum {
            key: "test".to_string(),
            version_id: None,
            checksum_algorithm: Some(ChecksumAlgorithm::Sha256),
            object_parts: Some(object_parts),
            final_checksum: Some(SHA256_BASE64_FINAL_DIGEST.to_string()),
        });

        let additional_checksum =
            Some(Arc::new(AdditionalChecksum::new(ChecksumAlgorithm::Sha256)));

        let mut file_with_callback = AsyncReadWithCallback::new(
            file,
            stats_sender,
            None,
            additional_checksum,
            object_checksum,
        );

        let mut buffer = Vec::new();
        assert!(file_with_callback.read_to_end(&mut buffer).await.is_err());
    }

    #[tokio::test]
    #[should_panic]
    async fn callback_with_additional_checksum_multipart_error_test() {
        init_dummy_tracing_subscriber();

        create_large_file().await;

        let file = File::open(LARGE_FILE_PATH).await.unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let mut object_parts = Vec::new();
        object_parts.push(
            ObjectPartBuilder::default()
                .size(8 * 1024 * 1024)
                .checksum_sha256(LARGE_FILE_SHA256_BASE64_FIRST_PART_DIGEST)
                .build(),
        );
        object_parts.push(
            ObjectPartBuilder::default()
                .size(1048576)
                .checksum_sha256("invalid-digest")
                .build(),
        );

        let object_checksum = Some(ObjectChecksum {
            key: "test".to_string(),
            version_id: None,
            checksum_algorithm: Some(ChecksumAlgorithm::Sha256),
            object_parts: Some(object_parts),
            final_checksum: Some(LARGE_FILE_SHA256_BASE64_FINAL_DIGEST.to_string()),
        });

        let additional_checksum =
            Some(Arc::new(AdditionalChecksum::new(ChecksumAlgorithm::Sha256)));

        let mut file_with_callback = AsyncReadWithCallback::new(
            file,
            stats_sender,
            None,
            additional_checksum,
            object_checksum,
        );

        let mut buffer = Vec::new();
        let _ = file_with_callback.read_to_end(&mut buffer).await;
    }

    #[tokio::test]
    async fn callback_with_additional_checksum_empty_data_error_test() {
        init_dummy_tracing_subscriber();

        let file = File::open("test_data/empty/.gitkeep").await.unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let mut object_parts = Vec::new();
        object_parts.push(
            ObjectPartBuilder::default()
                .size(0)
                .checksum_sha256("invalid-checksum")
                .build(),
        );

        let object_checksum = Some(ObjectChecksum {
            key: "test".to_string(),
            version_id: None,
            checksum_algorithm: Some(ChecksumAlgorithm::Sha256),
            object_parts: Some(object_parts),
            final_checksum: Some(EMPTY_SHA256_BASE64_FINAL_DIGEST.to_string()),
        });

        let additional_checksum =
            Some(Arc::new(AdditionalChecksum::new(ChecksumAlgorithm::Sha256)));

        let mut file_with_callback = AsyncReadWithCallback::new(
            file,
            stats_sender,
            None,
            additional_checksum,
            object_checksum,
        );

        let mut buffer = Vec::new();
        assert!(file_with_callback.read_to_end(&mut buffer).await.is_err())
    }

    async fn create_large_file() {
        if PathBuf::from(LARGE_FILE_PATH).try_exists().unwrap() {
            return;
        }

        tokio::fs::create_dir_all(LARGE_FILE_DIR).await.unwrap();

        let data = vec![0_u8; LARGE_FILE_SIZE];
        tokio::fs::write(LARGE_FILE_PATH, data.as_slice())
            .await
            .unwrap();
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
