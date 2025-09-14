use std::path::Path;

use crate::types::error::S3syncError;
use crate::types::token::PipelineCancellationToken;
use anyhow::{Result, anyhow};
use aws_sdk_s3::types::ServerSideEncryption;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::time::Instant;
use tracing::debug;

const UNKNOWN_E_TAG_VALUE: &str = "UNKNOWN";

pub fn verify_e_tag(
    verify_multipart_upload: bool,
    source_sse_c: &Option<String>,
    target_sse_c: &Option<String>,
    source_sse: &Option<ServerSideEncryption>,
    source_e_tag: &Option<String>,
    target_sse: &Option<ServerSideEncryption>,
    target_e_tag: &Option<String>,
) -> Option<bool> {
    if source_sse_c.is_some() || target_sse_c.is_some() {
        return None;
    }

    if !is_verification_supported_sse(source_sse) || !is_verification_supported_sse(target_sse) {
        return None;
    }

    if source_e_tag.is_none() || target_e_tag.is_none() {
        return None;
    }

    let source_normalized_e_tag = normalize_e_tag(source_e_tag);
    let target_normalized_e_tag = normalize_e_tag(target_e_tag);

    if verify_multipart_upload {
        return Some(source_normalized_e_tag == target_normalized_e_tag);
    }

    if is_multipart_upload_e_tag(&source_normalized_e_tag)
        || is_multipart_upload_e_tag(&target_normalized_e_tag)
    {
        return None;
    }

    Some(source_normalized_e_tag == target_normalized_e_tag)
}

pub fn is_multipart_upload_e_tag(e_tag: &Option<String>) -> bool {
    if e_tag.is_none() {
        return false;
    }

    let find_result = e_tag.as_ref().unwrap().find('-');
    find_result.is_some()
}

pub fn generate_e_tag_hash(concatnated_md5_hash: &[u8], parts_count: i64) -> String {
    if parts_count == 0 {
        format!("{:?}", hex::encode(concatnated_md5_hash))
    } else {
        format!(
            "\"{:?}-{}\"",
            md5::compute(concatnated_md5_hash),
            parts_count
        )
    }
}

pub async fn generate_e_tag_hash_from_path(
    path: &Path,
    multipart_chunksize: usize,
    multipart_threshold: usize,
    cancellation_token: PipelineCancellationToken,
) -> Result<String> {
    debug!(
        path = path.to_str(),
        "generate_e_tag_hash_from_path() start. It will take a time if the file is large."
    );

    let etag_generate_start_time = Instant::now();

    let mut file = File::open(path).await?;

    let mut remaining_bytes = file.metadata().await?.len();

    if remaining_bytes < multipart_threshold as u64 {
        let mut buffer = Vec::<u8>::with_capacity(multipart_threshold);
        buffer.resize_with(remaining_bytes as usize, Default::default);
        file.read_exact(buffer.as_mut_slice()).await?;

        let final_etag = Ok(generate_e_tag_hash(md5::compute(&buffer).as_slice(), 0));
        debug!(
            path = path.to_str().unwrap(),
            duration_ms = etag_generate_start_time.elapsed().as_millis(),
            "generate_e_tag_hash_from_path() end."
        );
        return final_etag;
    }

    let mut parts_count = 0;
    let mut concatnated_md5_hash = Vec::new();
    while 0 < remaining_bytes {
        let real_chunksize: usize = if remaining_bytes < multipart_chunksize as u64 {
            remaining_bytes as usize
        } else {
            multipart_chunksize
        };

        let mut buffer = Vec::<u8>::with_capacity(multipart_chunksize);
        buffer.resize_with(real_chunksize, Default::default);
        file.read_exact(buffer.as_mut_slice()).await?;

        let mut md5_digest = md5::compute(&buffer).as_slice().to_vec();
        concatnated_md5_hash.append(&mut md5_digest);
        remaining_bytes -= real_chunksize as u64;
        parts_count += 1;

        if cancellation_token.is_cancelled() {
            debug!(
                path = path.to_str(),
                "generate_e_tag_hash_from_path() cancelled"
            );
            return Err(anyhow!(S3syncError::Cancelled));
        }
    }

    let hash = Ok(generate_e_tag_hash(&concatnated_md5_hash, parts_count));
    debug!(
        path = path.to_str().unwrap(),
        duration_ms = etag_generate_start_time.elapsed().as_millis(),
        "generate_e_tag_hash_from_path() end."
    );

    hash
}

pub async fn generate_e_tag_hash_from_path_with_auto_chunksize(
    path: &Path,
    object_parts: Vec<i64>,
    cancellation_token: PipelineCancellationToken,
) -> Result<String> {
    debug!(
        path = path.to_str(),
        "generate_e_tag_hash_from_path_with_auto_chunksize() start. It will take a time if the file is large."
    );

    if object_parts.is_empty() {
        panic!("object_parts is empty");
    }

    let etag_generate_start_time = Instant::now();

    let mut file = File::open(path).await?;
    let file_size = file.metadata().await?.len();

    let mut parts_count = 0;
    let mut read_bytes: usize = 0;
    let mut concatnated_md5_hash = Vec::new();
    for chunksize in object_parts {
        let mut buffer = Vec::<u8>::with_capacity(chunksize as usize);
        buffer.resize_with(chunksize as usize, Default::default);
        let read_result = file.read_exact(buffer.as_mut_slice()).await;
        if read_result.is_err() {
            if let Err(e) = read_result {
                if e.kind() != std::io::ErrorKind::UnexpectedEof {
                    return Err(anyhow!("Failed to read: {:?}", e));
                } else {
                    return Ok(UNKNOWN_E_TAG_VALUE.to_string());
                }
            }
        }
        read_bytes += read_result?;

        let mut md5_digest = md5::compute(&buffer).as_slice().to_vec();
        concatnated_md5_hash.append(&mut md5_digest);
        parts_count += 1;

        if cancellation_token.is_cancelled() {
            debug!(
                path = path.to_str().unwrap(),
                "generate_e_tag_hash_from_path_with_auto_chunksize() cancelled."
            );
            return Err(anyhow!(S3syncError::Cancelled));
        }
    }

    if read_bytes != file_size as usize {
        return Ok(UNKNOWN_E_TAG_VALUE.to_string());
    }

    let hash = Ok(generate_e_tag_hash(&concatnated_md5_hash, parts_count));
    debug!(
        path = path.to_str().unwrap(),
        duration_ms = etag_generate_start_time.elapsed().as_millis(),
        "generate_e_tag_hash_from_path_with_auto_chunksize() end."
    );

    hash
}
pub fn normalize_e_tag(e_tag: &Option<String>) -> Option<String> {
    if e_tag.is_none() {
        return e_tag.clone();
    }

    Some(e_tag.as_ref().unwrap().replace('\"', ""))
}
fn is_verification_supported_sse(sse: &Option<ServerSideEncryption>) -> bool {
    if sse.is_none() {
        return true;
    }

    ServerSideEncryption::Aes256 == *sse.as_ref().unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::token::create_pipeline_cancellation_token;
    use std::path::PathBuf;
    use tracing_subscriber::EnvFilter;

    pub const LARGE_FILE_PATH: &str = "./playground/large_data/50MiB";
    pub const LARGE_FILE_DIR: &str = "./playground/large_data/";
    pub const LARGE_FILE_SIZE: usize = 50 * 1024 * 1024;
    pub const LARGE_FILE_S3_MULTIPART_ETAG: &str = "\"73d8a713f6f80a5e82a0ea8c92f0cab1-7\"";
    pub const LARGE_FILE_S3_CHUNK_5MB_ETAG: &str = "\"264bd8c2d8d9f3350ef680af6ddc51f2-10\"";
    pub const LARGE_FILE_MD5_DIGEST: &str = "\"25e317773f308e446cc84c503a6d1f85\"";
    pub const LARGE_FILE_S3_AUTO_CHUNKSIZE_ETAG: &str = "\"e84d1a5ef1f6afdb0d2c3072adf5fba5-4\"";

    #[test]
    fn is_verification_supported_sse_test() {
        init_dummy_tracing_subscriber();

        assert!(is_verification_supported_sse(&None));
        assert!(is_verification_supported_sse(&Some(
            ServerSideEncryption::Aes256
        )));
        assert!(!is_verification_supported_sse(&Some(
            ServerSideEncryption::AwsKms
        )));
    }

    #[test]
    fn normalize_e_tag_test() {
        init_dummy_tracing_subscriber();

        assert_eq!(normalize_e_tag(&None), None);
        assert_eq!(
            normalize_e_tag(&Some("\"b7c136b1987c972de7d0808e12221abe\"".to_string())),
            Some("b7c136b1987c972de7d0808e12221abe".to_string())
        );
        assert_eq!(
            normalize_e_tag(&Some("b7c136b1987c972de7d0808e12221abe".to_string())),
            Some("b7c136b1987c972de7d0808e12221abe".to_string())
        );
    }

    #[test]
    fn is_multipart_upload_e_tag_test() {
        init_dummy_tracing_subscriber();

        assert!(!is_multipart_upload_e_tag(&None));
        assert!(!is_multipart_upload_e_tag(&Some(
            "b7c136b1987c972de7d0808e12221abe".to_string()
        )));
        assert!(!is_multipart_upload_e_tag(&Some(
            "\"b7c136b1987c972de7d0808e12221abe\"".to_string()
        )));

        assert!(is_multipart_upload_e_tag(&Some(
            "b7c136b1987c972de7d0808e12221abe-2".to_string()
        )));
        assert!(is_multipart_upload_e_tag(&Some(
            "\"b7c136b1987c972de7d0808e12221abe-300\"".to_string()
        )));
    }

    #[test]
    fn verify_e_tag_with_multipart_test() {
        init_dummy_tracing_subscriber();

        let e_tag_1: &Option<String> = &Some("\"111136b1987c972de7d0808e4132423e\"".to_string());
        let e_tag_2: &Option<String> = &Some("\"222236b1987c972de7d0808e13241abe\"".to_string());

        let multipart_e_tag_1: &Option<String> =
            &Some("\"111136b1987c972de7d0808e4132423e-3\"".to_string());
        let multipart_e_tag_2: &Option<String> =
            &Some("\"222236b1987c972de7d0808e13241abe-4\"".to_string());

        let sse_aes = &Some(ServerSideEncryption::Aes256);
        let sse_kms = &Some(ServerSideEncryption::AwsKms);

        {
            assert_eq!(
                verify_e_tag(true, &None, &None, &None, e_tag_1, &None, e_tag_1),
                Some(true)
            );
            assert_eq!(
                verify_e_tag(
                    true,
                    &None,
                    &None,
                    &None,
                    multipart_e_tag_1,
                    &None,
                    multipart_e_tag_1
                ),
                Some(true)
            );
            assert_eq!(
                verify_e_tag(true, &None, &None, sse_aes, e_tag_1, sse_aes, e_tag_1),
                Some(true)
            );
            assert_eq!(
                verify_e_tag(
                    true,
                    &None,
                    &None,
                    sse_aes,
                    multipart_e_tag_1,
                    sse_aes,
                    multipart_e_tag_1
                ),
                Some(true)
            );

            assert_eq!(
                verify_e_tag(true, &None, &None, sse_aes, e_tag_1, &None, e_tag_1),
                Some(true)
            );
            assert_eq!(
                verify_e_tag(
                    true,
                    &None,
                    &None,
                    &None,
                    multipart_e_tag_1,
                    sse_aes,
                    multipart_e_tag_1
                ),
                Some(true)
            );
        }

        {
            assert_eq!(
                verify_e_tag(true, &None, &None, &None, e_tag_1, &None, e_tag_2),
                Some(false)
            );
            assert_eq!(
                verify_e_tag(
                    true,
                    &None,
                    &None,
                    &None,
                    multipart_e_tag_1,
                    &None,
                    multipart_e_tag_2
                ),
                Some(false)
            );
            assert_eq!(
                verify_e_tag(true, &None, &None, sse_aes, e_tag_1, sse_aes, e_tag_2),
                Some(false)
            );
            assert_eq!(
                verify_e_tag(
                    true,
                    &None,
                    &None,
                    sse_aes,
                    multipart_e_tag_1,
                    sse_aes,
                    multipart_e_tag_2
                ),
                Some(false)
            );
            assert_eq!(
                verify_e_tag(true, &None, &None, sse_aes, e_tag_1, &None, e_tag_2),
                Some(false)
            );
            assert_eq!(
                verify_e_tag(
                    true,
                    &None,
                    &None,
                    &None,
                    multipart_e_tag_1,
                    sse_aes,
                    multipart_e_tag_2
                ),
                Some(false)
            );
        }

        {
            assert_eq!(
                verify_e_tag(true, &None, &None, sse_kms, e_tag_1, &None, e_tag_1),
                None
            );
            assert_eq!(
                verify_e_tag(
                    true,
                    &None,
                    &None,
                    &None,
                    multipart_e_tag_1,
                    sse_kms,
                    multipart_e_tag_1
                ),
                None
            );
            assert_eq!(
                verify_e_tag(true, &None, &None, sse_kms, e_tag_1, sse_kms, e_tag_1),
                None
            );
            assert_eq!(
                verify_e_tag(
                    true,
                    &None,
                    &None,
                    sse_kms,
                    multipart_e_tag_1,
                    sse_kms,
                    multipart_e_tag_1
                ),
                None
            );

            assert_eq!(
                verify_e_tag(true, &None, &None, sse_kms, e_tag_1, &None, e_tag_1),
                None
            );
            assert_eq!(
                verify_e_tag(
                    true,
                    &None,
                    &None,
                    &None,
                    multipart_e_tag_1,
                    sse_kms,
                    multipart_e_tag_1
                ),
                None
            );
        }
    }

    #[test]
    fn verify_e_tag_without_multipart_test() {
        init_dummy_tracing_subscriber();

        let e_tag_1: &Option<String> = &Some("\"111136b1987c972de7d0808e4132423e\"".to_string());
        let e_tag_2: &Option<String> = &Some("\"222236b1987c972de7d0808e13241abe\"".to_string());

        let multipart_e_tag_1: &Option<String> =
            &Some("\"111136b1987c972de7d0808e4132423e-3\"".to_string());
        let multipart_e_tag_2: &Option<String> =
            &Some("\"222236b1987c972de7d0808e13241abe-4\"".to_string());

        let sse_aes = &Some(ServerSideEncryption::Aes256);
        let sse_kms = &Some(ServerSideEncryption::AwsKms);

        {
            assert_eq!(
                verify_e_tag(false, &None, &None, &None, e_tag_1, &None, e_tag_1),
                Some(true)
            );
            assert_eq!(
                verify_e_tag(
                    false,
                    &None,
                    &None,
                    &None,
                    multipart_e_tag_1,
                    &None,
                    multipart_e_tag_1
                ),
                None
            );
            assert_eq!(
                verify_e_tag(false, &None, &None, sse_aes, e_tag_1, sse_aes, e_tag_1),
                Some(true)
            );
            assert_eq!(
                verify_e_tag(
                    false,
                    &None,
                    &None,
                    sse_aes,
                    multipart_e_tag_1,
                    sse_aes,
                    multipart_e_tag_1
                ),
                None
            );

            assert_eq!(
                verify_e_tag(false, &None, &None, sse_aes, e_tag_1, &None, e_tag_1),
                Some(true)
            );
            assert_eq!(
                verify_e_tag(
                    false,
                    &None,
                    &None,
                    &None,
                    multipart_e_tag_1,
                    sse_aes,
                    multipart_e_tag_1
                ),
                None
            );
        }

        {
            assert_eq!(
                verify_e_tag(false, &None, &None, &None, e_tag_1, &None, e_tag_2),
                Some(false)
            );
            assert_eq!(
                verify_e_tag(
                    false,
                    &None,
                    &None,
                    &None,
                    multipart_e_tag_1,
                    &None,
                    multipart_e_tag_2
                ),
                None
            );
            assert_eq!(
                verify_e_tag(false, &None, &None, sse_aes, e_tag_1, sse_aes, e_tag_2),
                Some(false)
            );
            assert_eq!(
                verify_e_tag(
                    false,
                    &None,
                    &None,
                    sse_aes,
                    multipart_e_tag_1,
                    sse_aes,
                    multipart_e_tag_2
                ),
                None
            );
            assert_eq!(
                verify_e_tag(false, &None, &None, sse_aes, e_tag_1, &None, e_tag_2),
                Some(false)
            );
            assert_eq!(
                verify_e_tag(
                    false,
                    &None,
                    &None,
                    &None,
                    multipart_e_tag_1,
                    sse_aes,
                    multipart_e_tag_2
                ),
                None
            );
        }

        {
            assert_eq!(
                verify_e_tag(false, &None, &None, sse_kms, e_tag_1, &None, e_tag_1),
                None
            );
            assert_eq!(
                verify_e_tag(
                    false,
                    &None,
                    &None,
                    &None,
                    multipart_e_tag_1,
                    sse_kms,
                    multipart_e_tag_1
                ),
                None
            );
            assert_eq!(
                verify_e_tag(false, &None, &None, sse_kms, e_tag_1, sse_kms, e_tag_1),
                None
            );
            assert_eq!(
                verify_e_tag(
                    false,
                    &None,
                    &None,
                    sse_kms,
                    multipart_e_tag_1,
                    sse_kms,
                    multipart_e_tag_1
                ),
                None
            );

            assert_eq!(
                verify_e_tag(false, &None, &None, sse_kms, e_tag_1, &None, e_tag_1),
                None
            );
            assert_eq!(
                verify_e_tag(
                    false,
                    &None,
                    &None,
                    &None,
                    multipart_e_tag_1,
                    sse_kms,
                    multipart_e_tag_1
                ),
                None
            );
        }
    }

    #[tokio::test]
    async fn generate_e_tag_hash_from_path_test() {
        init_dummy_tracing_subscriber();

        create_large_file().await;

        assert_eq!(
            generate_e_tag_hash_from_path(
                &PathBuf::from(LARGE_FILE_PATH),
                8 * 1024 * 1024,
                8 * 1024 * 1024,
                create_pipeline_cancellation_token(),
            )
            .await
            .unwrap(),
            LARGE_FILE_S3_MULTIPART_ETAG.to_string()
        );

        assert_eq!(
            generate_e_tag_hash_from_path(
                &PathBuf::from(LARGE_FILE_PATH),
                100 * 1024 * 1024,
                100 * 1024 * 1024,
                create_pipeline_cancellation_token(),
            )
            .await
            .unwrap(),
            LARGE_FILE_MD5_DIGEST.to_string()
        );

        assert_eq!(
            generate_e_tag_hash_from_path(
                &PathBuf::from(LARGE_FILE_PATH),
                5 * 1024 * 1024,
                10 * 1024 * 1024,
                create_pipeline_cancellation_token(),
            )
            .await
            .unwrap(),
            LARGE_FILE_S3_CHUNK_5MB_ETAG.to_string()
        );
    }

    #[tokio::test]
    async fn generate_e_tag_hash_from_path_cancel_test() {
        init_dummy_tracing_subscriber();

        create_large_file().await;

        let cancel_token = create_pipeline_cancellation_token();
        cancel_token.cancel();

        assert!(
            generate_e_tag_hash_from_path(
                &PathBuf::from(LARGE_FILE_PATH),
                8 * 1024 * 1024,
                8 * 1024 * 1024,
                cancel_token,
            )
            .await
            .is_err()
        )
    }

    #[tokio::test]
    async fn generate_e_tag_hash_from_path_auto_chunksize_test() {
        init_dummy_tracing_subscriber();

        create_large_file().await;

        assert_eq!(
            generate_e_tag_hash_from_path_with_auto_chunksize(
                &PathBuf::from(LARGE_FILE_PATH),
                vec![17179870, 17179870, 17179870, 889190],
                create_pipeline_cancellation_token(),
            )
            .await
            .unwrap(),
            LARGE_FILE_S3_AUTO_CHUNKSIZE_ETAG
        );

        assert_eq!(
            generate_e_tag_hash_from_path_with_auto_chunksize(
                &PathBuf::from(LARGE_FILE_PATH),
                vec![17179870, 17179870, 17179870, 889191],
                create_pipeline_cancellation_token(),
            )
            .await
            .unwrap(),
            UNKNOWN_E_TAG_VALUE
        );

        assert_eq!(
            generate_e_tag_hash_from_path_with_auto_chunksize(
                &PathBuf::from(LARGE_FILE_PATH),
                vec![17179870, 17179870, 17179870, 889189],
                create_pipeline_cancellation_token(),
            )
            .await
            .unwrap(),
            UNKNOWN_E_TAG_VALUE
        );

        assert_eq!(
            generate_e_tag_hash_from_path_with_auto_chunksize(
                &PathBuf::from(LARGE_FILE_PATH),
                vec![17179870, 17179870, 889189],
                create_pipeline_cancellation_token(),
            )
            .await
            .unwrap(),
            UNKNOWN_E_TAG_VALUE
        );

        assert_eq!(
            generate_e_tag_hash_from_path_with_auto_chunksize(
                &PathBuf::from(LARGE_FILE_PATH),
                vec![17179870, 17179870, 17179870, 889190, 32],
                create_pipeline_cancellation_token(),
            )
            .await
            .unwrap(),
            UNKNOWN_E_TAG_VALUE
        );

        assert_eq!(
            generate_e_tag_hash_from_path_with_auto_chunksize(
                &PathBuf::from(LARGE_FILE_PATH),
                vec![17179870, 17179870],
                create_pipeline_cancellation_token(),
            )
            .await
            .unwrap(),
            UNKNOWN_E_TAG_VALUE
        );
    }

    #[tokio::test]
    async fn generate_e_tag_hash_from_path_auto_chunksize_cancel_test() {
        init_dummy_tracing_subscriber();

        create_large_file().await;

        let cancel_token = create_pipeline_cancellation_token();
        cancel_token.cancel();

        assert!(
            generate_e_tag_hash_from_path_with_auto_chunksize(
                &PathBuf::from(LARGE_FILE_PATH),
                vec![17179870, 17179870, 17179870, 889190],
                cancel_token,
            )
            .await
            .is_err()
        );
    }

    #[cfg_attr(coverage_nightly, coverage(off))]
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
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .or_else(|_| EnvFilter::try_new("dummy=trace"))
                    .unwrap(),
            )
            .try_init();
    }
}
