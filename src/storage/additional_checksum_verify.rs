use std::path::Path;

use crate::storage::checksum::AdditionalChecksum;
use crate::types::error::S3syncError;
use crate::types::token::PipelineCancellationToken;
use anyhow::{Result, anyhow};
use aws_sdk_s3::types::ChecksumAlgorithm;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::time::Instant;
use tracing::debug;

const UNKNOWN_CHECKSUM_VALUE: &str = "UNKNOWN";

pub fn is_multipart_upload_checksum(checksum: &Option<String>) -> bool {
    if checksum.is_none() {
        return false;
    }

    let find_result = checksum.as_ref().unwrap().find('-');
    find_result.is_some()
}

pub async fn generate_checksum_from_path(
    path: &Path,
    checksum_algorithm: ChecksumAlgorithm,
    object_parts: Vec<i64>,
    multipart_threshold: usize,
    full_object_checksum: bool,
    cancellation_token: PipelineCancellationToken,
) -> Result<String> {
    if object_parts.is_empty() {
        panic!("parts_size is empty");
    }

    debug!(
        path = path.to_str().unwrap(),
        "generate_checksum_from_path() start. It will take a time if the file is large."
    );

    let checksum_generate_start_time = Instant::now();

    let mut file = File::open(path).await?;
    let mut checksum = AdditionalChecksum::new(checksum_algorithm, full_object_checksum);
    let parts_count = object_parts.len();
    let file_size = file.metadata().await?.len();

    let multipart =
        parts_count > 1 || (parts_count == 1 && multipart_threshold as i64 <= object_parts[0]);
    let mut read_bytes: usize = 0;
    let mut last_hash = "".to_string();
    for chunksize in object_parts {
        let mut buffer = Vec::<u8>::with_capacity(chunksize as usize);
        buffer.resize_with(chunksize as usize, Default::default);
        let read_result = file.read_exact(buffer.as_mut_slice()).await;
        if let Err(e) = read_result {
            return if e.kind() != std::io::ErrorKind::UnexpectedEof {
                Err(anyhow!("Failed to read: {:?}", e))
            } else {
                Ok(UNKNOWN_CHECKSUM_VALUE.to_string())
            };
        }
        read_bytes += read_result?;

        if cancellation_token.is_cancelled() {
            debug!(
                path = path.to_str().unwrap(),
                "generate_checksum_from_path() is cancelled."
            );
            return Err(anyhow!(S3syncError::Cancelled));
        }

        checksum.update(buffer.as_slice());
        last_hash = checksum.finalize()
    }

    if read_bytes != file_size as usize {
        return Ok(UNKNOWN_CHECKSUM_VALUE.to_string());
    }

    if !multipart {
        debug!(
            path = path.to_str().unwrap(),
            duration_ms = checksum_generate_start_time.elapsed().as_millis(),
            "generate_checksum_from_path() end."
        );
        return Ok(last_hash);
    }

    let final_checksum = Ok(checksum.finalize_all());
    debug!(
        path = path.to_str().unwrap(),
        duration_ms = checksum_generate_start_time.elapsed().as_millis(),
        "generate_checksum_from_path() end."
    );

    final_checksum
}

pub async fn generate_checksum_from_path_for_check(
    path: &Path,
    checksum_algorithm: ChecksumAlgorithm,
    multipart: bool,
    object_parts: Vec<i64>,
    full_object_checksum: bool,
    cancellation_token: PipelineCancellationToken,
) -> Result<String> {
    if object_parts.is_empty() {
        panic!("parts_size is empty");
    }
    if !multipart && 2 <= object_parts.len() {
        panic!("multipart is false but object_parts has more than 1 element");
    }

    debug!(
        path = path.to_str().unwrap(),
        "generate_checksum_from_path_for_check() start. It will take a time if the file is large."
    );

    let checksum_generate_start_time = Instant::now();

    let mut file = File::open(path).await?;
    let file_size = file.metadata().await?.len();

    let mut checksum = AdditionalChecksum::new(checksum_algorithm, full_object_checksum);
    let mut read_bytes: usize = 0;
    let mut last_hash = "".to_string();
    for chunksize in object_parts {
        let mut buffer = Vec::<u8>::with_capacity(chunksize as usize);
        buffer.resize_with(chunksize as usize, Default::default);
        let read_result = file.read_exact(buffer.as_mut_slice()).await;
        if read_result.is_err() {
            if let Err(e) = read_result {
                if e.kind() != std::io::ErrorKind::UnexpectedEof {
                    return Err(anyhow!("Failed to read: {:?}", e));
                } else {
                    return Ok(UNKNOWN_CHECKSUM_VALUE.to_string());
                }
            }
        }
        read_bytes += read_result?;

        if cancellation_token.is_cancelled() {
            debug!(
                path = path.to_str().unwrap(),
                "generate_checksum_from_path_for_check() is cancelled."
            );
            return Err(anyhow!(S3syncError::Cancelled));
        }

        checksum.update(buffer.as_slice());
        last_hash = checksum.finalize()
    }

    if read_bytes != file_size as usize {
        return Ok(UNKNOWN_CHECKSUM_VALUE.to_string());
    }

    if !multipart {
        debug!(
            path = path.to_str().unwrap(),
            duration_ms = checksum_generate_start_time.elapsed().as_millis(),
            "generate_checksum_from_path_for_check() end."
        );
        return Ok(last_hash);
    }

    let final_checksum = Ok(checksum.finalize_all());
    debug!(
        path = path.to_str().unwrap(),
        duration_ms = checksum_generate_start_time.elapsed().as_millis(),
        "generate_checksum_from_path_for_check() end."
    );

    final_checksum
}

pub async fn generate_checksum_from_path_with_chunksize(
    path: &Path,
    checksum_algorithm: ChecksumAlgorithm,
    multipart_chunksize: usize,
    multipart_threshold: usize,
    full_object_checksum: bool,
    cancellation_token: PipelineCancellationToken,
) -> Result<String> {
    let checksum_generate_start_time = Instant::now();
    debug!(
        path = path.to_str().unwrap(),
        "generate_checksum_from_path_with_chunksize() start. It will take a time if the file is large."
    );

    let mut file = File::open(path).await?;
    let mut remaining_bytes = file.metadata().await?.len();

    let mut checksum = AdditionalChecksum::new(checksum_algorithm, full_object_checksum);

    if remaining_bytes < multipart_threshold as u64 {
        let mut buffer = Vec::<u8>::with_capacity(multipart_threshold);
        buffer.resize_with(remaining_bytes as usize, Default::default);
        file.read_exact(buffer.as_mut_slice()).await?;
        checksum.update(buffer.as_slice());

        let final_checksum = Ok(checksum.finalize());
        debug!(
            path = path.to_str().unwrap(),
            duration_ms = checksum_generate_start_time.elapsed().as_millis(),
            "generate_checksum_from_path_with_chunksize() end."
        );
        return final_checksum;
    }

    while 0 < remaining_bytes {
        let real_chunksize: usize = if remaining_bytes < multipart_chunksize as u64 {
            remaining_bytes as usize
        } else {
            multipart_chunksize
        };

        let mut buffer = Vec::<u8>::with_capacity(real_chunksize);
        buffer.resize_with(real_chunksize, Default::default);
        file.read_exact(buffer.as_mut_slice()).await?;
        checksum.update(buffer.as_slice());
        // A full object checksum covers the whole object, so the file must not be split into parts.
        // Hash algorithms (SHA, MD5, XXHASH) ignore the full object flag and would otherwise produce
        // a composite "xxx-N" checksum. In that case, the object was uploaded in a single part.
        if !full_object_checksum {
            let _ = checksum.finalize();
        }

        if cancellation_token.is_cancelled() {
            debug!(
                path = path.to_str().unwrap(),
                "generate_checksum_from_path_with_chunksize() is cancelled."
            );
            return Err(anyhow!(S3syncError::Cancelled));
        }

        remaining_bytes -= real_chunksize as u64;
    }

    let final_checksum = if full_object_checksum {
        Ok(checksum.finalize())
    } else {
        Ok(checksum.finalize_all())
    };
    debug!(
        path = path.to_str().unwrap(),
        duration_ms = checksum_generate_start_time.elapsed().as_millis(),
        "generate_checksum_from_path_for_check() end."
    );

    final_checksum
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::storage::additional_checksum_verify::{
        UNKNOWN_CHECKSUM_VALUE, generate_checksum_from_path, generate_checksum_from_path_for_check,
        generate_checksum_from_path_with_chunksize, is_multipart_upload_checksum,
    };
    use crate::storage::checksum::AdditionalChecksum;
    use crate::storage::test_helpers::create_large_file;
    use crate::types::token::create_pipeline_cancellation_token;
    use aws_sdk_s3::types::ChecksumAlgorithm;
    use tracing_subscriber::EnvFilter;

    const TEST_SHA256_BASE64_DIGEST: &str = "WZRHGrsBESr8wYFZ9sx0tPURuZgG2lmzyvWpwXPKz8U=";

    pub const LARGE_FILE_PATH: &str = "./playground/large_data/9MiB";
    pub const LARGE_FILE_DIR: &str = "./playground/large_data/";
    pub const LARGE_FILE_SIZE: usize = 9 * 1024 * 1024;

    const LARGE_FILE_SHA256_BASE64_FINAL_DIGEST: &str =
        "zWifJvli3SaQ9LZtHxzpOjkUE9x4ovgJZ+34As/NMwc=-2";
    const THRESHOLD_SHA256_BASE64_FINAL_DIGEST: &str =
        "CPUzmvYGEjhKR5UARYyMgSkXZMUvVQ2BMxs1zpgY97g=-1";

    const LARGE_FILE_CRC32_FULL_OBJECT_BASE64_DIGEST: &str = "YyFLrQ==";
    const LARGE_FILE_CRC32C_FULL_OBJECT_BASE64_DIGEST: &str = "Gs/MBw==";
    const LARGE_FILE_CRC64NVME_FULL_OBJECT_BASE64_DIGEST: &str = "1q6/IYHP8XY=";

    // Digests of the whole 9MiB file as a single part (e.g. `openssl dgst -sha256 -binary | base64`).
    const LARGE_FILE_SHA256_SINGLE_PART_BASE64_DIGEST: &str =
        "0u5HA82WmJRcp7n+FonqMJVZfqwaCv2NugDKx4lP3EM=";
    const LARGE_FILE_SHA1_SINGLE_PART_BASE64_DIGEST: &str = "j3ZZsPo5lPzOK+Biu+oNGD6bxE4=";
    const LARGE_FILE_SHA512_SINGLE_PART_BASE64_DIGEST: &str =
        "CBSQPS16xHGAnhdRo54040XWfiCzG/W8BE8ZC+pES/4ccMj8TSnl1cIMZX1611g8uh2BzXcH94/xVENxEwuTgQ==";
    const LARGE_FILE_MD5_SINGLE_PART_BASE64_DIGEST: &str = "uCtKuH5El2Akq8FKFnDawA==";

    const TEST_CRC32_BASE64_DIGEST: &str = "y/U6HA==";

    #[test]
    fn is_multipart_upload_checksum_test() {
        init_dummy_tracing_subscriber();

        assert!(!is_multipart_upload_checksum(&None));
        assert!(!is_multipart_upload_checksum(&Some(
            "G2nS8dFcgMbpp8z9aYGNm97xnMGIf9BRjqs7z7hskVk=".to_string()
        )));

        assert!(is_multipart_upload_checksum(&Some(
            "wCfoo1d9Hfd+fHyNC38fUcN2GClCUsbud3NqVCs9Vww=-1".to_string()
        )));
        assert!(is_multipart_upload_checksum(&Some(
            "wCfoo1d9Hfd+fHyNC38fUcN2GClCUsbud3NqVCs9Vww=-2".to_string()
        )));
    }

    #[tokio::test]
    async fn generate_checksum_from_path_test() {
        init_dummy_tracing_subscriber();

        let checksum = generate_checksum_from_path(
            PathBuf::from("test_data/5byte.dat").as_path(),
            ChecksumAlgorithm::Sha256,
            vec![5],
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();

        assert_eq!(checksum, TEST_SHA256_BASE64_DIGEST.to_string());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_multipart_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        let checksum = generate_checksum_from_path(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            vec![8 * 1024 * 1024, 1048576],
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, LARGE_FILE_SHA256_BASE64_FINAL_DIGEST.to_string());

        let checksum = generate_checksum_from_path(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            vec![8 * 1024 * 1024, 1048575],
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());

        let checksum = generate_checksum_from_path(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            vec![8 * 1024 * 1024, 1048577],
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());

        let checksum = generate_checksum_from_path(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            vec![8 * 1024 * 1024, 1048576, 5],
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());

        let checksum = generate_checksum_from_path(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            vec![7 * 1024 * 1024],
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_multipart_cancel_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;
        let cancel_token = create_pipeline_cancellation_token();
        cancel_token.cancel();

        assert!(
            generate_checksum_from_path(
                PathBuf::from(LARGE_FILE_PATH).as_path(),
                ChecksumAlgorithm::Sha256,
                vec![8 * 1024 * 1024, 1048576],
                8 * 1024 * 1024,
                false,
                cancel_token,
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn generate_checksum_from_path_multipart_full_object_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        let checksum = generate_checksum_from_path(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Crc32C,
            vec![8 * 1024 * 1024, 1048576],
            8 * 1024 * 1024,
            true,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(
            checksum,
            LARGE_FILE_CRC32C_FULL_OBJECT_BASE64_DIGEST.to_string()
        );

        let checksum = generate_checksum_from_path(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Crc32C,
            vec![8 * 1024 * 1024, 1048575],
            8 * 1024 * 1024,
            true,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_threshold_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        let checksum = generate_checksum_from_path(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            vec![9 * 1024 * 1024],
            9 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();

        assert_eq!(checksum, THRESHOLD_SHA256_BASE64_FINAL_DIGEST.to_string());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_chunksize_test() {
        init_dummy_tracing_subscriber();

        let checksum = generate_checksum_from_path_with_chunksize(
            PathBuf::from("test_data/5byte.dat").as_path(),
            ChecksumAlgorithm::Sha256,
            8 * 1024 * 1024,
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();

        assert_eq!(checksum, TEST_SHA256_BASE64_DIGEST.to_string());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_chunksize_cancel_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        let cancel_token = create_pipeline_cancellation_token();
        cancel_token.cancel();

        assert!(
            generate_checksum_from_path_with_chunksize(
                PathBuf::from(LARGE_FILE_PATH).as_path(),
                ChecksumAlgorithm::Sha256,
                8 * 1024 * 1024,
                8 * 1024 * 1024,
                false,
                cancel_token,
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn generate_checksum_from_path_chunksize_full_object_test() {
        init_dummy_tracing_subscriber();

        let checksum = generate_checksum_from_path_with_chunksize(
            PathBuf::from("test_data/5byte.dat").as_path(),
            ChecksumAlgorithm::Crc32,
            8 * 1024 * 1024,
            8 * 1024 * 1024,
            true,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();

        assert_eq!(checksum, TEST_CRC32_BASE64_DIGEST.to_string());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_chunksize_multipart_full_object_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        let checksum = generate_checksum_from_path_with_chunksize(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Crc32,
            8 * 1024 * 1024,
            8 * 1024 * 1024,
            true,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();

        assert_eq!(
            checksum,
            LARGE_FILE_CRC32_FULL_OBJECT_BASE64_DIGEST.to_string()
        );
    }

    // A checksum without the "-N" suffix means the object was uploaded in a single part.
    // Even if the local file is larger than the multipart threshold, hash algorithms must digest
    // the whole file at once instead of producing a composite "xxx-N" checksum.
    #[tokio::test]
    async fn generate_checksum_from_path_chunksize_single_part_hash_over_threshold_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        for (algorithm, expected) in [
            (
                ChecksumAlgorithm::Sha256,
                LARGE_FILE_SHA256_SINGLE_PART_BASE64_DIGEST,
            ),
            (
                ChecksumAlgorithm::Sha1,
                LARGE_FILE_SHA1_SINGLE_PART_BASE64_DIGEST,
            ),
            (
                ChecksumAlgorithm::Sha512,
                LARGE_FILE_SHA512_SINGLE_PART_BASE64_DIGEST,
            ),
            (
                ChecksumAlgorithm::Md5,
                LARGE_FILE_MD5_SINGLE_PART_BASE64_DIGEST,
            ),
        ] {
            let checksum = generate_checksum_from_path_with_chunksize(
                PathBuf::from(LARGE_FILE_PATH).as_path(),
                algorithm.clone(),
                8 * 1024 * 1024,
                8 * 1024 * 1024,
                true,
                create_pipeline_cancellation_token(),
            )
            .await
            .unwrap();

            assert_eq!(checksum, expected.to_string(), "{algorithm}");
        }

        // XXHASH algorithms: compare against a single digest of the whole file.
        let data = tokio::fs::read(LARGE_FILE_PATH).await.unwrap();
        for algorithm in [
            ChecksumAlgorithm::Xxhash128,
            ChecksumAlgorithm::Xxhash3,
            ChecksumAlgorithm::Xxhash64,
        ] {
            let mut single_part = AdditionalChecksum::new(algorithm.clone(), false);
            single_part.update(data.as_slice());
            let expected = single_part.finalize();

            let checksum = generate_checksum_from_path_with_chunksize(
                PathBuf::from(LARGE_FILE_PATH).as_path(),
                algorithm.clone(),
                8 * 1024 * 1024,
                8 * 1024 * 1024,
                true,
                create_pipeline_cancellation_token(),
            )
            .await
            .unwrap();

            assert!(!checksum.contains('-'), "{algorithm}: {checksum}");
            assert_eq!(checksum, expected, "{algorithm}");
        }
    }

    // CRC full object checksums must stay the same when the file is larger than the threshold.
    #[tokio::test]
    async fn generate_checksum_from_path_chunksize_crc_full_object_over_threshold_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        for (algorithm, expected) in [
            (
                ChecksumAlgorithm::Crc32,
                LARGE_FILE_CRC32_FULL_OBJECT_BASE64_DIGEST,
            ),
            (
                ChecksumAlgorithm::Crc32C,
                LARGE_FILE_CRC32C_FULL_OBJECT_BASE64_DIGEST,
            ),
            (
                ChecksumAlgorithm::Crc64Nvme,
                LARGE_FILE_CRC64NVME_FULL_OBJECT_BASE64_DIGEST,
            ),
        ] {
            let checksum = generate_checksum_from_path_with_chunksize(
                PathBuf::from(LARGE_FILE_PATH).as_path(),
                algorithm.clone(),
                8 * 1024 * 1024,
                8 * 1024 * 1024,
                true,
                create_pipeline_cancellation_token(),
            )
            .await
            .unwrap();

            assert_eq!(checksum, expected.to_string(), "{algorithm}");
        }
    }

    #[tokio::test]
    async fn generate_checksum_from_path_multipart_chunksize_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        let checksum = generate_checksum_from_path_with_chunksize(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            8 * 1024 * 1024,
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();

        assert_eq!(checksum, LARGE_FILE_SHA256_BASE64_FINAL_DIGEST.to_string());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_for_check_test() {
        init_dummy_tracing_subscriber();

        let checksum = generate_checksum_from_path_for_check(
            PathBuf::from("test_data/5byte.dat").as_path(),
            ChecksumAlgorithm::Sha256,
            false,
            vec![5],
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();

        assert_eq!(checksum, TEST_SHA256_BASE64_DIGEST.to_string());
    }
    #[tokio::test]
    async fn generate_checksum_from_path_for_check_multipart_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        let checksum = generate_checksum_from_path_for_check(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            true,
            vec![8 * 1024 * 1024, 1048576],
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, LARGE_FILE_SHA256_BASE64_FINAL_DIGEST.to_string());

        let checksum = generate_checksum_from_path_for_check(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            true,
            vec![8 * 1024 * 1024, 1048575],
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());

        let checksum = generate_checksum_from_path_for_check(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            true,
            vec![8 * 1024 * 1024, 1048577],
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());

        let checksum = generate_checksum_from_path_for_check(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            true,
            vec![8 * 1024 * 1024, 1048576, 5],
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());

        let checksum = generate_checksum_from_path_for_check(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            true,
            vec![7 * 1024 * 1024],
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_for_check_multipart_cancel_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        let cancel_token = create_pipeline_cancellation_token();
        cancel_token.cancel();

        assert!(
            generate_checksum_from_path_for_check(
                PathBuf::from(LARGE_FILE_PATH).as_path(),
                ChecksumAlgorithm::Sha256,
                true,
                vec![8 * 1024 * 1024, 1048576],
                false,
                cancel_token
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn generate_checksum_from_path_for_check_full_object_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        let checksum = generate_checksum_from_path_for_check(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Crc32,
            true,
            vec![8 * 1024 * 1024, 1048576],
            true,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(
            checksum,
            LARGE_FILE_CRC32_FULL_OBJECT_BASE64_DIGEST.to_string()
        );

        let checksum = generate_checksum_from_path_for_check(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Crc32C,
            true,
            vec![8 * 1024 * 1024, 1048576],
            true,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(
            checksum,
            LARGE_FILE_CRC32C_FULL_OBJECT_BASE64_DIGEST.to_string()
        );

        let checksum = generate_checksum_from_path_for_check(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Crc64Nvme,
            true,
            vec![8 * 1024 * 1024, 1048576],
            true,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(
            checksum,
            LARGE_FILE_CRC64NVME_FULL_OBJECT_BASE64_DIGEST.to_string()
        );
    }

    #[tokio::test]
    #[should_panic(expected = "parts_size is empty")]
    async fn generate_checksum_from_path_empty_parts_panic_test() {
        init_dummy_tracing_subscriber();

        let _ = generate_checksum_from_path(
            PathBuf::from("test_data/5byte.dat").as_path(),
            ChecksumAlgorithm::Sha256,
            vec![],
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "parts_size is empty")]
    async fn generate_checksum_from_path_for_check_empty_parts_panic_test() {
        init_dummy_tracing_subscriber();

        let _ = generate_checksum_from_path_for_check(
            PathBuf::from("test_data/5byte.dat").as_path(),
            ChecksumAlgorithm::Sha256,
            true,
            vec![],
            false,
            create_pipeline_cancellation_token(),
        )
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "multipart is false but object_parts has more than 1 element")]
    async fn generate_checksum_from_path_for_check_not_multipart_many_parts_panic_test() {
        init_dummy_tracing_subscriber();

        let _ = generate_checksum_from_path_for_check(
            PathBuf::from("test_data/5byte.dat").as_path(),
            ChecksumAlgorithm::Sha256,
            false,
            vec![2, 3],
            false,
            create_pipeline_cancellation_token(),
        )
        .await;
    }

    #[cfg(target_family = "unix")]
    #[tokio::test]
    async fn generate_checksum_from_path_read_error_test() {
        init_dummy_tracing_subscriber();

        // Reading a directory fails with an error that is not UnexpectedEof.
        let result = generate_checksum_from_path(
            PathBuf::from("test_data").as_path(),
            ChecksumAlgorithm::Sha256,
            vec![10],
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await;

        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .starts_with("Failed to read")
        );
    }

    #[cfg(target_family = "unix")]
    #[tokio::test]
    async fn generate_checksum_from_path_for_check_read_error_test() {
        init_dummy_tracing_subscriber();

        // Reading a directory fails with an error that is not UnexpectedEof.
        let result = generate_checksum_from_path_for_check(
            PathBuf::from("test_data").as_path(),
            ChecksumAlgorithm::Sha256,
            true,
            vec![10],
            false,
            create_pipeline_cancellation_token(),
        )
        .await;

        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .starts_with("Failed to read")
        );
    }

    #[tokio::test]
    async fn generate_checksum_from_path_cancel_with_trace_test() {
        init_dummy_tracing_subscriber();
        let _guard = set_scoped_trace_subscriber();

        let cancel_token = create_pipeline_cancellation_token();
        cancel_token.cancel();

        let result = generate_checksum_from_path(
            PathBuf::from("test_data/5byte.dat").as_path(),
            ChecksumAlgorithm::Sha256,
            vec![1],
            8 * 1024 * 1024,
            false,
            cancel_token,
        )
        .await;

        assert!(crate::types::error::is_cancelled_error(
            &result.err().unwrap()
        ));
    }

    #[tokio::test]
    async fn generate_checksum_from_path_for_check_cancel_with_trace_test() {
        init_dummy_tracing_subscriber();
        let _guard = set_scoped_trace_subscriber();

        let cancel_token = create_pipeline_cancellation_token();
        cancel_token.cancel();

        let result = generate_checksum_from_path_for_check(
            PathBuf::from("test_data/5byte.dat").as_path(),
            ChecksumAlgorithm::Sha256,
            true,
            vec![1],
            false,
            cancel_token,
        )
        .await;

        assert!(crate::types::error::is_cancelled_error(
            &result.err().unwrap()
        ));
    }

    #[tokio::test]
    async fn generate_checksum_from_path_for_check_not_multipart_with_trace_test() {
        init_dummy_tracing_subscriber();
        let _guard = set_scoped_trace_subscriber();

        let checksum = generate_checksum_from_path_for_check(
            PathBuf::from("test_data/5byte.dat").as_path(),
            ChecksumAlgorithm::Sha256,
            false,
            vec![5],
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();

        assert_eq!(checksum, TEST_SHA256_BASE64_DIGEST.to_string());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_chunksize_cancel_with_trace_test() {
        init_dummy_tracing_subscriber();
        let _guard = set_scoped_trace_subscriber();

        let cancel_token = create_pipeline_cancellation_token();
        cancel_token.cancel();

        let result = generate_checksum_from_path_with_chunksize(
            PathBuf::from("test_data/5byte.dat").as_path(),
            ChecksumAlgorithm::Sha256,
            2,
            1,
            false,
            cancel_token,
        )
        .await;

        assert!(crate::types::error::is_cancelled_error(
            &result.err().unwrap()
        ));
    }

    // A thread-local subscriber that enables s3sync events so that lazily evaluated
    // tracing macro fields are executed.
    fn set_scoped_trace_subscriber() -> tracing::subscriber::DefaultGuard {
        tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_env_filter(EnvFilter::try_new("s3sync=trace").unwrap())
                .with_writer(std::io::sink)
                .finish(),
        )
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
