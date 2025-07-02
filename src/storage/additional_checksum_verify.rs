use std::path::Path;

use crate::storage::checksum::AdditionalChecksum;
use anyhow::{anyhow, Result};
use aws_sdk_s3::types::ChecksumAlgorithm;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

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
) -> Result<String> {
    if object_parts.is_empty() {
        panic!("parts_size is empty");
    }

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

        checksum.update(buffer.as_slice());
        last_hash = checksum.finalize()
    }

    if read_bytes != file_size as usize {
        return Ok(UNKNOWN_CHECKSUM_VALUE.to_string());
    }

    if !multipart {
        return Ok(last_hash);
    }

    Ok(checksum.finalize_all())
}

pub async fn generate_checksum_from_path_for_check(
    path: &Path,
    checksum_algorithm: ChecksumAlgorithm,
    multipart: bool,
    object_parts: Vec<i64>,
    full_object_checksum: bool,
) -> Result<String> {
    if object_parts.is_empty() {
        panic!("parts_size is empty");
    }
    if !multipart && 2 <= object_parts.len() {
        panic!("multipart is false but object_parts has more than 1 element");
    }

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

        checksum.update(buffer.as_slice());
        last_hash = checksum.finalize()
    }

    if read_bytes != file_size as usize {
        return Ok(UNKNOWN_CHECKSUM_VALUE.to_string());
    }

    if !multipart {
        return Ok(last_hash);
    }

    Ok(checksum.finalize_all())
}

pub async fn generate_checksum_from_path_with_chunksize(
    path: &Path,
    checksum_algorithm: ChecksumAlgorithm,
    multipart_chunksize: usize,
    multipart_threshold: usize,
    full_object_checksum: bool,
) -> Result<String> {
    let mut file = File::open(path).await?;
    let mut remaining_bytes = file.metadata().await?.len();

    let mut checksum = AdditionalChecksum::new(checksum_algorithm, full_object_checksum);

    if remaining_bytes < multipart_threshold as u64 {
        let mut buffer = Vec::<u8>::with_capacity(multipart_threshold);
        buffer.resize_with(remaining_bytes as usize, Default::default);
        file.read_exact(buffer.as_mut_slice()).await?;
        checksum.update(buffer.as_slice());

        return Ok(checksum.finalize());
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
        let _ = checksum.finalize();

        remaining_bytes -= real_chunksize as u64;
    }

    Ok(checksum.finalize_all())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::storage::additional_checksum_verify::{
        generate_checksum_from_path, generate_checksum_from_path_for_check,
        generate_checksum_from_path_with_chunksize, is_multipart_upload_checksum,
        UNKNOWN_CHECKSUM_VALUE,
    };
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
        )
        .await
        .unwrap();

        assert_eq!(checksum, TEST_SHA256_BASE64_DIGEST.to_string());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_multipart_test() {
        init_dummy_tracing_subscriber();

        create_large_file().await;

        let checksum = generate_checksum_from_path(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            vec![8 * 1024 * 1024, 1048576],
            8 * 1024 * 1024,
            false,
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
        )
        .await
        .unwrap();
        assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_multipart_full_object_test() {
        init_dummy_tracing_subscriber();

        create_large_file().await;

        let checksum = generate_checksum_from_path(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Crc32C,
            vec![8 * 1024 * 1024, 1048576],
            8 * 1024 * 1024,
            true,
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
        )
        .await
        .unwrap();
        assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_threshold_test() {
        init_dummy_tracing_subscriber();

        create_large_file().await;

        let checksum = generate_checksum_from_path(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            vec![9 * 1024 * 1024],
            9 * 1024 * 1024,
            false,
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
        )
        .await
        .unwrap();

        assert_eq!(checksum, TEST_SHA256_BASE64_DIGEST.to_string());
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
        )
        .await
        .unwrap();

        assert_eq!(checksum, TEST_CRC32_BASE64_DIGEST.to_string());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_chunksize_multipart_full_object_test() {
        init_dummy_tracing_subscriber();

        create_large_file().await;

        let checksum = generate_checksum_from_path_with_chunksize(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Crc32,
            8 * 1024 * 1024,
            8 * 1024 * 1024,
            true,
        )
        .await
        .unwrap();

        assert_eq!(
            checksum,
            LARGE_FILE_CRC32_FULL_OBJECT_BASE64_DIGEST.to_string()
        );
    }

    #[tokio::test]
    async fn generate_checksum_from_path_multipart_chunksize_test() {
        init_dummy_tracing_subscriber();

        create_large_file().await;

        let checksum = generate_checksum_from_path_with_chunksize(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            8 * 1024 * 1024,
            8 * 1024 * 1024,
            false,
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
        )
        .await
        .unwrap();

        assert_eq!(checksum, TEST_SHA256_BASE64_DIGEST.to_string());
    }
    #[tokio::test]
    async fn generate_checksum_from_path_for_check_multipart_test() {
        init_dummy_tracing_subscriber();

        create_large_file().await;

        let checksum = generate_checksum_from_path_for_check(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            true,
            vec![8 * 1024 * 1024, 1048576],
            false,
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
        )
        .await
        .unwrap();
        assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_for_check_full_object_test() {
        init_dummy_tracing_subscriber();

        create_large_file().await;

        let checksum = generate_checksum_from_path_for_check(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Crc32,
            true,
            vec![8 * 1024 * 1024, 1048576],
            true,
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
        )
        .await
        .unwrap();
        assert_eq!(
            checksum,
            LARGE_FILE_CRC64NVME_FULL_OBJECT_BASE64_DIGEST.to_string()
        );
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
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .or_else(|_| EnvFilter::try_new("dummy=trace"))
                    .unwrap(),
            )
            .try_init();
    }
}
