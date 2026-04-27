use base64::{Engine as _, engine::general_purpose};

use crate::storage::checksum::Checksum;

pub struct ChecksumCRC64NVMe {
    digest: crc_fast::Digest,
}

impl Default for ChecksumCRC64NVMe {
    fn default() -> Self {
        ChecksumCRC64NVMe {
            digest: crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc64Nvme),
        }
    }
}

impl Checksum for ChecksumCRC64NVMe {
    fn new(_full_object_checksum: bool) -> Self {
        ChecksumCRC64NVMe {
            digest: crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc64Nvme),
        }
    }
    fn update(&mut self, data: &[u8]) {
        self.digest.update(data);
    }

    fn finalize(&mut self) -> String {
        general_purpose::STANDARD.encode(self.digest.finalize().to_be_bytes().as_slice())
    }

    fn finalize_all(&mut self) -> String {
        // AWS S3 CRC64NVME does not support composite checksum type. So this method always returns the same value as finalize().
        general_purpose::STANDARD.encode(self.digest.finalize().to_be_bytes().as_slice())
    }
}

#[cfg(test)]
mod tests {
    use tokio::fs::File;
    use tokio::io::AsyncReadExt;

    use super::*;
    use crate::storage::test_helpers::create_large_file;

    const LARGE_FILE_PATH: &str = "./playground/large_data/50MiB";
    const LARGE_FILE_DIR: &str = "./playground/large_data/";
    const LARGE_FILE_SIZE: usize = 50 * 1024 * 1024;

    const CHECKSUM_TOTAL: &str = "ZfX5vT9m/o8=";

    #[tokio::test]
    async fn checksum_crc64_nvme_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;
        let mut file = File::open(LARGE_FILE_PATH).await.unwrap();

        let mut checksum = ChecksumCRC64NVMe::default();

        let mut buffer = Vec::<u8>::with_capacity(LARGE_FILE_SIZE);
        buffer.resize_with(LARGE_FILE_SIZE, Default::default);
        file.read_exact(buffer.as_mut_slice()).await.unwrap();
        checksum.update(buffer.as_slice());

        assert_eq!(checksum.finalize(), CHECKSUM_TOTAL.to_string());
        assert_eq!(checksum.finalize_all(), CHECKSUM_TOTAL.to_string());
    }

    #[tokio::test]
    async fn checksum_crc64_nvme_test_with_new() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;
        let mut file = File::open(LARGE_FILE_PATH).await.unwrap();

        let mut checksum = ChecksumCRC64NVMe::new(true);

        let mut buffer = Vec::<u8>::with_capacity(LARGE_FILE_SIZE);
        buffer.resize_with(LARGE_FILE_SIZE, Default::default);
        file.read_exact(buffer.as_mut_slice()).await.unwrap();
        checksum.update(buffer.as_slice());

        assert_eq!(checksum.finalize(), CHECKSUM_TOTAL.to_string());
        assert_eq!(checksum.finalize_all(), CHECKSUM_TOTAL.to_string());
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
