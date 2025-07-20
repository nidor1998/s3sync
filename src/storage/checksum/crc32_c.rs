use base64::{Engine as _, engine::general_purpose};

use crate::storage::checksum::Checksum;

#[derive(Default)]
pub struct ChecksumCRC32c {
    crc32c_value: Option<u32>,
    full_object_checksum: bool,
    total_hash: Vec<u8>,
}

const HASH_SIZE_BYTE: usize = 4;

const EMPTY_CRC32C: &str = "AAAAAA==";

impl Checksum for ChecksumCRC32c {
    fn new(full_object_checksum: bool) -> Self {
        ChecksumCRC32c {
            crc32c_value: None,
            full_object_checksum,
            total_hash: vec![],
        }
    }
    fn update(&mut self, data: &[u8]) {
        self.crc32c_value = if self.crc32c_value.is_none() {
            Some(crc32c::crc32c(data))
        } else {
            Some(crc32c::crc32c_append(self.crc32c_value.unwrap(), data))
        };
    }

    fn finalize(&mut self) -> String {
        if self.crc32c_value.is_none() {
            return EMPTY_CRC32C.to_string();
        }

        let digest = self.crc32c_value.unwrap().to_be_bytes();
        if !self.full_object_checksum {
            self.total_hash.append(&mut digest.to_vec());
            self.crc32c_value = None;
        }

        general_purpose::STANDARD.encode(digest)
    }

    fn finalize_all(&mut self) -> String {
        if self.full_object_checksum {
            if self.crc32c_value.is_none() {
                return EMPTY_CRC32C.to_string();
            }

            let digest = self.crc32c_value.unwrap().to_be_bytes();
            return general_purpose::STANDARD.encode(digest);
        }

        let digest = crc32c::crc32c(self.total_hash.to_vec().as_slice());
        self.crc32c_value = None;
        format!(
            "{}-{}",
            general_purpose::STANDARD.encode(digest.to_be_bytes().as_slice()),
            self.total_hash.len() / HASH_SIZE_BYTE
        )
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tokio::fs::File;
    use tokio::io::AsyncReadExt;

    use super::*;

    const LARGE_FILE_PATH: &str = "./playground/large_data/50MiB";
    const LARGE_FILE_DIR: &str = "./playground/large_data/";
    const LARGE_FILE_SIZE: usize = 50 * 1024 * 1024;

    const CHECKSUM_PART_1_TO_3: &str = "gosw+g==";
    const CHECKSUM_LAST_PART: &str = "lWi6iA==";
    const CHECKSUM_TOTAL: &str = "RxZ+kg==-4";
    const FULL_OBJECT_CHECKSUM: &str = "L/rH8A==";
    #[tokio::test]
    async fn checksum_crc32_c_test() {
        init_dummy_tracing_subscriber();

        create_large_file().await;
        let mut file = File::open(LARGE_FILE_PATH).await.unwrap();

        let mut checksum = ChecksumCRC32c::default();

        let mut buffer = Vec::<u8>::with_capacity(17179870);
        buffer.resize_with(17179870, Default::default);
        file.read_exact(buffer.as_mut_slice()).await.unwrap();
        checksum.update(buffer.as_slice());
        assert_eq!(checksum.finalize(), CHECKSUM_PART_1_TO_3.to_string());

        let mut buffer = Vec::<u8>::with_capacity(17179860);
        buffer.resize_with(17179860, Default::default);
        file.read_exact(buffer.as_mut_slice()).await.unwrap();
        checksum.update(buffer.as_slice());

        let mut buffer = Vec::<u8>::with_capacity(10);
        buffer.resize_with(10, Default::default);
        file.read_exact(buffer.as_mut_slice()).await.unwrap();
        checksum.update(buffer.as_slice());

        assert_eq!(checksum.finalize(), CHECKSUM_PART_1_TO_3.to_string());

        let mut buffer = Vec::<u8>::with_capacity(17179870);
        buffer.resize_with(17179870, Default::default);
        file.read_exact(buffer.as_mut_slice()).await.unwrap();
        checksum.update(buffer.as_slice());
        assert_eq!(checksum.finalize(), CHECKSUM_PART_1_TO_3.to_string());

        let mut buffer = Vec::<u8>::with_capacity(889190);
        buffer.resize_with(889190, Default::default);
        file.read_exact(buffer.as_mut_slice()).await.unwrap();
        checksum.update(buffer.as_slice());
        assert_eq!(checksum.finalize(), CHECKSUM_LAST_PART.to_string());

        assert_eq!(checksum.finalize_all(), CHECKSUM_TOTAL.to_string());
    }

    #[tokio::test]
    async fn checksum_crc32_c_full_object_checksum_test() {
        init_dummy_tracing_subscriber();

        create_large_file().await;
        let mut file = File::open(LARGE_FILE_PATH).await.unwrap();

        let mut checksum = ChecksumCRC32c::new(true);

        let mut buffer = Vec::<u8>::with_capacity(17179870);
        buffer.resize_with(17179870, Default::default);
        file.read_exact(buffer.as_mut_slice()).await.unwrap();
        checksum.update(buffer.as_slice());
        checksum.finalize();

        let mut buffer = Vec::<u8>::with_capacity(17179860);
        buffer.resize_with(17179860, Default::default);
        file.read_exact(buffer.as_mut_slice()).await.unwrap();
        checksum.update(buffer.as_slice());

        let mut buffer = Vec::<u8>::with_capacity(10);
        buffer.resize_with(10, Default::default);
        file.read_exact(buffer.as_mut_slice()).await.unwrap();
        checksum.update(buffer.as_slice());
        checksum.finalize();

        let mut buffer = Vec::<u8>::with_capacity(17179870);
        buffer.resize_with(17179870, Default::default);
        file.read_exact(buffer.as_mut_slice()).await.unwrap();
        checksum.update(buffer.as_slice());
        checksum.finalize();

        let mut buffer = Vec::<u8>::with_capacity(889190);
        buffer.resize_with(889190, Default::default);
        file.read_exact(buffer.as_mut_slice()).await.unwrap();
        checksum.update(buffer.as_slice());
        checksum.finalize();

        assert_eq!(checksum.finalize(), FULL_OBJECT_CHECKSUM.to_string());
        assert_eq!(checksum.finalize_all(), FULL_OBJECT_CHECKSUM.to_string());
    }

    #[tokio::test]
    async fn checksum_crc32_c_full_object_checksum_empty_test() {
        init_dummy_tracing_subscriber();

        let mut checksum = ChecksumCRC32c::default();
        assert_eq!(checksum.finalize(), EMPTY_CRC32C.to_string());

        let mut checksum = ChecksumCRC32c::new(true);
        assert_eq!(checksum.finalize(), EMPTY_CRC32C.to_string());
        assert_eq!(checksum.finalize_all(), EMPTY_CRC32C.to_string());
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
