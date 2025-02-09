use base64::{engine::general_purpose, Engine as _};

use crate::storage::checksum::Checksum;

pub struct ChecksumCRC32 {
    hasher: crc32fast::Hasher,
    full_object_checksum: bool,
    total_hash: Vec<u8>,
}

const HASH_SIZE_BYTE: usize = 4;

impl Default for ChecksumCRC32 {
    fn default() -> Self {
        ChecksumCRC32 {
            hasher: crc32fast::Hasher::new(),
            full_object_checksum: false,
            total_hash: vec![],
        }
    }
}

impl Checksum for ChecksumCRC32 {
    fn new(full_object_checksum: bool) -> Self {
        ChecksumCRC32 {
            hasher: crc32fast::Hasher::new(),
            full_object_checksum,
            total_hash: vec![],
        }
    }
    fn update(&mut self, data: &[u8]) {
        self.hasher.update(data.as_ref());
    }

    fn finalize(&mut self) -> String {
        let digest = self.hasher.clone().finalize();
        if !self.full_object_checksum {
            self.total_hash.append(&mut digest.to_be_bytes().to_vec());
            self.hasher.reset();
        }
        general_purpose::STANDARD.encode(digest.to_be_bytes().as_slice())
    }

    fn finalize_all(&mut self) -> String {
        if self.full_object_checksum {
            let digest = self.hasher.clone().finalize();
            return general_purpose::STANDARD.encode(digest.to_be_bytes().as_slice());
        }

        self.hasher.reset();
        self.hasher.update(self.total_hash.to_vec().as_slice());
        let digest = self.hasher.clone().finalize();
        self.hasher.reset();
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

    const CHECKSUM_PART_1_TO_3: &str = "MbIqtw==";
    const CHECKSUM_LAST_PART: &str = "A+cwKQ==";
    const CHECKSUM_TOTAL: &str = "ef6o8Q==-4";
    const FULL_OBJECT_CHECKSUM: &str = "DpubNA==";
    #[tokio::test]
    async fn checksum_crc32_test() {
        init_dummy_tracing_subscriber();

        create_large_file().await;
        let mut file = File::open(LARGE_FILE_PATH).await.unwrap();

        let mut checksum = ChecksumCRC32::default();

        let mut buffer = Vec::<u8>::with_capacity(17179870);
        buffer.resize_with(17179870, Default::default);
        file.read_exact(buffer.as_mut_slice()).await.unwrap();
        checksum.update(buffer.as_slice());
        assert_eq!(checksum.finalize(), CHECKSUM_PART_1_TO_3.to_string());

        let mut buffer = Vec::<u8>::with_capacity(17179870);
        buffer.resize_with(17179870, Default::default);
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
    async fn checksum_crc32_full_object_checksum_test() {
        init_dummy_tracing_subscriber();

        create_large_file().await;
        let mut file = File::open(LARGE_FILE_PATH).await.unwrap();

        let mut checksum = ChecksumCRC32::new(true);

        let mut buffer = Vec::<u8>::with_capacity(17179870);
        buffer.resize_with(17179870, Default::default);
        file.read_exact(buffer.as_mut_slice()).await.unwrap();
        checksum.update(buffer.as_slice());
        checksum.finalize();

        let mut buffer = Vec::<u8>::with_capacity(17179870);
        buffer.resize_with(17179870, Default::default);
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

        assert_eq!(checksum.finalize_all(), FULL_OBJECT_CHECKSUM.to_string());
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
