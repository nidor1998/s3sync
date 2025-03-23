use base64::{engine::general_purpose, Engine as _};
use sha1;
use sha1::Digest;

use crate::storage::checksum::Checksum;

pub struct ChecksumSha1 {
    hasher: sha1::Sha1,
    total_hash: Vec<u8>,
}

const HASH_SIZE_BYTE: usize = 20;

impl Default for ChecksumSha1 {
    fn default() -> Self {
        ChecksumSha1 {
            hasher: sha1::Sha1::new(),
            total_hash: vec![],
        }
    }
}

impl Checksum for ChecksumSha1 {
    fn new(_full_object_checksum: bool) -> Self {
        Self::default()
    }

    fn update(&mut self, data: &[u8]) {
        self.hasher.update(data);
    }

    fn finalize(&mut self) -> String {
        let digest = self.hasher.clone().finalize();
        self.total_hash.append(&mut digest.to_vec());
        self.hasher.reset();
        general_purpose::STANDARD.encode(digest.as_slice())
    }

    fn finalize_all(&mut self) -> String {
        self.hasher.reset();
        self.hasher.update(self.total_hash.as_slice());

        let digest = self.hasher.clone().finalize();
        self.hasher.reset();
        format!(
            "{}-{}",
            general_purpose::STANDARD.encode(digest.as_slice()),
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

    const CHECKSUM_PART_1_TO_3: &str = "pRb/KMwUt71BWAik/2cOlXvClBU=";
    const CHECKSUM_LAST_PART: &str = "toPQOuojMD/wMjVeg86uSD4+I2w=";
    const CHECKSUM_TOTAL: &str = "UhzPmGarLG+JkxjMGKwEtN8Jc2s=-4";

    #[tokio::test]
    async fn checksum_sha1_test() {
        init_dummy_tracing_subscriber();

        create_large_file().await;
        let mut file = File::open(LARGE_FILE_PATH).await.unwrap();

        let mut checksum = ChecksumSha1::default();

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
    async fn checksum_sha1_test_with_new() {
        init_dummy_tracing_subscriber();

        create_large_file().await;
        let mut file = File::open(LARGE_FILE_PATH).await.unwrap();

        let mut checksum = ChecksumSha1::new(true);

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
