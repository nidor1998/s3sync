use aws_sdk_s3::types::ChecksumAlgorithm;

use crate::storage::checksum::crc32::ChecksumCRC32;
use crate::storage::checksum::crc32_c::ChecksumCRC32c;
use crate::storage::checksum::crc64_nvme::ChecksumCRC64NVMe;
use crate::storage::checksum::md5::ChecksumMd5;
use crate::storage::checksum::sha1::ChecksumSha1;
use crate::storage::checksum::sha256::ChecksumSha256;
use crate::storage::checksum::sha512::ChecksumSha512;
use crate::storage::checksum::xxhash3::ChecksumXXHash3;
use crate::storage::checksum::xxhash64::ChecksumXXHash64;
use crate::storage::checksum::xxhash128::ChecksumXXHash128;

pub mod crc32;
pub mod crc32_c;
pub mod crc64_nvme;
pub mod md5;
pub mod sha1;
pub mod sha256;
pub mod sha512;
pub mod xxhash128;
pub mod xxhash3;
pub mod xxhash64;

pub trait Checksum {
    fn new(full_object_checksum: bool) -> Self
    where
        Self: Sized;
    fn update(&mut self, data: &[u8]);
    fn finalize(&mut self) -> String;
    fn finalize_all(&mut self) -> String;
}

pub struct AdditionalChecksum {
    checksum: Box<dyn Checksum + Send + Sync + 'static>,
}

impl AdditionalChecksum {
    pub fn new(algorithm: ChecksumAlgorithm, full_object_checksum: bool) -> Self {
        match algorithm {
            ChecksumAlgorithm::Sha1 => AdditionalChecksum {
                checksum: Box::<ChecksumSha1>::default(),
            },
            ChecksumAlgorithm::Sha256 => AdditionalChecksum {
                checksum: Box::<ChecksumSha256>::default(),
            },
            ChecksumAlgorithm::Crc32 => AdditionalChecksum {
                checksum: Box::new(ChecksumCRC32::new(full_object_checksum)),
            },
            ChecksumAlgorithm::Crc32C => AdditionalChecksum {
                checksum: Box::new(ChecksumCRC32c::new(full_object_checksum)),
            },
            ChecksumAlgorithm::Crc64Nvme => AdditionalChecksum {
                checksum: Box::<ChecksumCRC64NVMe>::default(),
            },
            ChecksumAlgorithm::Sha512 => AdditionalChecksum {
                checksum: Box::<ChecksumSha512>::default(),
            },
            ChecksumAlgorithm::Xxhash3 => AdditionalChecksum {
                checksum: Box::<ChecksumXXHash3>::default(),
            },
            ChecksumAlgorithm::Xxhash64 => AdditionalChecksum {
                checksum: Box::<ChecksumXXHash64>::default(),
            },
            ChecksumAlgorithm::Xxhash128 => AdditionalChecksum {
                checksum: Box::<ChecksumXXHash128>::default(),
            },
            ChecksumAlgorithm::Md5 => AdditionalChecksum {
                checksum: Box::<ChecksumMd5>::default(),
            },
            _ => {
                panic!("Unknown ChecksumAlgorithm")
            }
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        self.checksum.update(data)
    }
    pub fn finalize(&mut self) -> String {
        self.checksum.finalize()
    }
    pub fn finalize_all(&mut self) -> String {
        self.checksum.finalize_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha1_test() {
        AdditionalChecksum::new(ChecksumAlgorithm::Sha1, false);
    }

    #[test]
    fn sha256_test() {
        AdditionalChecksum::new(ChecksumAlgorithm::Sha256, false);
    }

    #[test]
    fn crc32_test() {
        AdditionalChecksum::new(ChecksumAlgorithm::Crc32, false);
        AdditionalChecksum::new(ChecksumAlgorithm::Crc32, true);
    }

    #[test]
    fn crc32c_test() {
        AdditionalChecksum::new(ChecksumAlgorithm::Crc32C, false);
        AdditionalChecksum::new(ChecksumAlgorithm::Crc32C, true);
    }

    #[test]
    fn crc64nvme_test() {
        AdditionalChecksum::new(ChecksumAlgorithm::Crc64Nvme, false);
        AdditionalChecksum::new(ChecksumAlgorithm::Crc64Nvme, true);
    }

    #[test]
    fn sha512_test() {
        AdditionalChecksum::new(ChecksumAlgorithm::Sha512, false);
    }

    #[test]
    fn xxhash3_test() {
        AdditionalChecksum::new(ChecksumAlgorithm::Xxhash3, false);
    }

    #[test]
    fn xxhash64_test() {
        AdditionalChecksum::new(ChecksumAlgorithm::Xxhash64, false);
    }

    #[test]
    fn xxhash128_test() {
        AdditionalChecksum::new(ChecksumAlgorithm::Xxhash128, false);
    }

    #[test]
    fn md5_test() {
        AdditionalChecksum::new(ChecksumAlgorithm::Md5, false);
    }
}
