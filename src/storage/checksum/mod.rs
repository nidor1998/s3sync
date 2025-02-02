use aws_sdk_s3::types::ChecksumAlgorithm;

use crate::storage::checksum::crc32::ChecksumCRC32;
use crate::storage::checksum::crc32_c::ChecksumCRC32c;
use crate::storage::checksum::crc64_nvme::ChecksumCRC64NVMe;
use crate::storage::checksum::sha1::ChecksumSha1;
use crate::storage::checksum::sha256::ChecksumSha256;

pub mod crc32;
pub mod crc32_c;
pub mod crc64_nvme;
pub mod sha1;
pub mod sha256;

pub trait Checksum {
    fn update(&mut self, data: &[u8]);
    fn finalize(&mut self) -> String;
    fn finalize_all(&mut self) -> String;
}

pub struct AdditionalChecksum {
    checksum: Box<dyn Checksum + Send + Sync + 'static>,
}

impl AdditionalChecksum {
    pub fn new(algorithm: ChecksumAlgorithm) -> Self {
        match algorithm {
            ChecksumAlgorithm::Sha1 => AdditionalChecksum {
                checksum: Box::<ChecksumSha1>::default(),
            },
            ChecksumAlgorithm::Sha256 => AdditionalChecksum {
                checksum: Box::<ChecksumSha256>::default(),
            },
            ChecksumAlgorithm::Crc32 => AdditionalChecksum {
                checksum: Box::<ChecksumCRC32>::default(),
            },
            ChecksumAlgorithm::Crc32C => AdditionalChecksum {
                checksum: Box::<ChecksumCRC32c>::default(),
            },
            ChecksumAlgorithm::Crc64Nvme => AdditionalChecksum {
                checksum: Box::<ChecksumCRC64NVMe>::default(),
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
