use aws_sdk_s3::types::ChecksumAlgorithm;

const INVALID_CHECKSUM_ALGORITHM: &str =
    "invalid checksum_algorithm. valid choices: CRC32 | CRC32C | CRC64NVME | SHA1 | SHA256 .";

pub fn parse_checksum_algorithm(checksum_algorithm: &str) -> Result<String, String> {
    if !matches!(
        ChecksumAlgorithm::from(checksum_algorithm),
        ChecksumAlgorithm::Crc32
            | ChecksumAlgorithm::Crc32C
            | ChecksumAlgorithm::Crc64Nvme
            | ChecksumAlgorithm::Sha1
            | ChecksumAlgorithm::Sha256
    ) {
        return Err(INVALID_CHECKSUM_ALGORITHM.to_string());
    }

    Ok(checksum_algorithm.to_string())
}
