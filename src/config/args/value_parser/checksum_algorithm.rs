use aws_sdk_s3::types::ChecksumAlgorithm;

const INVALID_CHECKSUM_ALGORITHM: &str =
    "invalid checksum_algorithm. valid choices: CRC32 | CRC32C | SHA1 | SHA256 .";

pub fn parse_checksum_algorithm(checksum_algorithm: &str) -> Result<String, String> {
    #[allow(deprecated)]
    if matches!(
        ChecksumAlgorithm::from(checksum_algorithm),
        ChecksumAlgorithm::Unknown(_)
    ) {
        return Err(INVALID_CHECKSUM_ALGORITHM.to_string());
    }

    Ok(checksum_algorithm.to_string())
}
