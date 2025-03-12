use std::str::FromStr;

use aws_sdk_s3::types::ServerSideEncryption;

const INVALID_SSE_VALUE: &str =
    "invalid sse value. valid choices: AES256 | aws:kms | aws:kms:dsse.";

pub fn parse_sse(sse: &str) -> Result<String, String> {
    #[allow(deprecated)]
    if matches!(
        ServerSideEncryption::from_str(sse).unwrap(),
        ServerSideEncryption::Unknown(_)
    ) {
        return Err(INVALID_SSE_VALUE.to_string());
    }

    Ok(sse.to_string())
}

pub fn parse_sse_c(sse: &str) -> Result<String, String> {
    if !matches!(
        ServerSideEncryption::from_str(sse).unwrap(),
        ServerSideEncryption::Aes256
    ) {
        return Err(INVALID_SSE_VALUE.to_string());
    }

    Ok(sse.to_string())
}
