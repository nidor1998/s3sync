use aws_sdk_s3::types::StorageClass;

const INVALID_STORAGE_CLASS: &str = "invalid storage class. valid choices: STANDARD | REDUCED_REDUNDANCY | STANDARD_IA | ONE-ZONE_IA | INTELLIGENT_TIERING | GLACIER | DEEP_ARCHIVE | GLACIER_IR .";

pub fn parse_storage_class(class: &str) -> Result<String, String> {
    if matches!(StorageClass::from(class), StorageClass::Unknown(_)) {
        return Err(INVALID_STORAGE_CLASS.to_string());
    }

    Ok(class.to_string())
}
