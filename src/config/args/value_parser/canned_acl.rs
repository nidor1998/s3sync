use std::str::FromStr;

use aws_sdk_s3::types::ObjectCannedAcl;

const INVALID_ACL_VALUE: &str = "invalid acl value. valid choices: private | public-read | public-read-write | authenticated-read | aws-exec-read | bucket-owner-read | bucket-owner-full-control .";

pub fn parse_canned_acl(canned_acl: &str) -> Result<String, String> {
    #[allow(deprecated)]
    if matches!(
        ObjectCannedAcl::from_str(canned_acl).unwrap(),
        ObjectCannedAcl::Unknown(_)
    ) {
        return Err(INVALID_ACL_VALUE.to_string());
    }

    Ok(canned_acl.to_string())
}
