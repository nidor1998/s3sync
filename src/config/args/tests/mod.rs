mod build_config;
mod options;
mod parse_credentials;
mod parse_storage;

#[test]
fn is_express_onezone_storage_test() {
    use super::*;

    assert!(is_express_onezone_storage("bucket--x-s3"));

    assert!(!is_express_onezone_storage("bucket-x-s3"));
    assert!(!is_express_onezone_storage("bucket--x-s3s"));
    assert!(!is_express_onezone_storage("bucket"));
}
