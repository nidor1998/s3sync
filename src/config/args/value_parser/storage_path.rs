use percent_encoding::percent_decode_str;
use regex::Regex;
use url::{ParseError, Url};

use crate::types::StoragePath;

const INVALID_SCHEME: &str = "scheme must be s3:// .";
const INVALID_PATH: &str = "path must be a valid URL or a local path.";
const NO_BUCKET_NAME_SPECIFIED: &str = "bucket name must be specified.";
const NO_PATH_SPECIFIED: &str = "path must be specified.";
const MULTI_REGION_ARN_REGEX: &str = r"^s3://arn:aws:s3::.+:accesspoint/.+";

pub fn check_storage_path(path: &str) -> Result<String, String> {
    if is_multi_region_arn(path) {
        return Ok(path.to_string());
    }

    let result = Url::parse(path);
    if result == Err(ParseError::RelativeUrlWithoutBase) {
        if path.is_empty() {
            return Err(NO_PATH_SPECIFIED.to_string());
        }

        if !path.ends_with(std::path::MAIN_SEPARATOR) {
            return Ok(format!("{}{}", path, std::path::MAIN_SEPARATOR));
        }

        return Ok(path.to_string());
    }

    if result.is_err() {
        return Err(INVALID_PATH.to_string());
    }

    let parsed = result.unwrap();
    match parsed.scheme() {
        "s3" => {
            if parsed.host_str().is_none() {
                return Err(NO_BUCKET_NAME_SPECIFIED.to_string());
            }
        }
        _ => {
            if !is_windows_absolute_path(path) {
                return Err(INVALID_SCHEME.to_string());
            }
        }
    }

    Ok(path.to_string())
}

fn is_multi_region_arn(path: &str) -> bool {
    Regex::new(MULTI_REGION_ARN_REGEX).unwrap().is_match(path)
}

pub fn parse_storage_path(path: &str) -> StoragePath {
    check_storage_path(path).unwrap();

    if is_multi_region_arn(path) {
        return StoragePath::S3 {
            bucket: extract_multi_region_arn(path),
            prefix: extract_prefix(path),
        };
    }

    let result = Url::parse(path);
    if result == Err(ParseError::RelativeUrlWithoutBase) {
        return parse_local_path(path);
    }
    if is_windows_absolute_path(path) {
        return parse_local_path(path);
    }

    parse_s3_path(path)
}

fn extract_multi_region_arn(path: &str) -> String {
    let mut iter = path.match_indices('/');
    let third_slash = iter.nth(3);
    match third_slash {
        Some((idx, _)) => {
            let arn = &path[..=idx].to_string();
            arn.replace("s3://", "")
                .to_string()
                .strip_suffix('/')
                .map_or(arn.to_string(), |s| s.to_string())
                .to_string()
        }
        None => path.replace("s3://", "").to_string(),
    }
}

// skipcq: RS-W1201
fn extract_prefix(path: &str) -> String {
    path.char_indices()
        .filter(|&(_, c)| c == '/')
        .nth(3)
        .map(|(i, _)| &path[i + 1..])
        .map_or("", |s| s)
        .to_string()
}

pub fn is_both_storage_local(source: &StoragePath, target: &StoragePath) -> bool {
    let mut source_local = false;
    if matches!(source, StoragePath::Local(_)) {
        source_local = true;
    }

    let mut target_local = false;
    if matches!(target, StoragePath::Local(_)) {
        target_local = true;
    }

    source_local && target_local
}

pub fn is_both_storage_s3(source: &StoragePath, target: &StoragePath) -> bool {
    let mut source_s3 = false;
    if matches!(source, StoragePath::S3 { .. }) {
        source_s3 = true;
    }

    let mut target_s3 = false;
    if matches!(target, StoragePath::S3 { .. }) {
        target_s3 = true;
    }

    source_s3 && target_s3
}

fn parse_local_path(path: &str) -> StoragePath {
    StoragePath::Local(path.into())
}

fn parse_s3_path(path: &str) -> StoragePath {
    let bucket = Url::parse(path).unwrap().host_str().unwrap().to_string();
    let mut prefix = Url::parse(path).unwrap().path().to_string();

    // remove first '/'
    if !prefix.is_empty() {
        prefix.remove(0);
    }

    prefix = percent_decode_str(&prefix)
        .decode_utf8()
        .unwrap()
        .to_string();

    StoragePath::S3 { bucket, prefix }
}

fn is_windows_absolute_path(path: &str) -> bool {
    if !cfg!(windows) {
        return false;
    }

    let re = Regex::new(r"^[a-zA-Z]:\\").unwrap();
    re.is_match(path)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn check_valid_url() {
        init_dummy_tracing_subscriber();

        check_storage_path("/etc/").unwrap();
        check_storage_path("etc/dir").unwrap();
        check_storage_path("etc/dir/").unwrap();
        check_storage_path("../dir/").unwrap();
        check_storage_path("../dir").unwrap();
        check_storage_path("./dir/").unwrap();
        check_storage_path("./dir").unwrap();

        check_storage_path("s3://my-bucket").unwrap();
        check_storage_path("s3://my-bucket/").unwrap();
        check_storage_path("s3://my-bucket//").unwrap();
        check_storage_path("s3://my-bucket/xyz.dat").unwrap();
        check_storage_path("s3://my-bucket/xyz/").unwrap();
        check_storage_path("s3://my-bucket//xyz").unwrap();
        check_storage_path("s3://my-bucket//xyz/").unwrap();
        check_storage_path("s3://my-bucket/xyz//xxx").unwrap();
        check_storage_path("s3://my-bucket/../xxx").unwrap();
        check_storage_path("s3://my-bucket/x+y/y+z").unwrap();
        check_storage_path("s3://my-bucket/hello/こんばんは/☃").unwrap();

        check_storage_path("s3://arn:aws:s3::111111111111:accesspoint/xxxx.mrap").unwrap();
        check_storage_path("s3://arn:aws:s3::111111111111:accesspoint/xxxx.mrap/").unwrap();
        check_storage_path("s3://arn:aws:s3::111111111111:accesspoint/xxxx.mrap/prefix").unwrap();
        check_storage_path("s3://arn:aws:s3::111111111111:accesspoint/xxxx.mrap/prefix/").unwrap();
        check_storage_path("s3://arn:aws:s3::111111111111:accesspoint/xxxx.mrap//prefix/").unwrap();
    }

    #[test]
    fn check_valid_url_error() {
        init_dummy_tracing_subscriber();

        assert!(check_storage_path("s3://arn:aws").is_err());
    }

    #[test]
    fn parse_local_path() {
        init_dummy_tracing_subscriber();

        let local_path = "dir1/";

        if let StoragePath::Local(path) = parse_storage_path(local_path) {
            assert_eq!(path, PathBuf::from("dir1/"));
        } else {
            // skipcq: RS-W1021
            assert!(false, "local path not found");
        }
    }

    #[test]
    #[cfg(target_family = "windows")]
    fn parse_local_windows_absolute_path() {
        init_dummy_tracing_subscriber();

        let local_path = "c:\\dir1";

        if let StoragePath::Local(path) = parse_storage_path(local_path) {
            assert_eq!(path, PathBuf::from("c:\\dir1"));
        } else {
            // skipcq: RS-W1021
            assert!(false, "local path not found");
        }
    }

    #[test]
    #[cfg(target_family = "windows")]
    fn parse_local_windows_absolute_path_without_drive_letter() {
        init_dummy_tracing_subscriber();

        let local_path = "\\dir1";

        if let StoragePath::Local(path) = parse_storage_path(local_path) {
            assert_eq!(path, PathBuf::from("\\dir1"));
        } else {
            // skipcq: RS-W1021
            assert!(false, "local path not found");
        }
    }

    #[test]
    fn parse_local_unix_absolute_path() {
        init_dummy_tracing_subscriber();

        let local_path = "/dir1";

        if let StoragePath::Local(path) = parse_storage_path(local_path) {
            assert_eq!(path, PathBuf::from("/dir1"));
        } else {
            // skipcq: RS-W1021
            assert!(false, "local path not found");
        }
    }

    #[test]
    fn parse_s3_url_with_no_key() {
        init_dummy_tracing_subscriber();

        let s3_url = "s3://test-bucket";

        if let StoragePath::S3 { bucket, prefix } = parse_storage_path(s3_url) {
            assert_eq!(bucket, "test-bucket");
            assert_eq!(prefix, "");
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }
    }

    #[test]
    fn parse_s3_url_with_no_key_ends_with_slash() {
        init_dummy_tracing_subscriber();

        let s3_url = "s3://test-bucket/";

        if let StoragePath::S3 { bucket, prefix } = parse_storage_path(s3_url) {
            assert_eq!(bucket, "test-bucket");
            assert_eq!(prefix, "");
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }
    }

    #[test]
    fn parse_s3_url_with_no_key_ends_with_double_slash() {
        init_dummy_tracing_subscriber();

        let s3_url = "s3://test-bucket//";

        if let StoragePath::S3 { bucket, prefix } = parse_storage_path(s3_url) {
            assert_eq!(bucket, "test-bucket");
            assert_eq!(prefix, "/");
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }
    }

    #[test]
    fn parse_s3_url_with_key_without_slash() {
        init_dummy_tracing_subscriber();

        let s3_url = "s3://test-bucket/my_key";

        if let StoragePath::S3 { bucket, prefix } = parse_storage_path(s3_url) {
            assert_eq!(bucket, "test-bucket");
            assert_eq!(prefix, "my_key");
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }
    }

    #[test]
    fn parse_s3_url_with_key_ends_with_slash() {
        init_dummy_tracing_subscriber();

        let s3_url = "s3://test-bucket/my_key/";

        if let StoragePath::S3 { bucket, prefix } = parse_storage_path(s3_url) {
            assert_eq!(bucket, "test-bucket");
            assert_eq!(prefix, "my_key/");
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }
    }

    #[test]
    fn parse_s3_url_with_key_with_slash() {
        init_dummy_tracing_subscriber();

        let s3_url = "s3://test-bucket/dir1/dir2/my_key";

        if let StoragePath::S3 { bucket, prefix } = parse_storage_path(s3_url) {
            assert_eq!(bucket, "test-bucket");
            assert_eq!(prefix, "dir1/dir2/my_key");
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }
    }

    #[test]
    fn parse_s3_url_with_key_with_slash_ends_with_slash() {
        init_dummy_tracing_subscriber();

        let s3_url = "s3://test-bucket/dir1/dir2/my_key/";

        if let StoragePath::S3 { bucket, prefix } = parse_storage_path(s3_url) {
            assert_eq!(bucket, "test-bucket");
            assert_eq!(prefix, "dir1/dir2/my_key/");
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }
    }

    #[test]
    fn parse_s3_url_with_key_starts_with_double_slash() {
        init_dummy_tracing_subscriber();

        let s3_url = "s3://test-bucket//my_key";

        if let StoragePath::S3 { bucket, prefix } = parse_storage_path(s3_url) {
            assert_eq!(bucket, "test-bucket");
            assert_eq!(prefix, "/my_key");
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }
    }

    #[test]
    fn parse_s3_url_with_utf8_key() {
        init_dummy_tracing_subscriber();

        let s3_url = "s3://test-bucket/こんにちは/Καλησπέρα σας";

        if let StoragePath::S3 { bucket, prefix } = parse_storage_path(s3_url) {
            assert_eq!(bucket, "test-bucket");
            assert_eq!(prefix, "こんにちは/Καλησπέρα σας");
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }
    }

    #[test]
    fn parse_s3_url_arn() {
        init_dummy_tracing_subscriber();

        let s3_url = "s3://arn:aws:s3::1111111:accesspoint/xxxxxx.mrap";
        if let StoragePath::S3 { bucket, prefix } = parse_storage_path(s3_url) {
            assert_eq!(bucket, "arn:aws:s3::1111111:accesspoint/xxxxxx.mrap");
            assert_eq!(prefix, "");
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }

        let s3_url = "s3://arn:aws:s3::1111111:accesspoint/xxxxxx.mrap/";
        if let StoragePath::S3 { bucket, prefix } = parse_storage_path(s3_url) {
            assert_eq!(bucket, "arn:aws:s3::1111111:accesspoint/xxxxxx.mrap");
            assert_eq!(prefix, "");
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }

        let s3_url = "s3://arn:aws:s3::1111111:accesspoint/xxxxxx.mrap/prefix";
        if let StoragePath::S3 { bucket, prefix } = parse_storage_path(s3_url) {
            assert_eq!(bucket, "arn:aws:s3::1111111:accesspoint/xxxxxx.mrap");
            assert_eq!(prefix, "prefix");
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }

        let s3_url = "s3://arn:aws:s3::1111111:accesspoint/xxxxxx.mrap/prefix/";
        if let StoragePath::S3 { bucket, prefix } = parse_storage_path(s3_url) {
            assert_eq!(bucket, "arn:aws:s3::1111111:accesspoint/xxxxxx.mrap");
            assert_eq!(prefix, "prefix/");
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }

        let s3_url = "s3://arn:aws:s3::1111111:accesspoint/xxxxxx.mrap//prefix/";
        if let StoragePath::S3 { bucket, prefix } = parse_storage_path(s3_url) {
            assert_eq!(bucket, "arn:aws:s3::1111111:accesspoint/xxxxxx.mrap");
            assert_eq!(prefix, "/prefix/");
        } else {
            // skipcq: RS-W1021
            assert!(false, "s3 url not found");
        }
    }

    #[test]
    fn empty_local_path() {
        init_dummy_tracing_subscriber();

        let result = check_storage_path("");
        assert!(result.is_err());

        if let Err(e) = result {
            assert_eq!(e, NO_PATH_SPECIFIED);
        }
    }

    #[test]
    fn invalid_scheme() {
        init_dummy_tracing_subscriber();

        let result = check_storage_path("https://my-bucket");
        assert!(result.is_err());

        if let Err(e) = result {
            assert_eq!(e, INVALID_SCHEME);
        }
    }

    #[test]
    fn no_bucket_name() {
        init_dummy_tracing_subscriber();

        let result = check_storage_path("s3://");
        assert!(result.is_err());

        if let Err(e) = result {
            assert_eq!(e, NO_BUCKET_NAME_SPECIFIED);
        }
    }

    #[test]
    fn both_storage_local() {
        init_dummy_tracing_subscriber();

        assert!(is_both_storage_local(
            &StoragePath::Local("/source".into()),
            &StoragePath::Local("/target".into()),
        ));

        assert!(!is_both_storage_s3(
            &StoragePath::Local("/source".into()),
            &StoragePath::Local("/target".into()),
        ));
    }

    #[test]
    fn both_storage_s3() {
        init_dummy_tracing_subscriber();

        assert!(!is_both_storage_local(
            &StoragePath::S3 {
                bucket: "source-bucket".to_string(),
                prefix: "source-prefix".to_string(),
            },
            &StoragePath::S3 {
                bucket: "target-bucket".to_string(),
                prefix: "target-prefix".to_string(),
            },
        ));

        assert!(is_both_storage_s3(
            &StoragePath::S3 {
                bucket: "source-bucket".to_string(),
                prefix: "source-prefix".to_string(),
            },
            &StoragePath::S3 {
                bucket: "target-bucket".to_string(),
                prefix: "target-prefix".to_string(),
            },
        ));
    }

    #[test]
    fn one_side_storage_s3() {
        init_dummy_tracing_subscriber();

        assert!(!is_both_storage_local(
            &StoragePath::Local("/source".into()),
            &StoragePath::S3 {
                bucket: "target-bucket".to_string(),
                prefix: "target-prefix".to_string(),
            },
        ));

        assert!(!is_both_storage_s3(
            &StoragePath::Local("/source".into()),
            &StoragePath::S3 {
                bucket: "target-bucket".to_string(),
                prefix: "target-prefix".to_string(),
            },
        ));

        assert!(!is_both_storage_local(
            &StoragePath::S3 {
                bucket: "source-bucket".to_string(),
                prefix: "source-prefix".to_string(),
            },
            &StoragePath::Local("/target".into()),
        ));

        assert!(!is_both_storage_s3(
            &StoragePath::S3 {
                bucket: "source-bucket".to_string(),
                prefix: "source-prefix".to_string(),
            },
            &StoragePath::Local("/target".into()),
        ));
    }

    #[test]
    #[cfg(target_family = "windows")]
    fn is_windows_absolute_path_test() {
        init_dummy_tracing_subscriber();

        assert!(is_windows_absolute_path("c:\\"));
        assert!(is_windows_absolute_path("c:\\test"));
        assert!(is_windows_absolute_path("d:\\test"));
        assert!(is_windows_absolute_path("z:\\test"));
        assert!(is_windows_absolute_path("C:\\test"));
        assert!(is_windows_absolute_path("D:\\test"));
        assert!(is_windows_absolute_path("Z:\\test"));

        assert!(!is_windows_absolute_path("\\test"));
        assert!(!is_windows_absolute_path("s3://test-bucket/"));
        assert!(!is_windows_absolute_path("/xyz/"));
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
