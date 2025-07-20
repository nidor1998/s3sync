use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use aws_sdk_s3::primitives::DateTime;
use filetime::{FileTime, set_file_mtime};
use regex::Regex;
use tempfile::NamedTempFile;
use tokio::fs::File;
use tracing::trace;

pub fn check_directory_traversal(key: &str) -> bool {
    let re = Regex::new(r"\.\.[/\\]").unwrap();
    re.is_match(key)
}

pub async fn get_file_size(path: &PathBuf) -> u64 {
    File::open(path)
        .await
        .unwrap()
        .metadata()
        .await
        .unwrap()
        .len()
}

pub async fn is_regular_file(path: &PathBuf) -> Result<bool> {
    Ok(File::open(path).await?.metadata().await?.is_file())
}

pub async fn get_last_modified(path: &PathBuf) -> DateTime {
    DateTime::from(
        File::open(path)
            .await
            .unwrap()
            .metadata()
            .await
            .unwrap()
            .modified()
            .unwrap(),
    )
}

pub fn set_last_modified(
    path: PathBuf,
    key: &str,
    seconds: i64,
    nanos: u32,
) -> std::io::Result<()> {
    set_file_mtime(
        key_to_file_path(path, key),
        FileTime::from_unix_time(seconds, nanos),
    )
}

pub fn is_key_a_directory(key: &str) -> bool {
    if cfg!(windows) && key.ends_with('\\') {
        return true;
    }

    key.ends_with('/')
}

pub async fn create_temp_file_from_key(path: &Path, key: &str) -> Result<NamedTempFile> {
    create_directory_if_necessary(path, key).await?;

    let temp_directory_path = key_to_directory_without_filename(path.to_path_buf(), key);
    let file =
        NamedTempFile::new_in(temp_directory_path).context("NamedTempFile::new_in failed.")?;
    Ok(file)
}

pub async fn create_directory_hierarchy_from_key(path: PathBuf, key: &str) -> Result<bool> {
    let directory_path = key_to_directory_without_filename(path, key);

    let result = directory_path.try_exists();
    if result.is_ok() && result? {
        return Ok(false);
    }

    tokio::fs::create_dir_all(&directory_path)
        .await
        .context("tokio::fs::create_dir_all() failed.")?;

    let directory = directory_path.to_string_lossy().to_string();
    trace!(key = key, directory = directory, "directory created.");

    Ok(true)
}

pub fn remove_root_slash(key: &str) -> String {
    let re = Regex::new(r"^/+").unwrap();
    re.replace(key, "").to_string()
}

pub fn key_to_file_path(path: PathBuf, key: &str) -> PathBuf {
    let file = convert_os_specific_directory_char(&remove_root_slash(key));
    let lossy_path = path.to_string_lossy();

    format!("{lossy_path}{file}").into()
}

async fn create_directory_if_necessary(path: &Path, key: &str) -> Result<bool> {
    create_directory_hierarchy_from_key(path.to_path_buf(), key).await?;

    Ok(true)
}

fn key_to_directory_without_filename(path: PathBuf, key: &str) -> PathBuf {
    let lossy_path = path.to_string_lossy();
    let directory_from_key = remove_file_name_if_exist(
        convert_os_specific_directory_char(&remove_root_slash(key)).into(),
    )
    .to_string_lossy()
    .to_string();
    format!("{lossy_path}{directory_from_key}").into()
}

fn remove_file_name_if_exist(path: PathBuf) -> PathBuf {
    let mut path_str = path.to_str().unwrap().to_string();
    if path_str.ends_with(std::path::MAIN_SEPARATOR) {
        path_str.pop();
        return PathBuf::from(path_str);
    }

    path.parent().unwrap().to_path_buf()
}

fn convert_os_specific_directory_char(key: &str) -> String {
    key.replace('/', std::path::MAIN_SEPARATOR_STR)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::EnvFilter;

    const TEST_DATA_SIZE: u64 = 5;

    #[test]
    fn check_directory_traversal_test() {
        init_dummy_tracing_subscriber();

        assert!(check_directory_traversal("../etc/passwd"));
        assert!(check_directory_traversal("dir1/dir2/../../etc/passwd"));
        assert!(check_directory_traversal("/xyz/data/../../etc/passwd"));

        assert!(check_directory_traversal("..\\etc\\passwd"));
        assert!(check_directory_traversal("dir1\\dir2\\..\\..\\etc\\passwd"));
        assert!(check_directory_traversal(
            "\\xyz\\data\\..\\..\\etc\\passwd"
        ));
        assert!(check_directory_traversal(
            "c:\\xyz\\data\\..\\..\\etc\\passwd"
        ));

        assert!(!check_directory_traversal("/etc/passwd"));
        assert!(!check_directory_traversal("etc/passwd"));
        assert!(!check_directory_traversal("passwd"));
        assert!(!check_directory_traversal("/xyz/test.jpg"));
        assert!(!check_directory_traversal("/xyz/test..jpg"));

        assert!(!check_directory_traversal("\\etc\\passwd"));
        assert!(!check_directory_traversal("etc\\passwd"));
        assert!(!check_directory_traversal("\\xyz\\test.jpg"));
        assert!(!check_directory_traversal("\\xyz\\test..jpg"));
    }

    #[tokio::test]
    async fn get_file_size_test() {
        init_dummy_tracing_subscriber();

        assert_eq!(
            get_file_size(&PathBuf::from("test_data/5byte.dat")).await,
            TEST_DATA_SIZE
        );
    }

    #[tokio::test]
    async fn get_file_last_modified_test() {
        init_dummy_tracing_subscriber();

        get_last_modified(&PathBuf::from("test_data/5byte.dat")).await;
    }

    #[test]
    fn is_key_directory_test() {
        init_dummy_tracing_subscriber();

        assert!(is_key_a_directory("/dir/"));
        assert!(is_key_a_directory("dir/"));
        assert!(is_key_a_directory("/dir1/dir2/"));

        assert!(!is_key_a_directory("/dir"));
        assert!(!is_key_a_directory("dir"));
        assert!(!is_key_a_directory("/dir1/dir2"));
    }

    #[test]
    #[cfg(target_family = "windows")]
    fn is_key_directory_windows_test() {
        init_dummy_tracing_subscriber();

        assert!(is_key_a_directory("\\dir\\"));
        assert!(is_key_a_directory("dir\\"));
        assert!(is_key_a_directory("\\dir1\\dir\\"));

        assert!(!is_key_a_directory("\\dir"));
        assert!(!is_key_a_directory("dir"));
        assert!(!is_key_a_directory("\\dir1\\dir2"));
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn create_temp_file_from_key_test() {
        init_dummy_tracing_subscriber();

        create_temp_file_from_key(Path::new("playground/"), "tempdir/filename")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn create_directory_hierarchy_from_key_unix() {
        init_dummy_tracing_subscriber();

        create_directory_hierarchy_from_key(PathBuf::from("playground/"), "testdir1/filename")
            .await
            .unwrap();

        assert!(
            !create_directory_hierarchy_from_key(PathBuf::from("playground/"), "testdir1/",)
                .await
                .unwrap()
        );

        create_directory_hierarchy_from_key(
            PathBuf::from("playground/"),
            "testdir3/testdir4/filename",
        )
        .await
        .unwrap();

        assert!(
            !create_directory_hierarchy_from_key(
                PathBuf::from("playground/"),
                "testdir3/testdir4/",
            )
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    #[cfg(target_family = "windows")]
    async fn create_directory_hierarchy_from_key_windows() {
        init_dummy_tracing_subscriber();

        create_directory_hierarchy_from_key(PathBuf::from("playground\\"), "testdir1/filename")
            .await
            .unwrap();

        assert!(
            !create_directory_hierarchy_from_key(PathBuf::from("playground\\"), "testdir1/",)
                .await
                .unwrap()
        );

        create_directory_hierarchy_from_key(
            PathBuf::from("playground\\"),
            "testdir3/testdir4/filename",
        )
        .await
        .unwrap();

        assert!(
            !create_directory_hierarchy_from_key(
                PathBuf::from("playground\\"),
                "testdir3/testdir4/",
            )
            .await
            .unwrap()
        );
    }

    #[test]
    #[cfg(target_family = "unix")]
    fn key_to_local_directory_path_unix() {
        init_dummy_tracing_subscriber();

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("dir1"), "key/")
                .to_str()
                .unwrap(),
            "dir1key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("dir1/"), "key/")
                .to_str()
                .unwrap(),
            "dir1/key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("dir1/"), "/key/")
                .to_str()
                .unwrap(),
            "dir1/key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("dir1/"), "key/file1")
                .to_str()
                .unwrap(),
            "dir1/key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("dir1/"), "/key/file1")
                .to_str()
                .unwrap(),
            "dir1/key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("./dir1/"), "key/")
                .to_str()
                .unwrap(),
            "./dir1/key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("./dir1/"), "/key/")
                .to_str()
                .unwrap(),
            "./dir1/key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("./dir1/"), "key/file1")
                .to_str()
                .unwrap(),
            "./dir1/key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("./dir1/"), "/key/file1")
                .to_str()
                .unwrap(),
            "./dir1/key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("/dir1/"), "key/")
                .to_str()
                .unwrap(),
            "/dir1/key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("/dir1/"), "/key/")
                .to_str()
                .unwrap(),
            "/dir1/key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("/dir1/"), "/key/file1")
                .to_str()
                .unwrap(),
            "/dir1/key".to_string()
        );
    }

    #[test]
    #[cfg(target_family = "unix")]
    fn key_to_local_path_unix() {
        init_dummy_tracing_subscriber();

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1"), "data")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1/"), "data")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1/data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1/"), "/data")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1/data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1/"), "/data/")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1/data/".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1/"), "data")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1/data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1/"), "/data")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1/data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1/"), "/data/")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1/data/".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("/xyz/dir1/"), "data")
                .to_str()
                .unwrap()
                .to_string(),
            "/xyz/dir1/data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("/xyz/dir1/"), "/data")
                .to_str()
                .unwrap()
                .to_string(),
            "/xyz/dir1/data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("/xyz/dir1/"), "/data/")
                .to_str()
                .unwrap()
                .to_string(),
            "/xyz/dir1/data/".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("./dir1/"), "data")
                .to_str()
                .unwrap()
                .to_string(),
            "./dir1/data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("./dir1/"), "/data")
                .to_str()
                .unwrap()
                .to_string(),
            "./dir1/data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("./dir1/"), "/data/")
                .to_str()
                .unwrap()
                .to_string(),
            "./dir1/data/".to_string()
        );
    }

    #[test]
    #[cfg(target_family = "windows")]
    fn key_to_local_path_windows() {
        init_dummy_tracing_subscriber();

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1\\"), "data")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1\\data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1\\"), "/data")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1\\data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1\\"), "/data/")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1\\data\\".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1\\"), "data")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1\\data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1\\"), "/data")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1\\data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1\\"), "/data/")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1\\data\\".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("c:\\xyz\\dir1\\"), "data")
                .to_str()
                .unwrap()
                .to_string(),
            "c:\\xyz\\dir1\\data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("c:\\xyz\\dir1\\"), "/data")
                .to_str()
                .unwrap()
                .to_string(),
            "c:\\xyz\\dir1\\data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("c:\\xyz\\dir1\\"), "/data/")
                .to_str()
                .unwrap()
                .to_string(),
            "c:\\xyz\\dir1\\data\\".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from(".\\dir1\\"), "data")
                .to_str()
                .unwrap()
                .to_string(),
            ".\\dir1\\data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from(".\\dir1\\"), "/data")
                .to_str()
                .unwrap()
                .to_string(),
            ".\\dir1\\data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from(".\\dir1\\"), "/data/")
                .to_str()
                .unwrap()
                .to_string(),
            ".\\dir1\\data\\".to_string()
        );
    }

    #[test]
    #[cfg(target_family = "unix")]
    fn remove_file_name_if_exist_unix() {
        init_dummy_tracing_subscriber();

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from("dir1/file1"))
                .to_str()
                .unwrap(),
            "dir1"
        );

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from("dir1/dir2/"))
                .to_str()
                .unwrap(),
            "dir1/dir2"
        );

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from("/dir1/file1"))
                .to_str()
                .unwrap(),
            "/dir1"
        );

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from("/dir1/dir2/"))
                .to_str()
                .unwrap(),
            "/dir1/dir2"
        );

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from("./dir1/file1"))
                .to_str()
                .unwrap(),
            "./dir1"
        );

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from("./dir1/dir2/"))
                .to_str()
                .unwrap(),
            "./dir1/dir2"
        );
    }

    #[test]
    #[cfg(target_family = "windows")]
    fn remove_file_name_if_exist_windows() {
        init_dummy_tracing_subscriber();

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from("dir1\\file1"))
                .to_str()
                .unwrap(),
            "dir1"
        );

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from("dir1\\dir2\\"))
                .to_str()
                .unwrap(),
            "dir1\\dir2"
        );

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from("c:\\dir1\\file1"))
                .to_str()
                .unwrap(),
            "c:\\dir1"
        );

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from("c:\\dir1\\dir2\\"))
                .to_str()
                .unwrap(),
            "c:\\dir1\\dir2"
        );

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from(".\\dir1\\file1"))
                .to_str()
                .unwrap(),
            ".\\dir1"
        );

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from(".\\dir1\\dir2\\"))
                .to_str()
                .unwrap(),
            ".\\dir1\\dir2"
        );
    }

    #[test]
    #[cfg(target_family = "windows")]
    fn key_to_local_directory_path_windows() {
        init_dummy_tracing_subscriber();

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("dir1\\"), "key/")
                .to_str()
                .unwrap(),
            "dir1\\key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("dir1\\"), "/key/")
                .to_str()
                .unwrap(),
            "dir1\\key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("dir1\\"), "key/file1")
                .to_str()
                .unwrap(),
            "dir1\\key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("dir1\\"), "/key/file1")
                .to_str()
                .unwrap(),
            "dir1\\key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from(".\\dir1\\"), "key/")
                .to_str()
                .unwrap(),
            ".\\dir1\\key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from(".\\dir1\\"), "/key/")
                .to_str()
                .unwrap(),
            ".\\dir1\\key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from(".\\dir1\\"), "key/file1")
                .to_str()
                .unwrap(),
            ".\\dir1\\key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from(".\\dir1\\"), "/key/file1")
                .to_str()
                .unwrap(),
            ".\\dir1\\key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("c:\\dir1\\"), "key/")
                .to_str()
                .unwrap(),
            "c:\\dir1\\key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("c:\\dir1\\"), "/key/")
                .to_str()
                .unwrap(),
            "c:\\dir1\\key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("c:\\dir1\\"), "/key/file1")
                .to_str()
                .unwrap(),
            "c:\\dir1\\key".to_string()
        );
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn set_last_modification_time_unix() {
        init_dummy_tracing_subscriber();

        set_last_modified("./test_data/".into(), "5byte.dat", 0, 0).unwrap();
        let mtime = get_last_modified(&"./test_data/5byte.dat".into()).await;
        assert_eq!(mtime.secs(), 0);
        assert_eq!(mtime.subsec_nanos(), 0);

        set_last_modified("./test_data/".into(), "5byte.dat", 777, 999).unwrap();
        let mtime = get_last_modified(&"./test_data/5byte.dat".into()).await;
        assert_eq!(mtime.secs(), 777);
        assert_eq!(mtime.subsec_nanos(), 999);
    }

    #[tokio::test]
    #[cfg(target_family = "windows")]
    async fn set_last_modification_time_windows() {
        init_dummy_tracing_subscriber();

        set_last_modified(".\\test_data\\".into(), "5byte.dat", 0, 0).unwrap();
        let mtime = get_last_modified(&"./test_data/5byte.dat".into()).await;
        assert_eq!(mtime.secs(), 0);
        assert_eq!(mtime.subsec_nanos(), 0);

        set_last_modified(".\\test_data\\".into(), "5byte.dat", 777, 999).unwrap();
        let mtime = get_last_modified(&"./test_data/5byte.dat".into()).await;
        assert_eq!(mtime.secs(), 777);
    }

    #[test]
    fn remove_root_slash_test() {
        init_dummy_tracing_subscriber();

        assert_eq!(remove_root_slash("/"), "");
        assert_eq!(remove_root_slash("//"), "");
        assert_eq!(remove_root_slash("/dir1"), "dir1");
        assert_eq!(remove_root_slash("//dir1"), "dir1");
        assert_eq!(remove_root_slash("///dir1"), "dir1");
        assert_eq!(remove_root_slash("//dir1/"), "dir1/");

        assert_eq!(remove_root_slash("/dir1/dir2/dir3"), "dir1/dir2/dir3");
        assert_eq!(remove_root_slash("//dir1/dir2/dir3"), "dir1/dir2/dir3");
        assert_eq!(remove_root_slash("///dir1/dir2/dir3"), "dir1/dir2/dir3");

        assert_eq!(remove_root_slash("dir1/dir2/dir3"), "dir1/dir2/dir3");
        assert_eq!(remove_root_slash("dir1/dir2/dir3/"), "dir1/dir2/dir3/");

        assert_eq!(remove_root_slash("key1"), "key1");
        assert_eq!(remove_root_slash("key1/"), "key1/");
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .or_else(|_| EnvFilter::try_new("dummy=trace"))
                    .unwrap(),
            )
            .try_init();
    }
}
