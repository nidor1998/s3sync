#![allow(dead_code)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time;
use std::time::SystemTime;

use async_channel::Receiver;
use aws_config::meta::region::{ProvideRegion, RegionProviderChain};
use aws_config::{BehaviorVersion, ConfigLoader};
use aws_sdk_s3::client::Client;
use aws_sdk_s3::config::Builder;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::primitives::{DateTime, DateTimeFormat};
use aws_sdk_s3::types::{
    BucketLocationConstraint, BucketVersioningStatus, CreateBucketConfiguration, Object,
    ObjectVersion, Tag, Tagging, VersioningConfiguration,
};
use aws_types::SdkConfig;
use filetime::{set_file_mtime, FileTime};
use once_cell::sync::Lazy;
use tokio::sync::Semaphore;
use uuid::Uuid;
use walkdir::DirEntry;
use walkdir::WalkDir;

use s3sync::config::args::parse_from_args;
use s3sync::pipeline::Pipeline;
use s3sync::types::token::create_pipeline_cancellation_token;
use s3sync::types::SyncStatistics;
use s3sync::Config;

pub const REGION: &str = "ap-northeast-1";

pub const TEMP_DOWNLOAD_DIR: &str = "./playground/download/";

pub const LARGE_FILE_PATH: &str = "./playground/large_data_e2e_test/large_file";
pub const LARGE_FILE_DIR: &str = "./playground/large_data_e2e_test/";
pub const LARGE_FILE_SIZE: usize = 30 * 1024 * 1024;
pub const LARGE_FILE_KEY: &str = "large_file";
pub const LARGE_FILE_S3_ETAG: &str = "\"9be3303e9a8d67a0f1e609fb7a29030a-4\"";

const TEST_CONTENT_DISPOSITION: &str = "attachment; filename=\"filename.jpg\"";
const TEST_CONTENT_ENCODING: &str = "deflate";
const TEST_CONTENT_LANGUAGE: &str = "en-US,en-CA";
const TEST_CACHE_CONTROL: &str = "s-maxage=1604800";
const TEST_CONTENT_TYPE: &str = "application/vnd.ms-excel";
const TEST_TAGGING: &str = "tag1=tag_value1&tag2=tag_value2";
const TEST_METADATA_STRING: &str = "key1=value1,key2=value2";
pub static TEST_METADATA: Lazy<HashMap<String, String>> = Lazy::new(|| {
    HashMap::from([
        ("key1".to_string(), "value1".to_string()),
        ("key2".to_string(), "value2".to_string()),
    ])
});
const TEST_EXPIRES: &str = "2055-05-20T00:00:00.000Z";

pub static BUCKET1: Lazy<String> = Lazy::new(|| format!("bucket1-{}", Uuid::new_v4()));
pub static BUCKET2: Lazy<String> = Lazy::new(|| format!("bucket2-{}", Uuid::new_v4()));

pub static SEMAPHORE: Lazy<Arc<Semaphore>> = Lazy::new(|| Arc::new(Semaphore::new(1)));

pub const TOUCH_FILE_SECS_FROM_NOW: i64 = 10;
pub const SLEEP_SECS_BEFORE_RESYNC: u64 = 5;
pub const SLEEP_SECS_AFTER_DELETE_BUCKET: u64 = 10;

pub const TEST_SSE_C_KEY_1: &str = "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=";
pub const TEST_SSE_C_KEY_1_MD5: &str = "zZ5FnqcIqUjVwvWmyog4zw==";
pub const TEST_SSE_C_KEY_2: &str = "MTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTE=";
pub const TEST_SSE_C_KEY_2_MD5: &str = "GoDL8oWeAZVZNl1r5Hh5Tg==";

const PROFILE_NAME: &str = "s3sync-e2e-test";

#[cfg(feature = "e2e_test")]
pub struct TestHelper {
    client: Client,
}

#[derive(Debug, Default)]
pub struct StatsCount {
    pub sync_complete: u64,
    pub sync_skip: u64,
    pub sync_delete: u64,
    pub sync_error: u64,
    pub sync_warning: u64,
    pub etag_verified: u64,
    pub checksum_verified: u64,
}

#[cfg(feature = "e2e_test")]
impl TestHelper {
    pub async fn new() -> Self {
        Self {
            client: Self::create_client().await,
        }
    }

    pub async fn create_client() -> Client {
        Client::from_conf(Builder::from(&Self::load_sdk_config().await).build())
    }

    async fn load_sdk_config() -> SdkConfig {
        let config_loader =
            Self::load_config_credential(aws_config::defaults(BehaviorVersion::latest()))
                .region(Self::build_provider_region());

        config_loader.load().await
    }

    fn load_config_credential(config_loader: ConfigLoader) -> ConfigLoader {
        let builder = aws_config::profile::ProfileFileCredentialsProvider::builder();

        config_loader.credentials_provider(builder.profile_name(PROFILE_NAME).build())
    }

    fn build_provider_region() -> Box<dyn ProvideRegion> {
        let mut builder = aws_config::profile::ProfileFileRegionProvider::builder();

        builder = builder.profile_name(PROFILE_NAME);

        let provider_region = RegionProviderChain::first_try(builder.build());
        Box::new(provider_region)
    }

    pub async fn create_bucket(&self, bucket: &str, region: &str) {
        let constraint = BucketLocationConstraint::from(region);
        let cfg = CreateBucketConfiguration::builder()
            .location_constraint(constraint)
            .build();

        self.client
            .create_bucket()
            .create_bucket_configuration(cfg)
            .bucket(bucket)
            .send()
            .await
            .unwrap();
    }

    pub async fn enable_bucket_versioning(&self, bucket: &str) {
        self.client
            .put_bucket_versioning()
            .bucket(bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .unwrap();
    }

    pub async fn is_bucket_versioning_enabled(&self, bucket: &str) -> bool {
        let get_bucket_versioning_result = self
            .client
            .get_bucket_versioning()
            .bucket(bucket)
            .send()
            .await;

        if let Err(e) = get_bucket_versioning_result {
            let service_error = e.into_service_error();
            if let Some(code) = service_error.meta().code() {
                assert_eq!(code, "NoSuchBucket");
            } else {
                panic!("S3 API error has occurred.")
            }

            return false;
        }

        if let Some(status) = get_bucket_versioning_result.unwrap().status() {
            return *status == BucketVersioningStatus::Enabled;
        }

        false
    }

    pub async fn is_bucket_exist(&self, bucket: &str) -> bool {
        let head_bucket_result = self.client.head_bucket().bucket(bucket).send().await;

        if head_bucket_result.is_ok() {
            return true;
        }

        !head_bucket_result
            .err()
            .unwrap()
            .into_service_error()
            .is_not_found()
    }

    pub async fn delete_bucket_with_cascade(&self, bucket: &str) {
        if !self.is_bucket_exist(bucket).await {
            return;
        }

        if self.is_bucket_versioning_enabled(bucket).await {
            self.delete_all_object_versions(bucket).await;
        } else {
            self.delete_all_objects(bucket).await;
        }

        let result = self.client.delete_bucket().bucket(bucket).send().await;

        if let Err(e) = result {
            let service_error = e.into_service_error();
            if let Some(code) = service_error.meta().code() {
                assert_eq!(code, "NoSuchBucket");
            } else {
                panic!("S3 API error has occurred.")
            }
        }

        tokio::time::sleep(time::Duration::from_secs(SLEEP_SECS_AFTER_DELETE_BUCKET)).await;
    }

    pub async fn list_objects(&self, bucket: &str, prefix: &str) -> Vec<Object> {
        let list_objects_output = self
            .client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .send()
            .await
            .unwrap();

        list_objects_output.contents().to_vec()
    }

    pub async fn list_object_versions(&self, bucket: &str, prefix: &str) -> Vec<ObjectVersion> {
        let list_object_versions_output = self
            .client
            .list_object_versions()
            .bucket(bucket)
            .prefix(prefix)
            .send()
            .await
            .unwrap();

        list_object_versions_output.versions().to_vec()
    }

    pub async fn head_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
    ) -> HeadObjectOutput {
        self.client
            .head_object()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id.clone())
            .send()
            .await
            .unwrap()
    }

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
    ) -> GetObjectOutput {
        self.client
            .get_object()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id.clone())
            .send()
            .await
            .unwrap()
    }

    pub async fn get_object_last_modified(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
    ) -> i64 {
        let head_object_output = self.head_object(bucket, key, version_id).await;
        head_object_output.last_modified().unwrap().secs()
    }

    pub async fn get_object_tagging(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
    ) -> GetObjectTaggingOutput {
        self.client
            .get_object_tagging()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id)
            .send()
            .await
            .unwrap()
    }

    pub async fn is_object_exist(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
    ) -> bool {
        let head_object_result = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id.clone())
            .send()
            .await;

        if head_object_result.is_ok() {
            return true;
        }

        !head_object_result
            .err()
            .unwrap()
            .into_service_error()
            .is_not_found()
    }

    pub async fn put_object_with_metadata(&self, bucket: &str, key: &str, path: &str) {
        let stream = ByteStream::from_path(path).await.unwrap();
        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .cache_control(TEST_CACHE_CONTROL)
            .content_disposition(TEST_CONTENT_DISPOSITION)
            .content_encoding(TEST_CONTENT_ENCODING)
            .content_language(TEST_CONTENT_LANGUAGE)
            .content_type(TEST_CONTENT_TYPE)
            .set_metadata(Some(TEST_METADATA.clone()))
            .expires(DateTime::from_str(TEST_EXPIRES, DateTimeFormat::DateTime).unwrap())
            .tagging(TEST_TAGGING)
            .body(stream)
            .send()
            .await
            .unwrap();
    }

    pub async fn put_sized_object(&self, bucket: &str, key: &str, size: usize) {
        let mut buffer = Vec::<u8>::with_capacity(size);
        buffer.resize_with(size, Default::default);
        let stream = ByteStream::from(buffer);

        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(stream)
            .send()
            .await
            .unwrap();
    }

    pub async fn put_empty_object(&self, bucket: &str, key: &str) {
        let stream = ByteStream::from_static(&[]);
        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(stream)
            .send()
            .await
            .unwrap();
    }

    pub async fn put_object_tagging(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
        tagging: Tagging,
    ) {
        self.client
            .put_object_tagging()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id)
            .tagging(tagging)
            .send()
            .await
            .unwrap();
    }

    pub async fn delete_object(&self, bucket: &str, key: &str, version_id: Option<String>) {
        self.client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id.clone())
            .send()
            .await
            .unwrap();
    }

    pub async fn delete_object_tagging(&self, bucket: &str, key: &str, version_id: Option<String>) {
        self.client
            .delete_object_tagging()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id)
            .send()
            .await
            .unwrap();
    }

    pub async fn delete_all_object_versions(&self, bucket: &str) {
        let list_object_versions_output_result = self
            .client
            .list_object_versions()
            .bucket(bucket)
            .send()
            .await;

        if let Err(e) = list_object_versions_output_result {
            let service_error = e.into_service_error();
            if let Some(code) = service_error.meta().code() {
                assert_eq!(code, "NoSuchBucket");
            } else {
                println!("{:?}", service_error);
                panic!("S3 API error has occurred.")
            }

            return;
        }

        for object in list_object_versions_output_result
            .as_ref()
            .unwrap()
            .versions()
        {
            self.delete_object(
                bucket,
                object.key().unwrap(),
                Some(object.version_id().unwrap().to_string()),
            )
            .await;
        }

        for object in list_object_versions_output_result
            .as_ref()
            .unwrap()
            .delete_markers()
        {
            self.delete_object(
                bucket,
                object.key().unwrap(),
                Some(object.version_id().unwrap().to_string()),
            )
            .await;
        }
    }

    pub async fn delete_all_objects(&self, bucket: &str) {
        let list_objects_output_result = self.client.list_objects_v2().bucket(bucket).send().await;

        if let Err(e) = list_objects_output_result {
            assert!(e.into_service_error().is_no_such_bucket());
            return;
        }

        for object in list_objects_output_result.unwrap().contents() {
            self.delete_object(bucket, object.key().unwrap(), None)
                .await;
        }
    }

    pub async fn verify_test_object_metadata(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
    ) -> bool {
        let head_object_output = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id.clone())
            .send()
            .await
            .unwrap();

        assert_eq!(
            head_object_output.cache_control().unwrap(),
            TEST_CACHE_CONTROL
        );
        assert_eq!(
            head_object_output.content_disposition().unwrap(),
            TEST_CONTENT_DISPOSITION
        );
        assert_eq!(
            head_object_output.content_encoding().unwrap(),
            TEST_CONTENT_ENCODING
        );
        assert_eq!(
            head_object_output.content_language().unwrap(),
            TEST_CONTENT_LANGUAGE
        );
        assert_eq!(
            head_object_output.content_type().unwrap(),
            TEST_CONTENT_TYPE
        );
        assert_eq!(
            head_object_output.metadata().unwrap(),
            &TEST_METADATA.clone()
        );
        assert_eq!(
            head_object_output.expires().unwrap(),
            &DateTime::from_str(TEST_EXPIRES, DateTimeFormat::DateTime).unwrap()
        );

        let get_object_tagging_output = self
            .get_object_tagging(bucket, key, version_id.clone())
            .await;

        let tag_set = get_object_tagging_output.tag_set();
        let tag_map = TestHelper::tag_set_to_map(tag_set);
        let expected_tag_map = HashMap::from([
            ("tag1".to_string(), "tag_value1".to_string()),
            ("tag2".to_string(), "tag_value2".to_string()),
        ]);

        assert_eq!(tag_map, expected_tag_map);

        true
    }

    pub async fn verify_e_tag(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
        e_tag: &str,
    ) -> bool {
        let head_object_output = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id.clone())
            .send()
            .await
            .unwrap();

        head_object_output.e_tag().unwrap() == e_tag
    }

    pub fn verify_file_md5_digest(path: &str, digest: &str) -> bool {
        let normalized_path = path.replace(&std::path::MAIN_SEPARATOR.to_string(), "/");
        let md5_map = HashMap::from([
            (
                "./test_data/e2e_test/case1/dir2/data2",
                "ebfe38003ea20ca7a207aa370ce18a0e",
            ),
            (
                "./test_data/e2e_test/case1/dir5/data3",
                "e72f8e3896f1c5a1c0701e676f251d7c",
            ),
            (
                "./test_data/e2e_test/case1/data1",
                "648d0f85a39588608b4173f6371e9c37",
            ),
            (
                "./test_data/e2e_test/case1/dir21/data1",
                "6aeb82e0da27f3246840382773a38103",
            ),
            (
                "./test_data/e2e_test/case1/dir1/data1",
                "0dd7cd23c492b5a3a62672b4049bb1ed",
            ),
        ]);

        *md5_map.get(normalized_path.as_str()).unwrap() == digest.replace('\"', "")
    }

    pub fn verify_object_md5_digest(object: &str, digest: &str) -> bool {
        let md5_map = HashMap::from([
            ("dir2/data2", "ebfe38003ea20ca7a207aa370ce18a0e"),
            ("dir5/data3", "e72f8e3896f1c5a1c0701e676f251d7c"),
            ("data1", "648d0f85a39588608b4173f6371e9c37"),
            ("dir21/data1", "6aeb82e0da27f3246840382773a38103"),
            ("dir1/data1", "0dd7cd23c492b5a3a62672b4049bb1ed"),
        ]);

        *md5_map.get(object).unwrap() == digest.replace('\"', "")
    }

    pub fn list_all_files(path: &str) -> Vec<DirEntry> {
        WalkDir::new(path)
            .into_iter()
            .filter(|entry| {
                if entry.is_err() {
                    return false;
                }
                entry.as_ref().unwrap().file_type().is_file()
            })
            .map(|entry| entry.unwrap())
            .collect()
    }

    pub fn delete_all_files(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    pub fn read_file(path: &str) -> Vec<u8> {
        std::fs::read(path).unwrap()
    }

    #[allow(dead_code)]
    pub fn read_file_to_string(path: &str) -> String {
        std::fs::read_to_string(path).unwrap()
    }

    pub fn md5_digest(path: &str) -> String {
        format!("{:x}", md5::compute(Self::read_file(path)))
    }

    pub async fn sync_test_data(&self, target_bucket_url: &str) {
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "./test_data/e2e_test/case1/",
            target_bucket_url,
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

        pipeline.run().await;
        assert!(!pipeline.has_error());

        let object_list = self.list_objects(&BUCKET1.to_string(), "").await;
        assert_eq!(object_list.len(), 5);
    }

    pub async fn sync_test_data_with_all_metadata_option(&self, target_bucket_url: &str) {
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--content-disposition",
            TEST_CONTENT_DISPOSITION,
            "--content-encoding",
            TEST_CONTENT_ENCODING,
            "--content-language",
            TEST_CONTENT_LANGUAGE,
            "--cache-control",
            TEST_CACHE_CONTROL,
            "--content-type",
            TEST_CONTENT_TYPE,
            "--expires",
            TEST_EXPIRES,
            "--metadata",
            TEST_METADATA_STRING,
            "--tagging",
            TEST_TAGGING,
            "./test_data/e2e_test/case1/",
            target_bucket_url,
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

        pipeline.run().await;
        assert!(!pipeline.has_error());

        let object_list = self.list_objects(&BUCKET1.to_string(), "").await;
        assert_eq!(object_list.len(), 5);
    }

    pub async fn sync_test_data_with_sha256(&self, target_bucket_url: &str) {
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA256",
            "./test_data/e2e_test/case1/",
            target_bucket_url,
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

        pipeline.run().await;
        assert!(!pipeline.has_error());

        let object_list = self.list_objects(&BUCKET1.to_string(), "").await;
        assert_eq!(object_list.len(), 5);
    }

    pub async fn sync_large_test_data(&self, target_bucket_url: &str) {
        Self::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            LARGE_FILE_DIR,
            target_bucket_url,
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

        pipeline.run().await;
        assert!(!pipeline.has_error());
    }

    pub async fn sync_large_test_data_with_custom_chunksize(
        &self,
        target_bucket_url: &str,
        chunksize: &str,
    ) {
        Self::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            LARGE_FILE_DIR,
            "--multipart-chunksize",
            chunksize,
            target_bucket_url,
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

        pipeline.run().await;
        assert!(!pipeline.has_error());
    }

    pub async fn sync_large_test_data_with_custom_chunksize_sha256(
        &self,
        target_bucket_url: &str,
        chunksize: &str,
    ) {
        Self::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            LARGE_FILE_DIR,
            "--additional-checksum-algorithm",
            "SHA256",
            "--multipart-chunksize",
            chunksize,
            target_bucket_url,
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

        pipeline.run().await;
        assert!(!pipeline.has_error());
    }

    pub async fn sync_large_test_data_with_all_metadata_option(&self, target_bucket_url: &str) {
        Self::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--content-disposition",
            TEST_CONTENT_DISPOSITION,
            "--content-encoding",
            TEST_CONTENT_ENCODING,
            "--content-language",
            TEST_CONTENT_LANGUAGE,
            "--cache-control",
            TEST_CACHE_CONTROL,
            "--content-type",
            TEST_CONTENT_TYPE,
            "--expires",
            TEST_EXPIRES,
            "--metadata",
            TEST_METADATA_STRING,
            "--tagging",
            TEST_TAGGING,
            LARGE_FILE_DIR,
            target_bucket_url,
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

        pipeline.run().await;
        assert!(!pipeline.has_error());
    }

    pub async fn sync_large_test_data_with_sha256(&self, target_bucket_url: &str) {
        Self::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA256",
            LARGE_FILE_DIR,
            target_bucket_url,
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

        pipeline.run().await;
        assert!(!pipeline.has_error());
    }

    pub async fn sync_empty_data_with_sha256(&self, target_bucket_url: &str) {
        Self::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA256",
            "./test_data/e2e_test/case4/",
            target_bucket_url,
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

        pipeline.run().await;
        assert!(!pipeline.has_error());
    }

    pub async fn sync_large_test_data_with_sha1(&self, target_bucket_url: &str) {
        Self::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA1",
            LARGE_FILE_DIR,
            target_bucket_url,
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

        pipeline.run().await;
        assert!(!pipeline.has_error());
    }

    pub async fn sync_large_test_data_with_crc32(&self, target_bucket_url: &str) {
        Self::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "CRC32",
            LARGE_FILE_DIR,
            target_bucket_url,
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

        pipeline.run().await;
        assert!(!pipeline.has_error());
    }

    pub async fn sync_large_test_data_with_crc32_c(&self, target_bucket_url: &str) {
        Self::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "CRC32C",
            LARGE_FILE_DIR,
            target_bucket_url,
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

        pipeline.run().await;
        assert!(!pipeline.has_error());
    }

    pub fn get_skip_count(stats_receiver: Receiver<SyncStatistics>) -> u16 {
        let mut total_skip_count = 0;

        while let Ok(sync_stats) = stats_receiver.try_recv() {
            if matches!(sync_stats, SyncStatistics::SyncSkip { .. }) {
                total_skip_count += 1;
            }
        }

        total_skip_count
    }

    pub fn get_sync_count(stats_receiver: Receiver<SyncStatistics>) -> u16 {
        let mut total_sync_count = 0;

        while let Ok(sync_stats) = stats_receiver.try_recv() {
            if matches!(sync_stats, SyncStatistics::SyncComplete { .. }) {
                total_sync_count += 1;
            }
        }

        total_sync_count
    }

    pub fn get_warning_count(stats_receiver: Receiver<SyncStatistics>) -> u16 {
        let mut total_warning_count = 0;

        while let Ok(sync_stats) = stats_receiver.try_recv() {
            if matches!(sync_stats, SyncStatistics::SyncWarning { .. }) {
                total_warning_count += 1;
            }
        }

        total_warning_count
    }

    pub fn is_file_exist(path: &str) -> bool {
        PathBuf::from(path).try_exists().unwrap()
    }

    pub fn get_file_last_modified(path: &str) -> i64 {
        std::fs::File::open(path)
            .unwrap()
            .metadata()
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    pub fn create_large_file() {
        if Self::is_file_exist(LARGE_FILE_PATH) {
            return;
        }

        std::fs::create_dir_all(LARGE_FILE_DIR).unwrap();

        let data = vec![0_u8; LARGE_FILE_SIZE];
        std::fs::write(LARGE_FILE_PATH, data.as_slice()).unwrap();
    }

    pub fn touch_file(path: &str, add_sec: i64) {
        set_file_mtime(
            path,
            FileTime::from_unix_time(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64
                    + add_sec,
                0,
            ),
        )
        .unwrap();
    }

    pub fn tag_set_to_map(tag_set: &[Tag]) -> HashMap<String, String> {
        let mut map = HashMap::<_, _>::new();
        for tag in tag_set {
            map.insert(tag.key().to_string(), tag.value().to_string());
        }

        map
    }

    pub fn get_stats_count(stats_receiver: Receiver<SyncStatistics>) -> StatsCount {
        let mut stats = StatsCount::default();

        while let Ok(sync_stats) = stats_receiver.try_recv() {
            match sync_stats {
                SyncStatistics::SyncComplete { .. } => {
                    stats.sync_complete += 1;
                }
                SyncStatistics::SyncSkip { .. } => {
                    stats.sync_skip += 1;
                }
                SyncStatistics::SyncDelete { .. } => {
                    stats.sync_delete += 1;
                }
                SyncStatistics::SyncError { .. } => {
                    stats.sync_error += 1;
                }
                SyncStatistics::SyncWarning { .. } => {
                    stats.sync_warning += 1;
                }
                SyncStatistics::EtagVerified { .. } => {
                    stats.etag_verified += 1;
                }
                SyncStatistics::ChecksumVerified { .. } => {
                    stats.checksum_verified += 1;
                }
                _ => {}
            }
        }

        stats
    }

    pub fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
