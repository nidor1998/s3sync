#![allow(dead_code)]

use anyhow::Result;
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
    BucketInfo, BucketLocationConstraint, BucketType, BucketVersioningStatus, ChecksumMode,
    CreateBucketConfiguration, DataRedundancy, LocationInfo, LocationType, Object, ObjectVersion,
    Tag, Tagging, VersioningConfiguration,
};
use aws_smithy_types::checksum_config::RequestChecksumCalculation::WhenRequired;
use aws_types::SdkConfig;
use filetime::{set_file_mtime, FileTime};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time;
use std::time::SystemTime;
use tokio::sync::Semaphore;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;
use walkdir::DirEntry;
use walkdir::WalkDir;

use sha2::{Digest, Sha256};

use s3sync::config::args::parse_from_args;
use s3sync::pipeline::Pipeline;
use s3sync::types::token::create_pipeline_cancellation_token;
use s3sync::types::SyncStatistics;
use s3sync::Config;

pub const REGION: &str = "ap-northeast-1";
pub const EXPRESS_ONE_ZONE_AZ: &str = "apne1-az4";

pub const TEMP_DOWNLOAD_DIR: &str = "./playground/download/";

pub const LARGE_FILE_PATH: &str = "./playground/large_data_e2e_test/large_file";
pub const LARGE_FILE_PATH_CASE2: &str = "./playground/large_data_e2e_test_case2/large_file";
pub const LARGE_FILE_PATH_CASE3: &str = "./playground/large_data_e2e_test_case3/large_file";
pub const LARGE_FILE_DIR: &str = "./playground/large_data_e2e_test/";
pub const LARGE_FILE_DIR_CASE2: &str = "./playground/large_data_e2e_test_case2/";
pub const LARGE_FILE_DIR_CASE3: &str = "./playground/large_data_e2e_test_case3/";
pub const RANDOM_DATA_FILE_DIR: &str = "./playground/random_data_file_dir/";

pub const RANDOM_DATA_SEED_FILE: &str = "./test_data/random_data_seed";
pub const NOT_FOUND_TEST_DIR: &str = "./playground/not_found_test/";
pub const NOT_FOUND_TEST_FILE: &str =
    "./playground/not_found_test/s3sync_not_found_test_66143ea2-53cb-4ee9-98d6-7067bf5f325d";

pub const TEST_8MIB_FILE_DIR: &str = "./playground/large_data_e2e_8mib_test/";
pub const TEST_8MIB_FILE_PATH: &str = "./playground/large_data_e2e_8mib_test/8mib_file";

pub const LARGE_FILE_SIZE: usize = 30 * 1024 * 1024;
pub const LARGE_FILE_KEY: &str = "large_file";
pub const LARGE_FILE_S3_ETAG: &str = "\"9be3303e9a8d67a0f1e609fb7a29030a-4\"";
pub const TEST_FILE_SIZE_8MIB: usize = 8 * 1024 * 1024;
pub const TEST_8MIB_FILE_KEY: &str = "8mib_file";

pub const TEST_RANDOM_DATA_FILE_KEY: &str = "random_data";

pub const TEST_CONTENT_DISPOSITION: &str = "attachment; filename=\"filename.jpg\"";
pub const TEST_CONTENT_ENCODING: &str = "deflate";
pub const TEST_CONTENT_LANGUAGE: &str = "en-US,en-CA";
pub const TEST_CACHE_CONTROL: &str = "s-maxage=1604800";
pub const TEST_CONTENT_TYPE: &str = "application/vnd.ms-excel";
pub const TEST_TAGGING: &str = "tag1=tag_value1&tag2=tag_value2";
pub const TEST_METADATA_STRING: &str = "key1=value1,key2=value2";
pub const TEST_WEBSITE_REDIRECT: &str = "/redirect";

pub const TEST_CONTENT_DISPOSITION2: &str = "attachment; filename=\"filename2.jpg\"";
pub const TEST_CONTENT_ENCODING2: &str = "gzip";
pub const TEST_CONTENT_LANGUAGE2: &str = "en-US,en-GB";
pub const TEST_CACHE_CONTROL2: &str = "s-maxage=1704800";
pub const TEST_CONTENT_TYPE2: &str = "application/excel";
pub const TEST_TAGGING2: &str = "tag1=tag_value1&tag2=tag_valueNew";
pub const TEST_METADATA_STRING2: &str = "key1=value1,key2=value2,key3=value3";
pub const TEST_WEBSITE_REDIRECT2: &str = "/redirect2";

pub static TEST_METADATA: Lazy<HashMap<String, String>> = Lazy::new(|| {
    HashMap::from([
        ("key1".to_string(), "value1".to_string()),
        ("key2".to_string(), "value2".to_string()),
    ])
});
pub const TEST_EXPIRES: &str = "2055-05-20T00:00:00.000Z";
pub const TEST_EXPIRES2: &str = "2055-04-20T00:00:00.000Z";

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

const GET_OBJECT_DENY_BUCKET_POLICY: &str = r#"{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "DenyGetOperation",
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::{{ bucket }}/*"
        }
    ]
}"#;

pub const SLEEP_TIME_MILLIS_AFTER_INTEGRATION_TEST: u64 = 30 * 1000;

const NOT_FOUND_DANGEROUS_SIMULATION_ENV: &str = "S3SYNC_NOT_FOUND_DANGEROUS_SIMULATION";
const NOT_FOUND_DANGEROUS_SIMULATION_ENV_ALLOW: &str = "ALLOW";
const CANCEL_DANGEROUS_SIMULATION_ENV: &str = "S3SYNC_CANCEL_DANGEROUS_SIMULATION";
const CANCEL_DANGEROUS_SIMULATION_ENV_ALLOW: &str = "ALLOW";

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
    pub e_tag_verified: u64,
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
        Client::from_conf(
            Builder::from(&Self::load_sdk_config().await)
                .request_checksum_calculation(WhenRequired)
                .build(),
        )
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

    pub async fn create_directory_bucket(&self, bucket_name: &str, availability_zone: &str) {
        let location_info = LocationInfo::builder()
            .r#type(LocationType::AvailabilityZone)
            .name(availability_zone)
            .build();
        let bucket_info = BucketInfo::builder()
            .data_redundancy(DataRedundancy::SingleAvailabilityZone)
            .r#type(BucketType::Directory)
            .build();
        let configuration = CreateBucketConfiguration::builder()
            .location(location_info)
            .bucket(bucket_info)
            .build();

        self.client
            .create_bucket()
            .create_bucket_configuration(configuration)
            .bucket(bucket_name)
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
                // skipcq: RS-W1021
                assert!(false, "S3 API error has occurred.")
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
                // skipcq: RS-W1021
                assert!(false, "S3 API error has occurred.")
            }
        }

        tokio::time::sleep(time::Duration::from_secs(SLEEP_SECS_AFTER_DELETE_BUCKET)).await;
    }

    pub async fn delete_directory_bucket_with_cascade(&self, bucket: &str) {
        if !self.is_bucket_exist(bucket).await {
            return;
        }

        self.delete_all_objects(bucket).await;

        let result = self.client.delete_bucket().bucket(bucket).send().await;

        if let Err(e) = result {
            let service_error = e.into_service_error();
            if let Some(code) = service_error.meta().code() {
                assert_eq!(code, "NoSuchBucket");
            } else {
                // skipcq: RS-W1021
                assert!(false, "S3 API error has occurred.")
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
            .checksum_mode(ChecksumMode::Enabled)
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
            .website_redirect_location("/xxx")
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
                // skipcq: RS-W1021
                assert!(false, "S3 API error has occurred.")
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

        if list_objects_output_result.is_err() {
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
            head_object_output.expires_string.unwrap(),
            DateTime::from_str(TEST_EXPIRES, DateTimeFormat::DateTime)
                .unwrap()
                .fmt(DateTimeFormat::HttpDate)
                .unwrap()
                .to_string()
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

    pub async fn verify_test_object_no_system_metadata(
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

        assert!(head_object_output.cache_control().is_none());
        assert!(head_object_output.content_disposition().is_none());
        assert!(head_object_output.content_encoding().is_none());
        assert!(head_object_output.content_language().is_none());
        assert!(head_object_output.expires_string.is_none());
        assert!(head_object_output.website_redirect_location().is_none());

        // S3 does set content-type to application/octet-stream
        assert_eq!(
            head_object_output.content_type().unwrap(),
            "application/octet-stream"
        );

        assert_eq!(
            head_object_output.metadata().unwrap(),
            &TEST_METADATA.clone()
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

    pub async fn verify_test_object_no_user_defined_metadata(
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
        assert!(head_object_output.metadata().unwrap().is_empty());
        assert_eq!(
            head_object_output.expires_string.unwrap(),
            DateTime::from_str(TEST_EXPIRES, DateTimeFormat::DateTime)
                .unwrap()
                .fmt(DateTimeFormat::HttpDate)
                .unwrap()
                .to_string()
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

    pub async fn sync_test_data_with_website_redirect(
        &self,
        target_bucket_url: &str,
        redirect: &str,
    ) {
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--website-redirect",
            redirect,
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

    pub async fn sync_directory_bucket_test_data(&self, target_bucket_url: &str) {
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

    pub async fn sync_test_data_with_sha1(&self, target_bucket_url: &str) {
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA1",
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

    pub async fn sync_test_data_with_crc32(&self, target_bucket_url: &str) {
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "CRC32",
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

    pub async fn sync_test_data_with_crc32c(&self, target_bucket_url: &str) {
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "CRC32C",
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

    pub async fn sync_test_data_with_crc64nvme(&self, target_bucket_url: &str) {
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "CRC64NVME",
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

    pub async fn sync_large_test_data_with_website_redirect(
        &self,
        target_bucket_url: &str,
        redirect: &str,
    ) {
        Self::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--website-redirect",
            redirect,
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
            "--remove-modified-filter",
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

    pub async fn sync_large_test_data_with_custom_chunksize_sha1(
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
            "SHA1",
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

    pub async fn sync_large_test_data_with_custom_chunksize_crc64nvme(
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
            "CRC64NVME",
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

    pub async fn sync_large_test_data_with_custom_chunksize_crc32_full_object(
        &self,
        target_bucket_url: &str,
        chunksize: &str,
    ) {
        Self::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--full-object-checksum",
            "--additional-checksum-algorithm",
            "CRC32",
            "--multipart-chunksize",
            chunksize,
            LARGE_FILE_DIR,
            target_bucket_url,
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

        pipeline.run().await;
        assert!(!pipeline.has_error());
    }

    pub async fn sync_large_test_data_with_custom_chunksize_crc32c_full_object(
        &self,
        target_bucket_url: &str,
        chunksize: &str,
    ) {
        Self::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--full-object-checksum",
            "--additional-checksum-algorithm",
            "CRC32C",
            "--multipart-chunksize",
            chunksize,
            LARGE_FILE_DIR,
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

    pub async fn sync_8mib_test_data_with_sha256(&self, target_bucket_url: &str) {
        Self::create_8mib_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA256",
            TEST_8MIB_FILE_DIR,
            target_bucket_url,
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

        pipeline.run().await;
        assert!(!pipeline.has_error());
    }

    pub async fn sync_8mib_test_data_with_full_object_crc32(&self, target_bucket_url: &str) {
        Self::create_8mib_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--full-object-checksum",
            "--additional-checksum-algorithm",
            "CRC32",
            TEST_8MIB_FILE_DIR,
            target_bucket_url,
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

        pipeline.run().await;
        assert!(!pipeline.has_error());
    }

    pub async fn sync_8mib_test_data_with_full_object_crc32c(&self, target_bucket_url: &str) {
        Self::create_8mib_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--full-object-checksum",
            "--additional-checksum-algorithm",
            "CRC32C",
            TEST_8MIB_FILE_DIR,
            target_bucket_url,
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let cancellation_token = create_pipeline_cancellation_token();
        let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

        pipeline.run().await;
        assert!(!pipeline.has_error());
    }

    pub async fn sync_8mib_test_data_with_full_object_crc64nvme(&self, target_bucket_url: &str) {
        Self::create_8mib_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "CRC64NVME",
            TEST_8MIB_FILE_DIR,
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

    pub async fn sync_large_test_data_with_crc32_full_object_checksum(
        &self,
        target_bucket_url: &str,
    ) {
        Self::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--full-object-checksum",
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

    pub async fn sync_large_test_data_with_crc32c(&self, target_bucket_url: &str) {
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

    pub async fn sync_large_test_data_with_crc32c_full_object_checksum(
        &self,
        target_bucket_url: &str,
    ) {
        Self::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--full-object-checksum",
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

    pub async fn sync_large_test_data_with_crc64nvme(&self, target_bucket_url: &str) {
        Self::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "CRC64NVME",
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

    pub fn create_case3_large_file() {
        std::fs::create_dir_all(LARGE_FILE_DIR_CASE3).unwrap();

        let data = vec![0_u8; LARGE_FILE_SIZE];
        std::fs::write(LARGE_FILE_PATH_CASE3, data.as_slice()).unwrap();
    }

    pub fn modify_case3_large_file() {
        std::fs::create_dir_all(LARGE_FILE_DIR_CASE3).unwrap();

        std::thread::sleep(time::Duration::from_secs(2));

        let data = vec![1_u8; LARGE_FILE_SIZE];
        std::fs::write(LARGE_FILE_PATH_CASE3, data.as_slice()).unwrap();
    }

    pub fn update_case3_large_file_mtime() {
        let path = Path::new(LARGE_FILE_PATH_CASE3);

        std::thread::sleep(time::Duration::from_secs(2));

        let now = FileTime::now();
        set_file_mtime(path, now).unwrap();
    }

    pub fn create_not_found_test_file() {
        if Self::is_file_exist(NOT_FOUND_TEST_FILE) {
            return;
        }

        std::fs::create_dir_all(NOT_FOUND_TEST_DIR).unwrap();

        let data = vec![0_u8; 1];
        std::fs::write(NOT_FOUND_TEST_FILE, data.as_slice()).unwrap();
    }

    pub fn create_large_file_case2() {
        if Self::is_file_exist(LARGE_FILE_PATH_CASE2) {
            return;
        }

        std::fs::create_dir_all(LARGE_FILE_DIR_CASE2).unwrap();

        let data = vec![1_u8; LARGE_FILE_SIZE];
        std::fs::write(LARGE_FILE_PATH_CASE2, data.as_slice()).unwrap();
    }

    pub fn create_8mib_file() {
        if Self::is_file_exist(TEST_8MIB_FILE_PATH) {
            return;
        }

        std::fs::create_dir_all(TEST_8MIB_FILE_DIR).unwrap();

        let data = vec![0_u8; TEST_FILE_SIZE_8MIB];
        std::fs::write(TEST_8MIB_FILE_PATH, data.as_slice()).unwrap();
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

    pub fn create_random_test_data_file(size_mb: usize, extra: i32) -> Result<()> {
        std::fs::create_dir_all(RANDOM_DATA_FILE_DIR).unwrap();

        let mut random_file = File::open(RANDOM_DATA_SEED_FILE)?;
        let mut random_data = vec![0; 1024];
        random_file.read_exact(&mut random_data)?;

        let output_path = Path::new(RANDOM_DATA_FILE_DIR).join(TEST_RANDOM_DATA_FILE_KEY);
        let mut output_file = File::create(output_path)?;

        for _ in 0..size_mb * 1024 {
            output_file.write_all(&random_data)?;
        }

        if extra > 0 {
            output_file.write_all(b"Z")?;
        } else if extra < 0 {
            let current_pos = output_file.seek(SeekFrom::End(0))?;
            output_file.set_len(current_pos - 1)?;
        }

        output_file.flush()?;

        return Ok(());
    }

    pub fn get_sha256_from_file(file_path: &str) -> String {
        let mut file = File::open(file_path).unwrap();
        let mut hasher = Sha256::new();
        let mut buffer = [0; 1024];

        loop {
            let bytes_read = file.read(&mut buffer).unwrap();
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
        }

        let hash_result = hasher.finalize();
        format!("{:x}", hash_result)
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
                SyncStatistics::ETagVerified { .. } => {
                    stats.e_tag_verified += 1;
                }
                SyncStatistics::ChecksumVerified { .. } => {
                    stats.checksum_verified += 1;
                }
                _ => {}
            }
        }

        stats
    }

    pub async fn put_bucket_policy_deny_get_object(&self, bucket: &str) {
        let policy = GET_OBJECT_DENY_BUCKET_POLICY.replace("{{ bucket }}", bucket);

        self.client
            .put_bucket_policy()
            .bucket(bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();
    }

    pub fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .or_else(|_| EnvFilter::try_new("dummy=trace"))
                    .unwrap(),
            )
            .try_init();
    }

    pub fn init_tracing_subscriber_for_report() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .or_else(|_| EnvFilter::try_new("s3sync=info"))
                    .unwrap(),
            )
            .try_init();
    }

    pub fn enable_not_found_dangerous_simulation() {
        std::env::set_var(
            NOT_FOUND_DANGEROUS_SIMULATION_ENV,
            NOT_FOUND_DANGEROUS_SIMULATION_ENV_ALLOW,
        );
    }

    pub fn disable_not_found_dangerous_simulation() {
        std::env::remove_var(NOT_FOUND_DANGEROUS_SIMULATION_ENV);
    }

    pub fn enable_cancel_dangerous_simulation() {
        std::env::set_var(
            CANCEL_DANGEROUS_SIMULATION_ENV,
            CANCEL_DANGEROUS_SIMULATION_ENV_ALLOW,
        );
    }

    pub fn disable_cancel_dangerous_simulation() {
        std::env::remove_var(CANCEL_DANGEROUS_SIMULATION_ENV);
    }
}
