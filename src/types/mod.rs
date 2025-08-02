use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::primitives::DateTime;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumType, DeleteMarkerEntry, Object, ObjectPart, ObjectVersion, Tag,
};
use sha1::{Digest, Sha1};
use zeroize_derive::{Zeroize, ZeroizeOnDrop};

pub mod async_callback;
pub mod error;
pub mod event_callback;
pub mod event_manager;
pub mod token;

pub const S3SYNC_ORIGIN_VERSION_ID_METADATA_KEY: &str = "s3sync_origin_version_id";
pub const S3SYNC_ORIGIN_LAST_MODIFIED_METADATA_KEY: &str = "s3sync_origin_last_modified";
pub const SYNC_REPORT_SUMMERY_NAME: &str = "REPORT_SUMMARY";
pub const SYNC_REPORT_RECORD_NAME: &str = "SYNC_STATUS";
pub const SYNC_REPORT_EXISTENCE_TYPE: &str = "EXISTENCE";
pub const SYNC_REPORT_ETAG_TYPE: &str = "ETAG";
pub const SYNC_REPORT_CHECKSUM_TYPE: &str = "CHECKSUM";
pub const SYNC_REPORT_METADATA_TYPE: &str = "METADATA";
pub const SYNC_REPORT_TAGGING_TYPE: &str = "TAGGING";
pub const SYNC_REPORT_CONTENT_DISPOSITION_METADATA_KEY: &str = "Content-Disposition";
pub const SYNC_REPORT_CONTENT_ENCODING_METADATA_KEY: &str = "Content-Encoding";
pub const SYNC_REPORT_CONTENT_LANGUAGE_METADATA_KEY: &str = "Content-Language";
pub const SYNC_REPORT_CONTENT_TYPE_METADATA_KEY: &str = "Content-Type";
pub const SYNC_REPORT_CACHE_CONTROL_METADATA_KEY: &str = "Cache-Control";
pub const SYNC_REPORT_EXPIRES_METADATA_KEY: &str = "Expires";
pub const SYNC_REPORT_WEBSITE_REDIRECT_METADATA_KEY: &str = "x-amz-website-redirect-location";
pub const SYNC_REPORT_USER_DEFINED_METADATA_KEY: &str = "x-amz-meta-";

pub const METADATA_SYNC_REPORT_LOG_NAME: &str = "METADATA_SYNC_STATUS";
pub const TAGGING_SYNC_REPORT_LOG_NAME: &str = "TAGGING_SYNC_STATUS";
pub const SYNC_STATUS_MATCHES: &str = "MATCHES";
pub const SYNC_STATUS_MISMATCH: &str = "MISMATCH";
pub const SYNC_STATUS_NOT_FOUND: &str = "NOT_FOUND";
pub const SYNC_STATUS_UNKNOWN: &str = "UNKNOWN";
pub(crate) const MINIMUM_CHUNKSIZE: usize = 5 * 1024 * 1024;

pub type Sha1Digest = [u8; 20];

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ObjectKey {
    KeyString(String),
    KeySHA1Digest(Sha1Digest),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectEntry {
    pub last_modified: DateTime,
    pub content_length: i64,
    pub e_tag: Option<String>,
}

pub type ObjectKeyMap = Arc<Mutex<HashMap<ObjectKey, ObjectEntry>>>;

pub type ObjectVersions = Vec<S3syncObject>;

#[derive(Debug, Clone, PartialEq)]
pub struct PackedObjectVersions {
    pub key: String,
    pub packed_object_versions: ObjectVersions,
}

#[derive(Debug, Clone, Default)]
pub struct SyncStatsReport {
    pub number_of_objects: usize,
    pub not_found: usize,
    pub etag_matches: usize,
    pub etag_mismatch: usize,
    pub etag_unknown: usize,
    pub checksum_matches: usize,
    pub checksum_mismatch: usize,
    pub checksum_unknown: usize,
    pub metadata_matches: usize,
    pub metadata_mismatch: usize,
    pub tagging_matches: usize,
    pub tagging_mismatch: usize,
}

impl SyncStatsReport {
    pub fn increment_number_of_objects(&mut self) {
        self.number_of_objects += 1;
    }
    pub fn increment_not_found(&mut self) {
        self.not_found += 1;
    }
    pub fn increment_etag_matches(&mut self) {
        self.etag_matches += 1;
    }
    pub fn increment_etag_mismatch(&mut self) {
        self.etag_mismatch += 1;
    }
    pub fn increment_etag_unknown(&mut self) {
        self.etag_unknown += 1;
    }
    pub fn increment_checksum_matches(&mut self) {
        self.checksum_matches += 1;
    }
    pub fn increment_checksum_mismatch(&mut self) {
        self.checksum_mismatch += 1;
    }
    pub fn increment_checksum_unknown(&mut self) {
        self.checksum_unknown += 1;
    }
    pub fn increment_metadata_matches(&mut self) {
        self.metadata_matches += 1;
    }
    pub fn increment_metadata_mismatch(&mut self) {
        self.metadata_mismatch += 1;
    }
    pub fn increment_tagging_matches(&mut self) {
        self.tagging_matches += 1;
    }
    pub fn increment_tagging_mismatch(&mut self) {
        self.tagging_mismatch += 1;
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum S3syncObject {
    NotVersioning(Object),
    Versioning(ObjectVersion),
    DeleteMarker(DeleteMarkerEntry),
    PackedVersions(PackedObjectVersions),
}

#[derive(Clone, Default)]
pub struct ObjectChecksum {
    pub key: String,
    pub version_id: Option<String>,
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    pub checksum_type: Option<ChecksumType>,
    pub object_parts: Option<Vec<ObjectPart>>,
    pub final_checksum: Option<String>,
}

pub fn pack_object_versions(key: &str, object_versions: &ObjectVersions) -> S3syncObject {
    S3syncObject::PackedVersions(PackedObjectVersions {
        key: key.to_string(),
        packed_object_versions: object_versions.clone(),
    })
}

pub fn unpack_object_versions(object: &S3syncObject) -> ObjectVersions {
    match &object {
        S3syncObject::PackedVersions(packed) => packed.packed_object_versions.clone(),
        _ => {
            panic!("not PackedVersions")
        }
    }
}

pub fn format_metadata(metadata: &HashMap<String, String>) -> String {
    let mut sorted_keys: Vec<&String> = metadata.keys().collect();
    sorted_keys.sort();

    sorted_keys
        .iter()
        .map(|key| {
            let value = urlencoding::encode(&metadata[*key]).to_string();
            format!("{key}={value}")
        })
        .collect::<Vec<String>>()
        .join(",")
}

pub fn format_tags(tags: &[Tag]) -> String {
    let mut tags = tags
        .iter()
        .map(|tag| (tag.key(), tag.value()))
        .collect::<Vec<_>>();

    tags.sort_by(|a, b| a.0.cmp(b.0));

    tags.iter()
        .map(|(key, value)| {
            let escaped_key = urlencoding::encode(key).to_string();
            let encoded_value = urlencoding::encode(value).to_string();
            format!("{escaped_key}={encoded_value}")
        })
        .collect::<Vec<String>>()
        .join("&")
}

impl S3syncObject {
    pub fn key(&self) -> &str {
        match &self {
            Self::Versioning(object) => object.key().unwrap(),
            Self::NotVersioning(object) => object.key().unwrap(),
            Self::DeleteMarker(maker) => maker.key().unwrap(),
            Self::PackedVersions(packed_object_versions) => &packed_object_versions.key,
        }
    }

    pub fn last_modified(&self) -> &DateTime {
        match &self {
            Self::Versioning(object) => object.last_modified().unwrap(),
            Self::NotVersioning(object) => object.last_modified().unwrap(),
            Self::DeleteMarker(maker) => maker.last_modified().unwrap(),
            Self::PackedVersions(_) => {
                panic!("PackedVersions doesn't have last_modified.")
            }
        }
    }

    pub fn size(&self) -> i64 {
        match &self {
            Self::Versioning(object) => object.size().unwrap(),
            Self::NotVersioning(object) => object.size().unwrap(),
            _ => panic!("doesn't have size."),
        }
    }

    pub fn version_id(&self) -> Option<&str> {
        match &self {
            Self::Versioning(object) => object.version_id(),
            Self::NotVersioning(_) => None,
            _ => panic!("unsupported."),
        }
    }

    pub fn e_tag(&self) -> Option<&str> {
        match &self {
            Self::Versioning(object) => object.e_tag(),
            Self::NotVersioning(object) => object.e_tag(),
            _ => panic!("doesn't have ETag."),
        }
    }

    pub fn checksum_algorithm(&self) -> Option<&[ChecksumAlgorithm]> {
        match &self {
            Self::Versioning(object) => {
                if object.checksum_algorithm().is_empty() {
                    None
                } else {
                    Some(object.checksum_algorithm())
                }
            }
            Self::NotVersioning(object) => {
                if object.checksum_algorithm().is_empty() {
                    None
                } else {
                    Some(object.checksum_algorithm())
                }
            }
            _ => panic!("doesn't have checksum_algorithm."),
        }
    }

    pub fn checksum_type(&self) -> Option<&ChecksumType> {
        match &self {
            Self::Versioning(object) => object.checksum_type(),
            Self::NotVersioning(object) => object.checksum_type(),
            Self::DeleteMarker(_) => panic!("DeleteMarker doesn't have checksum_type."),
            Self::PackedVersions(_) => {
                panic!("PackedVersions doesn't have checksum_type.")
            }
        }
    }

    pub fn is_latest(&self) -> bool {
        match &self {
            Self::Versioning(object) => object.is_latest().unwrap(),
            Self::DeleteMarker(maker) => maker.is_latest().unwrap(),
            _ => panic!("doesn't have is_latest."),
        }
    }

    pub fn is_delete_marker(&self) -> bool {
        match &self {
            Self::Versioning(_) => false,
            Self::NotVersioning(_) => false,
            Self::DeleteMarker(_) => true,
            Self::PackedVersions(_) => false,
        }
    }

    pub fn clone_non_versioning_object_with_key(object: &Object, key: &str) -> Self {
        S3syncObject::NotVersioning(clone_object_with_key(object, key))
    }

    pub fn clone_versioning_object_with_key(object: &ObjectVersion, key: &str) -> Self {
        S3syncObject::Versioning(clone_object_version_with_key(object, key))
    }

    pub fn clone_delete_marker_with_key(delete_marker: &DeleteMarkerEntry, key: &str) -> Self {
        S3syncObject::DeleteMarker(
            DeleteMarkerEntry::builder()
                .key(key)
                .version_id(delete_marker.version_id().unwrap().to_string())
                .is_latest(delete_marker.is_latest().unwrap())
                .set_last_modified(delete_marker.last_modified().cloned())
                .set_owner(delete_marker.owner().cloned())
                .build(),
        )
    }
}

pub fn sha1_digest_from_key(key: &str) -> Sha1Digest {
    let digest = Sha1::digest(key);
    TryInto::<Sha1Digest>::try_into(digest.as_slice()).unwrap()
}

pub fn clone_object_with_key(object: &Object, key: &str) -> Object {
    let checksum_algorithm = if object.checksum_algorithm().is_empty() {
        None
    } else {
        Some(
            object
                .checksum_algorithm()
                .iter()
                .map(|checksum_algorithm| checksum_algorithm.to_owned())
                .collect(),
        )
    };

    Object::builder()
        .key(key)
        .size(object.size().unwrap())
        .set_last_modified(object.last_modified().cloned())
        .set_e_tag(object.e_tag().map(|e_tag| e_tag.to_string()))
        .set_owner(object.owner().cloned())
        .set_storage_class(object.storage_class().cloned())
        .set_checksum_algorithm(checksum_algorithm)
        .set_checksum_type(object.checksum_type().cloned())
        .build()
}

pub fn clone_object_version_with_key(object: &ObjectVersion, key: &str) -> ObjectVersion {
    let checksum_algorithm = if object.checksum_algorithm().is_empty() {
        None
    } else {
        Some(
            object
                .checksum_algorithm()
                .iter()
                .map(|checksum_algorithm| checksum_algorithm.to_owned())
                .collect(),
        )
    };

    ObjectVersion::builder()
        .key(key)
        .version_id(object.version_id().unwrap().to_string())
        .is_latest(object.is_latest().unwrap())
        .size(object.size().unwrap())
        .set_last_modified(object.last_modified().cloned())
        .set_e_tag(object.e_tag().map(|e_tag| e_tag.to_string()))
        .set_owner(object.owner().cloned())
        .set_storage_class(object.storage_class().cloned())
        .set_checksum_algorithm(checksum_algorithm)
        .set_checksum_type(object.checksum_type().cloned())
        .build()
}

pub fn get_additional_checksum(
    get_object_output: &GetObjectOutput,
    checksum_algorithm: Option<ChecksumAlgorithm>,
) -> Option<String> {
    checksum_algorithm.as_ref()?;

    match checksum_algorithm.unwrap() {
        ChecksumAlgorithm::Sha256 => get_object_output
            .checksum_sha256()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Sha1 => get_object_output
            .checksum_sha1()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc32 => get_object_output
            .checksum_crc32()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc32C => get_object_output
            .checksum_crc32_c()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc64Nvme => get_object_output
            .checksum_crc64_nvme()
            .map(|checksum| checksum.to_string()),
        _ => {
            panic!("unknown algorithm")
        }
    }
}

pub fn get_additional_checksum_with_head_object(
    head_object_output: &HeadObjectOutput,
    checksum_algorithm: Option<ChecksumAlgorithm>,
) -> Option<String> {
    checksum_algorithm.as_ref()?;

    match checksum_algorithm.unwrap() {
        ChecksumAlgorithm::Sha256 => head_object_output
            .checksum_sha256()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Sha1 => head_object_output
            .checksum_sha1()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc32 => head_object_output
            .checksum_crc32()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc32C => head_object_output
            .checksum_crc32_c()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc64Nvme => head_object_output
            .checksum_crc64_nvme()
            .map(|checksum| checksum.to_string()),
        _ => {
            panic!("unknown algorithm")
        }
    }
}

pub fn is_full_object_checksum(checksum: &Option<String>) -> bool {
    if checksum.is_none() {
        return false;
    }

    // As of February 2, 2025, Amazon S3 GetObject does not return ChecksumType::Composite.
    // So, we can't get the checksum type from GetObjectOutput and decide where checksum has '-' or not.
    let find_result = checksum.as_ref().unwrap().find('-');
    find_result.is_none()
}

#[derive(Debug, PartialEq)]
pub enum SyncStatistics {
    SyncBytes(u64),
    SyncComplete { key: String },
    SyncSkip { key: String },
    SyncDelete { key: String },
    SyncError { key: String },
    SyncWarning { key: String },
    ETagVerified { key: String },
    ChecksumVerified { key: String },
}

#[derive(Debug, Clone)]
pub enum StoragePath {
    S3 { bucket: String, prefix: String },
    Local(PathBuf),
}

#[derive(Debug, Clone)]
pub struct ClientConfigLocation {
    pub aws_config_file: Option<PathBuf>,
    pub aws_shared_credentials_file: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub enum S3Credentials {
    Profile(String),
    Credentials { access_keys: AccessKeys },
    FromEnvironment,
}

#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct AccessKeys {
    pub access_key: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl Debug for AccessKeys {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut keys = f.debug_struct("AccessKeys");
        let session_token = self
            .session_token
            .as_ref()
            .map_or("None", |_| "** redacted **");
        keys.field("access_key", &self.access_key)
            .field("secret_access_key", &"** redacted **")
            .field("session_token", &session_token);
        keys.finish()
    }
}

#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct SseKmsKeyId {
    pub id: Option<String>,
}

impl Debug for SseKmsKeyId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut keys = f.debug_struct("SseKmsKeyId");
        let sse_kms_key_id = self.id.as_ref().map_or("None", |_| "** redacted **");
        keys.field("sse_kms_key_id", &sse_kms_key_id);
        keys.finish()
    }
}

#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct SseCustomerKey {
    pub key: Option<String>,
}

impl Debug for SseCustomerKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut keys = f.debug_struct("SseCustomerKey");
        let sse_c_key = self.key.as_ref().map_or("None", |_| "** redacted **");
        keys.field("key", &sse_c_key);
        keys.finish()
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_s3::types::{
        ChecksumAlgorithm, ObjectStorageClass, ObjectVersionStorageClass, Owner,
    };

    use super::*;

    #[test]
    fn clone_non_versioning_object_with_key_test() {
        init_dummy_tracing_subscriber();

        let source_object = Object::builder()
            .key("source")
            .size(1)
            .e_tag("my-etag")
            .storage_class(ObjectStorageClass::Glacier)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_type(ChecksumType::FullObject)
            .owner(
                Owner::builder()
                    .id("test_id")
                    .display_name("test_name")
                    .build(),
            )
            .last_modified(DateTime::from_secs(777))
            .build();

        let expected_object = S3syncObject::NotVersioning(
            Object::builder()
                .key("cloned")
                .size(1)
                .e_tag("my-etag")
                .storage_class(ObjectStorageClass::Glacier)
                .checksum_algorithm(ChecksumAlgorithm::Sha256)
                .checksum_type(ChecksumType::FullObject)
                .owner(
                    Owner::builder()
                        .id("test_id")
                        .display_name("test_name")
                        .build(),
                )
                .last_modified(DateTime::from_secs(777))
                .build(),
        );

        let cloned_object =
            S3syncObject::clone_non_versioning_object_with_key(&source_object, "cloned");

        assert_eq!(cloned_object, expected_object);
    }
    #[test]
    fn clone_non_versioning_object_with_key_test_no_checksum() {
        init_dummy_tracing_subscriber();

        let source_object = Object::builder()
            .key("source")
            .size(1)
            .e_tag("my-etag")
            .storage_class(ObjectStorageClass::Glacier)
            .owner(
                Owner::builder()
                    .id("test_id")
                    .display_name("test_name")
                    .build(),
            )
            .last_modified(DateTime::from_secs(777))
            .build();

        let expected_object = S3syncObject::NotVersioning(
            Object::builder()
                .key("cloned")
                .size(1)
                .e_tag("my-etag")
                .storage_class(ObjectStorageClass::Glacier)
                .owner(
                    Owner::builder()
                        .id("test_id")
                        .display_name("test_name")
                        .build(),
                )
                .last_modified(DateTime::from_secs(777))
                .build(),
        );

        let cloned_object =
            S3syncObject::clone_non_versioning_object_with_key(&source_object, "cloned");

        assert_eq!(cloned_object, expected_object);
    }

    #[test]
    fn clone_versioning_object_with_key_test() {
        init_dummy_tracing_subscriber();

        let source_object = ObjectVersion::builder()
            .key("source")
            .version_id("version1".to_string())
            .is_latest(false)
            .size(1)
            .e_tag("my-etag")
            .storage_class(ObjectVersionStorageClass::Standard)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_type(ChecksumType::FullObject)
            .owner(
                Owner::builder()
                    .id("test_id")
                    .display_name("test_name")
                    .build(),
            )
            .last_modified(DateTime::from_secs(777))
            .build();

        let expected_object = S3syncObject::Versioning(
            ObjectVersion::builder()
                .key("cloned")
                .version_id("version1".to_string())
                .is_latest(false)
                .size(1)
                .e_tag("my-etag")
                .storage_class(ObjectVersionStorageClass::Standard)
                .checksum_algorithm(ChecksumAlgorithm::Sha256)
                .checksum_type(ChecksumType::FullObject)
                .owner(
                    Owner::builder()
                        .id("test_id")
                        .display_name("test_name")
                        .build(),
                )
                .last_modified(DateTime::from_secs(777))
                .build(),
        );

        let cloned_object =
            S3syncObject::clone_versioning_object_with_key(&source_object, "cloned");

        assert_eq!(cloned_object, expected_object);
    }

    #[test]
    fn clone_versioning_object_with_key_test_no_checksum() {
        init_dummy_tracing_subscriber();

        let source_object = ObjectVersion::builder()
            .key("source")
            .version_id("version1".to_string())
            .is_latest(false)
            .size(1)
            .e_tag("my-etag")
            .storage_class(ObjectVersionStorageClass::Standard)
            .owner(
                Owner::builder()
                    .id("test_id")
                    .display_name("test_name")
                    .build(),
            )
            .last_modified(DateTime::from_secs(777))
            .build();

        let expected_object = S3syncObject::Versioning(
            ObjectVersion::builder()
                .key("cloned")
                .version_id("version1".to_string())
                .is_latest(false)
                .size(1)
                .e_tag("my-etag")
                .storage_class(ObjectVersionStorageClass::Standard)
                .owner(
                    Owner::builder()
                        .id("test_id")
                        .display_name("test_name")
                        .build(),
                )
                .last_modified(DateTime::from_secs(777))
                .build(),
        );

        let cloned_object =
            S3syncObject::clone_versioning_object_with_key(&source_object, "cloned");

        assert_eq!(cloned_object, expected_object);
    }

    #[test]
    fn versioning_object_getter_test() {
        init_dummy_tracing_subscriber();

        let versioning_object = ObjectVersion::builder()
            .key("source")
            .version_id("version1".to_string())
            .is_latest(false)
            .size(1)
            .e_tag("my-etag")
            .storage_class(ObjectVersionStorageClass::Standard)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_type(ChecksumType::FullObject)
            .owner(
                Owner::builder()
                    .id("test_id")
                    .display_name("test_name")
                    .build(),
            )
            .last_modified(DateTime::from_secs(777))
            .build();

        let s3sync_object =
            S3syncObject::clone_versioning_object_with_key(&versioning_object, "cloned");

        assert_eq!(s3sync_object.key(), "cloned");
        assert_eq!(s3sync_object.version_id().unwrap(), "version1");
        assert!(!s3sync_object.is_latest());
        assert_eq!(s3sync_object.size(), 1);
        assert_eq!(s3sync_object.e_tag().unwrap(), "my-etag");
        assert_eq!(
            s3sync_object.checksum_type().unwrap(),
            &ChecksumType::FullObject
        );
        assert_eq!(
            s3sync_object.checksum_algorithm().unwrap(),
            &[ChecksumAlgorithm::Sha256]
        );
    }

    #[test]
    fn non_versioning_object_getter_test() {
        init_dummy_tracing_subscriber();

        let versioning_object = Object::builder()
            .key("source")
            .size(1)
            .e_tag("my-etag")
            .storage_class(ObjectStorageClass::Standard)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_type(ChecksumType::FullObject)
            .owner(
                Owner::builder()
                    .id("test_id")
                    .display_name("test_name")
                    .build(),
            )
            .last_modified(DateTime::from_secs(777))
            .build();

        let s3sync_object =
            S3syncObject::clone_non_versioning_object_with_key(&versioning_object, "cloned");

        assert_eq!(s3sync_object.key(), "cloned");
        assert_eq!(s3sync_object.size(), 1);
        assert_eq!(s3sync_object.e_tag().unwrap(), "my-etag");
        assert_eq!(
            s3sync_object.checksum_type().unwrap(),
            &ChecksumType::FullObject
        );
        assert_eq!(
            s3sync_object.checksum_algorithm().unwrap(),
            &[ChecksumAlgorithm::Sha256]
        );
    }

    #[test]
    fn versioning_object_getter_test_no_checksum() {
        init_dummy_tracing_subscriber();

        let versioning_object = ObjectVersion::builder()
            .key("source")
            .version_id("version1".to_string())
            .is_latest(false)
            .size(1)
            .e_tag("my-etag")
            .storage_class(ObjectVersionStorageClass::Standard)
            .owner(
                Owner::builder()
                    .id("test_id")
                    .display_name("test_name")
                    .build(),
            )
            .last_modified(DateTime::from_secs(777))
            .build();

        let s3sync_object =
            S3syncObject::clone_versioning_object_with_key(&versioning_object, "cloned");

        assert_eq!(s3sync_object.key(), "cloned");
        assert_eq!(s3sync_object.version_id().unwrap(), "version1");
        assert!(!s3sync_object.is_latest());
        assert_eq!(s3sync_object.size(), 1);
        assert_eq!(s3sync_object.e_tag().unwrap(), "my-etag");
        assert!(s3sync_object.checksum_algorithm().is_none());
    }

    #[test]
    fn clone_delete_marker_with_key_test() {
        init_dummy_tracing_subscriber();

        let source_object = DeleteMarkerEntry::builder()
            .key("source")
            .version_id("version1".to_string())
            .is_latest(true)
            .owner(
                Owner::builder()
                    .id("test_id")
                    .display_name("test_name")
                    .build(),
            )
            .last_modified(DateTime::from_secs(777))
            .build();

        let expected_object = S3syncObject::DeleteMarker(
            DeleteMarkerEntry::builder()
                .key("cloned")
                .version_id("version1".to_string())
                .is_latest(true)
                .owner(
                    Owner::builder()
                        .id("test_id")
                        .display_name("test_name")
                        .build(),
                )
                .last_modified(DateTime::from_secs(777))
                .build(),
        );

        let cloned_object = S3syncObject::clone_delete_marker_with_key(&source_object, "cloned");

        assert_eq!(cloned_object, expected_object);
    }

    #[test]
    fn debug_print_access_keys() {
        init_dummy_tracing_subscriber();

        let access_keys = AccessKeys {
            access_key: "access_key".to_string(),
            secret_access_key: "secret_access_key".to_string(),
            session_token: Some("session_token".to_string()),
        };
        let debug_string = format!("{access_keys:?}");

        assert!(debug_string.contains("secret_access_key: \"** redacted **\""));
        assert!(debug_string.contains("session_token: \"** redacted **\""));
    }

    #[test]
    fn unpack_object_versions_test() {
        init_dummy_tracing_subscriber();

        let object_versions = vec![
            S3syncObject::Versioning(ObjectVersion::builder().build()),
            S3syncObject::Versioning(ObjectVersion::builder().build()),
            S3syncObject::Versioning(ObjectVersion::builder().build()),
            S3syncObject::Versioning(ObjectVersion::builder().build()),
            S3syncObject::Versioning(ObjectVersion::builder().build()),
        ];

        let packed = S3syncObject::PackedVersions(PackedObjectVersions {
            key: "test".to_string(),
            packed_object_versions: object_versions.clone(),
        });

        let unpacked = unpack_object_versions(&packed);

        assert_eq!(object_versions, unpacked);
    }

    #[test]
    #[should_panic]
    fn unpack_object_versions_test_panic() {
        init_dummy_tracing_subscriber();

        unpack_object_versions(&S3syncObject::Versioning(ObjectVersion::builder().build()));
    }

    #[test]
    fn is_delete_marker_test() {
        init_dummy_tracing_subscriber();

        assert!(
            S3syncObject::DeleteMarker(DeleteMarkerEntry::builder().build()).is_delete_marker()
        );

        assert!(!S3syncObject::NotVersioning(Object::builder().build()).is_delete_marker());
        assert!(!S3syncObject::Versioning(ObjectVersion::builder().build()).is_delete_marker());
        assert!(
            !S3syncObject::PackedVersions(PackedObjectVersions {
                key: "test".to_string(),
                packed_object_versions: vec![]
            })
            .is_delete_marker()
        );
    }

    #[test]
    fn test_sse_kms_keyid_debug_string() {
        let secret = SseKmsKeyId {
            id: Some("secret".to_string()),
        };

        let debug_string = format!("{:?}", secret);
        assert!(debug_string.contains("redacted"))
    }

    #[test]
    fn test_sse_customer_key_debug_string() {
        let secret = SseCustomerKey {
            key: Some("secret".to_string()),
        };

        let debug_string = format!("{:?}", secret);
        assert!(debug_string.contains("redacted"))
    }

    #[test]
    fn test_format_metadata() {
        let metadata = HashMap::from([
            ("key3".to_string(), "value3".to_string()),
            ("key1".to_string(), "value1".to_string()),
            ("abc".to_string(), "☃".to_string()),
            ("xyz_abc".to_string(), "value_xyz".to_string()),
            ("key_comma".to_string(), "value,comma".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]);

        let formatted = format_metadata(&metadata);
        assert_eq!(
            formatted,
            "abc=%E2%98%83,key1=value1,key2=value2,key3=value3,key_comma=value%2Ccomma,xyz_abc=value_xyz"
        );
    }

    #[test]
    fn test_format_tags() {
        let tags = vec![
            Tag::builder().key("key3").value("value3").build().unwrap(),
            Tag::builder().key("key1").value("value1").build().unwrap(),
            Tag::builder().key("abc").value("☃").build().unwrap(),
            Tag::builder().key("☃").value("value").build().unwrap(),
            Tag::builder()
                .key("xyz_abc")
                .value("value_xyz")
                .build()
                .unwrap(),
            Tag::builder()
                .key("key_comma")
                .value("value,comma")
                .build()
                .unwrap(),
            Tag::builder()
                .key("key_and")
                .value("value&and")
                .build()
                .unwrap(),
            Tag::builder().key("key2").value("value2").build().unwrap(),
        ];

        let formatted = format_tags(tags.as_slice());
        assert_eq!(
            formatted,
            "abc=%E2%98%83&key1=value1&key2=value2&key3=value3&key_and=value%26and&key_comma=value%2Ccomma&xyz_abc=value_xyz&%E2%98%83=value"
        );
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
