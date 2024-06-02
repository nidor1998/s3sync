use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::primitives::DateTime;
use aws_sdk_s3::types::{ChecksumAlgorithm, DeleteMarkerEntry, Object, ObjectPart, ObjectVersion};
use sha1::{Digest, Sha1};
use zeroize_derive::{Zeroize, ZeroizeOnDrop};

pub mod async_callback;
pub mod error;
pub mod token;

pub const S3SYNC_ORIGIN_VERSION_ID_METADATA_KEY: &str = "s3sync_origin_version_id";
pub const S3SYNC_ORIGIN_LAST_MODIFIED_METADATA_KEY: &str = "s3sync_origin_last_modified";

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
        _ => {
            panic!("unknown algorithm")
        }
    }
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
        assert!(!S3syncObject::PackedVersions(PackedObjectVersions {
            key: "test".to_string(),
            packed_object_versions: vec![]
        })
        .is_delete_marker());
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

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
