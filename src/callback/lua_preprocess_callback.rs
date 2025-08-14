use crate::lua::lua_script_preprocess_callback_engine::LuaScriptCallbackEngine;
use crate::types::preprocess_callback::{PreprocessCallback, UploadMetadata};
use crate::types::preprocess_callback::{PreprocessError, is_callback_cancelled};
use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::primitives::DateTimeFormat;
use aws_sdk_s3::types::{ObjectCannedAcl, RequestPayer, StorageClass};
use aws_smithy_types::DateTime;
use std::collections::HashMap;
use tracing::{error, trace};

pub struct LuaPreprocessCallback {
    lua: LuaScriptCallbackEngine,
}

impl LuaPreprocessCallback {
    #[allow(clippy::new_without_default)]
    pub fn new(memory_limit: usize, allow_lua_os_library: bool, unsafe_lua: bool) -> Self {
        let lua = if unsafe_lua {
            LuaScriptCallbackEngine::unsafe_new(memory_limit)
        } else if allow_lua_os_library {
            LuaScriptCallbackEngine::new(memory_limit)
        } else {
            LuaScriptCallbackEngine::new_without_os_io_libs(memory_limit)
        };

        Self { lua }
    }

    pub fn load_and_compile(&mut self, script_path: &str) -> Result<()> {
        let lua_script = std::fs::read(script_path)?;
        self.lua.load_and_compile(&String::from_utf8(lua_script)?)
    }
}

#[async_trait]
impl PreprocessCallback for LuaPreprocessCallback {
    async fn preprocess_before_upload(
        &mut self,
        key: &str,                            // The key of the object being uploaded
        source_object: &GetObjectOutput,      // The source object being uploaded(read only)
        upload_metadata: &mut UploadMetadata, // The metadata for the upload, which can be modified
    ) -> Result<()> {
        let result = self
            .preprocess_before_upload_by_lua(key, source_object, upload_metadata)
            .await;
        if let Err(err) = &result {
            if !is_callback_cancelled(err) {
                error!("Lua script preprocess callback error: {}", err);
            }
        }

        result
    }
}

impl LuaPreprocessCallback {
    // skipcq: RS-R1000
    async fn preprocess_before_upload_by_lua(
        &mut self,
        key: &str,                            // The key of the object being uploaded
        source_object: &GetObjectOutput,      // The source object being uploaded(read only)
        upload_metadata: &mut UploadMetadata, // The metadata for the upload, which can be modified
    ) -> Result<()> {
        let source_object_lua = self.lua.get_engine().create_table()?;

        source_object_lua.set("key", key)?;
        source_object_lua.set("accept_ranges", source_object.accept_ranges.clone())?;
        source_object_lua.set("bucket_key_enabled", source_object.bucket_key_enabled)?;
        source_object_lua.set("cache_control", source_object.cache_control.clone())?;
        source_object_lua.set("checksum_crc32", source_object.checksum_crc32.clone())?;
        source_object_lua.set("checksum_crc32_c", source_object.checksum_crc32_c.clone())?;
        source_object_lua.set(
            "checksum_crc64_nvme",
            source_object.checksum_crc64_nvme.clone(),
        )?;
        source_object_lua.set("checksum_sha1", source_object.checksum_sha1.clone())?;
        source_object_lua.set("checksum_sha256", source_object.checksum_sha256.clone())?;
        source_object_lua.set(
            "checksum_type",
            source_object
                .checksum_type
                .clone()
                .map(|checksum_type| checksum_type.to_string()),
        )?;
        source_object_lua.set(
            "content_disposition",
            source_object.content_disposition.clone(),
        )?;
        source_object_lua.set("content_encoding", source_object.content_encoding.clone())?;
        source_object_lua.set("content_language", source_object.content_language.clone())?;
        source_object_lua.set("content_length", source_object.content_length)?;
        source_object_lua.set("content_range", source_object.content_range.clone())?;
        source_object_lua.set("content_type", source_object.content_type.clone())?;
        source_object_lua.set("e_tag", source_object.e_tag.clone())?;
        source_object_lua.set("expires_string", source_object.expires_string.clone())?;
        source_object_lua.set(
            "last_modified",
            source_object
                .last_modified
                .map(|last_modified| last_modified.to_string()),
        )?;
        let metadata = self.lua.get_engine().create_table()?;
        for (key, value) in source_object
            .metadata
            .clone()
            .unwrap_or_default()
            .into_iter()
        {
            metadata.set(key, value)?;
        }
        source_object_lua.set("metadata", metadata)?;
        source_object_lua.set("missing_meta", source_object.missing_meta)?;
        source_object_lua.set(
            "object_lock_legal_hold_status",
            source_object
                .object_lock_legal_hold_status
                .clone()
                .map(|status| status.to_string()),
        )?;
        source_object_lua.set(
            "object_lock_mode",
            source_object
                .object_lock_mode
                .clone()
                .map(|mode| mode.to_string()),
        )?;
        source_object_lua.set(
            "object_lock_retain_until_date",
            source_object
                .object_lock_retain_until_date
                .map(|date| date.to_string()),
        )?;
        source_object_lua.set("parts_count", source_object.parts_count)?;
        source_object_lua.set(
            "replication_status",
            source_object
                .replication_status
                .clone()
                .map(|status| status.to_string()),
        )?;
        source_object_lua.set(
            "request_charged",
            source_object
                .request_charged
                .clone()
                .map(|payer| payer.to_string()),
        )?;
        source_object_lua.set("restore", source_object.restore.clone())?;
        source_object_lua.set(
            "server_side_encryption",
            source_object
                .server_side_encryption
                .clone()
                .map(|encryption| encryption.to_string()),
        )?;
        source_object_lua.set(
            "sse_customer_algorithm",
            source_object.sse_customer_algorithm.clone(),
        )?;
        source_object_lua.set(
            "sse_customer_key_md5",
            source_object.sse_customer_key_md5.clone(),
        )?;
        source_object_lua.set("ssekms_key_id", source_object.ssekms_key_id.clone())?;
        source_object_lua.set(
            "storage_class",
            source_object
                .storage_class
                .clone()
                .map(|class| class.to_string()),
        )?;
        source_object_lua.set("tag_count", source_object.tag_count)?;
        source_object_lua.set("version_id", source_object.version_id.clone())?;
        source_object_lua.set(
            "website_redirect_location",
            source_object.website_redirect_location.clone(),
        )?;

        let upload_metadata_lua = self.lua.get_engine().create_table()?;
        upload_metadata_lua.set(
            "acl",
            upload_metadata.acl.clone().map(|acl| acl.to_string()),
        )?;
        upload_metadata_lua.set("cache_control", upload_metadata.cache_control.clone())?;
        upload_metadata_lua.set(
            "content_disposition",
            upload_metadata.content_disposition.clone(),
        )?;
        upload_metadata_lua.set("content_encoding", upload_metadata.content_encoding.clone())?;
        upload_metadata_lua.set("content_language", upload_metadata.content_language.clone())?;
        upload_metadata_lua.set("content_type", upload_metadata.content_type.clone())?;
        upload_metadata_lua.set(
            "expires_string",
            upload_metadata.expires.map(|expires| expires.to_string()),
        )?;
        let new_metadata = self.lua.get_engine().create_table()?;
        for (key, value) in upload_metadata
            .metadata
            .clone()
            .unwrap_or_default()
            .into_iter()
        {
            new_metadata.set(key, value)?;
        }
        upload_metadata_lua.set("metadata", new_metadata)?;
        upload_metadata_lua.set(
            "request_payer",
            upload_metadata
                .request_payer
                .clone()
                .map(|payer| payer.to_string()),
        )?;
        upload_metadata_lua.set(
            "storage_class",
            upload_metadata
                .storage_class
                .clone()
                .map(|class| class.to_string()),
        )?;
        upload_metadata_lua.set(
            "website_redirect_location",
            upload_metadata.website_redirect_location.clone(),
        )?;
        upload_metadata_lua.set("tagging", upload_metadata.tagging.clone())?;

        let func: mlua::Function = self.lua.get_engine().globals().get("preprocess_callback")?;
        let (result, modified_update_metadata): (bool, mlua::Table) = func
            .call_async((source_object_lua, upload_metadata_lua))
            .await?;

        let acl: Option<ObjectCannedAcl> =
            // skipcq: RS-W1006
            match modified_update_metadata.get::<Option<String>>("acl") {
                Ok(Some(acl_str)) => acl_str
                    .parse::<ObjectCannedAcl>()
                    .context("acl parse error")?
                    .into(),
                _ => None,
            };
        if let Some(acl) = &acl {
            // skipcq: RS-W1006
            #[allow(clippy::single_match)]
            match acl {
                #[allow(deprecated)]
                ObjectCannedAcl::Unknown(_) => {
                    error!("Lua preprocess callback returned an unknown ACL: {:?}", acl);
                    return Err(anyhow::Error::from(PreprocessError::Other(
                        "Unknown ACL".to_string(),
                    )));
                }
                _ => {}
            }
        }
        upload_metadata.acl = acl;
        upload_metadata.cache_control = modified_update_metadata
            .get("cache_control")
            .unwrap_or_default();
        upload_metadata.content_disposition = modified_update_metadata
            .get("content_disposition")
            .unwrap_or_default();
        upload_metadata.content_encoding = modified_update_metadata
            .get("content_encoding")
            .unwrap_or_default();
        upload_metadata.content_language = modified_update_metadata
            .get("content_language")
            .unwrap_or_default();
        upload_metadata.content_type = modified_update_metadata
            .get("content_type")
            .unwrap_or_default();

        let expires: Option<DateTime> =
            match modified_update_metadata.get::<Option<String>>("expires_string") {
                Ok(Some(expires_str)) => Some(
                    DateTime::from_str(&expires_str, DateTimeFormat::DateTime)
                        .context("expires_string parse error")?,
                ),
                _ => None,
            };
        upload_metadata.expires = expires;
        let metadata_lua: Option<mlua::Table> =
            modified_update_metadata.get("metadata").unwrap_or_default();
        if let Some(metadata_table) = metadata_lua {
            let mut metadata: HashMap<String, String> = HashMap::new();
            for pair in metadata_table.pairs::<String, String>() {
                let (key, value) = pair?;
                metadata.insert(key, value);
            }
            upload_metadata.metadata = Some(metadata);
        } else {
            upload_metadata.metadata = None;
        }

        let request_payer: Option<RequestPayer> =
            match modified_update_metadata.get::<Option<String>>("request_payer") {
                Ok(Some(payer_str)) => Some(
                    payer_str
                        .parse::<RequestPayer>()
                        .context("request_payer parse error")?,
                ),
                _ => None,
            };
        if let Some(request_payer) = &request_payer {
            // skipcq: RS-W1006
            #[allow(clippy::single_match)]
            match request_payer {
                #[allow(deprecated)]
                RequestPayer::Unknown(_) => {
                    error!(
                        "Lua preprocess callback returned an unknown RequestPayer: {:?}",
                        request_payer
                    );
                    return Err(anyhow::Error::from(PreprocessError::Other(
                        "Unknown RequestPayer".to_string(),
                    )));
                }
                _ => {}
            }
        }
        upload_metadata.request_payer = request_payer;
        let storage_class: Option<StorageClass> =
            match modified_update_metadata.get::<Option<String>>("storage_class") {
                Ok(Some(class_str)) => Some(
                    class_str
                        .parse::<StorageClass>()
                        .context("storage_class parse error")?,
                ),
                _ => None,
            };
        if let Some(storage_class) = &storage_class {
            // skipcq: RS-W1006
            #[allow(clippy::single_match)]
            match storage_class {
                #[allow(deprecated)]
                StorageClass::Unknown(_) => {
                    error!(
                        "Lua preprocess callback returned an unknown StorageClass: {:?}",
                        storage_class
                    );
                    return Err(anyhow::Error::from(PreprocessError::Other(
                        "Unknown StorageClass".to_string(),
                    )));
                }
                _ => {}
            }
        }
        upload_metadata.storage_class = storage_class;

        upload_metadata.website_redirect_location = modified_update_metadata
            .get("website_redirect_location")
            .unwrap_or_default();
        upload_metadata.tagging = modified_update_metadata.get("tagging").unwrap_or_default();

        trace!(
            "lua preprocess_callback upload_metadata: {:?}",
            upload_metadata
        );
        trace!("lua preprocess_callback result: {:?}", result);

        if !result {
            return Err(anyhow::Error::from(PreprocessError::Cancelled));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_callback() {
        let _callback = LuaPreprocessCallback::new(8 * 1024 * 1024, false, false);
        let _callback = LuaPreprocessCallback::new(8 * 1024 * 1024, true, false);
        let _callback = LuaPreprocessCallback::new(0, true, true);
    }
}
