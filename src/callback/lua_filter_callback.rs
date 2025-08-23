use crate::lua::lua_script_callback_engine::LuaScriptCallbackEngine;
use crate::types::S3syncObject;
use crate::types::filter_callback::FilterCallback;
use anyhow::Result;
use async_trait::async_trait;
use tracing::error;

pub struct LuaFilterCallback {
    lua: LuaScriptCallbackEngine,
}

impl LuaFilterCallback {
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
impl FilterCallback for LuaFilterCallback {
    async fn filter(&mut self, source_object: &S3syncObject) -> Result<bool> {
        let result = self.filter_by_lua(source_object).await;
        if let Err(err) = &result {
            error!("Lua script filter callback error: {}", err);
        }
        result
    }
}

impl LuaFilterCallback {
    async fn filter_by_lua(&mut self, source_object: &S3syncObject) -> Result<bool> {
        let source_object_lua = self.lua.get_engine().create_table()?;
        source_object_lua.set("key", source_object.key())?;
        source_object_lua.set("last_modified", source_object.last_modified().to_string())?;
        source_object_lua.set("version_id", source_object.version_id())?;
        source_object_lua.set("e_tag", source_object.e_tag())?;
        // Only the first checksum is set(s3sync designed to use only one checksum at a time)
        source_object_lua.set(
            "checksum_algorithm",
            source_object
                .checksum_algorithm()
                .map(|alg| alg.first().map(|alg| alg.to_string()).unwrap_or_default()),
        )?;
        source_object_lua.set(
            "checksum_type",
            source_object.checksum_type().map(|typ| typ.to_string()),
        )?;
        source_object_lua.set("is_latest", source_object.is_latest())?;
        source_object_lua.set("is_delete_marker", source_object.is_delete_marker())?;

        let func: mlua::Function = self.lua.get_engine().globals().get("filter")?;
        let result: bool = func.call_async(source_object_lua).await?;

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_callback() {
        let _callback = LuaFilterCallback::new(8 * 1024 * 1024, false, false);
        let _callback = LuaFilterCallback::new(8 * 1024 * 1024, true, false);
        let _callback = LuaFilterCallback::new(0, true, true);
    }
}
