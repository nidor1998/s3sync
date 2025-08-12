use crate::lua::lua_script_preprocess_callback_engine::LuaScriptCallbackEngine;
use crate::types::event_callback::{EventCallback, EventData};
use anyhow::Result;
use async_trait::async_trait;
use tracing::warn;

pub struct LuaEventCallback {
    lua: LuaScriptCallbackEngine,
}

impl LuaEventCallback {
    #[allow(clippy::new_without_default)]
    pub fn new(memory_limit: usize, allow_lua_os_library: bool, unsafe_lua: bool) -> Self {
        let lua = if unsafe_lua {
            LuaScriptCallbackEngine::unsafe_new(memory_limit)
        } else if allow_lua_os_library {
            LuaScriptCallbackEngine::new(memory_limit)
        } else {
            LuaScriptCallbackEngine::new_without_os(memory_limit)
        };

        Self { lua }
    }

    pub async fn load_and_compile(&mut self, script_path: &str) -> Result<()> {
        let lua_script = tokio::fs::read(script_path).await?;
        self.lua
            .load_and_compile(&String::from_utf8(lua_script)?)
            .await
    }
}

#[async_trait]
impl EventCallback for LuaEventCallback {
    async fn on_event(&mut self, event_data: EventData) {
        let event_data_lua = self.lua.get_engine().create_table().unwrap();
        event_data_lua
            .set("event_type", event_data.event_type.bits())
            .unwrap();
        event_data_lua.set("key", event_data.key.clone()).unwrap();
        event_data_lua
            .set("source_version_id", event_data.source_version_id.clone())
            .unwrap();
        event_data_lua
            .set("target_version_id", event_data.target_version_id.clone())
            .unwrap();
        event_data_lua
            .set(
                "source_last_modified",
                event_data.source_last_modified.map(|dt| dt.to_string()),
            )
            .unwrap();
        event_data_lua
            .set(
                "target_last_modified",
                event_data.target_last_modified.map(|dt| dt.to_string()),
            )
            .unwrap();
        event_data_lua
            .set("source_size", event_data.source_size)
            .unwrap();
        event_data_lua
            .set("target_size", event_data.target_size)
            .unwrap();
        event_data_lua
            .set(
                "checksum_algorithm",
                event_data.checksum_algorithm.map(|alg| alg.to_string()),
            )
            .unwrap();
        event_data_lua
            .set("source_checksum", event_data.source_checksum.clone())
            .unwrap();
        event_data_lua
            .set("target_checksum", event_data.target_checksum.clone())
            .unwrap();
        event_data_lua
            .set("source_etag", event_data.source_etag.clone())
            .unwrap();
        event_data_lua
            .set("target_etag", event_data.target_etag.clone())
            .unwrap();
        event_data_lua
            .set("message", event_data.message.clone())
            .unwrap();

        let func: mlua::Result<mlua::Function> = self.lua.get_engine().globals().get("on_event");
        if let Err(e) = func {
            warn!("Lua function 'on_event' not found: {}", e);
            return;
        }

        let func_result: mlua::Result<()> = func.unwrap().call_async(event_data_lua).await;
        if let Err(e) = func_result {
            warn!("Error executing Lua event callback: {}", e);
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_callback() {
        let _callback = LuaEventCallback::new(8 * 1024 * 1024, false, false);
        let _callback = LuaEventCallback::new(8 * 1024 * 1024, true, false);
        let _callback = LuaEventCallback::new(0, true, true);
    }
}
