#[allow(unused_imports)]
use anyhow::Result;

#[cfg(feature = "lua_support")]
use mlua::{Lua, LuaOptions, StdLib};

#[allow(unused_imports)]
use tracing::{debug, info};

#[cfg(feature = "lua_support")]
pub struct LuaScriptCallbackEngine {
    engine: Lua,
}

#[cfg(feature = "lua_support")]
impl LuaScriptCallbackEngine {
    pub fn new(memory_limit: usize) -> Self {
        debug!(
            "Creating Lua engine with all libraries enabled, memory limit: {} bytes",
            memory_limit
        );

        let engine = Lua::new();
        engine.set_memory_limit(memory_limit).unwrap();
        LuaScriptCallbackEngine { engine }
    }

    pub fn new_without_os_io_libs(memory_limit: usize) -> Self {
        debug!(
            "Creating Lua engine without OS library, memory limit: {} bytes",
            memory_limit
        );

        let engine = Lua::new_with(
            StdLib::ALL_SAFE ^ (StdLib::OS | StdLib::IO),
            LuaOptions::default(),
        )
        .unwrap();
        engine.set_memory_limit(memory_limit).unwrap();
        LuaScriptCallbackEngine { engine }
    }

    pub fn unsafe_new(memory_limit: usize) -> Self {
        debug!("Creating Lua engine with unsafe mode enabled");

        let engine;
        unsafe { engine = Lua::unsafe_new() };
        engine.set_memory_limit(memory_limit).unwrap();
        LuaScriptCallbackEngine { engine }
    }

    pub fn get_engine(&self) -> &Lua {
        &self.engine
    }

    pub fn load_and_compile(&self, script: &str) -> Result<()> {
        self.engine
            .load(script)
            .exec()
            .map_err(|e| anyhow::anyhow!("Failed to load and compile Lua script: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "lua_support")]
    #[tokio::test]
    async fn create_new() {
        let _lua_engine = LuaScriptCallbackEngine::new(8 * 1024 * 1024);
        let _lua_engine_without_os =
            LuaScriptCallbackEngine::new_without_os_io_libs(8 * 1024 * 1024);
        let _lua_engine_unsafe = LuaScriptCallbackEngine::unsafe_new(8 * 1024 * 1024);
    }
}
