#[allow(unused_imports)]
use anyhow::Result;

#[cfg(feature = "lua_support")]
use mlua::{HookTriggers, Lua, LuaOptions, StdLib, VmState};

#[allow(unused_imports)]
use tracing::{debug, info};

#[cfg(feature = "lua_support")]
use std::sync::{Arc, Mutex};
#[cfg(feature = "lua_support")]
use std::time::{Duration, Instant};

#[cfg(feature = "lua_support")]
const HOOK_INSTRUCTION_COUNT: u32 = 1000;

#[cfg(feature = "lua_support")]
pub struct LuaScriptCallbackEngine {
    engine: Lua,
    deadline: Arc<Mutex<Instant>>,
    callback_timeout_milliseconds: u64,
}

#[cfg(feature = "lua_support")]
impl LuaScriptCallbackEngine {
    pub fn new(memory_limit: usize, callback_timeout_milliseconds: u64) -> Self {
        debug!(
            "Creating Lua engine with all libraries enabled, memory limit: {} bytes, callback timeout: {} ms",
            memory_limit, callback_timeout_milliseconds
        );

        let engine = Lua::new();
        engine.set_memory_limit(memory_limit).unwrap();
        let deadline = Arc::new(Mutex::new(
            Instant::now() + Duration::from_secs(86400 * 365),
        ));
        Self::install_timeout_hook(&engine, callback_timeout_milliseconds, &deadline);
        LuaScriptCallbackEngine {
            engine,
            deadline,
            callback_timeout_milliseconds,
        }
    }

    pub fn new_without_os_io_libs(memory_limit: usize, callback_timeout_milliseconds: u64) -> Self {
        debug!(
            "Creating Lua engine without OS library, memory limit: {} bytes, callback timeout: {} ms",
            memory_limit, callback_timeout_milliseconds
        );

        let engine = Lua::new_with(
            StdLib::ALL_SAFE ^ (StdLib::OS | StdLib::IO),
            LuaOptions::default(),
        )
        .unwrap();
        engine.set_memory_limit(memory_limit).unwrap();
        let deadline = Arc::new(Mutex::new(
            Instant::now() + Duration::from_secs(86400 * 365),
        ));
        Self::install_timeout_hook(&engine, callback_timeout_milliseconds, &deadline);
        LuaScriptCallbackEngine {
            engine,
            deadline,
            callback_timeout_milliseconds,
        }
    }

    pub fn unsafe_new(memory_limit: usize, callback_timeout_milliseconds: u64) -> Self {
        debug!(
            "Creating Lua engine with unsafe mode enabled, callback timeout: {} ms",
            callback_timeout_milliseconds
        );

        let engine;
        unsafe { engine = Lua::unsafe_new() };
        engine.set_memory_limit(memory_limit).unwrap();
        let deadline = Arc::new(Mutex::new(
            Instant::now() + Duration::from_secs(86400 * 365),
        ));
        Self::install_timeout_hook(&engine, callback_timeout_milliseconds, &deadline);
        LuaScriptCallbackEngine {
            engine,
            deadline,
            callback_timeout_milliseconds,
        }
    }

    fn install_timeout_hook(
        engine: &Lua,
        callback_timeout_milliseconds: u64,
        deadline: &Arc<Mutex<Instant>>,
    ) {
        if callback_timeout_milliseconds == 0 {
            return;
        }

        let deadline = Arc::clone(deadline);
        engine
            .set_global_hook(
                HookTriggers::new().every_nth_instruction(HOOK_INSTRUCTION_COUNT),
                move |_lua, _debug| {
                    let deadline = *deadline.lock().unwrap();
                    if Instant::now() >= deadline {
                        Err(mlua::Error::RuntimeError(format!(
                            "Lua callback timed out after {}ms",
                            callback_timeout_milliseconds
                        )))
                    } else {
                        Ok(VmState::Continue)
                    }
                },
            )
            .unwrap();
    }

    pub fn reset_deadline(&self) {
        if self.callback_timeout_milliseconds > 0 {
            *self.deadline.lock().unwrap() =
                Instant::now() + Duration::from_millis(self.callback_timeout_milliseconds);
        }
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
        let _lua_engine = LuaScriptCallbackEngine::new(8 * 1024 * 1024, 0);
        let _lua_engine_without_os =
            LuaScriptCallbackEngine::new_without_os_io_libs(8 * 1024 * 1024, 0);
        let _lua_engine_unsafe = LuaScriptCallbackEngine::unsafe_new(8 * 1024 * 1024, 0);
    }

    #[cfg(feature = "lua_support")]
    #[tokio::test]
    async fn timeout_triggers_on_infinite_loop() {
        let engine = LuaScriptCallbackEngine::new_without_os_io_libs(8 * 1024 * 1024, 100);
        engine
            .load_and_compile("function infinite_loop() while true do end end")
            .unwrap();
        engine.reset_deadline();
        let func: mlua::Function = engine.get_engine().globals().get("infinite_loop").unwrap();
        let result: mlua::Result<()> = func.call_async(()).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("timed out"),
            "Expected timeout error, got: {}",
            err_msg
        );
    }

    #[cfg(feature = "lua_support")]
    #[tokio::test]
    async fn normal_script_completes_within_timeout() {
        let engine = LuaScriptCallbackEngine::new_without_os_io_libs(8 * 1024 * 1024, 10000);
        engine
            .load_and_compile("function add(a, b) return a + b end")
            .unwrap();
        engine.reset_deadline();
        let func: mlua::Function = engine.get_engine().globals().get("add").unwrap();
        let result: i64 = func.call_async((1, 2)).await.unwrap();
        assert_eq!(result, 3);
    }

    #[cfg(feature = "lua_support")]
    #[tokio::test]
    async fn timeout_zero_disables_hook() {
        let engine = LuaScriptCallbackEngine::new_without_os_io_libs(8 * 1024 * 1024, 0);
        engine
            .load_and_compile("function add(a, b) return a + b end")
            .unwrap();
        engine.reset_deadline();
        let func: mlua::Function = engine.get_engine().globals().get("add").unwrap();
        let result: i64 = func.call_async((1, 2)).await.unwrap();
        assert_eq!(result, 3);
    }
}
