#[cfg(test)]
#[cfg(feature = "e2e_test")]
mod common;

#[cfg(test)]
#[cfg(feature = "e2e_test")]
mod tests {

    use common::*;
    use s3sync::config::Config;
    use s3sync::config::args::parse_from_args;
    use s3sync::pipeline::Pipeline;
    use s3sync::types::event_callback::EventType;
    use s3sync::types::token::create_pipeline_cancellation_token;
    use std::convert::TryFrom;

    use super::*;

    #[tokio::test]
    async fn test_lua_event_callback_no_script() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--event-callback-lua-script",
                "./test_data/script/event_callback.luaERROR",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let args = parse_from_args(args);
            assert!(args.is_err());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn test_lua_event_callback() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--event-callback-lua-script",
                "./test_data/script/event_callback.lua",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];

            let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

            #[allow(clippy::collapsible_else_if)]
            if let Some(event_callback_lua_script) = config.event_callback_lua_script.as_ref() {
                let mut lua_event_callback =
                    s3sync::callback::lua_event_callback::LuaEventCallback::new(
                        config.lua_vm_memory_limit,
                        config.allow_lua_os_library,
                        config.allow_lua_unsafe_vm,
                    );
                if let Err(e) = lua_event_callback
                    .load_and_compile(event_callback_lua_script.as_str())
                    .await
                {
                    panic!(
                        "Failed to load and compile Lua script event callback: {}",
                        e
                    );
                }

                // Lua event callback is registered for all events.
                config
                    .event_manager
                    .register_callback(EventType::ALL_EVENTS, lua_event_callback);
            }

            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;

            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.sync_skip, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn test_lua_event_callback_no_entrypoint() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--event-callback-lua-script",
                "./test_data/script/no_entrypoint.lua",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

            #[allow(clippy::collapsible_else_if)]
            if let Some(event_callback_lua_script) = config.event_callback_lua_script.as_ref() {
                let mut lua_event_callback =
                    s3sync::callback::lua_event_callback::LuaEventCallback::new(
                        config.lua_vm_memory_limit,
                        config.allow_lua_os_library,
                        config.allow_lua_unsafe_vm,
                    );
                if let Err(e) = lua_event_callback
                    .load_and_compile(event_callback_lua_script.as_str())
                    .await
                {
                    panic!(
                        "Failed to load and compile Lua script event callback: {}",
                        e
                    );
                }

                // Lua event callback is registered for all events.
                config
                    .event_manager
                    .register_callback(EventType::ALL_EVENTS, lua_event_callback);
            }
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;

            assert!(!pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn test_lua_filter_callback_no_script() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-callback-lua-script",
                "./test_data/script/filter_callback.luaERROR",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let args = parse_from_args(args);
            assert!(args.is_err());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn test_lua_filter_callback() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-callback-lua-script",
                "./test_data/script/filter_callback.lua",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];

            let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

            #[allow(clippy::collapsible_else_if)]
            if let Some(filter_callback_lua_script) = config.filter_callback_lua_script.as_ref() {
                let mut lua_filter_callback =
                    s3sync::callback::lua_filter_callback::LuaFilterCallback::new(
                        config.lua_vm_memory_limit,
                        config.allow_lua_os_library,
                        config.allow_lua_unsafe_vm,
                    );
                if let Err(e) = lua_filter_callback
                    .load_and_compile(filter_callback_lua_script.as_str())
                    .await
                {
                    panic!(
                        "Failed to load and compile Lua script filter callback: {}",
                        e
                    );
                }
                config
                    .filter_config
                    .filter_manager
                    .register_callback(lua_filter_callback);
            }

            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;

            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.sync_skip, 4);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn test_lua_filter_callback_no_entrypoint() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-callback-lua-script",
                "./test_data/script/no_entrypoint.lua",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];

            let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

            #[allow(clippy::collapsible_else_if)]
            if let Some(filter_callback_lua_script) = config.filter_callback_lua_script.as_ref() {
                let mut lua_filter_callback =
                    s3sync::callback::lua_filter_callback::LuaFilterCallback::new(
                        config.lua_vm_memory_limit,
                        config.allow_lua_os_library,
                        config.allow_lua_unsafe_vm,
                    );
                if let Err(e) = lua_filter_callback
                    .load_and_compile(filter_callback_lua_script.as_str())
                    .await
                {
                    panic!(
                        "Failed to load and compile Lua script filter callback: {}",
                        e
                    );
                }
                config
                    .filter_config
                    .filter_manager
                    .register_callback(lua_filter_callback);
            }

            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn test_lua_filter_callback_with_os_lib_error() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-callback-lua-script",
                "./test_data/script/filter_callback_with_os_lib.lua",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];

            let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

            #[allow(clippy::collapsible_else_if)]
            if let Some(filter_callback_lua_script) = config.filter_callback_lua_script.as_ref() {
                let mut lua_filter_callback =
                    s3sync::callback::lua_filter_callback::LuaFilterCallback::new(
                        config.lua_vm_memory_limit,
                        config.allow_lua_os_library,
                        config.allow_lua_unsafe_vm,
                    );
                if let Err(e) = lua_filter_callback
                    .load_and_compile(filter_callback_lua_script.as_str())
                    .await
                {
                    panic!(
                        "Failed to load and compile Lua script filter callback: {}",
                        e
                    );
                }
                config
                    .filter_config
                    .filter_manager
                    .register_callback(lua_filter_callback);
            }

            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn test_lua_filter_callback_with_error() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-callback-lua-script",
                "./test_data/script/filter_callback_error.lua",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];

            let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

            #[allow(clippy::collapsible_else_if)]
            if let Some(filter_callback_lua_script) = config.filter_callback_lua_script.as_ref() {
                let mut lua_filter_callback =
                    s3sync::callback::lua_filter_callback::LuaFilterCallback::new(
                        config.lua_vm_memory_limit,
                        config.allow_lua_os_library,
                        config.allow_lua_unsafe_vm,
                    );
                if let Err(e) = lua_filter_callback
                    .load_and_compile(filter_callback_lua_script.as_str())
                    .await
                {
                    panic!(
                        "Failed to load and compile Lua script filter callback: {}",
                        e
                    );
                }
                config
                    .filter_config
                    .filter_manager
                    .register_callback(lua_filter_callback);
            }

            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn test_lua_filter_callback_with_os_lib() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-callback-lua-script",
                "./test_data/script/filter_callback_with_os_lib.lua",
                "--allow-lua-os-library",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];

            let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

            #[allow(clippy::collapsible_else_if)]
            if let Some(filter_callback_lua_script) = config.filter_callback_lua_script.as_ref() {
                let mut lua_filter_callback =
                    s3sync::callback::lua_filter_callback::LuaFilterCallback::new(
                        config.lua_vm_memory_limit,
                        config.allow_lua_os_library,
                        config.allow_lua_unsafe_vm,
                    );
                if let Err(e) = lua_filter_callback
                    .load_and_compile(filter_callback_lua_script.as_str())
                    .await
                {
                    panic!(
                        "Failed to load and compile Lua script filter callback: {}",
                        e
                    );
                }
                config
                    .filter_config
                    .filter_manager
                    .register_callback(lua_filter_callback);
            }

            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;

            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.sync_skip, 4);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn test_lua_filter_callback_with_unsafe_lua_vm() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-callback-lua-script",
                "./test_data/script/filter_callback_with_os_lib.lua",
                "--allow-lua-unsafe-vm",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];

            let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

            #[allow(clippy::collapsible_else_if)]
            if let Some(filter_callback_lua_script) = config.filter_callback_lua_script.as_ref() {
                let mut lua_filter_callback =
                    s3sync::callback::lua_filter_callback::LuaFilterCallback::new(
                        config.lua_vm_memory_limit,
                        config.allow_lua_os_library,
                        config.allow_lua_unsafe_vm,
                    );
                if let Err(e) = lua_filter_callback
                    .load_and_compile(filter_callback_lua_script.as_str())
                    .await
                {
                    panic!(
                        "Failed to load and compile Lua script filter callback: {}",
                        e
                    );
                }
                config
                    .filter_config
                    .filter_manager
                    .register_callback(lua_filter_callback);
            }

            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;

            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.sync_skip, 4);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn test_lua_preprocess_callback_no_script() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--preprocess-callback-lua-script",
                "./test_data/script/preprocess_callback.luaERROR",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let args = parse_from_args(args);
            assert!(args.is_err());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn test_lua_preprocess_callback_no_entrypoint() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--preprocess-callback-lua-script",
                "./test_data/script/no_entrypoint.lua",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];

            let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

            #[allow(clippy::collapsible_else_if)]
            if let Some(preprocess_callback_lua_script) =
                config.preprocess_callback_lua_script.as_ref()
            {
                let mut lua_preprocess_callback =
                    s3sync::callback::lua_preprocess_callback::LuaPreprocessCallback::new(
                        config.lua_vm_memory_limit,
                        config.allow_lua_os_library,
                        config.allow_lua_unsafe_vm,
                    );
                if let Err(e) = lua_preprocess_callback
                    .load_and_compile(preprocess_callback_lua_script.as_str())
                    .await
                {
                    panic!(
                        "Failed to load and compile Lua script preprocess callback: {}",
                        e
                    );
                }
                config
                    .preprocess_manager
                    .register_callback(lua_preprocess_callback);
            }

            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn test_lua_preprocess_callback() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--preprocess-callback-lua-script",
                "./test_data/script/preprocess_callback.lua",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];

            let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

            #[allow(clippy::collapsible_else_if)]
            if let Some(preprocess_callback_lua_script) =
                config.preprocess_callback_lua_script.as_ref()
            {
                let mut lua_preprocess_callback =
                    s3sync::callback::lua_preprocess_callback::LuaPreprocessCallback::new(
                        config.lua_vm_memory_limit,
                        config.allow_lua_os_library,
                        config.allow_lua_unsafe_vm,
                    );
                if let Err(e) = lua_preprocess_callback
                    .load_and_compile(preprocess_callback_lua_script.as_str())
                    .await
                {
                    panic!(
                        "Failed to load and compile Lua script preprocess callback: {}",
                        e
                    );
                }
                config
                    .preprocess_manager
                    .register_callback(lua_preprocess_callback);
            }

            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;

            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.sync_skip, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            helper
                .verify_test_object_metadata(&BUCKET1.to_string(), "dir1/data1", None)
                .await;
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn test_lua_preprocess_callback_multipart_upload() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            TestHelper::create_large_file();
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--preprocess-callback-lua-script",
                "./test_data/script/preprocess_callback.lua",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];

            let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

            #[allow(clippy::collapsible_else_if)]
            if let Some(preprocess_callback_lua_script) =
                config.preprocess_callback_lua_script.as_ref()
            {
                let mut lua_preprocess_callback =
                    s3sync::callback::lua_preprocess_callback::LuaPreprocessCallback::new(
                        config.lua_vm_memory_limit,
                        config.allow_lua_os_library,
                        config.allow_lua_unsafe_vm,
                    );
                if let Err(e) = lua_preprocess_callback
                    .load_and_compile(preprocess_callback_lua_script.as_str())
                    .await
                {
                    panic!(
                        "Failed to load and compile Lua script preprocess callback: {}",
                        e
                    );
                }
                config
                    .preprocess_manager
                    .register_callback(lua_preprocess_callback);
            }

            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;

            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.sync_skip, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            helper
                .verify_test_object_metadata(&BUCKET1.to_string(), LARGE_FILE_KEY, None)
                .await;
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn test_lua_preprocess_callback_invalid_acl() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--preprocess-callback-lua-script",
                "./test_data/script/preprocess_callback_invalid_acl.lua",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];

            let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

            #[allow(clippy::collapsible_else_if)]
            if let Some(preprocess_callback_lua_script) =
                config.preprocess_callback_lua_script.as_ref()
            {
                let mut lua_preprocess_callback =
                    s3sync::callback::lua_preprocess_callback::LuaPreprocessCallback::new(
                        config.lua_vm_memory_limit,
                        config.allow_lua_os_library,
                        config.allow_lua_unsafe_vm,
                    );
                if let Err(e) = lua_preprocess_callback
                    .load_and_compile(preprocess_callback_lua_script.as_str())
                    .await
                {
                    panic!(
                        "Failed to load and compile Lua script preprocess callback: {}",
                        e
                    );
                }
                config
                    .preprocess_manager
                    .register_callback(lua_preprocess_callback);
            }

            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn test_lua_preprocess_callback_invalid_expire() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--preprocess-callback-lua-script",
                "./test_data/script/preprocess_callback_invalid_expire.lua",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];

            let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

            #[allow(clippy::collapsible_else_if)]
            if let Some(preprocess_callback_lua_script) =
                config.preprocess_callback_lua_script.as_ref()
            {
                let mut lua_preprocess_callback =
                    s3sync::callback::lua_preprocess_callback::LuaPreprocessCallback::new(
                        config.lua_vm_memory_limit,
                        config.allow_lua_os_library,
                        config.allow_lua_unsafe_vm,
                    );
                if let Err(e) = lua_preprocess_callback
                    .load_and_compile(preprocess_callback_lua_script.as_str())
                    .await
                {
                    panic!(
                        "Failed to load and compile Lua script preprocess callback: {}",
                        e
                    );
                }
                config
                    .preprocess_manager
                    .register_callback(lua_preprocess_callback);
            }

            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn test_lua_preprocess_callback_invalid_storage_class() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--preprocess-callback-lua-script",
                "./test_data/script/preprocess_callback_invalid_storage_class.lua",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];

            let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

            #[allow(clippy::collapsible_else_if)]
            if let Some(preprocess_callback_lua_script) =
                config.preprocess_callback_lua_script.as_ref()
            {
                let mut lua_preprocess_callback =
                    s3sync::callback::lua_preprocess_callback::LuaPreprocessCallback::new(
                        config.lua_vm_memory_limit,
                        config.allow_lua_os_library,
                        config.allow_lua_unsafe_vm,
                    );
                if let Err(e) = lua_preprocess_callback
                    .load_and_compile(preprocess_callback_lua_script.as_str())
                    .await
                {
                    panic!(
                        "Failed to load and compile Lua script preprocess callback: {}",
                        e
                    );
                }
                config
                    .preprocess_manager
                    .register_callback(lua_preprocess_callback);
            }

            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn test_lua_preprocess_callback_invalid_request_payer() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--preprocess-callback-lua-script",
                "./test_data/script/preprocess_callback_invalid_request_payer.lua",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];

            let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

            #[allow(clippy::collapsible_else_if)]
            if let Some(preprocess_callback_lua_script) =
                config.preprocess_callback_lua_script.as_ref()
            {
                let mut lua_preprocess_callback =
                    s3sync::callback::lua_preprocess_callback::LuaPreprocessCallback::new(
                        config.lua_vm_memory_limit,
                        config.allow_lua_os_library,
                        config.allow_lua_unsafe_vm,
                    );
                if let Err(e) = lua_preprocess_callback
                    .load_and_compile(preprocess_callback_lua_script.as_str())
                    .await
                {
                    panic!(
                        "Failed to load and compile Lua script preprocess callback: {}",
                        e
                    );
                }
                config
                    .preprocess_manager
                    .register_callback(lua_preprocess_callback);
            }

            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn test_lua_preprocess_callback_skip() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--preprocess-callback-lua-script",
                "./test_data/script/preprocess_callback_skip.lua",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];

            let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

            #[allow(clippy::collapsible_else_if)]
            if let Some(preprocess_callback_lua_script) =
                config.preprocess_callback_lua_script.as_ref()
            {
                let mut lua_preprocess_callback =
                    s3sync::callback::lua_preprocess_callback::LuaPreprocessCallback::new(
                        config.lua_vm_memory_limit,
                        config.allow_lua_os_library,
                        config.allow_lua_unsafe_vm,
                    );
                if let Err(e) = lua_preprocess_callback
                    .load_and_compile(preprocess_callback_lua_script.as_str())
                    .await
                {
                    panic!(
                        "Failed to load and compile Lua script preprocess callback: {}",
                        e
                    );
                }
                config
                    .preprocess_manager
                    .register_callback(lua_preprocess_callback);
            }

            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;

            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 0);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.sync_skip, 5);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn test_lua_invalid_script() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--preprocess-callback-lua-script",
                "./test_data/script/invalid_script.lua",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

            #[allow(clippy::collapsible_else_if)]
            if let Some(preprocess_callback_lua_script) =
                config.preprocess_callback_lua_script.as_ref()
            {
                let mut lua_preprocess_callback =
                    s3sync::callback::lua_preprocess_callback::LuaPreprocessCallback::new(
                        config.lua_vm_memory_limit,
                        config.allow_lua_os_library,
                        config.allow_lua_unsafe_vm,
                    );
                // skipcq: RS-W1029
                if let Ok(_) = lua_preprocess_callback
                    .load_and_compile(preprocess_callback_lua_script.as_str())
                    .await
                {
                    // skipcq: RS-W1021
                    assert!(false, "Expected error loading Lua script, but succeeded");
                }
            }
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn test_lua_not_enough_memory() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--lua-vm-memory-limit",
                "1KiB",
                "--preprocess-callback-lua-script",
                "./test_data/script/preprocess_callback.lua",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

            #[allow(clippy::collapsible_else_if)]
            if let Some(preprocess_callback_lua_script) =
                config.preprocess_callback_lua_script.as_ref()
            {
                let mut lua_preprocess_callback =
                    s3sync::callback::lua_preprocess_callback::LuaPreprocessCallback::new(
                        config.lua_vm_memory_limit,
                        config.allow_lua_os_library,
                        config.allow_lua_unsafe_vm,
                    );

                // skipcq: RS-W1029
                if let Ok(_) = lua_preprocess_callback
                    .load_and_compile(preprocess_callback_lua_script.as_str())
                    .await
                {
                    // skipcq: RS-W1021
                    assert!(false, "Expected error loading Lua script, but succeeded");
                }
            }
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }
}
