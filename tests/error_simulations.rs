#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;
    use s3sync::config::Config;
    use s3sync::config::args::parse_from_args;
    use s3sync::pipeline::Pipeline;
    use s3sync::types::token::create_pipeline_cancellation_token;
    use std::convert::TryFrom;

    use super::*;

    #[tokio::test]
    async fn error_object_filter_base() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--error-simulation-point",
                "ObjectFilterBase::receive_and_filter",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_error_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_error_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn error_list_source() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--error-simulation-point",
                "ObjectLister::list_source",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_error_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_error_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn error_list_target() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--error-simulation-point",
                "ObjectLister::list_target",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_error_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_error_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn error_key_aggregator() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--error-simulation-point",
                "KeyAggregator::aggregate",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_error_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_error_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn error_user_defined_filter() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-callback-lua-script",
                "./test_data/script/filter_callback.lua",
                "--error-simulation-point",
                "UserDefinedFilter::receive_and_filter",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_error_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_error_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn error_syncer() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--error-simulation-point",
                "ObjectSyncer::receive_and_filter",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_error_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_error_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn precondition_error_syncer() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--error-simulation-point",
                "ObjectSyncer::sync_or_delete_object-precondition",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_error_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_error_dangerous_simulation();

            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 0);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.sync_skip, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 5);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn precondition_warn_as_error_syncer() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--warn-as-error",
                "--error-simulation-point",
                "ObjectSyncer::sync_or_delete_object-precondition",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_error_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_error_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn error_version_packer() {
        TestHelper::init_dummy_tracing_subscriber();
        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
            helper.enable_bucket_versioning(&BUCKET2.to_string()).await;

            helper.sync_test_data(&target_bucket_url).await;
        }

        let source_bucket_url = format!("s3://{}", *BUCKET1);
        let target_bucket_url = format!("s3://{}", *BUCKET2);

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--enable-versioning",
                "--error-simulation-point",
                "ObjectVersionsPacker::receive_and_filter",
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_error_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_error_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn error_point_in_time_packer() {
        TestHelper::init_dummy_tracing_subscriber();
        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;

            helper.sync_test_data(&target_bucket_url).await;
        }

        let source_bucket_url = format!("s3://{}", *BUCKET1);
        let target_bucket_url = format!("s3://{}", *BUCKET2);

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--point-in-time",
                "2021-01-01T00:00:00Z",
                "--error-simulation-point",
                "ObjectPointInTimePacker::receive_and_filter",
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_error_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_error_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn error_diff_lister() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        let target_bucket_url = format!("s3://{}", *BUCKET1);

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--delete",
                "--error-simulation-point",
                "DiffLister::receive_and_filter",
                "./test_data/e2e_test/case2/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_error_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_error_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn error_object_deleter() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        let target_bucket_url = format!("s3://{}", *BUCKET1);

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--delete",
                "--error-simulation-point",
                "ObjectDeleter::receive_and_filter",
                "./test_data/e2e_test/case2/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_error_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_error_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn precondition_error_object_deleter() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        let target_bucket_url = format!("s3://{}", *BUCKET1);

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--delete",
                "--error-simulation-point",
                "ObjectDeleter::delete-precondition",
                "./test_data/e2e_test/case2/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_error_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_error_dangerous_simulation();

            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 0);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.sync_skip, 4);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_delete, 0);
            assert_eq!(stats.sync_warning, 1);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn precondition_warn_as_error_object_deleter() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        let target_bucket_url = format!("s3://{}", *BUCKET1);

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--delete",
                "--warn-as-error",
                "--error-simulation-point",
                "ObjectDeleter::delete-precondition",
                "./test_data/e2e_test/case2/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_error_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_error_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn error_invalid_object_state_syncer() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

        let target_bucket_url = format!("s3://{}", *BUCKET1);

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--error-simulation-point",
                "ObjectSyncer::receive_and_filter-invalid_object_state_error",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_error_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_error_dangerous_simulation();

            assert!(!pipeline.has_error());
            assert!(pipeline.has_warning());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn error_invalid_object_state_as_error_syncer() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

        let target_bucket_url = format!("s3://{}", *BUCKET1);

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--error-simulation-point",
                "ObjectSyncer::receive_and_filter-invalid_object_state_error",
                "--warn-as-error",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_error_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_error_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }
}
