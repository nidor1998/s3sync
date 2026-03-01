#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use once_cell::sync::Lazy;
    use tokio::sync::Semaphore;
    use uuid::Uuid;

    use common::*;
    use s3sync::config::Config;
    use s3sync::config::args::parse_from_args;
    use s3sync::pipeline::Pipeline;
    use s3sync::types::token::create_pipeline_cancellation_token;
    use std::convert::TryFrom;

    use super::*;

    /// Cancel simulation tests toggle global state, so they must run sequentially
    /// with respect to each other.
    static CANCEL_SIM_SEMAPHORE: Lazy<Arc<Semaphore>> = Lazy::new(|| Arc::new(Semaphore::new(1)));

    #[tokio::test]
    async fn cancel_sync_object_with_force_retry() {
        TestHelper::init_dummy_tracing_subscriber();
        let _semaphore = CANCEL_SIM_SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();

        {
            let target_bucket_url = format!("s3://{}", bucket);
            helper.create_bucket(&bucket, REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--allow-e2e-test-dangerous-simulation",
                "--cancellation-point",
                "sync_object_with_force_retry",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_cancel_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_cancel_dangerous_simulation();

            assert!(!pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&bucket)
            .await;
    }

    #[tokio::test]
    async fn cancel_sync_or_delete_object() {
        TestHelper::init_dummy_tracing_subscriber();
        let _semaphore = CANCEL_SIM_SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();

        {
            let target_bucket_url = format!("s3://{}", bucket);
            helper.create_bucket(&bucket, REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--allow-e2e-test-dangerous-simulation",
                "--cancellation-point",
                "sync_or_delete_object",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_cancel_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_cancel_dangerous_simulation();

            assert!(!pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&bucket)
            .await;
    }

    #[tokio::test]
    async fn cancel_put_object() {
        TestHelper::init_dummy_tracing_subscriber();
        let _semaphore = CANCEL_SIM_SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();

        {
            let target_bucket_url = format!("s3://{}", bucket);
            helper.create_bucket(&bucket, REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--allow-e2e-test-dangerous-simulation",
                "--cancellation-point",
                "put_object",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_cancel_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_cancel_dangerous_simulation();

            assert!(!pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&bucket)
            .await;
    }

    #[tokio::test]
    async fn cancel_list_source_objects() {
        TestHelper::init_dummy_tracing_subscriber();
        let _semaphore = CANCEL_SIM_SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();

        {
            let target_bucket_url = format!("s3://{}", bucket);
            helper.create_bucket(&bucket, REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--allow-e2e-test-dangerous-simulation",
                "--check-etag",
                "--cancellation-point",
                "LocalStorage::list_objects",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_cancel_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_cancel_dangerous_simulation();

            assert!(!pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&bucket)
            .await;
    }

    #[tokio::test]
    async fn cancel_list_target_objects() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();

        {
            let target_bucket_url = format!("s3://{}", bucket);
            helper.create_bucket(&bucket, REGION).await;

            helper
                .sync_large_test_data_with_sha256(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--cancellation-point",
                "LocalStorage::list_objects_target",
                &source_bucket_url,
                &download_dir,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_cancel_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_cancel_dangerous_simulation();

            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_warning, 0);
        }        helper
            .delete_bucket_with_cascade(&bucket)
            .await;
    }

    #[tokio::test]
    async fn cancel_put_object_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();
        let _semaphore = CANCEL_SIM_SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            helper.create_bucket(&bucket1, REGION).await;
            helper.create_bucket(&bucket2, REGION).await;

            helper
                .sync_large_test_data_with_custom_chunksize(&target_bucket_url, "7340033")
                .await;
        }

        let source_bucket_url = format!("s3://{}", bucket1);
        let target_bucket_url = format!("s3://{}", bucket2);

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
                "--allow-e2e-test-dangerous-simulation",
                "--cancellation-point",
                "put_object",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_cancel_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_cancel_dangerous_simulation();

            assert!(!pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&bucket1)
            .await;
        helper
            .delete_bucket_with_cascade(&bucket2)
            .await;
    }

    #[tokio::test]
    async fn cancel_sync_object_versions() {
        TestHelper::init_dummy_tracing_subscriber();
        let _semaphore = CANCEL_SIM_SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            helper.create_bucket(&bucket1, REGION).await;
            helper.create_bucket(&bucket2, REGION).await;
            helper.enable_bucket_versioning(&bucket1).await;
            helper.enable_bucket_versioning(&bucket2).await;

            helper.sync_test_data(&target_bucket_url).await;
        }

        let source_bucket_url = format!("s3://{}", bucket1);
        let target_bucket_url = format!("s3://{}", bucket2);

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                "--allow-e2e-test-dangerous-simulation",
                "--cancellation-point",
                "sync_object_versions",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_cancel_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_cancel_dangerous_simulation();

            assert!(!pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&bucket1)
            .await;
        helper
            .delete_bucket_with_cascade(&bucket2)
            .await;
    }
}
