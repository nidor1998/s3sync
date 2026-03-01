#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use once_cell::sync::Lazy;
    use tokio::sync::Semaphore;

    use common::*;
    use s3sync::config::Config;
    use s3sync::config::args::parse_from_args;
    use s3sync::pipeline::Pipeline;
    use s3sync::types::token::create_pipeline_cancellation_token;
    use std::convert::TryFrom;

    use super::*;

    /// Panic simulation tests toggle global state, so they must run sequentially
    /// with respect to each other. This file-local semaphore serializes only these
    /// tests while allowing other test files to run in parallel.
    static PANIC_SIM_SEMAPHORE: Lazy<Arc<Semaphore>> = Lazy::new(|| Arc::new(Semaphore::new(1)));

    #[tokio::test]
    async fn panic_object_filter_base() {
        TestHelper::init_dummy_tracing_subscriber();
        let _semaphore = PANIC_SIM_SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();

        {
            let target_bucket_url = format!("s3://{}", bucket);
            helper.create_bucket(&bucket, REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--panic-simulation-point",
                "ObjectFilterBase::receive_and_filter",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_panic_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_panic_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&bucket)
            .await;
    }

    #[tokio::test]
    async fn panic_list_source() {
        TestHelper::init_dummy_tracing_subscriber();
        let _semaphore = PANIC_SIM_SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();

        {
            let target_bucket_url = format!("s3://{}", bucket);
            helper.create_bucket(&bucket, REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--panic-simulation-point",
                "ObjectLister::list_source",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_panic_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_panic_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&bucket)
            .await;
    }

    #[tokio::test]
    async fn panic_list_target() {
        TestHelper::init_dummy_tracing_subscriber();
        let _semaphore = PANIC_SIM_SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();

        {
            let target_bucket_url = format!("s3://{}", bucket);
            helper.create_bucket(&bucket, REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--panic-simulation-point",
                "ObjectLister::list_target",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_panic_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_panic_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&bucket)
            .await;
    }

    #[tokio::test]
    async fn panic_key_aggregator() {
        TestHelper::init_dummy_tracing_subscriber();
        let _semaphore = PANIC_SIM_SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();

        {
            let target_bucket_url = format!("s3://{}", bucket);
            helper.create_bucket(&bucket, REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--panic-simulation-point",
                "KeyAggregator::aggregate",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_panic_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_panic_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&bucket)
            .await;
    }

    #[tokio::test]
    async fn panic_user_defined_filter() {
        TestHelper::init_dummy_tracing_subscriber();
        let _semaphore = PANIC_SIM_SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();

        {
            let target_bucket_url = format!("s3://{}", bucket);
            helper.create_bucket(&bucket, REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-callback-lua-script",
                "./test_data/script/filter_callback.lua",
                "--panic-simulation-point",
                "UserDefinedFilter::receive_and_filter",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_panic_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_panic_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&bucket)
            .await;
    }

    #[tokio::test]
    async fn panic_syncer() {
        TestHelper::init_dummy_tracing_subscriber();
        let _semaphore = PANIC_SIM_SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();

        {
            let target_bucket_url = format!("s3://{}", bucket);
            helper.create_bucket(&bucket, REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--panic-simulation-point",
                "ObjectSyncer::receive_and_filter",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_panic_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_panic_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&bucket)
            .await;
    }

    #[tokio::test]
    async fn panic_version_packer() {
        TestHelper::init_dummy_tracing_subscriber();
        let _semaphore = PANIC_SIM_SEMAPHORE.clone().acquire_owned().await.unwrap();

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
                "--server-side-copy",
                "--enable-versioning",
                "--panic-simulation-point",
                "ObjectVersionsPacker::receive_and_filter",
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_panic_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_panic_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&bucket1)
            .await;
        helper
            .delete_bucket_with_cascade(&bucket2)
            .await;
    }

    #[tokio::test]
    async fn panic_point_in_time_packer() {
        TestHelper::init_dummy_tracing_subscriber();
        let _semaphore = PANIC_SIM_SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            helper.create_bucket(&bucket1, REGION).await;
            helper.create_bucket(&bucket2, REGION).await;
            helper.enable_bucket_versioning(&bucket1).await;

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
                "--point-in-time",
                "2021-01-01T00:00:00Z",
                "--panic-simulation-point",
                "ObjectPointInTimePacker::receive_and_filter",
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_panic_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_panic_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&bucket1)
            .await;
        helper
            .delete_bucket_with_cascade(&bucket2)
            .await;
    }

    #[tokio::test]
    async fn panic_diff_lister() {
        TestHelper::init_dummy_tracing_subscriber();
        let _semaphore = PANIC_SIM_SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();

        let target_bucket_url = format!("s3://{}", bucket);

        {
            helper.create_bucket(&bucket, REGION).await;
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
            let target_bucket_url = format!("s3://{}", bucket);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--delete",
                "--panic-simulation-point",
                "DiffLister::receive_and_filter",
                "./test_data/e2e_test/case2/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_panic_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_panic_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&bucket)
            .await;
    }

    #[tokio::test]
    async fn panic_object_deleter() {
        TestHelper::init_dummy_tracing_subscriber();
        let _semaphore = PANIC_SIM_SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();

        let target_bucket_url = format!("s3://{}", bucket);

        {
            helper.create_bucket(&bucket, REGION).await;
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
            let target_bucket_url = format!("s3://{}", bucket);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--delete",
                "--panic-simulation-point",
                "ObjectDeleter::receive_and_filter",
                "./test_data/e2e_test/case2/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            TestHelper::enable_panic_dangerous_simulation();
            pipeline.run().await;
            TestHelper::disable_panic_dangerous_simulation();

            assert!(pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&bucket)
            .await;
    }
}
