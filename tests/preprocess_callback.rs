#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;
    use s3sync::callback::debug_preprocess_callback::DebugPreprocessCallback;
    use s3sync::config::Config;
    use s3sync::config::args::parse_from_args;
    use s3sync::pipeline::Pipeline;
    use s3sync::types::token::create_pipeline_cancellation_token;
    use std::convert::TryFrom;

    use super::*;

    #[tokio::test]
    async fn test_preprocess_callback() {
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
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            config
                .preprocess_manager
                .register_callback(DebugPreprocessCallback {});
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;

            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 4);
            assert_eq!(stats.e_tag_verified, 4);
            assert_eq!(stats.sync_skip, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn test_preprocess_callback_multipart_upload() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        TestHelper::create_large_file_for_preprocess_test();

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "./playground/preprocess_test/",
                &target_bucket_url,
            ];
            let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            config
                .preprocess_manager
                .register_callback(DebugPreprocessCallback {});
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;

            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.sync_skip, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }
}
