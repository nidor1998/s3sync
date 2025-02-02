#[cfg(test)]
#[cfg(feature = "e2e_test")]
mod common;

#[cfg(test)]
#[cfg(feature = "e2e_test")]
mod tests {
    use std::collections::HashMap;

    use aws_sdk_s3::types::{ServerSideEncryption, Tag, Tagging};

    use common::*;
    use s3sync::config::args::parse_from_args;
    use s3sync::pipeline::Pipeline;
    use s3sync::types::token::create_pipeline_cancellation_token;
    use s3sync::Config;

    use super::*;

    #[tokio::test]
    async fn s3_to_s3_both_bucket_versioning_error() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
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
    async fn s3_to_s3_source_bucket_versioning_error() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET2.to_string()).await;
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
        let target_bucket_url = format!("s3://{}", BUCKET2.to_string());
        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
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
    async fn s3_to_s3_target_bucket_versioning_error() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
        let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
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
    async fn s3_to_s3_source_bucket_versioning_check_error() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
        let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
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
    async fn s3_to_s3_target_bucket_versioning_check_error() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
        let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
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
    async fn s3_to_s3_without_prefix() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
            helper.enable_bucket_versioning(&BUCKET2.to_string()).await;

            helper.sync_test_data(&target_bucket_url).await;
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
        let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_versions_list = helper.list_object_versions(&BUCKET2.to_string(), "").await;
            assert_eq!(object_versions_list.len(), 5);
        }

        {
            helper.delete_all_objects(&BUCKET1.to_string()).await;
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_versions_list = helper.list_object_versions(&BUCKET2.to_string(), "").await;
            assert_eq!(object_versions_list.len(), 5);

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 0);
        }

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--delete",
                "./test_data/e2e_test/case2/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_versions_list = helper.list_object_versions(&BUCKET2.to_string(), "").await;
            assert_eq!(object_versions_list.len(), 9);

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 4);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_prefix() {
        TestHelper::init_dummy_tracing_subscriber();

        const PREFIX: &str = "dir2";

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
            helper.enable_bucket_versioning(&BUCKET2.to_string()).await;

            helper.sync_test_data(&target_bucket_url).await;
        }

        let source_bucket_url = format!("s3://{}/{}", BUCKET1.to_string(), PREFIX);
        let target_bucket_url = format!("s3://{}/{}", BUCKET2.to_string(), PREFIX);

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_versions_list = helper.list_object_versions(&BUCKET2.to_string(), "").await;
            assert_eq!(object_versions_list.len(), 2);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_same_prefix() {
        TestHelper::init_dummy_tracing_subscriber();

        const PREFIX: &str = "dir1/data1";

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
            helper.enable_bucket_versioning(&BUCKET2.to_string()).await;

            helper.sync_test_data(&target_bucket_url).await;
            helper.delete_all_objects(&BUCKET1.to_string()).await;
            helper.sync_test_data(&target_bucket_url).await;
            helper.delete_all_objects(&BUCKET1.to_string()).await;
        }

        let source_bucket_url = format!("s3://{}/{}", BUCKET1.to_string(), PREFIX);
        let target_bucket_url = format!("s3://{}/{}", BUCKET2.to_string(), PREFIX);

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 3);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_dry_run() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
            helper.enable_bucket_versioning(&BUCKET2.to_string()).await;

            helper.sync_test_data(&target_bucket_url).await;
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
        let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                "--dry-run",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_versions_list = helper.list_object_versions(&BUCKET2.to_string(), "").await;
            assert_eq!(object_versions_list.len(), 0);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_skip_all() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
            helper.enable_bucket_versioning(&BUCKET2.to_string()).await;

            helper.sync_test_data(&target_bucket_url).await;
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
        let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            let object_versions_list = helper.list_object_versions(&BUCKET2.to_string(), "").await;

            assert_eq!(object_versions_list.len(), 5);
        }

        {
            helper.delete_all_objects(&BUCKET1.to_string()).await;
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_versions_list = helper.list_object_versions(&BUCKET2.to_string(), "").await;
            assert_eq!(object_versions_list.len(), 5);

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 0);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 0);
            assert_eq!(stats.sync_skip, 5);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_multipart_upload() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
            helper.enable_bucket_versioning(&BUCKET2.to_string()).await;

            helper.sync_large_test_data(&target_bucket_url).await;
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
        let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            assert!(
                helper
                    .verify_e_tag(&BUCKET2.to_string(), "large_file", None, LARGE_FILE_S3_ETAG)
                    .await
            );
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_tagging() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
            helper.enable_bucket_versioning(&BUCKET2.to_string()).await;

            helper.sync_test_data(&target_bucket_url).await;
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
        let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_versions_list = helper.list_object_versions(&BUCKET2.to_string(), "").await;
            assert_eq!(object_versions_list.len(), 5);
        }

        {
            helper.delete_all_objects(&BUCKET1.to_string()).await;
        }

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.sync_test_data(&target_bucket_url).await;

            helper
                .put_object_tagging(
                    &BUCKET1.to_string(),
                    "data1",
                    None,
                    Tagging::builder()
                        .tag_set(
                            Tag::builder()
                                .key("updated_key1")
                                .value("updated_value1")
                                .build()
                                .unwrap(),
                        )
                        .tag_set(
                            Tag::builder()
                                .key("updated_key2")
                                .value("updated_value2")
                                .build()
                                .unwrap(),
                        )
                        .build()
                        .unwrap(),
                )
                .await;
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let get_object_tagging_output = helper
                .get_object_tagging(&BUCKET2.to_string(), "data1", None)
                .await;

            let tag_set = get_object_tagging_output.tag_set();

            let tag_map = TestHelper::tag_set_to_map(tag_set);

            let expected_tag_map = HashMap::from([
                ("updated_key1".to_string(), "updated_value1".to_string()),
                ("updated_key2".to_string(), "updated_value2".to_string()),
            ]);

            assert_eq!(tag_map, expected_tag_map);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_version_check() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
            helper.enable_bucket_versioning(&BUCKET2.to_string()).await;

            for object_size in 1..=5 {
                helper
                    .put_sized_object(&BUCKET1.to_string(), "data1", object_size)
                    .await;
            }
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
        let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let get_object_output = helper.get_object(&BUCKET2.to_string(), "data1", None).await;
            assert_eq!(get_object_output.content_length().unwrap(), 5);
        }

        {
            helper
                .delete_object(&BUCKET2.to_string(), "data1", None)
                .await;
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            assert!(
                !helper
                    .is_object_exist(&BUCKET2.to_string(), "data1", None)
                    .await
            );
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
            helper.enable_bucket_versioning(&BUCKET2.to_string()).await;

            for object_size in 1..=5 {
                helper
                    .put_sized_object(&BUCKET1.to_string(), "data1", object_size)
                    .await;
            }
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
        let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                "--sse",
                "aws:kms",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let get_object_output = helper.get_object(&BUCKET2.to_string(), "data1", None).await;
            assert_eq!(get_object_output.content_length().unwrap(), 5);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                "--sse",
                "aws:kms",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let head_object_output = helper
                .head_object(&BUCKET2.to_string(), "data1", None)
                .await;
            assert_eq!(
                head_object_output.server_side_encryption().unwrap(),
                &ServerSideEncryption::AwsKms
            );
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_multipart_upload_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
            helper.enable_bucket_versioning(&BUCKET2.to_string()).await;

            helper.sync_large_test_data(&target_bucket_url).await;
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
        let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                "--sse",
                "aws:kms",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let head_object_output = helper
                .head_object(&BUCKET2.to_string(), "large_file", None)
                .await;
            assert_eq!(
                head_object_output.server_side_encryption().unwrap(),
                &ServerSideEncryption::AwsKms
            );
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn s3_to_s3_with_rate_limit() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
            helper.enable_bucket_versioning(&BUCKET2.to_string()).await;

            helper.sync_test_data(&target_bucket_url).await;
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
        let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_versions_list = helper.list_object_versions(&BUCKET2.to_string(), "").await;
            assert_eq!(object_versions_list.len(), 5);
        }

        {
            helper.delete_all_objects(&BUCKET1.to_string()).await;
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_versions_list = helper.list_object_versions(&BUCKET2.to_string(), "").await;
            assert_eq!(object_versions_list.len(), 5);

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 0);
        }

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--delete",
                "./test_data/e2e_test/case2/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                "--rate-limit-objects",
                "300",
                "--rate-limit-bandwidth",
                "100MiB",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_versions_list = helper.list_object_versions(&BUCKET2.to_string(), "").await;
            assert_eq!(object_versions_list.len(), 9);

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 4);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
            helper.enable_bucket_versioning(&BUCKET2.to_string()).await;

            helper.sync_test_data_with_sha256(&target_bucket_url).await;
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
        let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA256",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_versions_list = helper.list_object_versions(&BUCKET2.to_string(), "").await;
            assert_eq!(object_versions_list.len(), 5);
        }

        {
            helper.delete_all_objects(&BUCKET1.to_string()).await;
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA256",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_versions_list = helper.list_object_versions(&BUCKET2.to_string(), "").await;
            assert_eq!(object_versions_list.len(), 5);

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 0);
        }

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--delete",
                "./test_data/e2e_test/case2/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA256",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_versions_list = helper.list_object_versions(&BUCKET2.to_string(), "").await;
            assert_eq!(object_versions_list.len(), 9);

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 4);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_multipart_upload_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
            helper.enable_bucket_versioning(&BUCKET2.to_string()).await;

            helper.sync_test_data(&target_bucket_url).await;

            helper.delete_all_objects(&BUCKET1.to_string()).await;

            helper
                .sync_large_test_data_with_sha256(&target_bucket_url)
                .await;
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
        let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA256",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }
    #[tokio::test]
    async fn s3_to_s3_with_multipart_upload_checksum_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
            helper.enable_bucket_versioning(&BUCKET2.to_string()).await;

            helper.sync_test_data(&target_bucket_url).await;

            helper.delete_all_objects(&BUCKET1.to_string()).await;

            helper
                .sync_large_test_data_with_crc64nvme(&target_bucket_url)
                .await;
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
        let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_multipart_upload_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
            helper.enable_bucket_versioning(&BUCKET2.to_string()).await;

            helper
                .sync_large_test_data_with_custom_chunksize(&target_bucket_url, "6340033")
                .await;
        }

        {
            helper.delete_all_objects(&BUCKET1.to_string()).await;
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
        let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                "--auto-chunksize",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_warning, 0);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_sse_c() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
            helper.enable_bucket_versioning(&BUCKET2.to_string()).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--target-sse-c",
                "AES256",
                "--target-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--target-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
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
            helper.delete_all_objects(&BUCKET1.to_string()).await;
        }

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--target-sse-c",
                "AES256",
                "--target-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--target-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
        let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                "--target-sse-c",
                "AES256",
                "--target-sse-c-key",
                TEST_SSE_C_KEY_2,
                "--target-sse-c-key-md5",
                TEST_SSE_C_KEY_2_MD5,
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            assert_eq!(
                TestHelper::get_sync_count(pipeline.get_stats_receiver()),
                10
            );
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_sse_c_multipart_upload() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
            helper.enable_bucket_versioning(&BUCKET2.to_string()).await;

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--target-sse-c",
                "AES256",
                "--target-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--target-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            assert_eq!(
                TestHelper::get_warning_count(pipeline.get_stats_receiver()),
                0
            );
        }

        {
            helper.delete_all_objects(&BUCKET1.to_string()).await;
        }

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--target-sse-c",
                "AES256",
                "--target-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--target-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            assert_eq!(
                TestHelper::get_warning_count(pipeline.get_stats_receiver()),
                0
            );
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
        let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                "--target-sse-c",
                "AES256",
                "--target-sse-c-key",
                TEST_SSE_C_KEY_2,
                "--target-sse-c-key-md5",
                TEST_SSE_C_KEY_2_MD5,
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 2);
            assert_eq!(stats.sync_warning, 0);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_max_keys() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
            helper.enable_bucket_versioning(&BUCKET2.to_string()).await;

            helper.sync_test_data(&target_bucket_url).await;
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
        let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                "--max-keys",
                "1",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_versions_list = helper.list_object_versions(&BUCKET2.to_string(), "").await;
            assert_eq!(object_versions_list.len(), 5);
        }

        {
            helper.delete_all_objects(&BUCKET1.to_string()).await;
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                "--max-keys",
                "1",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_versions_list = helper.list_object_versions(&BUCKET2.to_string(), "").await;
            assert_eq!(object_versions_list.len(), 5);

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 0);
        }

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--delete",
                "./test_data/e2e_test/case2/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                "--max-keys",
                "1",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_versions_list = helper.list_object_versions(&BUCKET2.to_string(), "").await;
            assert_eq!(object_versions_list.len(), 9);

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 4);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_cancel() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
            helper.enable_bucket_versioning(&BUCKET2.to_string()).await;

            helper.sync_test_data(&target_bucket_url).await;
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
        let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_versions_list = helper.list_object_versions(&BUCKET2.to_string(), "").await;
            assert_eq!(object_versions_list.len(), 5);
        }

        {
            helper.delete_all_objects(&BUCKET1.to_string()).await;
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_versions_list = helper.list_object_versions(&BUCKET2.to_string(), "").await;
            assert_eq!(object_versions_list.len(), 5);

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 0);
        }

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--delete",
                "./test_data/e2e_test/case2/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-versioning",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token.clone()).await;
            cancellation_token.cancel();

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }
}
