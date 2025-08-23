#[cfg(test)]
#[cfg(feature = "e2e_test")]
mod common;

#[cfg(test)]
#[cfg(feature = "e2e_test")]
mod tests {
    use std::collections::{HashMap, HashSet};

    use aws_sdk_s3::types::{ServerSideEncryption, StorageClass, Tag, Tagging};

    use common::*;
    use s3sync::Config;
    use s3sync::config::args::parse_from_args;
    use s3sync::pipeline::Pipeline;
    use s3sync::types::token::create_pipeline_cancellation_token;

    use super::*;

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
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 5);

            for object in object_list {
                assert!(TestHelper::verify_object_md5_digest(
                    object.key().unwrap(),
                    object.e_tag().unwrap()
                ));
            }
        }

        {
            tokio::time::sleep(std::time::Duration::from_secs(SLEEP_SECS_BEFORE_RESYNC)).await;

            helper
                .put_object_with_metadata(
                    &BUCKET1.to_string(),
                    "data1",
                    "./test_data/e2e_test/case1/data1",
                )
                .await;

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 1);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_without_prefix_no_parallel_listing() {
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
                "--max-parallel-listings",
                "1",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 5);

            for object in object_list {
                assert!(TestHelper::verify_object_md5_digest(
                    object.key().unwrap(),
                    object.e_tag().unwrap()
                ));
            }
        }

        {
            tokio::time::sleep(std::time::Duration::from_secs(SLEEP_SECS_BEFORE_RESYNC)).await;

            helper
                .put_object_with_metadata(
                    &BUCKET1.to_string(),
                    "data1",
                    "./test_data/e2e_test/case1/data1",
                )
                .await;

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--max-parallel-listings",
                "1",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 1);
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

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        const TEST_PREFIX: &str = "mydir";

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}/{}/", BUCKET1.to_string(), TEST_PREFIX);

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;

            helper.sync_test_data(&target_bucket_url).await;
        }

        let source_bucket_url = format!("s3://{}/{}/", BUCKET1.to_string(), TEST_PREFIX);
        let target_bucket_url = format!("s3://{}/{}/", BUCKET2.to_string(), TEST_PREFIX);

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 5);

            for object in object_list {
                let key_set = HashSet::from([
                    format!("{}/dir2/data2", TEST_PREFIX),
                    format!("{}/dir5/data3", TEST_PREFIX),
                    format!("{}/data1", TEST_PREFIX),
                    format!("{}/dir21/data1", TEST_PREFIX),
                    format!("{}/dir1/data1", TEST_PREFIX),
                ]);

                assert!(key_set.get(object.key.as_ref().unwrap()).is_some())
            }
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_prefix_server_side_copy() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        const TEST_PREFIX: &str = "mydir";

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}/{}/", BUCKET1.to_string(), TEST_PREFIX);

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;

            helper.sync_test_data(&target_bucket_url).await;
        }

        let source_bucket_url = format!("s3://{}/{}/", BUCKET1.to_string(), TEST_PREFIX);
        let target_bucket_url = format!("s3://{}/{}/", BUCKET2.to_string(), TEST_PREFIX);

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 5);

            for object in object_list {
                let key_set = HashSet::from([
                    format!("{}/dir2/data2", TEST_PREFIX),
                    format!("{}/dir5/data3", TEST_PREFIX),
                    format!("{}/data1", TEST_PREFIX),
                    format!("{}/dir21/data1", TEST_PREFIX),
                    format!("{}/dir1/data1", TEST_PREFIX),
                ]);

                assert!(key_set.get(object.key.as_ref().unwrap()).is_some())
            }
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_prefix_server_side_copy_all_metadata() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        const TEST_PREFIX: &str = "mydir";

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}/{}/", BUCKET1.to_string(), TEST_PREFIX);

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;

            helper.sync_test_data(&target_bucket_url).await;
        }

        let source_bucket_url = format!("s3://{}/{}/", BUCKET1.to_string(), TEST_PREFIX);
        let target_bucket_url = format!("s3://{}/{}/", BUCKET2.to_string(), TEST_PREFIX);

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--content-disposition",
                TEST_CONTENT_DISPOSITION,
                "--content-encoding",
                TEST_CONTENT_ENCODING,
                "--content-language",
                TEST_CONTENT_LANGUAGE,
                "--cache-control",
                TEST_CACHE_CONTROL,
                "--content-type",
                TEST_CONTENT_TYPE,
                "--expires",
                TEST_EXPIRES,
                "--metadata",
                TEST_METADATA_STRING,
                "--tagging",
                TEST_TAGGING,
                "--server-side-copy",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 5);

            helper
                .verify_test_object_metadata(&BUCKET2.to_string(), "mydir/data1", None)
                .await;
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_prefix_all_metadata() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        const TEST_PREFIX: &str = "mydir";

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}/{}/", BUCKET1.to_string(), TEST_PREFIX);

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;

            helper.sync_test_data(&target_bucket_url).await;
        }

        let source_bucket_url = format!("s3://{}/{}/", BUCKET1.to_string(), TEST_PREFIX);
        let target_bucket_url = format!("s3://{}/{}/", BUCKET2.to_string(), TEST_PREFIX);

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--content-disposition",
                TEST_CONTENT_DISPOSITION,
                "--content-encoding",
                TEST_CONTENT_ENCODING,
                "--content-language",
                TEST_CONTENT_LANGUAGE,
                "--cache-control",
                TEST_CACHE_CONTROL,
                "--content-type",
                TEST_CONTENT_TYPE,
                "--expires",
                TEST_EXPIRES,
                "--metadata",
                TEST_METADATA_STRING,
                "--tagging",
                TEST_TAGGING,
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 5);

            helper
                .verify_test_object_metadata(&BUCKET2.to_string(), "mydir/data1", None)
                .await;
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_prefix_multipart_upload_server_side_copy_all_metadata() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        const TEST_PREFIX: &str = "mydir";

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}/{}/", BUCKET1.to_string(), TEST_PREFIX);

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;

            helper.sync_large_test_data(&target_bucket_url).await;
        }

        let source_bucket_url = format!("s3://{}/{}/", BUCKET1.to_string(), TEST_PREFIX);
        let target_bucket_url = format!("s3://{}/{}/", BUCKET2.to_string(), TEST_PREFIX);

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--content-disposition",
                TEST_CONTENT_DISPOSITION,
                "--content-encoding",
                TEST_CONTENT_ENCODING,
                "--content-language",
                TEST_CONTENT_LANGUAGE,
                "--cache-control",
                TEST_CACHE_CONTROL,
                "--content-type",
                TEST_CONTENT_TYPE,
                "--expires",
                TEST_EXPIRES,
                "--metadata",
                TEST_METADATA_STRING,
                "--tagging",
                TEST_TAGGING,
                "--server-side-copy",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 1);

            helper
                .verify_test_object_metadata(&BUCKET2.to_string(), "mydir/large_file", None)
                .await;
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_prefix_multipart_upload_all_metadata() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        const TEST_PREFIX: &str = "mydir";

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}/{}/", BUCKET1.to_string(), TEST_PREFIX);

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;

            helper.sync_large_test_data(&target_bucket_url).await;
        }

        let source_bucket_url = format!("s3://{}/{}/", BUCKET1.to_string(), TEST_PREFIX);
        let target_bucket_url = format!("s3://{}/{}/", BUCKET2.to_string(), TEST_PREFIX);

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--content-disposition",
                TEST_CONTENT_DISPOSITION,
                "--content-encoding",
                TEST_CONTENT_ENCODING,
                "--content-language",
                TEST_CONTENT_LANGUAGE,
                "--cache-control",
                TEST_CACHE_CONTROL,
                "--content-type",
                TEST_CONTENT_TYPE,
                "--expires",
                TEST_EXPIRES,
                "--metadata",
                TEST_METADATA_STRING,
                "--tagging",
                TEST_TAGGING,
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 1);

            helper
                .verify_test_object_metadata(&BUCKET2.to_string(), "mydir/large_file", None)
                .await;
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
                "--dry-run",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 0);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_server_side_copy_dry_run() {
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
                "--server-side-copy",
                "--dry-run",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 0);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_delete() {
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
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 5);
        }

        {
            helper
                .delete_object(&BUCKET1.to_string(), "data1", None)
                .await;
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--delete",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 4);

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
    async fn s3_to_s3_with_delete_dry_run() {
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
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 5);
        }

        {
            helper
                .delete_object(&BUCKET1.to_string(), "data1", None)
                .await;
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--delete",
                "--dry-run",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 5);

            assert!(
                helper
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
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 5);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            assert_eq!(TestHelper::get_skip_count(pipeline.get_stats_receiver()), 5);
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
    async fn s3_to_s3_metadata_test() {
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

            helper
                .put_object_with_metadata(
                    &BUCKET1.to_string(),
                    "data1",
                    "./test_data/e2e_test/case1/data1",
                )
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
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            helper
                .verify_test_object_metadata(&BUCKET2.to_string(), "data1", None)
                .await;
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_no_system_metadata_test() {
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

            helper
                .put_object_with_metadata(
                    &BUCKET1.to_string(),
                    "data1",
                    "./test_data/e2e_test/case1/data1",
                )
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
                "--no-sync-system-metadata",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            helper
                .verify_test_object_no_system_metadata(&BUCKET2.to_string(), "data1", None)
                .await;
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_no_user_defined_metadata_test() {
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

            helper
                .put_object_with_metadata(
                    &BUCKET1.to_string(),
                    "data1",
                    "./test_data/e2e_test/case1/data1",
                )
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
                "--no-sync-user-defined-metadata",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            helper
                .verify_test_object_no_user_defined_metadata(&BUCKET2.to_string(), "data1", None)
                .await;
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_sever_side_copy_metadata_test() {
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

            helper
                .put_object_with_metadata(
                    &BUCKET1.to_string(),
                    "data1",
                    "./test_data/e2e_test/case1/data1",
                )
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
                "--server-side-copy",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            helper
                .verify_test_object_metadata(&BUCKET2.to_string(), "data1", None)
                .await;
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_attribute_test_disable_tagging() {
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

            helper
                .put_object_with_metadata(
                    &BUCKET1.to_string(),
                    "data1",
                    "./test_data/e2e_test/case1/data1",
                )
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
                "--disable-tagging",
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
            assert!(tag_set.is_empty());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_metadata_test_with_multipart_upload() {
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

            TestHelper::create_large_file();

            helper
                .put_object_with_metadata(&BUCKET1.to_string(), "data1", LARGE_FILE_PATH)
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
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            helper
                .verify_test_object_metadata(&BUCKET2.to_string(), "data1", None)
                .await;
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_metadata_test_with_multipart_upload_server_side_copy() {
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

            TestHelper::create_large_file();

            helper
                .put_object_with_metadata(&BUCKET1.to_string(), "data1", LARGE_FILE_PATH)
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
                "--server-side-copy",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            helper
                .verify_test_object_metadata(&BUCKET2.to_string(), "data1", None)
                .await;
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_metadata_test_with_multipart_upload_server_side_copy_auto_chunksize() {
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

            TestHelper::create_large_file();

            helper
                .put_object_with_metadata(&BUCKET1.to_string(), "data1", LARGE_FILE_PATH)
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
                "--server-side-copy",
                "--auto-chunksize",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            helper
                .verify_test_object_metadata(&BUCKET2.to_string(), "data1", None)
                .await;
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
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;

            helper
                .put_object_with_metadata(
                    &BUCKET1.to_string(),
                    "data1",
                    "./test_data/e2e_test/case1/data1",
                )
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
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
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

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--sync-latest-tagging",
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

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--sync-latest-tagging",
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

        {
            helper
                .delete_object_tagging(&BUCKET1.to_string(), "data1", None)
                .await;

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--sync-latest-tagging",
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
                    .get_object_tagging(&BUCKET2.to_string(), "data1", None)
                    .await
                    .tag_set()
                    .is_empty()
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
    async fn s3_to_s3_with_tagging_dry_run() {
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

            helper
                .put_object_with_metadata(
                    &BUCKET1.to_string(),
                    "data1",
                    "./test_data/e2e_test/case1/data1",
                )
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
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
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

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--sync-latest-tagging",
                "--dry-run",
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
                ("tag1".to_string(), "tag_value1".to_string()),
                ("tag2".to_string(), "tag_value2".to_string()),
            ]);

            assert_eq!(tag_map, expected_tag_map);
        }

        {
            helper
                .delete_object_tagging(&BUCKET1.to_string(), "data1", None)
                .await;
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--sync-latest-tagging",
                "--dry-run",
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
                ("tag1".to_string(), "tag_value1".to_string()),
                ("tag2".to_string(), "tag_value2".to_string()),
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
    async fn s3_to_s3_with_copy_storage_class() {
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
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "./test_data/e2e_test/case1/",
                "--storage-class",
                "REDUCED_REDUNDANCY",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&BUCKET1.to_string(), "").await;
            assert_eq!(object_list.len(), 5);
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
                *head_object_output.storage_class().unwrap(),
                StorageClass::ReducedRedundancy
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
    async fn s3_to_s3_with_specific_storage_class() {
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
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "./test_data/e2e_test/case1/",
                "--storage-class",
                "REDUCED_REDUNDANCY",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&BUCKET1.to_string(), "").await;
            assert_eq!(object_list.len(), 5);
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
                "--storage-class",
                "INTELLIGENT_TIERING",
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
                *head_object_output.storage_class().unwrap(),
                StorageClass::IntelligentTiering
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
    async fn s3_to_s3_with_empty_directory() {
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

            helper
                .put_empty_object(&BUCKET1.to_string(), "dir1/dir2/")
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
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            assert_eq!(
                helper
                    .head_object(&BUCKET2.to_string(), "dir1/dir2/", None)
                    .await
                    .content_length()
                    .unwrap(),
                0
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
        }

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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

            let object_list = helper.list_objects(&BUCKET1.to_string(), "").await;
            assert_eq!(object_list.len(), 5);
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
    async fn s3_to_s3_with_dsse_kms() {
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
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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

            let object_list = helper.list_objects(&BUCKET1.to_string(), "").await;
            assert_eq!(object_list.len(), 5);
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
                "--sse",
                "aws:kms:dsse",
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
                &ServerSideEncryption::AwsKmsDsse
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
    async fn s3_to_s3_with_multipart_upload_dsse_kms() {
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

            TestHelper::create_large_file();

            helper
                .put_object_with_metadata(&BUCKET1.to_string(), "data1", LARGE_FILE_PATH)
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
                "--sse",
                "aws:kms:dsse",
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
                &ServerSideEncryption::AwsKmsDsse
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
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;

            TestHelper::create_large_file();

            helper
                .put_object_with_metadata(&BUCKET1.to_string(), "data1", LARGE_FILE_PATH)
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
    async fn s3_to_s3_with_acl() {
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
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 5);

            for object in object_list {
                assert!(TestHelper::verify_object_md5_digest(
                    object.key().unwrap(),
                    object.e_tag().unwrap()
                ));
            }
        }

        {
            tokio::time::sleep(std::time::Duration::from_secs(SLEEP_SECS_BEFORE_RESYNC)).await;

            helper
                .put_object_with_metadata(
                    &BUCKET1.to_string(),
                    "data1",
                    "./test_data/e2e_test/case1/data1",
                )
                .await;

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--acl",
                "bucket-owner-read",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 1);
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

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 5);

            for object in object_list {
                assert!(TestHelper::verify_object_md5_digest(
                    object.key().unwrap(),
                    object.e_tag().unwrap()
                ));
            }
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_additional_checksum() {
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

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 5);

            for object in object_list {
                assert!(TestHelper::verify_object_md5_digest(
                    object.key().unwrap(),
                    object.e_tag().unwrap()
                ));
            }
        }

        {
            tokio::time::sleep(std::time::Duration::from_secs(SLEEP_SECS_BEFORE_RESYNC)).await;

            helper
                .put_object_with_metadata(
                    &BUCKET1.to_string(),
                    "data1",
                    "./test_data/e2e_test/case1/data1",
                )
                .await;

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
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

            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 1);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_additional_checksum_crc64nvme_checksum() {
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

            helper
                .sync_test_data_with_crc64nvme(&target_bucket_url)
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

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 5);

            for object in object_list {
                assert!(TestHelper::verify_object_md5_digest(
                    object.key().unwrap(),
                    object.e_tag().unwrap()
                ));
            }
        }

        {
            tokio::time::sleep(std::time::Duration::from_secs(SLEEP_SECS_BEFORE_RESYNC)).await;

            helper
                .put_object_with_metadata(
                    &BUCKET1.to_string(),
                    "data1",
                    "./test_data/e2e_test/case1/data1",
                )
                .await;

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
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

            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 1);
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
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
    async fn s3_to_s3_with_multipart_upload_crc32_full_object_checksum() {
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

            helper
                .sync_large_test_data_with_crc32_full_object_checksum(&target_bucket_url)
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
                "--full-object-checksum",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "CRC32",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
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
    async fn s3_to_s3_with_multipart_upload_crc32c_full_object_checksum() {
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

            helper
                .sync_large_test_data_with_crc32c_full_object_checksum(&target_bucket_url)
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
                "--full-object-checksum",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "CRC32C",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
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

            helper
                .sync_large_test_data_with_custom_chunksize(&target_bucket_url, "7340033")
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
            assert_eq!(stats.sync_complete, 1);
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
    async fn s3_to_s3_with_multipart_upload_auto_chunksize_sha1() {
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

            helper
                .sync_large_test_data_with_custom_chunksize_sha1(&target_bucket_url, "7340033")
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
                "--auto-chunksize",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA1",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
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
    async fn s3_to_s3_with_multipart_upload_auto_chunksize_disable_payload_signing() {
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

            helper
                .sync_large_test_data_with_custom_chunksize(&target_bucket_url, "7340033")
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
                "--auto-chunksize",
                "--disable-payload-signing",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
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
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;

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
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_2,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_2_MD5,
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
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

            let dir_entry_list = TestHelper::list_all_files(TEMP_DOWNLOAD_DIR);

            for entry in dir_entry_list {
                let path = entry
                    .path()
                    .to_string_lossy()
                    .replace(TEMP_DOWNLOAD_DIR, "");

                assert!(TestHelper::verify_file_md5_digest(
                    &format!("./test_data/e2e_test/case1/{}", &path),
                    &TestHelper::md5_digest(&entry.path().to_string_lossy()),
                ));
            }
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_sse_c_server_side_copy() {
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
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;

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
                "--server-side-copy",
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
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_2,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_2_MD5,
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
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

            let dir_entry_list = TestHelper::list_all_files(TEMP_DOWNLOAD_DIR);

            for entry in dir_entry_list {
                let path = entry
                    .path()
                    .to_string_lossy()
                    .replace(TEMP_DOWNLOAD_DIR, "");

                assert!(TestHelper::verify_file_md5_digest(
                    &format!("./test_data/e2e_test/case1/{}", &path),
                    &TestHelper::md5_digest(&entry.path().to_string_lossy()),
                ));
            }
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
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_2,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_2_MD5,
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
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
    async fn s3_to_s3_with_sse_c_multipart_upload_server_side_copy() {
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
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;

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
                "--server-side-copy",
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
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_2,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_2_MD5,
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
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
    async fn s3_to_s3_attribute_test_with_etag_warning() {
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

            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper
                .sync_large_test_data_with_custom_chunksize(&target_bucket_url, "5MiB")
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
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            assert_eq!(
                TestHelper::get_warning_count(pipeline.get_stats_receiver()),
                1
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
    async fn s3_to_s3_with_last_modified_metadata() {
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
                "--put-last-modified-metadata",
                &source_bucket_url,
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

            let object = helper.get_object(&BUCKET2.to_string(), "data1", None).await;
            assert!(
                object
                    .metadata
                    .unwrap()
                    .contains_key("s3sync_origin_last_modified")
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
    async fn s3_to_s3_with_etag_check() {
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
                "--check-etag",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 5);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-etag",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(TestHelper::get_skip_count(pipeline.get_stats_receiver()), 5);
        }

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--remove-modified-filter",
                "./test_data/e2e_test/case1_2/",
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
                "--check-etag",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 1);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-etag",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(TestHelper::get_skip_count(pipeline.get_stats_receiver()), 5);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_etag_check_source_warn() {
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

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--sse",
                "aws:kms",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 5);
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
                "--check-etag",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 5);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--head-each-target",
                "--check-etag",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(
                TestHelper::get_warning_count(pipeline.get_stats_receiver()),
                5
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
    async fn s3_to_s3_with_etag_check_target_warn() {
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
            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 5);
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
            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 5);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--head-each-target",
                "--check-etag",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(
                TestHelper::get_warning_count(pipeline.get_stats_receiver()),
                5
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
    async fn s3_to_s3_with_etag_check_multipart_upload() {
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
                "--check-etag",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 1);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-etag",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(TestHelper::get_skip_count(pipeline.get_stats_receiver()), 1);
        }

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            TestHelper::create_large_file_case2();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--remove-modified-filter",
                "--additional-checksum-algorithm",
                "SHA256",
                LARGE_FILE_DIR_CASE2,
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
                "--check-etag",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 1);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-etag",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(TestHelper::get_skip_count(pipeline.get_stats_receiver()), 1);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_checksum_check() {
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
                "--check-additional-checksum",
                "SHA256",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 5);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(TestHelper::get_skip_count(pipeline.get_stats_receiver()), 5);
        }

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--remove-modified-filter",
                "--additional-checksum-algorithm",
                "SHA256",
                "./test_data/e2e_test/case1_2/",
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
                "--check-additional-checksum",
                "SHA256",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 1);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(TestHelper::get_skip_count(pipeline.get_stats_receiver()), 5);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_checksum_check_kms() {
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

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--additional-checksum-algorithm",
                "SHA256",
                "--sse",
                "aws:kms",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 5);
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
                "--check-additional-checksum",
                "SHA256",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
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
            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 5);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
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
            assert_eq!(TestHelper::get_skip_count(pipeline.get_stats_receiver()), 5);
        }

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--remove-modified-filter",
                "--additional-checksum-algorithm",
                "SHA256",
                "./test_data/e2e_test/case1_2/",
                "--sse",
                "aws:kms",
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
                "--check-additional-checksum",
                "SHA256",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
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
            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 1);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
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
            assert_eq!(TestHelper::get_skip_count(pipeline.get_stats_receiver()), 5);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_checksum_check_sse_c() {
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

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--additional-checksum-algorithm",
                "SHA256",
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
            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 5);
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
                "--check-additional-checksum",
                "SHA256",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                "--target-sse-c",
                "AES256",
                "--target-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--target-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 5);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                "--target-sse-c",
                "AES256",
                "--target-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--target-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(TestHelper::get_skip_count(pipeline.get_stats_receiver()), 5);
        }

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--remove-modified-filter",
                "--additional-checksum-algorithm",
                "SHA256",
                "--target-sse-c",
                "AES256",
                "--target-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--target-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                "./test_data/e2e_test/case1_2/",
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
                "--check-additional-checksum",
                "SHA256",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                "--target-sse-c",
                "AES256",
                "--target-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--target-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 1);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                "--target-sse-c",
                "AES256",
                "--target-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--target-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(TestHelper::get_skip_count(pipeline.get_stats_receiver()), 5);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_checksum_check_warn() {
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
            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 5);
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
                "--check-additional-checksum",
                "SHA256",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(
                TestHelper::get_warning_count(pipeline.get_stats_receiver()),
                5
            );
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
            assert_eq!(
                TestHelper::get_warning_count(pipeline.get_stats_receiver()),
                5
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
    async fn s3_to_s3_with_disable_payload_signing() {
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
                "--disable-payload-signing",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&BUCKET2.to_string(), "").await;
            assert_eq!(object_list.len(), 5);

            for object in object_list {
                assert!(TestHelper::verify_object_md5_digest(
                    object.key().unwrap(),
                    object.e_tag().unwrap()
                ));
            }
        }

        {
            tokio::time::sleep(std::time::Duration::from_secs(SLEEP_SECS_BEFORE_RESYNC)).await;

            helper
                .put_object_with_metadata(
                    &BUCKET1.to_string(),
                    "data1",
                    "./test_data/e2e_test/case1/data1",
                )
                .await;

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-payload-signing",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 1);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_disable_payload_signing_multipart_upload() {
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
                "--disable-payload-signing",
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
    async fn s3_to_s3_with_website_redirect() {
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

            helper
                .sync_test_data_with_website_redirect(&target_bucket_url, "/redirect1")
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
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object = helper
                .head_object(&BUCKET2.to_string(), "dir1/data1", None)
                .await;
            assert_eq!(
                object.website_redirect_location,
                Some("/redirect1".to_string())
            );
        }

        {
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            tokio::time::sleep(std::time::Duration::from_secs(SLEEP_SECS_BEFORE_RESYNC)).await;

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--website-redirect",
                "/redirect777",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object = helper
                .head_object(&BUCKET2.to_string(), "dir1/data1", None)
                .await;
            assert_eq!(
                object.website_redirect_location,
                Some("/redirect777".to_string())
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
    async fn s3_to_s3_with_website_redirect_server_side_copy() {
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

            helper
                .sync_test_data_with_website_redirect(&target_bucket_url, "/redirect1")
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
                "--server-side-copy",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object = helper
                .head_object(&BUCKET2.to_string(), "dir1/data1", None)
                .await;
            assert_eq!(
                object.website_redirect_location,
                Some("/redirect1".to_string())
            );
        }

        {
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            tokio::time::sleep(std::time::Duration::from_secs(SLEEP_SECS_BEFORE_RESYNC)).await;

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--website-redirect",
                "/redirect777",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object = helper
                .head_object(&BUCKET2.to_string(), "dir1/data1", None)
                .await;
            assert_eq!(
                object.website_redirect_location,
                Some("/redirect777".to_string())
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
    async fn s3_to_s3_with_multipart_upload_website_redirect() {
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

            helper
                .sync_large_test_data_with_website_redirect(&target_bucket_url, "/redirect")
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
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object = helper
                .head_object(&BUCKET1.to_string(), "large_file", None)
                .await;
            assert_eq!(
                object.website_redirect_location,
                Some("/redirect".to_string())
            );
        }

        {
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            tokio::time::sleep(std::time::Duration::from_secs(SLEEP_SECS_BEFORE_RESYNC)).await;

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--website-redirect",
                "/redirect777",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object = helper
                .head_object(&BUCKET2.to_string(), "large_file", None)
                .await;
            assert_eq!(
                object.website_redirect_location,
                Some("/redirect777".to_string())
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
    async fn s3_to_s3_with_multipart_upload_website_redirect_server_side_copy() {
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

            helper
                .sync_large_test_data_with_website_redirect(&target_bucket_url, "/redirect")
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
                "--server-side-copy",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object = helper
                .head_object(&BUCKET1.to_string(), "large_file", None)
                .await;
            assert_eq!(
                object.website_redirect_location,
                Some("/redirect".to_string())
            );
        }

        {
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            tokio::time::sleep(std::time::Duration::from_secs(SLEEP_SECS_BEFORE_RESYNC)).await;

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--website-redirect",
                "/redirect777",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object = helper
                .head_object(&BUCKET2.to_string(), "large_file", None)
                .await;
            assert_eq!(
                object.website_redirect_location,
                Some("/redirect777".to_string())
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
    async fn s3_to_s3_mtime_checksum_check() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;

            TestHelper::create_case3_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-chunksize",
                "5MiB",
                "--check-mtime-and-additional-checksum",
                "SHA256",
                "--additional-checksum-algorithm",
                "SHA256",
                LARGE_FILE_DIR_CASE3,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
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
                "--check-mtime-and-additional-checksum",
                "SHA256",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            TestHelper::update_case3_large_file_mtime();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-chunksize",
                "5MiB",
                "--remove-modified-filter",
                "--additional-checksum-algorithm",
                "SHA256",
                LARGE_FILE_DIR_CASE3,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-mtime-and-additional-checksum",
                "SHA256",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
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
            assert_eq!(stats.sync_complete, 0);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 1);
        }

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            TestHelper::modify_case3_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-chunksize",
                "5MiB",
                "--check-mtime-and-additional-checksum",
                "SHA256",
                "--additional-checksum-algorithm",
                "SHA256",
                LARGE_FILE_DIR_CASE3,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-mtime-and-additional-checksum",
                "SHA256",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }
    }

    #[tokio::test]
    async fn s3_to_s3_metadata_filtering() {
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

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--metadata",
                "stage=first_stage,test=s3_to_s3_metadata_filtering",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--metadata",
                "stage=second_stage,test=s3_to_s3_metadata_filtering",
                "./test_data/e2e_test/case5/",
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
                "--filter-include-metadata-regex",
                "^(?!.*,test=s3_to_s3_metadata_filtering).*stage=first_stage",
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
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 6);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-include-metadata-regex",
                "stage=first_stage",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 1);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-include-metadata-regex",
                "stage=second_stage",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 5);
        }

        helper.delete_all_objects(&BUCKET2.to_string()).await;

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-exclude-metadata-regex",
                "stage=first_stage",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 5);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-exclude-metadata-regex",
                "stage=second_stage",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 1);
        }

        helper.delete_all_objects(&BUCKET2.to_string()).await;

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-exclude-metadata-regex",
                "^(?!.*,test=s3_to_s3_metadata_filtering).*stage=first_stage",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 6);
            assert_eq!(stats.e_tag_verified, 6);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_metadata_filtering_no_metadata() {
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

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "./test_data/e2e_test/case5/",
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
                "--filter-include-metadata-regex",
                "stage=first_stage",
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
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 6);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-include-metadata-regex",
                "stage=second_stage",
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
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 6);
        }

        helper.delete_all_objects(&BUCKET2.to_string()).await;

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-exclude-metadata-regex",
                "stage=first_stage",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 6);
            assert_eq!(stats.e_tag_verified, 6);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-exclude-metadata-regex",
                "stage=second_stage",
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
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 6);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_tag_filtering() {
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

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--tagging",
                "stage=first_stage&test=s3_to_s3_tag_filtering",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--tagging",
                "stage=second_stage&test=s3_to_s3_tag_filtering",
                "./test_data/e2e_test/case5/",
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
                "--filter-include-tag-regex",
                "^(?!.*&test=s3_to_s3_tag_filtering).*stage=first_stage",
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
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 6);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-include-tag-regex",
                "stage=first_stage",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 1);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-include-tag-regex",
                "stage=second_stage",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 5);
        }

        helper.delete_all_objects(&BUCKET2.to_string()).await;

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-exclude-tag-regex",
                "stage=first_stage",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 5);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-exclude-tag-regex",
                "stage=second_stage",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 1);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_tag_filtering_no_tag() {
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

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "./test_data/e2e_test/case5/",
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
                "--filter-include-tag-regex",
                "stage=first_stage",
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
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 6);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-include-tag-regex",
                "stage=second_stage",
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
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 6);
        }

        helper.delete_all_objects(&BUCKET2.to_string()).await;

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-exclude-tag-regex",
                "stage=first_stage",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 6);
            assert_eq!(stats.e_tag_verified, 6);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-exclude-tag-regex",
                "stage=second_stage",
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
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 6);
        }

        helper.delete_all_objects(&BUCKET2.to_string()).await;

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-exclude-tag-regex",
                "^(?!.*&test=s3_to_s3_tag_filtering).*stage=first_stage",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 6);
            assert_eq!(stats.e_tag_verified, 6);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_content_type_filtering() {
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

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--content-type",
                "application/pdf",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--content-type",
                "video/mpeg",
                "./test_data/e2e_test/case5/",
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
                "--filter-include-content-type-regex",
                "application/.+",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 1);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-include-content-type-regex",
                "video/.+",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 5);
        }

        helper.delete_all_objects(&BUCKET2.to_string()).await;

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-exclude-content-type-regex",
                "application/.+",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 5);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--filter-exclude-content-type-regex",
                "video/.+",
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 1);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }
}
