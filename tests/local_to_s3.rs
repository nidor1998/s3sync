#[cfg(test)]
#[cfg(feature = "e2e_test")]
mod common;

#[cfg(test)]
#[cfg(feature = "e2e_test")]
mod tests {
    use std::collections::HashSet;
    use std::convert::TryFrom;

    use aws_sdk_s3::types::{ServerSideEncryption, StorageClass};

    use common::*;
    use s3sync::config::args::parse_from_args;
    use s3sync::config::Config;
    use s3sync::pipeline::Pipeline;
    use s3sync::types::token::create_pipeline_cancellation_token;

    use super::*;

    #[tokio::test]
    async fn local_to_s3_without_prefix() {
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
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&BUCKET1.to_string(), "").await;
            for object in object_list {
                assert!(TestHelper::verify_object_md5_digest(
                    object.key().unwrap(),
                    object.e_tag().unwrap()
                ));
            }
        }

        {
            TestHelper::touch_file("./test_data/e2e_test/case1/data1", TOUCH_FILE_SECS_FROM_NOW);

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

            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 1);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn local_to_s3_with_prefix() {
        TestHelper::init_dummy_tracing_subscriber();

        const TEST_PREFIX: &str = "mydir";

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}/{}/", BUCKET1.to_string(), TEST_PREFIX);

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

            let object_list = helper.list_objects(&BUCKET1.to_string(), "").await;

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
    }

    #[tokio::test]
    async fn local_to_s3_with_delete() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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

            let object_list = helper.list_objects(&BUCKET1.to_string(), "").await;
            assert_eq!(object_list.len(), 4);

            assert!(
                !helper
                    .is_object_exist(&BUCKET1.to_string(), "data1", None)
                    .await
            );
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn local_to_s3_with_delete_dry_run() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--delete",
                "--dry-run",
                "./test_data/e2e_test/case2/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&BUCKET1.to_string(), "").await;
            assert_eq!(object_list.len(), 5);

            assert!(
                helper
                    .is_object_exist(&BUCKET1.to_string(), "data1", None)
                    .await
            );
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn local_to_s3_with_dry_run() {
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
                "--dry-run",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&BUCKET1.to_string(), "").await;
            assert_eq!(object_list.len(), 0);
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn local_to_s3_with_all_skip() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.sync_test_data(&target_bucket_url).await;
        }

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--delete",
                "./test_data/e2e_test/case1/",
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
    }

    #[tokio::test]
    async fn local_to_s3_with_multipart_upload() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            helper.sync_large_test_data(&target_bucket_url).await;

            assert!(
                helper
                    .verify_e_tag(&BUCKET1.to_string(), "large_file", None, LARGE_FILE_S3_ETAG)
                    .await
            );
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn local_to_s3_with_storage_class() {
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

            let head_object_output = helper
                .head_object(&BUCKET1.to_string(), "data1", None)
                .await;
            assert_eq!(
                *head_object_output.storage_class().unwrap(),
                StorageClass::ReducedRedundancy
            );
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn local_to_s3_with_storage_class_multipart_upload() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                LARGE_FILE_DIR,
                "--storage-class",
                "STANDARD_IA",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let head_object_output = helper
                .head_object(&BUCKET1.to_string(), "large_file", None)
                .await;
            assert_eq!(
                *head_object_output.storage_class().unwrap(),
                StorageClass::StandardIa
            );
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn local_to_s3_with_guess_mime_type() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "./test_data/e2e_test/case3/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let head_object_output = helper
                .head_object(&BUCKET1.to_string(), "img.png", None)
                .await;
            assert_eq!(head_object_output.content_type().unwrap(), "image/png");
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn local_to_s3_without_guess_mime_type() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--no-guess-mime-type",
                "./test_data/e2e_test/case3/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let head_object_output = helper
                .head_object(&BUCKET1.to_string(), "img.png", None)
                .await;
            assert_eq!(
                head_object_output.content_type().unwrap(),
                "application/octet-stream"
            );
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn local_to_s3_with_sse_kms() {
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
                "--sse",
                "aws:kms",
                "./test_data/e2e_test/case3/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let head_object_output = helper
                .head_object(&BUCKET1.to_string(), "img.png", None)
                .await;
            assert_eq!(
                head_object_output.server_side_encryption().unwrap(),
                &ServerSideEncryption::AwsKms
            );
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn local_to_s3_with_sse_kms_multipart_upload() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                LARGE_FILE_DIR,
                "--sse",
                "aws:kms",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let head_object_output = helper
                .head_object(&BUCKET1.to_string(), "large_file", None)
                .await;
            assert_eq!(
                head_object_output.server_side_encryption().unwrap(),
                &ServerSideEncryption::AwsKms
            );
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn local_to_s3_with_acl() {
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
                "--acl",
                "bucket-owner-read",
                "./test_data/e2e_test/case3/",
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
    }

    #[tokio::test]
    async fn local_to_s3_with_acl_multipart_upload() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--acl",
                "bucket-owner-read",
                LARGE_FILE_DIR,
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
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn local_to_s3_with_rate_limit() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            helper.sync_test_data(&target_bucket_url).await;

            let object_list = helper.list_objects(&BUCKET1.to_string(), "").await;
            for object in object_list {
                assert!(TestHelper::verify_object_md5_digest(
                    object.key().unwrap(),
                    object.e_tag().unwrap()
                ));
            }
        }

        {
            TestHelper::touch_file("./test_data/e2e_test/case1/data1", TOUCH_FILE_SECS_FROM_NOW);

            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "./test_data/e2e_test/case1/",
                "--rate-limit-objects",
                "300",
                "--rate-limit-bandwidth",
                "100MiB",
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
    }

    #[tokio::test]
    async fn local_to_s3_with_additional_checksum() {
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
                "--additional-checksum-algorithm",
                "SHA256",
                "./test_data/e2e_test/case3/",
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
    }

    #[tokio::test]
    async fn local_to_s3_with_additional_checksum_multipart_upload() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                LARGE_FILE_DIR,
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
    }

    #[tokio::test]
    async fn local_to_s3_with_all_metadata_option() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            helper
                .sync_test_data_with_all_metadata_option(&target_bucket_url)
                .await;

            helper
                .verify_test_object_metadata(&BUCKET1.to_string(), "dir1/data1", None)
                .await;
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn local_to_s3_with_all_metadata_option_multipart_upload() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            helper
                .sync_large_test_data_with_all_metadata_option(&target_bucket_url)
                .await;

            helper
                .verify_test_object_metadata(&BUCKET1.to_string(), LARGE_FILE_KEY, None)
                .await;
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn local_to_s3_with_sse_c() {
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

            assert_eq!(
                TestHelper::get_warning_count(pipeline.get_stats_receiver()),
                0
            );
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn local_to_s3_with_sse_c_multipart_upload() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

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

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn local_to_s3_with_sha256() {
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
                "--additional-checksum-algorithm",
                "SHA256",
                "./test_data/e2e_test/case1/",
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

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn local_to_s3_with_sha1() {
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
                "--additional-checksum-algorithm",
                "SHA1",
                "./test_data/e2e_test/case1/",
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

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn local_to_s3_with_crc32() {
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
                "--additional-checksum-algorithm",
                "CRC32",
                "./test_data/e2e_test/case1/",
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

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn local_to_s3_with_crc32_c() {
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
                "--additional-checksum-algorithm",
                "CRC32C",
                "./test_data/e2e_test/case1/",
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

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn local_to_s3_with_last_modified_metadata() {
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
                "--put-last-modified-metadata",
                "./test_data/e2e_test/case1/",
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

            let object = helper.get_object(&BUCKET1.to_string(), "data1", None).await;
            assert!(object
                .metadata
                .unwrap()
                .contains_key("s3sync_origin_last_modified"));
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }
}
