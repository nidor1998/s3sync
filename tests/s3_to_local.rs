#[cfg(test)]
#[cfg(feature = "e2e_test")]
mod common;

#[cfg(test)]
#[cfg(feature = "e2e_test")]
mod tests {
    use std::convert::TryFrom;

    use common::*;
    use s3sync::config::args::parse_from_args;
    use s3sync::config::Config;
    use s3sync::pipeline::Pipeline;
    use s3sync::types::token::create_pipeline_cancellation_token;

    use super::*;

    #[tokio::test]
    async fn s3_to_local_without_prefix() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

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
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

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

            assert_eq!(
                helper
                    .get_object_last_modified(&BUCKET1.to_string(), "data1", None)
                    .await,
                TestHelper::get_file_last_modified(&format!("{}/data1", TEMP_DOWNLOAD_DIR))
            );
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

            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 1);
        }

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_local_with_prefix() {
        TestHelper::init_dummy_tracing_subscriber();

        const TEST_PREFIX: &str = "dir2";

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

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
            let source_bucket_url =
                format!("s3://{}/{}", BUCKET1.to_string(), TEST_PREFIX.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            let dir_entry_list = TestHelper::list_all_files(TEMP_DOWNLOAD_DIR);
            assert_eq!(dir_entry_list.len(), 2);
        }

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_local_with_same_prefix() {
        TestHelper::init_dummy_tracing_subscriber();

        const TEST_PREFIX: &str = "dir1/data1";

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

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
            let source_bucket_url =
                format!("s3://{}/{}", BUCKET1.to_string(), TEST_PREFIX.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 1);
        }

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_local_with_delete() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

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
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            let dir_entry_list = TestHelper::list_all_files(TEMP_DOWNLOAD_DIR);
            assert_eq!(dir_entry_list.len(), 5);

            helper
                .delete_object(&BUCKET1.to_string(), "data1", None)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--delete",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            let dir_entry_list = TestHelper::list_all_files(TEMP_DOWNLOAD_DIR);
            assert_eq!(dir_entry_list.len(), 4);

            assert!(!TestHelper::is_file_exist(&format!(
                "{}/data1",
                TEMP_DOWNLOAD_DIR
            )));
        }

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_local_with_delete_dry_run() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

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
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            let dir_entry_list = TestHelper::list_all_files(TEMP_DOWNLOAD_DIR);
            assert_eq!(dir_entry_list.len(), 5);

            helper
                .delete_object(&BUCKET1.to_string(), "data1", None)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--delete",
                "--dry-run",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            let dir_entry_list = TestHelper::list_all_files(TEMP_DOWNLOAD_DIR);
            assert_eq!(dir_entry_list.len(), 5);

            assert!(TestHelper::is_file_exist(&format!(
                "{}/data1",
                TEMP_DOWNLOAD_DIR
            )));
        }

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_local_with_dry_run() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

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
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--dry-run",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            let dir_entry_list = TestHelper::list_all_files(TEMP_DOWNLOAD_DIR);
            assert_eq!(dir_entry_list.len(), 0);
        }

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_local_with_skip_all() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            helper.sync_test_data(&target_bucket_url).await;
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            let dir_entry_list = TestHelper::list_all_files(TEMP_DOWNLOAD_DIR);
            assert_eq!(dir_entry_list.len(), 5);
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            assert_eq!(TestHelper::get_skip_count(pipeline.get_stats_receiver()), 5);
        }

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_local_with_empty_directory() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper
                .put_empty_object(&BUCKET1.to_string(), "dir1/dir2/")
                .await;
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            assert!(TestHelper::is_file_exist(&format!(
                "{}{}",
                TEMP_DOWNLOAD_DIR, "dir1/dir2/"
            )));
        }

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            assert!(TestHelper::is_file_exist(&format!(
                "{}{}",
                TEMP_DOWNLOAD_DIR, "dir1/dir2/"
            )));
        }

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_local_with_empty_directory_dry_run() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper
                .put_empty_object(&BUCKET1.to_string(), "dir1/dir2/")
                .await;
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--dry-run",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            assert!(!TestHelper::is_file_exist(&format!(
                "{}{}",
                TEMP_DOWNLOAD_DIR, "dir1/dir2/"
            )));
        }

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn s3_to_local_with_rate_limit() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

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
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

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

            assert_eq!(
                helper
                    .get_object_last_modified(&BUCKET1.to_string(), "data1", None)
                    .await,
                TestHelper::get_file_last_modified(&format!("{}/data1", TEMP_DOWNLOAD_DIR))
            );
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
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--rate-limit-objects",
                "300",
                "--rate-limit-bandwidth",
                "100MiB",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 1);
        }

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_local_with_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            helper.sync_test_data_with_sha256(&target_bucket_url).await;
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

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

            assert_eq!(
                helper
                    .get_object_last_modified(&BUCKET1.to_string(), "data1", None)
                    .await,
                TestHelper::get_file_last_modified(&format!("{}/data1", TEMP_DOWNLOAD_DIR))
            );
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
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 1);
        }

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_local_with_multipart_upload_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            helper
                .sync_large_test_data_with_sha256(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
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

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_local_with_multipart_upload_checksum_sha1() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            helper
                .sync_large_test_data_with_sha1(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
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

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_local_with_multipart_upload_checksum_crc32() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            helper
                .sync_large_test_data_with_crc32(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
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

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_local_with_multipart_upload_checksum_crc32_c() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            helper
                .sync_large_test_data_with_crc32_c(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
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

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_local_with_multipart_upload_auto_chunksize() {
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
                .sync_large_test_data_with_custom_chunksize(&target_bucket_url, "7340033")
                .await;
        }

        let source_bucket_url = format!("s3://{}", BUCKET1.to_string());

        {
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
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
    }

    #[tokio::test]
    async fn s3_to_local_with_sse_c() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

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

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
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

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_local_with_sse_c_multipart_upload() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

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

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
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

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_local_with_empty_data_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            helper.sync_empty_data_with_sha256(&target_bucket_url).await;
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
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

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_local_with_etag_warning() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            helper
                .sync_large_test_data_with_custom_chunksize(&target_bucket_url, "5MiB")
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
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
                1
            );
        }

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_local_with_max_keys() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

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
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--max-keys",
                "2",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

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

            assert_eq!(
                helper
                    .get_object_last_modified(&BUCKET1.to_string(), "data1", None)
                    .await,
                TestHelper::get_file_last_modified(&format!("{}/data1", TEMP_DOWNLOAD_DIR))
            );
        }

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_local_with_multipart_upload_checksum_max_keys() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            helper
                .sync_large_test_data_with_sha256(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--max-keys",
                "1",
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
        }

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_local_with_directory_traversal_error() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            helper
                .put_object_with_metadata(
                    &BUCKET1.to_string(),
                    "data1/../data2",
                    "./test_data/e2e_test/case1/data1",
                )
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 0);
            assert_eq!(stats.etag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 1);
        }

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }
}
