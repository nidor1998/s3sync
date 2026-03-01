#![cfg(e2e_test)]
#[cfg(test)]
mod common;

pub const EXPRESS_ONE_ZONE_BUCKET_SUFFIX: &str = "--apne1-az4--x-s3";

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use common::*;
    use uuid::Uuid;
    use s3sync::config::Config;
    use s3sync::config::args::parse_from_args;
    use s3sync::pipeline::Pipeline;
    use s3sync::types::token::create_pipeline_cancellation_token;

    use super::*;

    #[tokio::test]
    async fn local_to_s3_with_disable_stalled_protection() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket1 = format!("{}{}", TestHelper::generate_bucket_name(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let helper = TestHelper::new().await;
        let test_dir = format!("./playground/case1_{}/", Uuid::new_v4());
        TestHelper::copy_dir_all("./test_data/e2e_test/case1", &test_dir);

        {
            let target_bucket_url = format!("s3://{}", &bucket1);
            helper
                .create_directory_bucket(&bucket1, EXPRESS_ONE_ZONE_AZ)
                .await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-stalled-stream-protection",
                &test_dir,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&bucket1, "").await;
            assert_eq!(object_list.len(), 5);
        }

        {
            let touch_path = format!("{}data1", &test_dir);
            TestHelper::touch_file(&touch_path, TOUCH_FILE_SECS_FROM_NOW);

            let target_bucket_url = format!("s3://{}", &bucket1);
            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                &test_dir,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            assert_eq!(TestHelper::get_sync_count(pipeline.get_stats_receiver()), 1);
        }

        helper.delete_directory_bucket_with_cascade(&bucket1).await;
        let _ = std::fs::remove_dir_all(&test_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_multipart_upload() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket1 = format!("{}{}", TestHelper::generate_bucket_name(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", &bucket1);
            helper
                .create_directory_bucket(&bucket1, EXPRESS_ONE_ZONE_AZ)
                .await;

            helper.sync_large_test_data(&target_bucket_url).await;
        }

        helper.delete_directory_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_with_delete() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket1 = format!("{}{}", TestHelper::generate_bucket_name(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let helper = TestHelper::new().await;

        let target_bucket_url = format!("s3://{}", bucket1);

        {
            helper
                .create_directory_bucket(&bucket1, EXPRESS_ONE_ZONE_AZ)
                .await;
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

            let object_list = helper.list_objects(&bucket1, "").await;
            assert_eq!(object_list.len(), 4);

            assert!(!helper.is_object_exist(&bucket1, "data1", None).await);
        }

        helper.delete_directory_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket1 = format!("{}{}", TestHelper::generate_bucket_name(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let helper = TestHelper::new().await;
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());

        {
            let target_bucket_url = format!("s3://{}", &bucket1);
            helper
                .create_directory_bucket(&bucket1, EXPRESS_ONE_ZONE_AZ)
                .await;

            helper
                .sync_directory_bucket_test_data(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                &source_bucket_url,
                &download_dir,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            let dir_entry_list = TestHelper::list_all_files(&download_dir);

            for entry in dir_entry_list {
                let path = entry
                    .path()
                    .to_string_lossy()
                    .replace(&download_dir, "");

                assert!(TestHelper::verify_file_md5_digest(
                    &format!("./test_data/e2e_test/case1/{}", &path),
                    &TestHelper::md5_digest(&entry.path().to_string_lossy()),
                ));
            }

            assert_eq!(
                helper
                    .get_object_last_modified(&bucket1, "data1", None)
                    .await,
                TestHelper::get_file_last_modified(&format!("{}/data1", &download_dir))
            );
        }

        let _ = std::fs::remove_dir_all(&download_dir);
        helper.delete_directory_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_parallel_listing() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket1 = format!("{}{}", TestHelper::generate_bucket_name(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let helper = TestHelper::new().await;
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());

        {
            let target_bucket_url = format!("s3://{}", &bucket1);
            helper
                .create_directory_bucket(&bucket1, EXPRESS_ONE_ZONE_AZ)
                .await;

            helper
                .sync_directory_bucket_test_data(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--allow-parallel-listings-in-express-one-zone",
                &source_bucket_url,
                &download_dir,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            let dir_entry_list = TestHelper::list_all_files(&download_dir);

            for entry in dir_entry_list {
                let path = entry
                    .path()
                    .to_string_lossy()
                    .replace(&download_dir, "");

                assert!(TestHelper::verify_file_md5_digest(
                    &format!("./test_data/e2e_test/case1/{}", &path),
                    &TestHelper::md5_digest(&entry.path().to_string_lossy()),
                ));
            }

            assert_eq!(
                helper
                    .get_object_last_modified(&bucket1, "data1", None)
                    .await,
                TestHelper::get_file_last_modified(&format!("{}/data1", &download_dir))
            );
        }

        let _ = std::fs::remove_dir_all(&download_dir);
        helper.delete_directory_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_with_delete() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket1 = format!("{}{}", TestHelper::generate_bucket_name(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let helper = TestHelper::new().await;
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .create_directory_bucket(&bucket1, EXPRESS_ONE_ZONE_AZ)
                .await;

            helper
                .sync_directory_bucket_test_data(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                &source_bucket_url,
                &download_dir,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            let dir_entry_list = TestHelper::list_all_files(&download_dir);
            assert_eq!(dir_entry_list.len(), 5);

            helper.delete_object(&bucket1, "data1", None).await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--delete",
                &source_bucket_url,
                &download_dir,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            let dir_entry_list = TestHelper::list_all_files(&download_dir);
            assert_eq!(dir_entry_list.len(), 4);

            assert!(!TestHelper::is_file_exist(&format!(
                "{}/data1",
                &download_dir
            )));
        }

        let _ = std::fs::remove_dir_all(&download_dir);
        helper.delete_all_objects(&bucket1).await;
        helper.delete_directory_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_s3() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket1 = format!("{}{}", TestHelper::generate_bucket_name(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let bucket2 = format!("{}{}", TestHelper::generate_bucket_name(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);

        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            helper
                .create_directory_bucket(&bucket1, EXPRESS_ONE_ZONE_AZ)
                .await;

            helper
                .create_directory_bucket(&bucket2, EXPRESS_ONE_ZONE_AZ)
                .await;

            helper
                .sync_directory_bucket_test_data(&target_bucket_url)
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
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&bucket2, "").await;
            assert_eq!(object_list.len(), 5);
        }

        helper.delete_directory_bucket_with_cascade(&bucket1).await;
        helper.delete_directory_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_delete() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket1 = format!("{}{}", TestHelper::generate_bucket_name(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let bucket2 = format!("{}{}", TestHelper::generate_bucket_name(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);

        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            helper
                .create_directory_bucket(&bucket1, EXPRESS_ONE_ZONE_AZ)
                .await;

            helper
                .create_directory_bucket(&bucket2, EXPRESS_ONE_ZONE_AZ)
                .await;

            helper
                .sync_directory_bucket_test_data(&target_bucket_url)
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
                &source_bucket_url,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let object_list = helper.list_objects(&bucket2, "").await;
            assert_eq!(object_list.len(), 5);
        }

        {
            helper.delete_object(&bucket1, "data1", None).await;
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

            let object_list = helper.list_objects(&bucket2, "").await;
            assert_eq!(object_list.len(), 4);

            assert!(!helper.is_object_exist(&bucket2, "data1", None).await);
        }

        helper.delete_directory_bucket_with_cascade(&bucket1).await;
        helper.delete_directory_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn single_part_operations_with_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket1 = format!("{}{}", TestHelper::generate_bucket_name(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let bucket2 = format!("{}{}", TestHelper::generate_bucket_name(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);

        let helper = TestHelper::new().await;
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());

        {
            let target_bucket_url = format!("s3://{}", &bucket1);
            helper
                .create_directory_bucket(&bucket1, EXPRESS_ONE_ZONE_AZ)
                .await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        {
            helper
                .create_directory_bucket(&bucket2, EXPRESS_ONE_ZONE_AZ)
                .await;

            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                "--enable-additional-checksum",
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
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        {
            let source_bucket_url = format!("s3://{}", &bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "CRC64NVME",
                "--enable-additional-checksum",
                &source_bucket_url,
                &download_dir,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_directory_bucket_with_cascade(&bucket1).await;
        helper.delete_directory_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn single_part_operations_with_default_additional_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket1 = format!("{}{}", TestHelper::generate_bucket_name(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let bucket2 = format!("{}{}", TestHelper::generate_bucket_name(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);

        let helper = TestHelper::new().await;
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());

        {
            let target_bucket_url = format!("s3://{}", &bucket1);
            helper
                .create_directory_bucket(&bucket1, EXPRESS_ONE_ZONE_AZ)
                .await;

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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        {
            helper
                .create_directory_bucket(&bucket2, EXPRESS_ONE_ZONE_AZ)
                .await;

            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
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
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        {
            let source_bucket_url = format!("s3://{}", &bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                &source_bucket_url,
                &download_dir,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_directory_bucket_with_cascade(&bucket1).await;
        helper.delete_directory_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn single_part_operations_with_disable_additional_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket1 = format!("{}{}", TestHelper::generate_bucket_name(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let bucket2 = format!("{}{}", TestHelper::generate_bucket_name(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);

        let helper = TestHelper::new().await;
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());

        {
            let target_bucket_url = format!("s3://{}", &bucket1);
            helper
                .create_directory_bucket(&bucket1, EXPRESS_ONE_ZONE_AZ)
                .await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-express-one-zone-additional-checksum",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        {
            helper
                .create_directory_bucket(&bucket2, EXPRESS_ONE_ZONE_AZ)
                .await;

            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-express-one-zone-additional-checksum",
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
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        {
            let source_bucket_url = format!("s3://{}", &bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--disable-express-one-zone-additional-checksum",
                &source_bucket_url,
                &download_dir,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_directory_bucket_with_cascade(&bucket1).await;
        helper.delete_directory_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn multipart_operations_with_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket1 = format!("{}{}", TestHelper::generate_bucket_name(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let bucket2 = format!("{}{}", TestHelper::generate_bucket_name(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);

        TestHelper::create_large_file();

        let helper = TestHelper::new().await;
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());

        {
            let target_bucket_url = format!("s3://{}", &bucket1);
            helper
                .create_directory_bucket(&bucket1, EXPRESS_ONE_ZONE_AZ)
                .await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        {
            helper
                .create_directory_bucket(&bucket2, EXPRESS_ONE_ZONE_AZ)
                .await;

            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                "--enable-additional-checksum",
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
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}/test2/", bucket2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
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
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        {
            let source_bucket_url = format!("s3://{}", &bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "CRC64NVME",
                "--enable-additional-checksum",
                &source_bucket_url,
                &download_dir,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 2);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 2);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_directory_bucket_with_cascade(&bucket1).await;
        helper.delete_directory_bucket_with_cascade(&bucket2).await;
    }
}
