#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use common::*;
    use s3sync::config::Config;
    use s3sync::config::args::parse_from_args;
    use s3sync::pipeline::Pipeline;
    use s3sync::types::token::create_pipeline_cancellation_token;
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn local_to_s3_single_e_tag() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_single_no_verify_e_tag() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--disable-etag-verify",
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_single_without_content_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--disable-content-md5-header",
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_single_e_tag() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper.sync_test_data(&target_bucket_url).await;
        }

        {
            TestHelper::delete_all_files(&download_dir);

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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_single_no_verify_e_tag() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper.sync_test_data(&target_bucket_url).await;
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--disable-etag-verify",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_s3_single_e_tag() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper.sync_test_data(&target_bucket_url).await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_single_no_verify_e_tag() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper.sync_test_data(&target_bucket_url).await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-etag-verify",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_e_tag() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);

        TestHelper::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
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
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_no_verify_e_tag() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);

        TestHelper::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--disable-etag-verify",
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
        assert_eq!(stats.checksum_verified, 0);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_without_content_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);

        TestHelper::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--disable-content-md5-header",
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
        assert_eq!(stats.checksum_verified, 0);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_e_tag() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper.sync_large_test_data(&target_bucket_url).await;
        }

        {
            TestHelper::delete_all_files(&download_dir);

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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_no_verify_e_tag() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper.sync_large_test_data(&target_bucket_url).await;
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--disable-etag-verify",
                &source_bucket_url,
                &download_dir,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_e_tag() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper.sync_large_test_data(&target_bucket_url).await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_no_verify_e_tag() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper.sync_large_test_data(&target_bucket_url).await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-etag-verify",
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
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_e_tag_ng() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_custom_chunksize(&target_bucket_url, "5MiB")
                .await;
        }

        {
            TestHelper::delete_all_files(&download_dir);

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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 1);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_e_tag_auto() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_custom_chunksize(&target_bucket_url, "5MiB")
                .await;
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
                &source_bucket_url,
                &download_dir,
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
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_e_tag_ng() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_custom_chunksize(&target_bucket_url, "5MiB")
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 1);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_e_tag_auto() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_custom_chunksize(&target_bucket_url, "5MiB")
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.checksum_verified, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn local_to_s3_single_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA256",
            "./test_data/e2e_test/case1",
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
        assert_eq!(stats.checksum_verified, 5);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_single_checksum_without_content_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--disable-content-md5-header",
            "--additional-checksum-algorithm",
            "SHA256",
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_single_sha512_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA512",
            "./test_data/e2e_test/case1",
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
        assert_eq!(stats.checksum_verified, 5);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_single_md5_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "MD5",
            "./test_data/e2e_test/case1",
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
        assert_eq!(stats.checksum_verified, 5);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_single_xxhash128_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH128",
            "./test_data/e2e_test/case1",
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
        assert_eq!(stats.checksum_verified, 5);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_single_xxhash3_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH3",
            "./test_data/e2e_test/case1",
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
        assert_eq!(stats.checksum_verified, 5);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_single_xxhash64_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH64",
            "./test_data/e2e_test/case1",
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
        assert_eq!(stats.checksum_verified, 5);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_single_sha512_checksum_without_content_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--disable-content-md5-header",
            "--additional-checksum-algorithm",
            "SHA512",
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_single_md5_checksum_without_content_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--disable-content-md5-header",
            "--additional-checksum-algorithm",
            "MD5",
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_single_xxhash128_checksum_without_content_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--disable-content-md5-header",
            "--additional-checksum-algorithm",
            "XXHASH128",
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_single_xxhash3_checksum_without_content_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--disable-content-md5-header",
            "--additional-checksum-algorithm",
            "XXHASH3",
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_single_xxhash64_checksum_without_content_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--disable-content-md5-header",
            "--additional-checksum-algorithm",
            "XXHASH64",
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_single_crc64nvme_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "CRC64NVME",
            "./test_data/e2e_test/case1",
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
        assert_eq!(stats.checksum_verified, 5);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_single_crc64nvme_checksum_without_content_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--disable-content-md5-header",
            "--additional-checksum-algorithm",
            "CRC64NVME",
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_single_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper.sync_test_data_with_sha256(&target_bucket_url).await;
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_single_sha512_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper.sync_test_data_with_sha512(&target_bucket_url).await;
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_single_md5_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper.sync_test_data_with_md5(&target_bucket_url).await;
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_single_xxhash128_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_test_data_with_xxhash128(&target_bucket_url)
                .await;
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_single_xxhash3_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper.sync_test_data_with_xxhash3(&target_bucket_url).await;
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_single_xxhash64_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_test_data_with_xxhash64(&target_bucket_url)
                .await;
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_single_crc64nvme_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_test_data_with_crc64nvme(&target_bucket_url)
                .await;
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_s3_single_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper.sync_test_data_with_sha256(&target_bucket_url).await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA256",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_single_sha512_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper.sync_test_data_with_sha512(&target_bucket_url).await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA512",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_single_md5_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper.sync_test_data_with_md5(&target_bucket_url).await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "MD5",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_single_xxhash128_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_test_data_with_xxhash128(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH128",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_single_xxhash3_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper.sync_test_data_with_xxhash3(&target_bucket_url).await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH3",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_single_xxhash64_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_test_data_with_xxhash64(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH64",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_single_crc64nvme_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_test_data_with_crc64nvme(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "CRC64NVME",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);

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

        let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_checksum_without_content_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);

        TestHelper::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--disable-content-md5-header",
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

        let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_sha512_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);

        TestHelper::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA512",
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
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_md5_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);

        TestHelper::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "MD5",
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
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_xxhash128_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);

        TestHelper::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH128",
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
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_xxhash3_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);

        TestHelper::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH3",
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
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_xxhash64_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);

        TestHelper::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH64",
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
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_sha512_checksum_without_content_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);

        TestHelper::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--disable-content-md5-header",
            "--additional-checksum-algorithm",
            "SHA512",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_md5_checksum_without_content_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);

        TestHelper::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--disable-content-md5-header",
            "--additional-checksum-algorithm",
            "MD5",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_xxhash128_checksum_without_content_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);

        TestHelper::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--disable-content-md5-header",
            "--additional-checksum-algorithm",
            "XXHASH128",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_xxhash3_checksum_without_content_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);

        TestHelper::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--disable-content-md5-header",
            "--additional-checksum-algorithm",
            "XXHASH3",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_xxhash64_checksum_without_content_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);

        TestHelper::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--disable-content-md5-header",
            "--additional-checksum-algorithm",
            "XXHASH64",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_crc64nvme_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);

        TestHelper::create_large_file();

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
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_crc32_full_object_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);

        TestHelper::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--full-object-checksum",
            "--additional-checksum-algorithm",
            "CRC32",
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
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_crc32c_full_object_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);

        TestHelper::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--full-object-checksum",
            "--additional-checksum-algorithm",
            "CRC32C",
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
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_crc64nvme_checksum_without_content_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);

        TestHelper::create_large_file();

        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--disable-content-md5-header",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_sha256(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_sha512_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_sha512(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA512",
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
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_md5_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_md5(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "MD5",
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
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_xxhash128_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_xxhash128(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH128",
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
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_xxhash3_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_xxhash3(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH3",
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
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_xxhash64_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_xxhash64(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH64",
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
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_crc64nvme_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_crc64nvme(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_crc32_full_object_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_crc32_full_object_checksum(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_crc32c_full_object_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_crc32c_full_object_checksum(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_sha256(&target_bucket_url)
                .await;
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_sha512_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_sha512(&target_bucket_url)
                .await;
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_md5_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_md5(&target_bucket_url)
                .await;
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_xxhash128_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_xxhash128(&target_bucket_url)
                .await;
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_xxhash3_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_xxhash3(&target_bucket_url)
                .await;
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_xxhash64_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_xxhash64(&target_bucket_url)
                .await;
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_crc64nvme_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_crc64nvme(&target_bucket_url)
                .await;
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_crc32_full_object_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_crc32_full_object_checksum(&target_bucket_url)
                .await;
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_crc32c_full_object_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_crc32c_full_object_checksum(&target_bucket_url)
                .await;
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_checksum_auto() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_custom_chunksize_sha256(&target_bucket_url, "5MiB")
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA256",
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
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_sha512_checksum_auto() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_custom_chunksize_sha512(&target_bucket_url, "5MiB")
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA512",
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
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_md5_checksum_auto() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_custom_chunksize_md5(&target_bucket_url, "5MiB")
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "MD5",
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
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_xxhash128_checksum_auto() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_custom_chunksize_xxhash128(&target_bucket_url, "5MiB")
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH128",
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
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_xxhash3_checksum_auto() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_custom_chunksize_xxhash3(&target_bucket_url, "5MiB")
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH3",
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
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_xxhash64_checksum_auto() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_custom_chunksize_xxhash64(&target_bucket_url, "5MiB")
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH64",
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
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_crc64nvme_checksum_auto() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_custom_chunksize_crc64nvme(&target_bucket_url, "5MiB")
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "CRC64NVME",
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
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_crc32_full_object_checksum_auto() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_custom_chunksize_crc32_full_object(
                    &target_bucket_url,
                    "5MiB",
                )
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
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
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_crc32c_full_object_checksum_auto() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_custom_chunksize_crc32c_full_object(
                    &target_bucket_url,
                    "5MiB",
                )
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
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
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_crc64nvme_checksum_ok() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_custom_chunksize_crc64nvme(&target_bucket_url, "5MiB")
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 1);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_checksum_ng() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_custom_chunksize_sha256(&target_bucket_url, "5MiB")
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
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
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 2);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_checksum_ng_different_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_sha256(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 1);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn local_to_s3_sse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA256",
            "--sse",
            "aws:kms",
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_sse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
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
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_s3_sse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
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
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA256",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn local_to_s3_dsse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA256",
            "--sse",
            "aws:kms:dsse",
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_dsse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--sse",
                "aws:kms:dsse",
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
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_s3_dsse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--sse",
                "aws:kms:dsse",
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
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA256",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn local_to_s3_sse_c() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA256",
            "--target-sse-c",
            "AES256",
            "--target-sse-c-key",
            TEST_SSE_C_KEY_1,
            "--target-sse-c-key-md5",
            TEST_SSE_C_KEY_1_MD5,
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_sse_c() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
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
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_s3_sse_c() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;
        helper
            .create_bucket_with_sse_c_encryption(&bucket2, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
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
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA256",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_sse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        TestHelper::create_large_file();

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA256",
            "--sse",
            "aws:kms",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_dsse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        TestHelper::create_large_file();

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA256",
            "--sse",
            "aws:kms:dsse",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_sse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--sse",
                "aws:kms",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_sse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--sse",
                "aws:kms",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA256",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_dsse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--sse",
                "aws:kms:dsse",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_dsse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--sse",
                "aws:kms:dsse",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA256",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_sse_c() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        TestHelper::create_large_file();

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA256",
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

        let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_sse_c() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
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
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                &source_bucket_url,
                &download_dir,
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_sse_c() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;
        helper
            .create_bucket_with_sse_c_encryption(&bucket2, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
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
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA256",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_sha512_checksum_ng() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_custom_chunksize_sha512(&target_bucket_url, "5MiB")
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA512",
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
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 2);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_md5_checksum_ng() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_custom_chunksize_md5(&target_bucket_url, "5MiB")
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "MD5",
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
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 2);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_xxhash128_checksum_ng() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_custom_chunksize_xxhash128(&target_bucket_url, "5MiB")
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH128",
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
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 2);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_xxhash3_checksum_ng() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_custom_chunksize_xxhash3(&target_bucket_url, "5MiB")
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH3",
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
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 2);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_xxhash64_checksum_ng() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_custom_chunksize_xxhash64(&target_bucket_url, "5MiB")
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH64",
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
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 2);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_sha512_checksum_ng_different_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_sha512(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 1);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_md5_checksum_ng_different_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_md5(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 1);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_xxhash128_checksum_ng_different_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_xxhash128(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 1);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_xxhash3_checksum_ng_different_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_xxhash3(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 1);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_xxhash64_checksum_ng_different_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);
            helper
                .sync_large_test_data_with_xxhash64(&target_bucket_url)
                .await;
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 1);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn local_to_s3_sse_kms_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA512",
            "--sse",
            "aws:kms",
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_sse_kms_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "MD5",
            "--sse",
            "aws:kms",
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_sse_kms_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH128",
            "--sse",
            "aws:kms",
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_sse_kms_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH3",
            "--sse",
            "aws:kms",
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_sse_kms_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH64",
            "--sse",
            "aws:kms",
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_sse_kms_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA512",
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
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_sse_kms_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "MD5",
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
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_sse_kms_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH128",
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
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_sse_kms_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH3",
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
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_sse_kms_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH64",
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
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_s3_sse_kms_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA512",
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
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA512",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_sse_kms_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "MD5",
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
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "MD5",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_sse_kms_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH128",
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
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH128",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_sse_kms_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH3",
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
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH3",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_sse_kms_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH64",
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
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH64",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn local_to_s3_dsse_kms_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA512",
            "--sse",
            "aws:kms:dsse",
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_dsse_kms_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "MD5",
            "--sse",
            "aws:kms:dsse",
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_dsse_kms_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH128",
            "--sse",
            "aws:kms:dsse",
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_dsse_kms_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH3",
            "--sse",
            "aws:kms:dsse",
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_dsse_kms_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH64",
            "--sse",
            "aws:kms:dsse",
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_dsse_kms_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA512",
                "--sse",
                "aws:kms:dsse",
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
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_dsse_kms_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "MD5",
                "--sse",
                "aws:kms:dsse",
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
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_dsse_kms_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH128",
                "--sse",
                "aws:kms:dsse",
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
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_dsse_kms_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH3",
                "--sse",
                "aws:kms:dsse",
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
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_dsse_kms_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH64",
                "--sse",
                "aws:kms:dsse",
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
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_s3_dsse_kms_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA512",
                "--sse",
                "aws:kms:dsse",
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
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA512",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_dsse_kms_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "MD5",
                "--sse",
                "aws:kms:dsse",
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
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "MD5",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_dsse_kms_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH128",
                "--sse",
                "aws:kms:dsse",
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
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH128",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_dsse_kms_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH3",
                "--sse",
                "aws:kms:dsse",
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
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH3",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_dsse_kms_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH64",
                "--sse",
                "aws:kms:dsse",
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
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH64",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn local_to_s3_sse_c_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA512",
            "--target-sse-c",
            "AES256",
            "--target-sse-c-key",
            TEST_SSE_C_KEY_1,
            "--target-sse-c-key-md5",
            TEST_SSE_C_KEY_1_MD5,
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_sse_c_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "MD5",
            "--target-sse-c",
            "AES256",
            "--target-sse-c-key",
            TEST_SSE_C_KEY_1,
            "--target-sse-c-key-md5",
            TEST_SSE_C_KEY_1_MD5,
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_sse_c_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH128",
            "--target-sse-c",
            "AES256",
            "--target-sse-c-key",
            TEST_SSE_C_KEY_1,
            "--target-sse-c-key-md5",
            TEST_SSE_C_KEY_1_MD5,
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_sse_c_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH3",
            "--target-sse-c",
            "AES256",
            "--target-sse-c-key",
            TEST_SSE_C_KEY_1,
            "--target-sse-c-key-md5",
            TEST_SSE_C_KEY_1_MD5,
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_sse_c_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH64",
            "--target-sse-c",
            "AES256",
            "--target-sse-c-key",
            TEST_SSE_C_KEY_1,
            "--target-sse-c-key-md5",
            TEST_SSE_C_KEY_1_MD5,
            "./test_data/e2e_test/case1",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_sse_c_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA512",
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
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_sse_c_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "MD5",
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
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_sse_c_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH128",
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
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_sse_c_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH3",
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
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_sse_c_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH64",
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
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_s3_sse_c_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;
        helper
            .create_bucket_with_sse_c_encryption(&bucket2, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA512",
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
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA512",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_sse_c_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;
        helper
            .create_bucket_with_sse_c_encryption(&bucket2, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "MD5",
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
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "MD5",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_sse_c_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;
        helper
            .create_bucket_with_sse_c_encryption(&bucket2, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH128",
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
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH128",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_sse_c_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;
        helper
            .create_bucket_with_sse_c_encryption(&bucket2, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH3",
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
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH3",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_sse_c_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;
        helper
            .create_bucket_with_sse_c_encryption(&bucket2, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH64",
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
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH64",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_sse_kms_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        TestHelper::create_large_file();

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA512",
            "--sse",
            "aws:kms",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_sse_kms_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        TestHelper::create_large_file();

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "MD5",
            "--sse",
            "aws:kms",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_sse_kms_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        TestHelper::create_large_file();

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH128",
            "--sse",
            "aws:kms",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_sse_kms_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        TestHelper::create_large_file();

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH3",
            "--sse",
            "aws:kms",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_sse_kms_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        TestHelper::create_large_file();

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH64",
            "--sse",
            "aws:kms",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_dsse_kms_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        TestHelper::create_large_file();

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA512",
            "--sse",
            "aws:kms:dsse",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_dsse_kms_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        TestHelper::create_large_file();

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "MD5",
            "--sse",
            "aws:kms:dsse",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_dsse_kms_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        TestHelper::create_large_file();

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH128",
            "--sse",
            "aws:kms:dsse",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_dsse_kms_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        TestHelper::create_large_file();

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH3",
            "--sse",
            "aws:kms:dsse",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_dsse_kms_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;

        TestHelper::create_large_file();

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH64",
            "--sse",
            "aws:kms:dsse",
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_sse_kms_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA512",
                "--sse",
                "aws:kms",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_sse_kms_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "MD5",
                "--sse",
                "aws:kms",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_sse_kms_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH128",
                "--sse",
                "aws:kms",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_sse_kms_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH3",
                "--sse",
                "aws:kms",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_sse_kms_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH64",
                "--sse",
                "aws:kms",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_sse_kms_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA512",
                "--sse",
                "aws:kms",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA512",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_sse_kms_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "MD5",
                "--sse",
                "aws:kms",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "MD5",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_sse_kms_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH128",
                "--sse",
                "aws:kms",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH128",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_sse_kms_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH3",
                "--sse",
                "aws:kms",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH3",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_sse_kms_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH64",
                "--sse",
                "aws:kms",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH64",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_dsse_kms_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA512",
                "--sse",
                "aws:kms:dsse",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_dsse_kms_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "MD5",
                "--sse",
                "aws:kms:dsse",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_dsse_kms_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH128",
                "--sse",
                "aws:kms:dsse",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_dsse_kms_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH3",
                "--sse",
                "aws:kms:dsse",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_dsse_kms_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH64",
                "--sse",
                "aws:kms:dsse",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_dsse_kms_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA512",
                "--sse",
                "aws:kms:dsse",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA512",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_dsse_kms_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "MD5",
                "--sse",
                "aws:kms:dsse",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "MD5",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_dsse_kms_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH128",
                "--sse",
                "aws:kms:dsse",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH128",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_dsse_kms_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH3",
                "--sse",
                "aws:kms:dsse",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH3",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_dsse_kms_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH64",
                "--sse",
                "aws:kms:dsse",
                LARGE_FILE_DIR,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH64",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_sse_c_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        TestHelper::create_large_file();

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA512",
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

        let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_sse_c_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        TestHelper::create_large_file();

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "MD5",
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

        let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_sse_c_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        TestHelper::create_large_file();

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH128",
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

        let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_sse_c_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        TestHelper::create_large_file();

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH3",
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

        let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn local_to_s3_multipart_sse_c_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        TestHelper::create_large_file();

        let target_bucket_url = format!("s3://{}", bucket1);
        let args = vec![
            "s3sync",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "XXHASH64",
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

        let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_sse_c_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA512",
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
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                &source_bucket_url,
                &download_dir,
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_sse_c_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "MD5",
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
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                &source_bucket_url,
                &download_dir,
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_sse_c_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH128",
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
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                &source_bucket_url,
                &download_dir,
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_sse_c_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH3",
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
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                &source_bucket_url,
                &download_dir,
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_local_multipart_sse_c_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH64",
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
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket1);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                &source_bucket_url,
                &download_dir,
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

        helper.delete_bucket_with_cascade(&bucket1).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_sse_c_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;
        helper
            .create_bucket_with_sse_c_encryption(&bucket2, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA512",
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
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA512",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_sse_c_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;
        helper
            .create_bucket_with_sse_c_encryption(&bucket2, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "MD5",
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
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "MD5",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_sse_c_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;
        helper
            .create_bucket_with_sse_c_encryption(&bucket2, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH128",
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
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH128",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_sse_c_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;
        helper
            .create_bucket_with_sse_c_encryption(&bucket2, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH3",
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
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH3",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_sse_c_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;
        helper
            .create_bucket_with_sse_c_encryption(&bucket2, REGION)
            .await;

        {
            let target_bucket_url = format!("s3://{}", bucket1);

            TestHelper::create_large_file();

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH64",
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
        }

        {
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH64",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }
}
