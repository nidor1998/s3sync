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
    async fn integrity_check() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper.create_bucket(&BUCKET1.to_string(), REGION).await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
        helper.create_bucket(&BUCKET2.to_string(), REGION).await;

        {
            local_to_s3_single_no_verify_etag().await;
            local_to_s3_multipart_no_verify_etag().await;
            s3_to_local_single_no_verify_etag().await;
            s3_to_local_multipart_no_verify_etag().await;
            s3_to_s3_single_no_verify_etag().await;
            s3_to_s3_multipart_no_verify_etag().await;

            local_to_s3_single_etag().await;
            s3_to_local_single_etag().await;
            s3_to_s3_single_etag().await;
            local_to_s3_multipart_etag().await;
            s3_to_local_multipart_etag().await;
            s3_to_s3_multipart_etag().await;
            s3_to_local_multipart_etag_ng().await;
            s3_to_local_multipart_etag_auto().await;
            s3_to_s3_multipart_etag_ng().await;
            s3_to_s3_multipart_etag_auto().await;
            local_to_s3_single_checksum().await;
            s3_to_local_single_checksum().await;
            s3_to_s3_single_checksum().await;
            local_to_s3_multipart_checksum().await;
            local_to_s3_sse_kms().await;
            s3_to_local_sse_kms().await;
            s3_to_s3_sse_kms().await;
            local_to_s3_sse_c().await;
            s3_to_local_sse_c().await;
            s3_to_s3_sse_c().await;
            local_to_s3_multipart_sse_kms().await;
            local_to_s3_multipart_sse_kms().await;
            local_to_s3_multipart_sse_c().await;

            s3_to_s3_multipart_checksum().await;
            s3_to_local_multipart_checksum().await;
            s3_to_s3_multipart_checksum_auto().await;
            s3_to_s3_multipart_checksum_ng().await;
            s3_to_s3_multipart_checksum_ng_different_checksum().await;
            s3_to_local_multipart_sse_kms().await;
            s3_to_s3_multipart_sse_kms().await;
            s3_to_local_multipart_sse_c().await;
            s3_to_s3_multipart_sse_c().await;
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    async fn local_to_s3_single_etag() {
        let helper = TestHelper::new().await;

        let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
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
        assert_eq!(stats.etag_verified, 5);
        assert_eq!(stats.checksum_verified, 0);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn local_to_s3_single_no_verify_etag() {
        let helper = TestHelper::new().await;

        let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
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
        assert_eq!(stats.etag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn s3_to_local_single_etag() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.sync_test_data(&target_bucket_url).await;
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

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
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.etag_verified, 5);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn s3_to_local_single_no_verify_etag() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.sync_test_data(&target_bucket_url).await;
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--disable-etag-verify",
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
            assert_eq!(stats.etag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn s3_to_s3_single_etag() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.sync_test_data(&target_bucket_url).await;
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
            assert_eq!(stats.etag_verified, 5);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn s3_to_s3_single_no_verify_etag() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.sync_test_data(&target_bucket_url).await;
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
            assert_eq!(stats.etag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn local_to_s3_multipart_etag() {
        let helper = TestHelper::new().await;

        let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
        assert_eq!(stats.etag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn local_to_s3_multipart_no_verify_etag() {
        let helper = TestHelper::new().await;

        let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
        assert_eq!(stats.etag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn s3_to_local_multipart_etag() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.sync_large_test_data(&target_bucket_url).await;
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.etag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn s3_to_local_multipart_no_verify_etag() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.sync_large_test_data(&target_bucket_url).await;
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--disable-etag-verify",
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
            assert_eq!(stats.etag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn s3_to_s3_multipart_etag() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.sync_large_test_data(&target_bucket_url).await;
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
            assert_eq!(stats.etag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn s3_to_s3_multipart_no_verify_etag() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.sync_large_test_data(&target_bucket_url).await;
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
            assert_eq!(stats.etag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn s3_to_local_multipart_etag_ng() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper
                .sync_large_test_data_with_custom_chunksize(&target_bucket_url, "5MiB")
                .await;
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

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
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.etag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 1);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn s3_to_local_multipart_etag_auto() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper
                .sync_large_test_data_with_custom_chunksize(&target_bucket_url, "5MiB")
                .await;
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
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
            assert_eq!(stats.etag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn s3_to_s3_multipart_etag_ng() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper
                .sync_large_test_data_with_custom_chunksize(&target_bucket_url, "5MiB")
                .await;
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
            assert_eq!(stats.etag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 1);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn s3_to_s3_multipart_etag_auto() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper
                .sync_large_test_data_with_custom_chunksize(&target_bucket_url, "5MiB")
                .await;
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
            assert_eq!(stats.etag_verified, 1);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.checksum_verified, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn local_to_s3_single_checksum() {
        let helper = TestHelper::new().await;

        let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
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
        assert_eq!(stats.etag_verified, 5);
        assert_eq!(stats.checksum_verified, 5);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn s3_to_local_single_checksum() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.sync_test_data_with_sha256(&target_bucket_url).await;
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

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
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.etag_verified, 5);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn s3_to_s3_single_checksum() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.sync_test_data_with_sha256(&target_bucket_url).await;
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());
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
            assert_eq!(stats.etag_verified, 5);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn local_to_s3_multipart_checksum() {
        let helper = TestHelper::new().await;

        let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
        assert_eq!(stats.etag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn s3_to_s3_multipart_checksum() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper
                .sync_large_test_data_with_sha256(&target_bucket_url)
                .await;
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
            assert_eq!(stats.etag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn s3_to_local_multipart_checksum() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper
                .sync_large_test_data_with_sha256(&target_bucket_url)
                .await;
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

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
            assert_eq!(stats.etag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn s3_to_s3_multipart_checksum_auto() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper
                .sync_large_test_data_with_custom_chunksize_sha256(&target_bucket_url, "5MiB")
                .await;
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
            assert_eq!(stats.etag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn s3_to_s3_multipart_checksum_ng() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper
                .sync_large_test_data_with_custom_chunksize_sha256(&target_bucket_url, "5MiB")
                .await;
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
            assert_eq!(stats.etag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 2);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn s3_to_s3_multipart_checksum_ng_different_checksum() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper
                .sync_large_test_data_with_sha256(&target_bucket_url)
                .await;
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
            assert_eq!(stats.etag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 1);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn local_to_s3_sse_kms() {
        let helper = TestHelper::new().await;

        let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
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
        assert_eq!(stats.etag_verified, 0);
        assert_eq!(stats.checksum_verified, 5);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn s3_to_local_sse_kms() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

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
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.etag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn s3_to_s3_sse_kms() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());
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
            assert_eq!(stats.etag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn local_to_s3_sse_c() {
        let helper = TestHelper::new().await;

        let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
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
        assert_eq!(stats.etag_verified, 0);
        assert_eq!(stats.checksum_verified, 5);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn s3_to_local_sse_c() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
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
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.etag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn s3_to_s3_sse_c() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());
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
            assert_eq!(stats.etag_verified, 0);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn local_to_s3_multipart_sse_kms() {
        let helper = TestHelper::new().await;

        TestHelper::create_large_file();

        let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
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
        assert_eq!(stats.etag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn s3_to_local_multipart_sse_kms() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

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
            assert_eq!(stats.etag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn s3_to_s3_multipart_sse_kms() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());
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
            assert_eq!(stats.etag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn local_to_s3_multipart_sse_c() {
        let helper = TestHelper::new().await;

        TestHelper::create_large_file();

        let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
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
        assert_eq!(stats.etag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn s3_to_local_multipart_sse_c() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
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
                TEMP_DOWNLOAD_DIR,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.etag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn s3_to_s3_multipart_sse_c() {
        let helper = TestHelper::new().await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());
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
            assert_eq!(stats.etag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }
}
