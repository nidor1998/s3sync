#[cfg(test)]
#[cfg(feature = "e2e_test")]
mod common;

#[cfg(test)]
#[cfg(feature = "e2e_test")]
mod tests {
    use std::convert::TryFrom;

    use common::*;
    use s3sync::config::Config;
    use s3sync::config::args::parse_from_args;
    use s3sync::pipeline::Pipeline;
    use s3sync::types::token::create_pipeline_cancellation_token;

    const SHA256_30M_FILE_8M_CHUNK: &str = "5NrHBc0Z1wNCbADRDy8mJaIvc53oxncCrw/Fa48VhxY=-4";
    const SHA256_30M_FILE_NO_CHUNK: &str = "BcHHcdSIbkzv3wpMC5B5E/4vgp3XZ0GMlOoniwuLw/k=";
    const CRC64NVME_30M_FILE_8M_CHUNK: &str = "rrk4q4lsMS4=";
    const CRC64NVME_30M_FILE_NO_CHUNK: &str = "rrk4q4lsMS4=";
    const ETAG_30M_FILE_8M_CHUNK: &str = "\"a81230a7666d413e511f9c2c2523947a-4\"";
    const ETAG_30M_FILE_NO_CHUNK: &str = "\"94189ebb786dbc25aaf22d3d96e88aeb\"";
    const SHA256_30M_FILE_WHOLE_HEX: &str =
        "05c1c771d4886e4cefdf0a4c0b907913fe2f829dd767418c94ea278b0b8bc3f9";
    const SHA256_1M_FILE: &str = "nHjjstwawGidD0Sn7WX1sNMcsjAoK7gP8bUyhDOlnRU=";
    const CRC64NVME_1M_FILE: &str = "IWamHyhf59M=";
    const ETAG_1M_FILE: &str = "\"1ebdcfc23acf32f84f462e721e9db32c\"";
    const SHA256_1M_WHOLE: &str =
        "9c78e3b2dc1ac0689d0f44a7ed65f5b0d31cb230282bb80ff1b5328433a59d15";

    const SHA256_8M_FILE_8M_CHUNK: &str = "U+ZIEj2OXjCTTOp7PlJy43aKT7mL2X5NzgfjpYzeozw=-1";
    const CRC64NVME_8M_FILE_8M_CHUNK: &str = "io2hnVvxKgU=";
    const ETAG_8M_FILE_8M_CHUNK: &str = "\"13698b45ee34dbf0611fe527f76abfc7-1\"";
    const ETAG_8M_FILE_5M_CHUNK: &str = "\"ebff86fc334a63cefaad7a0b621a0109-2\"";

    const SHA256_8M_FILE_NO_CHUNK: &str = "zV9Xxv/j9oUQSrpuxyaLqrh5BgMDS97IMCKLVy2ExaQ=";
    const CRC64NVME_8M_FILE_NO_CHUNK: &str = "io2hnVvxKgU=";
    const ETAG_8M_FILE_NO_CHUNK: &str = "\"e9d3e2caa0ac28fd50b183dac706ee29\"";
    const SHA256_8M_FILE_5M_CHUNK: &str = "EZAvWUpvGrpch+0S5qFJhcwxd6bw9HtocRRVc/FAwQA=-2";

    const SHA256_8M_FILE_WHOLE: &str =
        "cd5f57c6ffe3f685104aba6ec7268baab8790603034bdec830228b572d84c5a4";

    use super::*;

    #[tokio::test]
    async fn integrity_check_edge_case1() {
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
            test_multipart_upload_30mb().await;
            test_multipart_upload_30mb_sha256().await;
            test_multipart_upload_30mb_crc64nvme().await;
            test_upload_1m().await;
            test_upload_1m_sha256().await;
            test_upload_1m_crc64nvme().await;
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        tokio::time::sleep(std::time::Duration::from_millis(
            SLEEP_TIME_MILLIS_AFTER_INTEGRATION_TEST,
        ))
        .await;
    }

    #[tokio::test]
    async fn integrity_check_edge_case2() {
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
            test_multipart_upload_8mb().await;
            test_multipart_upload_8mb_sha256().await;
            test_multipart_upload_8mb_crc64nvme().await;
            test_multipart_upload_8mb_no_chunk().await;
            test_multipart_upload_8mb_no_chunk_sha256().await;
            test_multipart_upload_8mb_no_chunk_crc64nvme().await;
            test_multipart_upload_8mb_7mb_threshold_5mb_chunk().await;
            test_multipart_upload_8mb_7mb_threshold_9mb_chunk().await;
            test_multipart_upload_8mb_9mb_threshold_5mb_chunk().await;
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        tokio::time::sleep(std::time::Duration::from_millis(
            SLEEP_TIME_MILLIS_AFTER_INTEGRATION_TEST,
        ))
        .await;
    }

    async fn test_multipart_upload_30mb() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(30, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "50MiB",
                "--multipart-chunksize",
                "50MiB",
                RANDOM_DATA_FILE_DIR,
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

            let object = helper
                .head_object(&BUCKET1.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_30M_FILE_NO_CHUNK);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

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

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_30M_FILE_8M_CHUNK);
        }

        {
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 1);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_30M_FILE_8M_CHUNK);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--remove-modified-filter",
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
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_30M_FILE_NO_CHUNK);
        }

        {
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--remove-modified-filter",
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
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_30M_FILE_NO_CHUNK);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", *BUCKET2);
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
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 1);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_30M_FILE_WHOLE_HEX);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", *BUCKET2);
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_30M_FILE_WHOLE_HEX);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_30mb_sha256() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(30, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "50MiB",
                "--multipart-chunksize",
                "50MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                RANDOM_DATA_FILE_DIR,
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

            let object = helper
                .head_object(&BUCKET1.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_30M_FILE_NO_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_30M_FILE_NO_CHUNK);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 2);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_30M_FILE_8M_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_30M_FILE_8M_CHUNK);
        }

        {
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 2);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_30M_FILE_8M_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_30M_FILE_8M_CHUNK);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--remove-modified-filter",
                "--auto-chunksize",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_30M_FILE_NO_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_30M_FILE_NO_CHUNK);
        }

        {
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--remove-modified-filter",
                "--auto-chunksize",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_30M_FILE_NO_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_30M_FILE_NO_CHUNK);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", *BUCKET2);
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
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 1);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_30M_FILE_WHOLE_HEX);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", *BUCKET2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_30M_FILE_WHOLE_HEX);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_30mb_crc64nvme() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(30, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "50MiB",
                "--multipart-chunksize",
                "50MiB",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                RANDOM_DATA_FILE_DIR,
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

            let object = helper
                .head_object(&BUCKET1.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_30M_FILE_NO_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_30M_FILE_NO_CHUNK
            );
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

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
            assert_eq!(stats.sync_warning, 1);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_30M_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_30M_FILE_8M_CHUNK
            );
        }

        {
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
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
            assert_eq!(stats.sync_warning, 1);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_30M_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_30M_FILE_8M_CHUNK
            );
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--remove-modified-filter",
                "--auto-chunksize",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_30M_FILE_NO_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_30M_FILE_8M_CHUNK
            );
        }

        {
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--remove-modified-filter",
                "--auto-chunksize",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_30M_FILE_NO_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_30M_FILE_8M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", *BUCKET2);
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
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 1);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_30M_FILE_WHOLE_HEX);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    /////
    async fn test_upload_1m() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(1, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "50MiB",
                "--multipart-chunksize",
                "50MiB",
                RANDOM_DATA_FILE_DIR,
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

            let object = helper
                .head_object(&BUCKET1.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_1M_FILE);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

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
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_1M_FILE);
        }

        {
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_1M_FILE);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", *BUCKET2);
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_1M_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_upload_1m_sha256() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(1, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "50MiB",
                "--multipart-chunksize",
                "50MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                RANDOM_DATA_FILE_DIR,
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

            let object = helper
                .head_object(&BUCKET1.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_1M_FILE);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_1M_FILE);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_1M_FILE);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_1M_FILE);
        }

        {
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--auto-chunksize",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_1M_FILE);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_1M_FILE);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", *BUCKET2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_1M_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_upload_1m_crc64nvme() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(1, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "50MiB",
                "--multipart-chunksize",
                "50MiB",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                RANDOM_DATA_FILE_DIR,
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

            let object = helper
                .head_object(&BUCKET1.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_1M_FILE);
            assert_eq!(object.checksum_crc64_nvme.unwrap(), CRC64NVME_1M_FILE);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_1M_FILE);
            assert_eq!(object.checksum_crc64_nvme.unwrap(), CRC64NVME_1M_FILE);
        }

        {
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--auto-chunksize",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_1M_FILE);
            assert_eq!(object.checksum_crc64_nvme.unwrap(), CRC64NVME_1M_FILE);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", *BUCKET2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_1M_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_8mb() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                RANDOM_DATA_FILE_DIR,
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

            let object = helper
                .head_object(&BUCKET1.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_8M_CHUNK);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

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
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_8M_CHUNK);
        }

        {
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_8M_CHUNK);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", *BUCKET2);
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_8M_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_8mb_sha256() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                RANDOM_DATA_FILE_DIR,
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

            let object = helper
                .head_object(&BUCKET1.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_8M_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_8M_FILE_8M_CHUNK);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
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

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_8M_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_8M_FILE_8M_CHUNK);
        }

        {
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
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

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_8M_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_8M_FILE_8M_CHUNK);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", *BUCKET2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_8M_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_8mb_crc64nvme() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                RANDOM_DATA_FILE_DIR,
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

            let object = helper
                .head_object(&BUCKET1.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_8M_FILE_8M_CHUNK
            );
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_8M_FILE_8M_CHUNK
            );
        }

        {
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_8M_FILE_8M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", *BUCKET2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_8M_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_8mb_no_chunk() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "50MiB",
                "--multipart-chunksize",
                "50MiB",
                RANDOM_DATA_FILE_DIR,
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

            let object = helper
                .head_object(&BUCKET1.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_NO_CHUNK);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

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
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_NO_CHUNK);
        }

        {
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_NO_CHUNK);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", *BUCKET2);
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_8M_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_8mb_no_chunk_sha256() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "50MiB",
                "--multipart-chunksize",
                "50MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                RANDOM_DATA_FILE_DIR,
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

            let object = helper
                .head_object(&BUCKET1.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_NO_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_8M_FILE_NO_CHUNK);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_NO_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_8M_FILE_NO_CHUNK);
        }

        {
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--auto-chunksize",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_NO_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_8M_FILE_NO_CHUNK);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", *BUCKET2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_8M_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_8mb_no_chunk_crc64nvme() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "50MiB",
                "--multipart-chunksize",
                "50MiB",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                RANDOM_DATA_FILE_DIR,
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

            let object = helper
                .head_object(&BUCKET1.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_NO_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_8M_FILE_NO_CHUNK
            );
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_NO_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_8M_FILE_NO_CHUNK
            );
        }

        {
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--auto-chunksize",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_NO_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_8M_FILE_NO_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", *BUCKET2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_8M_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_8mb_7mb_threshold_5mb_chunk() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "7MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                RANDOM_DATA_FILE_DIR,
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

            let object = helper
                .head_object(&BUCKET1.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_5M_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_8M_FILE_5M_CHUNK);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
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

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_5M_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_8M_FILE_5M_CHUNK);
        }

        {
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
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

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_5M_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_8M_FILE_5M_CHUNK);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", *BUCKET2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_8M_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_8mb_7mb_threshold_9mb_chunk() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "7MiB",
                "--multipart-chunksize",
                "9MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                RANDOM_DATA_FILE_DIR,
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

            let object = helper
                .head_object(&BUCKET1.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_8M_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_8M_FILE_8M_CHUNK);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
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

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_8M_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_8M_FILE_8M_CHUNK);
        }

        {
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
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

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_8M_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_8M_FILE_8M_CHUNK);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", *BUCKET2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_8M_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_8mb_9mb_threshold_5mb_chunk() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "9MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                RANDOM_DATA_FILE_DIR,
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

            let object = helper
                .head_object(&BUCKET1.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_NO_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_8M_FILE_NO_CHUNK);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
                "--multipart-threshold",
                "9MiB",
                "--multipart-chunksize",
                "5MiB",
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

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_NO_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_8M_FILE_NO_CHUNK);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", *BUCKET2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                "--multipart-threshold",
                "9MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_8M_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }
}
