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

    const SHA256_8M_FILE_8M_CHUNK: &str = "U+ZIEj2OXjCTTOp7PlJy43aKT7mL2X5NzgfjpYzeozw=-1";
    const CRC64NVME_8M_FILE_8M_CHUNK: &str = "io2hnVvxKgU=";
    const ETAG_8M_FILE_8M_CHUNK: &str = "\"13698b45ee34dbf0611fe527f76abfc7-1\"";
    const SHA256_8M_PLUS_1_FILE_8M_CHUNK: &str = "fMRKvd1OLwQTon7VE4yRXsWmVDtF8uLVJT9aKIM2SC8=-2";
    const CRC64NVME_8M_PLUS_1_FILE_8M_CHUNK: &str = "lCfg/yBs550=";
    const ETAG_8M_PLUS_1_FILE_8M_CHUNK: &str = "\"61b0524a157f9391c45c09ae2b48dde4-2\"";
    const SHA256_8M_MINUS_1_FILE_8M_CHUNK: &str = "LH/6UUEm4g2eT855uX/3OcshOFJADcHasHpSnaTsPkQ=";
    const CRC64NVME_8M_MINUS_1_FILE_8M_CHUNK: &str = "GPMNBOmnUuA=";
    const ETAG_8M_MINUS_1_FILE_8M_CHUNK: &str = "\"c9c7b65a175f43ff8147d8027403e177\"";
    const SHA256_8M_FILE_WHOLE: &str =
        "cd5f57c6ffe3f685104aba6ec7268baab8790603034bdec830228b572d84c5a4";
    const SHA256_8M_PLUS_1_FILE_WHOLE: &str =
        "e0a269be5fbff701eba9a07f82027f5a1e22bebc8df2f2027840a02184b84b3c";
    const SHA256_8M_MINUS_1_FILE_WHOLE: &str =
        "2c7ffa514126e20d9e4fce79b97ff739cb213852400dc1dab07a529da4ec3e44";

    use super::*;

    #[tokio::test]
    async fn integrity_check_8mb_file_multipart_upload_8mb_chunk() {
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
            test_multipart_upload_8mb_plus_1().await;
            test_multipart_upload_8mb_minus_1().await;
            test_multipart_upload_8mb_plus_1_auto_chunksize().await;
            test_multipart_upload_8mb_plus_1_kms().await;

            test_multipart_upload_8mb_sha256().await;
            test_multipart_upload_8mb_plus_1_sha256().await;
            test_multipart_upload_8mb_minus_1_sha256().await;
            test_multipart_upload_8mb_plus_1_sha256_auto_chunksize().await;
            test_multipart_upload_8mb_plus_1_sha256_kms().await;

            test_multipart_upload_8mb_crc64nvme().await;
            test_multipart_upload_8mb_plus_1_crc64nvme().await;
            test_multipart_upload_8mb_minus_1_crc64nvme().await;
            test_multipart_upload_8mb_plus_1_crc64nvme_auto_chunksize().await;
            test_multipart_upload_8mb_plus_1_crc64nvme_kms().await;
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

    async fn test_multipart_upload_8mb() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
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

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_FILE_8M_CHUNK);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
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
    }

    async fn test_multipart_upload_8mb_plus_1() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_PLUS_1_FILE_8M_CHUNK);
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
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

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_PLUS_1_FILE_8M_CHUNK);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_8M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn test_multipart_upload_8mb_minus_1() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, -1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_MINUS_1_FILE_8M_CHUNK);
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
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

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_MINUS_1_FILE_8M_CHUNK);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_8M_MINUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn test_multipart_upload_8mb_plus_1_auto_chunksize() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_PLUS_1_FILE_8M_CHUNK);
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
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
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_PLUS_1_FILE_8M_CHUNK);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
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
            assert_eq!(sha256_hash, SHA256_8M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn test_multipart_upload_8mb_plus_1_kms() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--sse",
                "aws:kms",
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
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
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
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_8M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn test_multipart_upload_8mb_sha256() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
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

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
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
    }

    async fn test_multipart_upload_8mb_plus_1_sha256() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_PLUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_8M_PLUS_1_FILE_8M_CHUNK
            );
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_PLUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_8M_PLUS_1_FILE_8M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_8M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn test_multipart_upload_8mb_minus_1_sha256() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, -1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_MINUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_8M_MINUS_1_FILE_8M_CHUNK
            );
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_MINUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_8M_MINUS_1_FILE_8M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_8M_MINUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn test_multipart_upload_8mb_plus_1_sha256_auto_chunksize() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_PLUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_8M_PLUS_1_FILE_8M_CHUNK
            );
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
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
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_PLUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_8M_PLUS_1_FILE_8M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
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
            assert_eq!(sha256_hash, SHA256_8M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn test_multipart_upload_8mb_plus_1_sha256_kms() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--sse",
                "aws:kms",
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
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET1.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_8M_PLUS_1_FILE_8M_CHUNK
            );
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--sse",
                "aws:kms",
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
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET1.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_8M_PLUS_1_FILE_8M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
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
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_8M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn test_multipart_upload_8mb_crc64nvme() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
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

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
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
    }

    async fn test_multipart_upload_8mb_plus_1_crc64nvme() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_PLUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_8M_PLUS_1_FILE_8M_CHUNK
            );
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_PLUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_8M_PLUS_1_FILE_8M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_8M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn test_multipart_upload_8mb_minus_1_crc64nvme() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, -1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_MINUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_8M_MINUS_1_FILE_8M_CHUNK
            );
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_MINUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_8M_MINUS_1_FILE_8M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_8M_MINUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn test_multipart_upload_8mb_plus_1_crc64nvme_auto_chunksize() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

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
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_PLUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_8M_PLUS_1_FILE_8M_CHUNK
            );
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
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
            assert_eq!(object.e_tag.unwrap(), ETAG_8M_PLUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_8M_PLUS_1_FILE_8M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
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
            assert_eq!(sha256_hash, SHA256_8M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }

    async fn test_multipart_upload_8mb_plus_1_crc64nvme_kms() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(8, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--sse",
                "aws:kms",
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
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET1.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_8M_PLUS_1_FILE_8M_CHUNK
            );
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--sse",
                "aws:kms",
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

            let object = helper
                .head_object(&BUCKET1.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_8M_PLUS_1_FILE_8M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
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
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_8M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
    }
}
