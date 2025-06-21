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

    const SHA256_5M_FILE_5M_CHUNK: &str = "MYqGkAXXRLk+5ZHLgr20ovud2DTYgzUpOygDmGBbh9k=-1";
    const CRC64NVME_5M_FILE_5M_CHUNK: &str = "L0elBfNQAM0=";
    const ETAG_5M_FILE_5M_CHUNK: &str = "\"41c54a21b664d10684a24bb15b86b81b-1\"";
    const SHA256_5M_PLUS_1_FILE_5M_CHUNK: &str = "YzoBvMkdS8iDkS8i/cbrtBN+hTpKC1ibfBjZ2MicyrI=-2";
    const CRC64NVME_5M_PLUS_1_FILE_5M_CHUNK: &str = "f6iP2wb1XBg=";
    const ETAG_5M_PLUS_1_FILE_5M_CHUNK: &str = "\"c9f6c942564f9ebead5cb09e63b70dd7-2\"";
    const SHA256_5M_MINUS_1_FILE_5M_CHUNK: &str = "X7YKLr+rI8buCGZV5DmFaNsA5px6uk4kclHxIocUfHU=";
    const CRC64NVME_5M_MINUS_1_FILE_5M_CHUNK: &str = "k3BLMbXLWlU=";
    const ETAG_5M_MINUS_1_FILE_5M_CHUNK: &str = "\"74222dcf8ba716d84efe0dc716360087\"";
    const SHA256_5M_FILE_WHOLE: &str =
        "27d49a61d9a504bf66761f4d3143702d97876ddf5864d4ba22467cd04cdc67f0";
    const SHA256_5M_PLUS_1_FILE_WHOLE: &str =
        "e3bfafb553570dea7233d3a6d6d5d3ac3cff422cd1b5b5f7af767f0778511ef9";
    const SHA256_5M_MINUS_1_FILE_WHOLE: &str =
        "5fb60a2ebfab23c6ee086655e4398568db00e69c7aba4e247251f12287147c75";

    use super::*;

    #[tokio::test]
    async fn integrity_check_5mb_file_multipart_upload_5mb_chunk() {
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
            test_multipart_upload_5mb().await;
            test_multipart_upload_5mb_plus_1().await;
            test_multipart_upload_5mb_minus_1().await;
            test_multipart_upload_5mb_plus_1_auto_chunksize().await;
            test_multipart_upload_5mb_plus_1_kms().await;

            test_multipart_upload_5mb_sha256().await;
            test_multipart_upload_5mb_plus_1_sha256().await;
            test_multipart_upload_5mb_minus_1_sha256().await;
            test_multipart_upload_5mb_plus_1_sha256_auto_chunksize().await;
            test_multipart_upload_5mb_plus_1_sha256_kms().await;

            test_multipart_upload_5mb_crc64nvme().await;
            test_multipart_upload_5mb_plus_1_crc64nvme().await;
            test_multipart_upload_5mb_minus_1_crc64nvme().await;
            test_multipart_upload_5mb_plus_1_crc64nvme_auto_chunksize().await;
            test_multipart_upload_5mb_plus_1_crc64nvme_kms().await;
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

    async fn test_multipart_upload_5mb() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(5, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_FILE_5M_CHUNK);
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
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
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_FILE_5M_CHUNK);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
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
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_5M_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_5mb_plus_1() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(5, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_PLUS_1_FILE_5M_CHUNK);
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
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
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_PLUS_1_FILE_5M_CHUNK);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
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
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_5M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_5mb_minus_1() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(5, -1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_MINUS_1_FILE_5M_CHUNK);
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
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
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_MINUS_1_FILE_5M_CHUNK);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
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
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_5M_MINUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_5mb_plus_1_auto_chunksize() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(5, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_PLUS_1_FILE_5M_CHUNK);
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
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_PLUS_1_FILE_5M_CHUNK);
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
            assert_eq!(sha256_hash, SHA256_5M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_5mb_plus_1_kms() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(5, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
                "--multipart-threshold",
                "5MiB",
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
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_5M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_5mb_sha256() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(5, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
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
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_FILE_5M_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_5M_FILE_5M_CHUNK);
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_FILE_5M_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_5M_FILE_5M_CHUNK);
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(sha256_hash, SHA256_5M_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_5mb_plus_1_sha256() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(5, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
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
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_PLUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_5M_PLUS_1_FILE_5M_CHUNK
            );
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_PLUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_5M_PLUS_1_FILE_5M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(sha256_hash, SHA256_5M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_5mb_minus_1_sha256() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(5, -1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
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
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_MINUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_5M_MINUS_1_FILE_5M_CHUNK
            );
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_MINUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_5M_MINUS_1_FILE_5M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(sha256_hash, SHA256_5M_MINUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_5mb_plus_1_sha256_auto_chunksize() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(5, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
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
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_PLUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_5M_PLUS_1_FILE_5M_CHUNK
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
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_PLUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_5M_PLUS_1_FILE_5M_CHUNK
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
            assert_eq!(sha256_hash, SHA256_5M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_5mb_plus_1_sha256_kms() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(5, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
                SHA256_5M_PLUS_1_FILE_5M_CHUNK
            );
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
                SHA256_5M_PLUS_1_FILE_5M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(sha256_hash, SHA256_5M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_5mb_crc64nvme() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(5, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_5M_FILE_5M_CHUNK
            );
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_5M_FILE_5M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(sha256_hash, SHA256_5M_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_5mb_plus_1_crc64nvme() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(5, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_PLUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_5M_PLUS_1_FILE_5M_CHUNK
            );
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_PLUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_5M_PLUS_1_FILE_5M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(sha256_hash, SHA256_5M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_5mb_minus_1_crc64nvme() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(5, -1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_MINUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_5M_MINUS_1_FILE_5M_CHUNK
            );
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_MINUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_5M_MINUS_1_FILE_5M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(sha256_hash, SHA256_5M_MINUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_5mb_plus_1_crc64nvme_auto_chunksize() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(5, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_PLUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_5M_PLUS_1_FILE_5M_CHUNK
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
            assert_eq!(object.e_tag.unwrap(), ETAG_5M_PLUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_5M_PLUS_1_FILE_5M_CHUNK
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
            assert_eq!(sha256_hash, SHA256_5M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_5mb_plus_1_crc64nvme_kms() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(5, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
                CRC64NVME_5M_PLUS_1_FILE_5M_CHUNK
            );
        }

        {
            let source_bucket_url = format!("s3://{}", BUCKET1.to_string());
            let target_bucket_url = format!("s3://{}", BUCKET2.to_string());

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
                CRC64NVME_5M_PLUS_1_FILE_5M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let source_bucket_url = format!("s3://{}", BUCKET2.to_string());
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            assert_eq!(sha256_hash, SHA256_5M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }
}
