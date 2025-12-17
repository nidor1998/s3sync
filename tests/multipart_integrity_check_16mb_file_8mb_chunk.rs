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

    const SHA256_16M_FILE_8M_CHUNK: &str = "LVOdCFd7WEX67+8xvcScVKkhkmmSweO04ZpwoISRidg=-2";
    const CRC64NVME_16M_FILE_8M_CHUNK: &str = "0k/JgmAhfhc=";
    const ETAG_16M_FILE_8M_CHUNK: &str = "\"93724d91845349c1695f224995fa68ea-2\"";
    const SHA256_16M_PLUS_1_FILE_8M_CHUNK: &str = "Iit+zZRe9Uyo6R+uoTqiMb54ZurDpJwvDjHj1pogXqI=-3";
    const CRC64NVME_16M_PLUS_1_FILE_8M_CHUNK: &str = "EkI8PNeCSro=";
    const ETAG_16M_PLUS_1_FILE_8M_CHUNK: &str = "\"4c72ed54398639c8fdbf38f9455353c9-3\"";
    const SHA256_16M_MINUS_1_FILE_8M_CHUNK: &str = "SH8JKa5JGy6vdNjmK6uC91GcqnWasSH+mNdfZZG2/6k=-2";
    const CRC64NVME_16M_MINUS_1_FILE_8M_CHUNK: &str = "G5Y2N0Yfx+o=";
    const ETAG_16M_MINUS_1_FILE_8M_CHUNK: &str = "\"a34a18fab3e8715b3b1edc3cb9a52647-2\"";
    const SHA256_16M_FILE_WHOLE: &str =
        "23bf32cdfd60784647663a160aee7c46ca7941173d48ad37db52713fda4562e1";
    const SHA256_16M_PLUS_1_FILE_WHOLE: &str =
        "0fbb2466d100013b3716965c89ac0c1375bba2c8f126e63ee6bc5ffff68ef33b";
    const SHA256_16M_MINUS_1_FILE_WHOLE: &str =
        "cf674acbd51c8c0e3c08ba06cb8b2bcfa871b2193399cca34d3915b8312f57cb";

    use super::*;

    #[tokio::test]
    async fn integrity_check_16mb_file_multipart_upload_8mb_chunk() {
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
            test_multipart_upload_16mb().await;
            test_multipart_upload_16mb_plus_1().await;
            test_multipart_upload_16mb_minus_1().await;
            test_multipart_upload_16mb_plus_1_auto_chunksize().await;
            test_multipart_upload_16mb_plus_1_kms().await;

            test_multipart_upload_16mb_sha256().await;
            test_multipart_upload_16mb_plus_1_sha256().await;
            test_multipart_upload_16mb_minus_1_sha256().await;
            test_multipart_upload_16mb_plus_1_sha256_auto_chunksize().await;
            test_multipart_upload_16mb_plus_1_sha256_kms().await;

            test_multipart_upload_16mb_crc64nvme().await;
            test_multipart_upload_16mb_plus_1_crc64nvme().await;
            test_multipart_upload_16mb_minus_1_crc64nvme().await;
            test_multipart_upload_16mb_plus_1_crc64nvme_auto_chunksize().await;
            test_multipart_upload_16mb_plus_1_crc64nvme_kms().await;
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

    async fn test_multipart_upload_16mb() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(16, 0).unwrap();

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
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_FILE_8M_CHUNK);
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_FILE_8M_CHUNK);
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_FILE_8M_CHUNK);
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_16M_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_16mb_plus_1() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(16, 1).unwrap();

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
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_16M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_16mb_minus_1() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(16, -1).unwrap();

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
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_MINUS_1_FILE_8M_CHUNK);
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_MINUS_1_FILE_8M_CHUNK);
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_MINUS_1_FILE_8M_CHUNK);
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_16M_MINUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_16mb_plus_1_auto_chunksize() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(16, 1).unwrap();

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
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
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
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
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
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
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
            assert_eq!(sha256_hash, SHA256_16M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_16mb_plus_1_kms() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(16, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

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
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

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

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
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
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_16M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_16mb_sha256() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(16, 0).unwrap();

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
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_FILE_8M_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_16M_FILE_8M_CHUNK);
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_FILE_8M_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_16M_FILE_8M_CHUNK);
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_FILE_8M_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_16M_FILE_8M_CHUNK);
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_16M_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_16mb_plus_1_sha256() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(16, 1).unwrap();

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
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_16M_PLUS_1_FILE_8M_CHUNK
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
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_16M_PLUS_1_FILE_8M_CHUNK
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
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_16M_PLUS_1_FILE_8M_CHUNK
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_16M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_16mb_minus_1_sha256() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(16, -1).unwrap();

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
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_MINUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_16M_MINUS_1_FILE_8M_CHUNK
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
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_MINUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_16M_MINUS_1_FILE_8M_CHUNK
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
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_MINUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_16M_MINUS_1_FILE_8M_CHUNK
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_16M_MINUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_16mb_plus_1_sha256_auto_chunksize() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(16, 1).unwrap();

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
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_16M_PLUS_1_FILE_8M_CHUNK
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
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_16M_PLUS_1_FILE_8M_CHUNK
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
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_16M_PLUS_1_FILE_8M_CHUNK
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
            assert_eq!(sha256_hash, SHA256_16M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_16mb_plus_1_sha256_kms() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(16, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

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
                SHA256_16M_PLUS_1_FILE_8M_CHUNK
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
                SHA256_16M_PLUS_1_FILE_8M_CHUNK
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
                SHA256_16M_PLUS_1_FILE_8M_CHUNK
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
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_16M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_16mb_crc64nvme() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(16, 0).unwrap();

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
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_16M_FILE_8M_CHUNK
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_16M_FILE_8M_CHUNK
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_16M_FILE_8M_CHUNK
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_16M_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_16mb_plus_1_crc64nvme() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(16, 1).unwrap();

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
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_16M_PLUS_1_FILE_8M_CHUNK
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_16M_PLUS_1_FILE_8M_CHUNK
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_16M_PLUS_1_FILE_8M_CHUNK
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_16M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_16mb_minus_1_crc64nvme() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(16, -1).unwrap();

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
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_MINUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_16M_MINUS_1_FILE_8M_CHUNK
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_MINUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_16M_MINUS_1_FILE_8M_CHUNK
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&BUCKET2.to_string(), TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_MINUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_16M_MINUS_1_FILE_8M_CHUNK
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
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_16M_MINUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_16mb_plus_1_crc64nvme_auto_chunksize() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(16, 1).unwrap();

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
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_16M_PLUS_1_FILE_8M_CHUNK
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
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_16M_PLUS_1_FILE_8M_CHUNK
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
            assert_eq!(object.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_16M_PLUS_1_FILE_8M_CHUNK
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
            assert_eq!(sha256_hash, SHA256_16M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }

    async fn test_multipart_upload_16mb_plus_1_crc64nvme_kms() {
        let helper = TestHelper::new().await;

        TestHelper::create_random_test_data_file(16, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

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
                CRC64NVME_16M_PLUS_1_FILE_8M_CHUNK
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
                CRC64NVME_16M_PLUS_1_FILE_8M_CHUNK
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
                CRC64NVME_16M_PLUS_1_FILE_8M_CHUNK
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
            assert_eq!(stats.sync_warning, 0);

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                TEMP_DOWNLOAD_DIR, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_16M_PLUS_1_FILE_WHOLE);
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
    }
}
