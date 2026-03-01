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

    const SHA256_10M_FILE_5M_CHUNK: &str = "uLuYA9R1hbgmjJlZcP8jzEDygVtq4mopxM//prh1GNk=-2";
    const CRC64NVME_10M_FILE_5M_CHUNK: &str = "cHKhFzFyIPM=";
    const ETAG_10M_FILE_5M_CHUNK: &str = "\"fd863860e4b73868097377d43bd65a58-2\"";
    const SHA256_10M_PLUS_1_FILE_5M_CHUNK: &str = "kkwf92kzoqv1UUa5LcRJdQ5wwTE1KsJHmUGQM3EURu8=-3";
    const CRC64NVME_10M_PLUS_1_FILE_5M_CHUNK: &str = "Z3VTxB3LlLM=";
    const ETAG_10M_PLUS_1_FILE_5M_CHUNK: &str = "\"527cc728f7dd89f1ef3256e2ff5c808c-3\"";
    const SHA256_10M_MINUS_1_FILE_5M_CHUNK: &str = "WC7KMpjlemoxe3Y8n/jdKlUTDZLGonZ26ktyBfuEBMo=-2";
    const CRC64NVME_10M_MINUS_1_FILE_5M_CHUNK: &str = "6XaOQ2PhG04=";
    const ETAG_10M_MINUS_1_FILE_5M_CHUNK: &str = "\"6e11660e4457458f925f9a92227be331-2\"";
    const SHA256_10M_FILE_WHOLE: &str =
        "d5fc3f080e832d82161f9461291f87989b81a9e6281c33589d9563adefb46055";
    const SHA256_10M_PLUS_1_FILE_WHOLE: &str =
        "cbb719063c17ba48ec3925cc4ba8267addd8515b4f69e689da1dfc3a6683191a";
    const SHA256_10M_MINUS_1_FILE_WHOLE: &str =
        "15b3422113fb29d70652d87367f92c23520963d13b71ef8efc73e158850bbadf";

    use uuid::Uuid;

    use super::*;


    #[tokio::test]
    async fn test_multipart_upload_10mb() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;


        TestHelper::create_random_test_data_file(10, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", bucket1);

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
                .head_object(&bucket1, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_FILE_5M_CHUNK);
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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_FILE_5M_CHUNK);
        }

        {
            helper.delete_all_objects(&bucket2).await;

            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_FILE_5M_CHUNK);
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                &download_dir, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_10M_FILE_WHOLE);
        }

        helper
            .delete_bucket_with_cascade(&bucket1)
            .await;
        helper
            .delete_bucket_with_cascade(&bucket2)
            .await;
    }

    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;


        TestHelper::create_random_test_data_file(10, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", bucket1);

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
                .head_object(&bucket1, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_PLUS_1_FILE_5M_CHUNK);
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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_PLUS_1_FILE_5M_CHUNK);
        }

        {
            helper.delete_all_objects(&bucket2).await;

            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_PLUS_1_FILE_5M_CHUNK);
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                &download_dir, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_10M_PLUS_1_FILE_WHOLE);
        }

        helper
            .delete_bucket_with_cascade(&bucket1)
            .await;
        helper
            .delete_bucket_with_cascade(&bucket2)
            .await;
    }

    #[tokio::test]
    async fn test_multipart_upload_10mb_minus_1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;


        TestHelper::create_random_test_data_file(10, -1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", bucket1);

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
                .head_object(&bucket1, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_MINUS_1_FILE_5M_CHUNK);
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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_MINUS_1_FILE_5M_CHUNK);
        }

        {
            helper.delete_all_objects(&bucket2).await;

            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_MINUS_1_FILE_5M_CHUNK);
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                &download_dir, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_10M_MINUS_1_FILE_WHOLE);
        }

        helper
            .delete_bucket_with_cascade(&bucket1)
            .await;
        helper
            .delete_bucket_with_cascade(&bucket2)
            .await;
    }

    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;


        TestHelper::create_random_test_data_file(10, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", bucket1);

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
                .head_object(&bucket1, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_PLUS_1_FILE_5M_CHUNK);
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
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            let object = helper
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_PLUS_1_FILE_5M_CHUNK);
        }

        {
            helper.delete_all_objects(&bucket2).await;

            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_PLUS_1_FILE_5M_CHUNK);
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket2);
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

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                &download_dir, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_10M_PLUS_1_FILE_WHOLE);
        }

        helper
            .delete_bucket_with_cascade(&bucket1)
            .await;
        helper
            .delete_bucket_with_cascade(&bucket2)
            .await;
    }

    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;


        TestHelper::create_random_test_data_file(10, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", bucket1);

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
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
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
            helper.delete_all_objects(&bucket2).await;

            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
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
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                &download_dir, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_10M_PLUS_1_FILE_WHOLE);
        }

        helper
            .delete_bucket_with_cascade(&bucket1)
            .await;
        helper
            .delete_bucket_with_cascade(&bucket2)
            .await;
    }

    #[tokio::test]
    async fn test_multipart_upload_10mb_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;


        TestHelper::create_random_test_data_file(10, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", bucket1);

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
                .head_object(&bucket1, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_FILE_5M_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_10M_FILE_5M_CHUNK);
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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_FILE_5M_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_10M_FILE_5M_CHUNK);
        }

        {
            helper.delete_all_objects(&bucket2).await;

            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_FILE_5M_CHUNK);
            assert_eq!(object.checksum_sha256.unwrap(), SHA256_10M_FILE_5M_CHUNK);
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket2);
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

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                &download_dir, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_10M_FILE_WHOLE);
        }

        helper
            .delete_bucket_with_cascade(&bucket1)
            .await;
        helper
            .delete_bucket_with_cascade(&bucket2)
            .await;
    }

    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;


        TestHelper::create_random_test_data_file(10, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", bucket1);

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
                .head_object(&bucket1, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_PLUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_10M_PLUS_1_FILE_5M_CHUNK
            );
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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_PLUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_10M_PLUS_1_FILE_5M_CHUNK
            );
        }

        {
            helper.delete_all_objects(&bucket2).await;

            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_PLUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_10M_PLUS_1_FILE_5M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket2);
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

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                &download_dir, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_10M_PLUS_1_FILE_WHOLE);
        }

        helper
            .delete_bucket_with_cascade(&bucket1)
            .await;
        helper
            .delete_bucket_with_cascade(&bucket2)
            .await;
    }

    #[tokio::test]
    async fn test_multipart_upload_10mb_minus_1_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;


        TestHelper::create_random_test_data_file(10, -1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", bucket1);

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
                .head_object(&bucket1, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_MINUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_10M_MINUS_1_FILE_5M_CHUNK
            );
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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_MINUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_10M_MINUS_1_FILE_5M_CHUNK
            );
        }

        {
            helper.delete_all_objects(&bucket2).await;

            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_MINUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_10M_MINUS_1_FILE_5M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket2);
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

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                &download_dir, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_10M_MINUS_1_FILE_WHOLE);
        }

        helper
            .delete_bucket_with_cascade(&bucket1)
            .await;
        helper
            .delete_bucket_with_cascade(&bucket2)
            .await;
    }

    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_sha256_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;


        TestHelper::create_random_test_data_file(10, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", bucket1);

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
                .head_object(&bucket1, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_PLUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_10M_PLUS_1_FILE_5M_CHUNK
            );
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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_PLUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_10M_PLUS_1_FILE_5M_CHUNK
            );
        }

        {
            helper.delete_all_objects(&bucket2).await;

            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_PLUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_10M_PLUS_1_FILE_5M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
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

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                &download_dir, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_10M_PLUS_1_FILE_WHOLE);
        }

        helper
            .delete_bucket_with_cascade(&bucket1)
            .await;
        helper
            .delete_bucket_with_cascade(&bucket2)
            .await;
    }

    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_sha256_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;


        TestHelper::create_random_test_data_file(10, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", bucket1);

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
                .head_object(&bucket1, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_10M_PLUS_1_FILE_5M_CHUNK
            );
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
                .head_object(&bucket1, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_10M_PLUS_1_FILE_5M_CHUNK
            );
        }

        {
            helper.delete_all_objects(&bucket2).await;

            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
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
                .head_object(&bucket1, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(
                object.checksum_sha256.unwrap(),
                SHA256_10M_PLUS_1_FILE_5M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket2);
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

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                &download_dir, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_10M_PLUS_1_FILE_WHOLE);
        }

        helper
            .delete_bucket_with_cascade(&bucket1)
            .await;
        helper
            .delete_bucket_with_cascade(&bucket2)
            .await;
    }

    #[tokio::test]
    async fn test_multipart_upload_10mb_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;


        TestHelper::create_random_test_data_file(10, 0).unwrap();

        {
            let target_bucket_url = format!("s3://{}", bucket1);

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
                .head_object(&bucket1, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_10M_FILE_5M_CHUNK
            );
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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_10M_FILE_5M_CHUNK
            );
        }

        {
            helper.delete_all_objects(&bucket2).await;

            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_10M_FILE_5M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket2);
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

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                &download_dir, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_10M_FILE_WHOLE);
        }

        helper
            .delete_bucket_with_cascade(&bucket1)
            .await;
        helper
            .delete_bucket_with_cascade(&bucket2)
            .await;
    }

    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;


        TestHelper::create_random_test_data_file(10, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", bucket1);

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
                .head_object(&bucket1, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_PLUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_10M_PLUS_1_FILE_5M_CHUNK
            );
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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_PLUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_10M_PLUS_1_FILE_5M_CHUNK
            );
        }

        {
            helper.delete_all_objects(&bucket2).await;

            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_PLUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_10M_PLUS_1_FILE_5M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket2);
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

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                &download_dir, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_10M_PLUS_1_FILE_WHOLE);
        }

        helper
            .delete_bucket_with_cascade(&bucket1)
            .await;
        helper
            .delete_bucket_with_cascade(&bucket2)
            .await;
    }

    #[tokio::test]
    async fn test_multipart_upload_10mb_minus_1_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;


        TestHelper::create_random_test_data_file(10, -1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", bucket1);

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
                .head_object(&bucket1, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_MINUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_10M_MINUS_1_FILE_5M_CHUNK
            );
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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_MINUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_10M_MINUS_1_FILE_5M_CHUNK
            );
        }

        {
            helper.delete_all_objects(&bucket2).await;

            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_MINUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_10M_MINUS_1_FILE_5M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket2);
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

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                &download_dir, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_10M_MINUS_1_FILE_WHOLE);
        }

        helper
            .delete_bucket_with_cascade(&bucket1)
            .await;
        helper
            .delete_bucket_with_cascade(&bucket2)
            .await;
    }

    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_crc64nvme_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;


        TestHelper::create_random_test_data_file(10, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", bucket1);

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
                .head_object(&bucket1, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_PLUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_10M_PLUS_1_FILE_5M_CHUNK
            );
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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_PLUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_10M_PLUS_1_FILE_5M_CHUNK
            );
        }

        {
            helper.delete_all_objects(&bucket2).await;

            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

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
                .head_object(&bucket2, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(object.e_tag.unwrap(), ETAG_10M_PLUS_1_FILE_5M_CHUNK);
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_10M_PLUS_1_FILE_5M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket2);
            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
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

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                &download_dir, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_10M_PLUS_1_FILE_WHOLE);
        }

        helper
            .delete_bucket_with_cascade(&bucket1)
            .await;
        helper
            .delete_bucket_with_cascade(&bucket2)
            .await;
    }

    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_crc64nvme_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;


        TestHelper::create_random_test_data_file(10, 1).unwrap();

        {
            let target_bucket_url = format!("s3://{}", bucket1);

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
                .head_object(&bucket1, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_10M_PLUS_1_FILE_5M_CHUNK
            );
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
                .head_object(&bucket1, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_10M_PLUS_1_FILE_5M_CHUNK
            );
        }

        {
            helper.delete_all_objects(&bucket2).await;

            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
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
                .head_object(&bucket1, TEST_RANDOM_DATA_FILE_KEY, None)
                .await;
            assert_eq!(
                object.checksum_crc64_nvme.unwrap(),
                CRC64NVME_10M_PLUS_1_FILE_5M_CHUNK
            );
        }

        {
            TestHelper::delete_all_files(&download_dir);

            let source_bucket_url = format!("s3://{}", bucket2);
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

            let sha256_hash = TestHelper::get_sha256_from_file(&format!(
                "{}/{}",
                &download_dir, TEST_RANDOM_DATA_FILE_KEY
            ));
            assert_eq!(sha256_hash, SHA256_10M_PLUS_1_FILE_WHOLE);
        }

        helper
            .delete_bucket_with_cascade(&bucket1)
            .await;
        helper
            .delete_bucket_with_cascade(&bucket2)
            .await;
    }
}
