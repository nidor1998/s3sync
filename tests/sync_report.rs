#[cfg(test)]
#[cfg(feature = "e2e_test")]
mod common;

#[cfg(test)]
#[cfg(feature = "e2e_test")]
mod tests {
    use super::*;
    use common::*;
    use s3sync::config::Config;
    use s3sync::config::args::parse_from_args;
    use s3sync::pipeline::Pipeline;
    use s3sync::types::token::create_pipeline_cancellation_token;
    use std::convert::TryFrom;
    use std::time::Duration;

    #[tokio::test]
    async fn local_to_s3_sync_report() {
        TestHelper::init_tracing_subscriber_for_report();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
        }

        {
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--metadata",
                "key2=value2,key1=value1",
                "--tagging",
                "tag2=valueB&tag1=valueA",
                "./test_data/point_in_time/step1",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 2);
            assert_eq!(stats.e_tag_verified, 2);
            assert_eq!(stats.checksum_verified, 2);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--report-sync-status",
                "./test_data/point_in_time/step1",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 2);
            assert_eq!(sync_stats.metadata_matches, 0);
            assert_eq!(sync_stats.tagging_matches, 0);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(!pipeline.has_warning());
        }

        {
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--report-sync-status",
                "./test_data/point_in_time/step2",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 5);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 0);
            assert_eq!(sync_stats.metadata_matches, 0);
            assert_eq!(sync_stats.tagging_matches, 0);
            assert_eq!(sync_stats.not_found, 3);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 2);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(pipeline.has_warning());
        }

        {
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--report-sync-status",
                "./test_data/point_in_time/step1",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 2);
            assert_eq!(sync_stats.checksum_matches, 0);
            assert_eq!(sync_stats.metadata_matches, 0);
            assert_eq!(sync_stats.tagging_matches, 0);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(!pipeline.has_warning());
        }

        {
            let target_bucket_url = format!("s3://{}/step2/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--report-sync-status",
                "./test_data/point_in_time/step2",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 5);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 0);
            assert_eq!(sync_stats.metadata_matches, 0);
            assert_eq!(sync_stats.tagging_matches, 0);
            assert_eq!(sync_stats.not_found, 5);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(pipeline.has_warning());
        }

        {
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--report-sync-status",
                "./test_data/point_in_time/step2",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 5);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 0);
            assert_eq!(sync_stats.metadata_matches, 0);
            assert_eq!(sync_stats.tagging_matches, 0);
            assert_eq!(sync_stats.not_found, 3);
            assert_eq!(sync_stats.etag_mismatch, 2);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(pipeline.has_warning());
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        {
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--sse",
                "aws:kms",
                "--metadata",
                "key2=value2,key1=value1",
                "--tagging",
                "tag2=valueB&tag1=valueA",
                "./test_data/point_in_time/step1",
                &target_bucket_url,
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
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--report-sync-status",
                "./test_data/point_in_time/step1",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 2);
            assert_eq!(sync_stats.metadata_matches, 0);
            assert_eq!(sync_stats.tagging_matches, 0);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(!pipeline.has_warning());
        }

        {
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--report-sync-status",
                "./test_data/point_in_time/step1",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 0);
            assert_eq!(sync_stats.metadata_matches, 0);
            assert_eq!(sync_stats.tagging_matches, 0);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 2);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(pipeline.has_warning());
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        {
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--metadata",
                "key2=value2,key1=value1",
                "--tagging",
                "tag2=valueB&tag1=valueA",
                "./test_data/point_in_time/step1",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 2);
            assert_eq!(stats.e_tag_verified, 2);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--report-sync-status",
                "./test_data/point_in_time/step1",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 0);
            assert_eq!(sync_stats.metadata_matches, 0);
            assert_eq!(sync_stats.tagging_matches, 0);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 2);

            assert!(pipeline.has_warning());
        }

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_local_sync_report() {
        TestHelper::init_tracing_subscriber_for_report();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
        }

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--metadata",
                "key2=value2,key1=value1",
                "--tagging",
                "tag2=valueB&tag1=valueA",
                "./test_data/point_in_time",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 8);
            assert_eq!(stats.e_tag_verified, 8);
            assert_eq!(stats.checksum_verified, 8);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 2);
            assert_eq!(stats.e_tag_verified, 2);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--report-sync-status",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 2);
            assert_eq!(sync_stats.metadata_matches, 0);
            assert_eq!(sync_stats.tagging_matches, 0);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(!pipeline.has_warning());
        }

        {
            let source_bucket_url = format!("s3://{}/step2/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--report-sync-status",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 5);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 0);
            assert_eq!(sync_stats.metadata_matches, 0);
            assert_eq!(sync_stats.tagging_matches, 0);
            assert_eq!(sync_stats.not_found, 3);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 2);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(pipeline.has_warning());
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--report-sync-status",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 2);
            assert_eq!(sync_stats.checksum_matches, 0);
            assert_eq!(sync_stats.metadata_matches, 0);
            assert_eq!(sync_stats.tagging_matches, 0);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(!pipeline.has_warning());
        }

        {
            let source_bucket_url = format!("s3://{}/step2/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--report-sync-status",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 5);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 0);
            assert_eq!(sync_stats.metadata_matches, 0);
            assert_eq!(sync_stats.tagging_matches, 0);
            assert_eq!(sync_stats.not_found, 3);
            assert_eq!(sync_stats.etag_mismatch, 2);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(pipeline.has_warning());
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--sse",
                "aws:kms",
                "--metadata",
                "key2=value2,key1=value1",
                "--tagging",
                "tag2=valueB&tag1=valueA",
                "./test_data/point_in_time",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 8);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 8);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--report-sync-status",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 0);
            assert_eq!(sync_stats.metadata_matches, 0);
            assert_eq!(sync_stats.tagging_matches, 0);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 2);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(pipeline.has_warning());
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--report-sync-status",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 2);
            assert_eq!(sync_stats.metadata_matches, 0);
            assert_eq!(sync_stats.tagging_matches, 0);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(!pipeline.has_warning());
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--metadata",
                "key2=value2,key1=value1",
                "--tagging",
                "tag2=valueB&tag1=valueA",
                "./test_data/point_in_time",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 8);
            assert_eq!(stats.e_tag_verified, 8);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--report-sync-status",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 0);
            assert_eq!(sync_stats.metadata_matches, 0);
            assert_eq!(sync_stats.tagging_matches, 0);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 2);

            assert!(pipeline.has_warning());
        }

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_sync_report() {
        TestHelper::init_tracing_subscriber_for_report();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
        }

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--metadata",
                "key2=value2,key1=value1",
                "--tagging",
                "tag2=valueB&tag1=valueA",
                "./test_data/point_in_time",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 8);
            assert_eq!(stats.e_tag_verified, 8);
            assert_eq!(stats.checksum_verified, 8);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

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
            assert_eq!(stats.sync_complete, 2);
            assert_eq!(stats.e_tag_verified, 2);
            assert_eq!(stats.checksum_verified, 2);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--report-sync-status",
                "--report-metadata-sync-status",
                "--report-tagging-sync-status",
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 2);
            assert_eq!(sync_stats.metadata_matches, 2);
            assert_eq!(sync_stats.tagging_matches, 2);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(!pipeline.has_warning());
        }

        {
            let source_bucket_url = format!("s3://{}/step2/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--report-sync-status",
                "--report-metadata-sync-status",
                "--report-tagging-sync-status",
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 5);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 0);
            assert_eq!(sync_stats.metadata_matches, 2);
            assert_eq!(sync_stats.tagging_matches, 2);
            assert_eq!(sync_stats.not_found, 3);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 2);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(pipeline.has_warning());
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--report-sync-status",
                "--report-metadata-sync-status",
                "--report-tagging-sync-status",
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 2);
            assert_eq!(sync_stats.checksum_matches, 0);
            assert_eq!(sync_stats.metadata_matches, 2);
            assert_eq!(sync_stats.tagging_matches, 2);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(!pipeline.has_warning());
        }

        {
            let source_bucket_url = format!("s3://{}/step2/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--report-sync-status",
                "--report-metadata-sync-status",
                "--report-tagging-sync-status",
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 5);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 0);
            assert_eq!(sync_stats.metadata_matches, 2);
            assert_eq!(sync_stats.tagging_matches, 2);
            assert_eq!(sync_stats.not_found, 3);
            assert_eq!(sync_stats.etag_mismatch, 2);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(pipeline.has_warning());
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--sse",
                "aws:kms",
                "--metadata",
                "key2=value2,key1=value1",
                "--tagging",
                "tag2=valueB&tag1=valueA",
                "./test_data/point_in_time",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 8);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 8);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--sse",
                "aws:kms",
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
            assert_eq!(stats.sync_complete, 2);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 2);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--report-sync-status",
                "--report-metadata-sync-status",
                "--report-tagging-sync-status",
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 0);
            assert_eq!(sync_stats.metadata_matches, 2);
            assert_eq!(sync_stats.tagging_matches, 2);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 2);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(pipeline.has_warning());
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--report-sync-status",
                "--report-metadata-sync-status",
                "--report-tagging-sync-status",
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 2);
            assert_eq!(sync_stats.metadata_matches, 2);
            assert_eq!(sync_stats.tagging_matches, 2);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(!pipeline.has_warning());
        }

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--metadata",
                "key2=value2,key1=value1",
                "--tagging",
                "tag2=valueB&tag1=valueA",
                "./test_data/point_in_time",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 8);
            assert_eq!(stats.e_tag_verified, 8);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
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
            assert_eq!(stats.sync_complete, 2);
            assert_eq!(stats.e_tag_verified, 2);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--report-sync-status",
                "--report-metadata-sync-status",
                "--report-tagging-sync-status",
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 0);
            assert_eq!(sync_stats.metadata_matches, 2);
            assert_eq!(sync_stats.tagging_matches, 2);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 2);

            assert!(pipeline.has_warning());
        }

        /////////

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--sse",
                "aws:kms",
                "--metadata",
                "key2=value2,key1=value1",
                "--tagging",
                "tag2=valueB&tag1=valueA",
                "./test_data/point_in_time",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 8);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 8);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

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
            assert_eq!(stats.sync_complete, 2);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 2);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--report-sync-status",
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 0);
            assert_eq!(sync_stats.metadata_matches, 0);
            assert_eq!(sync_stats.tagging_matches, 0);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 2);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(pipeline.has_warning());
        }

        /////////

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        helper.delete_all_objects(&BUCKET2.to_string()).await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--metadata",
                "key2=value2,key1=value1",
                "--tagging",
                "tag2=valueB&tag1=valueA",
                "./test_data/point_in_time",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 8);
            assert_eq!(stats.e_tag_verified, 8);
            assert_eq!(stats.checksum_verified, 8);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

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
            assert_eq!(stats.sync_complete, 2);
            assert_eq!(stats.e_tag_verified, 2);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--report-sync-status",
                "--report-metadata-sync-status",
                "--report-tagging-sync-status",
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 0);
            assert_eq!(sync_stats.metadata_matches, 2);
            assert_eq!(sync_stats.tagging_matches, 2);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 2);

            assert!(pipeline.has_warning());
        }

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_sync_metadata_report() {
        TestHelper::init_tracing_subscriber_for_report();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
        }

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--content-disposition",
                TEST_CONTENT_DISPOSITION,
                "--content-encoding",
                TEST_CONTENT_ENCODING,
                "--content-language",
                TEST_CONTENT_LANGUAGE,
                "--cache-control",
                TEST_CACHE_CONTROL,
                "--content-type",
                TEST_CONTENT_TYPE,
                "--expires",
                TEST_EXPIRES,
                "--metadata",
                TEST_METADATA_STRING,
                "--tagging",
                TEST_TAGGING,
                "--website-redirect",
                TEST_WEBSITE_REDIRECT,
                "./test_data/point_in_time",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 8);
            assert_eq!(stats.e_tag_verified, 8);
            assert_eq!(stats.checksum_verified, 8);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

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
            assert_eq!(stats.sync_complete, 2);
            assert_eq!(stats.e_tag_verified, 2);
            assert_eq!(stats.checksum_verified, 2);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--report-sync-status",
                "--report-metadata-sync-status",
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 2);
            assert_eq!(sync_stats.metadata_matches, 2);
            assert_eq!(sync_stats.tagging_matches, 0);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(!pipeline.has_warning());
        }

        helper.delete_all_objects(&BUCKET2.to_string()).await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
                "--no-sync-system-metadata",
                "--no-sync-user-defined-metadata",
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 2);
            assert_eq!(stats.e_tag_verified, 2);
            assert_eq!(stats.checksum_verified, 2);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--report-sync-status",
                "--report-metadata-sync-status",
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 2);
            assert_eq!(sync_stats.metadata_matches, 0);
            assert_eq!(sync_stats.tagging_matches, 0);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.metadata_mismatch, 2);
            assert_eq!(sync_stats.tagging_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(pipeline.has_warning());
        }

        helper.delete_all_objects(&BUCKET2.to_string()).await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
                "--content-disposition",
                TEST_CONTENT_DISPOSITION2,
                "--content-encoding",
                TEST_CONTENT_ENCODING2,
                "--content-language",
                TEST_CONTENT_LANGUAGE2,
                "--cache-control",
                TEST_CACHE_CONTROL2,
                "--content-type",
                TEST_CONTENT_TYPE2,
                "--expires",
                TEST_EXPIRES2,
                "--metadata",
                TEST_METADATA_STRING2,
                "--tagging",
                TEST_TAGGING2,
                "--website-redirect",
                TEST_WEBSITE_REDIRECT2,
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 2);
            assert_eq!(stats.e_tag_verified, 2);
            assert_eq!(stats.checksum_verified, 2);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--report-sync-status",
                "--report-metadata-sync-status",
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 2);
            assert_eq!(sync_stats.metadata_matches, 0);
            assert_eq!(sync_stats.tagging_matches, 0);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.metadata_mismatch, 2);
            assert_eq!(sync_stats.tagging_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(pipeline.has_warning());
        }

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_sync_tagging_report() {
        TestHelper::init_tracing_subscriber_for_report();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
        }

        {
            let target_bucket_url = format!("s3://{}", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--tagging",
                TEST_TAGGING,
                "./test_data/point_in_time",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 8);
            assert_eq!(stats.e_tag_verified, 8);
            assert_eq!(stats.checksum_verified, 8);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

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
            assert_eq!(stats.sync_complete, 2);
            assert_eq!(stats.e_tag_verified, 2);
            assert_eq!(stats.checksum_verified, 2);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--report-sync-status",
                "--report-tagging-sync-status",
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 2);
            assert_eq!(sync_stats.metadata_matches, 0);
            assert_eq!(sync_stats.tagging_matches, 2);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(!pipeline.has_warning());
        }

        helper.delete_all_objects(&BUCKET2.to_string()).await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
                "--disable-tagging",
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 2);
            assert_eq!(stats.e_tag_verified, 2);
            assert_eq!(stats.checksum_verified, 2);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--report-sync-status",
                "--report-tagging-sync-status",
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 2);
            assert_eq!(sync_stats.metadata_matches, 0);
            assert_eq!(sync_stats.tagging_matches, 0);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.metadata_mismatch, 0);
            assert_eq!(sync_stats.tagging_mismatch, 2);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(pipeline.has_warning());
        }

        helper.delete_all_objects(&BUCKET2.to_string()).await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
                "--tagging",
                TEST_TAGGING2,
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 2);
            assert_eq!(stats.e_tag_verified, 2);
            assert_eq!(stats.checksum_verified, 2);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}/step1/", *BUCKET1);
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET2);

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--check-additional-checksum",
                "SHA256",
                "--report-sync-status",
                "--report-tagging-sync-status",
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let sync_stats_tmp = pipeline.get_sync_stats_report();
            let sync_stats = sync_stats_tmp.lock().unwrap();
            assert_eq!(sync_stats.number_of_objects, 2);
            assert_eq!(sync_stats.etag_matches, 0);
            assert_eq!(sync_stats.checksum_matches, 2);
            assert_eq!(sync_stats.metadata_matches, 0);
            assert_eq!(sync_stats.tagging_matches, 0);
            assert_eq!(sync_stats.not_found, 0);
            assert_eq!(sync_stats.etag_mismatch, 0);
            assert_eq!(sync_stats.checksum_mismatch, 0);
            assert_eq!(sync_stats.metadata_mismatch, 0);
            assert_eq!(sync_stats.tagging_mismatch, 2);
            assert_eq!(sync_stats.etag_unknown, 0);
            assert_eq!(sync_stats.checksum_unknown, 0);

            assert!(pipeline.has_warning());
        }

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;
    }
}
