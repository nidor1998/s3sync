#[cfg(test)]
#[cfg(feature = "e2e_test")]
mod common;

#[cfg(test)]
#[cfg(feature = "e2e_test")]
mod tests {
    use chrono::Utc;
    use common::*;
    use s3sync::config::Config;
    use s3sync::config::args::parse_from_args;
    use s3sync::pipeline::Pipeline;
    use s3sync::types::token::create_pipeline_cancellation_token;
    use std::convert::TryFrom;
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn s3_to_local_point_in_time_source_bucket_versioning_error() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
        }

        let step1_complete_time;
        {
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "./test_data/point_in_time/step1",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            step1_complete_time = Utc::now() + chrono::Duration::seconds(2);

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 2);
            assert_eq!(stats.e_tag_verified, 2);
            assert_eq!(stats.checksum_verified, 2);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        tokio::time::sleep(Duration::from_secs(3)).await;

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);

            let point_in_time = step1_complete_time.to_rfc3339();

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--point-in-time",
                &point_in_time,
                "--check-additional-checksum",
                "SHA256",
                "--enable-additional-checksum",
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(pipeline.has_error());
        }

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_local_point_in_time() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
        }

        let step1_complete_time;
        {
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "./test_data/point_in_time/step1",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            step1_complete_time = Utc::now() + chrono::Duration::seconds(2);

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 2);
            assert_eq!(stats.e_tag_verified, 2);
            assert_eq!(stats.checksum_verified, 2);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        tokio::time::sleep(Duration::from_secs(3)).await;

        let step2_complete_time;
        {
            let target_bucket_url = format!("s3://{}/step2/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "./test_data/point_in_time/step2",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            step2_complete_time = Utc::now() + chrono::Duration::seconds(2);

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        tokio::time::sleep(Duration::from_secs(3)).await;

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        let delete_complete_time = Utc::now() + chrono::Duration::seconds(2);

        tokio::time::sleep(Duration::from_secs(3)).await;

        let step3_complete_time;
        {
            let target_bucket_url = format!("s3://{}/step3/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "./test_data/point_in_time/step3",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            step3_complete_time = Utc::now() + chrono::Duration::seconds(2);

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        tokio::time::sleep(Duration::from_secs(3)).await;

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let point_in_time = step1_complete_time.to_rfc3339();

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--point-in-time",
                &point_in_time,
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
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let point_in_time = step2_complete_time.to_rfc3339();

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--point-in-time",
                &point_in_time,
                &source_bucket_url,
                TEMP_DOWNLOAD_DIR,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 7);
            assert_eq!(stats.e_tag_verified, 7);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let point_in_time = delete_complete_time.to_rfc3339();

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--point-in-time",
                &point_in_time,
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
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);

            let point_in_time = step3_complete_time.to_rfc3339();

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--point-in-time",
                &point_in_time,
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
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);

            let point_in_time = step3_complete_time.to_rfc3339();

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--point-in-time",
                &point_in_time,
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
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);

            let point_in_time = step3_complete_time.to_rfc3339();

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--point-in-time",
                &point_in_time,
                "--check-additional-checksum",
                "SHA256",
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
            assert_eq!(stats.sync_complete, 0);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 1);
        }

        TestHelper::delete_all_files(TEMP_DOWNLOAD_DIR);
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }

    #[tokio::test]
    async fn s3_to_s3_point_in_time() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
        helper
            .delete_bucket_with_cascade(&BUCKET2.to_string())
            .await;

        {
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;
            helper.create_bucket(&BUCKET2.to_string(), REGION).await;
            helper.enable_bucket_versioning(&BUCKET1.to_string()).await;
        }

        let step1_complete_time;
        {
            let target_bucket_url = format!("s3://{}/step1/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "./test_data/point_in_time/step1",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            step1_complete_time = Utc::now() + chrono::Duration::seconds(2);

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 2);
            assert_eq!(stats.e_tag_verified, 2);
            assert_eq!(stats.checksum_verified, 2);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        tokio::time::sleep(Duration::from_secs(3)).await;

        let step2_complete_time;
        {
            let target_bucket_url = format!("s3://{}/step2/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "./test_data/point_in_time/step2",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            step2_complete_time = Utc::now() + chrono::Duration::seconds(2);

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 5);
            assert_eq!(stats.e_tag_verified, 5);
            assert_eq!(stats.checksum_verified, 5);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        tokio::time::sleep(Duration::from_secs(3)).await;

        helper.delete_all_objects(&BUCKET1.to_string()).await;
        let delete_complete_time = Utc::now() + chrono::Duration::seconds(2);

        tokio::time::sleep(Duration::from_secs(3)).await;

        let step3_complete_time;
        {
            let target_bucket_url = format!("s3://{}/step3/", *BUCKET1);

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "./test_data/point_in_time/step3",
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            step3_complete_time = Utc::now() + chrono::Duration::seconds(2);

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 1);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        tokio::time::sleep(Duration::from_secs(3)).await;

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let helper = TestHelper::new().await;
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let point_in_time = step1_complete_time.to_rfc3339();

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--point-in-time",
                &point_in_time,
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
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let helper = TestHelper::new().await;
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let point_in_time = step2_complete_time.to_rfc3339();

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--point-in-time",
                &point_in_time,
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 7);
            assert_eq!(stats.e_tag_verified, 7);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let helper = TestHelper::new().await;
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let point_in_time = delete_complete_time.to_rfc3339();

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--point-in-time",
                &point_in_time,
                &source_bucket_url,
                &target_bucket_url,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(!pipeline.has_error());

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 0);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let helper = TestHelper::new().await;
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let point_in_time = step3_complete_time.to_rfc3339();

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--point-in-time",
                &point_in_time,
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
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let helper = TestHelper::new().await;
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let point_in_time = step3_complete_time.to_rfc3339();

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--point-in-time",
                &point_in_time,
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
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let helper = TestHelper::new().await;
            helper.delete_all_objects(&BUCKET2.to_string()).await;

            let point_in_time = step3_complete_time.to_rfc3339();

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--point-in-time",
                &point_in_time,
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
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}", *BUCKET1);
            let target_bucket_url = format!("s3://{}", *BUCKET2);

            let point_in_time = step3_complete_time.to_rfc3339();

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--point-in-time",
                &point_in_time,
                "--check-additional-checksum",
                "SHA256",
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
            assert_eq!(stats.sync_complete, 0);
            assert_eq!(stats.e_tag_verified, 0);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 1);
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
