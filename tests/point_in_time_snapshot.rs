#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use common::*;
    use s3sync::config::Config;
    use s3sync::config::args::parse_from_args;
    use s3sync::pipeline::Pipeline;
    use s3sync::types::token::create_pipeline_cancellation_token;
    use std::convert::TryFrom;
    use std::time::Duration;
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn s3_to_local_point_in_time_source_bucket_versioning_error() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());

        {
            helper.create_bucket(&bucket, REGION).await;
        }

        let step1_complete_time;
        {
            let target_bucket_url = format!("s3://{}/step1/", bucket);

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
            let source_bucket_url = format!("s3://{}", bucket);

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
                &download_dir,
            ];
            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;
            assert!(pipeline.has_error());
        }
        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn s3_to_local_point_in_time() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());

        {
            helper.create_bucket(&bucket, REGION).await;
            helper.enable_bucket_versioning(&bucket).await;
        }

        let step1_complete_time;
        {
            let target_bucket_url = format!("s3://{}/step1/", bucket);

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
            let target_bucket_url = format!("s3://{}/step2/", bucket);

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

        helper.delete_all_objects(&bucket).await;
        let delete_complete_time = Utc::now() + chrono::Duration::seconds(2);

        tokio::time::sleep(Duration::from_secs(3)).await;

        let step3_complete_time;
        {
            let target_bucket_url = format!("s3://{}/step3/", bucket);

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
            let source_bucket_url = format!("s3://{}", bucket);
            TestHelper::delete_all_files(&download_dir);

            let point_in_time = step1_complete_time.to_rfc3339();

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--point-in-time",
                &point_in_time,
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
            assert_eq!(stats.e_tag_verified, 2);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}", bucket);
            TestHelper::delete_all_files(&download_dir);

            let point_in_time = step2_complete_time.to_rfc3339();

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--point-in-time",
                &point_in_time,
                &source_bucket_url,
                &download_dir,
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
            let source_bucket_url = format!("s3://{}", bucket);
            TestHelper::delete_all_files(&download_dir);

            let point_in_time = delete_complete_time.to_rfc3339();

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--point-in-time",
                &point_in_time,
                &source_bucket_url,
                &download_dir,
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
            let source_bucket_url = format!("s3://{}", bucket);
            TestHelper::delete_all_files(&download_dir);

            let point_in_time = step3_complete_time.to_rfc3339();

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--point-in-time",
                &point_in_time,
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
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}", bucket);

            let point_in_time = step3_complete_time.to_rfc3339();

            let args = vec![
                "s3sync",
                "--source-profile",
                "s3sync-e2e-test",
                "--point-in-time",
                &point_in_time,
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
            assert_eq!(stats.sync_skip, 0);
        }

        {
            let source_bucket_url = format!("s3://{}", bucket);

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
                &download_dir,
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
        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn s3_to_s3_point_in_time() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();

        {
            helper.create_bucket(&bucket1, REGION).await;
            helper.create_bucket(&bucket2, REGION).await;
            helper.enable_bucket_versioning(&bucket1).await;
        }

        let step1_complete_time;
        {
            let target_bucket_url = format!("s3://{}/step1/", bucket1);

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
            let target_bucket_url = format!("s3://{}/step2/", bucket1);

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

        helper.delete_all_objects(&bucket1).await;
        let delete_complete_time = Utc::now() + chrono::Duration::seconds(2);

        tokio::time::sleep(Duration::from_secs(3)).await;

        let step3_complete_time;
        {
            let target_bucket_url = format!("s3://{}/step3/", bucket1);

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
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let helper = TestHelper::new().await;
            helper.delete_all_objects(&bucket2).await;

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
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let helper = TestHelper::new().await;
            helper.delete_all_objects(&bucket2).await;

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
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let helper = TestHelper::new().await;
            helper.delete_all_objects(&bucket2).await;

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
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let helper = TestHelper::new().await;
            helper.delete_all_objects(&bucket2).await;

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
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let helper = TestHelper::new().await;
            helper.delete_all_objects(&bucket2).await;

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
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

            let helper = TestHelper::new().await;
            helper.delete_all_objects(&bucket2).await;

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
            let source_bucket_url = format!("s3://{}", bucket1);
            let target_bucket_url = format!("s3://{}", bucket2);

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
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }
}
