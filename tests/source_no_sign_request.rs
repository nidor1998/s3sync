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
    async fn source_no_sign_request_reads_public_bucket() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        let download_dir = format!("./playground/download_{}/", Uuid::new_v4());

        helper.create_bucket(&bucket, REGION).await;
        helper.disable_block_public_access(&bucket).await;
        helper
            .put_object_with_metadata(&bucket, "data1", "./test_data/e2e_test/case1/data1")
            .await;
        helper
            .put_bucket_policy_public_read_get_object(&bucket)
            .await;

        {
            let source_bucket_url = format!("s3://{}", bucket);
            let args = vec![
                "s3sync",
                "--source-no-sign-request",
                "--source-region",
                REGION,
                &source_bucket_url,
                &download_dir,
            ];

            let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;
            pipeline.run().await;
            assert!(!pipeline.has_error(), "anonymous source sync errored");

            let stats = TestHelper::get_stats_count(pipeline.get_stats_receiver());
            assert_eq!(stats.sync_complete, 1);
            assert_eq!(stats.e_tag_verified, 1);
            assert_eq!(stats.checksum_verified, 0);
            assert_eq!(stats.sync_warning, 0);

            assert!(TestHelper::verify_file_md5_digest(
                "./test_data/e2e_test/case1/data1",
                &TestHelper::md5_digest(&format!("{}/data1", &download_dir)),
            ));
        }

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&download_dir);
    }
}
