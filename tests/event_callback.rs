use async_trait::async_trait;
use s3sync::types::event_callback::{EventCallback, EventData, EventType};

pub struct TestEventCallback;

#[async_trait]
impl EventCallback for TestEventCallback {
    async fn on_event(&mut self, event_data: EventData) {
        match event_data.event_type {
            EventType::SYNC_COMPLETE => {
                println!("Sync complete: {event_data:?}");
            }
            _ => {
                println!("Other events: {event_data:?}");
            }
        }
    }
}

#[cfg(test)]
#[cfg(feature = "e2e_test")]
mod common;

#[cfg(test)]
#[cfg(feature = "e2e_test")]
mod tests {
    use common::*;
    use s3sync::config::Config;
    use s3sync::config::args::parse_from_args;
    use s3sync::pipeline::Pipeline;
    use s3sync::types::token::create_pipeline_cancellation_token;
    use std::convert::TryFrom;

    use super::*;

    #[tokio::test]
    async fn test_event_callback() {
        TestHelper::init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let helper = TestHelper::new().await;
        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;

        {
            let target_bucket_url = format!("s3://{}", BUCKET1.to_string());
            helper.create_bucket(&BUCKET1.to_string(), REGION).await;

            let args = vec![
                "s3sync",
                "--target-profile",
                "s3sync-e2e-test",
                "./test_data/e2e_test/case1/",
                &target_bucket_url,
            ];
            let mut config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
            config
                .event_manager
                .register_callback(EventType::ALL_EVENTS, TestEventCallback {});
            let cancellation_token = create_pipeline_cancellation_token();
            let mut pipeline = Pipeline::new(config.clone(), cancellation_token).await;

            pipeline.run().await;

            assert!(!pipeline.has_error());
        }

        helper
            .delete_bucket_with_cascade(&BUCKET1.to_string())
            .await;
    }
}
