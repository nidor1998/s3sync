use crate::Config;
use crate::pipeline::diff_detector::{DiffDetectionStrategy, DiffDetector};
use crate::types::S3syncObject;
use crate::types::event_callback::{EventData, EventType};
use async_trait::async_trait;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_smithy_types::DateTime;
use aws_smithy_types_convert::date_time::DateTimeExt;
use tracing::debug;

pub struct StandardDiffDetector {
    config: Config,
}

const FILTER_NAME: &str = "StandardDiffDetector";
#[async_trait]
impl DiffDetectionStrategy for StandardDiffDetector {
    async fn is_different(
        &self,
        source_object: &S3syncObject,
        target_object: &HeadObjectOutput,
    ) -> anyhow::Result<bool> {
        if source_object.size() == 0 && target_object.content_length().unwrap() == 0 {
            let mut event_data = EventData::new(EventType::SYNC_FILTERED);
            event_data.key = Some(source_object.key().to_string());
            // skipcq: RS-W1070
            event_data.source_version_id = source_object.version_id().map(|v| v.to_string());
            // skipcq: RS-W1070
            event_data.target_version_id = target_object.version_id().map(|v| v.to_string());
            event_data.source_last_modified = Some(*source_object.last_modified());
            event_data.target_last_modified = target_object.last_modified;
            event_data.source_size = Some(source_object.size() as u64);
            event_data.target_size = target_object.content_length().map(|v| v as u64);
            event_data.message = Some("Object filtered. Both sizes are zero".to_string());
            self.config.event_manager.trigger_event(event_data).await;

            return Ok(false);
        }

        // GetObjectOutput doesn't have nanos
        if target_object.last_modified().unwrap().secs() < source_object.last_modified().secs() {
            return Ok(true);
        }

        let source_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            source_object.last_modified().to_millis().unwrap(),
        ))?
        .to_rfc3339();
        let target_last_modified = DateTime::to_chrono_utc(&DateTime::from_millis(
            target_object.last_modified().unwrap().to_millis().unwrap(),
        ))?
        .to_rfc3339();
        let key = source_object.key();

        let mut event_data = EventData::new(EventType::SYNC_FILTERED);
        event_data.key = Some(key.to_string());
        // skipcq: RS-W1070
        event_data.source_version_id = source_object.version_id().map(|v| v.to_string());
        // skipcq: RS-W1070
        event_data.target_version_id = target_object.version_id().map(|v| v.to_string());
        event_data.source_last_modified = Some(*source_object.last_modified());
        event_data.target_last_modified = target_object.last_modified;
        event_data.source_size = Some(source_object.size() as u64);
        event_data.target_size = target_object.content_length().map(|v| v as u64);
        event_data.message = Some("Object filtered. The target is newer".to_string());
        self.config.event_manager.trigger_event(event_data).await;

        debug!(
            name = FILTER_NAME,
            source_last_modified = source_last_modified,
            target_last_modified = target_last_modified,
            key = key,
            "Object filtered."
        );

        Ok(false)
    }
}

impl StandardDiffDetector {
    pub fn boxed_new(config: Config) -> DiffDetector {
        Box::new(StandardDiffDetector { config })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::args::parse_from_args;
    use aws_sdk_s3::operation::head_object;
    use aws_sdk_s3::primitives::DateTime;
    use aws_sdk_s3::types::Object;
    use tracing_subscriber::EnvFilter;

    #[tokio::test]
    async fn check_same() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let diff_detector = StandardDiffDetector::boxed_new(config);

        let head_object_output = head_object::builders::HeadObjectOutputBuilder::default()
            .set_content_length(Some(6))
            .last_modified(DateTime::from_secs(1))
            .build();
        let source_object = S3syncObject::NotVersioning(
            Object::builder()
                .key("6byte.dat")
                .size(6)
                .last_modified(DateTime::from_secs(1))
                .e_tag("e_tag")
                .build(),
        );
        assert!(
            !diff_detector
                .is_different(&source_object, &head_object_output)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn check_same_size_zero() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let diff_detector = StandardDiffDetector::boxed_new(config);

        let head_object_output = head_object::builders::HeadObjectOutputBuilder::default()
            .set_content_length(Some(0))
            .last_modified(DateTime::from_secs(1))
            .build();
        let source_object = S3syncObject::NotVersioning(
            Object::builder()
                .key("6byte.dat")
                .size(0)
                .last_modified(DateTime::from_secs(2))
                .e_tag("e_tag")
                .build(),
        );
        assert!(
            !diff_detector
                .is_different(&source_object, &head_object_output)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn check_different() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--allow-both-local-storage",
            "./test_data/source/dir1/",
            "./test_data/target/dir1/",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();

        let diff_detector = StandardDiffDetector::boxed_new(config);

        let head_object_output = head_object::builders::HeadObjectOutputBuilder::default()
            .set_content_length(Some(6))
            .last_modified(DateTime::from_secs(1))
            .build();
        let source_object = S3syncObject::NotVersioning(
            Object::builder()
                .key("6byte.dat")
                .size(5)
                .last_modified(DateTime::from_secs(2))
                .e_tag("e_tag")
                .build(),
        );
        assert!(
            diff_detector
                .is_different(&source_object, &head_object_output)
                .await
                .unwrap()
        );
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .or_else(|_| EnvFilter::try_new("dummy=trace"))
                    .unwrap(),
            )
            .try_init();
    }
}
