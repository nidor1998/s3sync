use std::sync::Arc;

use async_channel::Sender;
use leaky_bucket::RateLimiter;

use crate::config::ClientConfig;
use crate::storage::local::LocalStorageFactory;
use crate::storage::s3::S3StorageFactory;
use crate::storage::{Storage, StorageFactory, StoragePair};
use crate::types::token::PipelineCancellationToken;
use crate::types::{StoragePath, SyncStatistics};
use crate::Config;

// default refill interval 100ms
const REFILL_PER_INTERVAL_DIVIDER: usize = 10;

pub async fn create_storage_pair(
    config: Config,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> StoragePair {
    let rate_limit_objects_per_sec = if config.rate_limit_objects.is_some() {
        let rate_limit_value = config.rate_limit_objects.unwrap();
        let refill = if rate_limit_value <= REFILL_PER_INTERVAL_DIVIDER as u32 {
            1
        } else {
            rate_limit_value as usize / REFILL_PER_INTERVAL_DIVIDER
        };

        Some(Arc::new(
            RateLimiter::builder()
                .max(rate_limit_value as usize)
                .initial(rate_limit_value as usize)
                .refill(refill)
                .fair(false)
                .build(),
        ))
    } else {
        None
    };

    let rate_limit_bandwidth = if config.rate_limit_bandwidth.is_some() {
        let rate_limit_bandwidth = config.rate_limit_bandwidth.unwrap();
        let refill = rate_limit_bandwidth as usize / REFILL_PER_INTERVAL_DIVIDER;
        Some(Arc::new(
            RateLimiter::builder()
                .max(rate_limit_bandwidth as usize)
                .initial(rate_limit_bandwidth as usize)
                .refill(refill)
                .fair(false)
                .build(),
        ))
    } else {
        None
    };

    let source = create_storage(
        config.clone(),
        config.source_client_config.clone(),
        config.source.clone(),
        cancellation_token.clone(),
        stats_sender.clone(),
        rate_limit_objects_per_sec.clone(),
        rate_limit_bandwidth.clone(),
    )
    .await;

    let target = create_storage(
        config.clone(),
        config.target_client_config,
        config.target,
        cancellation_token,
        stats_sender.clone(),
        rate_limit_objects_per_sec.clone(),
        rate_limit_bandwidth.clone(),
    )
    .await;

    StoragePair { source, target }
}

async fn create_storage(
    config: Config,
    client_config: Option<ClientConfig>,
    storage_path: StoragePath,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
    rate_limit_objects_per_sec: Option<Arc<RateLimiter>>,
    rate_limit_bandwidth: Option<Arc<RateLimiter>>,
) -> Storage {
    let factory_fn = match storage_path {
        StoragePath::S3 { .. } => S3StorageFactory::create,
        StoragePath::Local(_) => LocalStorageFactory::create,
    };

    factory_fn(
        config,
        storage_path,
        cancellation_token,
        stats_sender,
        client_config,
        rate_limit_objects_per_sec,
        rate_limit_bandwidth,
    )
    .await
}

#[cfg(test)]
mod tests {
    use crate::config::args::parse_from_args;
    use crate::types::token::create_pipeline_cancellation_token;

    use super::*;

    #[tokio::test]
    async fn create_s3_storage_pair() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "source_access_key",
            "--source-secret-access-key",
            "source_secret_access_key",
            "--target-access-key",
            "target_access_key",
            "--target-secret-access-key",
            "target_secret_access_key",
            "s3://source-bucket",
            "s3://target-bucket",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage_pair =
            create_storage_pair(config, create_pipeline_cancellation_token(), stats_sender).await;

        assert_eq!(
            storage_pair
                .source
                .get_client()
                .unwrap()
                .conf()
                .credentials_cache()
                .as_ref()
                .provide_cached_credentials()
                .await
                .unwrap()
                .access_key_id(),
            "source_access_key".to_string()
        );

        assert_eq!(
            storage_pair
                .target
                .get_client()
                .unwrap()
                .conf()
                .credentials_cache()
                .as_ref()
                .provide_cached_credentials()
                .await
                .unwrap()
                .access_key_id(),
            "target_access_key".to_string()
        );
    }

    #[tokio::test]
    async fn create_storage_pair_source_local() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--target-access-key",
            "target_access_key",
            "--target-secret-access-key",
            "target_secret_access_key",
            "./test_data/source",
            "s3://target-bucket",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage_pair =
            create_storage_pair(config, create_pipeline_cancellation_token(), stats_sender).await;

        assert!(storage_pair.source.get_client().is_none());

        assert_eq!(
            storage_pair
                .target
                .get_client()
                .unwrap()
                .conf()
                .credentials_cache()
                .as_ref()
                .provide_cached_credentials()
                .await
                .unwrap()
                .access_key_id(),
            "target_access_key".to_string()
        );
    }

    #[tokio::test]
    async fn create_storage_pair_target_local() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "source_access_key",
            "--source-secret-access-key",
            "source_secret_access_key",
            "s3://source-bucket",
            "./test_data/target",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage_pair =
            create_storage_pair(config, create_pipeline_cancellation_token(), stats_sender).await;

        assert_eq!(
            storage_pair
                .source
                .get_client()
                .unwrap()
                .conf()
                .credentials_cache()
                .as_ref()
                .provide_cached_credentials()
                .await
                .unwrap()
                .access_key_id(),
            "source_access_key".to_string()
        );

        assert!(storage_pair.target.get_client().is_none());
    }

    #[tokio::test]
    async fn create_storage_pair_with_ratelimit() {
        init_dummy_tracing_subscriber();

        let args = vec![
            "s3sync",
            "--source-access-key",
            "source_access_key",
            "--source-secret-access-key",
            "source_secret_access_key",
            "--rate-limit-objects",
            "10",
            "--rate-limit-bandwidth",
            "100MiB",
            "s3://source-bucket",
            "./test_data/target",
        ];
        let config = Config::try_from(parse_from_args(args).unwrap()).unwrap();
        let (stats_sender, _) = async_channel::unbounded();

        let storage_pair =
            create_storage_pair(config, create_pipeline_cancellation_token(), stats_sender).await;

        assert_eq!(
            storage_pair
                .source
                .get_client()
                .unwrap()
                .conf()
                .credentials_cache()
                .as_ref()
                .provide_cached_credentials()
                .await
                .unwrap()
                .access_key_id(),
            "source_access_key".to_string()
        );

        assert!(storage_pair.target.get_client().is_none());
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
