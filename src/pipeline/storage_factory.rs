use crate::Config;
use crate::config::ClientConfig;
use crate::storage::local::{LocalStorageFactory, fs_util};
use crate::storage::s3::S3StorageFactory;
use crate::storage::{Storage, StorageFactory, StoragePair};
use crate::types::token::PipelineCancellationToken;
use crate::types::{StoragePath, SyncStatistics};
use async_channel::Sender;
use aws_sdk_s3::types::RequestPayer;
use leaky_bucket::RateLimiter;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

// default refill interval 100ms
const REFILL_PER_INTERVAL_DIVIDER: usize = 10;

pub async fn create_storage_pair(
    config: Config,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
    has_warning: Arc<AtomicBool>,
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
                .fair(true)
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
                .fair(true)
                .build(),
        ))
    } else {
        None
    };

    let mut source_path = config.source.clone();
    let mut object_to_list = None;
    if let StoragePath::Local(path) = config.source.clone() {
        if path.is_dir() {
            let mut source_dir = path;
            if !source_dir
                .to_string_lossy()
                .ends_with(std::path::MAIN_SEPARATOR_STR)
            {
                source_dir = (source_dir.to_string_lossy().to_string()
                    + std::path::MAIN_SEPARATOR_STR)
                    .into();
            }
            source_path = StoragePath::Local(source_dir);
        } else {
            object_to_list = Some(path.file_name().unwrap().to_str().unwrap().to_string());
            let mut source_dir = fs_util::remove_file_name_if_exist(path);
            if source_dir.as_os_str().is_empty() {
                source_dir = ".".into();
            }
            if !source_dir
                .to_string_lossy()
                .ends_with(std::path::MAIN_SEPARATOR_STR)
            {
                source_dir = (source_dir.to_string_lossy().to_string()
                    + std::path::MAIN_SEPARATOR_STR)
                    .into();
            }
            source_path = StoragePath::Local(source_dir);
        }
    }

    let source = create_storage(
        config.clone(),
        config.source_client_config.clone(),
        config.source_client_config.clone().unwrap().request_payer,
        source_path,
        cancellation_token.clone(),
        stats_sender.clone(),
        rate_limit_objects_per_sec.clone(),
        rate_limit_bandwidth.clone(),
        has_warning.clone(),
        object_to_list,
    )
    .await;

    let mut target_path = config.target.clone();
    if let StoragePath::Local(path) = config.target.clone() {
        let mut tmp_path = path;
        if !tmp_path
            .to_string_lossy()
            .ends_with(std::path::MAIN_SEPARATOR_STR)
        {
            tmp_path =
                (tmp_path.to_string_lossy().to_string() + std::path::MAIN_SEPARATOR_STR).into();
        }
        target_path = StoragePath::Local(tmp_path);
    }
    let target = create_storage(
        config.clone(),
        config.target_client_config.clone(),
        config.target_client_config.clone().unwrap().request_payer,
        target_path,
        cancellation_token,
        stats_sender.clone(),
        rate_limit_objects_per_sec.clone(),
        rate_limit_bandwidth.clone(),
        has_warning.clone(),
        None,
    )
    .await;

    StoragePair { source, target }
}

#[allow(clippy::too_many_arguments)]
async fn create_storage(
    config: Config,
    client_config: Option<ClientConfig>,
    request_payer: Option<RequestPayer>,
    storage_path: StoragePath,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
    rate_limit_objects_per_sec: Option<Arc<RateLimiter>>,
    rate_limit_bandwidth: Option<Arc<RateLimiter>>,
    has_warning: Arc<AtomicBool>,
    object_to_list: Option<String>,
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
        request_payer,
        rate_limit_objects_per_sec,
        rate_limit_bandwidth,
        has_warning,
        object_to_list,
    )
    .await
}

#[cfg(test)]
mod tests {
    use crate::config::args::parse_from_args;
    use crate::types::token::create_pipeline_cancellation_token;
    use tracing_subscriber::EnvFilter;

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

        create_storage_pair(
            config,
            create_pipeline_cancellation_token(),
            stats_sender,
            Arc::new(AtomicBool::new(false)),
        )
        .await;
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

        let storage_pair = create_storage_pair(
            config,
            create_pipeline_cancellation_token(),
            stats_sender,
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        assert!(storage_pair.source.get_client().is_none());
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

        let storage_pair = create_storage_pair(
            config,
            create_pipeline_cancellation_token(),
            stats_sender,
            Arc::new(AtomicBool::new(false)),
        )
        .await;

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

        let storage_pair = create_storage_pair(
            config,
            create_pipeline_cancellation_token(),
            stats_sender,
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        assert!(storage_pair.target.get_client().is_none());
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
