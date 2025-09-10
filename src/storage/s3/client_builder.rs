use aws_config::meta::region::{ProvideRegion, RegionProviderChain};
use aws_config::retry::RetryConfig;
use aws_config::{BehaviorVersion, ConfigLoader};
use aws_runtime::env_config::file::{EnvConfigFileKind, EnvConfigFiles};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Builder;
use std::time::Duration;

use crate::config::ClientConfig;
use aws_smithy_runtime_api::client::stalled_stream_protection::StalledStreamProtectionConfig;
use aws_smithy_types::timeout::TimeoutConfig;
use aws_types::SdkConfig;
use aws_types::region::Region;

impl ClientConfig {
    pub async fn create_client(&self) -> Client {
        let mut config_builder = Builder::from(&self.load_sdk_config().await)
            .force_path_style(self.force_path_style)
            .request_checksum_calculation(self.request_checksum_calculation)
            .accelerate(self.accelerate);

        if let Some(timeout_config) = self.build_timeout_config() {
            config_builder = config_builder.timeout_config(timeout_config);
        }

        Client::from_conf(config_builder.build())
    }

    async fn load_sdk_config(&self) -> SdkConfig {
        let config_loader = if self.disable_stalled_stream_protection {
            aws_config::defaults(BehaviorVersion::latest())
                .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
        } else {
            aws_config::defaults(BehaviorVersion::latest())
                .stalled_stream_protection(StalledStreamProtectionConfig::enabled().build())
        };
        let mut config_loader = self
            .load_config_credential(config_loader)
            .region(self.build_region_provider())
            .retry_config(self.build_retry_config());

        if let Some(endpoint_url) = &self.endpoint_url {
            config_loader = config_loader.endpoint_url(endpoint_url);
        };

        config_loader.load().await
    }

    fn load_config_credential(&self, mut config_loader: ConfigLoader) -> ConfigLoader {
        match &self.credential {
            crate::types::S3Credentials::Credentials { access_keys } => {
                let credentials = aws_sdk_s3::config::Credentials::new(
                    access_keys.access_key.to_string(),
                    access_keys.secret_access_key.to_string(),
                    access_keys.session_token.clone(),
                    None,
                    "",
                );
                config_loader = config_loader.credentials_provider(credentials);
            }
            crate::types::S3Credentials::Profile(profile_name) => {
                let mut builder = aws_config::profile::ProfileFileCredentialsProvider::builder();

                if let Some(aws_shared_credentials_file) = self
                    .client_config_location
                    .aws_shared_credentials_file
                    .as_ref()
                {
                    let profile_files = EnvConfigFiles::builder()
                        .with_file(EnvConfigFileKind::Credentials, aws_shared_credentials_file)
                        .build();
                    builder = builder.profile_files(profile_files)
                }

                config_loader =
                    config_loader.credentials_provider(builder.profile_name(profile_name).build());
            }
            crate::types::S3Credentials::FromEnvironment => {}
        }
        config_loader
    }

    fn build_region_provider(&self) -> Box<dyn ProvideRegion> {
        let mut builder = aws_config::profile::ProfileFileRegionProvider::builder();

        if let crate::types::S3Credentials::Profile(profile_name) = &self.credential {
            if let Some(aws_config_file) = self.client_config_location.aws_config_file.as_ref() {
                let profile_files = EnvConfigFiles::builder()
                    .with_file(EnvConfigFileKind::Config, aws_config_file)
                    .build();
                builder = builder.profile_files(profile_files);
            }
            builder = builder.profile_name(profile_name)
        }

        let provider_region = if matches!(
            &self.credential,
            crate::types::S3Credentials::FromEnvironment
        ) {
            RegionProviderChain::first_try(self.region.clone().map(Region::new))
                .or_default_provider()
        } else {
            RegionProviderChain::first_try(self.region.clone().map(Region::new))
                .or_else(builder.build())
        };

        Box::new(provider_region)
    }

    fn build_retry_config(&self) -> RetryConfig {
        RetryConfig::standard()
            .with_max_attempts(self.retry_config.aws_max_attempts)
            .with_initial_backoff(std::time::Duration::from_millis(
                self.retry_config.initial_backoff_milliseconds,
            ))
    }

    fn build_timeout_config(&self) -> Option<TimeoutConfig> {
        // TimeoutConfig is optional, but setting each timeout to None does not cause the SDK to use default timeouts.
        let operation_timeout = self
            .cli_timeout_config
            .operation_timeout_milliseconds
            .map(Duration::from_millis);
        let operation_attempt_timeout = self
            .cli_timeout_config
            .operation_attempt_timeout_milliseconds
            .map(Duration::from_millis);
        let connect_timeout = self
            .cli_timeout_config
            .connect_timeout_milliseconds
            .map(Duration::from_millis);
        let read_timeout = self
            .cli_timeout_config
            .read_timeout_milliseconds
            .map(Duration::from_millis);

        if operation_timeout.is_none()
            && operation_attempt_timeout.is_none()
            && connect_timeout.is_none()
            && read_timeout.is_none()
        {
            return None;
        }

        let mut builder = TimeoutConfig::builder();

        builder = if let Some(operation_timeout) = operation_timeout {
            builder.operation_timeout(operation_timeout)
        } else {
            builder
        };

        builder = if let Some(operation_attempt_timeout) = operation_attempt_timeout {
            builder.operation_attempt_timeout(operation_attempt_timeout)
        } else {
            builder
        };

        builder = if let Some(connect_timeout) = connect_timeout {
            builder.connect_timeout(connect_timeout)
        } else {
            builder
        };

        builder = if let Some(read_timeout) = read_timeout {
            builder.read_timeout(read_timeout)
        } else {
            builder
        };

        Some(builder.build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{AccessKeys, ClientConfigLocation};
    use aws_smithy_types::checksum_config::RequestChecksumCalculation;
    use std::sync::Arc;
    use tokio::sync::Semaphore;
    use tracing_subscriber::EnvFilter;

    #[tokio::test]
    async fn create_client_from_credentials() {
        init_dummy_tracing_subscriber();

        let client_config = ClientConfig {
            client_config_location: ClientConfigLocation {
                aws_config_file: None,
                aws_shared_credentials_file: None,
            },
            credential: crate::types::S3Credentials::Credentials {
                access_keys: AccessKeys {
                    access_key: "my_access_key".to_string(),
                    secret_access_key: "my_secret_access_key".to_string(),
                    session_token: Some("my_session_token".to_string()),
                },
            },
            region: Some("my-region".to_string()),
            endpoint_url: Some("https://my.endpoint.local".to_string()),
            force_path_style: false,
            retry_config: crate::config::RetryConfig {
                aws_max_attempts: 10,
                initial_backoff_milliseconds: 100,
            },
            cli_timeout_config: crate::config::CLITimeoutConfig {
                operation_timeout_milliseconds: None,
                operation_attempt_timeout_milliseconds: None,
                connect_timeout_milliseconds: None,
                read_timeout_milliseconds: None,
            },
            disable_stalled_stream_protection: false,
            request_checksum_calculation: RequestChecksumCalculation::WhenRequired,
            parallel_upload_semaphore: Arc::new(Semaphore::new(1)),
            accelerate: false,
            request_payer: None,
        };

        let client = client_config.create_client().await;

        let retry_config = client.config().retry_config().unwrap();
        assert_eq!(retry_config.max_attempts(), 10);
        assert_eq!(
            retry_config.initial_backoff(),
            std::time::Duration::from_millis(100)
        );

        let timeout_config = client.config().timeout_config().unwrap();
        assert!(timeout_config.operation_timeout().is_none());
        assert!(timeout_config.operation_attempt_timeout().is_none());
        assert!(timeout_config.connect_timeout().is_some());
        assert!(timeout_config.read_timeout().is_none());
        assert!(timeout_config.has_timeouts());

        // AWS SDK have default connect timeout
        assert_eq!(
            timeout_config.connect_timeout(),
            Some(Duration::from_millis(3100))
        );

        assert_eq!(
            client.config().region().unwrap().to_string(),
            "my-region".to_string()
        );
    }

    #[tokio::test]
    async fn create_client_from_credentials_with_default_region() {
        init_dummy_tracing_subscriber();

        let client_config = ClientConfig {
            client_config_location: ClientConfigLocation {
                aws_config_file: None,
                aws_shared_credentials_file: None,
            },
            credential: crate::types::S3Credentials::Credentials {
                access_keys: AccessKeys {
                    access_key: "my_access_key".to_string(),
                    secret_access_key: "my_secret_access_key".to_string(),
                    session_token: Some("my_session_token".to_string()),
                },
            },
            region: None,
            endpoint_url: Some("https://my.endpoint.local".to_string()),
            force_path_style: false,
            retry_config: crate::config::RetryConfig {
                aws_max_attempts: 10,
                initial_backoff_milliseconds: 100,
            },
            cli_timeout_config: crate::config::CLITimeoutConfig {
                operation_timeout_milliseconds: Some(1000),
                operation_attempt_timeout_milliseconds: Some(2000),
                connect_timeout_milliseconds: Some(3000),
                read_timeout_milliseconds: Some(4000),
            },
            disable_stalled_stream_protection: false,
            request_checksum_calculation: RequestChecksumCalculation::WhenRequired,
            parallel_upload_semaphore: Arc::new(Semaphore::new(1)),
            accelerate: false,
            request_payer: None,
        };

        let client = client_config.create_client().await;

        let retry_config = client.config().retry_config().unwrap();
        assert_eq!(retry_config.max_attempts(), 10);
        assert_eq!(
            retry_config.initial_backoff(),
            std::time::Duration::from_millis(100)
        );

        let timeout_config = client.config().timeout_config().unwrap();
        assert_eq!(
            timeout_config.operation_timeout(),
            Some(Duration::from_millis(1000))
        );
        assert_eq!(
            timeout_config.operation_attempt_timeout(),
            Some(Duration::from_millis(2000))
        );
        assert_eq!(
            timeout_config.connect_timeout(),
            Some(Duration::from_millis(3000))
        );
        assert_eq!(
            timeout_config.read_timeout(),
            Some(Duration::from_millis(4000))
        );
        assert!(timeout_config.has_timeouts());
    }

    #[tokio::test]
    async fn create_client_from_custom_profile() {
        init_dummy_tracing_subscriber();

        let client_config = ClientConfig {
            client_config_location: ClientConfigLocation {
                aws_config_file: Some("./test_data/test_config/config".into()),
                aws_shared_credentials_file: Some("./test_data/test_config/credentials".into()),
            },
            credential: crate::types::S3Credentials::Profile("aws".to_string()),
            region: Some("my-region".to_string()),
            endpoint_url: Some("https://my.endpoint.local".to_string()),
            force_path_style: false,
            retry_config: crate::config::RetryConfig {
                aws_max_attempts: 10,
                initial_backoff_milliseconds: 100,
            },
            cli_timeout_config: crate::config::CLITimeoutConfig {
                operation_timeout_milliseconds: None,
                operation_attempt_timeout_milliseconds: None,
                connect_timeout_milliseconds: None,
                read_timeout_milliseconds: None,
            },
            disable_stalled_stream_protection: false,
            request_checksum_calculation: RequestChecksumCalculation::WhenRequired,
            parallel_upload_semaphore: Arc::new(Semaphore::new(1)),
            accelerate: false,
            request_payer: None,
        };

        let client = client_config.create_client().await;

        let retry_config = client.config().retry_config().unwrap();
        assert_eq!(retry_config.max_attempts(), 10);
        assert_eq!(
            retry_config.initial_backoff(),
            std::time::Duration::from_millis(100)
        );

        assert_eq!(
            client.config().region().unwrap().to_string(),
            "my-region".to_string()
        );
    }

    #[tokio::test]
    async fn create_client_from_custom_timeout() {
        init_dummy_tracing_subscriber();

        let client_config = ClientConfig {
            client_config_location: ClientConfigLocation {
                aws_config_file: Some("./test_data/test_config/config".into()),
                aws_shared_credentials_file: Some("./test_data/test_config/credentials".into()),
            },
            credential: crate::types::S3Credentials::Profile("aws".to_string()),
            region: Some("my-region".to_string()),
            endpoint_url: Some("https://my.endpoint.local".to_string()),
            force_path_style: false,
            retry_config: crate::config::RetryConfig {
                aws_max_attempts: 10,
                initial_backoff_milliseconds: 100,
            },
            cli_timeout_config: crate::config::CLITimeoutConfig {
                operation_timeout_milliseconds: Some(1000),
                operation_attempt_timeout_milliseconds: None,
                connect_timeout_milliseconds: None,
                read_timeout_milliseconds: None,
            },
            disable_stalled_stream_protection: false,
            request_checksum_calculation: RequestChecksumCalculation::WhenRequired,
            parallel_upload_semaphore: Arc::new(Semaphore::new(1)),
            accelerate: false,
            request_payer: None,
        };

        let client = client_config.create_client().await;

        let retry_config = client.config().retry_config().unwrap();
        assert_eq!(retry_config.max_attempts(), 10);
        assert_eq!(
            retry_config.initial_backoff(),
            std::time::Duration::from_millis(100)
        );

        let timeout_config = client.config().timeout_config().unwrap();
        assert_eq!(
            timeout_config.operation_timeout(),
            Some(Duration::from_millis(1000))
        );
        assert!(timeout_config.operation_attempt_timeout().is_none());
        assert!(timeout_config.connect_timeout().is_some());
        assert!(timeout_config.read_timeout().is_none());
        assert!(timeout_config.has_timeouts());

        assert_eq!(
            client.config().region().unwrap().to_string(),
            "my-region".to_string()
        );
    }

    #[tokio::test]
    async fn create_client_from_custom_timeout_case2() {
        init_dummy_tracing_subscriber();

        let client_config = ClientConfig {
            client_config_location: ClientConfigLocation {
                aws_config_file: Some("./test_data/test_config/config".into()),
                aws_shared_credentials_file: Some("./test_data/test_config/credentials".into()),
            },
            credential: crate::types::S3Credentials::Profile("aws".to_string()),
            region: Some("my-region".to_string()),
            endpoint_url: Some("https://my.endpoint.local".to_string()),
            force_path_style: false,
            retry_config: crate::config::RetryConfig {
                aws_max_attempts: 10,
                initial_backoff_milliseconds: 100,
            },
            cli_timeout_config: crate::config::CLITimeoutConfig {
                operation_timeout_milliseconds: None,
                operation_attempt_timeout_milliseconds: None,
                connect_timeout_milliseconds: Some(1000),
                read_timeout_milliseconds: None,
            },
            disable_stalled_stream_protection: false,
            request_checksum_calculation: RequestChecksumCalculation::WhenRequired,
            parallel_upload_semaphore: Arc::new(Semaphore::new(1)),
            accelerate: false,
            request_payer: None,
        };

        let client = client_config.create_client().await;

        let retry_config = client.config().retry_config().unwrap();
        assert_eq!(retry_config.max_attempts(), 10);
        assert_eq!(
            retry_config.initial_backoff(),
            std::time::Duration::from_millis(100)
        );

        let timeout_config = client.config().timeout_config().unwrap();
        assert!(timeout_config.connect_timeout().is_some());
        assert!(timeout_config.operation_attempt_timeout().is_none());
        assert!(timeout_config.connect_timeout().is_some());
        assert!(timeout_config.read_timeout().is_none());
        assert!(timeout_config.has_timeouts());

        assert_eq!(
            client.config().region().unwrap().to_string(),
            "my-region".to_string()
        );
    }

    #[tokio::test]
    async fn create_client_from_default_profile() {
        init_dummy_tracing_subscriber();

        let client_config = ClientConfig {
            client_config_location: ClientConfigLocation {
                aws_config_file: Some("./test_data/test_config/config".into()),
                aws_shared_credentials_file: Some("./test_data/test_config/credentials".into()),
            },
            credential: crate::types::S3Credentials::Profile("default".to_string()),
            region: None,
            endpoint_url: Some("https://my.endpoint.local".to_string()),
            force_path_style: false,
            retry_config: crate::config::RetryConfig {
                aws_max_attempts: 10,
                initial_backoff_milliseconds: 100,
            },
            cli_timeout_config: crate::config::CLITimeoutConfig {
                operation_timeout_milliseconds: None,
                operation_attempt_timeout_milliseconds: None,
                connect_timeout_milliseconds: None,
                read_timeout_milliseconds: None,
            },
            disable_stalled_stream_protection: false,
            request_checksum_calculation: RequestChecksumCalculation::WhenRequired,
            parallel_upload_semaphore: Arc::new(Semaphore::new(1)),
            accelerate: false,
            request_payer: None,
        };

        let client = client_config.create_client().await;

        let retry_config = client.config().retry_config().unwrap();
        assert_eq!(retry_config.max_attempts(), 10);
        assert_eq!(
            retry_config.initial_backoff(),
            std::time::Duration::from_millis(100)
        );

        assert_eq!(
            client.config().region().unwrap().to_string(),
            "us-west-1".to_string()
        );
    }

    // In cloud environment, this test may fail because of the lack of credentials.
    #[cfg(feature = "e2e_test")]
    #[tokio::test]
    async fn create_client_from_env() {
        init_dummy_tracing_subscriber();

        let client_config = ClientConfig {
            client_config_location: ClientConfigLocation {
                aws_config_file: Some("./test_data/test_config/config".into()),
                aws_shared_credentials_file: Some("./test_data/test_config/credentials".into()),
            },
            credential: crate::types::S3Credentials::FromEnvironment,
            region: None,
            endpoint_url: Some("https://my.endpoint.local".to_string()),
            force_path_style: false,
            retry_config: crate::config::RetryConfig {
                aws_max_attempts: 10,
                initial_backoff_milliseconds: 100,
            },
            cli_timeout_config: crate::config::CLITimeoutConfig {
                operation_timeout_milliseconds: None,
                operation_attempt_timeout_milliseconds: None,
                connect_timeout_milliseconds: None,
                read_timeout_milliseconds: None,
            },
            disable_stalled_stream_protection: false,
            request_checksum_calculation: RequestChecksumCalculation::WhenRequired,
            parallel_upload_semaphore: Arc::new(Semaphore::new(1)),
            accelerate: false,
            request_payer: None,
        };

        let _ = client_config.create_client().await;
    }

    #[tokio::test]
    async fn create_client_from_custom_profile_overriding_region() {
        init_dummy_tracing_subscriber();

        let client_config = ClientConfig {
            client_config_location: ClientConfigLocation {
                aws_config_file: Some("./test_data/test_config/config".into()),
                aws_shared_credentials_file: Some("./test_data/test_config/credentials".into()),
            },
            credential: crate::types::S3Credentials::Profile("aws".to_string()),
            region: Some("my-region2".to_string()),
            endpoint_url: Some("https://my.endpoint.local".to_string()),
            force_path_style: false,
            retry_config: crate::config::RetryConfig {
                aws_max_attempts: 10,
                initial_backoff_milliseconds: 100,
            },
            cli_timeout_config: crate::config::CLITimeoutConfig {
                operation_timeout_milliseconds: None,
                operation_attempt_timeout_milliseconds: None,
                connect_timeout_milliseconds: None,
                read_timeout_milliseconds: None,
            },
            disable_stalled_stream_protection: false,
            request_checksum_calculation: RequestChecksumCalculation::WhenRequired,
            parallel_upload_semaphore: Arc::new(Semaphore::new(1)),
            accelerate: false,
            request_payer: None,
        };

        let client = client_config.create_client().await;

        let retry_config = client.config().retry_config().unwrap();
        assert_eq!(retry_config.max_attempts(), 10);
        assert_eq!(
            retry_config.initial_backoff(),
            std::time::Duration::from_millis(100)
        );

        assert_eq!(
            client.config().region().unwrap().to_string(),
            "my-region2".to_string()
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
