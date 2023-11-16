use std::sync::Arc;

use aws_config::meta::region::{ProvideRegion, RegionProviderChain};
use aws_config::profile::profile_file::{ProfileFileKind, ProfileFiles};
use aws_config::retry::RetryConfig;
use aws_config::ConfigLoader;
use aws_sdk_s3::config::{Builder, SharedHttpClient};
use aws_sdk_s3::Client;
use aws_types::region::Region;
use aws_types::SdkConfig;
use hyper::client::HttpConnector;
use hyper_proxy::{Intercept, Proxy, ProxyConnector};
use hyper_rustls::HttpsConnector;
use rustls::client::ServerCertVerified;
use rustls::client::ServerCertVerifier;
use rustls::ServerName;

use aws_smithy_runtime::client::http::hyper_014::HyperClientBuilder;

use crate::config::ClientConfig;

pub struct NoCertificateVerification {}

impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }
}

impl ClientConfig {
    pub async fn create_client(&self) -> Client {
        let config_builder =
            Builder::from(&self.load_sdk_config().await).force_path_style(self.force_path_style);

        if self.https_proxy.is_some() || self.http_proxy.is_some() {
            return Client::from_conf(config_builder.http_client(self.create_proxy()).build());
        } else if self.no_verify_ssl {
            return Client::from_conf(
                config_builder
                    .http_client(create_no_verify_ssl_connector())
                    .build(),
            );
        }

        Client::from_conf(config_builder.build())
    }

    fn create_proxy(&self) -> SharedHttpClient {
        let connector = HttpConnector::new();
        let mut proxy_connector = ProxyConnector::new(connector).unwrap();

        if self.https_proxy.is_some() {
            if let Ok(uri) = self.https_proxy.as_ref().unwrap().to_string().parse() {
                proxy_connector.add_proxy(Proxy::new(Intercept::Https, uri));
            }
        }
        if self.http_proxy.is_some() {
            if let Ok(uri) = self.http_proxy.as_ref().unwrap().to_string().parse() {
                proxy_connector.add_proxy(Proxy::new(Intercept::Http, uri));
            }
        }

        HyperClientBuilder::new().build(proxy_connector)
    }

    async fn load_sdk_config(&self) -> SdkConfig {
        let mut config_loader = self
            .load_config_credential(aws_config::from_env())
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
                    let profile_files = ProfileFiles::builder()
                        .with_file(ProfileFileKind::Credentials, aws_shared_credentials_file)
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
                let profile_files = ProfileFiles::builder()
                    .with_file(ProfileFileKind::Config, aws_config_file)
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
}

fn create_no_verify_ssl_http_connector() -> HttpsConnector<HttpConnector> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store.add_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.iter().map(|ta| {
        rustls::OwnedTrustAnchor::from_subject_spki_name_constraints(
            ta.subject,
            ta.spki,
            ta.name_constraints,
        )
    }));

    let mut tls_config = rustls::client::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    tls_config
        .dangerous()
        .set_certificate_verifier(Arc::new(NoCertificateVerification {}));

    hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls_config)
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build()
}

fn create_no_verify_ssl_connector() -> SharedHttpClient {
    HyperClientBuilder::new().build(create_no_verify_ssl_http_connector())
}

#[cfg(test)]
mod tests {
    use crate::types::{AccessKeys, ClientConfigLocation};
    use aws_sdk_s3::config::ProvideCredentials;

    use super::*;

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
            https_proxy: None,
            http_proxy: None,
            no_verify_ssl: false,
        };

        let client = client_config.create_client().await;

        let credentials = client
            .config()
            .credentials_provider()
            .unwrap()
            .provide_credentials()
            .await
            .unwrap();
        assert_eq!(credentials.access_key_id(), "my_access_key");
        assert_eq!(credentials.secret_access_key(), "my_secret_access_key");
        assert_eq!(credentials.session_token().unwrap(), "my_session_token");

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
            https_proxy: None,
            http_proxy: None,
            no_verify_ssl: false,
        };

        let client = client_config.create_client().await;

        let credentials = client
            .config()
            .credentials_provider()
            .unwrap()
            .provide_credentials()
            .await
            .unwrap();

        assert_eq!(credentials.access_key_id(), "my_access_key");
        assert_eq!(credentials.secret_access_key(), "my_secret_access_key");
        assert_eq!(credentials.session_token().unwrap(), "my_session_token");

        let retry_config = client.config().retry_config().unwrap();
        assert_eq!(retry_config.max_attempts(), 10);
        assert_eq!(
            retry_config.initial_backoff(),
            std::time::Duration::from_millis(100)
        );
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
            https_proxy: None,
            http_proxy: None,
            no_verify_ssl: false,
        };

        let client = client_config.create_client().await;

        let credentials = client
            .config()
            .credentials_provider()
            .unwrap()
            .provide_credentials()
            .await
            .unwrap();

        assert_eq!(credentials.access_key_id(), "my_aws_profile_access_key");
        assert_eq!(
            credentials.secret_access_key(),
            "my_aws_profile_secret_access_key"
        );
        assert_eq!(
            credentials.session_token().unwrap(),
            "my_aws_profile_session_token"
        );

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
            https_proxy: None,
            http_proxy: None,
            no_verify_ssl: false,
        };

        let client = client_config.create_client().await;

        let credentials = client
            .config()
            .credentials_provider()
            .unwrap()
            .provide_credentials()
            .await
            .unwrap();

        assert_eq!(credentials.access_key_id(), "my_default_profile_access_key");
        assert_eq!(
            credentials.secret_access_key(),
            "my_default_profile_secret_access_key"
        );
        assert_eq!(
            credentials.session_token().unwrap(),
            "my_default_profile_session_token"
        );

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
            https_proxy: None,
            http_proxy: None,
            no_verify_ssl: false,
        };

        let client = client_config.create_client().await;

        let _ = client
            .config()
            .identity_cache()
            .as_ref()
            .unwrap()
            .as_ref()
            .provide_cached_credentials()
            .await
            .unwrap();
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
            https_proxy: None,
            http_proxy: None,
            no_verify_ssl: false,
        };

        let client = client_config.create_client().await;

        let credentials = client
            .config()
            .credentials_provider()
            .unwrap()
            .provide_credentials()
            .await
            .unwrap();

        assert_eq!(credentials.access_key_id(), "my_aws_profile_access_key");
        assert_eq!(
            credentials.secret_access_key(),
            "my_aws_profile_secret_access_key"
        );
        assert_eq!(
            credentials.session_token().unwrap(),
            "my_aws_profile_session_token"
        );

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

    #[tokio::test]
    async fn create_client_with_no_verify_ssl() {
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
            https_proxy: None,
            http_proxy: None,
            no_verify_ssl: true,
        };

        client_config.create_client().await;
    }

    #[tokio::test]
    async fn create_client_with_proxy() {
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
            https_proxy: Some("https://localhost:8080".to_string()),
            http_proxy: Some("http://localhost:8080".to_string()),
            no_verify_ssl: false,
        };

        client_config.create_client().await;
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
