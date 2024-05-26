use url::Url;

const INVALID_SCHEME: &str = "scheme must be https:// or http:// .";
const HTTP_PROXY_AUTH_NOT_SUPPORTED: &str = "HTTP proxy with authentication is not supported.";

pub fn check_scheme(url: &str) -> Result<String, String> {
    let parsed = Url::parse(url).map_err(|e| e.to_string())?;

    if parsed.scheme() != "https" && parsed.scheme() != "http" {
        return Err(INVALID_SCHEME.to_string());
    }

    Ok(url.to_string())
}

pub fn check_scheme_and_no_authority_exist(url: &str) -> Result<String, String> {
    check_scheme(url)?;

    let parsed = Url::parse(url).map_err(|e| e.to_string())?;
    if parsed.authority().split_once('@').is_some() {
        return Err(HTTP_PROXY_AUTH_NOT_SUPPORTED.to_string());
    }

    Ok(url.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_url() {
        init_dummy_tracing_subscriber();

        check_scheme("https://endpoint_url.local").unwrap();
        check_scheme("https://endpoint_url.local/").unwrap();
        check_scheme("https://endpoint_url.local/bucket").unwrap();
        check_scheme("https://endpoint_url.local/bucket/").unwrap();

        check_scheme("http://endpoint_url.local").unwrap();
        check_scheme("http://endpoint_url.local/").unwrap();
        check_scheme("http://endpoint_url.local/bucket").unwrap();
        check_scheme("http://endpoint_url.local/bucket/").unwrap();
    }

    #[test]
    fn no_authority_check() {
        init_dummy_tracing_subscriber();

        check_scheme_and_no_authority_exist("https://endpoint_url.local").unwrap();
        check_scheme_and_no_authority_exist("https://endpoint_url.local/").unwrap();
        check_scheme_and_no_authority_exist("https://endpoint_url.local/bucket").unwrap();
        check_scheme_and_no_authority_exist("https://endpoint_url.local/bucket/").unwrap();

        assert_eq!(
            check_scheme_and_no_authority_exist("http://user:pass@endpoint_url.local").unwrap_err(),
            HTTP_PROXY_AUTH_NOT_SUPPORTED
        );

        check_scheme_and_no_authority_exist("http://endpoint_url.local").unwrap();
        check_scheme_and_no_authority_exist("http://endpoint_url.local/").unwrap();
        check_scheme_and_no_authority_exist("http://endpoint_url.local/bucket").unwrap();
        check_scheme_and_no_authority_exist("http://endpoint_url.local/bucket/").unwrap();

        assert_eq!(
            check_scheme_and_no_authority_exist("https://user:pass@endpoint_url.local")
                .unwrap_err(),
            HTTP_PROXY_AUTH_NOT_SUPPORTED
        );
    }

    #[test]
    fn invalid_scheme() {
        init_dummy_tracing_subscriber();

        assert_eq!(check_scheme("ftp://my-bucket").unwrap_err(), INVALID_SCHEME);
        assert_eq!(
            check_scheme_and_no_authority_exist("ftp://my-bucket").unwrap_err(),
            INVALID_SCHEME
        );
    }

    #[test]
    fn empty_host() {
        init_dummy_tracing_subscriber();

        assert_eq!(check_scheme("https://").unwrap_err(), "empty host");
        assert_eq!(
            check_scheme_and_no_authority_exist("https://").unwrap_err(),
            "empty host"
        );
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
