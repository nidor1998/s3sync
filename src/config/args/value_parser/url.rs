use url::Url;

const INVALID_SCHEME: &str = "scheme must be https:// or http:// .";

pub fn check_scheme(url: &str) -> Result<String, String> {
    let parsed = Url::parse(url).map_err(|e| e.to_string())?;

    if parsed.scheme() != "https" && parsed.scheme() != "http" {
        return Err(INVALID_SCHEME.to_string());
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

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
