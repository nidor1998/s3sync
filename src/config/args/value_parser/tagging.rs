use regex::Regex;

const INVALID_TAGGING: &str = "invalid tagging.";

pub fn parse_tagging(tagging: &str) -> Result<String, String> {
    let regex = Regex::new(r"(&?([a-zA-Z0-9+%._\-~]+)=([a-zA-Z0-9+%._\-~]*))+").unwrap();

    let mat = regex.find(tagging);
    if mat.is_none() {
        return Err(INVALID_TAGGING.to_string());
    }

    if mat.unwrap().as_str() != tagging {
        return Err(INVALID_TAGGING.to_string());
    }

    Ok(tagging.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tagging_test() {
        init_dummy_tracing_subscriber();

        parse_tagging("key=value").unwrap();
        parse_tagging("%E3%82%AD%E3%83%BC=%E3%83%90%E3%83%AA%E3%83%A5%E3%83%BC").unwrap();
        parse_tagging("key.-_1=value.-_1+ok").unwrap();
        parse_tagging("key1=value1&key2=value2").unwrap();
        parse_tagging("key1=value1&key2=value2&key3=value3").unwrap();

        assert!(parse_tagging("key").is_err());
        assert!(parse_tagging("key&key2=value2").is_err());
        assert!(parse_tagging("key=value&key2").is_err());
        assert!(parse_tagging("key=value&key1=value&").is_err());
        assert!(parse_tagging("key=value&key1=value=3&").is_err());
        assert!(parse_tagging("key#1=value").is_err());
        assert!(parse_tagging("key1=value^").is_err());
        assert!(parse_tagging("key=value,key2=value2").is_err());
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
