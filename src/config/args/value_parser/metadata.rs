use std::collections::HashMap;

use regex::Regex;

const INVALID_METADATA: &str = "invalid metadata.";

pub fn check_metadata(metadata: &str) -> Result<String, String> {
    let regex = Regex::new(r"(,?([a-zA-Z0-9_\-.]+)=([a-zA-Z0-9_\-.]*))+").unwrap();

    let mat = regex.find(metadata);
    if mat.is_none() {
        return Err(INVALID_METADATA.to_string());
    }

    if mat.unwrap().as_str() != metadata {
        return Err(INVALID_METADATA.to_string());
    }

    Ok(metadata.to_string())
}

pub fn parse_metadata(value: &str) -> anyhow::Result<HashMap<String, String>, String> {
    check_metadata(value)?;

    Ok(value
        .split(',')
        .map(|key_value| key_value.split('=').collect::<Vec<&str>>())
        .map(|key_value| (key_value[0].to_string(), key_value[1].to_string()))
        .collect::<HashMap<String, String>>())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_metadata_test() {
        init_dummy_tracing_subscriber();

        check_metadata("key=value").unwrap();
        check_metadata("key=Va-lu_e.1").unwrap();
        check_metadata("key-1=value").unwrap();
        check_metadata("key_1=value").unwrap();
        check_metadata("key.1=value").unwrap();
        check_metadata("key1=value1,key2=value2").unwrap();
        check_metadata("key1=value1,key2=value2,key3=value3").unwrap();

        assert!(check_metadata("key").is_err());
        assert!(check_metadata("key=Va-lu_e.%1").is_err());
        assert!(check_metadata("key,key2=value2").is_err());
        assert!(check_metadata("key=value,key2").is_err());
        assert!(check_metadata("key=value,key1=value,").is_err());
        assert!(check_metadata("key=value,key1=value=3,").is_err());
        assert!(check_metadata("key#1=value").is_err());
        assert!(check_metadata("key1=value^").is_err());
    }

    #[test]
    fn parse_valid_metadata() {
        init_dummy_tracing_subscriber();

        assert_eq!(parse_metadata("key1=value1").unwrap(), {
            let mut map = HashMap::new();
            map.insert("key1".to_string(), "value1".to_string());
            map
        });

        assert_eq!(parse_metadata("key1=").unwrap(), {
            let mut map = HashMap::new();
            map.insert("key1".to_string(), "".to_string());
            map
        });

        assert_eq!(parse_metadata("key1=value1,key2=value2").unwrap(), {
            let mut map = HashMap::new();
            map.insert("key1".to_string(), "value1".to_string());
            map.insert("key2".to_string(), "value2".to_string());
            map
        });

        assert_eq!(parse_metadata("key1=,key2=value2").unwrap(), {
            let mut map = HashMap::new();
            map.insert("key1".to_string(), "".to_string());
            map.insert("key2".to_string(), "value2".to_string());
            map
        });

        assert_eq!(parse_metadata("key1=value1,key2=").unwrap(), {
            let mut map = HashMap::new();
            map.insert("key1".to_string(), "value1".to_string());
            map.insert("key2".to_string(), "".to_string());
            map
        });

        assert_eq!(
            parse_metadata("key1=value1,key2=value2,key3=value3").unwrap(),
            {
                let mut map = HashMap::new();
                map.insert("key1".to_string(), "value1".to_string());
                map.insert("key2".to_string(), "value2".to_string());
                map.insert("key3".to_string(), "value3".to_string());
                map
            }
        );
    }

    #[test]
    fn parse_invalid_metadata() {
        init_dummy_tracing_subscriber();

        assert!(parse_metadata("key").is_err());
        assert!(parse_metadata("key=Va-lu_e.%1").is_err());
        assert!(parse_metadata("key,key2=value2").is_err());
        assert!(parse_metadata("key=value,key2").is_err());
        assert!(parse_metadata("key=value,key1=value,").is_err());
        assert!(parse_metadata("key=value,key1=value=3,").is_err());
        assert!(parse_metadata("key#1=value").is_err());
        assert!(parse_metadata("key1=value^").is_err());
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
