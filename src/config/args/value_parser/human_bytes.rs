use byte_unit::Byte;
use std::str::FromStr;

const UNDER_MIN_VALUE: &str = "must be greater than or equal to 5MiB";
const OVER_MAX_VALUE: &str = "must be smaller than or equal to 5GiB";

const MIN_VALUE: u128 = 5 * 1024 * 1024;
const MAX_VALUE: u128 = 5 * 1024 * 1024 * 1024;

const UNDER_MIN_BANDWIDTH: &str = "must be greater than or equal to 1MiB";
const OVER_MAX_BANDWIDTH: &str = "must be smaller than or equal to 100GiB";

const MIN_BANDWIDTH: u128 = 1024 * 1024;
const MAX_BANDWIDTH: u128 = 100 * 1024 * 1024 * 1024;

pub fn check_human_bytes(value: &str) -> Result<String, String> {
    let result = Byte::from_str(value).map_err(|e| e.to_string())?;

    if result.as_u128() < MIN_VALUE {
        return Err(UNDER_MIN_VALUE.to_string());
    }
    if result.as_u128() > MAX_VALUE {
        return Err(OVER_MAX_VALUE.to_string());
    }

    Ok(value.to_string())
}

pub fn parse_human_bytes(value: &str) -> Result<u64, String> {
    check_human_bytes(value)?;

    let result = Byte::from_str(value).map_err(|e| e.to_string())?;
    Ok(result.as_u128().try_into().unwrap())
}

pub fn check_human_bytes_without_limit(value: &str) -> Result<String, String> {
    let result = Byte::from_str(value).map_err(|e| e.to_string())?;
    TryInto::<u64>::try_into(result.as_u128()).unwrap();

    Ok(value.to_string())
}

pub fn parse_human_bytes_without_limit(value: &str) -> Result<u64, String> {
    check_human_bytes_without_limit(value)?;

    let result = Byte::from_str(value).map_err(|e| e.to_string())?;
    Ok(result.as_u128().try_into().unwrap())
}

pub fn check_human_bandwidth(value: &str) -> Result<String, String> {
    let result = Byte::from_str(value).map_err(|e| e.to_string())?;

    if result.as_u128() < MIN_BANDWIDTH {
        return Err(UNDER_MIN_BANDWIDTH.to_string());
    }
    if result.as_u128() > MAX_BANDWIDTH {
        return Err(OVER_MAX_BANDWIDTH.to_string());
    }

    Ok(value.to_string())
}

pub fn parse_human_bandwidth(value: &str) -> Result<u64, String> {
    check_human_bandwidth(value)?;

    let result = Byte::from_str(value).map_err(|e| e.to_string())?;
    Ok(result.as_u128().try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_valid_value() {
        init_dummy_tracing_subscriber();

        check_human_bytes("5MiB").unwrap();
        check_human_bytes("5242880").unwrap();
        check_human_bytes("5GiB").unwrap();
        check_human_bytes("5368709120").unwrap();
        check_human_bytes("8MiB").unwrap();
    }

    #[test]
    fn check_valid_value_without_limit() {
        init_dummy_tracing_subscriber();

        check_human_bytes_without_limit("5MiB").unwrap();
        check_human_bytes_without_limit("5242880").unwrap();
        check_human_bytes_without_limit("5GiB").unwrap();
        check_human_bytes_without_limit("5368709120").unwrap();
        check_human_bytes_without_limit("8MiB").unwrap();
        check_human_bytes_without_limit("10GiB").unwrap();
        check_human_bytes_without_limit("1TiB").unwrap();
    }

    #[test]
    fn check_valid_bandwidth() {
        init_dummy_tracing_subscriber();

        check_human_bandwidth("500MiB").unwrap();
        check_human_bandwidth("5242880").unwrap();
        check_human_bandwidth("1GiB").unwrap();
        check_human_bandwidth("5368709120").unwrap();
        check_human_bandwidth("10GiB").unwrap();
    }

    #[test]
    fn check_invalid_value() {
        init_dummy_tracing_subscriber();

        let result = check_human_bytes("524287a");
        assert!(result.is_err());

        let result = check_human_bytes("5Zib");
        assert!(result.is_err());
    }

    #[test]
    fn check_invalid_value_without_limit() {
        init_dummy_tracing_subscriber();

        let result = check_human_bytes_without_limit("524287a");
        assert!(result.is_err());

        let result = check_human_bytes_without_limit("5Zib");
        assert!(result.is_err());
    }

    #[test]
    fn check_invalid_bandwidth() {
        init_dummy_tracing_subscriber();

        let result = check_human_bandwidth("524287a");
        assert!(result.is_err());

        let result = check_human_bandwidth("5Zib");
        assert!(result.is_err());
    }

    #[test]
    fn check_under_min_value() {
        init_dummy_tracing_subscriber();

        let result = check_human_bytes("5242879");
        assert!(result.is_err());

        if let Err(e) = result {
            assert_eq!(e, UNDER_MIN_VALUE);
        }
    }

    #[test]
    fn check_over_max_value() {
        init_dummy_tracing_subscriber();

        let result = check_human_bytes("5368709121");
        assert!(result.is_err());

        if let Err(e) = result {
            assert_eq!(e, OVER_MAX_VALUE);
        }

        let result = check_human_bytes("5Eib");
        assert!(result.is_err());

        if let Err(e) = result {
            assert_eq!(e, OVER_MAX_VALUE);
        }
    }

    #[test]
    fn check_under_min_bandwidth() {
        init_dummy_tracing_subscriber();

        let result = check_human_bandwidth("1048575");
        assert!(result.is_err());

        if let Err(e) = result {
            assert_eq!(e, UNDER_MIN_BANDWIDTH);
        }
    }

    #[test]
    fn check_over_max_bandwidth() {
        init_dummy_tracing_subscriber();

        let result = check_human_bandwidth("107374182401");
        assert!(result.is_err());

        if let Err(e) = result {
            assert_eq!(e, OVER_MAX_BANDWIDTH);
        }

        let result = check_human_bandwidth("5Eib");
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e, OVER_MAX_BANDWIDTH);
        }
    }

    #[test]
    fn parse_valid_value() {
        init_dummy_tracing_subscriber();

        assert_eq!(8 * 1024 * 1024, parse_human_bytes("8MiB").unwrap());
        assert_eq!(5 * 1024 * 1024, parse_human_bytes("5242880").unwrap());
    }

    #[test]
    fn parse_valid_value_without_limit() {
        init_dummy_tracing_subscriber();

        assert_eq!(
            8 * 1024 * 1024,
            parse_human_bytes_without_limit("8MiB").unwrap()
        );
        assert_eq!(
            5 * 1024 * 1024,
            parse_human_bytes_without_limit("5242880").unwrap()
        );
        assert_eq!(
            10 * 1024 * 1024 * 1024,
            parse_human_bytes_without_limit("10GiB").unwrap()
        );
    }

    #[test]
    fn parse_invalid_value() {
        init_dummy_tracing_subscriber();

        let result = parse_human_bytes("524287a");
        assert!(result.is_err());

        let result = parse_human_bytes("5Zib");
        assert!(result.is_err());
    }

    #[test]
    fn parse_invalid_value_without_limit() {
        init_dummy_tracing_subscriber();

        let result = parse_human_bytes_without_limit("524287a");
        assert!(result.is_err());

        let result = parse_human_bytes_without_limit("5Zib");
        assert!(result.is_err());
    }

    #[test]
    fn parse_under_min_value() {
        init_dummy_tracing_subscriber();

        let result = parse_human_bytes("5242879");
        assert!(result.is_err());

        if let Err(e) = result {
            assert_eq!(e, UNDER_MIN_VALUE);
        }
    }

    #[test]
    fn parse_over_max_value() {
        init_dummy_tracing_subscriber();

        let result = parse_human_bytes("5368709121");
        assert!(result.is_err());

        if let Err(e) = result {
            assert_eq!(e, OVER_MAX_VALUE);
        }
    }

    #[test]
    fn parse_valid_bandwidth() {
        init_dummy_tracing_subscriber();

        assert_eq!(100 * 1024 * 1024, parse_human_bandwidth("100MiB").unwrap());
        assert_eq!(
            10 * 1024 * 1024 * 1024,
            parse_human_bandwidth("10GiB").unwrap()
        );
    }

    #[test]
    fn parse_invalid_bandwidth() {
        init_dummy_tracing_subscriber();

        let result = parse_human_bandwidth("524287a");
        assert!(result.is_err());
    }

    #[test]
    fn parse_under_min_bandwidth() {
        init_dummy_tracing_subscriber();

        let result = parse_human_bandwidth("1048575");
        assert!(result.is_err());

        if let Err(e) = result {
            assert_eq!(e, UNDER_MIN_BANDWIDTH);
        }
    }

    #[test]
    fn parse_over_max_bandwidth() {
        init_dummy_tracing_subscriber();

        let result = parse_human_bandwidth("107374182401");
        assert!(result.is_err());

        if let Err(e) = result {
            assert_eq!(e, OVER_MAX_BANDWIDTH);
        }
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
