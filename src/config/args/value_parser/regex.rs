use fancy_regex::Regex;

const INVALID_REGEX: &str = "invalid regular expression.";

pub fn parse_regex(regex: &str) -> Result<String, String> {
    if Regex::new(regex).is_err() {
        return Err(INVALID_REGEX.to_string());
    }

    Ok(regex.to_string())
}
