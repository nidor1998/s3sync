pub fn get_filtered_message_by_name(name: &str) -> String {
    match name {
        "MtimeBeforeFilter" => "Filtered by --filter-mtime-before".to_string(),
        "MtimeAfterFilter" => "Filtered by --filter-mtime-after".to_string(),
        "SmallerSizeFilter" => "Filtered by --filter-smaller-size".to_string(),
        "LargerSizeFilter" => "Filtered by --filter-larger-size".to_string(),
        "IncludeRegexFilter" => "Filtered by --filter-include-regex".to_string(),
        "ExcludeRegexFilter" => "Filtered by --filter-exclude-regex".to_string(),
        "TargetModifiedFilter" => {
            "Filtered by last modified time or size or etag(not modified)".to_string()
        }
        _ => {
            panic!("Unknown filter: {}", name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_filtered_message_by_name() {
        assert!(!get_filtered_message_by_name("MtimeBeforeFilter").is_empty());
        assert!(!get_filtered_message_by_name("MtimeAfterFilter").is_empty());
        assert!(!get_filtered_message_by_name("SmallerSizeFilter").is_empty());
        assert!(!get_filtered_message_by_name("LargerSizeFilter").is_empty());
        assert!(!get_filtered_message_by_name("IncludeRegexFilter").is_empty());
        assert!(!get_filtered_message_by_name("ExcludeRegexFilter").is_empty());
        assert!(!get_filtered_message_by_name("TargetModifiedFilter").is_empty());
    }

    #[test]
    #[should_panic]
    fn test_get_filtered_message_by_name_unknown_panic() {
        get_filtered_message_by_name("UnknownFilter");
    }
}
