//! UTC date-time helpers built on the `time` crate.
//!
//! The string formats produced here intentionally match what `chrono` produced
//! before the migration (`+00:00` offset, `AutoSi` fractional seconds), so log
//! output and the `s3sync_origin_last_modified` metadata value stay unchanged.

use std::fmt::Write;

use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, UtcOffset};

/// Parses an RFC 3339 date-time (any offset) and normalizes it to UTC.
///
/// Returns an error (rather than panicking) if the value is out of range after
/// conversion to UTC.
pub fn parse_rfc3339_utc(value: &str) -> Result<OffsetDateTime, String> {
    let dt = OffsetDateTime::parse(value, &Rfc3339).map_err(|e| e.to_string())?;
    dt.checked_to_offset(UtcOffset::UTC)
        .ok_or_else(|| format!("date-time out of range after conversion to UTC: {value}"))
}

/// RFC 3339 formatting helpers for [`OffsetDateTime`].
///
/// Years outside `0000..=9999` are written with an explicit sign and at least
/// four digits (e.g. `-0001`, `+10000`), matching `chrono`. Such output is an
/// extension of RFC 3339, which only allows four-digit non-negative years.
pub trait Rfc3339Ext {
    /// Formats as RFC 3339 in UTC with `+00:00` and 0, 3, 6, or 9 fractional digits.
    /// e.g. `2023-01-20T17:56:37.552+00:00`
    fn to_rfc3339(&self) -> String;

    /// Formats as RFC 3339 in UTC with whole seconds only.
    /// e.g. `2023-01-20T17:56:37+00:00`
    fn to_rfc3339_secs(&self) -> String;
}

impl Rfc3339Ext for OffsetDateTime {
    fn to_rfc3339(&self) -> String {
        let dt = self.to_offset(UtcOffset::UTC);
        let mut s = format_date_time_secs(&dt);

        let nanos = dt.nanosecond();
        if nanos != 0 {
            if nanos.is_multiple_of(1_000_000) {
                let _ = write!(s, ".{:03}", nanos / 1_000_000);
            } else if nanos.is_multiple_of(1_000) {
                let _ = write!(s, ".{:06}", nanos / 1_000);
            } else {
                let _ = write!(s, ".{nanos:09}");
            }
        }

        s.push_str("+00:00");
        s
    }

    fn to_rfc3339_secs(&self) -> String {
        let dt = self.to_offset(UtcOffset::UTC);
        let mut s = format_date_time_secs(&dt);
        s.push_str("+00:00");
        s
    }
}

fn format_date_time_secs(dt: &OffsetDateTime) -> String {
    let year = dt.year();
    let mut s = String::with_capacity(35);
    if (0..=9999).contains(&year) {
        let _ = write!(s, "{year:04}");
    } else {
        let _ = write!(s, "{year:+05}");
    }
    let _ = write!(
        s,
        "-{:02}-{:02}T{:02}:{:02}:{:02}",
        u8::from(dt.month()),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    );
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_utc() {
        let dt = parse_rfc3339_utc("2023-01-20T17:56:37.552Z").unwrap();
        assert_eq!(dt.offset(), UtcOffset::UTC);
        assert_eq!(dt.to_rfc3339(), "2023-01-20T17:56:37.552+00:00");
    }

    #[test]
    fn parse_with_offset_normalizes_to_utc() {
        let dt = parse_rfc3339_utc("2023-04-20T21:40:11.111+09:00").unwrap();
        assert_eq!(dt.offset(), UtcOffset::UTC);
        assert_eq!(dt.to_rfc3339(), "2023-04-20T12:40:11.111+00:00");
    }

    #[test]
    fn parse_space_separator() {
        let dt = parse_rfc3339_utc("2023-02-19 12:00:00Z").unwrap();
        assert_eq!(dt.to_rfc3339(), "2023-02-19T12:00:00+00:00");
    }

    #[test]
    fn parse_invalid() {
        assert!(parse_rfc3339_utc("2023-02-19").is_err());
        assert!(parse_rfc3339_utc("not a date").is_err());
        assert!(parse_rfc3339_utc("2023-02-19T12:00:00").is_err());
    }

    #[test]
    fn to_rfc3339_fraction_digits() {
        let base = OffsetDateTime::from_unix_timestamp(0).unwrap();
        assert_eq!(base.to_rfc3339(), "1970-01-01T00:00:00+00:00");
        assert_eq!(
            base.replace_millisecond(5).unwrap().to_rfc3339(),
            "1970-01-01T00:00:00.005+00:00"
        );
        assert_eq!(
            base.replace_microsecond(5).unwrap().to_rfc3339(),
            "1970-01-01T00:00:00.000005+00:00"
        );
        assert_eq!(
            base.replace_nanosecond(5).unwrap().to_rfc3339(),
            "1970-01-01T00:00:00.000000005+00:00"
        );
    }

    #[test]
    fn to_rfc3339_secs_truncates() {
        let dt = OffsetDateTime::from_unix_timestamp(0)
            .unwrap()
            .replace_millisecond(999)
            .unwrap();
        assert_eq!(dt.to_rfc3339_secs(), "1970-01-01T00:00:00+00:00");
    }

    #[test]
    fn negative_year() {
        let dt = parse_rfc3339_utc("0000-01-01T00:00:00Z").unwrap() - time::Duration::days(1);
        assert_eq!(dt.to_rfc3339(), "-0001-12-31T00:00:00+00:00");
    }

    #[test]
    fn large_year_has_plus_sign() {
        let dt = parse_rfc3339_utc("9999-12-31T23:59:59Z").unwrap() + time::Duration::seconds(1);
        assert_eq!(dt.year(), 10000);
        assert_eq!(dt.to_rfc3339(), "+10000-01-01T00:00:00+00:00");
        assert_eq!(dt.to_rfc3339_secs(), "+10000-01-01T00:00:00+00:00");
    }

    #[test]
    fn parse_max_date_with_negative_offset_does_not_panic() {
        let dt = parse_rfc3339_utc("9999-12-31T23:59:59-01:00").unwrap();
        assert_eq!(dt.to_rfc3339(), "+10000-01-01T00:59:59+00:00");
    }

    #[test]
    fn aws_date_time_beyond_year_9999_converts() {
        use aws_smithy_types::DateTime;
        use aws_smithy_types_convert::date_time::DateTimeExt;

        // 10000-01-01T00:00:00Z: rejected by `time` without `large-dates`.
        let dt = DateTime::from_secs(253_402_300_800).to_time().unwrap();
        assert_eq!(dt.to_rfc3339(), "+10000-01-01T00:00:00+00:00");
    }
}
