//! UTC date-time helpers built on the `time` crate.
//!
//! The string formats produced here intentionally match what `chrono` produced
//! before the migration (`+00:00` offset, `AutoSi` fractional seconds), so log
//! output and the `s3sync_origin_last_modified` metadata value stay unchanged.

use std::fmt::Write;

use aws_smithy_types::DateTime;
use aws_smithy_types_convert::date_time::DateTimeExt;
use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, UtcOffset};

/// Parses an RFC 3339 date-time (any offset) and normalizes it to UTC.
///
/// The date/time separator must be `T`, `t`, or a space (as RFC 3339 allows);
/// `time` itself accepts any byte there.
///
/// Returns an error (rather than panicking) if the value is out of range after
/// conversion to UTC.
pub fn parse_rfc3339_utc(value: &str) -> Result<OffsetDateTime, String> {
    if !matches!(value.as_bytes().get(10), Some(b'T' | b't' | b' ')) {
        return Err(format!(
            "invalid RFC 3339 date-time (expected 'T' between date and time): {value}"
        ));
    }
    let dt = OffsetDateTime::parse(value, &Rfc3339).map_err(|e| e.to_string())?;
    dt.checked_to_offset(UtcOffset::UTC)
        .ok_or_else(|| format!("date-time out of range after conversion to UTC: {value}"))
}

/// Parses an RFC 3339 date-time for the `Expires` header and normalizes it to UTC.
///
/// The value is sent as an HTTP date, which only supports years `0001..=9999`,
/// so anything outside that range (after conversion to UTC) is rejected here
/// instead of failing every upload later.
pub fn parse_expires(value: &str) -> Result<OffsetDateTime, String> {
    let dt = parse_rfc3339_utc(value)?;
    if !(1..=9999).contains(&dt.year()) {
        return Err(format!(
            "expires must be between 0001-01-01T00:00:00Z and 9999-12-31T23:59:59Z: {value}"
        ));
    }
    Ok(dt)
}

/// Converts an AWS [`DateTime`] to an [`OffsetDateTime`] truncated to
/// millisecond precision.
///
/// The truncation is intentional: last-modified values are compared and
/// formatted at millisecond precision throughout s3sync.
pub fn to_time_millis(dt: &DateTime) -> anyhow::Result<OffsetDateTime> {
    Ok(DateTime::from_millis(dt.to_millis()?).to_time()?)
}

/// Formats an AWS [`DateTime`] as RFC 3339 in UTC at millisecond precision.
/// See [`to_time_millis`] and [`Rfc3339Ext::to_rfc3339`].
pub fn to_rfc3339_millis(dt: &DateTime) -> anyhow::Result<String> {
    Ok(to_time_millis(dt)?.to_rfc3339())
}

/// Like [`to_rfc3339_millis`], but never fails: a value that cannot be
/// represented as an [`OffsetDateTime`] is formatted as seconds since the Unix
/// epoch instead. Intended for log output.
pub fn to_rfc3339_millis_lossy(dt: &DateTime) -> String {
    to_rfc3339_millis(dt).unwrap_or_else(|_| format!("{}s since Unix epoch", dt.secs()))
}

/// Returns the instant as nanoseconds since the Unix epoch, truncated to
/// millisecond precision (the same truncation as [`to_time_millis`]).
///
/// Unlike [`to_time_millis`], this never fails, so it can be used for
/// comparisons on any [`DateTime`].
pub fn unix_nanos_millis(dt: &DateTime) -> i128 {
    let millis = i128::from(dt.secs()) * 1_000 + i128::from(dt.subsec_nanos() / 1_000_000);
    millis * 1_000_000
}

/// RFC 3339 formatting helpers for [`OffsetDateTime`].
///
/// Years outside `0000..=9999` are written with an explicit sign and at least
/// four digits (e.g. `-0001`, `+10000`), matching `chrono`. Such output is an
/// extension of RFC 3339, which only allows four-digit non-negative years.
///
/// Values are converted to UTC. If that conversion would go out of range, the
/// value is formatted in its own offset instead of panicking.
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
        let dt = utc_or_self(self);
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

        push_offset(&mut s, dt.offset());
        s
    }

    fn to_rfc3339_secs(&self) -> String {
        let dt = utc_or_self(self);
        let mut s = format_date_time_secs(&dt);
        push_offset(&mut s, dt.offset());
        s
    }
}

fn utc_or_self(dt: &OffsetDateTime) -> OffsetDateTime {
    dt.checked_to_offset(UtcOffset::UTC).unwrap_or(*dt)
}

fn push_offset(s: &mut String, offset: UtcOffset) {
    let (hours, minutes, _) = offset.as_hms();
    let sign = if offset.is_negative() { '-' } else { '+' };
    let _ = write!(
        s,
        "{sign}{:02}:{:02}",
        hours.unsigned_abs(),
        minutes.unsigned_abs()
    );
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
    fn to_rfc3339_out_of_range_utc_does_not_panic() {
        let max =
            time::PrimitiveDateTime::MAX.assume_offset(UtcOffset::from_hms(-1, -30, 0).unwrap());
        assert!(max.checked_to_offset(UtcOffset::UTC).is_none());
        assert_eq!(max.to_rfc3339(), "+999999-12-31T23:59:59.999999999-01:30");
        assert_eq!(max.to_rfc3339_secs(), "+999999-12-31T23:59:59-01:30");
    }

    #[test]
    fn parse_expires_range() {
        assert_eq!(
            parse_expires("9999-12-31T23:59:59Z").unwrap().to_rfc3339(),
            "9999-12-31T23:59:59+00:00"
        );
        assert_eq!(
            parse_expires("0001-01-01T00:00:00Z").unwrap().to_rfc3339(),
            "0001-01-01T00:00:00+00:00"
        );
        assert!(parse_expires("9999-12-31T23:59:59-01:00").is_err());
        assert!(parse_expires("0000-12-31T23:59:59Z").is_err());
        assert!(parse_expires("0001-01-01T00:00:00+01:00").is_err());
        assert!(parse_expires("not a date").is_err());
    }

    #[test]
    fn to_time_millis_truncates_sub_millis() {
        let dt = DateTime::from_secs_and_nanos(1_674_237_397, 552_123_456);
        let t = to_time_millis(&dt).unwrap();
        assert_eq!(t.nanosecond(), 552_000_000);
        assert_eq!(t.to_rfc3339(), "2023-01-20T17:56:37.552+00:00");
        assert_eq!(
            to_rfc3339_millis(&dt).unwrap(),
            "2023-01-20T17:56:37.552+00:00"
        );
    }

    #[test]
    fn aws_date_time_beyond_year_9999_converts() {
        use aws_smithy_types::DateTime;
        use aws_smithy_types_convert::date_time::DateTimeExt;

        // 10000-01-01T00:00:00Z: rejected by `time` without `large-dates`.
        let dt = DateTime::from_secs(253_402_300_800).to_time().unwrap();
        assert_eq!(dt.to_rfc3339(), "+10000-01-01T00:00:00+00:00");
    }

    #[test]
    fn parse_separator() {
        assert!(parse_rfc3339_utc("2023-02-19t12:00:00Z").is_ok());
        assert!(parse_rfc3339_utc("2023-02-19X12:00:00Z").is_err());
        assert!(parse_rfc3339_utc("2023-02-19_12:00:00Z").is_err());
        assert!(parse_rfc3339_utc("").is_err());
    }

    #[test]
    fn parse_leap_second() {
        // `time` accepts a leap second only at the end of a UTC month and
        // clamps it to the last representable instant of that minute.
        assert_eq!(
            parse_rfc3339_utc("2016-12-31T23:59:60Z")
                .unwrap()
                .to_rfc3339(),
            "2016-12-31T23:59:59.999999999+00:00"
        );
        assert!(parse_rfc3339_utc("2016-12-31T12:00:60Z").is_err());
    }

    #[test]
    fn to_rfc3339_millis_lossy_formats() {
        let dt = DateTime::from_secs_and_nanos(1_674_237_397, 552_123_456);
        assert_eq!(
            to_rfc3339_millis_lossy(&dt),
            "2023-01-20T17:56:37.552+00:00"
        );

        let out_of_range = DateTime::from_secs(40_000_000_000_000);
        assert!(to_time_millis(&out_of_range).is_err());
        assert_eq!(
            to_rfc3339_millis_lossy(&out_of_range),
            "40000000000000s since Unix epoch"
        );
    }

    #[test]
    fn unix_nanos_millis_matches_to_time_millis() {
        for dt in [
            DateTime::from_secs_and_nanos(1_674_237_397, 552_123_456),
            DateTime::from_secs_and_nanos(-1, 999_999_999),
            DateTime::from_secs_and_nanos(-62_135_596_800, 1_500_000),
            DateTime::from_secs(0),
        ] {
            assert_eq!(
                unix_nanos_millis(&dt),
                to_time_millis(&dt).unwrap().unix_timestamp_nanos()
            );
        }

        // Out of range for `time`, but still comparable.
        assert!(
            unix_nanos_millis(&DateTime::from_secs(40_000_000_000_000))
                > unix_nanos_millis(&DateTime::from_secs(0))
        );
    }
}
