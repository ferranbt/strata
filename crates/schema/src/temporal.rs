//! The temporal types: a UTC-normalized [`Timestamp`] and a calendar [`Date`],
//! both parsed from the many shapes source APIs use.

use serde::{Deserialize, Serialize};

use crate::{DataType, HasDataType};

/// An instant, normalized to UTC. Schema type [`DataType::Timestamp`]; serializes
/// as RFC 3339. Deserialization accepts the layouts [`Timestamp::parse`] lists and
/// errors on anything else.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(pub chrono::DateTime<chrono::Utc>);

const NAIVE_FORMATS: &[&str] = &[
    "%Y-%m-%dT%H:%M:%S%.f",
    "%Y-%m-%d %H:%M:%S%.f",
    "%Y/%m/%d %H:%M:%S",
];

impl Timestamp {
    /// RFC 3339, RFC 2822, [`NAIVE_FORMATS`], or `YYYY-MM-DD`. Offset-less values
    /// are taken as UTC.
    pub fn parse(value: &str) -> Option<Self> {
        let value = value.trim();
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
            return Some(Timestamp(dt.with_timezone(&chrono::Utc)));
        }
        if let Ok(dt) = chrono::DateTime::parse_from_rfc2822(value) {
            return Some(Timestamp(dt.with_timezone(&chrono::Utc)));
        }
        for format in NAIVE_FORMATS {
            if let Some(ts) = Timestamp::parse_with(value, format) {
                return Some(ts);
            }
        }
        Timestamp::parse_with(value, "%Y-%m-%d")
    }

    /// Parse against one `chrono` format, trying the offset / offset-less /
    /// date-only shapes it may produce.
    pub fn parse_with(value: &str, format: &str) -> Option<Self> {
        use chrono::TimeZone;
        let value = value.trim();
        if let Ok(dt) = chrono::DateTime::parse_from_str(value, format) {
            return Some(Timestamp(dt.with_timezone(&chrono::Utc)));
        }
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(value, format) {
            return Some(Timestamp(chrono::Utc.from_utc_datetime(&naive)));
        }
        let date = chrono::NaiveDate::parse_from_str(value, format).ok()?;
        Some(Timestamp(chrono::Utc.from_utc_datetime(
            &date.and_time(chrono::NaiveTime::from_hms_opt(0, 0, 0)?),
        )))
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.to_rfc3339())
    }
}

impl std::str::FromStr for Timestamp {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Timestamp::parse(s).ok_or_else(|| format!("unrecognized timestamp `{s}`"))
    }
}

impl Serialize for Timestamp {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_rfc3339())
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TimestampVisitor;

        impl serde::de::Visitor<'_> for TimestampVisitor {
            type Value = Timestamp;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("an RFC 3339/2822 timestamp string, or epoch seconds")
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Timestamp, E> {
                Timestamp::parse(value)
                    .ok_or_else(|| E::custom(format!("unrecognized timestamp `{value}`")))
            }

            fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Timestamp, E> {
                // Too large to be seconds → treat as milliseconds.
                let (secs, millis) = if value.abs() < 100_000_000_000 {
                    (value, 0)
                } else {
                    (value / 1000, value % 1000)
                };
                chrono::DateTime::from_timestamp(secs, (millis.unsigned_abs() as u32) * 1_000_000)
                    .map(Timestamp)
                    .ok_or_else(|| E::custom(format!("epoch timestamp out of range: {value}")))
            }

            fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Timestamp, E> {
                self.visit_i64(
                    i64::try_from(value)
                        .map_err(|_| E::custom(format!("epoch timestamp out of range: {value}")))?,
                )
            }
        }

        deserializer.deserialize_any(TimestampVisitor)
    }
}

/// Generate a `#[serde(with = "...")]` module decoding a [`Timestamp`] with one
/// explicit `chrono` format. Serialization stays RFC 3339.
///
/// ```ignore
/// new_timestamp_deserialize!(http_date, "%a, %d %b %Y %H:%M:%S GMT");
/// #[serde(with = "http_date")]
/// created_at: Timestamp,
/// ```
#[macro_export]
macro_rules! new_timestamp_deserialize {
    ($name:ident, $format:literal) => {
        #[allow(dead_code)]
        pub mod $name {
            pub const FORMAT: &str = $format;

            pub fn serialize<S: ::serde::Serializer>(
                value: &$crate::Timestamp,
                serializer: S,
            ) -> ::core::result::Result<S::Ok, S::Error> {
                ::serde::Serialize::serialize(value, serializer)
            }

            pub fn deserialize<'de, D: ::serde::Deserializer<'de>>(
                deserializer: D,
            ) -> ::core::result::Result<$crate::Timestamp, D::Error> {
                let raw =
                    <::std::string::String as ::serde::Deserialize>::deserialize(deserializer)?;
                $crate::Timestamp::parse_with(&raw, FORMAT).ok_or_else(|| {
                    <D::Error as ::serde::de::Error>::custom(::std::format!(
                        "expected timestamp in format `{}`, got `{}`",
                        FORMAT,
                        raw
                    ))
                })
            }
        }
    };
}

impl HasDataType for Timestamp {
    fn data_type() -> DataType {
        DataType::Timestamp
    }
}

/// A calendar date. Schema type [`DataType::Date`]; serializes as `YYYY-MM-DD`.
/// Parsing reuses [`Timestamp`]'s layouts, keeping just the date part — so a source
/// that returns a full instant for a date field still decodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date(pub chrono::NaiveDate);

const DATE_FORMAT: &str = "%Y-%m-%d";

impl Date {
    pub fn parse(value: &str) -> Option<Self> {
        let value = value.trim();
        if let Ok(date) = chrono::NaiveDate::parse_from_str(value, DATE_FORMAT) {
            return Some(Date(date));
        }
        Timestamp::parse(value).map(|ts| Date(ts.0.date_naive()))
    }
}

impl std::fmt::Display for Date {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.format(DATE_FORMAT))
    }
}

impl std::str::FromStr for Date {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Date::parse(s).ok_or_else(|| format!("unrecognized date `{s}`"))
    }
}

impl Serialize for Date {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0.format(DATE_FORMAT))
    }
}

impl<'de> Deserialize<'de> for Date {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Date::parse(&raw)
            .ok_or_else(|| serde::de::Error::custom(format!("unrecognized date `{raw}`")))
    }
}

impl HasDataType for Date {
    fn data_type() -> DataType {
        DataType::Date
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> Timestamp {
        Timestamp::parse(s).expect("parses")
    }

    fn on(s: &str) -> Date {
        Date::parse(s).expect("parses")
    }

    #[test]
    fn accepts_the_common_layouts() {
        let expected = at("2024-01-02T03:04:05Z");
        for value in [
            "2024-01-02T03:04:05Z",
            "2024-01-02T03:04:05+00:00",
            "2024-01-02 03:04:05",
            "2024-01-02T03:04:05",
            "2024/01/02 03:04:05",
            "  2024-01-02T03:04:05Z  ",
        ] {
            assert_eq!(at(value), expected, "{value}");
        }
    }

    #[test]
    fn offsets_normalize_to_utc() {
        assert_eq!(at("2024-01-02T05:04:05+02:00"), at("2024-01-02T03:04:05Z"));
        assert_eq!(
            at("Tue, 2 Jan 2024 03:04:05 +0000"),
            at("2024-01-02T03:04:05Z")
        );
    }

    #[test]
    fn naive_is_utc_and_date_only_is_midnight() {
        assert_eq!(at("2024-01-02T03:04:05"), at("2024-01-02T03:04:05Z"));
        assert_eq!(at("2024-01-02"), at("2024-01-02T00:00:00Z"));
    }

    #[test]
    fn rejects_unparseable_values() {
        for value in ["", "not a date", "01/02/2024", "2024-13-99T00:00:00Z"] {
            assert!(Timestamp::parse(value).is_none(), "{value}");
        }
    }

    #[test]
    fn ordering_follows_the_instant_not_the_string() {
        let earlier = at("2024-01-02T05:04:05+02:00");
        let later = at("2024-01-02T04:00:00Z");
        assert!(earlier < later);
    }

    #[test]
    fn serializes_as_rfc3339_and_round_trips() {
        let ts = at("2024-01-02T03:04:05Z");
        let json = serde_json::to_string(&ts).unwrap();
        assert_eq!(json, "\"2024-01-02T03:04:05+00:00\"");
        assert_eq!(serde_json::from_str::<Timestamp>(&json).unwrap(), ts);
    }

    #[test]
    fn deserializes_epoch_numbers() {
        let expected = at("2024-01-02T03:04:05Z");
        let secs = expected.0.timestamp();
        assert_eq!(
            serde_json::from_str::<Timestamp>(&secs.to_string()).unwrap(),
            expected
        );
        assert_eq!(
            serde_json::from_str::<Timestamp>(&(secs * 1000).to_string()).unwrap(),
            expected
        );
    }

    #[test]
    fn strict_decode_errors_on_garbage() {
        assert!(serde_json::from_str::<Timestamp>("\"tomorrow\"").is_err());
    }

    crate::new_timestamp_deserialize!(http_date, "%a, %d %b %Y %H:%M:%S GMT");

    #[test]
    fn generated_module_decodes_its_format() {
        #[derive(Deserialize)]
        struct Entry {
            #[serde(with = "http_date")]
            created_at: Timestamp,
        }

        let entry: Entry =
            serde_json::from_str(r#"{"created_at":"Tue, 02 Jan 2024 03:04:05 GMT"}"#).unwrap();
        assert_eq!(entry.created_at, at("2024-01-02T03:04:05Z"));

        // A value in a different format is rejected, not silently accepted.
        assert!(serde_json::from_str::<Entry>(r#"{"created_at":"2024-01-02T03:04:05Z"}"#).is_err());
    }

    #[test]
    fn parses_plain_dates_and_reuses_timestamp_layouts() {
        let expected = on("2024-01-02");
        for value in [
            "2024-01-02",
            "  2024-01-02 ",
            "2024-01-02T03:04:05Z",
            "2024-01-02 03:04:05",
            "Tue, 2 Jan 2024 03:04:05 +0000",
        ] {
            assert_eq!(on(value), expected, "{value}");
        }
    }

    /// An offset that shifts the instant across midnight yields the UTC date.
    #[test]
    fn offset_resolves_against_utc() {
        assert_eq!(on("2024-01-02T00:30:00+02:00"), on("2024-01-01"));
    }

    #[test]
    fn date_rejects_unparseable_values() {
        for value in ["", "not a date", "01/02/2024"] {
            assert!(Date::parse(value).is_none(), "{value}");
        }
    }

    #[test]
    fn serializes_as_plain_date_and_round_trips() {
        let date = on("2024-01-02");
        let json = serde_json::to_string(&date).unwrap();
        assert_eq!(json, "\"2024-01-02\"");
        assert_eq!(serde_json::from_str::<Date>(&json).unwrap(), date);
    }

    #[test]
    fn ordering_follows_the_calendar() {
        assert!(on("2024-01-02") < on("2024-02-01"));
    }
}
