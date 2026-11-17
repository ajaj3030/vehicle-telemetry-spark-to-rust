use chrono::{DateTime, NaiveDate, NaiveDateTime};
use deltalake::arrow::array::{StringArray, TimestampMicrosecondArray};

/// Parse the telemetry Utf8 event_time as Spark to_timestamp in a UTC session.
/// Invalid values and nulls become null; fractional seconds truncate to micros.
#[allow(dead_code)]
pub fn event_time(values: &StringArray) -> TimestampMicrosecondArray {
    values
        .iter()
        .map(|value| value.and_then(parse_event_time))
        .collect::<TimestampMicrosecondArray>()
        .with_timezone("UTC")
}

fn parse_event_time(value: &str) -> Option<i64> {
    let value = value.trim();
    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Some(dt.timestamp_micros());
    }
    for format in ["%Y-%m-%dT%H:%M%#z", "%Y-%m-%d %H:%M%#z"] {
        if let Ok(dt) = DateTime::parse_from_str(value, format) {
            return Some(dt.timestamp_micros());
        }
    }
    for format in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M",
    ] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(value, format) {
            return Some(dt.and_utc().timestamp_micros());
        }
    }
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .ok()?
        .and_hms_opt(0, 0, 0)
        .map(|dt| dt.and_utc().timestamp_micros())
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::{
        array::Array,
        datatypes::{DataType, TimeUnit},
    };

    #[test]
    fn minute_precision_defaults_seconds_to_zero() {
        let input = StringArray::from(vec![
            Some("2025-01-02 03:04"),
            Some("2025-01-02T03:04"),
            Some("2025-01-02 03:04Z"),
            Some("2025-01-02T03:04Z"),
            Some("2025-01-02 04:04+01:00"),
            Some("2025-01-02T04:04+01:00"),
            Some("2025-01-01 22:04-05:00"),
            Some("2025-01-01T22:04-05:00"),
            Some("2025-01-02T03:60Z"),
            Some("2025-01-02 03:04+25:00"),
            None,
        ]);
        let expected = DateTime::parse_from_rfc3339("2025-01-02T03:04:00Z")
            .unwrap()
            .timestamp_micros();
        let mut values = vec![Some(expected); 8];
        values.extend([None, None, None]);
        assert_eq!(event_time(&input).iter().collect::<Vec<_>>(), values);
    }

    #[test]
    fn utc_offsets_fractional_seconds_and_nulls() {
        let input = StringArray::from(vec![
            Some("1970-01-01T00:00:00Z"),
            Some("1970-01-01T01:00:00.123456789+01:00"),
            Some("1969-12-31T23:59:59.999999999Z"),
            Some("1970-01-01 00:00:01"),
            Some("1970-01-02"),
            Some("not a timestamp"),
            Some(""),
            Some("1700000000000"),
            Some("2025-02-30T00:00:00Z"),
            None,
        ]);
        let result = event_time(&input);
        assert_eq!(
            result.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            result.iter().collect::<Vec<_>>(),
            vec![
                Some(0),
                Some(123456),
                Some(-1),
                Some(1_000_000),
                Some(86_400_000_000),
                None,
                None,
                None,
                None,
                None
            ]
        );
        assert!(event_time(&StringArray::from(Vec::<&str>::new())).is_empty());
    }

    #[tokio::test]
    async fn committed_fixture_event_times_are_strings_and_parse() {
        use deltalake::datafusion::prelude::*;
        let ctx = SessionContext::new();
        let path = format!(
            "{}/tests/fixtures/input/telemetry",
            env!("CARGO_MANIFEST_DIR")
        );
        let batches = ctx
            .read_parquet(path, ParquetReadOptions::default())
            .await
            .unwrap()
            .select_columns(&["event_time"])
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut count = 0;
        for batch in batches {
            let column = batch.column(0);
            assert!(matches!(
                column.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ));
            let column = deltalake::arrow::compute::cast(column, &DataType::Utf8).unwrap();
            let strings = column.as_any().downcast_ref::<StringArray>().unwrap();
            let parsed = event_time(strings);
            assert_eq!(parsed.null_count(), strings.null_count());
            count += parsed.len();
        }
        assert!(count > 0);
    }
}
