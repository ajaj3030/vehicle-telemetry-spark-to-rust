# `battery_health_daily` functional spec

Summarizes daily battery readings for warranty and service, including charge level, temperature, alerts and changes in end-of-day health. [BatteryHealthDaily.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala#L7-L41)

## Inputs and output

Reads enriched Delta `vehicle_telemetry` columns `event_type`, `vin_hash`, `event_date`, `model_safe`, `charge_percent`, `max_cell_temp`, `cell_temp_spread`, `thermal_alert`, `battery_health_level` and `sequence_number`. Stream enrichment supplies the UTC date, cell temperature spread and thermal alert. Overwrites Delta `analytics/battery_health_daily`, partitioned by `event_date`. The actual grain is `(vin_hash, event_date, model_safe)`. The Rust comparison key is `(vin_hash, event_date)`, which assumes a single model group per VIN and date. [BatteryHealthDaily.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala#L7-L44) · [streaming_job.py · UTC session](../../../spark_jobs/streaming_job.py#L123-L144) · [spark_jobs/transforms.py](../../../spark_jobs/transforms.py) · [AnalyticsJob.scala · paths, UTC session and Delta writer](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala#L42-L94) · [config/app_config.yaml](../../../config/app_config.yaml) · [Rust implementation and tests](../../../rust_pipeline/src/analytics/battery_health_daily.rs)

| Output column, in calculation order | Type |
|---|---|
| `vin_hash` | string |
| `event_date` | date |
| `model_safe` | string |
| `readings` | bigint |
| `avg_charge_percent` | double |
| `min_charge_percent` | double |
| `peak_cell_temp` | double |
| `avg_cell_temp_spread` | double |
| `thermal_alert_count` | bigint |
| `health_level_end_of_day` | double |
| `peak_cell_temp_3d` | double |
| `health_change` | double |

[Spark Parquet fixture](../../../rust_pipeline/tests/fixtures/expected/battery_health_daily) · [BatteryHealthDaily.scala · output expressions](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala#L14-L41)

## Calculation

1. Keep `BATTERY` events. Group by `vin_hash`, `event_date` and `model_safe`. [BatteryHealthDaily.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala#L14-L17)
2. Count all rows as `readings`. Take average charge rounded to two decimals, minimum charge, maximum cell temperature and average cell temperature spread rounded to three decimals. Sum `thermal_alert` cast to integer (true = 1, false = 0). Use `max_by(battery_health_level, sequence_number)` for the end-of-day reading. [BatteryHealthDaily.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala#L18-L26)
3. For each VIN, convert `event_date` to UTC midnight and then to epoch seconds. Use an inclusive value-based `RANGE` from 172,800 seconds before the current row to the current row. Take the maximum daily `peak_cell_temp` over this frame as `peak_cell_temp_3d`. [BatteryHealthDaily.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala#L10-L12) · [BatteryHealthDaily.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala#L28-L36) · [AnalyticsJob.scala · paths, UTC session and Delta writer](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala#L42-L94)
4. For each VIN, sort by ascending date and subtract the preceding output row's `health_level_end_of_day` from the current row's value, rounded HALF_UP to two decimals. `lag(…, 1)` steps back one row, so it can reach over gaps longer than the rolling temperature window. [BatteryHealthDaily.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala#L33-L40) · [rust_pipeline/src/analytics/spark_functions.rs](../../../rust_pipeline/src/analytics/spark_functions.rs)

## Nulls and edge cases

- Null or non-battery event types are filtered. Null grouping keys are their own buckets. `count(*)` includes readings with missing values. `avg`, `min`, `max` and `sum` skip nulls and return null if their inputs are all null. [BatteryHealthDaily.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala#L14-L26) · [Rust implementation and tests](../../../rust_pipeline/src/analytics/battery_health_daily.rs)
- `max_by` skips null ordering keys and returns null if all ordering keys are null. It can return a null health value from the highest sequence. Equal maximum sequences have no tiebreaker. The first lagged row and any subtraction with either operand null produce null `health_change`. [BatteryHealthDaily.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala#L18-L26) · [BatteryHealthDaily.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala#L33-L40) · [rust_pipeline/src/analytics/spark_functions.rs](../../../rust_pipeline/src/analytics/spark_functions.rs)
- No rows are synthesized for missing dates. A null date sorts first in the lag window, and its rolling range covers its null-date peers. Multiple models on one date produce multiple daily rows for the same VIN/date. Both windows cross models within that VIN; the range includes same-date peers and the date-only lag has no defined order within those peers. [BatteryHealthDaily.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala#L17-L39) · [Rust implementation and tests](../../../rust_pipeline/src/analytics/battery_health_daily.rs)

## Hardest Spark behaviour to reproduce

The two windows use different frames over the same daily rows. A three-row frame changes the temperature result across missing dates, and a calendar join changes the meaning of lag. Preserve inclusive UTC day boundaries and null ordering. Preserve `max_by` selection when the winning measurement is null, and apply decimal HALF_UP rounding at the same stages. [BatteryHealthDaily.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala#L18-L40) · [Rust implementation and tests](../../../rust_pipeline/src/analytics/battery_health_daily.rs) · [rust_pipeline/src/analytics/spark_functions.rs](../../../rust_pipeline/src/analytics/spark_functions.rs)
