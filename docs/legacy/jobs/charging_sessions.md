# `charging_sessions` functional spec

Summarizes charging sessions with their time span, energy, power, state of charge (SoC), end state and fault indicator. [ChargingSessions.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala#L6-L41)

## Inputs and output

Reads enriched Delta `vehicle_telemetry` columns `event_type`, `charge_session_id`, `vin_hash`, `model_safe`, `event_time`, `energy_added_kwh`, `charging_power_kw`, `charge_percent`, `sequence_number`, `charge_state` and `is_fault`. Overwrites Delta `analytics/charging_sessions`, partitioned by `session_date`. Spark groups by `(charge_session_id, vin_hash, model_safe)`. The Rust comparison key is `charge_session_id`, which assumes each ID identifies a single group. [ChargingSessions.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala#L7-L41) · [AnalyticsJob.scala · paths, UTC session and Delta writer](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala#L42-L94) · [config/app_config.yaml](../../../config/app_config.yaml) · [Rust implementation and tests](../../../rust_pipeline/src/analytics/charging_sessions.rs)

| Output column, in calculation order | Type |
|---|---|
| `charge_session_id` | string |
| `vin_hash` | string |
| `model_safe` | string |
| `session_start` | timestamp (UTC, microseconds) |
| `session_end` | timestamp (UTC, microseconds) |
| `events` | bigint |
| `energy_added_kwh` | double |
| `peak_power_kw` | double |
| `soc_start` | double |
| `soc_end` | double |
| `end_state` | string |
| `had_fault` | boolean |
| `duration_minutes` | double |
| `avg_power_kw` | double |
| `session_date` | date |

[Spark Parquet fixture](../../../rust_pipeline/tests/fixtures/expected/charging_sessions) · [ChargingSessions.scala · output expressions](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala#L12-L37)

## Calculation

1. Keep `CHARGING` rows with a non-null `charge_session_id`. Group by session ID, VIN hash and safe model. [ChargingSessions.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala#L11-L14)
2. Parse `event_time` with Spark `to_timestamp`. Use the earliest and latest parsed instants as `session_start` and `session_end`. Count every retained row as `events`. Sum `energy_added_kwh` and round to three decimals. Take the maximum measured `charging_power_kw`. [ChargingSessions.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala#L15-L20) · [AnalyticsJob.scala · paths, UTC session and Delta writer](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala#L42-L94)
3. Choose `soc_start` from the smallest sequence (`min_by`) and `soc_end` and `end_state` from the largest sequence (`max_by`). The sequence extrema can be different rows from the timestamp extrema. Cast the fault flag to integer, take its maximum, and cast back to boolean. [ChargingSessions.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala#L16-L24)
4. Cast start/end timestamps separately to double epoch seconds, subtract start from end, divide by 60 and round to two decimals for `duration_minutes`. When the rounded duration is positive, divide the rounded energy by the rounded duration in hours and round to two decimals for `avg_power_kw`. Derive `session_date` from `session_start` in UTC. All rounding is HALF_UP. [ChargingSessions.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala#L27-L37) · [AnalyticsJob.scala · paths, UTC session and Delta writer](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala#L42-L94) · [rust_pipeline/src/analytics/spark_functions.rs](../../../rust_pipeline/src/analytics/spark_functions.rs)

## Nulls and edge cases

- A missing session ID is excluded. An empty string is a groupable ID. Reuse of an ID with a different hash or model yields separate rows. A session spanning midnight still produces one row per group, partitioned using its start date. [ChargingSessions.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala#L11-L17) · [ChargingSessions.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala#L27-L37)
- Null and unparseable timestamps become null under the stock non-ANSI session. They are ignored by the timestamp extrema and still count toward `events`. All-invalid times yield null start, end, duration, date and average power. A single valid instant, or a short positive duration that rounds to 0.00 minutes, yields null average power. [ChargingSessions.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala#L15-L24) · [ChargingSessions.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala#L27-L37) · [AnalyticsJob.scala · paths, UTC session and Delta writer](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala#L42-L94) · [Rust implementation and tests](../../../rust_pipeline/src/analytics/charging_sessions.rs)
- `sum` and `max` skip nulls and return null if every reading is null. Thus all-null energy leaves null average power and all-null flags leave null `had_fault`. The extrema-by-sequence skip null sequences, return null for an all-null sequence group and return the chosen value even when that value is null. Equal extreme sequences have no tiebreaker. [ChargingSessions.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala#L18-L24) · [ChargingSessions.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala#L33-L36) · [rust_pipeline/src/analytics/spark_functions.rs](../../../rust_pipeline/src/analytics/spark_functions.rs) · [Rust implementation and tests](../../../rust_pipeline/src/analytics/charging_sessions.rs)

## Hardest Spark behaviour to reproduce

Duration depends on Spark's permissive UTC microsecond parser and the subtraction of two floating-point epoch values, followed by rounding. Changing the parser, subtracting integer microseconds first, or calculating power from unrounded intermediates can change results near rounding boundaries. Spark's `min_by` and `max_by` also preserve a null payload at the winning sequence. [ChargingSessions.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala#L16-L37) · [Rust implementation and tests](../../../rust_pipeline/src/analytics/charging_sessions.rs) · [rust_pipeline/src/analytics/spark_functions.rs](../../../rust_pipeline/src/analytics/spark_functions.rs)
