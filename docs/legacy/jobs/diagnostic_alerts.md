# `diagnostic_alerts`

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala` (below `DiagnosticAlerts.scala`). Runner: `spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala` (below `AnalyticsJob.scala`).

## Purpose

Impact of each diagnostic trouble code per ECU, model and battery pack: event and vehicle counts, warning-on events, first and last occurrence, share of the whole fleet affected, and rank within the ECU by vehicles affected (`DiagnosticAlerts.scala:11-34`). Shown on the dashboard's diagnostics view (`dashboard/app.py:732-748`).

## Inputs

- `vehicle_telemetry`, rows with `event_type = 'DIAGNOSTIC'` (`DiagnosticAlerts.scala:17,38`). Columns used: `vin_hash`, `dtc_code`, `ecu`, `model_safe`, `warning_state`, `event_time` (`DiagnosticAlerts.scala:21-27`).
- `vehicle_state` (`DiagnosticAlerts.scala:39`): raw `vin` and `battery_pack_type` (`DiagnosticAlerts.scala:13`), plus its row count (`DiagnosticAlerts.scala:14`). It is the only analytics job that reads this table (`../dependencies.md`). In Rust it is `vehicle_state_rust`, registered as `vehicle_state` (`rust_pipeline/src/analytics/mod.rs:86-87`).

## Output schema

Delta `<analytics.output_base>/diagnostic_alerts`, overwrite, unpartitioned (`AnalyticsJob.scala:77,87-91`). Types from `rust_pipeline/tests/fixtures/expected/diagnostic_alerts/` (235 rows):

| Column | Type | Definition |
|--------|------|------------|
| `dtc_code` | string | group key |
| `ecu` | string | group key |
| `model_safe` | string | group key |
| `battery_pack_type` | string, nullable | group key, from `vehicle_state` |
| `events` | int64 | `count(*)` |
| `vehicles_affected` | int64 | `countDistinct(vin_hash)` |
| `warning_on_events` | int64 | `sum(when(warning_state = 'WARNING_ON', 1).otherwise(0))` |
| `first_seen` | timestamp[µs, UTC] | `min(to_timestamp(event_time))` |
| `last_seen` | timestamp[µs, UTC] | `max(to_timestamp(event_time))` |
| `fleet_pct_affected` | double | `round(vehicles_affected / fleet_size * 100, 3)` |
| `rank_in_ecu` | **int32** | `dense_rank()` per `ecu` by `vehicles_affected` desc |

Sources: `DiagnosticAlerts.scala:13-33`.

## Calculation

1. `fleet` = `vehicle_state` projected to `vin_hash = sha2(vin, 256)` (lower-case hex) and `battery_pack_type` (`DiagnosticAlerts.scala:12-13`).
2. `fleet_size = vehicle_state.count()`, evaluated eagerly as a literal before the main query (`DiagnosticAlerts.scala:14`). It counts every vehicle with a state row, not only vehicles with diagnostics.
3. Filter telemetry to `event_type = 'DIAGNOSTIC'`, drop telemetry's own `battery_pack_type` (diagnostic events carry none), and **left** join `fleet` on `vin_hash` with a broadcast hint (`DiagnosticAlerts.scala:16-20`).
4. Group by `(dtc_code, ecu, model_safe, battery_pack_type)` and compute the five aggregates (`DiagnosticAlerts.scala:21-28`).
5. `fleet_pct_affected = round(vehicles_affected / fleet_size * 100, 3)`; `/` on two longs yields double (`DiagnosticAlerts.scala:32`).
6. `rank_in_ecu = dense_rank() OVER (PARTITION BY ecu ORDER BY vehicles_affected DESC)` (`DiagnosticAlerts.scala:30,33`).

## Nulls and edge cases

*(probe)* = confirmed with `pyspark==3.5.1`, session time zone UTC.

- **Telemetry vehicle missing from `vehicle_state`:** the left join keeps the event with `battery_pack_type = null`, and null is its own group. 17 DIAGNOSTIC events in the fixture have no matching state row (`rust_pipeline/tests/fixtures/input/`).
- **Duplicate VINs in `vehicle_state`** would duplicate events and inflate `fleet_size`; the MERGE keeps one row per VIN (`spark_jobs/vehicle_state_job.py:120-143`), which the job relies on.
- **`warning_state` null or anything other than `WARNING_ON`** counts 0, so `warning_on_events` is never null.
- **`countDistinct`** ignores null `vin_hash`; `count(*)` does not.
- **Unparseable `event_time`:** null from `to_timestamp` *(probe)*, skipped by `min`/`max`.
- **Empty `vehicle_state`:** `fleet_size = 0`, so `fleet_pct_affected` is null (non-ANSI division by zero) *(probe for `x/0`)*.
- **Ties in `vehicles_affected`** share a rank and the next rank is not skipped (`dense_rank`). Null `ecu` would form its own window partition.
- The output keeps `vin_hash` only inside aggregates; no raw `vin` leaves the job, as required by `rust_pipeline/src/validate.rs:19`.

## Hardest Spark behaviour to reproduce in Rust

1. **The SHA-256 join key.** Spark's `sha2(vin, 256)` is a lower-case hex string (`spark_jobs/transforms.py:173` uses the same function for telemetry's `vin_hash`); DataFusion's `sha256` returns binary, so the port needs `encode(sha256(vin), 'hex')` or equivalent to match.
2. **Two inputs and an eager count.** The denominator is a separate action over the whole `vehicle_state`, so Rust needs a separate `COUNT(*)` over `vehicle_state` before (or cross-joined into) the main plan, and parity depends on `vehicle_state_rust` already matching Spark (`rust_pipeline/src/validate.rs:44-50`).
3. **Left join with null grouping keys**: `battery_pack_type` null must group, and `validate` already matches null keys to each other via `COALESCE(..., '\u{0}NULL')` (`rust_pipeline/src/validate.rs:235-243`).
4. **`dense_rank` typed int32.** DataFusion returns `UInt64`; the column must be cast to `Int32` to pass `check_schema_contract` (`rust_pipeline/src/validate.rs:185-212`).
5. **`to_timestamp` to microsecond timestamps** (units are type-checked, time zones are not: `rust_pipeline/src/validate.rs:205-209`) with null-on-error (as in `docs/legacy/jobs/charging_sessions.md`), and **HALF_UP `round(…, 3)`**.
