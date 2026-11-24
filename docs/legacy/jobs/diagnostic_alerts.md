# `diagnostic_alerts` functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala` (cited as `DiagnosticAlerts.scala:N`).
Rust target: `rust_pipeline/src/analytics/diagnostic_alerts.rs` (stub).

## Purpose
Fleet impact of each diagnostic trouble code, for service triage: vehicles affected,
share of fleet and rank within ECU (`DiagnosticAlerts.scala:7`, `README.md:79`). Shown on
the dashboard diagnostics tab (`dashboard/app.py:732-748`).

## Inputs
- `vehicle_telemetry` rows with `event_type = 'DIAGNOSTIC'`; columns `vin_hash`, `dtc_code`,
  `ecu`, `model_safe`, `warning_state`, `event_time` (`DiagnosticAlerts.scala:16-28`, `:38`).
- The whole `vehicle_state` table: `vin`, `battery_pack_type`, and its row count
  (`DiagnosticAlerts.scala:13-14`, `:39`). This is the only analytics job with two inputs.
  The Rust runner reads `vehicle_state_rust` instead (`rust_pipeline/src/analytics/mod.rs:84-87`).

## Output schema
Table `analytics/diagnostic_alerts`, overwritten each run, not partitioned
(`AnalyticsJob.scala:75-97`; no `partitionCols` override). Key: `dtc_code, ecu, model_safe,
battery_pack_type` (`rust_pipeline/src/analytics/diagnostic_alerts.rs:10`). Types from
`rust_pipeline/tests/fixtures/expected/diagnostic_alerts/`:

| Column | Type | Nullable |
|---|---|---|
| `dtc_code`, `ecu`, `model_safe`, `battery_pack_type` | string | yes |
| `events`, `vehicles_affected` | long | no |
| `warning_on_events` | long | yes |
| `first_seen`, `last_seen` | timestamp (µs, UTC) | yes |
| `fleet_pct_affected` | double | yes |
| `rank_in_ecu` | int | no |

## Calculation
1. `fleet` = `vehicle_state` projected to `sha2(vin, 256)` as `vin_hash` and `battery_pack_type`;
   `fleetSize` = number of `vehicle_state` rows (`:13-14`).
2. Keep `DIAGNOSTIC` telemetry rows, drop their own `battery_pack_type`, and left-join `fleet` on
   `vin_hash` (broadcast hint) (`:16-20`).
3. Group by `(dtc_code, ecu, model_safe, battery_pack_type)` (`:21`) and compute `events` = row count,
   `vehicles_affected` = distinct `vin_hash`, `warning_on_events` = rows with
   `warning_state = 'WARNING_ON'`, `first_seen` / `last_seen` = min / max of
   `to_timestamp(event_time)` (`:22-28`).
4. `fleet_pct_affected` = `round(vehicles_affected / fleetSize * 100, 3)` (`:32`).
5. `rank_in_ecu` = `dense_rank()` within `ecu`, ordered by `vehicles_affected` descending (`:30`, `:33`).

## Nulls and edge cases
- **Vehicles in telemetry but not in `vehicle_state`:** the left join keeps them with a null
  `battery_pack_type`, so they form separate groups (`:20`). The fixture has 150 telemetry
  vehicles and 147 state rows; 17 expected rows have a null `battery_pack_type`
  (`rust_pipeline/tests/fixtures/input/`, `.../expected/diagnostic_alerts/`). Null key parts are
  matched null-safely by the parity check (`rust_pipeline/src/validate.rs:235-243`).
- **Fleet size counts every `vehicle_state` row**, including vehicles with no telemetry, and is
  not limited to the telemetry date range (`:14`). If `vehicle_state` is empty, the division is
  by zero and, in non-ANSI mode, `fleet_pct_affected` is null.
- **The join only works if `sha2(vin, 256)` equals telemetry's `vin_hash`**, which T6 computes the
  same way (`spark_jobs/transforms.py:169-176`). A null `vin` hashes to null and joins nothing.
  `vehicle_state` has one row per VIN (`spark_jobs/vehicle_state_job.py:120-143`); duplicates
  would multiply `events`.
- **Ties in `vehicles_affected`** share a rank and the next rank is not skipped (`dense_rank`, `:33`).
  Rows with a null `ecu` rank among themselves. Fixture max rank is 9.
- **`warning_on_events`** is a `sum` of 0/1, so it is never null in practice but typed nullable.
- **Unparseable `event_time`** gives null in `to_timestamp`, skipped by min/max.

## Spark behaviour hardest to reproduce in Rust
1. **Second input and its provenance:** in Rust the input is `vehicle_state_rust`, which must be at
   parity with Spark's `vehicle_state` (`validate --state`, `rust_pipeline/src/validate.rs:44-50`),
   or fleet size and pack type differ.
2. **`sha2(col, 256)`** must give lowercase hex of the UTF-8 VIN, the same as the Rust T6
   (`rust_pipeline/src/transforms/`).
3. **`dense_rank` returns `Int32`** non-null; DataFusion's window functions return `UInt64`, so
   the result needs a cast to pass the type check (`rust_pipeline/src/validate.rs:185-212`).
4. **`countDistinct` and `count(*)` as non-null longs**, long/long division to double, `round`
   HALF_UP to 3 decimals (`rust_pipeline/src/transforms/rounding.rs`).
5. **`to_timestamp`** on ISO strings, UTC, microseconds, null on failure (open PR #20 helper).
