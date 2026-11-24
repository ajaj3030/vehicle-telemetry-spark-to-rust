# diagnostic_alerts

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala`
(cited below as `DiagnosticAlerts.scala:<line>`). Rust stub:
`rust_pipeline/src/analytics/diagnostic_alerts.rs`.

## Purpose

Fleet impact of each diagnostic trouble code (DTC), for service triage
(`DiagnosticAlerts.scala:7`): events, vehicles affected, share of the fleet
and rank within the ECU, per code, ECU, model and battery pack. The
dashboard's Diagnostics tab reads it (`dashboard/app.py:732-748`).

## Inputs

Two tables (`DiagnosticAlerts.scala:36-40`):

- `vehicle_telemetry`, DIAGNOSTIC rows. Columns used: `event_type`,
  `vin_hash`, `dtc_code`, `ecu`, `model_safe`, `warning_state`, `event_time`.
  Its `battery_pack_type` is dropped, because diagnostic events carry none
  (`DiagnosticAlerts.scala:18-19`). All 897 fixture DIAGNOSTIC rows have it
  NULL.
- `vehicle_state`, keyed by raw `vin`. Columns used: `vin`,
  `battery_pack_type`, and the total row count. The live Rust run reads
  `vehicle_state_rust` (`rust_pipeline/src/analytics/mod.rs:86-87`).

## Output schema

Table `analytics/diagnostic_alerts`, overwritten, not partitioned
(`AnalyticsJob.scala:77`, `:87-91`). Rust parity key: `dtc_code`, `ecu`,
`model_safe`, `battery_pack_type`
(`rust_pipeline/src/analytics/diagnostic_alerts.rs:10`). Types from
`rust_pipeline/tests/fixtures/expected/diagnostic_alerts/`.

| Column | Type | Meaning |
|---|---|---|
| `dtc_code` | string | Trouble code |
| `ecu` | string | Reporting ECU |
| `model_safe` | string | Model |
| `battery_pack_type` | string (nullable) | From `vehicle_state`. NULL if the vehicle has no state row |
| `events` | bigint, not null | DIAGNOSTIC events |
| `vehicles_affected` | bigint, not null | Distinct `vin_hash` |
| `warning_on_events` | bigint | Events with `warning_state = 'WARNING_ON'` |
| `first_seen` | timestamp (µs, UTC) | Earliest `event_time` |
| `last_seen` | timestamp (µs, UTC) | Latest `event_time` |
| `fleet_pct_affected` | double | `vehicles_affected / fleet size * 100`, 3 dp |
| `rank_in_ecu` | int, not null | `dense_rank` by `vehicles_affected` desc within the ECU |

## Calculation

1. `fleet = SELECT sha2(vin, 256) AS vin_hash, battery_pack_type FROM vehicle_state`
   (`DiagnosticAlerts.scala:12-13`).
2. `fleetSize = vehicle_state.count()`. This is a separate Spark action,
   evaluated once and used as a literal. It counts **every**
   `vehicle_state` row, including vehicles with no diagnostic events
   (`DiagnosticAlerts.scala:14`).
3. Keep `event_type = 'DIAGNOSTIC'`, drop the telemetry `battery_pack_type`,
   then **left** join `broadcast(fleet)` on `vin_hash`
   (`DiagnosticAlerts.scala:16-20`).
4. Group by `dtc_code, ecu, model_safe, battery_pack_type`
   (`DiagnosticAlerts.scala:21`):
   - `events = count(*)`
   - `vehicles_affected = count(DISTINCT vin_hash)`
   - `warning_on_events = sum(CASE WHEN warning_state = 'WARNING_ON' THEN 1 ELSE 0 END)`
   - `first_seen = min(to_timestamp(event_time))`, `last_seen = max(…)`

   (`DiagnosticAlerts.scala:22-28`)
5. `fleet_pct_affected = round(vehicles_affected / fleetSize * 100, 3)`
   (`DiagnosticAlerts.scala:32`).
6. `rank_in_ecu = dense_rank()` over `PARTITION BY ecu ORDER BY vehicles_affected DESC`
   (`DiagnosticAlerts.scala:30`, `:33`). Ties share a rank, and ranks span
   all models and packs within the ECU.

## Nulls and edge cases

*Verified* means run through the same Spark functions under PySpark 3.5.1
locally. Fixture counts are from `rust_pipeline/tests/fixtures/`.

| Case | Spark behaviour |
|---|---|
| Vehicle has telemetry but no `vehicle_state` row | The left join keeps it with `battery_pack_type = NULL`, which becomes a group key. 3 fixture vehicles give 17 output rows with a NULL pack. |
| Fleet size vs. affected vehicles | The denominator is the `vehicle_state` row count (147 in the fixture), but the numerator also counts vehicles with no state row (150 telemetry vehicles). The two populations differ. |
| Empty `vehicle_state` | `fleetSize = 0`, so `fleet_pct_affected` is NULL (division by zero gives NULL with ANSI mode off). |
| Duplicate VIN in `vehicle_state` | The join fans out, which inflates `events`, and `fleetSize` grows. The MERGE keeps one row per VIN (`spark_jobs/vehicle_state_job.py:120-143`). The fixture has no duplicates. |
| NULL `vin` in `vehicle_state` | `sha2(NULL)` is NULL (*verified*) and matches nothing, but the row still counts in `fleetSize`. The state stream drops NULL VINs (`spark_jobs/vehicle_state_job.py:171`). |
| NULL `dtc_code` / `ecu` | Grouped as keys. A NULL `ecu` forms its own rank partition. The fixture has none. |
| `warning_on_events` | 0 when there are no warnings, never NULL for an existing group. |
| Rank ties | `dense_rank` gives equal ranks with no gaps. 181 fixture rows share their (ecu, rank) with another row. |
| `event_time` parsing | Same as `charging_sessions`: unparseable strings become NULL and are ignored by `min` / `max` (*verified*). |

## Hardest Spark behaviour to reproduce

1. **Two inputs and an eager fleet count.** Count all `vehicle_state` rows
   first, then use the count as a constant. Do not count joined or distinct
   vehicles. Live parity also depends on `vehicle_state_rust` matching
   `vehicle_state` (`README.md:60`).
2. **`sha2(vin, 256)` as a lowercase hex string.** DataFusion's `sha256`
   returns binary, so it needs hex encoding or a UDF around
   `rust_pipeline/src/transforms/anonymized.rs:20` (`sha256_hex`).
3. **`dense_rank` type.** Spark returns int (Int32, not null). DataFusion
   43.1.0 returns UInt64 (checked locally), which fails the type check
   (`rust_pipeline/src/validate.rs:185-212`) unless cast to Int32.
4. **Integer division.** `bigint / bigint` must be double division, not
   DataFusion's integer division. Then apply HALF_UP rounding at 3 dp
   (`rust_pipeline/src/transforms/rounding.rs:1-7`).
5. **`to_timestamp` to `Timestamp(µs, UTC)`**, as in
   [charging_sessions.md](charging_sessions.md#hardest-spark-behaviour-to-reproduce).
6. **NULL grouping keys from the left join**, which must survive grouping,
   ranking and the parity join.
