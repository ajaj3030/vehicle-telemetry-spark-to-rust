# `diagnostic_alerts` — functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala` (cited as `DiagnosticAlerts.scala`).
Rust target: `rust_pipeline/src/analytics/diagnostic_alerts.rs` (stub).

## Purpose

Vehicles affected per diagnostic trouble code, ECU, model and battery pack; share of the fleet; rank
of the code within its ECU (`README.md:69-78`, `dashboard/app.py:732-760`).

## Inputs

| Table | Rows used | Columns read |
|---|---|---|
| `vehicle_telemetry` (`AnalyticsJob.scala:49`) | `event_type = 'DIAGNOSTIC'` (`DiagnosticAlerts.scala:17`) | `vin_hash`, `dtc_code`, `ecu`, `model_safe`, `warning_state`, `event_time`; `battery_pack_type` is dropped (`DiagnosticAlerts.scala:19`) |
| `vehicle_state` (`AnalyticsJob.scala:50`) | all rows | `vin` (hashed), `battery_pack_type`; `count()` of all rows (`DiagnosticAlerts.scala:13-14`) |

This is the only analytics job that reads `vehicle_state` (`README.md:69-72`) and therefore the only one
that touches a raw VIN (`spark_jobs/vehicle_state_job.py:58-73`).

## Output schema

Table `<analytics base>/diagnostic_alerts`, overwrite, **no partition columns** (`DiagnosticAlerts.scala:8-40`).
Parity key: `dtc_code`, `ecu`, `model_safe`, `battery_pack_type` (`rust_pipeline/src/analytics/diagnostic_alerts.rs`).
Types from `rust_pipeline/tests/fixtures/expected/diagnostic_alerts`:

| Column | Type | Definition |
|---|---|---|
| `dtc_code` | string | Group key |
| `ecu` | string | Group key |
| `model_safe` | string | Group key |
| `battery_pack_type` | string | Group key, from `vehicle_state` (null on lookup miss) |
| `events` | int64 | `count(*)` |
| `vehicles_affected` | int64 | `countDistinct(vin_hash)` |
| `warning_on_events` | int64 | count of `warning_state = 'WARNING_ON'` |
| `first_seen` | timestamp[us, UTC] | `min(to_timestamp(event_time))` |
| `last_seen` | timestamp[us, UTC] | `max(to_timestamp(event_time))` |
| `fleet_pct_affected` | double | `round(vehicles_affected / fleetSize * 100, 3)` |
| `rank_in_ecu` | int32 | `dense_rank()` over `ecu` by `vehicles_affected DESC` |

## Calculation

1. `fleet = vehicle_state.select(sha2(vin, 256) AS vin_hash, battery_pack_type)`; `fleetSize = vehicle_state.count()`
   (an eager Spark action) (`DiagnosticAlerts.scala:13-14`).
2. Telemetry filtered to DIAGNOSTIC, drop its `battery_pack_type`, `broadcast` left join to `fleet` on `vin_hash`
   (`DiagnosticAlerts.scala:17-20`).
3. `groupBy(dtc_code, ecu, model_safe, battery_pack_type)` and aggregate (`DiagnosticAlerts.scala:21-28`).
4. `fleet_pct_affected` (`DiagnosticAlerts.scala:32`); `rank_in_ecu` (`DiagnosticAlerts.scala:30`, `:33`).

## Nulls and edge cases

| Case | Spark result |
|---|---|
| Telemetry VIN not in `vehicle_state` | `battery_pack_type` null → its own group. Fixture: 3 VINs without state, 17 output rows with null pack. |
| Diagnostic rows' own `battery_pack_type` | Always null in the fixture; it is dropped, so it never matters. |
| `fleetSize` | Counts **all** state rows, not just vehicles with diagnostics, and excludes telemetry VINs missing from state. |
| Empty `vehicle_state` | `fleetSize = 0` → `fleet_pct_affected` null. |
| Duplicate VIN in state | Join fans out events (inflates `events`, not `vehicles_affected`); state is one row per VIN by MERGE (`spark_jobs/vehicle_state_job.py:120-143`). |
| Ties in `vehicles_affected` | Same `dense_rank`, no gaps. Fixture: 7 ECUs, max rank 9, many ties. |
| Null `ecu` | Own rank partition. |

Measured on `rust_pipeline/tests/fixtures/expected/diagnostic_alerts` and `rust_pipeline/tests/fixtures/input/`.

## Hardest Spark behaviour to reproduce

1. **`sha2(vin, 256)` hex** must equal the stream's `vin_hash` (`spark_jobs/transforms.py:169-177`); the Rust
   stream has a SHA-256 helper (`rust_pipeline/src/transforms/anonymized.rs`) but DataFusion needs it as a UDF
   or a pre-hashed column.
2. **Two inputs** — the Rust runner already registers both (`rust_pipeline/src/analytics/mod.rs:82-87`), but parity
   depends on the Rust state stream's correctness too.
3. **`dense_rank` type** is `Int32` in Spark; DataFusion returns `UInt64` → cast required for the exact
   schema check (`rust_pipeline/src/validate.rs:185-212`).
4. **Null group keys** in the join output and null-safe parity key matching (`rust_pipeline/src/validate.rs:235-243`).
5. **`count(DISTINCT)`, timestamp parsing, HALF_UP rounding** as in other jobs.
