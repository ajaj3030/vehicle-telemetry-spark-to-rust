# diagnostic_alerts

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala`. Rust stub: `rust_pipeline/src/analytics/diagnostic_alerts.rs`.
Fixture numbers measured with DuckDB over `rust_pipeline/tests/fixtures/input/{telemetry,vehicle_state}` and `rust_pipeline/tests/fixtures/expected/diagnostic_alerts`.

## Purpose
Which fault codes affect how much of the fleet, ranked by vehicles affected within each ECU, split by model and battery pack (`README.md:79`, `spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala:11-34`).

## Inputs
| Table | Columns used | Cited in |
|---|---|---|
| `vehicle_telemetry` (Rust: `vehicle_telemetry_rust`) | `event_type`, `vin_hash`, `dtc_code`, `ecu`, `model_safe`, `warning_state`, `event_time` | `:17-27,38` |
| `vehicle_state` (Rust: `vehicle_state_rust`) | `vin` (raw), `battery_pack_type`, row count | `:13-14,39`, `rust_pipeline/src/analytics/mod.rs:84-87` |

This is the only analytics job that reads `vehicle_state`, so it depends on the state MERGE job (`spark_jobs/vehicle_state_job.py:120-143`) or its Rust port.

## Output schema
Unpartitioned (`spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala:77`). Key `dtc_code, ecu, model_safe, battery_pack_type` (`rust_pipeline/src/analytics/diagnostic_alerts.rs:8-12`).

| # | Column | Spark type | Source |
|---|---|---|---|
| 1 | `dtc_code` | string | key, `:21` |
| 2 | `ecu` | string | key, `:21` |
| 3 | `model_safe` | string | key, `:21` |
| 4 | `battery_pack_type` | string, nullable | key from `vehicle_state`, `:19-21` |
| 5 | `events` | bigint | `:23` |
| 6 | `vehicles_affected` | bigint | `countDistinct(vin_hash)`, `:24` |
| 7 | `warning_on_events` | bigint | `:25` |
| 8 | `first_seen` | timestamp (Parquet `timestamp[us, tz=UTC]`) | `:26` |
| 9 | `last_seen` | timestamp (Parquet `timestamp[us, tz=UTC]`) | `:27` |
| 10 | `fleet_pct_affected` | double | `:32` |
| 11 | `rank_in_ecu` | int | `dense_rank`, `:33` |

## Calculation
1. `fleet` = `vehicle_state` projected to (`sha2(vin,256)` as `vin_hash`, `battery_pack_type`); `fleetSize` = `vehicle_state.count()`, evaluated eagerly as a literal (`:13-14`).
2. Keep `event_type = 'DIAGNOSTIC'`, drop telemetry's own (always-null) `battery_pack_type`, broadcast-**left**-join `fleet` on `vin_hash` (`:17-20`).
3. Group by (`dtc_code`, `ecu`, `model_safe`, `battery_pack_type`) and aggregate; `warning_on_events` counts `warning_state = 'WARNING_ON'` (`:21-28`).
4. `fleet_pct_affected` = round(vehicles_affected / fleetSize × 100, 3) (`:32`).
5. `rank_in_ecu` = `dense_rank()` over partition `ecu` ordered by `vehicles_affected` desc (`:30,33`).

## Nulls and edge cases
- **Vehicle missing from `vehicle_state`:** left join keeps it with null `battery_pack_type`, forming a null-key group. Fixture: 3 telemetry VINs have no state row → 17 output rows with null pack type. Parity joins keys null-safely (`rust_pipeline/src/validate.rs:232-243`).
- **Denominator is the state table size**, not the vehicles seen in telemetry (fixture: 147 state rows vs 150 telemetry VINs), so the percentage is not bounded by vehicles with diagnostics.
- **Empty `vehicle_state`:** division by zero → null `fleet_pct_affected` (ANSI off, `spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala:55-68`).
- **Duplicate VINs in `vehicle_state`** would duplicate telemetry rows (inflating `events`, not `vehicles_affected`) and inflate `fleetSize`; the MERGE keeps one row per VIN (`spark_jobs/vehicle_state_job.py:120-143`).
- **Ties:** equal `vehicles_affected` in one ECU share a rank, no gaps; 43 tied (ecu, vehicles_affected) groups in the fixture, max rank 9.
- `warning_state` null counts as 0 (`otherwise(0)`, `:25`).

## Hardest Spark behaviour to reproduce
1. **Two inputs and a cross-table semantic**: hashing raw VIN in Rust must match Spark `sha2(…,256)` hex output, and the Rust job reads `vehicle_state_rust`, whose schema comes from `rust_pipeline/src/vehicle_state.rs`, not from Spark.
2. **Null-key grouping after a left join** and a global count used as a scalar.
3. **`dense_rank` returning Spark `int`** (DataFusion returns UInt64) with tie handling; `countDistinct`; `to_timestamp` ×2; HALF_UP rounding at scale 3.
