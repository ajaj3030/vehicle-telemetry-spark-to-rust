# diagnostic_alerts — functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala` (line refs to this file unless stated).
Rust placeholder: `rust_pipeline/src/analytics/diagnostic_alerts.rs`.
Expected output fixture: `rust_pipeline/tests/fixtures/expected/diagnostic_alerts/`.

## Purpose

Fleet impact of each diagnostic trouble code, for service triage (line 7): per (DTC, ECU, model, battery
pack) the number of events, distinct vehicles affected, warning-lamp events, first/last seen, share of the
whole fleet affected, and a rank within the ECU by vehicles affected. Dashboard: Diagnostics tab
(`dashboard/app.py:732-748`).

## Inputs

| Table | Rows used | Columns used |
|---|---|---|
| telemetry | `event_type = 'DIAGNOSTIC'` (line 17) | `vin_hash`, `dtc_code`, `ecu`, `model_safe`, `warning_state`, `event_time` |
| **vehicle_state** (Spark: `vehicle_state.delta_output_path`; Rust: `vehicle_state_rust`) | all rows | `vin` (raw VIN), `battery_pack_type`; plus `count()` of all rows |

It is the only analytics job with two inputs, and the only one that reads raw VINs: it recomputes
`sha2(vin, 256)` to join (lines 12-13), duplicating the T6 pseudonymisation in
`spark_jobs/transforms.py:173` / `rust_pipeline/src/transforms/anonymized.rs:13-24`.
DIAGNOSTIC events never carry `battery_pack_type` (only BATTERY events set it,
`data_generator/generator.py:69-84`), so it is dropped and taken from vehicle_state (lines 18-20).

## Output

Not partitioned; key (`dtc_code`, `ecu`, `model_safe`, `battery_pack_type`)
(`rust_pipeline/src/analytics/diagnostic_alerts.rs:8-12`). The key can contain null `battery_pack_type`;
the parity join is null-safe (`rust_pipeline/src/validate.rs:232-243`).

| Column | Type | Definition |
|---|---|---|
| dtc_code, ecu, model_safe | string | group keys |
| battery_pack_type | string | group key from vehicle_state; null if the vehicle has no state row |
| events | int64 | `count(*)` |
| vehicles_affected | int64 | `countDistinct(vin_hash)` |
| warning_on_events | int64 | `sum(when(warning_state = 'WARNING_ON', 1).otherwise(0))` |
| first_seen / last_seen | timestamp[us, UTC] | `min` / `max` of `to_timestamp(event_time)` |
| fleet_pct_affected | double | `round(vehicles_affected / count(vehicle_state) * 100, 3)` |
| rank_in_ecu | **int32** | `dense_rank()` over `PARTITION BY ecu ORDER BY vehicles_affected DESC` |

## Calculation

1. `fleet` = vehicle_state projected to (`sha2(vin,256)` as `vin_hash`, `battery_pack_type`) (line 13).
2. `fleetSize` = `vehicleState.count()` — **every** vehicle_state row, including vehicles with no diagnostic
   events, computed eagerly as a literal (line 14).
3. Filter DIAGNOSTIC telemetry, drop its (always null) `battery_pack_type`, **left** join `broadcast(fleet)` on
   `vin_hash` (lines 16-20).
4. `groupBy(dtc_code, ecu, model_safe, battery_pack_type)` and aggregate (lines 21-28).
5. `fleet_pct_affected` (line 32) and `rank_in_ecu` (lines 30, 33).

## Nulls and edge cases

| Case | Spark result | Evidence |
|---|---|---|
| vehicle in telemetry but not in vehicle_state | left join keeps it; `battery_pack_type` null → its own group | 3 fixture vehicles, 17 events → 17 output rows with null pack |
| fleet denominator ≠ telemetry vehicles | uses vehicle_state row count (147 in fixture), not the 150 telemetry vehicles | fixture |
| empty vehicle_state | `x / 0` → null `fleet_pct_affected` (non-ANSI) | PySpark 3.5.1 probe |
| ties in `vehicles_affected` within an ECU | same rank, next rank not skipped (dense); e.g. 5 ADAS groups with 5 vehicles | fixture |
| `vehicles_affected` null in ranking | cannot happen (`countDistinct` is non-null); Spark `.desc` would sort nulls last | probe |
| `warning_state` null | counts 0 (`otherwise(0)`); `warning_on_events` is nullable int64 but never null for non-empty groups | fixture schema |
| duplicate VIN in vehicle_state | would duplicate telemetry rows and inflate `events` (not `vehicles_affected`); MERGE keeps one row per VIN (`spark_jobs/vehicle_state_job.py:136`) | 147 rows / 147 distinct VINs in fixture |
| `dtc_code`/`ecu` null | null group / rank partition | 0 in fixture |

## Spark behaviour hardest to reproduce in Rust

1. **`sha2(vin, 256)` inside the query** (line 13): must produce Spark's lower-case hex. DataFusion's
   `sha256` returns binary, so the port needs a UDF; `rust_pipeline/src/transforms/anonymized.rs:20-24`
   (`sha256_hex`) already matches Spark's T6 output. This is also a privacy-sensitive read of raw VINs.
2. **`dense_rank` result type int32** (fixture schema) vs DataFusion's unsigned 64-bit rank — needs an explicit
   cast or the schema check fails (`rust_pipeline/src/validate.rs:183-203`); plus DESC must be NULLS LAST.
3. **`countDistinct`** grouped with a nullable key, and the **null group key** for unmatched vehicles.
4. **Fleet size as a separate eager count** (line 14) — must be the full vehicle_state count, not the joined count.
5. **`to_timestamp` → timestamp[us, UTC]** for first/last seen (lines 26-27), and `round(…, 3)` HALF_UP (line 32).
6. Two inputs means the live check also depends on `vehicle_state_rust` matching Spark's `vehicle_state`
   (`rust_pipeline/src/analytics/mod.rs:86-87`, `README.md:141`).
