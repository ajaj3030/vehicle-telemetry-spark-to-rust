# `diagnostic_alerts` — functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala`
(cited as `DiagnosticAlerts.scala:<line>`). Rust stub:
`rust_pipeline/src/analytics/diagnostic_alerts.rs`.

## Purpose

Fleet impact of each diagnostic trouble code, for service triage
(`DiagnosticAlerts.scala:7`): events and vehicles affected per DTC, ECU, model
and battery pack, share of the fleet, and rank within the ECU
(`dashboard/app.py:200-206`). Shown on the dashboard's Diagnostics tab
(`dashboard/app.py:732-748`).

## Inputs

This is the only analytics job with two inputs (`DiagnosticAlerts.scala:36-40`).

| Table | Columns used | Filter |
|-------|--------------|--------|
| enriched telemetry (`AnalyticsJob.scala:49`) | `event_type`, `vin_hash`, `dtc_code`, `ecu`, `model_safe`, `warning_state`, `event_time` | `event_type = 'DIAGNOSTIC'` (`DiagnosticAlerts.scala:17`) |
| `vehicle_state` (`AnalyticsJob.scala:50`) | `vin` (raw), `battery_pack_type`; and its row count | none |

Diagnostic events carry no pack type, so the telemetry `battery_pack_type` is
dropped and taken from `vehicle_state` (`DiagnosticAlerts.scala:18-20`). In the
Rust CLI this second input is `vehicle_state_rust` (`rust_pipeline/src/analytics/mod.rs:86-87`),
which is already validated against Spark's table (`rust_pipeline/src/validate.rs:44-50`).

## Output

Table `analytics/diagnostic_alerts`, overwritten every run, not partitioned
(`DiagnosticAlerts.scala:9`). Parity key `dtc_code`, `ecu`, `model_safe`,
`battery_pack_type` (`rust_pipeline/src/analytics/diagnostic_alerts.rs:8-12`).
Types from `rust_pipeline/tests/fixtures/expected/diagnostic_alerts/`:

| Column | Type | Definition |
|--------|------|------------|
| `dtc_code` | string | group key |
| `ecu` | string | group key |
| `model_safe` | string | group key |
| `battery_pack_type` | string | group key, from `vehicle_state`; null if the vehicle has no state row |
| `events` | bigint | `count(*)` |
| `vehicles_affected` | bigint | `count(DISTINCT vin_hash)` |
| `warning_on_events` | bigint | events with `warning_state = 'WARNING_ON'` |
| `first_seen` | timestamp (µs, UTC) | `min(to_timestamp(event_time))` |
| `last_seen` | timestamp (µs, UTC) | `max(to_timestamp(event_time))` |
| `fleet_pct_affected` | double | `round(vehicles_affected / fleet_size × 100, 3)` |
| `rank_in_ecu` | **int (32-bit)** | `dense_rank()` per `ecu` by `vehicles_affected` desc |

## Calculation, step by step

1. `fleet` = `vehicle_state` projected to `sha2(vin, 256) AS vin_hash`,
   `battery_pack_type` (`DiagnosticAlerts.scala:12-13`). `sha2` returns
   lowercase hex, matching the T6 hash in telemetry (`spark_jobs/transforms.py:173`).
2. `fleet_size` = `vehicle_state.count()`, evaluated eagerly when the plan is
   built and used as a literal (`DiagnosticAlerts.scala:14`).
3. Filter DIAGNOSTIC events, drop their `battery_pack_type`, **left** join
   `fleet` on `vin_hash` with a broadcast hint (`DiagnosticAlerts.scala:16-20`).
4. Group by `dtc_code`, `ecu`, `model_safe`, `battery_pack_type` and aggregate
   (`DiagnosticAlerts.scala:21-28`).
5. `fleet_pct_affected` (`DiagnosticAlerts.scala:32`) and `rank_in_ecu` over
   `PARTITION BY ecu ORDER BY vehicles_affected DESC` (`DiagnosticAlerts.scala:30`,
   `DiagnosticAlerts.scala:33`). The rank spans all models and pack types within an ECU.

## Nulls and edge cases

*Probe* = checked with local PySpark 3.5.1 (not committed).

| Case | Spark result | In the fixture? |
|------|--------------|-----------------|
| Vehicle has diagnostic events but no `vehicle_state` row | kept by the left join with `battery_pack_type` null; its events form separate null-pack groups | yes: 3 vehicles without a state row; 17 output rows with null pack |
| Denominator | `fleet_size` counts **every** state row, including vehicles with no diagnostics, and excludes vehicles that are only in telemetry, which still count in the numerator | yes: 147 state rows vs 150 telemetry vehicles |
| `vehicle_state` empty | `fleet_size = 0`, `x / 0` null, `fleet_pct_affected` null (*probe*) | no |
| Ties in `vehicles_affected` within an ECU | same rank, next rank +1 (dense) (*probe*) | yes: e.g. BMS has 36 rows over 6 ranks |
| `ecu` null | one rank partition for null; null key matched null-safely by parity (`rust_pipeline/src/validate.rs:232-243`) (*probe*) | no: 0 null `ecu`/`dtc_code` |
| `warning_state` null | not `WARNING_ON`, counts 0 | no |
| Duplicate VIN in `vehicle_state` | would duplicate the joined events; the MERGE keeps one row per VIN (`spark_jobs/vehicle_state_job.py:136-142`) | no |
| `event_time` unparseable | null, ignored by min/max (*probe*) | no |

## Spark behaviour hardest to reproduce in Rust

1. **Hashing the raw VIN for the join.** In DataFusion 43, `sha256(vin)`
   returns `Binary`, not hex (checked with the `datafusion==43.1.0` Python
   bindings); the port must hex-encode in lowercase (e.g. `encode(sha256(vin), 'hex')`)
   or reuse the stream's hasher `sha256_hex` (`rust_pipeline/src/transforms/anonymized.rs:20-24`). The raw `vin` must not leak into the
   output (`rust_pipeline/src/validate.rs:19`).
2. **Two-input semantics.** Left join (not inner), and a denominator taken from
   the full state table, not from the joined rows (step 2).
3. **Types.** `dense_rank()` is `UInt64` in DataFusion 43 vs Spark's 32-bit int,
   and `bigint / bigint` is integer division in DataFusion (`7 / 147 × 100 = 0`).
   Both fail parity unless cast (`rust_pipeline/src/validate.rs:185-203`).
4. **Timestamps** parsed from the `event_time` string must be microsecond
   UTC, as in [charging_sessions](charging_sessions.md#spark-behaviour-hardest-to-reproduce-in-rust).
5. **`round` HALF_UP** at 3 dp (`rust_pipeline/src/transforms/rounding.rs:1-28`).
