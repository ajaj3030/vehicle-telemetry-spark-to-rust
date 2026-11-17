# `diagnostic_alerts`: functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala` (abbreviated `DiagnosticAlerts.scala`).
Rust target: `rust_pipeline/src/analytics/diagnostic_alerts.rs` (currently a stub, `rust_pipeline/src/analytics/diagnostic_alerts.rs:14-17`).

## Purpose

Fleet impact of each diagnostic trouble code, for service triage. For each DTC, ECU, model and battery pack it gives events,
vehicles affected, share of the fleet, first and last seen, and rank within the ECU (`DiagnosticAlerts.scala:7`,
`dashboard/app.py:199-205`). The Fleet → Diagnostics view reads it (`dashboard/app.py:732-760`).

## Inputs

| Table | Rows used | Columns used | Source |
|-------|-----------|--------------|--------|
| `vehicle_telemetry` / `vehicle_telemetry_rust` | `event_type = 'DIAGNOSTIC'` | `vin_hash`, `dtc_code`, `ecu`, `model_safe`, `warning_state`, `event_time`; its own `battery_pack_type` is dropped | `DiagnosticAlerts.scala:16-27` |
| `vehicle_state` / `vehicle_state_rust` | all rows | `vin` (**raw VIN**), `battery_pack_type`, and the row count | `DiagnosticAlerts.scala:13-14` |

This is the only analytics job with two inputs, and the only one that reads the raw VIN.

## Output

Delta `analytics/diagnostic_alerts`, overwritten, **not partitioned**. Key: `dtc_code, ecu, model_safe, battery_pack_type`
(`rust_pipeline/src/analytics/diagnostic_alerts.rs:8-12`). Schema from
`rust_pipeline/tests/fixtures/expected/diagnostic_alerts/*.parquet`; the fixture has 235 rows across 7 ECUs:

| # | Column | Type | Nullable | Definition (`DiagnosticAlerts.scala`) |
|---|--------|------|----------|------------|
| 1–4 | `dtc_code`, `ecu`, `model_safe`, `battery_pack_type` | string | yes | Group keys (`:21`) |
| 5 | `events` | int64 | **no** | `count(*)` (`:23`) |
| 6 | `vehicles_affected` | int64 | **no** | `countDistinct(vin_hash)` (`:24`) |
| 7 | `warning_on_events` | int64 | yes | `sum(when(warning_state = 'WARNING_ON', 1).otherwise(0))` (`:25`) |
| 8 | `first_seen` | timestamp[us, UTC] | yes | `min(to_timestamp(event_time))` (`:26`) |
| 9 | `last_seen` | timestamp[us, UTC] | yes | `max(to_timestamp(event_time))` (`:27`) |
| 10 | `fleet_pct_affected` | double | yes | `round(vehicles_affected / count(vehicle_state) * 100, 3)` (`:14,32`) |
| 11 | `rank_in_ecu` | int32 | **no** | `dense_rank()` over `ecu`, ordered by `vehicles_affected DESC` (`:30,33`) |

## Calculation, step by step

1. Build `fleet` = `(sha2(vin, 256) AS vin_hash, battery_pack_type)` from `vehicle_state` (`DiagnosticAlerts.scala:13`). This must
   produce the same lowercase hex hash as the stream's `vin_hash` (`spark_jobs/transforms.py:173`).
2. Set `fleetSize` = `vehicle_state.count()`, evaluated **eagerly** on the driver and used as a literal (`DiagnosticAlerts.scala:14`).
   It counts all state rows, not only vehicles that had diagnostics.
3. Filter `DIAGNOSTIC` rows, drop their `battery_pack_type`, and **left**-join the broadcast `fleet` on `vin_hash`
   (`DiagnosticAlerts.scala:17-20`). Diagnostic events carry no pack type (`data_generator/generator.py:108-113`).
4. Group by `(dtc_code, ecu, model_safe, battery_pack_type)` and compute aggregates 5–9 (`DiagnosticAlerts.scala:21-28`).
5. Compute `fleet_pct_affected`: `vehicles_affected / fleetSize` (long / long, which Spark evaluates as a **double**), times 100,
   rounded to 3 dp (`DiagnosticAlerts.scala:32`).
6. Compute `rank_in_ecu` = `dense_rank` within `ecu` by `vehicles_affected` descending. Ties share a rank and the next rank is not
   skipped (`DiagnosticAlerts.scala:30,33`).

## Nulls and edge cases

Checked with a local PySpark 3.5.1 probe unless a file is cited.

| Case | Spark result | In the fixture? |
|------|--------------|-----------------|
| Vehicle with telemetry but no `vehicle_state` row | Left join gives a null `battery_pack_type`, which becomes its own group | Yes. 3 vehicles have no state row, giving 17 output rows with a null `battery_pack_type` |
| Duplicate VIN rows in `vehicle_state` | Would duplicate events in the join and inflate `fleetSize`. Not possible in the MERGE-maintained table (`spark_jobs/vehicle_state_job.py:120-143`) | No (147 rows, 147 distinct VINs) |
| Ties on `vehicles_affected` within an ECU | Same `dense_rank` | Yes (for example ADAS rank 5 is shared by 7 rows) |
| `vin_hash` null | `countDistinct` ignores nulls, `count(*)` does not | No |
| `warning_state` null | Counts as 0 (`otherwise(0)`) | No |
| Empty `vehicle_state` | `fleetSize = 0`, so `fleet_pct_affected` is null (division by zero, non-ANSI) | No (147 rows, so the minimum `fleet_pct_affected` is `0.68` = 1/147) |
| Unparseable `event_time` | `to_timestamp` null; `min`/`max` ignore it | No |

## Spark behaviour that is hardest to reproduce in Rust

1. **The join through a hashed raw VIN.** DataFusion's `sha256()` returns `Binary`, not Spark's hex string (DataFusion 43 probe).
   The port needs hex encoding, or the existing `sha256_hex` (`rust_pipeline/src/transforms/anonymized.rs:20`) as a UDF. The raw
   `vin` column must never appear in the output (`rust_pipeline/src/validate.rs:19`).
2. **`fleetSize` as an eager scalar** over the whole state table, not over the joined rows. A port that counts after the join, or
   counts distinct joined VINs, gives a different denominator.
3. **Integer division.** In DataFusion, `int64 / int64` truncates (`1/147 = 0`) and fails on `/0`. Spark returns a double
   (DataFusion 43 probe).
4. **`dense_rank` type and null ordering.** DataFusion returns `UInt64` and sorts nulls first for `DESC`. Spark returns non-null
   `int32` (fixture schema) with `DESC NULLS LAST` (probe).
5. **Timestamps, as in `charging_sessions`:** parse ISO strings with `Z`, and output microsecond UTC timestamps.
6. **HALF_UP `round(…, 3)`**, as in the other jobs.
7. **Live input parity.** In production the port reads `vehicle_state_rust` (`rust_pipeline/src/analytics/mod.rs:86-87`), so its
   output depends on the state stream matching Spark (`rust-pipeline validate --state`, `rust_pipeline/src/main.rs:106-113`).
