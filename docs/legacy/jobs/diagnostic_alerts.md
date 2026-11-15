# diagnostic_alerts

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala`
(below, `DiagnosticAlerts.scala:N`). Rust placeholder: `rust_pipeline/src/analytics/diagnostic_alerts.rs`.
`AnalyticsJob.scala` and `BuildParityFixtures.scala` are in the same directory as the job.

## Purpose

Fleet impact of each diagnostic trouble code, for service triage
(`DiagnosticAlerts.scala:7`; `dashboard/app.py:201-206`). Shown on the Fleet
page's Diagnostics tab (`dashboard/app.py:732-748`).

## Inputs

- Enriched telemetry, rows with `event_type = 'DIAGNOSTIC'`
  (`DiagnosticAlerts.scala:17,38`): `vin_hash`, `dtc_code`, `ecu`,
  `model_safe`, `warning_state`, `event_time` (string).
- **`vehicle_state`** (`DiagnosticAlerts.scala:39`): `vin` (**raw**),
  `battery_pack_type`, and the total row count. It is the only analytics job
  that reads this table or any raw VIN.

## Output

`analytics/diagnostic_alerts`, Delta overwrite, unpartitioned
(`AnalyticsJob.scala:77,86-91`). One row per
`(dtc_code, ecu, model_safe, battery_pack_type)`. Types from
`rust_pipeline/tests/fixtures/expected/diagnostic_alerts/`.

| Column | Type | Definition |
|---|---|---|
| `dtc_code`, `ecu`, `model_safe`, `battery_pack_type` | string | group keys |
| `events` | bigint not null | `count(*)` (`:23`) |
| `vehicles_affected` | bigint not null | `countDistinct(vin_hash)` (`:24`) |
| `warning_on_events` | bigint | `sum(when(warning_state = 'WARNING_ON', 1).otherwise(0))` (`:25`) |
| `first_seen` | timestamp(µs, UTC) | `min(to_timestamp(event_time))` (`:26`) |
| `last_seen` | timestamp(µs, UTC) | `max(to_timestamp(event_time))` (`:27`) |
| `fleet_pct_affected` | double | `round(vehicles_affected / fleetSize * 100, 3)` (`:32`) |
| `rank_in_ecu` | **int** not null | `dense_rank()` per `ecu` by `vehicles_affected desc` (`:30,33`) |

## Calculation

1. `fleet` = `vehicle_state` projected to `(sha2(vin, 256) as vin_hash, battery_pack_type)` (`:13`).
2. `fleetSize` = `vehicle_state.count()`, evaluated **eagerly** while the plan
   is built and embedded as a literal (`:14`).
3. Keep DIAGNOSTIC events, drop telemetry's own `battery_pack_type`
   (diagnostic events do not carry one, `:18-19`), **left** broadcast join
   `fleet` on `vin_hash` (`:20`).
4. Group by `(dtc_code, ecu, model_safe, battery_pack_type)` and aggregate (`:21-28`).
5. `fleet_pct_affected` (`:32`), then `rank_in_ecu` (`:30,33`).

## Nulls and edge cases

- **Vehicle missing from `vehicle_state`:** the left join keeps its events
  with `battery_pack_type` null, forming separate null-pack groups. 3 of the
  150 fixture vehicles have no state row; 17 expected rows have null
  `battery_pack_type` (pyarrow on `rust_pipeline/tests/fixtures/`).
- **Join key hash must match T6 exactly:** lowercase hex SHA-256 of the
  UTF-8 VIN, the same as `spark_jobs/transforms.py:173` and
  `rust_pipeline/src/transforms/anonymized.rs:20`.
- **Duplicate VINs in `vehicle_state`** would multiply events and inflate
  `fleetSize`; the MERGE keeps one row per VIN (`spark_jobs/vehicle_state_job.py:120-143`).
- **`fleetSize` counts all state rows,** not only vehicles with diagnostics,
  and not telemetry vehicles missing from state. Empty `vehicle_state` → `/ 0`
  → null `fleet_pct_affected` (ANSI off, `AnalyticsJob.scala:56-64`).
- **`count(*)` vs `countDistinct`:** `countDistinct` ignores null
  `vin_hash`; `count(*)` does not.
- **`warning_on_events`:** null or other `warning_state` counts 0.
- **Ties:** `dense_rank` gives equal ranks without gaps (43 tied
  `(ecu, rank)` pairs in the fixture, max rank 9). Null `ecu` forms its own window partition.
- **Unparseable `event_time`:** null, skipped by `min`/`max`, as in charging.

## Hardest Spark behaviour to reproduce

1. **Two inputs and a raw-VIN table:** the port must read
   `vehicle_state_rust` (`rust_pipeline/src/analytics/mod.rs:84-87`) and
   reproduce `sha2(vin, 256)`; any difference in the hash or the state table
   silently moves rows into the null-pack group.
2. **Eager `count()` as a literal** — the fleet size is fixed before the
   job's query runs; the same in DataFusion requires a separate query.
3. **`dense_rank` output type `int`** (DataFusion returns UInt64) and
   **DESC NULLS LAST** ordering (DataFusion `DESC` defaults to NULLS FIRST).
   The parity check compares types (`rust_pipeline/src/validate.rs:92-94`).
4. **`to_timestamp(string)`** to µs UTC, null on failure (see charging).
5. **`round` HALF_UP** at 3 dp, divide-then-multiply order, null on zero division.
