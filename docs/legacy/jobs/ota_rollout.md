# `ota_rollout`: functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala` (abbreviated `OtaRollout.scala`).
Rust target: `rust_pipeline/src/analytics/ota_rollout.rs` (currently a stub, `rust_pipeline/src/analytics/ota_rollout.rs:14-17`).

## Purpose

OTA rollout status per target software version and vehicle model. For each rollout it shows how many vehicles are in each update
state and the success and failure rates (`OtaRollout.scala:7`, `dashboard/app.py:192-198`). The dashboard's Fleet → OTA rollout view
reads it (`dashboard/app.py:714-731`).

## Inputs

| Table | Rows used | Columns used | Source |
|-------|-----------|--------------|--------|
| `vehicle_telemetry` (Spark) / `vehicle_telemetry_rust` (Rust) | `event_type = 'OTA'` | `vin_hash`, `target_software_version`, `sequence_number`, `update_state`, `model_safe` | `OtaRollout.scala:27-38`, `OtaRollout.scala:46-47` |

There is one input. `vehicle_state` is not read by this job.

## Output

Delta `analytics/ota_rollout`, overwritten on every run, **not partitioned** (`OtaRollout.scala:8-9`,
`spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala:86-91`). Key: `target_software_version, model_safe`
(`rust_pipeline/src/analytics/ota_rollout.rs:8-12`). The schema below, in column order, is from
`rust_pipeline/tests/fixtures/expected/ota_rollout/*.parquet`:

| # | Column | Type | Nullable | Meaning |
|---|--------|------|----------|---------|
| 1 | `target_software_version` | string | yes | Group key |
| 2 | `model_safe` | string | yes (never null in practice: T1 coalesces it, `spark_jobs/transforms.py:73`) | Group key |
| 3–9 | `UPDATE_STATE_IN_PROGRESS`, `UPDATE_STATE_WAITING_ON_BCM`, `UPDATE_STATE_SUCCESS`, `UPDATE_SUCCESS_WITH_WARNINGS`, `UPDATE_STATE_FAILED`, `UPDATE_FAILED_DRIVE_ALLOWED`, `UPDATE_FAILED_NO_ACTION` | int64 | **no** | Vehicles whose latest state for this target is that state (`OtaRollout.scala:11-19`) |
| 10 | `vehicles` | int64 | **no** | Sum of the seven state columns (`OtaRollout.scala:41`) |
| 11 | `success_rate_pct` | double | yes | `round((SUCCESS + SUCCESS_WITH_WARNINGS) / vehicles * 100, 2)` (`OtaRollout.scala:20,42`) |
| 12 | `failure_rate_pct` | double | yes | `round((FAILED + FAILED_DRIVE_ALLOWED + FAILED_NO_ACTION) / vehicles * 100, 2)` (`OtaRollout.scala:21,43`) |

The fixture has 6 rows: 3 target versions × 2 models.

## Calculation, step by step

1. Keep rows with `event_type = 'OTA'` (`OtaRollout.scala:32`).
2. For each `(vin_hash, target_software_version)`, number the rows by `sequence_number` **descending** with `row_number()` and keep
   row 1. This is each vehicle's latest event for that target (`OtaRollout.scala:27-34`). A vehicle that saw several targets is
   counted once per target.
3. Group by `(target_software_version, model_safe)`. Pivot on `update_state` over the **fixed** list of seven states, counting rows
   (`OtaRollout.scala:36-39`).
4. Replace null counts with `0` in the seven state columns (`OtaRollout.scala:40`).
5. `vehicles` = `0 + s1 + … + s7` (`OtaRollout.scala:23,41`).
6. Compute `success_rate_pct` and `failure_rate_pct`. Each is the sum of the relevant state columns, divided by `vehicles`, times 100,
   rounded to 2 dp (`OtaRollout.scala:24,42-43`).

## Nulls and edge cases

Checked with a local PySpark 3.5.1 run of the same expressions (not committed) unless a file is cited.

| Case | Spark result | In the fixture? |
|------|--------------|-----------------|
| `sequence_number` null on some rows | `desc` in Spark is `DESC NULLS LAST`, so a null never wins step 2 unless every row in the window is null | No (0 null sequence numbers in `input/telemetry`) |
| Ties on `sequence_number` within a window | `row_number` picks one row, and which row is not defined | No (no duplicate `(vin_hash, target_software_version, sequence_number)`) |
| `target_software_version` null | Becomes its own group (null key), and `model_safe` splits it as usual | No |
| `update_state` null, or a value not in the list | The vehicle still forms or joins its group, but adds to no state column. If every vehicle in a group is like this, the row is still emitted with all seven counts `0` and `vehicles = 0` | No: all 7 listed states appear and no others |
| `vehicles = 0` | Division by zero gives **null** for both rates (Spark's default non-ANSI mode). Counts stay `0`, not null | No |
| A rate with a .5 at the rounding digit | `round` is HALF_UP on the decimal value: `round(0.285, 2) = 0.29` | Fixture rates such as `77.27`, `4.55` |
| No OTA events at all | Empty output with the full schema | No |

Order of operations: Spark computes `numerator / vehicles * 100`, so it divides first. `1/3*100` is not the same double as `100/3`;
the result only matches after rounding, so keep Spark's order.

## Spark behaviour that is hardest to reproduce in Rust

1. **Pivot with a fixed value list plus `na.fill(0)`.** DataFusion has no `PIVOT`. The port needs seven conditional counts in
   Spark's column order, typed **non-null int64**. Analytics parity checks types (`rust_pipeline/src/analytics/mod.rs:36-45`). The
   port must also keep groups whose states are all null or unlisted, emitting them with zero counts. A `WHERE update_state IN (…)`
   filter would silently drop them.
2. **HALF_UP rounding.** DataFusion's `round(0.285, 2)` returns `0.28`, but Spark returns `0.29` (DataFusion 43 probe, not
   committed). `spark_round` reproduces Spark, but only as a scalar Rust function (`rust_pipeline/src/transforms/rounding.rs:1-28`).
   To use it in DataFusion it must be wrapped as a UDF or applied after `collect`.
3. **Division semantics.** In DataFusion, `int64 / int64` is integer division (`1/3 = 0`), and `x / 0` raises a divide-by-zero error.
   Even as doubles, `1.0/0` gives `inf`. Spark gives a double, and null for `/0` (DataFusion 43 probe). The port must cast to double
   and guard `vehicles = 0` with `NULLIF` or `CASE`.
4. **Null ordering.** DataFusion's `ORDER BY x DESC` puts nulls first, so `row_number() = 1` would pick a null `sequence_number`.
   Write `DESC NULLS LAST` explicitly (DataFusion 43 probe).
5. `row_number()` is `UInt64` in DataFusion and `int` in Spark. This only matters if the column leaks into the output; Spark's output
   has no `rn` column.

Not needed here: timestamps, `max_by`/`min_by`, range windows, joins, hashing, partitioned writes.
