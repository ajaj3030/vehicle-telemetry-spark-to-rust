# `ota_rollout` functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala` (cited as `OtaRollout.scala:N`).
Rust target: `rust_pipeline/src/analytics/ota_rollout.rs` (stub, `:14-17`).

## Purpose
Latest OTA update state of each vehicle for each target software version, counted per
target version and model, with success and failure rates (`OtaRollout.scala:7`,
`README.md:78`). Shown on the dashboard OTA tab (`dashboard/app.py:714-730`).

## Inputs
`vehicle_telemetry` (`OtaRollout.scala:46-47`), rows with `event_type = 'OTA'` (`:32`).
Columns used: `vin_hash`, `target_software_version`, `sequence_number` (`:27-29`),
`update_state`, `model_safe` (`:37-38`). `model_safe` is the null-filled `model`
(`spark_jobs/transforms.py:69-79`); `vin_hash` is SHA-256 of the VIN (`spark_jobs/transforms.py:169-176`).

## Output schema
Table `analytics/ota_rollout`, overwritten each run, not partitioned
(`spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala:75-97`; no `partitionCols` override in `OtaRollout.scala`).
Key: `target_software_version, model_safe` (`rust_pipeline/src/analytics/ota_rollout.rs:10`).
Types from `rust_pipeline/tests/fixtures/expected/ota_rollout/`:

| Column | Type | Nullable |
|---|---|---|
| `target_software_version` | string | yes |
| `model_safe` | string | yes |
| `UPDATE_STATE_IN_PROGRESS` | long | no |
| `UPDATE_STATE_WAITING_ON_BCM` | long | no |
| `UPDATE_STATE_SUCCESS` | long | no |
| `UPDATE_SUCCESS_WITH_WARNINGS` | long | no |
| `UPDATE_STATE_FAILED` | long | no |
| `UPDATE_FAILED_DRIVE_ALLOWED` | long | no |
| `UPDATE_FAILED_NO_ACTION` | long | no |
| `vehicles` | long | no |
| `success_rate_pct` | double | yes |
| `failure_rate_pct` | double | yes |

## Calculation
1. Keep rows with `event_type = 'OTA'` (`OtaRollout.scala:32`).
2. Per `(vin_hash, target_software_version)`, number rows by `sequence_number` descending and keep row 1: the vehicle's newest event for that target version (`:27-34`).
3. Group the kept rows by `(target_software_version, model_safe)` and count rows per `update_state`, pivoting on the fixed list of 7 states, in that column order (`:11-19`, `:36-39`).
4. Replace null counts with 0 (`:40`).
5. `vehicles` = sum of the 7 state columns (`:23`, `:41`).
6. `success_rate_pct` = `round((SUCCESS + SUCCESS_WITH_WARNINGS) / vehicles * 100, 2)` (`:20`, `:24`, `:42`).
7. `failure_rate_pct` = `round((FAILED + FAILED_DRIVE_ALLOWED + FAILED_NO_ACTION) / vehicles * 100, 2)` (`:21`, `:24`, `:43`). `IN_PROGRESS` and `WAITING_ON_BCM` count in `vehicles` but in neither rate.

Example from the fixture: 2.6.1 / MODEL_GRAVITY has 16 success + 1 with warnings out of 22 vehicles, so `success_rate_pct = 77.27` (`rust_pipeline/tests/fixtures/expected/ota_rollout/`).

## Nulls and edge cases
- **Vehicle counted per target version:** a vehicle with events for several target versions is counted once under each (`:28`).
- **Latest row first, then filter by state:** if a vehicle's newest event has a null `update_state` or one outside the 7 values, it still wins step 2 and then adds 0 to every column, so the vehicle drops out of `vehicles` rather than falling back to an older state (`:31-41`). The generator emits only the 7 states (`data_generator/schemas.py:89-97`), and the fixture has no other values.
- **Group with no counted vehicles:** `vehicles = 0`, so both rates are `0 / 0`. Spark runs with default (non-ANSI) settings (`AnalyticsJob.scala:55-68` sets no `spark.sql.ansi.enabled`), so the result is null, not an error. Not covered by the fixture.
- **Null `target_software_version` or `model_safe`:** form their own group (Spark groups nulls together). `model_safe` is never null after T1 (`spark_jobs/transforms.py:69-79`). The fixture has neither.
- **Ties on `sequence_number`** for one vehicle and target version: `row_number` picks one row arbitrarily. Sequence numbers are per VIN (`config/app_config.yaml:20-22`); the fixture has no ties.
- **Null `sequence_number`:** descending order puts nulls last in Spark, so a null row only wins when all rows for that key are null.
- **Unused states never create columns:** the pivot list is fixed, so the schema does not depend on the data (`:38`).

## Spark behaviour hardest to reproduce in Rust
1. **`round` is HALF_UP on the decimal value of the double**, not on its binary value. The shared scalar `spark_round` already does this (`rust_pipeline/src/transforms/rounding.rs`); the job needs it as a DataFusion UDF (open PR #20).
2. **Pivot + `na.fill(0L)` schema:** the 7 count columns and `vehicles` must come out as non-null `Int64` with the exact state names. A `SUM(CASE ...)` rewrite produces nullable columns unless coalesced, and the parity check compares types (`rust_pipeline/src/validate.rs:185-212`).
3. **Integer division to double:** Spark `/` on two longs returns a double; Rust/DataFusion integer division must be cast first, and `0 / 0` must give null (non-ANSI).
4. **Latest-row selection** must match `row_number` on `sequence_number DESC NULLS LAST`, with ties left undefined.
