# `ota_rollout`

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala` (below `OtaRollout.scala`). Runner: `spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala` (below `AnalyticsJob.scala`).

## Purpose

For each target software version and model, how many vehicles are in each OTA update state (taking each vehicle's latest state for that target), and the success and failure rates (`OtaRollout.scala:26-43`). Shown on the dashboard's OTA view (`dashboard/app.py:714-730`).

## Inputs

- `vehicle_telemetry` Delta table, rows with `event_type = 'OTA'` (`OtaRollout.scala:32,46-47`). Columns used: `vin_hash`, `target_software_version`, `sequence_number`, `model_safe`, `update_state` (`OtaRollout.scala:28-38`).
- Rust equivalent: table `telemetry`, registered from `vehicle_telemetry_rust` (`rust_pipeline/src/analytics/mod.rs:84-85`).

## Output schema

Delta `<analytics.output_base>/ota_rollout`, overwrite, unpartitioned (`AnalyticsJob.scala:51-52,77,87-91`). Types from Spark's fixture output `rust_pipeline/tests/fixtures/expected/ota_rollout/` (6 rows):

| Column | Type | Nullable in practice |
|--------|------|----------------------|
| `target_software_version` | string | yes (null target forms its own group) |
| `model_safe` | string | no (`spark_jobs/transforms.py:69-73` coalesces to `MODEL_UNKNOWN`) |
| `UPDATE_STATE_IN_PROGRESS` | int64 | no (filled with 0) |
| `UPDATE_STATE_WAITING_ON_BCM` | int64 | no |
| `UPDATE_STATE_SUCCESS` | int64 | no |
| `UPDATE_SUCCESS_WITH_WARNINGS` | int64 | no |
| `UPDATE_STATE_FAILED` | int64 | no |
| `UPDATE_FAILED_DRIVE_ALLOWED` | int64 | no |
| `UPDATE_FAILED_NO_ACTION` | int64 | no |
| `vehicles` | int64 | no |
| `success_rate_pct` | double | yes (when `vehicles = 0`) |
| `failure_rate_pct` | double | yes (when `vehicles = 0`) |

Column order is the order above (`OtaRollout.scala:11-19,37-43`). Rust parity key columns are declared in `rust_pipeline/src/analytics/ota_rollout.rs:8-12` (`target_software_version`, `model_safe`).

## Calculation

1. Keep rows where `event_type = 'OTA'` (`OtaRollout.scala:32`).
2. Within each `(vin_hash, target_software_version)`, number rows by `sequence_number` descending with `row_number()` and keep `rn = 1`: each vehicle's latest OTA event for that target (`OtaRollout.scala:27-34`). A vehicle with events for two targets is counted once under each.
3. Group by `(target_software_version, model_safe)` and pivot `update_state` over the **fixed** list of seven states, counting rows per state (`OtaRollout.scala:11-19,37-39`).
4. Replace null pivot counts with `0` (`OtaRollout.scala:40`).
5. `vehicles` = sum of the seven state columns (`OtaRollout.scala:23,41`).
6. `success_rate_pct = round((UPDATE_STATE_SUCCESS + UPDATE_SUCCESS_WITH_WARNINGS) / vehicles * 100, 2)` (`OtaRollout.scala:20,24,42`).
7. `failure_rate_pct = round((UPDATE_STATE_FAILED + UPDATE_FAILED_DRIVE_ALLOWED + UPDATE_FAILED_NO_ACTION) / vehicles * 100, 2)` (`OtaRollout.scala:21,24,43`). `UPDATE_STATE_IN_PROGRESS` and `UPDATE_STATE_WAITING_ON_BCM` count towards `vehicles` but neither rate.

## Nulls and edge cases

Behaviours below marked *(probe)* were confirmed against `pyspark==3.5.1` with `spark.sql.session.timeZone=UTC`, matching `AnalyticsJob.scala:63` and `spark_jobs/analytics/build.sbt`.

- **`update_state` null or not in the list:** the latest row still wins step 2, but the pivot counts it in no column, so that vehicle is not in `vehicles` at all *(probe)*. It is not counted from an older, valid state either.
- **Group whose latest rows are all null/unknown states:** the group still appears, with seven zeros and `vehicles = 0`; both rates are **null**, because Spark's non-ANSI `x / 0` returns null *(probe)*.
- **`target_software_version` null:** null is a valid partition key in step 2 and a valid grouping key in step 3, so a null-target row is produced *(probe)*.
- **Ties on `sequence_number`** within a `(vin_hash, target)`: `row_number` picks one arbitrarily (`OtaRollout.scala:27-29` has no tie-breaker). The fixture has no ties (checked against `rust_pipeline/tests/fixtures/input/telemetry/`).
- **Integer division:** `/` on two int64 columns yields double in Spark, so `1/3*100` is `33.33`, not `0` (`OtaRollout.scala:24`).
- **No OTA rows at all:** zero output rows (the pivot over a fixed list still defines all columns).
- The fixture has 0 OTA rows with null `update_state` or null `target_software_version` and 7 distinct states (`rust_pipeline/tests/fixtures/input/telemetry/`), so none of the null paths above are covered by the parity test.

## Hardest Spark behaviour to reproduce in Rust

1. **Pivot over a fixed list with `na.fill(0)`.** DataFusion has no `pivot`; it must be written as seven `SUM(CASE WHEN update_state = '<state>' THEN 1 ELSE 0 END)` (or `COUNT(... )` with a `COALESCE`), in the list's order, typed int64. States outside the list, including null, must fall into no column.
2. **Null rate when `vehicles = 0`.** DataFusion's float division by zero yields `NaN`/`inf`, and integer division by zero is an error, so the port must cast to double and return null explicitly when `vehicles = 0`.
3. **HALF_UP `round(…, 2)`.** Must use the Spark-compatible helper (`rust_pipeline/src/transforms/rounding.rs:1-28`), not DataFusion's `round`; see `../dependencies.md`.
4. **`row_number` with ties** is non-deterministic in Spark; a deterministic Rust tie-break matches Spark only when there are no ties.
