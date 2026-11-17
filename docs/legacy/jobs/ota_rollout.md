# `ota_rollout` — functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala`
(cited below as `OtaRollout.scala:<line>`). Rust stub:
`rust_pipeline/src/analytics/ota_rollout.rs`.

## Purpose

OTA rollout status per target software version and model: for every vehicle,
its latest update state for each target version, counted per state, with
success and failure rates (`OtaRollout.scala:7`, `dashboard/app.py:193-199`).
Shown on the dashboard's OTA Rollout tab (`dashboard/app.py:714-730`).
`README.md:5-7` says downstream service, warranty and OTA systems read the
pipeline's Delta tables; which of them reads this one is not recorded in the repo.

## Inputs

| Table | Columns used | Filter |
|-------|--------------|--------|
| enriched telemetry (`AnalyticsJob.scala:49`; Rust registers it as `telemetry`, `rust_pipeline/src/analytics/mod.rs:84-85`) | `event_type`, `vin_hash`, `target_software_version`, `sequence_number`, `update_state`, `model_safe` | `event_type = 'OTA'` (`OtaRollout.scala:32`) |

No `vehicle_state`, no timestamps.

## Output

Table `analytics/ota_rollout`, overwritten every run, **not partitioned**
(`OtaRollout.scala:9`, `AnalyticsJob.scala:77`, `AnalyticsJob.scala:86-91`).
One row per (`target_software_version`, `model_safe`), the parity key
(`rust_pipeline/src/analytics/ota_rollout.rs:8-12`). Column order and types
(from `rust_pipeline/tests/fixtures/expected/ota_rollout/`):

| # | Column | Type | Definition |
|---|--------|------|------------|
| 1 | `target_software_version` | string | group key |
| 2 | `model_safe` | string | group key (T1 default `MODEL_UNKNOWN`, `spark_jobs/transforms.py:73`) |
| 3 | `UPDATE_STATE_IN_PROGRESS` | bigint | vehicles whose latest state is this |
| 4 | `UPDATE_STATE_WAITING_ON_BCM` | bigint | 〃 |
| 5 | `UPDATE_STATE_SUCCESS` | bigint | 〃 |
| 6 | `UPDATE_SUCCESS_WITH_WARNINGS` | bigint | 〃 |
| 7 | `UPDATE_STATE_FAILED` | bigint | 〃 |
| 8 | `UPDATE_FAILED_DRIVE_ALLOWED` | bigint | 〃 |
| 9 | `UPDATE_FAILED_NO_ACTION` | bigint | 〃 |
| 10 | `vehicles` | bigint | sum of columns 3–9 |
| 11 | `success_rate_pct` | double | (5 + 6) / `vehicles` × 100, rounded to 2 dp |
| 12 | `failure_rate_pct` | double | (7 + 8 + 9) / `vehicles` × 100, rounded to 2 dp |

The state columns and their order come from the fixed list
`OtaRollout.scala:11-19`; success and failure sets from `OtaRollout.scala:20-21`.
The dashboard finds the state columns by the `UPDATE_` prefix
(`dashboard/app.py:715`), so their names are part of the contract.

## Calculation, step by step

1. Keep OTA events: `event_type = 'OTA'` (`OtaRollout.scala:32`).
2. Latest event per vehicle **per target version**: `row_number()` over
   `PARTITION BY vin_hash, target_software_version ORDER BY sequence_number DESC`,
   keep `rn = 1` (`OtaRollout.scala:27-34`). The helper column `rn` does not
   reach the output, because the next `groupBy` keeps only its keys.
3. Group by `target_software_version`, `model_safe` and pivot `update_state`
   over the 7 listed values, counting rows (`OtaRollout.scala:36-39`).
4. Replace null counts with 0 in the 7 state columns (`OtaRollout.scala:40`).
5. `vehicles` = sum of the 7 state columns, folded from `lit(0)`
   (`OtaRollout.scala:23`, `OtaRollout.scala:41`).
6. `success_rate_pct` = `round(succeeded / vehicles * 100, 2)`;
   `failure_rate_pct` = `round(failed / vehicles * 100, 2)`
   (`OtaRollout.scala:24`, `OtaRollout.scala:42-43`).

## Nulls and edge cases

Results marked *probe* were checked with local PySpark 3.5.1 (the version in
`spark_jobs/analytics/build.sbt:13`); the probe is not committed.

| Case | Spark result | In the fixture? |
|------|--------------|-----------------|
| A vehicle has events for several target versions | counted once **in each** version's row, because the window partitions by version (`OtaRollout.scala:28`) | yes: 58 vehicles have >1 target version |
| Latest event for a (vehicle, version) has `update_state` null or a value outside the list | the event still wins `rn = 1`, then the pivot has no column for it, so the vehicle is **dropped from every count**, even if an earlier event had a listed state (*probe*) | no: all 587 OTA events have a listed state |
| Every vehicle in a group is in that situation | the group still produces a row, all 7 counts 0 after `na.fill`, `vehicles = 0` (*probe*) | no |
| `vehicles = 0` | `x / 0` is null (Spark non-ANSI), so both rates are **null**, not 0 or NaN (*probe*) | no |
| `target_software_version` null | null is one window partition and one group key; parity matches null keys null-safely (`rust_pipeline/src/validate.rs:232-243`) | no: 0 null versions |
| `model_safe` | never null after T1 (`spark_jobs/transforms.py:73`) | — |
| Two events with the same `sequence_number` for one (vehicle, version) | `row_number` picks one arbitrarily (non-deterministic) | no: sequence numbers are unique per VIN (`data_generator/generator.py:150-154`) |
| Rate rounding | HALF_UP on the decimal value of the double, e.g. `round(1.005, 2) = 1.01` (*probe*; see `rust_pipeline/src/transforms/rounding.rs:1-6`) | yes (e.g. 45.59, 8.82) |
| Integer division | `bigint / bigint` is a double division in Spark | yes |

Fixture output (6 rows) covers versions 2.5.2, 2.6.1 and 3.0.0 for both models;
`UPDATE_FAILED_NO_ACTION` is 0 in every row
(`rust_pipeline/tests/fixtures/expected/ota_rollout/`). The null-state and
zero-vehicle cases above are not in the fixture, so a port needs unit tests for
them with the Spark results listed here.

## Spark behaviour hardest to reproduce in Rust

1. **Pivot with a fixed value list.** DataFusion 43 (the engine in
   deltalake 0.22, `rust_pipeline/Cargo.toml:13`) rejects SQL `PIVOT` as not
   implemented (checked with the `datafusion==43.1.0` Python bindings). The
   port must expand it by hand as 7 `SUM(CASE WHEN update_state = '…' THEN 1 ELSE 0 END)`
   columns in the order of `OtaRollout.scala:11-19`. This also reproduces
   `na.fill(0)` and keeps all-zero groups (step 3–4) as long as the `CASE`
   has `ELSE 0` and the null-state rows are not filtered out before grouping.
2. **Division semantics.** In DataFusion `bigint / bigint` is integer division
   (`7/147*100 = 0`), and float division by zero gives `inf`. Spark gives a
   double and null. The port must cast to double and return null when
   `vehicles = 0`.
3. **`round` HALF_UP.** DataFusion's `round(1.005, 2)` returns `1.0`; Spark
   returns `1.01`. A Spark-compatible round exists only as a scalar Rust
   function (`rust_pipeline/src/transforms/rounding.rs:7-28`), not as a
   DataFusion function, so it has to be wrapped as a UDF (or applied to the
   collected batches).
4. **Output types.** The counts must be `Int64` and the rates `Float64`
   to pass the type check (`rust_pipeline/src/validate.rs:185-203`).

## Open PR touching this job

[PR #6](https://github.com/ajaj3030/vehicle-telemetry-spark-to-rust/pull/6)
(branch `demo-privacy-gate`) changes only `rust_pipeline/src/analytics/ota_rollout.rs`
to add the raw `vin` of failed vehicles from `vehicle_state`. That breaks the
Spark contract (no `vin` column in `OtaRollout.scala`) and fails the privacy
check, since `vin` is in `FORBIDDEN_PII_COLUMNS` (`rust_pipeline/src/validate.rs:19`,
`rust_pipeline/src/analytics/mod.rs:42`). It should be closed or rescoped
before the `ota_rollout` port lands.
