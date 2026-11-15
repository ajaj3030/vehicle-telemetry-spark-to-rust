# `ota_rollout` — functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala` (cited below as `OtaRollout.scala`).
Rust target: `rust_pipeline/src/analytics/ota_rollout.rs` (stub, `:14-17`).

## Purpose

Rollout status of each OTA software version per model: how many vehicles' latest attempt at that
target is in each update state, and the success and failure rates (`OtaRollout.scala:1-7`, `README.md:69-78`).
README names OTA systems as table consumers (`README.md:6`); the dashboard shows it as the OTA rollout
view (`dashboard/app.py:714-731`).

## Inputs

| Table | Rows used | Columns read |
|---|---|---|
| `vehicle_telemetry` (`AnalyticsJob.scala:49`) | `event_type = 'OTA'` (`OtaRollout.scala:32`) | `vin_hash`, `target_software_version`, `sequence_number`, `model_safe`, `update_state` (`OtaRollout.scala:26-43`) |

No `vehicle_state`, no joins. `model_safe` is `coalesce(model, 'MODEL_UNKNOWN')` from the stream
(`spark_jobs/transforms.py:69-80`).

## Output schema

Table `<analytics base>/ota_rollout`, overwrite, **no partition columns** (`OtaRollout.scala:8-47`,
`rust_pipeline/src/analytics/ota_rollout.rs:8-12`). Parity key: `target_software_version`, `model_safe`
(`rust_pipeline/src/analytics/ota_rollout.rs:10`). Types from `rust_pipeline/tests/fixtures/expected/ota_rollout`:

| Column | Type | Meaning |
|---|---|---|
| `target_software_version` | string | Group key |
| `model_safe` | string | Group key |
| `UPDATE_STATE_IN_PROGRESS` | int64 | Vehicles whose latest attempt is in this state |
| `UPDATE_STATE_WAITING_ON_BCM` | int64 | 〃 |
| `UPDATE_STATE_SUCCESS` | int64 | 〃 |
| `UPDATE_SUCCESS_WITH_WARNINGS` | int64 | 〃 |
| `UPDATE_STATE_FAILED` | int64 | 〃 |
| `UPDATE_FAILED_DRIVE_ALLOWED` | int64 | 〃 |
| `UPDATE_FAILED_NO_ACTION` | int64 | 〃 |
| `vehicles` | int64 | Sum of the seven state columns |
| `success_rate_pct` | double | `round(succeeded / vehicles * 100, 2)` |
| `failure_rate_pct` | double | `round(failed / vehicles * 100, 2)` |

Column order is exactly as above (pivot emits states in the `UpdateStates` list order, `OtaRollout.scala:11-19`, `:38`).

## Calculation

1. Filter telemetry to `event_type == "OTA"` (`OtaRollout.scala:32`).
2. Window `partitionBy(vin_hash, target_software_version).orderBy(sequence_number.desc)`;
   `row_number()`; keep `rn == 1` — each vehicle's latest event per target version (`OtaRollout.scala:27-34`).
3. `groupBy(target_software_version, model_safe)` (`OtaRollout.scala:37`).
4. `pivot("update_state", UpdateStates).count()` with the fixed seven-value list (`OtaRollout.scala:11-19`, `:38-39`).
5. `na.fill(0L, UpdateStates)` (`OtaRollout.scala:40`).
6. `vehicles = Σ seven state columns` (`OtaRollout.scala:23`, `:41`).
7. `success_rate_pct = round((UPDATE_STATE_SUCCESS + UPDATE_SUCCESS_WITH_WARNINGS) / vehicles * 100, 2)`
   (`OtaRollout.scala:20`, `:24`, `:42`).
8. `failure_rate_pct = round((UPDATE_STATE_FAILED + UPDATE_FAILED_DRIVE_ALLOWED + UPDATE_FAILED_NO_ACTION) / vehicles * 100, 2)`
   (`OtaRollout.scala:21`, `:24`, `:43`). The failed list matches the stream's `FAILED_UPDATE_STATES`
   (`spark_jobs/transforms.py:33-37`) but is a separate copy.

## Nulls and edge cases

Behaviour below is Spark 3.5.1 (`spark_jobs/analytics/build.sbt:12`), checked on local PySpark 3.5.1.

| Case | Spark result |
|---|---|
| `update_state` outside the seven values, or null | The row still lands in its `(target, model)` group; it adds to no state column. A group made only of such rows has all-zero counts after step 5, `vehicles = 0`, and both rates **null** (long/long `/` returns null on divide-by-zero in non-ANSI mode). |
| `target_software_version` null | Null is its own window partition and its own output group. |
| `sequence_number` null | `DESC` sorts nulls **last**, so a null-sequence row is only picked when it is the only row for that vehicle/target. |
| Ties on `sequence_number` | `row_number` picks one arbitrarily (non-deterministic). |
| A vehicle on several targets | Counted once per target (partition includes the target). |
| A vehicle whose `model_safe` changed | Counted under the model on its latest row for that target. |
| Rounding | `round` is HALF_UP on the decimal value (e.g. `round(1.005, 2) = 1.01`); Rust must use `spark_round` (`rust_pipeline/src/transforms/rounding.rs`). |

Fixture coverage (`rust_pipeline/tests/fixtures/input/telemetry`, `rust_pipeline/tests/fixtures/expected/ota_rollout`):
587 OTA events, 148 VINs, 218 vehicle/target pairs, **6 output rows**; no zero-vehicle groups, null
keys, out-of-list states or sequence ties occur. The parity test therefore does not exercise the edge
cases above — a port needs its own unit tests for them.

## Hardest Spark behaviour to reproduce

1. **Fixed-list pivot that keeps non-matching groups.** A SQL `CASE`/`FILTER` count per state reproduces
   this only if the group-by runs over all OTA rows, not over rows pre-filtered to the seven states.
2. **Exact types.** Counts must be `Int64`, rates `Float64`, and the schema check is exact
   (`rust_pipeline/src/validate.rs:185-212`).
3. **Null-on-zero division** (DataFusion integer division by zero errors; cast to double and guard with `CASE`/`NULLIF`).
4. **`DESC` null ordering** — DataFusion `DESC` defaults to NULLS FIRST; specify NULLS LAST.
5. **HALF_UP rounding** of a double.
