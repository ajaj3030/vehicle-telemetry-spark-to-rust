# ota_rollout

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala`. Rust stub: `rust_pipeline/src/analytics/ota_rollout.rs`.
Fixture numbers below were measured with DuckDB over
`rust_pipeline/tests/fixtures/input/telemetry` and `rust_pipeline/tests/fixtures/expected/ota_rollout`.

## Purpose
How far each software version has rolled out per model, and how many
vehicles succeeded or failed: one row per (`target_software_version`,
`model_safe`) counting each vehicle's **latest** OTA state for that version
(`README.md:78`, `spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala:26-44`). Read by the dashboard Fleet page (`dashboard/app.py:714`).

## Inputs
| Table | Columns used | Cited in |
|---|---|---|
| `vehicle_telemetry` (Rust: `vehicle_telemetry_rust`) | `event_type`, `vin_hash`, `target_software_version`, `sequence_number`, `update_state`, `model_safe` | `spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala:28-38,46-47`, `rust_pipeline/src/analytics/mod.rs:84-87` |

OTA events are produced by `data_generator/generator.py:116` with states weighted per `data_generator/schemas.py` (`UPDATE_STATES_WEIGHTED`).

## Output schema
Unpartitioned (`spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala:77`), overwritten each run (`spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala:87-91`). Key: `target_software_version, model_safe` (`rust_pipeline/src/analytics/ota_rollout.rs:8-12`).

| # | Column | Spark type | Nullable | Source |
|---|---|---|---|---|
| 1 | `target_software_version` | string | yes | group key, `OtaRollout.scala:37` |
| 2 | `model_safe` | string | yes (never null in practice, `spark_jobs/transforms.py:73`) | group key, `:37` |
| 3–9 | `UPDATE_STATE_IN_PROGRESS`, `UPDATE_STATE_WAITING_ON_BCM`, `UPDATE_STATE_SUCCESS`, `UPDATE_SUCCESS_WITH_WARNINGS`, `UPDATE_STATE_FAILED`, `UPDATE_FAILED_DRIVE_ALLOWED`, `UPDATE_FAILED_NO_ACTION` | bigint | no (filled with 0) | `:11-19,38-40` |
| 10 | `vehicles` | bigint | no | `:41` |
| 11 | `success_rate_pct` | double | yes | `:42` |
| 12 | `failure_rate_pct` | double | yes | `:43` |

Column names are the raw state strings, in the order of `UpdateStates` (`:11-19`); the parity gate checks names and types exactly (`rust_pipeline/src/validate.rs:185-203`).
Fixture: 587 OTA events over 218 (vehicle, version) pairs give 6 output rows.

## Calculation
1. Keep `event_type = 'OTA'` (`:32`).
2. Number rows per (`vin_hash`, `target_software_version`) by `sequence_number` descending with `row_number`; keep `rn = 1` — the vehicle's latest event for that target version (`:27-34`). A vehicle that saw several versions contributes once per version.
3. Group by (`target_software_version`, `model_safe`) and pivot on `update_state` over the **fixed** 7-value list, counting rows (`:36-39`).
4. Replace null counts with `0L` in the 7 state columns only (`:40`).
5. `vehicles` = sum of the 7 state columns, folded from `lit(0)` (`:23,41`).
6. `success_rate_pct` = round((SUCCESS + SUCCESS_WITH_WARNINGS) / vehicles × 100, 2); `failure_rate_pct` = round((STATE_FAILED + FAILED_DRIVE_ALLOWED + FAILED_NO_ACTION) / vehicles × 100, 2) (`:20-24,42-43`). IN_PROGRESS and WAITING_ON_BCM count towards `vehicles` but neither rate.

Example from the fixture: 3.0.0 / MODEL_AIR has 26,5,24,7,4,2,0 → vehicles 68, success (24+7)/68 = 45.59, failure (4+2+0)/68 = 8.82.

## Nulls and edge cases
- **`update_state` null or not in the list:** the row survives step 2 (it can be the latest row) but matches no pivot value, so it is counted in no column and not in `vehicles` (`:38-41`). If every latest row of a group is like that, the group still appears with all zeros and `vehicles = 0`.
- **`vehicles = 0`:** `0 / 0` in Spark with ANSI mode off (the session sets no ANSI config, `spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala:55-68`) returns null, so both rates are null, not NaN.
- **Null `target_software_version`:** forms its own window partition and its own output group (Spark groups null keys). The parity key join is null-safe (`rust_pipeline/src/validate.rs:232-243`).
- **Ties on `sequence_number`** within a (vehicle, version): `row_number` picks an arbitrary row. `sequence_number` is strictly increasing per VIN at the producer (`data_generator/generator.py:150,154`), and the fixture has no duplicates.
- **Model change across events:** grouping uses `model_safe` from the latest row only (`:34,37`).
- Fixture coverage: no null `update_state` or `target_software_version`, no unknown states, `UPDATE_FAILED_NO_ACTION` is 0 in every row — so the null/zero paths above are **not** exercised by the parity test (`rust_pipeline/tests/fixtures/expected/ota_rollout`).

## Hardest Spark behaviour to reproduce
1. **`pivot(col, values)` + `na.fill(0L)`**: DataFusion has no pivot; it must be written as 7 conditional counts (`COUNT(*) FILTER` / `SUM(CASE…)`) producing **non-null bigint** columns with the exact uppercase names and order, plus zero-only groups for unknown states (`:38-40`).
2. **HALF_UP rounding** of the rates: Spark rounds via decimal; native float rounding differs on halves. The scalar helper `spark_round` exists (`rust_pipeline/src/transforms/rounding.rs:1-7`) but is not yet usable as a DataFusion expression.
3. **Division semantics**: long / long → double, and divide-by-zero → null (`:24`).

No timestamps, no partition columns, no `max_by`, one input — see `../migration-slices.md`.

## Open work touching this job
Open PR #6 (branch `demo-privacy-gate`) changes only `rust_pipeline/src/analytics/ota_rollout.rs` to add a raw `vin` column looked up from `vehicle_state`. That would add a second input and emit a column on the forbidden list (`rust_pipeline/src/validate.rs:19`), and is not in the Spark job (`spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala:36-43`). Resolve it before starting the OTA slice.
