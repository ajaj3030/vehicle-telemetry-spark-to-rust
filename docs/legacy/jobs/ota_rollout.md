# ota_rollout

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala`
(below, `OtaRollout.scala:N`). Rust placeholder: `rust_pipeline/src/analytics/ota_rollout.rs`.
`AnalyticsJob.scala` and `BuildParityFixtures.scala` are in the same directory as the job.

## Purpose

OTA rollout status per target software version and model: the latest update
state of each vehicle, counted per state, with success and failure rates
(`OtaRollout.scala:7`; `dashboard/app.py:194-199`). Shown on the Fleet page's
OTA tab (`dashboard/app.py:714-730`).

## Inputs

Enriched telemetry (`OtaRollout.scala:46-47`), rows with `event_type = 'OTA'`
(`OtaRollout.scala:32`). Columns: `vin_hash`, `target_software_version`,
`sequence_number`, `model_safe`, `update_state`.

## Output

`analytics/ota_rollout`, Delta overwrite, unpartitioned (`AnalyticsJob.scala:77,86-91`).
One row per `(target_software_version, model_safe)`.

| Column | Type | Null? |
|---|---|---|
| `target_software_version` | string | yes (group key) |
| `model_safe` | string | yes (group key) |
| `UPDATE_STATE_IN_PROGRESS` … `UPDATE_FAILED_NO_ACTION` (7 columns, order of `UpdateStates`, `OtaRollout.scala:11-19`) | bigint | no (`na.fill(0)`) |
| `vehicles` | bigint | no |
| `success_rate_pct` | double | yes |
| `failure_rate_pct` | double | yes |

Types from `rust_pipeline/tests/fixtures/expected/ota_rollout/`.

## Calculation

1. Keep OTA events (`OtaRollout.scala:32`).
2. Per `(vin_hash, target_software_version)`, number rows by
   `sequence_number` descending and keep row 1: each vehicle's latest event
   *for that target version* (`OtaRollout.scala:27-34`). A vehicle updating to
   two versions counts once in each.
3. Group by `(target_software_version, model_safe)` and pivot `update_state`
   over the fixed seven-value list, counting rows (`OtaRollout.scala:36-39`).
4. Replace null counts with 0 (`OtaRollout.scala:40`).
5. `vehicles` = sum of the seven counts, starting from `lit(0)` (`OtaRollout.scala:23,41`).
6. `success_rate_pct` = `round((SUCCESS + SUCCESS_WITH_WARNINGS) / vehicles * 100, 2)`;
   `failure_rate_pct` = same over the three failed states
   (`OtaRollout.scala:20-21,24,42-43`).

## Nulls and edge cases

- **Null `update_state` or a value outside the list:** the row survives step 2
  (it can be the latest) but matches no pivot column. If every latest row of a
  group is like that, the group is still emitted with seven zeros and
  `vehicles = 0`. (Spark pivot with explicit values aggregates per group, then
  projects the listed values.)
- **`vehicles = 0`:** `0 / 0` returns NULL with ANSI off (not set in
  `AnalyticsJob.scala:56-64`), so both rates are NULL, not NaN.
- **Null `sequence_number`:** Spark `desc` is NULLS LAST, so such a row is
  only picked when the vehicle has no non-null sequence for that version.
- **Ties on `sequence_number`:** `row_number` picks one arbitrarily; Spark
  is not deterministic here, so no port can match it "exactly". The pipeline
  assumes per-VIN sequence numbers are unique (`README.md` "Guarantees").
- **Null `target_software_version` / `model_safe`:** ordinary partition and
  group keys. `model_safe` is never null in practice (`spark_jobs/transforms.py:73`).
- **Fixture coverage:** 6 expected rows; no nulls, no out-of-list states, no
  sequence ties in the OTA input (checked with pyarrow on
  `rust_pipeline/tests/fixtures/`). None of the cases above is exercised.

## Hardest Spark behaviour to reproduce

1. **Pivot semantics** — fixed column set and order, groups with no matching
   values still emitted with zeros, counts non-null bigint. DataFusion has no
   `PIVOT` for DataFrames; the natural port is `SUM(CASE WHEN … THEN 1 ELSE 0 END)`,
   which gives the right zero rows for free but must produce non-null Int64.
2. **`round` HALF_UP on the decimal value**: DataFusion's `round` works on the
   binary double (`round(1.005, 2)` is 1.01 in Spark; the naive scaled round gives 1.0); `rust_pipeline/src/transforms/rounding.rs:1-6`
   documents and implements Spark's behaviour. Also divide *then* multiply by
   100, as Spark does, for bit-identical doubles.
3. **DESC NULLS LAST** — DataFusion `DESC` defaults to NULLS FIRST.
4. Null-on-zero division.
