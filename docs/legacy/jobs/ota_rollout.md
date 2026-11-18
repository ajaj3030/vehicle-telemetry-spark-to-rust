# `ota_rollout` functional spec

Tracks the latest OTA result for each vehicle and target version, then reports status counts and success and failure rates by target version and model. Results describe the telemetry snapshot available to the batch run. [OtaRollout.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala#L8-L47) · [dashboard/app.py · Fleet](../../../dashboard/app.py#L714-L731)

## Inputs and output

Reads enriched Delta `vehicle_telemetry` columns `event_type`, `vin_hash`, `target_software_version`, `sequence_number`, `model_safe` and `update_state`. `vin_hash` and `model_safe` come from stream enrichment. Writes the unpartitioned Delta table `analytics/ota_rollout` by full overwrite. The output grain and Rust comparison key are `(target_software_version, model_safe)`. [OtaRollout.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala#L26-L47) · [AnalyticsJob.scala · paths, UTC session and Delta writer](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala#L42-L94) · [config/app_config.yaml](../../../config/app_config.yaml) · [spark_jobs/transforms.py](../../../spark_jobs/transforms.py) · [Rust implementation and tests](../../../rust_pipeline/src/analytics/ota_rollout.rs)

| Output column, in calculation order | Type |
|---|---|
| `target_software_version` | string |
| `model_safe` | string |
| `UPDATE_STATE_IN_PROGRESS` | bigint |
| `UPDATE_STATE_WAITING_ON_BCM` | bigint |
| `UPDATE_STATE_SUCCESS` | bigint |
| `UPDATE_SUCCESS_WITH_WARNINGS` | bigint |
| `UPDATE_STATE_FAILED` | bigint |
| `UPDATE_FAILED_DRIVE_ALLOWED` | bigint |
| `UPDATE_FAILED_NO_ACTION` | bigint |
| `vehicles` | bigint |
| `success_rate_pct` | double |
| `failure_rate_pct` | double |

[Spark Parquet fixture](../../../rust_pipeline/tests/fixtures/expected/ota_rollout) · [OtaRollout.scala · status columns and expressions](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala#L11-L44)

## Calculation

1. Filter to `event_type = 'OTA'`. Use `row_number()` partitioned by `(vin_hash, target_software_version)` and ordered by `sequence_number DESC NULLS LAST`. Keep row 1. A vehicle can contribute to multiple target versions. For a single target it contributes to the model on that selected row. [OtaRollout.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala#L26-L34)
2. Group the selected events by `(target_software_version, model_safe)`. Pivot onto the seven listed states, count rows per state and fill missing pivot counts with zero. Sum those seven counts as `vehicles`. [OtaRollout.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala#L11-L23) · [OtaRollout.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala#L36-L43)
3. For success, sum `UPDATE_STATE_SUCCESS` and `UPDATE_SUCCESS_WITH_WARNINGS`. For failure, sum `UPDATE_STATE_FAILED`, `UPDATE_FAILED_DRIVE_ALLOWED` and `UPDATE_FAILED_NO_ACTION`. For each, divide by `vehicles` as a double, multiply by 100 and round HALF_UP to two decimal places. The in-progress and waiting states contribute to the denominator only, so the rates can sum to less than 100%. [OtaRollout.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala#L20-L24) · [OtaRollout.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala#L36-L43) · [rust_pipeline/src/analytics/spark_functions.rs](../../../rust_pipeline/src/analytics/spark_functions.rs)

## Nulls and edge cases

- A null `event_type` fails the filter. A null target version forms a bucket of its own. A null VIN hash groups with other null hashes for the same target. A null sequence sorts after populated sequences. A tie for first, including an all-null sequence partition, has no deterministic tiebreaker. [OtaRollout.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala#L26-L37)
- A null or unlisted state on the latest event contributes to none of the seven counts, even when an older event had a listed state. A group with only these latest states survives with `vehicles = 0` and both rates null under the stock non-ANSI session. Empty OTA input yields no groups. [OtaRollout.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala#L23-L44) · [AnalyticsJob.scala · paths, UTC session and Delta writer](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala#L42-L94) · [Rust implementation and tests](../../../rust_pipeline/src/analytics/ota_rollout.rs)

## Hardest Spark behaviour to reproduce

The explicit pivot preserves groups whose latest states are all outside the list. Conditional counts must run over all latest rows, retain the fixed output column names and order, and return Int64. Filtering to recognised states before selecting the latest row changes which observation wins. The Rust port uses conditional aggregation, explicit null ordering and the shared Spark division and rounding helpers. [OtaRollout.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala#L26-L44) · [Rust implementation and tests](../../../rust_pipeline/src/analytics/ota_rollout.rs) · [rust_pipeline/src/analytics/spark_functions.rs](../../../rust_pipeline/src/analytics/spark_functions.rs)
