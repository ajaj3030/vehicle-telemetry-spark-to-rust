# `diagnostic_alerts` functional spec

Measures the fleet impact of each diagnostic trouble code (DTC) by ECU, model and pack type for service triage. [DiagnosticAlerts.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala#L8-L34)

## Inputs and output

| Delta input | Fields used |
|---|---|
| `vehicle_telemetry` | `event_type`, `vin_hash`, `dtc_code`, `ecu`, `model_safe`, `warning_state` and `event_time`. The telemetry `battery_pack_type` is dropped before joining. |
| `vehicle_state` | Raw `vin` for SHA-256 join keys, `battery_pack_type` for grouping, and all rows for the fleet denominator. |

Overwrites unpartitioned Delta `analytics/diagnostic_alerts`. The output grain and Rust comparison key are `(dtc_code, ecu, model_safe, battery_pack_type)`. [DiagnosticAlerts.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala#L9-L41) · [AnalyticsJob.scala · paths, UTC session and Delta writer](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala#L42-L94) · [config/app_config.yaml](../../../config/app_config.yaml) · [Rust implementation and tests](../../../rust_pipeline/src/analytics/diagnostic_alerts.rs)

| Output column, in calculation order | Type |
|---|---|
| `dtc_code` | string |
| `ecu` | string |
| `model_safe` | string |
| `battery_pack_type` | string |
| `events` | bigint |
| `vehicles_affected` | bigint |
| `warning_on_events` | bigint |
| `first_seen` | timestamp (UTC, microseconds) |
| `last_seen` | timestamp (UTC, microseconds) |
| `fleet_pct_affected` | double |
| `rank_in_ecu` | int |

[Spark Parquet fixture](../../../rust_pipeline/tests/fixtures/expected/diagnostic_alerts) · [DiagnosticAlerts.scala · output expressions](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala#L20-L33)

## Calculation

1. Project each state row to `sha2(vin, 256) AS vin_hash` and `battery_pack_type`. Use the same lowercase hex hash as telemetry enrichment. Count all state rows eagerly and store the count as a literal denominator for the run. [DiagnosticAlerts.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala#L11-L14) · [transforms.py · VIN hash](../../../spark_jobs/transforms.py#L169-L176)
2. Keep `DIAGNOSTIC` telemetry. Drop its pack field and left join the projected state rows on `vin_hash` with a Spark broadcast hint. Model comes from telemetry. Pack comes from state. [DiagnosticAlerts.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala#L16-L21)
3. Group by DTC, ECU, model and pack. Count all joined rows as `events`, count distinct non-null hashes as `vehicles_affected`, and sum 1 only when `warning_state = 'WARNING_ON'`. Take the minimum and maximum parsed timestamps for first and last seen. [DiagnosticAlerts.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala#L21-L28)
4. Divide affected vehicles by the number of state rows, multiply by 100 and round HALF_UP to three decimals. Dense-rank by descending `vehicles_affected` within each ECU across all DTC, model and pack groups. Ties share a rank and subsequent ranks are consecutive. [DiagnosticAlerts.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala#L30-L33) · [rust_pipeline/src/analytics/spark_functions.rs](../../../rust_pipeline/src/analytics/spark_functions.rs)

## Nulls and edge cases

- A telemetry-only hash survives in a null-pack group and is included in `vehicles_affected`. A matched state row with null pack also lands in a null-pack group. A null telemetry hash cannot join and contributes to `events` but not `vehicles_affected`. Null DTCs, ECUs and models remain groupable. Null ECU values rank in the same window partition. [DiagnosticAlerts.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala#L16-L33) · [Rust implementation and tests](../../../rust_pipeline/src/analytics/diagnostic_alerts.rs)
- Every state row contributes to the denominator, including vehicles with no alerts. The join does not deduplicate state rows, so duplicate matching VIN rows can multiply events and warnings or place one VIN in multiple pack groups; the operational MERGE is designed to maintain one state row per VIN. The numerator can include hashes absent from state, so a percentage can exceed 100%. An empty state table still yields diagnostic groups and their percentages are null under the stock non-ANSI session. [DiagnosticAlerts.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala#L11-L33) · [vehicle_state_job.py · collapse and MERGE](../../../spark_jobs/vehicle_state_job.py#L104-L143) · [AnalyticsJob.scala · paths, UTC session and Delta writer](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala#L42-L94) · [Rust implementation and tests](../../../rust_pipeline/src/analytics/diagnostic_alerts.rs)
- Null warnings contribute zero. Parse failures become null timestamps under stock settings, are skipped by extrema and leave both timestamps null when there is no parseable event time. No diagnostic input yields no groups. [DiagnosticAlerts.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala#L16-L28) · [AnalyticsJob.scala · paths, UTC session and Delta writer](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala#L42-L94) · [rust_pipeline/src/analytics/spark_functions.rs](../../../rust_pipeline/src/analytics/spark_functions.rs)

## Hardest Spark behaviour to reproduce

Keep the left-join cardinality, count distinct hashes after the join, and derive the denominator from the same state input as the join. Reuse the streaming VIN hash and keep raw VIN out of the output. Match nullable timestamp parsing at UTC microsecond precision, guarded floating division and HALF_UP rounding. Cast dense rank to Spark's signed Int32 in the output. [DiagnosticAlerts.scala](../../../spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala#L11-L33) · [Rust implementation and tests](../../../rust_pipeline/src/analytics/diagnostic_alerts.rs) · [rust_pipeline/src/transforms/anonymized.rs](../../../rust_pipeline/src/transforms/anonymized.rs) · [rust_pipeline/src/analytics/spark_functions.rs](../../../rust_pipeline/src/analytics/spark_functions.rs) · [rust_pipeline/src/validate.rs](../../../rust_pipeline/src/validate.rs)
