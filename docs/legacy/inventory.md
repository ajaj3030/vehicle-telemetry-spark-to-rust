# Legacy pipeline inventory

Every job that produces, transforms or consumes pipeline data, in any language.
Citations are `path:line` (or `path:start-end`) relative to the repo root.
Spark-semantics claims cite the Spark version in `spark_jobs/analytics/build.sbt:12`
and were checked against a local PySpark 3.5.1 with default (non-ANSI) settings;
the repo never enables ANSI mode (`spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala:56-64`).
Fixture counts were measured on the committed Parquet under `rust_pipeline/tests/fixtures/`.

## Summary

| # | Job | Language | Kind | Inputs | Output | Deployed by |
|---|---|---|---|---|---|---|
| 1 | Data generator | Python | Producer | none (simulated fleet) | Kafka `vehicle_telemetry`, `vehicle_state_updates` | Manual `python -m data_generator.run` / dashboard |
| 2 | Telemetry stream (Spark) | PySpark | Streaming | Kafka `vehicle_telemetry` | Delta `vehicle_telemetry` | `spark-submit` on the Compose Spark cluster |
| 3 | Vehicle-state stream (Spark) | PySpark | Streaming | Kafka `vehicle_state_updates` | Delta `vehicle_state` | `spark-submit` on the Compose Spark cluster |
| 4 | Telemetry stream (Rust) | Rust | Streaming | Kafka `vehicle_telemetry` | Delta `vehicle_telemetry_rust` | Kubernetes Deployment `telemetry-stream` |
| 5 | Vehicle-state stream (Rust) | Rust | Streaming | Kafka `vehicle_state_updates` | Delta `vehicle_state_rust` | Kubernetes Deployment `vehicle-state-stream` |
| 6 | `battery_health_daily` | Scala Spark | Batch analytics | Delta `vehicle_telemetry` | Delta `analytics/battery_health_daily` | `spark-submit --class` (manual / dashboard) |
| 7 | `charging_sessions` | Scala Spark | Batch analytics | Delta `vehicle_telemetry` | Delta `analytics/charging_sessions` | `spark-submit --class` (manual / dashboard) |
| 8 | `ota_rollout` | Scala Spark | Batch analytics | Delta `vehicle_telemetry` | Delta `analytics/ota_rollout` | `spark-submit --class` (manual / dashboard) |
| 9 | `diagnostic_alerts` | Scala Spark | Batch analytics | Delta `vehicle_telemetry`, `vehicle_state` | Delta `analytics/diagnostic_alerts` | `spark-submit --class` (manual / dashboard) |
| 10 | `BuildParityFixtures` | Scala Spark | Test-data utility | Delta `vehicle_telemetry`, `vehicle_state` | Parquet `rust_pipeline/tests/fixtures/` | `spark-submit --class` (manual) |
| 11 | Rust analytics (`rust-pipeline analytics <job>`) | Rust | Batch analytics (stubs) | Delta `vehicle_telemetry_rust`, `vehicle_state_rust` | Delta `analytics/<job>_rust` | CLI / dashboard Migration page |
| 12 | Parity validator (`rust-pipeline validate`) | Rust | Verification | Spark and Rust Delta tables | JSON report | CLI / dashboard |
| 13 | Dashboard | Python (Streamlit) | Consumer / launcher | All Delta tables above | UI | Compose service `dashboard` |
| 14 | Demo data loader | Python | Loader | Parquet fixtures | Delta tables under `delta_output/` | Manual script |

Status: jobs 2–3 are ported to Rust as jobs 4–5; jobs 6–9 are not ported
(`README.md:55-65`). All four Rust analytics modules are stubs that `bail!`
(`rust_pipeline/src/analytics/ota_rollout.rs:14-17`,
`rust_pipeline/src/analytics/charging_sessions.rs:15-16`) with `#[ignore]`d
parity tests (`rust_pipeline/src/analytics/charging_sessions.rs:22`).

## Infrastructure the jobs share

| Item | Value | Source |
|---|---|---|
| Telemetry topic | `vehicle_telemetry`, keyed by VIN | `config/app_config.yaml`, `data_generator/generator.py:272` |
| State topic | `vehicle_state_updates`, keyed by VIN, `cleanup.policy=compact` | `config/app_config.yaml`, `docker-compose.yml:54-90`, `data_generator/generator.py:273` |
| Partitions / retention | 8 partitions, 7 days | `config/app_config.yaml`, `docker-compose.yml:54-90` |
| Bootstrap | `localhost:9092` (host), `kafka:29092` (Compose) | `config/app_config.yaml` |
| Spark runtime | `apache/spark:3.5.1` master + 2 workers | `docker-compose.yml:92-160` |
| Analytics output base | `/opt/delta_output/analytics` (cluster), `./delta_output/analytics` (local) | `config/app_config.yaml`, `spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala:45-53` |
| Rust image | `rust-pipeline` built from `rust_pipeline/Dockerfile` | `docker-compose.yml:162-178`, `deploy/k8s/telemetry-stream.yaml:35` |
| CI | Rust fmt/clippy/test (incl. parity tests), Python lint, `sbt package`, kubeconform, image build | `.github/workflows/ci.yml:35-86` |

---

## 1. Data generator

- **What:** simulates a fleet; for every telemetry event it also publishes a
  latest-state snapshot (`data_generator/generator.py:1-12`). Vehicles are
  sharded per producer thread so per-VIN Kafka ordering holds, and every event
  has a per-VIN `sequence_number` (`data_generator/generator.py:8-11`,
  `data_generator/generator.py:143-163`).
- **Inputs:** none.
- **Outputs:** Kafka `vehicle_telemetry` and `vehicle_state_updates`, both keyed by VIN
  (`data_generator/generator.py:207-211`, `data_generator/generator.py:272-273`).
  Event payload fields per type: `data_generator/generator.py:69-140`;
  schema reference `data_generator/schemas.py`, `README.md` (event schema section).
- **Edge cases it never produces:** null `charge_percent`/`battery_health_level`
  (`data_generator/generator.py:69-85`). The null cases in the fixtures come from
  the fixture data, not the generator (see job specs).
- **Deployment:** run manually (`README.md:173-175`) or from the dashboard
  Data Generator page (`dashboard/app.py:529`).

## 2. Telemetry stream — `spark_jobs/streaming_job.py`

- **What:** Structured Streaming job that parses telemetry JSON
  (`spark_jobs/streaming_job.py:83-115`), applies transforms T1–T7
  (`spark_jobs/transforms.py:190-200`), drops `kafka_key`
  (`spark_jobs/streaming_job.py:233`) and appends to Delta.
- **Inputs:** Kafka `vehicle_telemetry`, `startingOffsets=earliest`,
  `failOnDataLoss=false`, `maxOffsetsPerTrigger` from config
  (`spark_jobs/streaming_job.py:187-195`).
- **Output table:** Delta `vehicle_telemetry` (`spark.delta_output_path` in
  `config/app_config.yaml`), append with `mergeSchema=true`, partitioned by
  **`event_year`, `event_month`, `event_type`** (`spark_jobs/streaming_job.py:240-247`).
  Checkpointed, 10-second trigger (`spark_jobs/streaming_job.py:310-311`, `config/app_config.yaml`).
- **Columns and types** (types from `rust_pipeline/tests/fixtures/input/telemetry`, a
  row-subset of this table per `spark_jobs/analytics/src/main/scala/telemetry/analytics/BuildParityFixtures.scala:34-39`):

  | Group | Columns : type | Produced by |
  |---|---|---|
  | Envelope | `event_id`:string, `event_type`:string, `event_time`:string, `sequence_number`:int64 | `spark_jobs/streaming_job.py:83-115` |
  | Vehicle context | `model`, `model_variant`, `software_version`, `power_state`:string, `odometer_km`:double | `spark_jobs/streaming_job.py:83-115` |
  | Battery | `charge_percent`, `remaining_range`, `kwhr`, `capacity_kwhr`, `battery_health_level`, `max_cell_temp`, `min_cell_temp`:double, `preconditioning_status`, `battery_pack_type`:string | `spark_jobs/streaming_job.py:83-115` |
  | Charging | `charge_state`, `charge_session_id`:string, `charging_power_kw`, `energy_added_kwh`:double | `spark_jobs/streaming_job.py:83-115` |
  | Diagnostic | `ecu`, `dtc_code`, `warning_state`:string | `spark_jobs/streaming_job.py:83-115` |
  | OTA | `update_state`, `target_software_version`:string, `update_progress_percent`:int32 | `spark_jobs/streaming_job.py:83-115` |
  | Kafka metadata | `topic`:string, `kafka_partition`:int32, `kafka_offset`:int64, `kafka_timestamp`:timestamp[us, UTC] | `spark_jobs/streaming_job.py:187-233` |
  | T1 null handling | `model_safe`, `software_version_safe`, `power_state_safe`:string (`odometer_km` null→0.0) | `spark_jobs/transforms.py:69-80` |
  | T2 time | `event_date`:date, `event_year`, `event_month`, `event_day`, `event_hour`, `event_dayofweek`:int32, `is_weekend`:bool, `event_quarter`:int32, `date_str`:string | `spark_jobs/transforms.py:82-97` |
  | T3 battery | `soc_bucket`:string, `energy_pct`, `cell_temp_spread`, `degradation_pct`, `km_per_percent`:double | `spark_jobs/transforms.py:99-125` |
  | T4 health | `health_tier`:string, `thermal_alert`, `is_fault`, `needs_service`:bool, `severity_level`:int32 | `spark_jobs/transforms.py:127-157` |
  | T5 location | `lat_coarse`, `lon_coarse`:double (raw lat/lon dropped) | `spark_jobs/transforms.py:159-167` |
  | T6 anonymisation | `vin_hash`:string (SHA-256 of VIN), `vin_wmi`:string (raw VIN dropped) | `spark_jobs/transforms.py:169-177` |
  | T7 event | `row_id`:string, `sw_major`, `sw_minor`:int32 | `spark_jobs/transforms.py:179-188` |

- **Deployment:** `spark-submit` inside the Compose `spark-master` container
  (`README.md:176-184`, `dashboard/app.py:338-348`); not in Kubernetes
  (`deploy/k8s/kustomization.yaml:9-13`).

## 3. Vehicle-state stream — `spark_jobs/vehicle_state_job.py`

- **What:** collapses each micro-batch to the newest snapshot per VIN
  (`row_number` over `sequence_number DESC`), summing `event_count` and taking the
  minimum `first_seen_at` (`spark_jobs/vehicle_state_job.py:104-117`), then MERGEs on
  `vin`, updating only when `source.sequence_number > target.sequence_number`
  (`spark_jobs/vehicle_state_job.py:120-143`). Null VINs are filtered first (see `run_vehicle_state`,
  `spark_jobs/vehicle_state_job.py:146-207`).
- **Inputs:** Kafka `vehicle_state_updates`, `startingOffsets=earliest`, `failOnDataLoss=false`
  (`spark_jobs/vehicle_state_job.py:160-172`).
- **Output table:** Delta `vehicle_state` (`vehicle_state.delta_output_path` in
  `config/app_config.yaml`), one row per VIN. First write is overwrite partitioned by
  **`model`** (`spark_jobs/vehicle_state_job.py:125`); later writes MERGE.
- **Columns and types** (`spark_jobs/vehicle_state_job.py:58-73`,
  `rust_pipeline/tests/fixtures/input/vehicle_state`): `vin`:string, `model`:string,
  `model_variant`:string, `battery_pack_type`:string, `software_version`:string,
  `power_state`:string, `odometer_km`:double, `charge_percent`:double,
  `battery_health_level`:double, `last_event_type`:string, `sequence_number`:int64,
  `updated_at`:string, `event_count`:int32, `first_seen_at`:string.
  Note: this table **holds the raw VIN**, unlike telemetry.
- **Deployment:** as job 2 (`README.md:176-184`).

## 4. Telemetry stream — Rust (`rust-pipeline stream`)

- **What:** Kafka consumer → same T1–T7 transforms
  (`rust_pipeline/src/consumer.rs:25-115`, `rust_pipeline/src/transforms/mod.rs:10-20`),
  checkpointed offsets in SQLite (`rust_pipeline/src/checkpoint.rs`).
- **Inputs:** Kafka `vehicle_telemetry` (`rust_pipeline/src/consumer.rs:40-48`).
- **Output:** Delta `vehicle_telemetry_rust` (`rust_pipeline/src/config.rs:89-93`), same
  columns as job 2, partitioned by `event_year`, `event_month`, `event_type`
  (`rust_pipeline/src/consumer.rs:33-37`). Parity with job 2 is checked by
  `rust-pipeline validate`, ignoring `kafka_timestamp` (`rust_pipeline/src/validate.rs:36-42`).
- **Deployment:** Kubernetes Deployment, 1 replica, `Recreate`, args `stream`
  (`deploy/k8s/telemetry-stream.yaml:14-36`, `README.md:148-156`); also runnable in the Compose
  `rust-pipeline` container, which idles with `sleep infinity` (`docker-compose.yml:162-178`).

## 5. Vehicle-state stream — Rust (`rust-pipeline state`)

- **What:** same collapse and conditional MERGE as job 3 (`rust_pipeline/src/vehicle_state.rs:1-8`).
- **Inputs:** Kafka `vehicle_state_updates`.
- **Output:** Delta `vehicle_state_rust` (`rust_pipeline/src/config.rs:101-105`), job 3's columns,
  partitioned by `model` (`rust_pipeline/src/vehicle_state.rs:25-45`).
- **Deployment:** Kubernetes Deployment, 1 replica, `Recreate`, args `state`
  (`deploy/k8s/vehicle-state-stream.yaml:14-36`).

## 6–9. Scala analytics jobs

Common contract (`spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala`):
session timezone UTC (`:63`); reads Delta paths from config (`:45-53`); writes one Delta table at
`<analytics base>/<job>` with `mode("overwrite")`, `overwriteSchema=true` and the job's
`partitionCols` (`:80-91`). One jar, `telemetry-analytics_2.12-1.0.0.jar`, Scala 2.12 / Spark 3.5.1
(`spark_jobs/analytics/build.sbt:1-17`).

**Deployment:** `sbt package` then `spark-submit --class telemetry.analytics.<Job>` on the Compose
cluster (`README.md:67-93`), or from the dashboard Pipeline Jobs page, which uses the class names in
`dashboard/app.py:178-205` and the command in `dashboard/app.py:338-348`. There is **no scheduler**
for them in the repo: Kubernetes only deploys the two Rust stream processors
(`deploy/k8s/kustomization.yaml:9-13`), and CI only compiles the jar (`.github/workflows/ci.yml:55-65`).

| Job | Inputs | Output columns : type | Partition cols | Spec |
|---|---|---|---|---|
| `battery_health_daily` | `vehicle_telemetry` (`event_type='BATTERY'`) | `vin_hash`:string, `event_date`:date, `model_safe`:string, `readings`:int64, `avg_charge_percent`:double, `min_charge_percent`:double, `peak_cell_temp`:double, `avg_cell_temp_spread`:double, `thermal_alert_count`:int64, `health_level_end_of_day`:double, `peak_cell_temp_3d`:double, `health_change`:double | `event_date` | [jobs/battery_health_daily.md](jobs/battery_health_daily.md) |
| `charging_sessions` | `vehicle_telemetry` (`event_type='CHARGING'`) | `charge_session_id`:string, `vin_hash`:string, `model_safe`:string, `session_start`:timestamp[us, UTC], `session_end`:timestamp[us, UTC], `events`:int64, `energy_added_kwh`:double, `peak_power_kw`:double, `soc_start`:double, `soc_end`:double, `end_state`:string, `had_fault`:bool, `duration_minutes`:double, `avg_power_kw`:double, `session_date`:date | `session_date` | [jobs/charging_sessions.md](jobs/charging_sessions.md) |
| `ota_rollout` | `vehicle_telemetry` (`event_type='OTA'`) | `target_software_version`:string, `model_safe`:string, 7 × update-state count:int64, `vehicles`:int64, `success_rate_pct`:double, `failure_rate_pct`:double | none | [jobs/ota_rollout.md](jobs/ota_rollout.md) |
| `diagnostic_alerts` | `vehicle_telemetry` (`event_type='DIAGNOSTIC'`), `vehicle_state` | `dtc_code`:string, `ecu`:string, `model_safe`:string, `battery_pack_type`:string, `events`:int64, `vehicles_affected`:int64, `warning_on_events`:int64, `first_seen`:timestamp[us, UTC], `last_seen`:timestamp[us, UTC], `fleet_pct_affected`:double, `rank_in_ecu`:int32 | none | [jobs/diagnostic_alerts.md](jobs/diagnostic_alerts.md) |

Types are from the committed Spark outputs `rust_pipeline/tests/fixtures/expected/<job>`;
partition columns from each object's `partitionCols` and the Rust registry
(`rust_pipeline/src/analytics/mod.rs:49-54`, `scripts/load_demo_data.py:35-40`).

## 10. `BuildParityFixtures` (Scala utility)

- **What:** samples the first N (default 150) `vin_hash` values in sort order from telemetry,
  writes them and the matching state rows (joined via `sha2(vin, 256)`) as Parquet inputs, then runs
  every job's `summarize` on the re-read inputs and writes expected outputs
  (`spark_jobs/analytics/src/main/scala/telemetry/analytics/BuildParityFixtures.scala:19-61`,
  `spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala:9-17`).
- **Outputs:** `rust_pipeline/tests/fixtures/input/{telemetry,vehicle_state}` and
  `rust_pipeline/tests/fixtures/expected/<job>` (single Parquet file each, `BuildParityFixtures.scala:27-28`).
  Committed fixtures: 5,763 telemetry rows over 150 VINs; 147 state rows.
- **Deployment:** manual `spark-submit --class telemetry.analytics.BuildParityFixtures` (`README.md:124-137`).

## 11. Rust analytics (`rust-pipeline analytics <job>`)

- **What:** registry of the four jobs with parity keys and partition columns
  (`rust_pipeline/src/analytics/mod.rs:49-54`), dispatch by name (`:70-79`). Every job is a stub.
- **Inputs:** registers **both** `telemetry` (Rust telemetry table) and `vehicle_state`
  (Rust state table) for every job (`rust_pipeline/src/analytics/mod.rs:82-87`).
- **Output:** `<analytics base>/<job>_rust` (`rust_pipeline/src/config.rs:225-232`), overwrite,
  partitioned like Spark (`rust_pipeline/src/analytics/mod.rs:97-110`, test at `:121-135`).
- **Deployment:** CLI or dashboard Migration page (`dashboard/app.py:501-512`); not in Kubernetes.

## 12. Parity validator (`rust-pipeline validate`)

Compares a Spark and a Rust table on key columns, null-safe join, float tolerance `1e-9`, forbidden
PII columns `vin`, `latitude`, `longitude`, `kafka_key`; for analytics it also requires identical
column names and Arrow types (`rust_pipeline/src/validate.rs:19-49`, `:85-245`). The per-job unit
test `assert_matches_spark` runs a job on the fixtures (`rust_pipeline/src/analytics/parity.rs:22-45`).

## 13. Dashboard — `dashboard/app.py`

- Reads all Delta tables (`dashboard/app.py:137-147`), launches Spark jobs (`:338-348`), renders
  Fleet views per analytics output (`:662-760`), and runs/validates Rust ports (`:501-512`).
- Treats a job as ported when its `.rs` file has no `#[ignore]` (`dashboard/app.py:461-468`).
- Column contract it depends on: see `dependencies.md`.
- **Deployment:** Compose service `dashboard` (`docker-compose.yml:181-207`).

## 14. Demo data loader — `scripts/load_demo_data.py`

Writes fixture inputs and expected Spark outputs into local Delta tables with the analytics partition
map (`scripts/load_demo_data.py:35-65`). Run manually.

## Supporting modules (not jobs)

- `spark_jobs/metrics_bridge.py`, `spark_jobs/spark_metrics_collector.py`: metrics files for the
  dashboard, imported by both Spark stream jobs (`spark_jobs/streaming_job.py`, `spark_jobs/vehicle_state_job.py`).
- `rust_pipeline/src/metrics.rs`: Rust equivalent (`rust_pipeline/src/vehicle_state.rs:12`).

## Downstream consumers outside the repo

The README states service, warranty and OTA systems read the Delta tables (`README.md:6`); none of
their code is in this repository, so their column dependencies are unknown. Treat every analytics
output's name, columns, types and partitioning as a public contract.
