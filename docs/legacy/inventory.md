# Legacy pipeline inventory

Every job and long-running component in the pipeline, as it exists on `main`.
Paths are relative to the repo root and cited as `path:line`. Output types for
the analytics tables come from the Spark-written fixtures in
`rust_pipeline/tests/fixtures/expected/<job>/` (written by
`spark_jobs/analytics/src/main/scala/telemetry/analytics/BuildParityFixtures.scala:59-63`).

Scala paths below are shortened to `analytics/<File>.scala`, meaning
`spark_jobs/analytics/src/main/scala/telemetry/analytics/<File>.scala`.

## Summary

| # | Job | Language / engine | Kind | Reads | Writes | Deployed by |
|---|-----|-------------------|------|-------|--------|-------------|
| 1 | Data generator | Python (kafka-python) | producer | — | Kafka `vehicle_telemetry`, `vehicle_state_updates` | local venv or dashboard |
| 2 | Telemetry stream (Spark) | PySpark Structured Streaming | streaming | Kafka `vehicle_telemetry` | Delta `vehicle_telemetry` | `spark-submit` on the docker-compose Spark cluster |
| 3 | Vehicle state MERGE (Spark) | PySpark Structured Streaming + Delta MERGE | streaming | Kafka `vehicle_state_updates` | Delta `vehicle_state` | `spark-submit` on the docker-compose Spark cluster |
| 4 | Telemetry stream (Rust) | Rust (rdkafka, delta-rs) | streaming | Kafka `vehicle_telemetry` | Delta `vehicle_telemetry_rust` | Kubernetes Deployment `telemetry-stream` |
| 5 | Vehicle state MERGE (Rust) | Rust (rdkafka, delta-rs) | streaming | Kafka `vehicle_state_updates` | Delta `vehicle_state_rust` | Kubernetes Deployment `vehicle-state-stream` |
| 6 | `battery_health_daily` | Scala Spark 3.5.1 | batch analytics | Delta `vehicle_telemetry` | Delta `analytics/battery_health_daily` | `spark-submit --class` (manual / dashboard) |
| 7 | `charging_sessions` | Scala Spark 3.5.1 | batch analytics | Delta `vehicle_telemetry` | Delta `analytics/charging_sessions` | `spark-submit --class` (manual / dashboard) |
| 8 | `ota_rollout` | Scala Spark 3.5.1 | batch analytics | Delta `vehicle_telemetry` | Delta `analytics/ota_rollout` | `spark-submit --class` (manual / dashboard) |
| 9 | `diagnostic_alerts` | Scala Spark 3.5.1 | batch analytics | Delta `vehicle_telemetry`, `vehicle_state` | Delta `analytics/diagnostic_alerts` | `spark-submit --class` (manual / dashboard) |
| 10 | `BuildParityFixtures` | Scala Spark 3.5.1 | batch, test tooling | Delta `vehicle_telemetry`, `vehicle_state` | Parquet fixtures under `rust_pipeline/tests/fixtures/` | `spark-submit --class` (manual) |
| 11 | Rust analytics ports | Rust (DataFusion via deltalake 0.22) | batch analytics | Delta `vehicle_telemetry_rust`, `vehicle_state_rust` | Delta `analytics/<job>_rust` | CLI only; all four are stubs |
| 12 | Parity validation | Rust (DataFusion) | batch check | a Spark table + its Rust twin | `logs/validation_report_<table>.json` | CLI, CI tests, dashboard |
| 13 | Dashboard | Python (Streamlit) | UI / orchestrator | all Delta tables, JSON sidecars | starts/stops jobs | docker-compose `dashboard` or local Streamlit |
| 14 | Demo data loader | Python (delta-rs) | one-off batch | Parquet fixtures | all Delta tables except `*_rust` analytics | local venv |

Infrastructure: Kafka + ZooKeeper, the Spark master and two workers, and a
`rust-pipeline` container are defined in `docker-compose.yml:3-178`. Only the
two Rust stream processors have Kubernetes manifests
(`deploy/k8s/kustomization.yaml:9-13`).

## Kafka topics

| Topic | Key | Partitions | Retention / cleanup | Producer | Consumers |
|-------|-----|------------|---------------------|----------|-----------|
| `vehicle_telemetry` | VIN | 8 | 7 days, delete | data generator (`data_generator/generator.py:272`) | jobs 2 and 4 |
| `vehicle_state_updates` | VIN | 8 | 7 days, **compacted** | data generator (`data_generator/generator.py:273-274`) | jobs 3 and 5 |

Topic names: `config/app_config.yaml:8-11`. Creation and config:
`docker-compose.yml:64-81` (compaction at `docker-compose.yml:81`).
Event field groups by `event_type`: `README.md:229-249`, schema at
`spark_jobs/streaming_job.py:83-115`.

---

## 1. Data generator

- **What it does:** simulates a fleet (`config/app_config.yaml:24`, 2,000
  vehicles) and publishes one telemetry event plus one state snapshot per
  step (`data_generator/generator.py:266-275`). Vehicles are sharded across
  producer threads so each VIN has one producer, with
  `max_in_flight_requests_per_connection=1` (`data_generator/generator.py:8-9`,
  `data_generator/generator.py:255`). `sequence_number` increases by one per
  event per VIN (`data_generator/generator.py:150-154`).
- **Inputs:** none (config `generator:` section, `config/app_config.yaml:15-26`).
- **Outputs:** Kafka `vehicle_telemetry` (JSON event, `data_generator/generator.py:161-183`)
  and `vehicle_state_updates` (JSON snapshot, `data_generator/generator.py:186-203`).
  Sidecar `logs/generator_metrics.json` (`dashboard/metrics.py:6`).
- **Deployed:** `python -m data_generator.run` from a venv (`README.md:173-174`)
  or from the dashboard's Data Generator page (`dashboard/app.py:868-905`).

## 2. Telemetry stream — Spark (`spark_jobs/streaming_job.py`)

- **What it does:** Kafka → `from_json` → enrichment T1–T7 → drop
  `kafka_key` → append to Delta, one micro-batch per trigger
  (`spark_jobs/streaming_job.py:186-216`, `spark_jobs/streaming_job.py:225-247`).
  The transforms live in `spark_jobs/transforms.py:190-201` (T1 null
  defaults, T2 time dimensions, T3 battery metrics, T4 health flags, T5 GPS
  coarsening, T6 VIN hashing, T7 row id / software version parts).
- **Inputs:** Kafka `vehicle_telemetry`, `startingOffsets=earliest`,
  `maxOffsetsPerTrigger=500000` (`spark_jobs/streaming_job.py:186-195`,
  `config/app_config.yaml:42`).
- **Output:** Delta `vehicle_telemetry` at `/opt/delta_output/vehicle_telemetry`
  (`config/app_config.yaml:36`), mode `append`, `mergeSchema=true`,
  **partitioned by `event_year`, `event_month`, `event_type`**
  (`spark_jobs/streaming_job.py:240-247`). Checkpoint
  `/opt/checkpoints/vehicle_telemetry` (`config/app_config.yaml:34`,
  `spark_jobs/streaming_job.py:310`). Trigger every 10 s
  (`config/app_config.yaml:32`).
- **Columns and types** (Spark side, from
  `rust_pipeline/tests/fixtures/input/telemetry/part-00000-edge-cases.snappy.parquet`;
  the Rust schema with nullability is `rust_pipeline/src/schema.rs:152-216`):

  | Group | Columns (type) |
  |-------|----------------|
  | Envelope | `event_id` string, `event_type` string, `event_time` **string** (ISO-8601 with `Z`, `data_generator/generator.py:58-59`), `sequence_number` bigint |
  | Raw context | `model`, `model_variant`, `software_version`, `power_state` string; `odometer_km` double |
  | BATTERY | `charge_percent`, `remaining_range`, `kwhr`, `capacity_kwhr`, `battery_health_level`, `max_cell_temp`, `min_cell_temp` double; `preconditioning_status`, `battery_pack_type` string |
  | CHARGING | `charge_state`, `charge_session_id` string; `charging_power_kw`, `energy_added_kwh` double |
  | DIAGNOSTIC | `ecu`, `dtc_code`, `warning_state` string |
  | OTA | `update_state`, `target_software_version` string; `update_progress_percent` int |
  | Kafka metadata | `topic` string, `kafka_partition` int, `kafka_offset` bigint, `kafka_timestamp` timestamp (Rust writes epoch-millis bigint, ignored by validation: `rust_pipeline/src/validate.rs:39-40`) |
  | T1 | `model_safe`, `software_version_safe`, `power_state_safe` string (`spark_jobs/transforms.py:69-79`) |
  | T2 | `event_date` date; `event_year`, `event_month`, `event_day`, `event_hour`, `event_dayofweek`, `event_quarter` int; `is_weekend` boolean; `date_str` string (`spark_jobs/transforms.py:82-96`) |
  | T3 | `soc_bucket` string; `energy_pct`, `cell_temp_spread`, `degradation_pct`, `km_per_percent` double (`spark_jobs/transforms.py:99-124`) |
  | T4 | `health_tier` string; `thermal_alert`, `is_fault`, `needs_service` boolean; `severity_level` int (`spark_jobs/transforms.py:127-156`) |
  | T5 | `lat_coarse`, `lon_coarse` double (raw lat/lon dropped, `spark_jobs/transforms.py:159-166`) |
  | T6 | `vin_hash` string (SHA-256 hex), `vin_wmi` string (raw VIN dropped, `spark_jobs/transforms.py:169-176`) |
  | T7 | `row_id` string; `sw_major`, `sw_minor` int (`spark_jobs/transforms.py:179-187`) |

- **Deployed:** `spark-submit` inside the `spark-master` container with
  `--py-files transforms.py` (`README.md:177-180`, `dashboard/app.py:251-261`,
  `dashboard/app.py:337-347`). Writes metrics sidecars via
  `spark_jobs/metrics_bridge.py` and `spark_jobs/spark_metrics_collector.py`
  (`spark_jobs/streaming_job.py:60-75`).

## 3. Vehicle state MERGE — Spark (`spark_jobs/vehicle_state_job.py`)

- **What it does:** collapses each micro-batch to one row per VIN (newest
  snapshot by `sequence_number`, `event_count` summed, earliest
  `first_seen_at`) and MERGEs it into a one-row-per-VIN table, updating only
  when `source.sequence_number > target.sequence_number`
  (`spark_jobs/vehicle_state_job.py:104-143`). Known limit documented at
  `spark_jobs/vehicle_state_job.py:15-17`.
- **Inputs:** Kafka `vehicle_state_updates` (`spark_jobs/vehicle_state_job.py:160-172`).
- **Output:** Delta `vehicle_state` at `/opt/delta_output/vehicle_state`
  (`config/app_config.yaml:56`), **partitioned by `model`** on first write
  (`spark_jobs/vehicle_state_job.py:125`). Columns (`spark_jobs/vehicle_state_job.py:58-73`;
  fixture types in `rust_pipeline/tests/fixtures/input/vehicle_state/`):
  `vin` string (raw VIN, by design: `README.md:111-113`), `model`,
  `model_variant`, `battery_pack_type`, `software_version`, `power_state`
  string; `odometer_km`, `charge_percent`, `battery_health_level` double;
  `last_event_type` string; `sequence_number` bigint; `event_count` int;
  `updated_at`, `first_seen_at` string.
- **Deployed:** `spark-submit` in `spark-master` (`README.md:181-183`,
  `dashboard/app.py:262-273`).

## 4. Telemetry stream — Rust (`rust-pipeline stream`)

- **What it does:** port of job 2: same transforms
  (`rust_pipeline/src/transforms/mod.rs:13`), manual partition assignment,
  offsets committed to SQLite after each Delta write
  (`rust_pipeline/src/consumer.rs:25-60`).
- **Inputs:** Kafka `vehicle_telemetry`, consumer group
  `rust-vehicle-telemetry-consumer` (`config/app_config.yaml:74`).
- **Output:** Delta `vehicle_telemetry_rust` (`config/app_config.yaml:77`),
  partitioned by `event_year`, `event_month`, `event_type`
  (`rust_pipeline/src/consumer.rs:33-37`); schema `rust_pipeline/src/schema.rs:152-216`.
- **Deployed:** Kubernetes Deployment, 1 replica, `Recreate`
  (`deploy/k8s/telemetry-stream.yaml:1-36`), config from the ConfigMap
  `deploy/k8s/app_config.yaml`, PVC `telemetry-stream-data`
  (`deploy/k8s/storage.yaml:4-12`). Image built from
  `rust_pipeline/Dockerfile` (`.github/workflows/ci.yml:75-89`).

## 5. Vehicle state MERGE — Rust (`rust-pipeline state`)

- **What it does:** port of job 3 with the same collapse and
  sequence-number guard (`rust_pipeline/src/vehicle_state.rs:1-8`).
- **Inputs:** Kafka `vehicle_state_updates`, group
  `rust-vehicle-state-consumer` (`config/app_config.yaml:85`).
- **Output:** Delta `vehicle_state_rust` (`config/app_config.yaml:81`),
  partitioned by `model` (`rust_pipeline/src/vehicle_state.rs:124`); schema
  `rust_pipeline/src/schema.rs:222-239`.
- **Deployed:** Kubernetes Deployment `vehicle-state-stream`, 1 replica,
  `Recreate` (`deploy/k8s/vehicle-state-stream.yaml:1-36`).

## 6–9. Scala batch analytics jobs

Shared runtime for all four (`analytics/AnalyticsJob.scala`):

- One object per job extends `AnalyticsJob`; `main` parses `--config`,
  `--local` (`analytics/AnalyticsJob.scala:9-28`), builds a session with
  Delta extensions, `spark.sql.shuffle.partitions=8` and
  **`spark.sql.session.timeZone=UTC`** (`analytics/AnalyticsJob.scala:56-67`,
  `config/app_config.yaml:41`).
- Input paths: telemetry `spark.delta_output_path`, state
  `vehicle_state.delta_output_path` (`analytics/AnalyticsJob.scala:49-50`).
  Output: `analytics.output_base/<job>` (`analytics/AnalyticsJob.scala:51-52`,
  `config/app_config.yaml:61`).
- Write: Delta, **mode `overwrite` with `overwriteSchema=true`** (full
  rebuild every run), optionally `partitionBy`
  (`analytics/AnalyticsJob.scala:86-91`); then prints the row count
  (`analytics/AnalyticsJob.scala:93-94`).
- Built as one jar `telemetry-analytics_2.12-1.0.0.jar`, Scala 2.12.18,
  Spark 3.5.1 `Provided` (`spark_jobs/analytics/build.sbt:4-15`).
- **Deployed:** no scheduler or manifest exists in the repo. Jobs are run on
  demand with `spark-submit --class telemetry.analytics.<Job>` in the
  `spark-master` container (`README.md:82-93`), or from the dashboard's
  Pipeline Jobs page (`dashboard/app.py:275-284`, `dashboard/app.py:337-347`).
  CI only compiles them (`.github/workflows/ci.yml:50-65`).

| Job | Class | Inputs | Output table | Partition columns | Key (parity) | Spec |
|-----|-------|--------|--------------|-------------------|--------------|------|
| `battery_health_daily` | `BatteryHealthDaily` | telemetry (`event_type = BATTERY`) | `analytics/battery_health_daily` | `event_date` (`analytics/BatteryHealthDaily.scala:10`) | `vin_hash`, `event_date` | [jobs/battery_health_daily.md](jobs/battery_health_daily.md) |
| `charging_sessions` | `ChargingSessions` | telemetry (`event_type = CHARGING`) | `analytics/charging_sessions` | `session_date` (`analytics/ChargingSessions.scala:9`) | `charge_session_id` | [jobs/charging_sessions.md](jobs/charging_sessions.md) |
| `ota_rollout` | `OtaRollout` | telemetry (`event_type = OTA`) | `analytics/ota_rollout` | none | `target_software_version`, `model_safe` | [jobs/ota_rollout.md](jobs/ota_rollout.md) |
| `diagnostic_alerts` | `DiagnosticAlerts` | telemetry (`event_type = DIAGNOSTIC`) + `vehicle_state` | `analytics/diagnostic_alerts` | none | `dtc_code`, `ecu`, `model_safe`, `battery_pack_type` | [jobs/diagnostic_alerts.md](jobs/diagnostic_alerts.md) |

Keys come from the Rust stubs (`rust_pipeline/src/analytics/<job>.rs:8-12`).
Note `README.md:78` lists the `ota_rollout` key as `model`; the actual column
is `model_safe` (`analytics/OtaRollout.scala:37`).

### Output schemas (Spark types)

**`battery_health_daily`** (`rust_pipeline/tests/fixtures/expected/battery_health_daily/`, 396 rows):
`vin_hash` string, `event_date` date, `model_safe` string, `readings` bigint,
`avg_charge_percent` double, `min_charge_percent` double, `peak_cell_temp`
double, `avg_cell_temp_spread` double, `thermal_alert_count` bigint,
`health_level_end_of_day` double, `peak_cell_temp_3d` double, `health_change`
double. Partition: `event_date`.

**`charging_sessions`** (`rust_pipeline/tests/fixtures/expected/charging_sessions/`, 540 rows):
`charge_session_id` string, `vin_hash` string, `model_safe` string,
`session_start` timestamp (µs, UTC), `session_end` timestamp (µs, UTC),
`events` bigint, `energy_added_kwh` double, `peak_power_kw` double,
`soc_start` double, `soc_end` double, `end_state` string, `had_fault`
boolean, `duration_minutes` double, `avg_power_kw` double, `session_date`
date. Partition: `session_date`.

**`ota_rollout`** (`rust_pipeline/tests/fixtures/expected/ota_rollout/`, 6 rows):
`target_software_version` string, `model_safe` string, then one bigint per
state in this order: `UPDATE_STATE_IN_PROGRESS`, `UPDATE_STATE_WAITING_ON_BCM`,
`UPDATE_STATE_SUCCESS`, `UPDATE_SUCCESS_WITH_WARNINGS`, `UPDATE_STATE_FAILED`,
`UPDATE_FAILED_DRIVE_ALLOWED`, `UPDATE_FAILED_NO_ACTION`; then `vehicles`
bigint, `success_rate_pct` double, `failure_rate_pct` double. No partitions.

**`diagnostic_alerts`** (`rust_pipeline/tests/fixtures/expected/diagnostic_alerts/`, 235 rows):
`dtc_code` string, `ecu` string, `model_safe` string, `battery_pack_type`
string, `events` bigint, `vehicles_affected` bigint, `warning_on_events`
bigint, `first_seen` timestamp (µs, UTC), `last_seen` timestamp (µs, UTC),
`fleet_pct_affected` double, `rank_in_ecu` **int** (32-bit). No partitions.

## 10. `BuildParityFixtures` (`analytics/BuildParityFixtures.scala`)

- **What it does:** samples the first `--vehicles` (default 150,
  `analytics/AnalyticsJob.scala:13`) vehicles by `vin_hash` from the live
  tables, writes them as single-file Parquet, reads them back, and writes
  each job's `summarize` output (`analytics/BuildParityFixtures.scala:34-63`).
  Hashes the state table's VIN to select the same vehicles
  (`analytics/BuildParityFixtures.scala:41`). Forces
  `TIMESTAMP_MICROS` Parquet timestamps (`analytics/BuildParityFixtures.scala:51`).
  `--expected-only` keeps the curated input (`analytics/BuildParityFixtures.scala:53`).
- **Inputs:** Delta `vehicle_telemetry`, `vehicle_state` (or existing fixtures).
- **Outputs:** `rust_pipeline/tests/fixtures/input/{telemetry,vehicle_state}/`
  and `rust_pipeline/tests/fixtures/expected/<job>/`.
- **Calls all four `summarize` functions** directly
  (`analytics/BuildParityFixtures.scala:19-24`).
- **Deployed:** manual `spark-submit --local` (`README.md:131-136`); the
  fixtures directory is mounted into `spark-master` (`docker-compose.yml:111`).
- The committed fixtures: 5,763 telemetry rows for 150 vehicles (BATTERY
  2,867, CHARGING 1,412, DIAGNOSTIC 897, OTA 587), event dates 2026-11-05 to
  2026-11-08, and 147 vehicle_state rows, so 3 vehicles have no state row
  (counted with DuckDB over `rust_pipeline/tests/fixtures/input/`).

## 11. Rust analytics ports (`rust-pipeline analytics <job>`)

- **What it does:** `run` registers `telemetry` and `vehicle_state` on a
  DataFusion `SessionContext`, calls the job's `summarize`, and overwrites
  `<job>_rust` with the job's partition columns
  (`rust_pipeline/src/analytics/mod.rs:82-111`).
- **Inputs:** the **Rust** streaming tables `vehicle_telemetry_rust` and
  `vehicle_state_rust` (`rust_pipeline/src/analytics/mod.rs:84-87`). Both are
  opened for every job, including jobs that don't use `vehicle_state`.
- **Outputs:** `analytics/<job>_rust` (`rust_pipeline/src/config.rs:230-232`).
- **Status:** all four `summarize` functions `bail!` and their parity tests
  are `#[ignore]`d (`rust_pipeline/src/analytics/ota_rollout.rs:14-25`, same
  lines in the other three files).
- **Deployed:** CLI only (`rust_pipeline/src/main.rs:38-41`,
  `rust_pipeline/src/main.rs:77-79`); run by the dashboard's "Run Rust &
  validate" button (`dashboard/app.py:501-521`). No Kubernetes manifest.

## 12. Parity validation (`rust-pipeline validate`, parity tests)

- **What it does:** compares a Spark table (`spark_data`) to its Rust twin
  (`rust_data`): rows matched on key columns with a null-safe join
  (`rust_pipeline/src/validate.rs:235-243`), every non-key column equal
  (doubles within 1e-9, `rust_pipeline/src/validate.rs:23`,
  `rust_pipeline/src/validate.rs:220-224`), no `vin`/`latitude`/`longitude`/`kafka_key`
  (`rust_pipeline/src/validate.rs:19`), and for analytics jobs exactly Spark's
  column names and types (nullability ignored; timestamps by unit only:
  `rust_pipeline/src/validate.rs:185-212`; enabled by
  `rust_pipeline/src/analytics/mod.rs:38-46`).
- **Inputs:** live Delta tables (`rust_pipeline/src/main.rs:80-114`) or the
  fixtures (`rust_pipeline/src/analytics/parity.rs:22-58`).
- **Outputs:** `logs/validation_report_<table>.json`
  (`rust_pipeline/src/validate.rs:249-260`), non-zero exit on FAIL.
- **Deployed:** CLI; `cargo test` in CI (`.github/workflows/ci.yml:14-36`).

## 13. Dashboard (`dashboard/app.py`)

- **What it does:** job registry (`dashboard/app.py:178-303`), starts Spark
  jobs with `docker exec spark-master spark-submit` (`dashboard/app.py:337-347`)
  and Rust jobs locally or via `docker exec rust-pipeline`
  (`dashboard/app.py:313-317`). Decides "ported" by the absence of
  `#[ignore` in `rust_pipeline/src/analytics/<job>.rs` (`dashboard/app.py:461-468`).
- **Reads (consumer of every analytics table):** Fleet page, from the Spark
  or Rust table chosen by the user (`dashboard/app.py:644-657`):
  - battery: `vin_hash`, `event_date`, `model_safe`, `health_level_end_of_day`,
    `thermal_alert_count`, `health_change`, `peak_cell_temp` (`dashboard/app.py:662-691`)
  - charging: `energy_added_kwh`, `had_fault`, `duration_minutes`, `end_state`,
    `session_date`, `model_safe` (`dashboard/app.py:693-712`)
  - OTA: every column starting `UPDATE_`, `vehicles`, `success_rate_pct`,
    `failure_rate_pct`, `target_software_version`, `model_safe` (`dashboard/app.py:714-730`)
  - diagnostics: `ecu`, `dtc_code`, `events`, `warning_on_events`,
    `vehicles_affected`, `rank_in_ecu` (`dashboard/app.py:732-748`)
  - JSON sidecars listed at `dashboard/metrics.py:4-11`.
- **Deployed:** docker-compose service `dashboard` with the analytics source
  mounted read-only (`docker-compose.yml:181-204`, `docker-compose.yml:197`),
  or `streamlit run dashboard/app.py` (`README.md:205-207`).

## 14. Demo data loader (`scripts/load_demo_data.py`)

- **What it does:** writes every dashboard table from the fixtures without
  Kafka or Spark, partitioned like the pipeline, and deletes any
  `<job>_rust` tables and validation reports (`scripts/load_demo_data.py:1-20`,
  `scripts/load_demo_data.py:53-74`). Partitioning is duplicated here
  (`scripts/load_demo_data.py:33-40`).
- **Deployed:** `.venv/bin/python scripts/load_demo_data.py` (`README.md:212-214`).

## External consumers (not in this repo)

Downstream service, warranty and OTA systems read the Delta tables
(`README.md:5-7`, `spark_jobs/vehicle_state_job.py:4-6`). Their code and the
exact columns they read are not in the repository; the column-and-type
contract enforced by `rust_pipeline/src/validate.rs:183-203` is the only
record of what they depend on.
