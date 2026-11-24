# Legacy pipeline inventory

Every job in the vehicle-telemetry pipeline, as it exists on this branch. Citations are `path:line` relative to the repository root; `SCALA` below is shorthand for `spark_jobs/analytics/src/main/scala/telemetry/analytics`.

## Overview

| # | Job | Language / engine | Kind | Reads | Writes | Partition columns |
|---|-----|-------------------|------|-------|--------|-------------------|
| 1 | Telemetry enrichment stream | PySpark Structured Streaming | streaming | Kafka `vehicle_telemetry` | Delta `vehicle_telemetry` | `event_year`, `event_month`, `event_type` |
| 2 | Vehicle-state MERGE stream | PySpark Structured Streaming | streaming | Kafka `vehicle_state_updates` | Delta `vehicle_state` | `model` |
| 3 | Rust telemetry stream (`stream`) | Rust | streaming | Kafka `vehicle_telemetry` | Delta `vehicle_telemetry_rust` | same as 1 |
| 4 | Rust vehicle-state stream (`state`) | Rust | streaming | Kafka `vehicle_state_updates` | Delta `vehicle_state_rust` | `model` |
| 5 | `battery_health_daily` | Scala Spark batch | analytics | Delta `vehicle_telemetry` | Delta `analytics/battery_health_daily` | `event_date` |
| 6 | `charging_sessions` | Scala Spark batch | analytics | Delta `vehicle_telemetry` | Delta `analytics/charging_sessions` | `session_date` |
| 7 | `ota_rollout` | Scala Spark batch | analytics | Delta `vehicle_telemetry` | Delta `analytics/ota_rollout` | none |
| 8 | `diagnostic_alerts` | Scala Spark batch | analytics | Delta `vehicle_telemetry` + `vehicle_state` | Delta `analytics/diagnostic_alerts` | none |
| 9 | `BuildParityFixtures` | Scala Spark batch (tooling) | fixture builder | telemetry + vehicle_state | `rust_pipeline/tests/fixtures/` Parquet | none |
| 10 | Rust analytics (`analytics <job>`) | Rust / DataFusion | analytics (stubs) | `vehicle_telemetry_rust` + `vehicle_state_rust` | Delta `analytics/<job>_rust` | per job |
| 11 | Data generator | Python | producer | — | Kafka both topics | — |
| 12 | Dashboard | Python / Streamlit | consumer | all Delta tables above | — | — |

Sources: `README.md`, `config/app_config.yaml:8-84`, `spark_jobs/streaming_job.py:186-247`, `spark_jobs/vehicle_state_job.py:120-172`, `SCALA/AnalyticsJob.scala`, `SCALA/BuildParityFixtures.scala:19-24`, `rust_pipeline/src/main.rs:33-55`, `dashboard/app.py:178-225`.

## Kafka topics

| Topic | Key | Partitions | Retention | Producer | Source |
|-------|-----|-----------|-----------|----------|--------|
| `vehicle_telemetry` | VIN | 8 | default (delete) | `data_generator/generator.py` | `config/app_config.yaml:8,11`, `docker-compose.yml:65-68`, `data_generator/schemas.py` |
| `vehicle_state_updates` | VIN | 8 | `cleanup.policy=compact` | `data_generator/generator.py` | `config/app_config.yaml:10-11`, `docker-compose.yml:74-81` |

## 1. Telemetry enrichment stream — `spark_jobs/streaming_job.py`

- **What it does:** reads raw JSON telemetry from Kafka, parses it with `TELEMETRY_SCHEMA` (`spark_jobs/streaming_job.py:83`, `:202-216`), applies the seven enrichment transforms `add_null_handling → add_time_dimensions → add_battery_features → add_health_features → add_location_features → add_anonymized_columns → add_event_features` (`spark_jobs/transforms.py:190-200`), drops `kafka_key` (`spark_jobs/streaming_job.py:233`) and appends to Delta (`spark_jobs/streaming_job.py:241-247`).
- **Privacy:** raw `vin` is replaced by `vin_hash = sha2(vin, 256)` and `vin_wmi` (`spark_jobs/transforms.py:169-175`); `latitude`/`longitude` are replaced by 2-dp `lat_coarse`/`lon_coarse` (`spark_jobs/transforms.py:159-165`).
- **Inputs:** Kafka topic `vehicle_telemetry` (`spark_jobs/streaming_job.py:187-192`, `config/app_config.yaml:8`).
- **Output:** Delta `vehicle_telemetry` at `/opt/delta_output/vehicle_telemetry` (local `./delta_output/vehicle_telemetry`) (`config/app_config.yaml:36,39`), mode `append`, partitioned by `event_year`, `event_month`, `event_type` (`spark_jobs/streaming_job.py:243-245`). Checkpoint `config/app_config.yaml:34,38`; trigger `spark_jobs/streaming_job.py:310-311`.
- **Columns (Arrow types, from the committed sample `rust_pipeline/tests/fixtures/input/telemetry/`, produced from this table by `SCALA/BuildParityFixtures.scala:34-44`):**

| Group | Columns |
|-------|---------|
| identity / event | `vin_hash` string, `event_id` string, `event_type` string, `event_time` string (ISO-8601 with ms and `Z`), `sequence_number` int64 |
| vehicle | `model`, `model_variant`, `software_version`, `power_state` string, `odometer_km` double |
| battery | `charge_percent`, `remaining_range`, `kwhr`, `capacity_kwhr`, `battery_health_level`, `max_cell_temp`, `min_cell_temp` double, `preconditioning_status`, `battery_pack_type` string |
| charging | `charge_state`, `charge_session_id` string, `charging_power_kw`, `energy_added_kwh` double |
| diagnostic | `ecu`, `dtc_code`, `warning_state` string |
| OTA | `update_state`, `target_software_version` string, `update_progress_percent` int32 |
| Kafka metadata | `topic` string, `kafka_partition` int32, `kafka_offset` int64, `kafka_timestamp` timestamp[us, UTC] |
| null handling (`spark_jobs/transforms.py:69-79`) | `model_safe`, `software_version_safe`, `power_state_safe` string |
| time dimensions (`spark_jobs/transforms.py:82-96`) | `event_date` date32, `event_year`, `event_month`, `event_day`, `event_hour`, `event_dayofweek` int32, `is_weekend` bool, `event_quarter` int32, `date_str` string |
| battery features (`spark_jobs/transforms.py:99-124`) | `soc_bucket` string, `energy_pct`, `cell_temp_spread`, `degradation_pct`, `km_per_percent` double |
| health features (`spark_jobs/transforms.py:127-156`) | `health_tier` string, `thermal_alert`, `is_fault`, `needs_service` bool, `severity_level` int32 |
| location / anonymisation (`spark_jobs/transforms.py:159-175`) | `lat_coarse`, `lon_coarse` double, `vin_wmi` string |
| event features (`spark_jobs/transforms.py:179-186`) | `row_id` string, `sw_major`, `sw_minor` int32 |

Event-type-specific columns (battery, charging, diagnostic, OTA groups) are null on rows of other event types (`data_generator/schemas.py`, `spark_jobs/streaming_job.py:83`).
- **Deployment:** `spark-submit` with the Kafka and Delta packages onto the standalone cluster in `docker-compose.yml:92-158`, started manually (`README.md`) or from the dashboard's Pipeline Jobs page (`dashboard/app.py:249-285`, `:337-350`).

## 2. Vehicle-state MERGE stream — `spark_jobs/vehicle_state_job.py`

- **What it does:** reads latest-state snapshots, collapses each micro-batch to one row per VIN (newest `sequence_number` wins, `event_count` summed, earliest `first_seen_at`) (`spark_jobs/vehicle_state_job.py:103-117`), then MERGEs into Delta, updating only when `source.sequence_number > target.sequence_number` and inserting unseen VINs (`spark_jobs/vehicle_state_job.py:120-143`). The first batch creates the table (`spark_jobs/vehicle_state_job.py:123-126`).
- **Inputs:** Kafka `vehicle_state_updates`, `maxOffsetsPerTrigger` 200000 (`spark_jobs/vehicle_state_job.py:161-169`, `config/app_config.yaml:10`).
- **Output:** Delta `vehicle_state` at `/opt/delta_output/vehicle_state` (`config/app_config.yaml:56-57`), partitioned by `model` (`spark_jobs/vehicle_state_job.py:125`). Checkpoint `config/app_config.yaml:53-54`.
- **Columns** (`spark_jobs/vehicle_state_job.py:58-73`; types from `rust_pipeline/tests/fixtures/input/vehicle_state/`): `vin` string, `model` string, `model_variant` string, `battery_pack_type` string, `software_version` string, `power_state` string, `odometer_km` double, `charge_percent` double, `battery_health_level` double, `last_event_type` string, `sequence_number` int64, `updated_at` string, `event_count` int32, `first_seen_at` string.
- **Note:** this table holds the **raw VIN** (`spark_jobs/vehicle_state_job.py:58-73`), so any job that reads it must hash before output (`rust_pipeline/src/validate.rs:19`).
- **Deployment:** `spark-submit` like job 1 (`dashboard/app.py:249-285`, `README.md`).

## 3. Rust telemetry stream — `rust_pipeline/src/consumer.rs`

- **What it does:** port of job 1 (`rust_pipeline/src/main.rs:33-34`): consumes all 8 partitions from checkpointed offsets (`rust_pipeline/src/consumer.rs:26-60`), applies the Rust ports of the transforms in `rust_pipeline/src/transforms/`, writes Delta with the same partition columns (`rust_pipeline/src/consumer.rs:33`, `:115`) and stores offsets in SQLite (`rust_pipeline/src/checkpoint.rs`, `config/app_config.yaml:79-80`). Separate consumer group from Spark (`rust_pipeline/src/consumer.rs:42`, `dashboard/app.py:295`).
- **Inputs / outputs:** Kafka `vehicle_telemetry` → Delta `vehicle_telemetry_rust` (`config/app_config.yaml:77-78`, `rust_pipeline/src/config.rs:158`); schema must match job 1, checked by `validate` (`rust_pipeline/src/validate.rs:35-43`).
- **Deployment:** Kubernetes Deployment, 1 replica, `Recreate` strategy, args `--config /opt/config/app_config.yaml stream`, PVC `telemetry-stream-data` (`deploy/k8s/telemetry-stream.yaml:3-36,72`, `deploy/k8s/kustomization.yaml`); locally `cargo run -- stream` in the `rust-pipeline` container (`docker-compose.yml:162-178`).

## 4. Rust vehicle-state stream — `rust_pipeline/src/vehicle_state.rs`

- **What it does:** port of job 2 with the same collapse and `sequence_number` guard (`rust_pipeline/src/vehicle_state.rs:1-8`, `rust_pipeline/src/main.rs:35-36`).
- **Inputs / outputs:** Kafka `vehicle_state_updates` → Delta `vehicle_state_rust` (`rust_pipeline/src/vehicle_state.rs:61`, `config/app_config.yaml:81-82`, `rust_pipeline/src/config.rs:174`); columns as job 2, checked by `rust_pipeline/src/validate.rs:44-50`; partitioned by `model` like job 2 (`rust_pipeline/src/vehicle_state.rs:124`).
- **Deployment:** Kubernetes Deployment, args `... state`, PVC `vehicle-state-stream-data` (`deploy/k8s/vehicle-state-stream.yaml:14-36,72`).

## 5-8. Scala analytics jobs — `spark_jobs/analytics/`

Shared runner `SCALA/AnalyticsJob.scala`: parses `--config`/`--local`, loads `config/app_config.yaml`, opens a session with `spark.sql.session.timeZone=UTC` (`SCALA/AnalyticsJob.scala:63`), calls the job's `build`, and writes Delta with `mode("overwrite")`, `overwriteSchema=true`, `partitionBy(partitionCols)` when non-empty (`SCALA/AnalyticsJob.scala:77`, `:87-91`), to `<analytics.output_base>/<job name>` (`SCALA/AnalyticsJob.scala:51-52`, `config/app_config.yaml:59-62`). Detailed specs are in `jobs/`.

| Job | Reads | Output columns (type) | Partition | Spec |
|-----|-------|------------------------|-----------|------|
| `battery_health_daily` | telemetry (`SCALA/BatteryHealthDaily.scala:43-44`) | `vin_hash` string, `event_date` date, `model_safe` string, `readings` int64, `avg_charge_percent` double, `min_charge_percent` double, `peak_cell_temp` double, `avg_cell_temp_spread` double, `thermal_alert_count` int64, `health_level_end_of_day` double, `peak_cell_temp_3d` double, `health_change` double | `event_date` (`SCALA/BatteryHealthDaily.scala:10`) | `jobs/battery_health_daily.md` |
| `charging_sessions` | telemetry (`SCALA/ChargingSessions.scala:40-41`) | `charge_session_id` string, `vin_hash` string, `model_safe` string, `session_start` timestamp[us, UTC], `session_end` timestamp[us, UTC], `events` int64, `energy_added_kwh` double, `peak_power_kw` double, `soc_start` double, `soc_end` double, `end_state` string, `had_fault` bool, `duration_minutes` double, `avg_power_kw` double, `session_date` date | `session_date` (`SCALA/ChargingSessions.scala:9`) | `jobs/charging_sessions.md` |
| `ota_rollout` | telemetry (`SCALA/OtaRollout.scala:46-47`) | `target_software_version` string, `model_safe` string, 7 × int64 state counts (`UPDATE_STATE_IN_PROGRESS`, `UPDATE_STATE_WAITING_ON_BCM`, `UPDATE_STATE_SUCCESS`, `UPDATE_SUCCESS_WITH_WARNINGS`, `UPDATE_STATE_FAILED`, `UPDATE_FAILED_DRIVE_ALLOWED`, `UPDATE_FAILED_NO_ACTION`), `vehicles` int64, `success_rate_pct` double, `failure_rate_pct` double | none (default `Nil`, `SCALA/AnalyticsJob.scala:77`) | `jobs/ota_rollout.md` |
| `diagnostic_alerts` | telemetry + vehicle_state (`SCALA/DiagnosticAlerts.scala:36-40`) | `dtc_code` string, `ecu` string, `model_safe` string, `battery_pack_type` string, `events` int64, `vehicles_affected` int64, `warning_on_events` int64, `first_seen` timestamp[us, UTC], `last_seen` timestamp[us, UTC], `fleet_pct_affected` double, `rank_in_ecu` int32 | none (default `Nil`, `SCALA/AnalyticsJob.scala:77`) | `jobs/diagnostic_alerts.md` |

Column types are read from Spark's own output in `rust_pipeline/tests/fixtures/expected/<job>/`, written by `SCALA/BuildParityFixtures.scala:27-32`.

- **Deployment:** one fat JAR built with `sbt package` (Scala 2.12.18, Spark SQL 3.5.1, Java 11 target; `spark_jobs/analytics/build.sbt`), built in CI (`.github/workflows/ci.yml:55-65`), run with `spark-submit --class telemetry.analytics.<Class>` on the compose cluster (`dashboard/app.py:275-284`, `:337-350`, `README.md:88`). There is no scheduler or Kubernetes manifest for analytics: they are started by hand or from the dashboard (`deploy/k8s/kustomization.yaml` lists only the two stream Deployments).

## 9. `BuildParityFixtures` — `SCALA/BuildParityFixtures.scala`

- Reads telemetry and vehicle_state, takes the first N `vin_hash` values in sort order and the matching vehicle_state rows (hashing `vin` to join) (`SCALA/BuildParityFixtures.scala:34-44`), runs all four `summarize` functions and writes Parquet with `TIMESTAMP_MICROS` (`SCALA/BuildParityFixtures.scala:51`) to `rust_pipeline/tests/fixtures/input/{telemetry,vehicle_state}` and `.../expected/<job>` (`SCALA/BuildParityFixtures.scala:19-65`, `rust_pipeline/src/analytics/parity.rs:1-7`).
- Fixture sizes: telemetry 5,763 rows, vehicle_state 147 rows; expected `battery_health_daily` 396, `charging_sessions` 540, `ota_rollout` 6, `diagnostic_alerts` 235 rows (`rust_pipeline/tests/fixtures/`).
- Deployment: run manually with `spark-submit --class telemetry.analytics.BuildParityFixtures` (`README.md:132-133`).

## 10. Rust analytics (`analytics <job>`) — `rust_pipeline/src/analytics/`

- `rust_pipeline/src/analytics/mod.rs:49-54` registers all four jobs and `:70-78` dispatches to each module's `summarize`. On this branch every job module is a stub whose `summarize` returns an error and whose parity test is `#[ignore]` (e.g. `rust_pipeline/src/analytics/ota_rollout.rs:8-21`).
- `run` opens **both** `vehicle_telemetry_rust` and `vehicle_state_rust` for every job, registers them as `telemetry` and `vehicle_state`, collects the result and overwrites `<analytics base>/<job>_rust` with the job's `partition_cols` (`rust_pipeline/src/analytics/mod.rs:82-111`, `rust_pipeline/src/config.rs:224-232`).
- Parity: `cargo test` runs each job on the fixtures and compares with Spark's expected output (`rust_pipeline/src/analytics/parity.rs:22-58`, `.github/workflows/ci.yml:35-36`); `validate --job <job>` compares live Spark and Rust tables on rows, values (float tolerance 1e-9), column types, and the forbidden columns `vin`, `latitude`, `longitude`, `kafka_key` (`rust_pipeline/src/validate.rs:19-23`, `:155-212`, `rust_pipeline/src/main.rs:43-55`).
- Deployment: CLI only (`rust_pipeline/src/main.rs:37-41`); no Kubernetes manifest (`deploy/k8s/kustomization.yaml`).

## 11. Data generator — `data_generator/`

Produces synthetic telemetry events and latest-state snapshots, keyed by VIN, to the two topics (`data_generator/generator.py`, `data_generator/schemas.py`).

## 12. Dashboard — `dashboard/app.py`

- Fleet page reads each analytics output from the Spark or Rust table (`dashboard/app.py:212-225`, `:650-658`) and renders battery (`:662-691`), charging (`:693-712`), OTA (`:714-730`) and diagnostics (`:732-748`) views.
- Migration page treats a job as ported once its Rust file no longer contains `#[ignore` (`dashboard/app.py:461-468`) and can build, run and validate a port (`dashboard/app.py:501-521`).
- Deployment: `dashboard` service in `docker-compose.yml:181-190`.
