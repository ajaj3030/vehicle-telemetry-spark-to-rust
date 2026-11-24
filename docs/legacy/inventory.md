# Legacy pipeline inventory

Every job in the vehicle telemetry pipeline as it exists on `main`: what it
does, what it reads, what it writes and how it runs. Citations are
`path:line` in this repository.

Related: [dependencies.md](dependencies.md) (who reads and shares what),
[migration-slices.md](migration-slices.md) (proposed port order), and one
functional spec per analytics job in [jobs/](jobs/).

## Summary

| # | Job | Kind | Language | Reads | Writes | How it runs |
|---|---|---|---|---|---|---|
| 1 | Data generator | Producer | Python | none (synthetic) | Kafka `vehicle_telemetry`, `vehicle_state_updates` | `python -m data_generator.run` (`README.md:174`) |
| 2 | `streaming_job.py` | Streaming | PySpark | Kafka `vehicle_telemetry` | Delta `vehicle_telemetry` | `spark-submit` on the docker-compose Spark cluster (`README.md:178-180`) |
| 3 | `vehicle_state_job.py` | Streaming | PySpark | Kafka `vehicle_state_updates` | Delta `vehicle_state` (MERGE) | `spark-submit` on the docker-compose Spark cluster (`README.md:181-183`) |
| 4 | `rust-pipeline stream` | Streaming (port of 2) | Rust | Kafka `vehicle_telemetry` | Delta `vehicle_telemetry_rust` | Kubernetes Deployment (`deploy/k8s/telemetry-stream.yaml:36`) |
| 5 | `rust-pipeline state` | Streaming (port of 3) | Rust | Kafka `vehicle_state_updates` | Delta `vehicle_state_rust` | Kubernetes Deployment (`deploy/k8s/vehicle-state-stream.yaml:36`) |
| 6 | `BatteryHealthDaily` | Batch analytics | Scala | `vehicle_telemetry` | `analytics/battery_health_daily` | `spark-submit --class` (see [Analytics deployment](#analytics-deployment)) |
| 7 | `ChargingSessions` | Batch analytics | Scala | `vehicle_telemetry` | `analytics/charging_sessions` | same |
| 8 | `OtaRollout` | Batch analytics | Scala | `vehicle_telemetry` | `analytics/ota_rollout` | same |
| 9 | `DiagnosticAlerts` | Batch analytics | Scala | `vehicle_telemetry`, `vehicle_state` | `analytics/diagnostic_alerts` | same |
| 10 | `BuildParityFixtures` | Test-data utility | Scala | `vehicle_telemetry`, `vehicle_state` | `rust_pipeline/tests/fixtures/` | `spark-submit --class` by hand (`README.md:130-135`) |
| 11 | `rust-pipeline analytics <job>` | Batch analytics (ports of 6-9) | Rust | `vehicle_telemetry_rust`, `vehicle_state_rust` | `analytics/<job>_rust` | CLI only; every job is a stub that fails (`rust_pipeline/src/analytics/ota_rollout.rs:14-17`) |
| 12 | `rust-pipeline validate` | Parity check | Rust | a Spark table and its Rust twin | `logs/validation_report_<table>.json` | CLI (`rust_pipeline/src/main.rs:42-56`, `rust_pipeline/src/validate.rs:249-259`) |

Migration status on `main`: the two streams are ported and validated, the four
Scala analytics jobs are not (`README.md:55-65`). A port counts as done when
its parity test is no longer `#[ignore]`d (`README.md:226-227`,
`dashboard/app.py:461-468`).

## Kafka topics

| Topic | Key | Partitions | Retention / cleanup | Producer | Consumers |
|---|---|---|---|---|---|
| `vehicle_telemetry` | VIN | 8 | 7 days (`docker-compose.yml:64-71`) | `data_generator/generator.py:272` | jobs 2 and 4 |
| `vehicle_state_updates` | VIN | 8 | 7 days, log-compacted (`docker-compose.yml:73-81`) | `data_generator/generator.py:273` | jobs 3 and 5 |

Topic names come from `config/app_config.yaml:7-10`. One producer thread owns
each VIN, so per-VIN order holds within a partition, and every event carries a
per-VIN `sequence_number` (`README.md:96-99`, `data_generator/generator.py:8-9`).
`event_time` is an ISO-8601 UTC string with milliseconds, for example
`2026-11-06T19:06:03.318Z` (`data_generator/generator.py:59`, `:172`).

## 1. Data generator

- **What:** simulates an EV fleet (2,000 vehicles by default,
  `config/app_config.yaml:24`) and publishes one telemetry event plus one
  latest-state snapshot per step, both keyed by VIN
  (`data_generator/generator.py:207-211`, `:272-273`).
- **Event groups:** `event_type` decides which fields are populated
  (BATTERY, CHARGING, DIAGNOSTIC, OTA). The others are null (`README.md:229-251`).
- **Deployment:** run by hand (`README.md:174`) or from the dashboard's Data
  Generator page (`dashboard/app.py:868`).

## 2. `spark_jobs/streaming_job.py`: telemetry enrichment (PySpark)

- **What:** reads Kafka JSON, parses it with `TELEMETRY_SCHEMA`
  (`spark_jobs/streaming_job.py:83`), applies the seven enrichment transforms
  in `spark_jobs/transforms.py:190-201`, drops the Kafka key (the VIN)
  (`spark_jobs/streaming_job.py:233`) and appends to Delta.
- **Input:** topic `vehicle_telemetry`, `startingOffsets=earliest`,
  `maxOffsetsPerTrigger` 500,000, `failOnDataLoss=false`
  (`spark_jobs/streaming_job.py:186-194`, `config/app_config.yaml:42`).
  Trigger every 10 s (`spark_jobs/streaming_job.py:311`, `config/app_config.yaml:32`).
- **Output:** Delta `vehicle_telemetry` at `spark.delta_output_path`
  (`config/app_config.yaml:36`, `:39`). Append mode with `mergeSchema`,
  **partitioned by `event_year`, `event_month`, `event_type`**
  (`spark_jobs/streaming_job.py:241-246`). Exactly-once through the streaming
  checkpoint (`spark_jobs/streaming_job.py:310`, `README.md:101-105`).
- **Session:** UTC session time zone, so all calendar columns are UTC
  (`spark_jobs/streaming_job.py:136`).
- **Deployment:** `spark-submit` with `--py-files transforms.py` on the
  docker-compose cluster (`README.md:178-180`, `docker-compose.yml:92-160`),
  or from the dashboard (`dashboard/app.py:251-261`).

### `vehicle_telemetry` columns

Types are those of the Spark table, as captured in the fixture Spark writes
from it (`spark_jobs/analytics/src/main/scala/telemetry/analytics/BuildParityFixtures.scala:39`,
`rust_pipeline/tests/fixtures/input/telemetry/`).

| Group | Columns (type) | Source |
|---|---|---|
| Event envelope | `event_id` string, `event_type` string, `event_time` string, `sequence_number` bigint | `spark_jobs/streaming_job.py:84-88` |
| Vehicle context | `model`, `model_variant`, `software_version`, `power_state` string; `odometer_km` double (null becomes 0.0) | `spark_jobs/streaming_job.py:89-95`, `spark_jobs/transforms.py:78` |
| BATTERY | `charge_percent`, `remaining_range`, `kwhr`, `capacity_kwhr`, `battery_health_level`, `max_cell_temp`, `min_cell_temp` double; `preconditioning_status`, `battery_pack_type` string | `spark_jobs/streaming_job.py:96-104` |
| CHARGING | `charge_state`, `charge_session_id` string; `charging_power_kw`, `energy_added_kwh` double | `spark_jobs/streaming_job.py:105-108` |
| DIAGNOSTIC | `ecu`, `dtc_code`, `warning_state` string | `spark_jobs/streaming_job.py:109-111` |
| OTA | `update_state`, `target_software_version` string; `update_progress_percent` int | `spark_jobs/streaming_job.py:112-114` |
| Kafka metadata | `topic` string, `kafka_partition` int, `kafka_offset` bigint, `kafka_timestamp` timestamp | `spark_jobs/streaming_job.py` (parse step) |
| T1 null defaults | `model_safe`, `software_version_safe`, `power_state_safe` string | `spark_jobs/transforms.py:69-79` |
| T2 time dimensions (UTC) | `event_date` date; `event_year`, `event_month`, `event_day`, `event_hour`, `event_dayofweek`, `event_quarter` int; `is_weekend` boolean; `date_str` string | `spark_jobs/transforms.py:82-96` |
| T3 battery metrics | `soc_bucket` string; `energy_pct`, `cell_temp_spread`, `degradation_pct`, `km_per_percent` double | `spark_jobs/transforms.py:99-124` |
| T4 health flags | `health_tier` string (Python UDF); `thermal_alert`, `is_fault`, `needs_service` boolean; `severity_level` int | `spark_jobs/transforms.py:46-57`, `:127-156` |
| T5 coarse location | `lat_coarse`, `lon_coarse` double (raw `latitude`, `longitude` dropped) | `spark_jobs/transforms.py:159-166` |
| T6 pseudonymised VIN | `vin_hash` (SHA-256 hex), `vin_wmi` string (raw `vin` dropped) | `spark_jobs/transforms.py:169-176` |
| T7 event features | `row_id` string; `sw_major`, `sw_minor` int | `spark_jobs/transforms.py:179-187` |

Privacy contract: no raw `vin`, `latitude`, `longitude` or `kafka_key`
(`spark_jobs/transforms.py:8-11`, `README.md:107-113`), enforced by
`FORBIDDEN_PII_COLUMNS` (`rust_pipeline/src/validate.rs:19`).

## 3. `spark_jobs/vehicle_state_job.py`: vehicle state MERGE (PySpark)

- **What:** keeps one row per VIN with the newest snapshot. Each micro-batch
  is collapsed to the newest snapshot per VIN, summing `event_count` and
  taking the minimum `first_seen_at` (`spark_jobs/vehicle_state_job.py:104-117`).
  It is then MERGEd: a matched row is updated only when
  `source.sequence_number > target.sequence_number`, `event_count`
  accumulates, `first_seen_at` takes the least value, and unmatched rows are
  inserted (`spark_jobs/vehicle_state_job.py:120-143`).
- **Input:** topic `vehicle_state_updates`, `maxOffsetsPerTrigger` 200,000,
  null VINs dropped (`spark_jobs/vehicle_state_job.py:160-172`).
- **Output:** Delta `vehicle_state` (`config/app_config.yaml:56-57`),
  **partitioned by `model`** (set when the table is first created,
  `spark_jobs/vehicle_state_job.py:123-126`). Keyed by raw VIN by design: it
  is the operational lookup and holds no location (`README.md:113-114`).
- **Columns:** `vin`, `model`, `model_variant`, `battery_pack_type`,
  `software_version`, `power_state`, `last_event_type`, `updated_at`,
  `first_seen_at` string; `odometer_km`, `charge_percent`,
  `battery_health_level` double; `sequence_number` bigint; `event_count` int
  (`spark_jobs/vehicle_state_job.py:58-73`, `rust_pipeline/tests/fixtures/input/vehicle_state/`).
- **Deployment:** `spark-submit` on the docker-compose cluster
  (`README.md:181-183`), or from the dashboard (`dashboard/app.py:262-272`).

## 4-5. Rust stream processors (already ported)

| | `rust-pipeline stream` | `rust-pipeline state` |
|---|---|---|
| Port of | `streaming_job.py` (`rust_pipeline/src/main.rs:33-34`) | `vehicle_state_job.py` (`rust_pipeline/src/main.rs:35-36`) |
| Output | `vehicle_telemetry_rust` (`config/app_config.yaml:77-78`), partitioned `event_year`, `event_month`, `event_type` (`rust_pipeline/src/consumer.rs:33-37`) | `vehicle_state_rust` (`config/app_config.yaml:81-82`), partitioned `model` (`rust_pipeline/src/vehicle_state.rs:124`) |
| Schema difference | `kafka_timestamp` is Int64 epoch millis, not a timestamp (`rust_pipeline/src/schema.rs:187`, `rust_pipeline/src/validate.rs:39-40`) | none checked (`rust_pipeline/src/validate.rs:44-50`) |
| Offsets | SQLite checkpoint next to the table (`config/app_config.yaml:79-80`, `deploy/k8s/storage.yaml:1-3`) | `config/app_config.yaml:83-84` |
| Deployment | Deployment, 1 replica, `Recreate` (`deploy/k8s/telemetry-stream.yaml:14-16`) | Deployment, 1 replica, `Recreate` (`deploy/k8s/vehicle-state-stream.yaml:14-16`) |
| Parity | `rust-pipeline validate` (`README.md:59`) | `rust-pipeline validate --state` (`README.md:60`) |

Both are rendered by `deploy/k8s/kustomization.yaml:9-13` with the in-cluster
config `deploy/k8s/app_config.yaml`. In docker-compose the `rust-pipeline`
container only idles (`docker-compose.yml:162-178`).

## 6-9. Scala batch analytics jobs

Common shape (`spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala:70-97`):
read the Delta tables, run a pure `summarize` function, and **overwrite** one
Delta table with `overwriteSchema=true`, partitioned by the job's
`partitionCols` if any (`AnalyticsJob.scala:87-91`). The Spark session uses
the UTC time zone and 8 shuffle partitions (`AnalyticsJob.scala:62-63`,
`config/app_config.yaml:41`). Output path is `analytics.output_base/<job>`
(`AnalyticsJob.scala:51-52`, `config/app_config.yaml:59-62`).

| Job | Inputs | Output table | Partition columns | Key (Rust parity) | Spec |
|---|---|---|---|---|---|
| `BatteryHealthDaily` | `vehicle_telemetry` (BATTERY rows) | `battery_health_daily` | `event_date` (`BatteryHealthDaily.scala:10`) | `vin_hash`, `event_date` (`rust_pipeline/src/analytics/battery_health_daily.rs:10`) | [jobs/battery_health_daily.md](jobs/battery_health_daily.md) |
| `ChargingSessions` | `vehicle_telemetry` (CHARGING rows) | `charging_sessions` | `session_date` (`ChargingSessions.scala:9`) | `charge_session_id` (`rust_pipeline/src/analytics/charging_sessions.rs:10`) | [jobs/charging_sessions.md](jobs/charging_sessions.md) |
| `OtaRollout` | `vehicle_telemetry` (OTA rows) | `ota_rollout` | none | `target_software_version`, `model_safe` (`rust_pipeline/src/analytics/ota_rollout.rs:10`) | [jobs/ota_rollout.md](jobs/ota_rollout.md) |
| `DiagnosticAlerts` | `vehicle_telemetry` (DIAGNOSTIC rows), `vehicle_state` | `diagnostic_alerts` | none | `dtc_code`, `ecu`, `model_safe`, `battery_pack_type` (`rust_pipeline/src/analytics/diagnostic_alerts.rs:10`) | [jobs/diagnostic_alerts.md](jobs/diagnostic_alerts.md) |

(Scala files are under `spark_jobs/analytics/src/main/scala/telemetry/analytics/`.)

### Output columns and types

From Spark's own output in `rust_pipeline/tests/fixtures/expected/<job>/`.
"not null" is the Parquet nullability Spark wrote. The parity check compares
names and types but not nullability (`rust_pipeline/src/validate.rs:183-212`).

**`battery_health_daily`**: `vin_hash` string, `event_date` date,
`model_safe` string, `readings` bigint not null, `avg_charge_percent` double,
`min_charge_percent` double, `peak_cell_temp` double, `avg_cell_temp_spread`
double, `thermal_alert_count` bigint, `health_level_end_of_day` double,
`peak_cell_temp_3d` double, `health_change` double.

**`charging_sessions`**: `charge_session_id` string, `vin_hash` string,
`model_safe` string, `session_start` timestamp(µs, UTC), `session_end`
timestamp(µs, UTC), `events` bigint not null, `energy_added_kwh` double,
`peak_power_kw` double, `soc_start` double, `soc_end` double, `end_state`
string, `had_fault` boolean, `duration_minutes` double, `avg_power_kw`
double, `session_date` date.

**`ota_rollout`**: `target_software_version` string, `model_safe` string,
`UPDATE_STATE_IN_PROGRESS`, `UPDATE_STATE_WAITING_ON_BCM`,
`UPDATE_STATE_SUCCESS`, `UPDATE_SUCCESS_WITH_WARNINGS`,
`UPDATE_STATE_FAILED`, `UPDATE_FAILED_DRIVE_ALLOWED`,
`UPDATE_FAILED_NO_ACTION`, `vehicles` (all bigint not null),
`success_rate_pct` double, `failure_rate_pct` double.

**`diagnostic_alerts`**: `dtc_code` string, `ecu` string, `model_safe`
string, `battery_pack_type` string, `events` bigint not null,
`vehicles_affected` bigint not null, `warning_on_events` bigint, `first_seen`
timestamp(µs, UTC), `last_seen` timestamp(µs, UTC), `fleet_pct_affected`
double, `rank_in_ecu` int not null.

### Analytics deployment

- **Build:** one jar for all four jobs, `sbt package`, Scala 2.12.18 against
  Spark 3.5.1 (`spark_jobs/analytics/build.sbt:1-15`), built in Docker
  (`README.md:82-84`). CI compiles it only and runs no Spark job
  (`.github/workflows/ci.yml:50-65`).
- **Run:** `spark-submit --class telemetry.analytics.<Job>` against the
  cluster, after the streams have written their tables (`README.md:86-91`).
  The dashboard's Pipeline Jobs page builds the same command per job
  (`dashboard/app.py:275-284`, `:984-995`).
- **Schedule:** none. The repository has no scheduler, cron or Kubernetes
  CronJob for analytics. `deploy/k8s/kustomization.yaml:9-13` deploys only
  the two streams, and docker-compose has no analytics service
  (`docker-compose.yml:1-204`). Each run is a manual full recompute.
- **CLI flags:** `--config`, `--local` (switches to `./` paths and
  `local[*]`) (`AnalyticsJob.scala:9-28`, `:45-53`, `:59`).

## 10. `BuildParityFixtures` (Scala utility)

Samples the first `--vehicles` (default 150) vehicles by `vin_hash` from
`vehicle_telemetry` and their `vehicle_state` rows, writes them as single
Parquet files, reads them back, and writes each job's `summarize` output as
the expected result
(`spark_jobs/analytics/src/main/scala/telemetry/analytics/BuildParityFixtures.scala:8-64`).
Timestamps are written as `TIMESTAMP_MICROS` (`BuildParityFixtures.scala:51`).
`--expected-only` keeps the curated input (`BuildParityFixtures.scala:53`,
`README.md:124-128`). The committed fixture holds 5,763 telemetry rows for 150
vehicles and 147 `vehicle_state` rows (`rust_pipeline/tests/fixtures/input/`).

## 11. Rust analytics ports (stubs on `main`)

`rust-pipeline analytics <job>` registers `vehicle_telemetry_rust` as
`telemetry` and `vehicle_state_rust` as `vehicle_state`, runs the job, and
overwrites `analytics/<job>_rust` with the Spark job's partitioning
(`rust_pipeline/src/analytics/mod.rs:80-111`, `rust_pipeline/src/config.rs:229-232`).
Each job's `summarize` returns an error and its parity test is ignored
(`rust_pipeline/src/analytics/ota_rollout.rs:14-24`, same in the other three
job files). The parity test runs the port on the fixtures and compares the
result with Spark's expected output (`rust_pipeline/src/analytics/parity.rs:22-58`).
