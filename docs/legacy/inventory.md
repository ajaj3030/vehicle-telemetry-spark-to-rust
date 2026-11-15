# Legacy pipeline inventory

Every job in the pipeline as it exists on `main`, in either language. Paths
are relative to the repo root; `file:N` and `file:N-M` are line references.
Bare Scala file names (`AnalyticsJob.scala`, `OtaRollout.scala`, …) live in
`spark_jobs/analytics/src/main/scala/telemetry/analytics/`.

Status at a glance (`README.md:55-61`): the two streaming jobs exist in both
PySpark and Rust and pass parity; the four Scala analytics jobs exist only in
Spark. Their Rust files are placeholders whose `summarize` bails and whose
parity test is `#[ignore]`d (`rust_pipeline/src/analytics/ota_rollout.rs:14-23`,
and the same lines in the other three files).

## Sources: Kafka topics

| Topic | Key | Partitions | Retention / cleanup | Producer | Evidence |
|---|---|---|---|---|---|
| `vehicle_telemetry` | VIN | 8 | 7 days (`retention.ms`), default cleanup | `data_generator` | `config/app_config.yaml:7-11`, `docker-compose.yml:64-71`, `data_generator/generator.py:272` |
| `vehicle_state_updates` | VIN | 8 | 7 days, **compacted** | `data_generator` | `config/app_config.yaml:9-10`, `docker-compose.yml:73-81`, `data_generator/generator.py:273-274` |

The producer keeps per-VIN order with one in-flight request per connection
(`data_generator/generator.py:243-257`) and shards vehicles across threads
(`config/app_config.yaml:20-22`).

## Streaming jobs (ported)

### 1. Telemetry enrichment — `spark_jobs/streaming_job.py` / `rust-pipeline stream`

- **What it does:** parses each JSON event against `TELEMETRY_SCHEMA`
  (`spark_jobs/streaming_job.py:83-115`, `:206`), applies the T1–T7
  enrichment chain (`spark_jobs/transforms.py:190-201`), drops the Kafka key
  (the VIN) (`spark_jobs/streaming_job.py:233`) and appends to Delta. Privacy:
  GPS rounded to 2 dp and raw lat/lon dropped (`spark_jobs/transforms.py:159-166`),
  VIN replaced by `vin_hash` = SHA-256 hex and `vin_wmi` (`spark_jobs/transforms.py:169-176`).
- **Input:** topic `vehicle_telemetry`, from `earliest`
  (`spark_jobs/streaming_job.py:187-191`).
- **Output:** Delta `vehicle_telemetry` (Spark, `config/app_config.yaml:36,39`)
  and `vehicle_telemetry_rust` (Rust, `config/app_config.yaml:77-78`); append,
  `mergeSchema`, **partitioned by `event_year`, `event_month`, `event_type`**
  (`spark_jobs/streaming_job.py:243-245`, `rust_pipeline/src/consumer.rs:33-37`).
- **Columns used by the analytics jobs** (types as stored in the parity
  fixture `rust_pipeline/tests/fixtures/input/telemetry/`, read with pyarrow):

  | Column | Type | Origin |
  |---|---|---|
  | `vin_hash` | string | T6, `spark_jobs/transforms.py:173` |
  | `event_type` | string (BATTERY/CHARGING/DIAGNOSTIC/OTA) | `spark_jobs/streaming_job.py:86` |
  | `event_time` | **string** (ISO-8601, e.g. `2026-11-05T23:23:51.318Z`) | `spark_jobs/streaming_job.py:87` |
  | `sequence_number` | bigint | `spark_jobs/streaming_job.py:88` |
  | `event_date` | date | T2 `to_date(event_time)`, `spark_jobs/transforms.py:87` |
  | `model_safe` | string, `MODEL_UNKNOWN` when `model` is null | T1, `spark_jobs/transforms.py:73` |
  | `charge_percent`, `max_cell_temp`, `battery_health_level`, `charging_power_kw`, `energy_added_kwh` | double | `spark_jobs/streaming_job.py:96-108` |
  | `cell_temp_spread` | double, `round(max-min, 2)` | T3, `spark_jobs/transforms.py:117-118` |
  | `thermal_alert`, `is_fault` | boolean, never null (`coalesce(…, false)`) | T4, `spark_jobs/transforms.py:127-156` |
  | `charge_session_id`, `charge_state`, `ecu`, `dtc_code`, `warning_state`, `battery_pack_type`, `update_state`, `target_software_version` | string | `spark_jobs/streaming_job.py:104-113` |

  The full output (61 columns, incl. `kafka_*` metadata, time dimensions,
  `soc_bucket`, `health_tier`, `severity_level`, `lat_coarse`/`lon_coarse`,
  `row_id`, `sw_major`/`sw_minor`) is defined by `spark_jobs/transforms.py:69-201`
  and mirrored in `rust_pipeline/src/transforms/mod.rs:13-85`.
- **Deployment:** Spark: `spark-submit` on the docker-compose standalone
  cluster (`README.md` Quick start step 5; dashboard `spark_submit_command`,
  `dashboard/app.py:337-348`, job entry `dashboard/app.py:251-260`), 10 s
  micro-batches (`config/app_config.yaml:32`), checkpoint
  `config/app_config.yaml:34`. Rust: Kubernetes Deployment `telemetry-stream`,
  1 replica, `Recreate`, args `stream` (`deploy/k8s/telemetry-stream.yaml:1-16,36`),
  PVC `telemetry-stream-data` 50Gi RWO (`deploy/k8s/storage.yaml:4-12`),
  offsets in SQLite (`config/app_config.yaml:79-80`).
- **Parity:** `rust-pipeline validate` with `TELEMETRY` rules: key `event_id`,
  `kafka_timestamp` ignored, no type check (`rust_pipeline/src/validate.rs:35-42`).

### 2. Vehicle state MERGE — `spark_jobs/vehicle_state_job.py` / `rust-pipeline state`

- **What it does:** collapses each micro-batch to the newest snapshot per VIN
  (row_number over `sequence_number desc`), sums `event_count`, takes the
  earliest `first_seen_at` (`spark_jobs/vehicle_state_job.py:104-117`), then
  MERGEs on `vin`, updating only when `source.sequence_number > target.sequence_number`
  (`spark_jobs/vehicle_state_job.py:120-143`). Rust mirror:
  `rust_pipeline/src/vehicle_state.rs:1-8,154-190`.
- **Input:** topic `vehicle_state_updates` (`spark_jobs/vehicle_state_job.py:160-172`).
- **Output:** Delta `vehicle_state` (Spark, `config/app_config.yaml:56-57`)
  and `vehicle_state_rust` (`config/app_config.yaml:81-82`), one row per VIN,
  **partitioned by `model`** (`spark_jobs/vehicle_state_job.py:125`,
  `rust_pipeline/src/vehicle_state.rs:124`). Schema
  (`spark_jobs/vehicle_state_job.py:58-73`): `vin` string (**raw VIN**, by
  design, `README.md` "Privacy"), `model`, `model_variant`, `battery_pack_type`,
  `software_version`, `power_state` string; `odometer_km`, `charge_percent`,
  `battery_health_level` double; `last_event_type` string; `sequence_number`
  bigint; `event_count` int; `updated_at`, `first_seen_at` string.
- **Deployment:** Spark: `spark-submit` (`dashboard/app.py:262-272`). Rust:
  Deployment `vehicle-state-stream`, args `state`
  (`deploy/k8s/vehicle-state-stream.yaml:36`), PVC `vehicle-state-stream-data`
  10Gi RWO (`deploy/k8s/storage.yaml:14-22`).
- **Parity:** `validate --state`, key `vin`, no type check
  (`rust_pipeline/src/validate.rs:44-50`).

## Batch analytics jobs (Scala, not ported)

All four are Scala 2.12 / Spark 3.5.1 objects in one jar
(`spark_jobs/analytics/build.sbt:4-13`) extending `AnalyticsJob`
(`spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala:75-97`).
Shared behaviour, all from `AnalyticsJob.scala`:

- Input paths: telemetry = `spark.delta_output_path`, vehicle state =
  `vehicle_state.delta_output_path` (`AnalyticsJob.scala:49-50`).
- Output: `analytics.output_base/<job>` (`AnalyticsJob.scala:51-52`,
  `config/app_config.yaml:61`), written as Delta **overwrite** with
  `overwriteSchema=true`, partitioned by `partitionCols` if any
  (`AnalyticsJob.scala:86-91`).
- Session: `spark.sql.session.timeZone=UTC`, `shuffle.partitions` from config;
  ANSI mode is not set, so Spark 3.5's default (off) applies
  (`AnalyticsJob.scala:56-64`).
- Deployment: no scheduler, CronJob or k8s manifest exists for any analytics
  job (the kustomization lists only the two streams, `deploy/k8s/kustomization.yaml:9-13`).
  They are run by hand with `spark-submit --class telemetry.analytics.<Job>`
  (`README.md:81-92`) or from the dashboard's Pipeline Jobs page
  (`dashboard/app.py:274-283`, `dashboard/app.py:337-348`). CI only compiles
  the jar (`.github/workflows/ci.yml:50-65`).

| Job | Scala object | Reads | Writes | Partitioned by | Key (parity) | Spec |
|---|---|---|---|---|---|---|
| `battery_health_daily` | `BatteryHealthDaily.scala` | telemetry (BATTERY) | `analytics/battery_health_daily` | `event_date` | `vin_hash, event_date` | [jobs/battery_health_daily.md](jobs/battery_health_daily.md) |
| `charging_sessions` | `ChargingSessions.scala` | telemetry (CHARGING) | `analytics/charging_sessions` | `session_date` | `charge_session_id` | [jobs/charging_sessions.md](jobs/charging_sessions.md) |
| `ota_rollout` | `OtaRollout.scala` | telemetry (OTA) | `analytics/ota_rollout` | — | `target_software_version, model_safe` | [jobs/ota_rollout.md](jobs/ota_rollout.md) |
| `diagnostic_alerts` | `DiagnosticAlerts.scala` | telemetry (DIAGNOSTIC) **+ vehicle_state** | `analytics/diagnostic_alerts` | — | `dtc_code, ecu, model_safe, battery_pack_type` | [jobs/diagnostic_alerts.md](jobs/diagnostic_alerts.md) |

Partitioning: `BatteryHealthDaily.scala:10`, `ChargingSessions.scala:9`; the
other two inherit `Nil` (`AnalyticsJob.scala:77`). Rust pins the same values
in `rust_pipeline/src/analytics/mod.rs:120-132`. Keys: the `key_cols` of each
Rust `JOB` constant (e.g. `rust_pipeline/src/analytics/ota_rollout.rs:8-12`).
Note `README.md:78-79` lists the OTA and diagnostics keys with `model`; the
code groups by `model_safe` (`OtaRollout.scala:37`, `DiagnosticAlerts.scala:21`).

### Output schemas

Column order and types as written by Spark into
`rust_pipeline/tests/fixtures/expected/<job>/` (read with pyarrow); the Rust
parity check requires exactly these names and types (`rust_pipeline/src/validate.rs:92-94`,
`rust_pipeline/src/analytics/mod.rs:38-46`).

**battery_health_daily** — `vin_hash` string, `event_date` date, `model_safe`
string, `readings` bigint not null, `avg_charge_percent` double,
`min_charge_percent` double, `peak_cell_temp` double, `avg_cell_temp_spread`
double, `thermal_alert_count` bigint, `health_level_end_of_day` double,
`peak_cell_temp_3d` double, `health_change` double.

**charging_sessions** — `charge_session_id` string, `vin_hash` string,
`model_safe` string, `session_start` timestamp(µs, UTC), `session_end`
timestamp(µs, UTC), `events` bigint not null, `energy_added_kwh` double,
`peak_power_kw` double, `soc_start` double, `soc_end` double, `end_state`
string, `had_fault` boolean, `duration_minutes` double, `avg_power_kw` double,
`session_date` date.

**ota_rollout** — `target_software_version` string, `model_safe` string, then
seven bigint-not-null counts in this order: `UPDATE_STATE_IN_PROGRESS`,
`UPDATE_STATE_WAITING_ON_BCM`, `UPDATE_STATE_SUCCESS`,
`UPDATE_SUCCESS_WITH_WARNINGS`, `UPDATE_STATE_FAILED`,
`UPDATE_FAILED_DRIVE_ALLOWED`, `UPDATE_FAILED_NO_ACTION`; `vehicles` bigint
not null, `success_rate_pct` double, `failure_rate_pct` double.

**diagnostic_alerts** — `dtc_code` string, `ecu` string, `model_safe` string,
`battery_pack_type` string, `events` bigint not null, `vehicles_affected`
bigint not null, `warning_on_events` bigint, `first_seen` timestamp(µs, UTC),
`last_seen` timestamp(µs, UTC), `fleet_pct_affected` double, `rank_in_ecu`
**int** not null.

### Rust side of the analytics jobs (scaffolding already on `main`)

- `rust-pipeline --config … analytics <job>` (`rust_pipeline/src/main.rs:38-41,77-79`)
  reads **the Rust-written** `vehicle_telemetry_rust` and `vehicle_state_rust`
  (`rust_pipeline/src/analytics/mod.rs:82-87`) and overwrites
  `analytics/<job>_rust` with the job's partition columns
  (`rust_pipeline/src/analytics/mod.rs:89-111`, `rust_pipeline/src/config.rs:229-232`).
- `rust-pipeline validate --job <job>` compares `analytics/<job>` with
  `analytics/<job>_rust` (`rust_pipeline/src/main.rs:80-96`).
- Parity tests run each port on the committed fixtures
  (`rust_pipeline/src/analytics/parity.rs:22-58`); CI runs them with `cargo test`
  (`.github/workflows/ci.yml:35-36`).

## Utility jobs and helpers

| Job | What it does | Inputs → outputs | Deployment |
|---|---|---|---|
| `BuildParityFixtures` (Scala) | Samples the first N (default 150) vehicles by `vin_hash` and writes the fixture inputs, then each job's `summarize` output, as single Parquet files with `TIMESTAMP_MICROS` | `vehicle_telemetry`, `vehicle_state` → `rust_pipeline/tests/fixtures/{input,expected}` (`BuildParityFixtures.scala:19-63`) | Manual `spark-submit --local` (`README.md` "Parity validation"); fixtures dir mounted into `spark-master` (`docker-compose.yml:111`) |
| `scripts/load_demo_data.py` | Loads fixtures into the dashboard's Delta tables without Kafka/Spark | fixtures → `vehicle_telemetry[_rust]`, `vehicle_state[_rust]`, `analytics/<job>`; deletes `<job>_rust` (`scripts/load_demo_data.py:35-69`) | Manual |
| `spark_jobs/metrics_bridge.py`, `spark_jobs/spark_metrics_collector.py` | Write Spark streaming metrics to `logs/*.json` for the dashboard; imported by the streaming jobs only (`spark_jobs/streaming_job.py:61-75`, `spark_jobs/vehicle_state_job.py:51-56`) | Spark REST API → `logs/` | Runs inside the streaming jobs |
| `data_generator` | Synthetic producer for both topics (`data_generator/generator.py:206-274`) | → Kafka | Manual / dashboard |
