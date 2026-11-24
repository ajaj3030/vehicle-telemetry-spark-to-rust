# Legacy pipeline inventory

Every job that produces or consumes pipeline data on `main`, in any language.
Paths are relative to the repo root; `file:N-M` means lines N–M.

Status today (`README.md:9-12`, `README.md:55-65`): both streaming jobs are
ported to Rust; the four Scala analytics jobs are not (each Rust file in
`rust_pipeline/src/analytics/` is a stub that `bail!`s, e.g.
`rust_pipeline/src/analytics/ota_rollout.rs:14-17`).

## Summary

| Job | Kind | Language / source | Inputs | Output table | Partitioned by | Deployed by |
|---|---|---|---|---|---|---|
| Data generator | producer | Python `data_generator/generator.py` | – | Kafka `vehicle_telemetry`, `vehicle_state_updates` | Kafka key = VIN | `python -m data_generator.run` (`README.md:173-174`), dashboard Data Generator page |
| Telemetry stream | streaming | PySpark `spark_jobs/streaming_job.py` + `spark_jobs/transforms.py` | Kafka `vehicle_telemetry` | Delta `vehicle_telemetry` | `event_year`, `event_month`, `event_type` | `spark-submit` on the docker-compose Spark cluster |
| Vehicle-state stream | streaming | PySpark `spark_jobs/vehicle_state_job.py` | Kafka `vehicle_state_updates` | Delta `vehicle_state` (MERGE) | `model` | `spark-submit` on the docker-compose Spark cluster |
| Telemetry stream (Rust) | streaming | `rust_pipeline/src/consumer.rs` + `rust_pipeline/src/transforms/` | Kafka `vehicle_telemetry` | Delta `vehicle_telemetry_rust` | `event_year`, `event_month`, `event_type` | k8s `deploy/k8s/telemetry-stream.yaml` |
| Vehicle-state stream (Rust) | streaming | `rust_pipeline/src/vehicle_state.rs` | Kafka `vehicle_state_updates` | Delta `vehicle_state_rust` | `model` | k8s `deploy/k8s/vehicle-state-stream.yaml` |
| `battery_health_daily` | batch analytics | Scala `spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala` | Delta `vehicle_telemetry` | Delta `analytics/battery_health_daily` | `event_date` | `spark-submit --class telemetry.analytics.BatteryHealthDaily` |
| `charging_sessions` | batch analytics | Scala `.../ChargingSessions.scala` | Delta `vehicle_telemetry` | Delta `analytics/charging_sessions` | `session_date` | `spark-submit --class telemetry.analytics.ChargingSessions` |
| `ota_rollout` | batch analytics | Scala `.../OtaRollout.scala` | Delta `vehicle_telemetry` | Delta `analytics/ota_rollout` | none | `spark-submit --class telemetry.analytics.OtaRollout` |
| `diagnostic_alerts` | batch analytics | Scala `.../DiagnosticAlerts.scala` | Delta `vehicle_telemetry`, `vehicle_state` | Delta `analytics/diagnostic_alerts` | none | `spark-submit --class telemetry.analytics.DiagnosticAlerts` |
| `BuildParityFixtures` | test tooling | Scala `.../BuildParityFixtures.scala` | Delta `vehicle_telemetry`, `vehicle_state` | Parquet `rust_pipeline/tests/fixtures/{input,expected}` | none | manual `spark-submit` (`README.md:131-136`) |
| Dashboard | consumer | Python `dashboard/app.py` | all Delta tables above | – (reads only) | – | `streamlit run dashboard/app.py`, docker-compose `dashboard` (`docker-compose.yml:181-204`) |

Helpers that are not jobs: `spark_jobs/metrics_bridge.py` (JSON metrics file
for the dashboard, `spark_jobs/metrics_bridge.py:1-15`) and
`spark_jobs/spark_metrics_collector.py` (Spark REST metrics, imported at
`spark_jobs/streaming_job.py:69-75`).

Abbreviation used below: `scala/` = `spark_jobs/analytics/src/main/scala/telemetry/analytics/`.

## Kafka topics

| Topic | Key | Producer | Config | Topic settings |
|---|---|---|---|---|
| `vehicle_telemetry` | VIN | `data_generator/generator.py:272` | `config/app_config.yaml:8` | 8 partitions, 7-day retention (`docker-compose.yml:65-71`) |
| `vehicle_state_updates` | VIN | `data_generator/generator.py:273` | `config/app_config.yaml:10` | 8 partitions, 7-day retention, `cleanup.policy=compact` (`docker-compose.yml:73-81`) |

Per-VIN ordering: one producer per VIN and a per-VIN `sequence_number`
(`README.md:96-99`, `config/app_config.yaml:20-22`). Event fields by
`event_type` are listed in `README.md:229-245`.

## Streaming jobs

### Telemetry stream — `spark_jobs/streaming_job.py`

- **Does:** reads JSON from `vehicle_telemetry` (`spark_jobs/streaming_job.py:186-195`),
  parses with `TELEMETRY_SCHEMA` (`spark_jobs/streaming_job.py:83-115`), applies
  T1–T7 from `spark_jobs/transforms.py:190-201`, drops `kafka_key`
  (`spark_jobs/streaming_job.py:233`) and appends to Delta.
- **Output:** `vehicle_telemetry` at `config/app_config.yaml:36` (cluster) / `:39` (local);
  append with `mergeSchema`, partitioned by `event_year, event_month, event_type`
  (`spark_jobs/streaming_job.py:240-246`).
- **Columns** (61, types as written; nullable): the 31 parsed fields
  (`spark_jobs/streaming_job.py:83-115`: strings, `sequence_number` long,
  readings double, `update_progress_percent` int), Kafka metadata `topic` string,
  `kafka_partition` int, `kafka_offset` long, `kafka_timestamp` timestamp
  (`spark_jobs/streaming_job.py:213-225`), and the derived columns:
  T1 `model_safe`, `software_version_safe`, `power_state_safe` string
  (`spark_jobs/transforms.py:69-79`); T2 `event_date` date, `event_year`,
  `event_month`, `event_day`, `event_hour`, `event_dayofweek`, `event_quarter` int,
  `is_weekend` boolean, `date_str` string (`spark_jobs/transforms.py:82-96`);
  T3 `soc_bucket` string, `energy_pct`, `cell_temp_spread`, `degradation_pct`,
  `km_per_percent` double (`spark_jobs/transforms.py:99-124`); T4 `health_tier`
  string, `thermal_alert`, `is_fault`, `needs_service` boolean, `severity_level`
  int (`spark_jobs/transforms.py:127-156`); T5 `lat_coarse`, `lon_coarse` double,
  raw lat/lon dropped (`spark_jobs/transforms.py:159-166`); T6 `vin_hash`
  (SHA-256 hex), `vin_wmi` string, raw `vin` dropped (`spark_jobs/transforms.py:169-176`);
  T7 `row_id` string, `sw_major`, `sw_minor` int (`spark_jobs/transforms.py:179-187`).
  The committed fixture `rust_pipeline/tests/fixtures/input/telemetry/` has exactly
  this schema (event_time stays a string, e.g. `2026-11-05T23:23:51.318Z`).
- **Settings:** session time zone UTC (`spark_jobs/streaming_job.py:136`), trigger
  `10 seconds` (`spark_jobs/streaming_job.py:311`, `config/app_config.yaml:32`),
  checkpoint `config/app_config.yaml:34`.
- **Deployed:** `spark-submit ... --py-files transforms.py streaming_job.py`
  on the docker-compose cluster (`README.md:177-180`; the dashboard builds the same
  command in `dashboard/app.py:337-348`). Cluster: `docker-compose.yml:92-160`.
- **Rust twin:** `rust-pipeline stream` (`rust_pipeline/src/main.rs:69-72`), writes
  `vehicle_telemetry_rust` (`config/app_config.yaml:77-78`) with the same partition
  columns (`rust_pipeline/src/consumer.rs:33-37`); parity via `validate`
  (`rust_pipeline/src/validate.rs:35-42`).

### Vehicle-state stream — `spark_jobs/vehicle_state_job.py`

- **Does:** reads `vehicle_state_updates` (`spark_jobs/vehicle_state_job.py:160-172`),
  collapses each micro-batch to the newest snapshot per VIN plus summed
  `event_count` and min `first_seen_at` (`spark_jobs/vehicle_state_job.py:104-117`),
  then MERGEs on `vin`, updating only when `source.sequence_number > target.sequence_number`
  (`spark_jobs/vehicle_state_job.py:120-143`).
- **Output:** `vehicle_state` (`config/app_config.yaml:56-57`), one row per VIN,
  partitioned by `model` (`spark_jobs/vehicle_state_job.py:125`).
- **Columns** (`spark_jobs/vehicle_state_job.py:58-73`): `vin`, `model`,
  `model_variant`, `battery_pack_type`, `software_version`, `power_state`,
  `last_event_type`, `updated_at`, `first_seen_at` string; `odometer_km`,
  `charge_percent`, `battery_health_level` double; `sequence_number` long;
  `event_count` int. Keyed by raw VIN by design (`README.md:111-114`).
- **Deployed:** `spark-submit vehicle_state_job.py` (`README.md:181-183`).
- **Rust twin:** `rust-pipeline state` (`rust_pipeline/src/main.rs:73-76`) →
  `vehicle_state_rust` (`config/app_config.yaml:81-82`), partitioned by `model`
  (`rust_pipeline/src/vehicle_state.rs:124`); parity via `validate --state`
  (`rust_pipeline/src/validate.rs:44-50`).

### Rust stream deployment

`deploy/k8s/kustomization.yaml:9-13` deploys `telemetry-stream`
(`deploy/k8s/telemetry-stream.yaml:36`, args `stream`) and `vehicle-state-stream`
(`deploy/k8s/vehicle-state-stream.yaml:36`, args `state`), each one replica with
`Recreate` (`deploy/k8s/telemetry-stream.yaml:1-6,14-16`) and its own PVC
(`deploy/k8s/storage.yaml`). There is **no** k8s manifest for analytics jobs or for
Spark: the manifests cover only the two Rust streams.

## Batch analytics jobs (Scala, the migration scope)

All four share `scala/AnalyticsJob.scala`: CLI (`:9-28`), YAML config (`:31-42`),
table paths (`:44-53`), a Spark session with UTC time zone and
`shuffle_partitions` from config (`:55-68`), and a `main` that overwrites one
Delta table with `overwriteSchema=true`, partitioned by `partitionCols` if set
(`:75-97`). Inputs resolve to `spark.delta_output_path` / `vehicle_state.delta_output_path`
and outputs to `analytics.output_base/<job>` (`config/app_config.yaml:36,56,59-62`).
Built as one jar against Spark 3.5.1 / Scala 2.12.18 (`spark_jobs/analytics/build.sbt:4,13`),
run with `spark-submit --class telemetry.analytics.<Job>` (`README.md:81-92`,
`dashboard/app.py:275-283`). No scheduler: they run when someone starts them
(README or the dashboard's Pipeline Jobs page, `dashboard/app.py:984`).
CI only compiles them (`.github/workflows/ci.yml:50-65`).

### `battery_health_daily` — `scala/BatteryHealthDaily.scala`
- **Does:** per vehicle per day battery summary, 3-day peak cell temperature, day-over-day health change (`:7`, `:14-41`).
- **Inputs:** `vehicle_telemetry` rows with `event_type = 'BATTERY'`; columns `vin_hash, event_date, model_safe, charge_percent, max_cell_temp, cell_temp_spread, thermal_alert, battery_health_level, sequence_number` (`:16-26`).
- **Output:** `analytics/battery_health_daily`, partitioned by `event_date` (`:10`). Columns (types from `rust_pipeline/tests/fixtures/expected/battery_health_daily/`): `vin_hash` string, `event_date` date, `model_safe` string, `readings` long (non-null), `avg_charge_percent` double, `min_charge_percent` double, `peak_cell_temp` double, `avg_cell_temp_spread` double, `thermal_alert_count` long, `health_level_end_of_day` double, `peak_cell_temp_3d` double, `health_change` double. Key `vin_hash, event_date` (`rust_pipeline/src/analytics/battery_health_daily.rs:10`).

### `charging_sessions` — `scala/ChargingSessions.scala`
- **Does:** one row per charging session (`:6`, `:11-38`).
- **Inputs:** `vehicle_telemetry` rows with `event_type = 'CHARGING'` and non-null `charge_session_id`; columns `charge_session_id, vin_hash, model_safe, event_time, energy_added_kwh, charging_power_kw, charge_percent, charge_state, is_fault, sequence_number` (`:13-25`).
- **Output:** `analytics/charging_sessions`, partitioned by `session_date` (`:9`). Columns (fixture `expected/charging_sessions/`): `charge_session_id`, `vin_hash`, `model_safe` string, `session_start`, `session_end` timestamp(µs, UTC), `events` long (non-null), `energy_added_kwh`, `peak_power_kw`, `soc_start`, `soc_end` double, `end_state` string, `had_fault` boolean, `duration_minutes`, `avg_power_kw` double, `session_date` date. Key `charge_session_id` (`rust_pipeline/src/analytics/charging_sessions.rs:10`).

### `ota_rollout` — `scala/OtaRollout.scala`
- **Does:** latest OTA update state per vehicle and target version, pivoted per target version and model, with success and failure rates (`:7`, `:26-44`).
- **Inputs:** `vehicle_telemetry` rows with `event_type = 'OTA'`; columns `vin_hash, target_software_version, sequence_number, update_state, model_safe` (`:27-38`).
- **Output:** `analytics/ota_rollout`, not partitioned. Columns (fixture `expected/ota_rollout/`): `target_software_version`, `model_safe` string; `UPDATE_STATE_IN_PROGRESS`, `UPDATE_STATE_WAITING_ON_BCM`, `UPDATE_STATE_SUCCESS`, `UPDATE_SUCCESS_WITH_WARNINGS`, `UPDATE_STATE_FAILED`, `UPDATE_FAILED_DRIVE_ALLOWED`, `UPDATE_FAILED_NO_ACTION`, `vehicles` long (non-null); `success_rate_pct`, `failure_rate_pct` double. Key `target_software_version, model_safe` (`rust_pipeline/src/analytics/ota_rollout.rs:10`).

### `diagnostic_alerts` — `scala/DiagnosticAlerts.scala`
- **Does:** fleet impact of each diagnostic trouble code, for service triage (`:7`, `:11-34`).
- **Inputs:** `vehicle_telemetry` rows with `event_type = 'DIAGNOSTIC'` (columns `vin_hash, dtc_code, ecu, model_safe, warning_state, event_time`) and the whole `vehicle_state` table (`vin, battery_pack_type`, plus its row count) (`:13-28`, `:36-40`).
- **Output:** `analytics/diagnostic_alerts`, not partitioned. Columns (fixture `expected/diagnostic_alerts/`): `dtc_code`, `ecu`, `model_safe`, `battery_pack_type` string; `events`, `vehicles_affected` long (non-null); `warning_on_events` long; `first_seen`, `last_seen` timestamp(µs, UTC); `fleet_pct_affected` double; `rank_in_ecu` int (non-null). Key `dtc_code, ecu, model_safe, battery_pack_type` (`rust_pipeline/src/analytics/diagnostic_alerts.rs:10`).

### `BuildParityFixtures` — `scala/BuildParityFixtures.scala`
Samples the first `--vehicles` (default 150) `vin_hash`es from `vehicle_telemetry`
and the matching `vehicle_state` rows (`:34-44`), writes them as single Parquet
files with `TIMESTAMP_MICROS` (`:27-32`, `:51`), reads them back and writes each
job's `summarize` output to `expected/<job>` (`:19-24`, `:56-63`). It calls all
four jobs' `summarize`, so it must stay until every job is migrated.

## Rust side already in place for analytics

- CLI `rust-pipeline analytics <job>` and `validate --job <job>` (`rust_pipeline/src/main.rs:37-41,77-96`).
- Registry and dispatcher already list all four jobs (`rust_pipeline/src/analytics/mod.rs:9-10,49-78`).
- `run` reads the **Rust** tables `vehicle_telemetry_rust` and `vehicle_state_rust` and overwrites `analytics/<job>_rust` (`rust_pipeline/src/analytics/mod.rs:82-111`, `rust_pipeline/src/config.rs:225-232`).
- Parity tests against the fixtures (`rust_pipeline/src/analytics/parity.rs:22-58`) using the shared comparison: key match, doubles within 1e-9, exact column names and types (`rust_pipeline/src/validate.rs:19-23,85-243`). CI runs them (`.github/workflows/ci.yml:14-36`).
- Not deployed anywhere: no k8s manifest runs `analytics` (`deploy/k8s/kustomization.yaml:9-13`).
