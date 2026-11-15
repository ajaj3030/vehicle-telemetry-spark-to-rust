# Legacy pipeline inventory

Snapshot of `main`. Every claim cites the file (and lines) it comes from.
"Spark side" is the legacy system; the Rust jobs are listed because the
analytics ports read their output.

## Kafka topics

| Topic | Key | Producer | Config | Cited in |
|---|---|---|---|---|
| `vehicle_telemetry` | VIN | `data_generator` (one producer per VIN shard) | 8 partitions, 7-day retention, 10 MB max message | `config/app_config.yaml:8-11`, `docker-compose.yml:65-71`, `data_generator/generator.py:272` |
| `vehicle_state_updates` | VIN | `data_generator`, one snapshot per telemetry event | 8 partitions, 7-day retention, `cleanup.policy=compact` | `docker-compose.yml:74-85`, `data_generator/generator.py:186-203,273` |

Per-VIN `sequence_number` increases by 1 per event (`data_generator/generator.py:150,154`).
Event schema: `spark_jobs/streaming_job.py:83-115`; state snapshot schema: `spark_jobs/vehicle_state_job.py:58-73`.

## Jobs

| # | Job | Lang / engine | Kind | Inputs | Output table | Partition cols | Deployed / run by |
|---|---|---|---|---|---|---|---|
| 1 | `spark_jobs/streaming_job.py` (telemetry enrichment) | PySpark 3.5.1 Structured Streaming | streaming, append | Kafka `vehicle_telemetry` (`streaming_job.py:187-195`) | `delta_output/vehicle_telemetry` (`config/app_config.yaml:36,39`) | `event_year`, `event_month`, `event_type` (`streaming_job.py:245`) | `spark-submit` on the docker-compose Spark cluster (`docker-compose.yml:92-131`, `README.md:178-180`) or the dashboard's Pipeline Jobs page (`dashboard/app.py:253-258`) |
| 2 | `spark_jobs/vehicle_state_job.py` (state MERGE) | PySpark 3.5.1 + Delta 3.1.0 | streaming, MERGE upsert | Kafka `vehicle_state_updates` (`vehicle_state_job.py:160-172`) | `delta_output/vehicle_state`, one row per VIN (`config/app_config.yaml:51-57`) | `model` (on first write, `vehicle_state_job.py:125`) | Same as #1 (`README.md:181-183`, `dashboard/app.py:264-270`) |
| 3 | `BatteryHealthDaily` | Scala 2.12 / Spark 3.5.1 | batch, overwrite | `vehicle_telemetry` (`BatteryHealthDaily.scala:43-44`) | `delta_output/analytics/battery_health_daily` | `event_date` (`BatteryHealthDaily.scala:10`) | `spark-submit --class telemetry.analytics.BatteryHealthDaily` from the jar built with `sbt package` (`README.md:85-92`, `dashboard/app.py:276-285`) |
| 4 | `ChargingSessions` | Scala / Spark | batch, overwrite | `vehicle_telemetry` (`ChargingSessions.scala:40-41`) | `delta_output/analytics/charging_sessions` | `session_date` (`ChargingSessions.scala:9`) | as #3 |
| 5 | `OtaRollout` | Scala / Spark | batch, overwrite | `vehicle_telemetry` (`OtaRollout.scala:46-47`) | `delta_output/analytics/ota_rollout` | none (trait default, `AnalyticsJob.scala:77`) | as #3 |
| 6 | `DiagnosticAlerts` | Scala / Spark | batch, overwrite | `vehicle_telemetry` + `vehicle_state` (`DiagnosticAlerts.scala:36-40`) | `delta_output/analytics/diagnostic_alerts` | none (`AnalyticsJob.scala:77`) | as #3 |
| 7 | `BuildParityFixtures` | Scala / Spark | batch, test tooling | `vehicle_telemetry`, `vehicle_state` (`BuildParityFixtures.scala:34-44`) | Parquet under `rust_pipeline/tests/fixtures/{input,expected}` (`BuildParityFixtures.scala:9-12`) | none | Run by hand with `spark-submit --master local[*]` (`README.md:131-136`) |
| 8 | `rust-pipeline stream` | Rust (rdkafka, delta-rs) | streaming, append | Kafka `vehicle_telemetry` | `delta_output/vehicle_telemetry_rust` (`config/app_config.yaml:77-78`) | `event_year`, `event_month`, `event_type` (`rust_pipeline/src/consumer.rs:33-37`) | k8s Deployment `telemetry-stream`, 1 replica, `Recreate` (`deploy/k8s/telemetry-stream.yaml:14-16,36`) |
| 9 | `rust-pipeline state` | Rust | streaming, MERGE | Kafka `vehicle_state_updates` | `delta_output/vehicle_state_rust` (`config/app_config.yaml:81-82`) | none set (no partition columns in `rust_pipeline/src/vehicle_state.rs`) | k8s Deployment `vehicle-state-stream` (`deploy/k8s/vehicle-state-stream.yaml:14-16,36`) |
| 10 | `rust-pipeline analytics <job>` | Rust (DataFusion via deltalake 0.22) | batch, overwrite | `vehicle_telemetry_rust`, `vehicle_state_rust` (`rust_pipeline/src/analytics/mod.rs:84-87`) | `delta_output/analytics/<job>_rust` (`rust_pipeline/src/config.rs:230-232`) | same as Spark job (`rust_pipeline/src/analytics/mod.rs:98-111,121-132`) | CLI / dashboard Migration page only; **every job is a stub** that bails (`rust_pipeline/src/analytics/ota_rollout.rs:14-17`) with its parity test `#[ignore]`d (`ota_rollout.rs:22`) |

Supporting processes (not data jobs): `spark_jobs/metrics_bridge.py` and
`spark_jobs/spark_metrics_collector.py` write JSON metrics for the dashboard
(`spark_jobs/metrics_bridge.py:1-15`); the Streamlit dashboard reads every table
(`dashboard/app.py:212-227`).

### What each streaming job does

- **Telemetry enrichment** (`spark_jobs/streaming_job.py`): parses the JSON
  value with a fixed schema, applies T1–T7 from `spark_jobs/transforms.py:190-201`
  (null defaults, time dimensions, battery features, health flags, GPS
  coarsening, VIN → `vin_hash` SHA-256, `row_id`/`sw_major`/`sw_minor`),
  drops `kafka_key` (`streaming_job.py:233`) and appends with `mergeSchema`
  (`streaming_job.py:240-247`). Checkpointed exactly-once (`streaming_job.py:310`).
  Session time zone is UTC (`streaming_job.py:136`).
- **Vehicle state** (`spark_jobs/vehicle_state_job.py`): collapses each
  micro-batch to the newest snapshot per VIN with summed `event_count` and
  earliest `first_seen_at` (`vehicle_state_job.py:104-117`), then MERGEs only
  when `source.sequence_number > target.sequence_number` (`vehicle_state_job.py:120-143`).
  The table keeps the raw `vin` by design (`README.md:111-113`).

### Shared Scala runtime for analytics jobs

All four analytics jobs extend `AnalyticsJob` (`AnalyticsJob.scala:75-97`):
`--config/--local` CLI (`:8-28`), table paths from `config/app_config.yaml`
(`:45-53`), a Spark session with Delta, `spark.sql.shuffle.partitions` from
config and **`spark.sql.session.timeZone=UTC`** (`:55-68`), then
`write.format("delta").mode("overwrite").option("overwriteSchema","true")`
partitioned by `partitionCols` (`:87-91`). Built as one jar, Spark/snakeyaml
`Provided`, `-Xfatal-warnings` (`spark_jobs/analytics/build.sbt:4-18`).

## Table schemas

### `vehicle_telemetry` (input to every analytics job)

61 columns; the fixture copy is `rust_pipeline/tests/fixtures/input/telemetry`.
Columns the analytics jobs use:

| Column | Type | Source |
|---|---|---|
| `vin_hash` | string (SHA-256 hex of VIN) | `spark_jobs/transforms.py:173` |
| `event_type` | string: BATTERY/CHARGING/DIAGNOSTIC/OTA | `streaming_job.py:86`, `data_generator/schemas.py:101-106` |
| `event_time` | **string**, ISO-8601 `yyyy-MM-ddTHH:mm:ss.SSSZ` | `streaming_job.py:87`, `data_generator/generator.py:58-59` |
| `sequence_number` | bigint | `streaming_job.py:88` |
| `event_date` | date (UTC `to_date(event_time)`) | `spark_jobs/transforms.py:87` |
| `model_safe` | string, `coalesce(model,'MODEL_UNKNOWN')` | `spark_jobs/transforms.py:73` |
| `charge_percent`, `battery_health_level`, `max_cell_temp`, `charging_power_kw`, `energy_added_kwh` | double | `streaming_job.py:96-108` |
| `cell_temp_spread` | double, `round(max-min,2)` | `spark_jobs/transforms.py:117-118` |
| `thermal_alert`, `is_fault` | boolean, null-safe (never null) | `spark_jobs/transforms.py:129-142` |
| `charge_state`, `charge_session_id`, `ecu`, `dtc_code`, `warning_state`, `update_state`, `target_software_version`, `battery_pack_type` | string | `streaming_job.py:104-113` |

### `vehicle_state`

`vin` string, `model`, `model_variant`, `battery_pack_type`, `software_version`,
`power_state` string, `odometer_km`, `charge_percent`, `battery_health_level`
double, `last_event_type` string, `sequence_number` bigint, `event_count` int,
`updated_at`, `first_seen_at` string (`spark_jobs/vehicle_state_job.py:58-73`).
Partitioned by `model` (`vehicle_state_job.py:125`).

### Analytics outputs

Types are read from Spark's own output in `rust_pipeline/tests/fixtures/expected/<job>`
(the parity gate checks them exactly, `rust_pipeline/src/validate.rs:185-203`).
Full column lists are in each spec under `jobs/`.

| Table | Key (Rust `JOB.key_cols`) | Partition | Columns |
|---|---|---|---|
| `battery_health_daily` | `vin_hash, event_date` (`README.md:76`) | `event_date` | 12 — see `jobs/battery_health_daily.md` |
| `charging_sessions` | `charge_session_id` (`README.md:77`) | `session_date` | 15 — see `jobs/charging_sessions.md` |
| `ota_rollout` | `target_software_version, model_safe` (`rust_pipeline/src/analytics/ota_rollout.rs:8-12`) | none | 12 — see `jobs/ota_rollout.md` |
| `diagnostic_alerts` | `dtc_code, ecu, model_safe, battery_pack_type` (`rust_pipeline/src/analytics/diagnostic_alerts.rs:8-12`) | none | 11 — see `jobs/diagnostic_alerts.md` |

## Deployment summary

- **Local / demo:** `docker-compose.yml` runs ZooKeeper, Kafka, topic setup,
  Spark master + 2 workers (`apache/spark:3.5.1`, `docker-compose.yml:92-131`),
  an idle `rust-pipeline` container (`command: sleep infinity`, `:162-178`) and the dashboard (`:181-206`).
- **Kubernetes:** only the two Rust stream processors (`deploy/k8s/kustomization.yaml:9-13`).
  No Spark job and no analytics job (Spark or Rust) has a manifest or schedule in the repo.
- **CI** (`.github/workflows/ci.yml`): Rust fmt/clippy/tests incl. parity (`:14-36`),
  ruff (`:38-48`), `sbt package` of the Scala jobs (`:50-65`), kustomize + kubeconform (`:67-73`),
  image build on push to `main` (`:75-89`).
- **Ownership:** the repo has no CODEOWNERS or owner metadata. The only
  team-level reference is the README naming "service, warranty and OTA
  systems" as downstream readers (`README.md:5-7`).
