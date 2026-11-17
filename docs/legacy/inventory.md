# Legacy pipeline inventory

Every job that produces or consumes pipeline data, in any language, as of `main`.
Each claim cites `path:line`. `…/analytics/` means
`spark_jobs/analytics/src/main/scala/telemetry/analytics/`.

Related: [dependencies.md](dependencies.md) (dependency map),
[migration-slices.md](migration-slices.md) (proposed slices) and the per-job specs in [jobs/](jobs/).

## Summary

| # | Job | Kind | Language | Reads | Writes | Partition columns | Deployed by |
|---|-----|------|----------|-------|--------|-------------------|-------------|
| 1 | `data_generator` | Producer | Python | — | Kafka `vehicle_telemetry`, `vehicle_state_updates` | Kafka key = VIN | Host process `python -m data_generator.run`, or the dashboard's Data Generator page |
| 2 | `streaming_job.py` | Streaming | PySpark | Kafka `vehicle_telemetry` | Delta `vehicle_telemetry` (append) | `event_year`, `event_month`, `event_type` | `spark-submit` on the Docker Spark cluster |
| 3 | `vehicle_state_job.py` | Streaming | PySpark | Kafka `vehicle_state_updates` | Delta `vehicle_state` (MERGE) | `model` | `spark-submit` on the Docker Spark cluster |
| 4 | `BatteryHealthDaily` | Batch analytics | Scala Spark | Delta `vehicle_telemetry` | Delta `analytics/battery_health_daily` (overwrite) | `event_date` | `spark-submit --class telemetry.analytics.BatteryHealthDaily` |
| 5 | `ChargingSessions` | Batch analytics | Scala Spark | Delta `vehicle_telemetry` | Delta `analytics/charging_sessions` (overwrite) | `session_date` | `spark-submit --class telemetry.analytics.ChargingSessions` |
| 6 | `OtaRollout` | Batch analytics | Scala Spark | Delta `vehicle_telemetry` | Delta `analytics/ota_rollout` (overwrite) | none | `spark-submit --class telemetry.analytics.OtaRollout` |
| 7 | `DiagnosticAlerts` | Batch analytics | Scala Spark | Delta `vehicle_telemetry`, `vehicle_state` | Delta `analytics/diagnostic_alerts` (overwrite) | none | `spark-submit --class telemetry.analytics.DiagnosticAlerts` |
| 8 | `BuildParityFixtures` | Batch utility | Scala Spark | Delta `vehicle_telemetry`, `vehicle_state` | Parquet under `rust_pipeline/tests/fixtures/` | none | `spark-submit` by hand |
| 9 | `rust-pipeline stream` | Streaming (port of 2) | Rust | Kafka `vehicle_telemetry` | Delta `vehicle_telemetry_rust` (append) | `event_year`, `event_month`, `event_type` | Kubernetes Deployment `telemetry-stream`; Docker `rust-pipeline` container |
| 10 | `rust-pipeline state` | Streaming (port of 3) | Rust | Kafka `vehicle_state_updates` | Delta `vehicle_state_rust` (MERGE) | `model` | Kubernetes Deployment `vehicle-state-stream`; Docker `rust-pipeline` container |
| 11 | `rust-pipeline analytics <job>` | Batch analytics (ports of 4–7) | Rust | Delta `vehicle_telemetry_rust`, `vehicle_state_rust` | Delta `analytics/<job>_rust` (overwrite) | same as the Spark job | CLI only. **All four are stubs.** |
| 12 | `rust-pipeline validate` | Parity check | Rust | A Spark table and its Rust twin | JSON report in `logs/` | — | CLI; the fixture form runs in CI via `cargo test` |
| 13 | Dashboard | Consumer and launcher | Python (Streamlit) | Every Delta table, metrics JSON | — (starts jobs) | — | Docker `dashboard` service |
| 14 | `metrics_bridge.py`, `spark_metrics_collector.py` | Metrics sidecars | Python | Spark REST API | JSON under `logs/` | — | Imported by jobs 2 and 3 |
| 15 | `scripts/load_demo_data.py` | Demo loader | Python | Parquet fixtures | Every Delta table except `analytics/<job>_rust` | same as the real tables | Run by hand |

Sources: `README.md:39-53` (architecture), `README.md:55-65` (migration status), `docker-compose.yml:92-197`,
`deploy/k8s/kustomization.yaml:9-13`, `dashboard/app.py:178-207` (analytics registry), `dashboard/app.py:249-300`
(Spark and Rust job commands), `rust_pipeline/src/main.rs:31-57` (Rust subcommands).

## Shared infrastructure

### Kafka topics

| Topic | Key | Partitions | Retention | Producer | Consumers | Source |
|-------|-----|-----------|-----------|----------|-----------|--------|
| `vehicle_telemetry` | VIN | 8 | 7 days, delete | `data_generator` | `streaming_job.py`, `rust-pipeline stream` (consumer group `rust-vehicle-telemetry-consumer`) | `config/app_config.yaml:8,11,74`, `docker-compose.yml:64-71`, `data_generator/generator.py:272` |
| `vehicle_state_updates` | VIN | 8 | 7 days, `cleanup.policy=compact` | `data_generator` | `vehicle_state_job.py`, `rust-pipeline state` (group `rust-vehicle-state-consumer`) | `config/app_config.yaml:10,85`, `docker-compose.yml:73-81`, `data_generator/generator.py:273` |

The Rust consumers assign every partition to themselves and seek to offsets stored in SQLite, rather than using
consumer-group rebalancing (`rust_pipeline/src/consumer.rs:53-59`, `rust_pipeline/src/vehicle_state.rs:76-77`).
That is why each Kubernetes Deployment runs one replica with the `Recreate` strategy (`deploy/k8s/telemetry-stream.yaml:1-16`).

### Table locations

Paths come from `config/app_config.yaml`. Container paths start with `/opt/delta_output`. Local runs
(`--local` for Spark, no `/opt/config` for Rust) use `./delta_output`.

| Table | Cluster path | Local path | Source |
|-------|--------------|-----------|--------|
| `vehicle_telemetry` | `/opt/delta_output/vehicle_telemetry` | `./delta_output/vehicle_telemetry` | `config/app_config.yaml:36,39`, `…/analytics/AnalyticsJob.scala:49` |
| `vehicle_state` | `/opt/delta_output/vehicle_state` | `./delta_output/vehicle_state` | `config/app_config.yaml:56-57`, `…/analytics/AnalyticsJob.scala:50` |
| `analytics/<job>` | `/opt/delta_output/analytics/<job>` | `./delta_output/analytics/<job>` | `config/app_config.yaml:59-62`, `…/analytics/AnalyticsJob.scala:51-52` |
| `vehicle_telemetry_rust` | `/opt/delta_output/vehicle_telemetry_rust` | `./delta_output/vehicle_telemetry_rust` | `config/app_config.yaml:77-78`, `rust_pipeline/src/config.rs:158-164` |
| `vehicle_state_rust` | `/opt/delta_output/vehicle_state_rust` | `./delta_output/vehicle_state_rust` | `config/app_config.yaml:81-82`, `rust_pipeline/src/config.rs:174-180` |
| `analytics/<job>_rust` | `<analytics base>/<job>_rust` | same | `rust_pipeline/src/config.rs:229-241` |

Rust decides between cluster and local paths by checking whether `/opt/config` or `/opt/spark-jobs` exists
(`rust_pipeline/src/config.rs:146-148`). Spark decides with the `--local` flag
(`…/analytics/AnalyticsJob.scala:45-47`, `spark_jobs/streaming_job.py:167-174`).

All Spark sessions set `spark.sql.session.timeZone=UTC` (`…/analytics/AnalyticsJob.scala:63`,
`spark_jobs/streaming_job.py:136`, `spark_jobs/vehicle_state_job.py:98`).

---

## 1. `data_generator` (Python)

- **What it does:** simulates a fleet (2,000 vehicles by default) and publishes one telemetry event plus one state snapshot per
  step (`config/app_config.yaml:15-26`, `data_generator/generator.py:258-278`).
- **Event mix:** `BATTERY` 50%, `CHARGING` 25%, `DIAGNOSTIC` 15%, `OTA` 10% (`data_generator/schemas.py:101-106`). Each type
  fills only its own fields (`data_generator/generator.py:69-135`), so a field from another type is null on that row.
- **Event fields that affect analytics:**
  - `event_time` is an ISO-8601 string in UTC with milliseconds and `Z`, for example `2026-11-05T10:00:00.123Z` (`data_generator/generator.py:58-59`).
  - `sequence_number` increases by 1 per event for each VIN. It starts from wall-clock seconds × 10⁶ (`data_generator/generator.py:143-154`).
  - A charging session id is created on the first `CHARGING` event. It is cleared after any state other than CHARGING, CABLE_CONNECTED, PAUSED or SCHEDULED (`data_generator/generator.py:87-105`).
  - OTA events carry `update_state`, `target_software_version` and `update_progress_percent` (`data_generator/generator.py:116-127`).
- **Outputs:** Kafka, keyed by VIN (`data_generator/generator.py:246`, `272-273`). One producer per vehicle shard keeps per-VIN
  order (`data_generator/generator.py:8-11`, `README.md:96-99`).
- **Deployment:** `python -m data_generator.run` (`data_generator/run.py:5-7`). It also runs as a subprocess of the dashboard
  container (`dashboard/requirements.txt:8`).

## 2. `spark_jobs/streaming_job.py`: telemetry enrichment (PySpark)

- **What it does:** Structured Streaming from Kafka. It parses JSON with a fixed schema, applies transforms T1–T7, drops the
  Kafka key (the VIN) and appends to Delta (`spark_jobs/streaming_job.py:186-247`).
- **Input:** Kafka `vehicle_telemetry`, `startingOffsets=earliest`, `maxOffsetsPerTrigger=500000`, 10 s trigger
  (`spark_jobs/streaming_job.py:186-195`, `config/app_config.yaml:32,42`). Payload schema: `spark_jobs/streaming_job.py:83-115`.
- **Transforms** (`spark_jobs/transforms.py:190-201`):
  - T1 null defaults: `model_safe = coalesce(model, 'MODEL_UNKNOWN')`, `software_version_safe`, `power_state_safe`, `odometer_km` filled with 0 (`spark_jobs/transforms.py:69-79`).
  - T2 UTC calendar parts: `event_date`, `event_year`, `event_month`, `event_day`, `event_hour`, `event_dayofweek`, `is_weekend`, `event_quarter`, `date_str` (`spark_jobs/transforms.py:82-96`).
  - T3 battery features: `soc_bucket`, `energy_pct`, `cell_temp_spread`, `degradation_pct`, `km_per_percent` (`spark_jobs/transforms.py:99-124`).
  - T4 flags: `health_tier` (Python UDF), `thermal_alert`, `is_fault`, `needs_service`, `severity_level`. All boolean flags treat a null comparison as `false` (`spark_jobs/transforms.py:46-62`, `127-156`).
  - T5 GPS rounded to 2 dp as `lat_coarse` / `lon_coarse`; `latitude` and `longitude` dropped (`spark_jobs/transforms.py:159-166`).
  - T6 `vin_hash = sha2(vin, 256)`, `vin_wmi` = first 3 characters of the VIN; `vin` dropped (`spark_jobs/transforms.py:169-176`).
  - T7 `row_id = sha2(event_id, 256)`, `sw_major`, `sw_minor` (`spark_jobs/transforms.py:179-187`).
- **Output:** Delta `vehicle_telemetry`, append with `mergeSchema`, partitioned by `event_year`, `event_month`, `event_type`
  (`spark_jobs/streaming_job.py:240-247`). There are 61 columns. The fixture copy shows the exact types: for example `event_time` is
  a string, `sequence_number` int64, `event_date` date32, `kafka_timestamp` timestamp[us, UTC], and the flags are bool
  (`rust_pipeline/tests/fixtures/input/telemetry/part-00000-edge-cases.snappy.parquet`). `event_time` stays a string, so
  every analytics job that needs a timestamp parses it again with `to_timestamp`.
- **Deployment:** `spark-submit` in `spark-master` (`spark_jobs/streaming_job.py:27-29`, `dashboard/app.py:337-348`). Checkpoint at
  `/opt/checkpoints/vehicle_telemetry` (`config/app_config.yaml:34`).

## 3. `spark_jobs/vehicle_state_job.py`: vehicle-state MERGE (PySpark)

- **What it does:** collapses each micro-batch to the newest snapshot per VIN, then MERGEs it into a one-row-per-VIN table. A row is
  only updated when the snapshot's `sequence_number` is greater than the stored one (`spark_jobs/vehicle_state_job.py:104-143`).
- **Input:** Kafka `vehicle_state_updates`, rows with a null VIN dropped (`spark_jobs/vehicle_state_job.py:160-172`). Schema:
  `spark_jobs/vehicle_state_job.py:58-73`.
- **Output:** Delta `vehicle_state`. It has 14 columns: `vin` string (raw, by design per `README.md:107-113`), `model`,
  `model_variant`, `battery_pack_type`, `software_version`, `power_state`, `odometer_km` double, `charge_percent` double,
  `battery_health_level` double, `last_event_type`, `sequence_number` int64, `updated_at` string, `event_count` int32,
  `first_seen_at` string (`rust_pipeline/tests/fixtures/input/vehicle_state/part-00000-edge-cases.snappy.parquet`).
  It is partitioned by `model` when the table is first created (`spark_jobs/vehicle_state_job.py:125`).
- **Deployment:** `spark-submit` (`spark_jobs/vehicle_state_job.py:19-21`).

## 4–7. Scala batch analytics jobs

All four share `AnalyticsJob` (`…/analytics/AnalyticsJob.scala:75-97`):

- It parses `--config` and `--local` (`…/analytics/AnalyticsJob.scala:9-24`) and resolves paths (`…/analytics/AnalyticsJob.scala:45-53`).
- It builds a SparkSession with Delta, `shuffle_partitions` and UTC (`…/analytics/AnalyticsJob.scala:55-68`).
- It writes `build()` as Delta with `mode("overwrite")` and `overwriteSchema=true`, partitioned by `partitionCols` if there are any
  (`…/analytics/AnalyticsJob.scala:86-91`). It then re-reads the table and prints the row count (`…/analytics/AnalyticsJob.scala:93-94`).

Each job keeps its logic in a pure `summarize` function, so `BuildParityFixtures` can call it on fixture data.
Built as one jar by `spark_jobs/analytics/build.sbt` (Spark 3.5.1, Scala 2.12.18).

| Job | Purpose | Inputs | Output key | Output columns (type, `!` = non-null) | Partitioned by | Spec |
|-----|---------|--------|-----------|----------------------------------------|----------------|------|
| `battery_health_daily` | Daily battery summary per vehicle, 3-day peak cell temperature, day-over-day health change (`…/analytics/BatteryHealthDaily.scala:7-41`) | `vehicle_telemetry` where `event_type = 'BATTERY'` | `vin_hash, event_date` | `vin_hash` string, `event_date` date32, `model_safe` string, `readings` int64!, `avg_charge_percent` double, `min_charge_percent` double, `peak_cell_temp` double, `avg_cell_temp_spread` double, `thermal_alert_count` int64, `health_level_end_of_day` double, `peak_cell_temp_3d` double, `health_change` double | `event_date` (`…/analytics/BatteryHealthDaily.scala:10`) | [jobs/battery_health_daily.md](jobs/battery_health_daily.md) |
| `charging_sessions` | One row per charging session (`…/analytics/ChargingSessions.scala:6-38`) | `vehicle_telemetry` where `event_type = 'CHARGING'` and the session id is not null | `charge_session_id` | `charge_session_id` string, `vin_hash` string, `model_safe` string, `session_start` timestamp[us, UTC], `session_end` timestamp[us, UTC], `events` int64!, `energy_added_kwh` double, `peak_power_kw` double, `soc_start` double, `soc_end` double, `end_state` string, `had_fault` bool, `duration_minutes` double, `avg_power_kw` double, `session_date` date32 | `session_date` (`…/analytics/ChargingSessions.scala:9`) | [jobs/charging_sessions.md](jobs/charging_sessions.md) |
| `ota_rollout` | Latest OTA state per vehicle, pivoted by target version and model, with success and failure rates (`…/analytics/OtaRollout.scala:7-44`) | `vehicle_telemetry` where `event_type = 'OTA'` | `target_software_version, model_safe` | `target_software_version` string, `model_safe` string, seven state columns `UPDATE_STATE_IN_PROGRESS` … `UPDATE_FAILED_NO_ACTION` int64!, `vehicles` int64!, `success_rate_pct` double, `failure_rate_pct` double | none | [jobs/ota_rollout.md](jobs/ota_rollout.md) |
| `diagnostic_alerts` | Fleet impact of each trouble code (`…/analytics/DiagnosticAlerts.scala:7-34`) | `vehicle_telemetry` where `event_type = 'DIAGNOSTIC'`, plus `vehicle_state` | `dtc_code, ecu, model_safe, battery_pack_type` | `dtc_code` string, `ecu` string, `model_safe` string, `battery_pack_type` string, `events` int64!, `vehicles_affected` int64!, `warning_on_events` int64, `first_seen` timestamp[us, UTC], `last_seen` timestamp[us, UTC], `fleet_pct_affected` double, `rank_in_ecu` int32! | none | [jobs/diagnostic_alerts.md](jobs/diagnostic_alerts.md) |

Output columns and types were read from Spark's own output in `rust_pipeline/tests/fixtures/expected/<job>/*.parquet`. They are the
contract the Rust ports must match, because analytics parity checks types (`rust_pipeline/src/analytics/mod.rs:36-45`).
Keys come from `rust_pipeline/src/analytics/<job>.rs:8-12`. Partition columns are also asserted in
`rust_pipeline/src/analytics/mod.rs:121-132`.

**Deployment of all four:** there is no schedule or orchestrator in the repository, and Kubernetes deploys only the two Rust
streams (`deploy/k8s/kustomization.yaml:9-13`). The jobs are built with `sbt package` (`README.md:81-84`, CI
`.github/workflows/ci.yml:50-65`). They are run by hand with `spark-submit --class telemetry.analytics.<Job>` on the Docker
cluster (`README.md:86-91`) or from the dashboard's Pipeline Jobs page (`dashboard/app.py:245-280`, `337-348`).

## 8. `BuildParityFixtures` (Scala utility)

- **What it does:** samples the first `--vehicles` (default 150) `vin_hash` values from the telemetry table, plus those vehicles'
  `vehicle_state` rows (matched via `sha2(vin, 256)`). It writes each sample as one Parquet file, then runs every job's `summarize`
  on the fixtures and writes the expected outputs (`…/analytics/BuildParityFixtures.scala:18-65`, `…/analytics/AnalyticsJob.scala:13`).
  `--expected-only` keeps the input and rebuilds only the expected outputs (`…/analytics/BuildParityFixtures.scala:15-16`).
- Timestamps are written as `TIMESTAMP_MICROS`, not INT96 (`…/analytics/BuildParityFixtures.scala:51`).
- The committed input also contains hand-made edge cases: missing days, null readings at the lowest or highest
  `sequence_number`, and vehicles with telemetry but no state row (commit `32e0875`, `README.md:124-127`). Measured in
  the fixtures: 5,763 telemetry rows for 150 vehicles, and 147 `vehicle_state` rows.
- **Deployment:** `spark-submit --class telemetry.analytics.BuildParityFixtures` by hand (`README.md:130-134`).

## 9. `rust-pipeline stream` (Rust port of 2)

- Same transforms as job 2, one Rust module per transform (`rust_pipeline/src/transforms/mod.rs:13`). It writes
  `vehicle_telemetry_rust` partitioned by `event_year`, `event_month`, `event_type` (`rust_pipeline/src/consumer.rs:33-37`, `115`).
- One difference from Spark: `kafka_timestamp` is stored as int64 epoch milliseconds, not a timestamp
  (`rust_pipeline/src/schema.rs:187`). Parity ignores that column (`rust_pipeline/src/validate.rs:35-42`). None of the analytics
  jobs read it.
- **Deployment:** `deploy/k8s/telemetry-stream.yaml` (1 replica, `Recreate`, args `stream`, a 50 Gi `ReadWriteOnce` PVC from
  `deploy/k8s/storage.yaml:9`). Locally it runs in the `rust-pipeline` container, which idles with `sleep infinity` until a job is
  started (`docker-compose.yml:162-178`).

## 10. `rust-pipeline state` (Rust port of 3)

- Same collapse-and-MERGE rules as job 3 (`rust_pipeline/src/vehicle_state.rs:1-8`, `154-190`). It writes `vehicle_state_rust`
  partitioned by `model` (`rust_pipeline/src/vehicle_state.rs:124`).
- **Deployment:** `deploy/k8s/vehicle-state-stream.yaml` (1 replica, args `state`, a 10 Gi `ReadWriteOnce` PVC from `deploy/k8s/storage.yaml:19`).

## 11. `rust-pipeline analytics <job>` (Rust ports of 4–7)

- **Registry:** `JOBS` lists all four jobs (`rust_pipeline/src/analytics/mod.rs:49-54`). The `summarize` dispatcher has an entry
  for each (`rust_pipeline/src/analytics/mod.rs:70-78`). `run` opens **both** `vehicle_telemetry_rust` and `vehicle_state_rust` for
  every job (`rust_pipeline/src/analytics/mod.rs:82-88`). It writes `<job>_rust` with `SaveMode::Overwrite` and the job's partition
  columns (`rust_pipeline/src/analytics/mod.rs:98-111`).
- **Status:** every job file is a stub that returns "not ported to Rust yet". Its `matches_spark_output` parity test is
  `#[ignore]`d (`rust_pipeline/src/analytics/ota_rollout.rs:14-25`, and the same lines in the other three files).
  `README.md:226-227` defines a job as ported once that `#[ignore]` is removed, and the dashboard checks for the same thing
  (`dashboard/app.py:461-468`).
- **Engine:** DataFusion 43 via `deltalake` 0.22 (`rust_pipeline/Cargo.toml:13`, `rust_pipeline/Cargo.lock`).
- **Deployment:** CLI only (`rust_pipeline/src/main.rs:37-41`), started from the dashboard's Migration page
  (`dashboard/app.py:501-510`). There is no Kubernetes manifest.

## 12. `rust-pipeline validate`

- Compares a Spark table with its Rust twin (`rust_pipeline/src/validate.rs:85-135`). It checks:
  - no forbidden columns: `vin`, `latitude`, `longitude`, `kafka_key` (`rust_pipeline/src/validate.rs:19`)
  - no missing rows on either side, matched on the key columns with a null-safe join (`rust_pipeline/src/validate.rs:96-114`, `235-243`)
  - every column equal on every row, with doubles within `1e-9` (`rust_pipeline/src/validate.rs:23`, `214-230`)
  - for analytics, the same column names and types, where timestamps only need the same unit (`rust_pipeline/src/validate.rs:92-94`, `204-211`)
- **Modes:** telemetry by default, `--state` for `vehicle_state`, `--job <name>` for analytics (`rust_pipeline/src/main.rs:80-114`).
  The fixture form runs in CI with `cargo test` (`rust_pipeline/src/analytics/parity.rs:37-58`, `.github/workflows/ci.yml:36`).

## 13. Dashboard (`dashboard/app.py`)

- Reads every Spark and Rust table in `TABLES` (`dashboard/app.py:212-229`). The Fleet page renders the four analytics tables, with
  a switch between Spark and Rust as the source (`dashboard/app.py:637-760`). Its renderers depend on output column names. For
  example, the OTA view picks up every column whose name starts with `UPDATE_` (`dashboard/app.py:714-722`).
- It also starts Spark and Rust jobs (`dashboard/app.py:249-348`, `984-1041`) and shows parity reports (`dashboard/app.py:767-866`).
- **Deployment:** Docker `dashboard` service. It mounts `rust_pipeline/src/analytics` read-only, so it can see which jobs are
  ported (`docker-compose.yml:181-197`).

## 14. Metrics sidecars

- `spark_jobs/metrics_bridge.py` writes Spark job metrics as JSON to `logs/` for the dashboard (`spark_jobs/metrics_bridge.py:1-14`).
- `spark_jobs/spark_metrics_collector.py` polls the Spark REST API on `localhost:4040` (`spark_jobs/spark_metrics_collector.py:1-10`).
- Only the streaming jobs import them (`spark_jobs/streaming_job.py:61-75`, `spark_jobs/vehicle_state_job.py:52-57`). The analytics
  jobs do not.

## 15. `scripts/load_demo_data.py`

- Builds every dashboard table from the fixtures, with no Kafka or Spark needed. Both stream tables are loaded from the same
  fixture, and `analytics/<job>` is loaded from Spark's expected output. `_rust` analytics tables are left for the ports to write
  (`scripts/load_demo_data.py:1-20`). Partitions mirror the jobs (`scripts/load_demo_data.py:35-40`).
