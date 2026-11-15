# Legacy pipeline inventory

Every job in the vehicle telemetry pipeline, as of `main` at the time of writing.
Paths are relative to the repo root; `file:N` / `file:N-M` are line references.

| # | Job | Kind | Language | Status |
|---|-----|------|----------|--------|
| 1 | [Data generator](#1-data-generator-producer) | producer | Python | not part of the migration |
| 2 | [Telemetry enrichment stream](#2-telemetry-enrichment-stream) | streaming | PySpark → ported to Rust | Rust twin passes `validate` |
| 3 | [Vehicle state MERGE stream](#3-vehicle-state-merge-stream) | streaming | PySpark → ported to Rust | Rust twin passes `validate --state` |
| 4 | [battery_health_daily](#4-battery_health_daily) | batch analytics | Scala Spark | **not ported** |
| 5 | [charging_sessions](#5-charging_sessions) | batch analytics | Scala Spark | **not ported** |
| 6 | [ota_rollout](#6-ota_rollout) | batch analytics | Scala Spark | **not ported** |
| 7 | [diagnostic_alerts](#7-diagnostic_alerts) | batch analytics | Scala Spark | **not ported** |
| 8 | [BuildParityFixtures](#8-buildparityfixtures-test-tooling) | test tooling | Scala Spark | stays until all analytics jobs are ported |

Status sources: `README.md:55-65`; each Rust analytics file is still a stub that
`bail!`s and keeps its parity test `#[ignore]`d (`rust_pipeline/src/analytics/ota_rollout.rs:14-25`,
same shape in `battery_health_daily.rs`, `charging_sessions.rs`, `diagnostic_alerts.rs`); the
dashboard counts a job as ported only when that `#[ignore` disappears (`dashboard/app.py:462-468`).

Types below are Arrow types read from the committed Spark-written fixtures
(`rust_pipeline/tests/fixtures/input/*` and `rust_pipeline/tests/fixtures/expected/*`), which are
what the Rust parity check compares against (`rust_pipeline/src/analytics/parity.rs:37-57`).
"NOT NULL" is shown only where Spark marks the column non-nullable.

---

## Kafka topics

| Topic | Key | Partitions | Retention / cleanup | Defined in |
|---|---|---|---|---|
| `vehicle_telemetry` | VIN | 8 | 7 days, delete | `docker-compose.yml:65-71`, `config/app_config.yaml:8,11` |
| `vehicle_state_updates` | VIN | 8 | 7 days, **compacted** | `docker-compose.yml:74-81`, `config/app_config.yaml:10` |

In Kubernetes the brokers are external (`kafka-bootstrap.kafka.svc:9092`, `deploy/k8s/app_config.yaml:4-9`);
topic creation is only scripted for docker-compose (`docker-compose.yml:54-89`).

## 1. Data generator (producer)

- **What:** synthetic EV fleet; for every telemetry event it also publishes a latest-state snapshot
  (`data_generator/generator.py:206-279`, `data_generator/schemas.py:1-16`).
- **Writes:** `vehicle_telemetry` (key = VIN, JSON event) and `vehicle_state_updates`
  (key = VIN, JSON snapshot) (`data_generator/generator.py:272-274`).
- **Ordering:** one producer per VIN shard, `max_in_flight_requests_per_connection=1`
  (`data_generator/generator.py:242-256`, `data_generator/generator.py:290`); per-VIN monotonic
  `sequence_number` (`data_generator/generator.py:143-154`).
- **Event shape:** `event_type` ∈ BATTERY/CHARGING/DIAGNOSTIC/OTA selects which field group is
  populated, the rest are null (`data_generator/generator.py:161-183`, `README.md:229-247`).
  `event_time` is an ISO-8601 string with millisecond precision and a `Z` suffix
  (`data_generator/generator.py:58-59`).
- **Deployed:** run by hand (`python -m data_generator.run`, `README.md:174-175`) or from the
  dashboard's Data Generator page (`dashboard/app.py:868`). Not containerised in `deploy/k8s/`.

## 2. Telemetry enrichment stream

| | Spark (legacy) | Rust (ported) |
|---|---|---|
| Code | `spark_jobs/streaming_job.py`, transforms in `spark_jobs/transforms.py` | `rust_pipeline/src/consumer.rs`, `rust_pipeline/src/transforms/*.rs`, schema `rust_pipeline/src/schema.rs:150-218` |
| Input | Kafka `vehicle_telemetry`, `startingOffsets=earliest` (`spark_jobs/streaming_job.py:186-195`) | same topic, own consumer group `rust-vehicle-telemetry-consumer` (`config/app_config.yaml:74`, `rust_pipeline/src/consumer.rs:40-60`) |
| Output table | `delta_output/vehicle_telemetry` (`config/app_config.yaml:36,39`) | `delta_output/vehicle_telemetry_rust` (`config/app_config.yaml:77-78`) |
| Write mode | Delta append, `mergeSchema` (`spark_jobs/streaming_job.py:240-247`) | Delta append (`rust_pipeline/src/writer.rs:20-31`) |
| Partition columns | `event_year`, `event_month`, `event_type` (`spark_jobs/streaming_job.py:245`) | same (`rust_pipeline/src/consumer.rs:33-37`) |
| Exactly-once | Structured Streaming checkpoint (`spark_jobs/streaming_job.py:307-313`) | offsets in SQLite after each Delta write (`rust_pipeline/src/checkpoint.rs`, `README.md:100-104`) |
| Deployed | `spark-submit` on the docker-compose Spark standalone cluster (`README.md:176-180`, `docker-compose.yml:92-160`; dashboard runner `dashboard/app.py:249-262`) | Kubernetes Deployment `telemetry-stream`, 1 replica, `Recreate` (`deploy/k8s/telemetry-stream.yaml:1-74`); docker-compose `rust-pipeline` container (`docker-compose.yml:162-178`) |

**What it does:** parse JSON (`spark_jobs/streaming_job.py:202-216`), apply T1–T7
(`spark_jobs/transforms.py:190-201`), drop `kafka_key` (`spark_jobs/streaming_job.py:233`).
T5 coarsens GPS and drops `latitude`/`longitude` (`spark_jobs/transforms.py:159-166`); T6 replaces
`vin` with `vin_hash = sha2(vin, 256)` and `vin_wmi` (`spark_jobs/transforms.py:169-176`).

**Output columns** (`rust_pipeline/tests/fixtures/input/telemetry/`, Spark-written):

| Group | Columns (type) |
|---|---|
| Identity | `vin_hash` string, `event_id` string, `row_id` string, `vin_wmi` string |
| Envelope | `event_type` string, `event_time` string (ISO text, not a timestamp), `sequence_number` int64 |
| Vehicle context | `model`, `model_variant`, `software_version`, `power_state` string; `odometer_km` double |
| BATTERY | `charge_percent`, `remaining_range`, `kwhr`, `capacity_kwhr`, `battery_health_level`, `max_cell_temp`, `min_cell_temp` double; `preconditioning_status`, `battery_pack_type` string |
| CHARGING | `charge_state`, `charge_session_id` string; `charging_power_kw`, `energy_added_kwh` double |
| DIAGNOSTIC | `ecu`, `dtc_code`, `warning_state` string |
| OTA | `update_state`, `target_software_version` string; `update_progress_percent` int32 |
| Kafka metadata | `topic` string, `kafka_partition` int32, `kafka_offset` int64, `kafka_timestamp` timestamp[us, UTC] |
| T1 null defaults | `model_safe`, `software_version_safe`, `power_state_safe` string |
| T2 time | `event_date` date32, `event_year`, `event_month`, `event_day`, `event_hour`, `event_dayofweek`, `event_quarter` int32, `is_weekend` bool, `date_str` string |
| T3 battery | `soc_bucket` string; `energy_pct`, `cell_temp_spread`, `degradation_pct`, `km_per_percent` double |
| T4 health | `health_tier` string; `thermal_alert`, `is_fault`, `needs_service` bool; `severity_level` int32 |
| T5 location | `lat_coarse`, `lon_coarse` double |
| T7 | `sw_major`, `sw_minor` int32 |

## 3. Vehicle state MERGE stream

| | Spark (legacy) | Rust (ported) |
|---|---|---|
| Code | `spark_jobs/vehicle_state_job.py` | `rust_pipeline/src/vehicle_state.rs`, schema `rust_pipeline/src/schema.rs:220-241` |
| Input | Kafka `vehicle_state_updates` (`spark_jobs/vehicle_state_job.py:160-172`) | same topic, group `rust-vehicle-state-consumer` (`config/app_config.yaml:85`) |
| Output table | `delta_output/vehicle_state` (`config/app_config.yaml:56-57`) | `delta_output/vehicle_state_rust` (`config/app_config.yaml:81-82`) |
| Write mode | create, then Delta MERGE on `vin`, update only when `source.sequence_number > target.sequence_number` (`spark_jobs/vehicle_state_job.py:120-143`) | same rule (`rust_pipeline/src/vehicle_state.rs:1-10`, `rust_pipeline/src/vehicle_state.rs:183-215`) |
| Partition columns | `model` (`spark_jobs/vehicle_state_job.py:125`) | `model` (`rust_pipeline/src/vehicle_state.rs:124`) |
| Deployed | `spark-submit` on the docker-compose cluster (`README.md:181-184`) | Kubernetes Deployment `vehicle-state-stream`, 1 replica, `Recreate` (`deploy/k8s/vehicle-state-stream.yaml:1-74`) |

**What it does:** collapses each micro-batch to the newest snapshot per VIN and sums `event_count`
(`spark_jobs/vehicle_state_job.py:104-117`), then MERGEs. One row per **raw VIN** by design: it is the
operational lookup for service and warranty (`README.md:109-113`).

**Output columns** (`rust_pipeline/tests/fixtures/input/vehicle_state/`): `vin` string, `model` string,
`model_variant` string, `battery_pack_type` string, `software_version` string, `power_state` string,
`odometer_km` double, `charge_percent` double, `battery_health_level` double, `last_event_type` string,
`sequence_number` int64, `updated_at` string, `event_count` int32, `first_seen_at` string.

---

## Batch analytics jobs (Scala, not ported)

Shared facts for jobs 4–7:

- **Code:** one Scala object per job in `spark_jobs/analytics/src/main/scala/telemetry/analytics/`,
  all extending the `AnalyticsJob` trait (`spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala:70-97`).
  Each has a pure `summarize(...)` used by both `build` (production) and `BuildParityFixtures` (tests).
- **Build:** a single jar `telemetry-analytics_2.12-1.0.0.jar`, Scala 2.12.18, Spark 3.5.1 `Provided`,
  `-Xfatal-warnings` (`spark_jobs/analytics/build.sbt:1-18`); CI compiles it (`.github/workflows/ci.yml:50-65`).
- **Session:** `spark.sql.session.timeZone=UTC`, `spark.sql.shuffle.partitions` from config (8),
  Delta extensions (`AnalyticsJob.scala:55-68`, `config/app_config.yaml:41`). ANSI mode is not set, so
  Spark 3.5's default (`spark.sql.ansi.enabled=false`) applies.
- **Input:** the **Spark** telemetry Delta table (`spark.delta_output_path`, or `spark.local_delta_output`
  with `--local`) and, for diagnostic_alerts, the Spark `vehicle_state` table (`AnalyticsJob.scala:44-53`).
- **Output:** Delta, `mode("overwrite")`, `overwriteSchema=true`, at `analytics.output_base/<job>`,
  partitioned by the job's `partitionCols` (`AnalyticsJob.scala:80-96`, `config/app_config.yaml:59-62`).
  Full recompute every run; no incremental state.
- **Deployed:** no scheduler, CronJob or k8s manifest exists for them (`deploy/k8s/kustomization.yaml:9-13`
  lists only the two streams). They run on demand with `spark-submit --class telemetry.analytics.<Job>`
  on the docker-compose Spark cluster (`README.md:81-92`) or from the dashboard's Pipeline Jobs page,
  which builds the same command (`dashboard/app.py:244-246`, `dashboard/app.py:275-284`).
- **Readers:** the dashboard Fleet page (`dashboard/app.py:637-760`, Spark or Rust output via a toggle at
  `dashboard/app.py:644-648`) and, per `README.md:4-7`, downstream service, warranty and OTA systems
  (not in this repo). No analytics job reads another analytics job's output.
- **Rust placeholders:** `rust_pipeline/src/analytics/<job>.rs`, registered in
  `rust_pipeline/src/analytics/mod.rs:12-15,49-54,70-78`. A ported job writes `<job>_rust`
  (`rust_pipeline/src/config.rs:229-232`) from the **Rust** upstream tables
  (`rust_pipeline/src/analytics/mod.rs:82-95`).

### 4. battery_health_daily

- **Code:** `spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala`
- **Does:** daily battery summary per vehicle, 3-day peak cell temperature, day-over-day change in
  end-of-day health ("for warranty and service", `BatteryHealthDaily.scala:7`).
- **Inputs:** telemetry, `event_type = 'BATTERY'` rows only (`BatteryHealthDaily.scala:15-16`).
- **Output:** `analytics/battery_health_daily`, key (`vin_hash`, `event_date`)
  (`rust_pipeline/src/analytics/battery_health_daily.rs:8-12`), **partitioned by `event_date`**
  (`BatteryHealthDaily.scala:10`).

| Column | Type |
|---|---|
| vin_hash | string |
| event_date | date32 (partition) |
| model_safe | string |
| readings | int64 NOT NULL |
| avg_charge_percent | double |
| min_charge_percent | double |
| peak_cell_temp | double |
| avg_cell_temp_spread | double |
| thermal_alert_count | int64 |
| health_level_end_of_day | double |
| peak_cell_temp_3d | double |
| health_change | double |

Spec: [jobs/battery_health_daily.md](jobs/battery_health_daily.md)

### 5. charging_sessions

- **Code:** `spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala`
- **Does:** one row per charging session: duration, energy, peak/average power, start/end SoC, end
  state, faults (`ChargingSessions.scala:6`, `README.md:76`).
- **Inputs:** telemetry, `event_type = 'CHARGING' AND charge_session_id IS NOT NULL` (`ChargingSessions.scala:12-13`).
- **Output:** `analytics/charging_sessions`, key `charge_session_id`
  (`rust_pipeline/src/analytics/charging_sessions.rs:8-12`), **partitioned by `session_date`**
  (`ChargingSessions.scala:9`).

| Column | Type |
|---|---|
| charge_session_id | string |
| vin_hash | string |
| model_safe | string |
| session_start | timestamp[us, UTC] |
| session_end | timestamp[us, UTC] |
| events | int64 NOT NULL |
| energy_added_kwh | double |
| peak_power_kw | double |
| soc_start | double |
| soc_end | double |
| end_state | string |
| had_fault | bool |
| duration_minutes | double |
| avg_power_kw | double |
| session_date | date32 (partition) |

Spec: [jobs/charging_sessions.md](jobs/charging_sessions.md)

### 6. ota_rollout

- **Code:** `spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala`
- **Does:** latest OTA update state per vehicle and target version, pivoted into one count column per
  state, with success and failure rates per (target version, model) (`OtaRollout.scala:7`, `README.md:77`).
- **Inputs:** telemetry, `event_type = 'OTA'` rows only (`OtaRollout.scala:31-32`).
- **Output:** `analytics/ota_rollout`, key (`target_software_version`, `model_safe`)
  (`rust_pipeline/src/analytics/ota_rollout.rs:8-12`), **not partitioned** (`AnalyticsJob.scala:77` default).

| Column | Type |
|---|---|
| target_software_version | string |
| model_safe | string |
| UPDATE_STATE_IN_PROGRESS | int64 NOT NULL |
| UPDATE_STATE_WAITING_ON_BCM | int64 NOT NULL |
| UPDATE_STATE_SUCCESS | int64 NOT NULL |
| UPDATE_SUCCESS_WITH_WARNINGS | int64 NOT NULL |
| UPDATE_STATE_FAILED | int64 NOT NULL |
| UPDATE_FAILED_DRIVE_ALLOWED | int64 NOT NULL |
| UPDATE_FAILED_NO_ACTION | int64 NOT NULL |
| vehicles | int64 NOT NULL |
| success_rate_pct | double |
| failure_rate_pct | double |

Spec: [jobs/ota_rollout.md](jobs/ota_rollout.md)

### 7. diagnostic_alerts

- **Code:** `spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala`
- **Does:** fleet impact of each diagnostic trouble code: events, vehicles affected, share of fleet,
  rank within ECU ("for service triage", `DiagnosticAlerts.scala:7`).
- **Inputs:** telemetry `event_type = 'DIAGNOSTIC'` rows **and the `vehicle_state` table**
  (`DiagnosticAlerts.scala:11-20`, `DiagnosticAlerts.scala:36-40`). It is the only analytics job that
  reads `vehicle_state`, and therefore the only one that touches raw VINs (it hashes them,
  `DiagnosticAlerts.scala:12-13`).
- **Output:** `analytics/diagnostic_alerts`, key (`dtc_code`, `ecu`, `model_safe`, `battery_pack_type`)
  (`rust_pipeline/src/analytics/diagnostic_alerts.rs:8-12`), **not partitioned**.

| Column | Type |
|---|---|
| dtc_code | string |
| ecu | string |
| model_safe | string |
| battery_pack_type | string (null for vehicles without a state row) |
| events | int64 NOT NULL |
| vehicles_affected | int64 NOT NULL |
| warning_on_events | int64 |
| first_seen | timestamp[us, UTC] |
| last_seen | timestamp[us, UTC] |
| fleet_pct_affected | double |
| rank_in_ecu | int32 NOT NULL |

Spec: [jobs/diagnostic_alerts.md](jobs/diagnostic_alerts.md)

### 8. BuildParityFixtures (test tooling)

- **Code:** `spark_jobs/analytics/src/main/scala/telemetry/analytics/BuildParityFixtures.scala`
- **Does:** samples the first 150 vehicles by `vin_hash` from the Spark telemetry and vehicle_state
  tables into `rust_pipeline/tests/fixtures/input/`, then runs **all four** `summarize` functions on that
  input and writes `rust_pipeline/tests/fixtures/expected/<job>/` (`BuildParityFixtures.scala:8-17`,
  `BuildParityFixtures.scala:34-44`, `BuildParityFixtures.scala:46-65`). Writes Parquet with
  `TIMESTAMP_MICROS` (`BuildParityFixtures.scala:50-51`). `--expected-only` keeps the curated input
  (`BuildParityFixtures.scala:53`).
- **Deployed:** run by hand with `spark-submit --master 'local[*]'` (`README.md:128-136`).
- **Fixture contents** (measured on the committed files): 5,763 telemetry rows for 150 vehicles
  (2,867 BATTERY, 1,412 CHARGING, 897 DIAGNOSTIC, 587 OTA), 147 vehicle_state rows (3 vehicles have no
  state row), event times 2026-11-05T21:34Z to 2026-11-08T14:34Z.
