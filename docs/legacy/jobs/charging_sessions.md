# charging_sessions — functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala` (line refs to this file unless stated).
Rust placeholder: `rust_pipeline/src/analytics/charging_sessions.rs`.
Expected output fixture: `rust_pipeline/tests/fixtures/expected/charging_sessions/`.

## Purpose

One row per charging session (line 6): start/end time, duration, energy added, peak and average power,
start/end state of charge, final charge state and whether any fault occurred. Dashboard: Charging tab
(`dashboard/app.py:693-712`).

## Inputs

| Table | Rows used | Columns used |
|---|---|---|
| telemetry | `event_type = 'CHARGING' AND charge_session_id IS NOT NULL` (line 13) | `charge_session_id`, `vin_hash`, `model_safe`, `event_time` (string), `energy_added_kwh`, `charging_power_kw`, `charge_percent`, `charge_state`, `sequence_number`, `is_fault` |

`event_time` is stored as an ISO-8601 **string** like `2026-11-05T21:34:25.318Z` (generator
`data_generator/generator.py:58-59`; Spark schema `spark_jobs/streaming_job.py:87`).
`is_fault` is a non-null boolean that is true for faulted charge states, warning lamps or failed updates
(`spark_jobs/transforms.py:133-142`; fault charge states at `spark_jobs/transforms.py:24-31`).
Sessions: the generator keeps one `charge_session_id` per vehicle until a terminal charge state
(`data_generator/generator.py:87-105`).

## Output

Partitioned by `session_date` (line 9); key `charge_session_id` (`rust_pipeline/src/analytics/charging_sessions.rs:8-12`).

| Column | Type | Definition |
|---|---|---|
| charge_session_id | string | group key |
| vin_hash | string | group key |
| model_safe | string | group key |
| session_start | timestamp[us, UTC] | `min(to_timestamp(event_time))` |
| session_end | timestamp[us, UTC] | `max(to_timestamp(event_time))` |
| events | int64 | `count(*)` |
| energy_added_kwh | double | `round(sum(energy_added_kwh), 3)` |
| peak_power_kw | double | `max(charging_power_kw)` |
| soc_start | double | `min_by(charge_percent, sequence_number)` |
| soc_end | double | `max_by(charge_percent, sequence_number)` |
| end_state | string | `max_by(charge_state, sequence_number)` |
| had_fault | bool | `cast(max(cast(is_fault as int)) as boolean)` |
| duration_minutes | double | `round((double(session_end) - double(session_start)) / 60, 2)` |
| avg_power_kw | double | `round(energy_added_kwh / (duration_minutes / 60), 2)` when `duration_minutes > 0`, else null |
| session_date | date32 | `to_date(session_start)` (UTC) |

## Calculation

1. Filter CHARGING rows with a session id (line 13).
2. `groupBy(charge_session_id, vin_hash, model_safe)` and aggregate (lines 14-25).
3. `duration_minutes` from fractional epoch seconds (`cast(timestamp as double)`), rounded to 2 dp (lines 27-28, 31).
4. `avg_power_kw` uses the **already rounded** `energy_added_kwh` and `duration_minutes` (lines 32-36);
   `when` without `otherwise` → null when duration is 0.
5. `session_date = to_date(session_start)` (line 37): sessions that cross midnight are filed under their start day.

## Nulls and edge cases

| Case | Spark result | Evidence |
|---|---|---|
| single-event session | start = end, `duration_minutes = 0`, `avg_power_kw` null | 194 of 540 fixture sessions |
| `charge_percent` null on the first event | `min_by` returns that null → `soc_start` null | 15 null-`charge_percent` CHARGING rows → 15 null `soc_start` (fixture) |
| session crossing midnight | one row, `session_date` = start date | 136 fixture sessions span >1 `event_date` |
| unparseable `event_time` | `to_timestamp` → null (non-ANSI); `min`/`max` ignore it | PySpark 3.5.1 probe; cannot occur with generator data |
| `energy_added_kwh` all null | `sum` → null | not in fixture |
| a session id reused across vehicles/models | multiple rows with the same `charge_session_id` key → key collision | 0 in fixture (UUIDs) |
| ties on `sequence_number` | `min_by`/`max_by` arbitrary | none in fixture |
| `charge_session_id` null | rows dropped before grouping (line 13) | 0 in fixture |

## Spark behaviour hardest to reproduce in Rust

1. **`to_timestamp(string)`** (lines 16-17) with Spark's lenient ISO parsing and a **microsecond, UTC**
   result type — the schema check compares the timestamp unit (`rust_pipeline/src/validate.rs:205-212`), and
   the fixture is `timestamp[us, UTC]`.
2. **`min_by` / `max_by`** ×3 (lines 21-23), keeping null values (see battery_health_daily spec).
3. **`cast(timestamp as double)`** = fractional epoch seconds (line 28), then HALF_UP rounding; ms precision
   must survive.
4. **Rounding before reuse**: `avg_power_kw` divides two already-rounded values (lines 19, 28, 35) — computing
   it from unrounded intermediates changes results.
5. **`max(cast bool as int)` cast back to boolean** (line 24) and `when` without `otherwise` (lines 34-35).
6. **`to_date` in the session time zone** (UTC, `spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala:63`) and a partitioned write by `session_date`.
