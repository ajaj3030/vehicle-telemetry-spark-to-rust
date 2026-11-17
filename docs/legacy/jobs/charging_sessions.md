# `charging_sessions` — functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala`
(cited as `ChargingSessions.scala:<line>`). Rust stub:
`rust_pipeline/src/analytics/charging_sessions.rs`.

## Purpose

One row per charging session: start, end, duration, energy added, peak and
average power, state of charge at start and end, final charge state and
whether any event was a fault (`ChargingSessions.scala:6`,
`dashboard/app.py:186-192`). Shown on the dashboard's Charging tab
(`dashboard/app.py:693-712`).

## Inputs

| Table | Columns used | Filter |
|-------|--------------|--------|
| enriched telemetry (`AnalyticsJob.scala:49`) | `event_type`, `charge_session_id`, `vin_hash`, `model_safe`, `event_time` (string), `energy_added_kwh`, `charging_power_kw`, `charge_percent`, `charge_state`, `sequence_number`, `is_fault` | `event_type = 'CHARGING' AND charge_session_id IS NOT NULL` (`ChargingSessions.scala:13`) |

`is_fault` is the T4 flag (`spark_jobs/transforms.py:133-137`, `spark_jobs/transforms.py:142`),
true for fault charge states (`spark_jobs/transforms.py:24-31`), and never null.

## Output

Table `analytics/charging_sessions`, overwritten every run, **partitioned by
`session_date`** (`ChargingSessions.scala:8-9`, `AnalyticsJob.scala:86-91`).
Parity key `charge_session_id` (`rust_pipeline/src/analytics/charging_sessions.rs:8-12`).
Types from `rust_pipeline/tests/fixtures/expected/charging_sessions/`:

| Column | Type | Definition |
|--------|------|------------|
| `charge_session_id` | string | group key |
| `vin_hash` | string | group key |
| `model_safe` | string | group key |
| `session_start` | timestamp (µs, UTC) | `min(to_timestamp(event_time))` |
| `session_end` | timestamp (µs, UTC) | `max(to_timestamp(event_time))` |
| `events` | bigint | `count(*)` |
| `energy_added_kwh` | double | `round(sum(energy_added_kwh), 3)` |
| `peak_power_kw` | double | `max(charging_power_kw)` |
| `soc_start` | double | `charge_percent` of the lowest `sequence_number` |
| `soc_end` | double | `charge_percent` of the highest `sequence_number` |
| `end_state` | string | `charge_state` of the highest `sequence_number` |
| `had_fault` | boolean | any `is_fault` |
| `duration_minutes` | double | `round((end − start) in seconds / 60, 2)` |
| `avg_power_kw` | double | `round(energy_added_kwh / (duration_minutes / 60), 2)` when `duration_minutes > 0`, else null |
| `session_date` | date | `to_date(session_start)` |

## Calculation, step by step

1. Filter CHARGING events with a session id (`ChargingSessions.scala:13`).
2. Group by `charge_session_id`, `vin_hash`, `model_safe` (`ChargingSessions.scala:14`).
3. Aggregate (`ChargingSessions.scala:16-24`): parse `event_time` with
   `to_timestamp` in the session time zone UTC (`AnalyticsJob.scala:63`) and
   take min/max; `count(*)`; `round(sum, 3)`; `max`; `min_by` / `max_by` on
   `sequence_number`; `max(cast(is_fault as int))` cast back to boolean.
4. `duration_minutes` = `round((cast(session_end as double) − cast(session_start as double)) / 60, 2)`;
   casting a timestamp to double gives epoch seconds with the fraction
   (`ChargingSessions.scala:27-28`).
5. `avg_power_kw` uses the **already-rounded** `energy_added_kwh` and the
   **already-rounded** `duration_minutes` from the previous steps, because
   `col(...)` refers to the replaced columns (`ChargingSessions.scala:31-36`).
   No `otherwise`, so it is null when `duration_minutes <= 0` or null.
6. `session_date` = `to_date(session_start)` in UTC (`ChargingSessions.scala:37`).

## Nulls and edge cases

*Probe* = checked with local PySpark 3.5.1 (not committed).

| Case | Spark result | In the fixture? |
|------|--------------|-----------------|
| Single-event session | start = end, `duration_minutes = 0.0`, `avg_power_kw` **null** | yes: 194 of 540 sessions |
| Session crosses midnight UTC | `session_date` is the **start** date | yes: 136 sessions |
| `charge_percent` null on the first event | `min_by` returns the value at the minimum ordering even when it is null, so `soc_start` is **null** (it does not skip to the next non-null) (*probe*) | yes: 15 null `soc_start` |
| `charge_percent` null on the last event | `soc_end` null, same rule (*probe*) | no |
| All `energy_added_kwh` null in a session | `sum` is null, `round(null)` null, `avg_power_kw` null (*probe*) | no: no null energy |
| `event_time` unparseable | `to_timestamp` returns null (non-ANSI) and is ignored by min/max (*probe*) | no |
| `is_fault` | never null (`spark_jobs/transforms.py:133-137`); `had_fault` true if any event is a fault | yes |
| Same `charge_session_id` under two `vin_hash` or `model_safe` values | two output rows with the same parity key | no: 0 such ids |
| Ties on `sequence_number` | `min_by`/`max_by` pick one arbitrarily | no: unique per VIN (`data_generator/generator.py:150-154`) |
| Session ids | the generator starts a new UUID per session and ends it on a non-charging state (`data_generator/generator.py:87-105`) | — |

## Spark behaviour hardest to reproduce in Rust

1. **Timestamps.** `event_time` is a string (`rust_pipeline/src/schema.rs:158`,
   `spark_jobs/streaming_job.py:87`). DataFusion 43's `to_timestamp`
   returns `Timestamp(Nanosecond, None)` (checked with the `datafusion==43.1.0`
   Python bindings); Spark writes `timestamp[us, tz=UTC]`. The validator
   compares timestamp units (`rust_pipeline/src/validate.rs:205-212`), so the
   port must cast to microseconds. The duration must come from epoch seconds
   **with microsecond fraction**, divided by 60 and rounded HALF_UP.
2. **`min_by` / `max_by`.** DataFusion 43 has no `max_by`/`min_by` ("Invalid
   function 'max_by'"). `first_value(x ORDER BY sequence_number ASC|DESC)`
   keeps a null value at the extreme, like Spark, but Spark also ignores rows
   whose *ordering* is null; `sequence_number` is non-null in the schema
   (`spark_jobs/streaming_job.py:88`), so this only matters if that changes.
3. **Rounding twice.** `avg_power_kw` must divide rounded values (step 5), and
   every `round` is Spark HALF_UP (`rust_pipeline/src/transforms/rounding.rs:1-6`),
   which DataFusion's `round` is not (`round(1.005, 2) = 1.0` in DataFusion).
4. **Partitioned output.** The Rust writer must partition by `session_date`
   (`rust_pipeline/src/analytics/mod.rs:98-111`); a test already pins this
   (`rust_pipeline/src/analytics/mod.rs:121-132`).
