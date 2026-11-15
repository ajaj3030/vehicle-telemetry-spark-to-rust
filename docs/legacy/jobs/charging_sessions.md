# charging_sessions

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala`
(below, `ChargingSessions.scala:N`). Rust placeholder: `rust_pipeline/src/analytics/charging_sessions.rs`.
`AnalyticsJob.scala` and `BuildParityFixtures.scala` are in the same directory as the job.

## Purpose

One row per charging session (`ChargingSessions.scala:6`; `dashboard/app.py:187-192`).
Shown on the Fleet page's Charging tab (`dashboard/app.py:693-712`).

## Inputs

Enriched telemetry (`ChargingSessions.scala:40-41`), rows with
`event_type = 'CHARGING'` and non-null `charge_session_id`
(`ChargingSessions.scala:13`). Columns: `charge_session_id`, `vin_hash`,
`model_safe`, `event_time` (**string**, `spark_jobs/streaming_job.py:87`),
`energy_added_kwh`, `charging_power_kw`, `charge_percent`, `charge_state`,
`is_fault`, `sequence_number`.

## Output

`analytics/charging_sessions`, Delta overwrite, **partitioned by
`session_date`** (`ChargingSessions.scala:9`, `AnalyticsJob.scala:86-91`).
Types from `rust_pipeline/tests/fixtures/expected/charging_sessions/`.

| Column | Type | Definition |
|---|---|---|
| `charge_session_id` | string | group key |
| `vin_hash` | string | group key |
| `model_safe` | string | group key |
| `session_start` | timestamp(µs, UTC) | `min(to_timestamp(event_time))` (`:16`) |
| `session_end` | timestamp(µs, UTC) | `max(to_timestamp(event_time))` (`:17`) |
| `events` | bigint not null | `count(*)` (`:18`) |
| `energy_added_kwh` | double | `round(sum(energy_added_kwh), 3)` (`:19`) |
| `peak_power_kw` | double | `max(charging_power_kw)` (`:20`) |
| `soc_start` | double | `min_by(charge_percent, sequence_number)` (`:21`) |
| `soc_end` | double | `max_by(charge_percent, sequence_number)` (`:22`) |
| `end_state` | string | `max_by(charge_state, sequence_number)` (`:23`) |
| `had_fault` | boolean | `cast(max(cast(is_fault as int)) as boolean)` (`:24`) |
| `duration_minutes` | double | `round((end − start in seconds) / 60, 2)` (`:27-28,31`) |
| `avg_power_kw` | double | `round(energy_added_kwh / (duration_minutes / 60), 2)` when `duration_minutes > 0`, else null (`:32-36`) |
| `session_date` | date | `to_date(session_start)` in UTC (`:37`) |

## Calculation

1. Keep CHARGING events with a session id (`:13`).
2. Group by `(charge_session_id, vin_hash, model_safe)` and aggregate (`:14-25`).
3. `duration_minutes`: cast both timestamps to double (epoch seconds with
   fractional microseconds), subtract, divide by 60, round 2 dp (`:27-28`).
4. `avg_power_kw` uses the **already rounded** `energy_added_kwh` and
   `duration_minutes` (`:32-36`).
5. `session_date` from `session_start` in the session time zone, UTC (`:37`, `AnalyticsJob.scala:63`).

## Nulls and edge cases

- **Single-event sessions:** start = end → `duration_minutes = 0` →
  `avg_power_kw` null (`when` without `otherwise`). 194 of 540 expected rows.
- **Very short sessions:** a duration under 0.3 s rounds to 0.0 and also
  yields null; the guard is on the rounded value.
- **Null `charge_percent`:** `min_by`/`max_by` return the value of the
  earliest/latest row even if it is null. 15 CHARGING inputs have it null and
  15 expected rows have null `soc_start` (0 null `soc_end`).
- **Unparseable `event_time`:** `to_timestamp` returns null with ANSI off
  (`AnalyticsJob.scala:56-64`); `min`/`max` skip it. If all are null, the
  duration, `avg_power_kw` and `session_date` are null. The fixture has none.
- **Sessions crossing midnight UTC:** `session_date` is the start date
  (136 fixture sessions cross dates).
- **Session id reused by another vehicle/model:** produces separate rows,
  but the parity key is `charge_session_id` alone
  (`rust_pipeline/src/analytics/charging_sessions.rs:8-12`), so this is assumed not to happen.
- **`had_fault`:** `is_fault` is never null (`spark_jobs/transforms.py:133-137`),
  so it is never null in practice.

## Hardest Spark behaviour to reproduce

1. **`to_timestamp(string)`** — Spark's lenient ISO-8601 parsing (`Z`,
   offsets, variable fractional digits), null on failure, microsecond
   precision, UTC session time zone; written as `timestamp(µs, UTC)` because
   fixtures use `TIMESTAMP_MICROS` (`BuildParityFixtures.scala:50-51`).
   DataFusion's `to_timestamp` defaults to nanoseconds and errors on bad input.
2. **`min_by` / `max_by`** with null values, as for battery.
3. **Timestamp → double cast** (seconds with fractional µs) and the
   chain of roundings; any difference in the intermediate double changes
   `avg_power_kw`.
4. **`round` HALF_UP** (`rust_pipeline/src/transforms/rounding.rs:1-6`) and
   `when` on the rounded duration.
5. Partitioned write by `session_date`.
