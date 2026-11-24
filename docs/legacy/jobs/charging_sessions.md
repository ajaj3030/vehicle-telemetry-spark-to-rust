# `charging_sessions`

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala` (below `ChargingSessions.scala`). Runner: `spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala` (below `AnalyticsJob.scala`).

## Purpose

One row per charging session: start and end time, event count, energy added, peak and average power, state of charge at start and end, final charge state, and whether any fault occurred (`ChargingSessions.scala:11-38`). Shown on the dashboard's charging view (`dashboard/app.py:693-712`).

## Inputs

- `vehicle_telemetry`, rows with `event_type = 'CHARGING'` and non-null `charge_session_id` (`ChargingSessions.scala:13,40-41`). Columns used: `charge_session_id`, `vin_hash`, `model_safe`, `event_time`, `energy_added_kwh`, `charging_power_kw`, `charge_percent`, `charge_state`, `is_fault`, `sequence_number` (`ChargingSessions.scala:14-24`).
- `event_time` is an ISO-8601 string such as `2026-11-05T23:23:51.318Z` (`rust_pipeline/tests/fixtures/input/telemetry/`). `is_fault` comes from `spark_jobs/transforms.py:133-137`.

## Output schema

Delta `<analytics.output_base>/charging_sessions`, overwrite, **partitioned by `session_date`** (`ChargingSessions.scala:9`, `AnalyticsJob.scala:87-91`). Types from `rust_pipeline/tests/fixtures/expected/charging_sessions/` (540 rows):

| Column | Type | Definition |
|--------|------|------------|
| `charge_session_id` | string | group key |
| `vin_hash` | string | group key |
| `model_safe` | string | group key |
| `session_start` | timestamp[µs, UTC] | `min(to_timestamp(event_time))` |
| `session_end` | timestamp[µs, UTC] | `max(to_timestamp(event_time))` |
| `events` | int64 | `count(*)` |
| `energy_added_kwh` | double | `round(sum(energy_added_kwh), 3)` |
| `peak_power_kw` | double | `max(charging_power_kw)` |
| `soc_start` | double | `min_by(charge_percent, sequence_number)` |
| `soc_end` | double | `max_by(charge_percent, sequence_number)` |
| `end_state` | string | `max_by(charge_state, sequence_number)` |
| `had_fault` | bool | `cast(max(cast(is_fault as int)) as boolean)` |
| `duration_minutes` | double | `round((end_seconds - start_seconds) / 60, 2)` |
| `avg_power_kw` | double | `round(energy_added_kwh / (duration_minutes / 60), 2)` when `duration_minutes > 0`, else null |
| `session_date` | date32 | `to_date(session_start)` |

Sources: `ChargingSessions.scala:14-37`.

## Calculation

1. Filter `event_type = 'CHARGING' AND charge_session_id IS NOT NULL` (`ChargingSessions.scala:13`).
2. Group by `(charge_session_id, vin_hash, model_safe)` and compute the nine aggregates (`ChargingSessions.scala:14-25`).
3. `duration_minutes`: cast both timestamps to double (epoch seconds with fractional milliseconds), subtract, divide by 60, round to 2 (`ChargingSessions.scala:27-28,31`).
4. `avg_power_kw`: only when `duration_minutes > 0`, `round(energy_added_kwh / (duration_minutes / 60), 2)`; otherwise null, because `when` without `otherwise` yields null (`ChargingSessions.scala:32-36`). Note it divides by the **rounded** duration.
5. `session_date = to_date(session_start)` in the UTC session time zone (`ChargingSessions.scala:37`, `AnalyticsJob.scala:63`).

## Nulls and edge cases

*(probe)* = confirmed with `pyspark==3.5.1`, session time zone UTC.

- **Unparseable `event_time`:** `to_timestamp` returns null (non-ANSI) *(probe)*; `min`/`max` skip it, so one bad row does not null the session unless every row is bad.
- **Single-event or zero-length sessions:** `duration_minutes = 0` and `avg_power_kw` is null. The expected fixture has 194 such rows (`rust_pipeline/tests/fixtures/expected/charging_sessions/`).
- **`min_by`/`max_by`** return the value at the lowest/highest `sequence_number` even if that value is null, and skip rows whose `sequence_number` is null *(probe)*. Ties are non-deterministic.
- **`energy_added_kwh`:** `sum` skips nulls; all-null gives null, and then `avg_power_kw` is null too.
- **`had_fault`:** `is_fault` is never null (`spark_jobs/transforms.py:133-137` wraps each test in `_is_true`), so `had_fault` is `true` iff any event was a fault *(probe for the cast)*.
- **Sessions over midnight:** `session_date` is the start date. 136 expected rows end on a later date than `session_date` (`rust_pipeline/tests/fixtures/expected/charging_sessions/`).
- **Session id reused across vehicles or models** produces separate rows, because `vin_hash` and `model_safe` are part of the key (`ChargingSessions.scala:14`).
- The fixture has no CHARGING rows with null `charge_session_id` (`rust_pipeline/tests/fixtures/input/telemetry/`), so step 1's null filter is not exercised.

## Hardest Spark behaviour to reproduce in Rust

1. **`to_timestamp` on the ISO string** with millisecond precision and `Z`, producing microsecond timestamps to pass the type check, which compares timestamp units but not time zones (`rust_pipeline/src/validate.rs:185,205-209`), and null rather than an error on bad input. DataFusion's `to_timestamp` returns nanoseconds without a time zone and errors on unparseable strings.
2. **Timestamp → double seconds** for the duration (`ChargingSessions.scala:28`); millisecond fractions must survive to two decimal places of minutes.
3. **`min_by`/`max_by` ×3** with Spark's null semantics (see `docs/legacy/jobs/battery_health_daily.md`, same functions).
4. **HALF_UP `round`** at scales 3 and 2 (`ChargingSessions.scala:19,28,35`), with `avg_power_kw` computed from the already-rounded duration.
5. **Partitioned write by `session_date`.**
