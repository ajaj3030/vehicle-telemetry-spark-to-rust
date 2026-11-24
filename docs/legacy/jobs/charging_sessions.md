# charging_sessions

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala`
(cited below as `ChargingSessions.scala:<line>`). Rust stub:
`rust_pipeline/src/analytics/charging_sessions.rs`.

## Purpose

One row per charging session (`ChargingSessions.scala:6`): start, end,
duration, energy added, peak and average power, start and end state of
charge, final charge state and whether any fault occurred. The dashboard's
Charging tab reads it (`dashboard/app.py:693-712`).

## Inputs

One table, `vehicle_telemetry` (`ChargingSessions.scala:40-41`), CHARGING
rows with a session id. Columns used: `event_type`, `charge_session_id`,
`vin_hash`, `model_safe`, `event_time` (string), `energy_added_kwh`,
`charging_power_kw`, `charge_percent`, `sequence_number`, `charge_state`,
`is_fault`. `is_fault` is a null-safe boolean that includes the fault charge
states (`spark_jobs/transforms.py:24-31`, `:133-137`). `event_time` is
generated as `yyyy-MM-ddTHH:mm:ss.SSSZ` (`data_generator/generator.py:59`).

## Output schema

Table `analytics/charging_sessions`, overwritten, **partitioned by
`session_date`** (`ChargingSessions.scala:9`, `AnalyticsJob.scala:87-91`).
Rust parity key: `charge_session_id`
(`rust_pipeline/src/analytics/charging_sessions.rs:10`). Types from
`rust_pipeline/tests/fixtures/expected/charging_sessions/`.

| Column | Type | Meaning |
|---|---|---|
| `charge_session_id` | string | Session |
| `vin_hash` | string | Vehicle |
| `model_safe` | string | Model |
| `session_start` | timestamp (µs, UTC) | Earliest `event_time` |
| `session_end` | timestamp (µs, UTC) | Latest `event_time` |
| `events` | bigint, not null | Events in the session |
| `energy_added_kwh` | double | Sum of `energy_added_kwh`, 3 dp |
| `peak_power_kw` | double | Max `charging_power_kw` |
| `soc_start` | double | `charge_percent` at the lowest `sequence_number` |
| `soc_end` | double | `charge_percent` at the highest `sequence_number` |
| `end_state` | string | `charge_state` at the highest `sequence_number` |
| `had_fault` | boolean | Any event with `is_fault` |
| `duration_minutes` | double | (end − start) in minutes, 2 dp |
| `avg_power_kw` | double | energy / hours, 2 dp. NULL when duration is not > 0 |
| `session_date` | date | UTC date of `session_start` |

## Calculation

1. Keep `event_type = 'CHARGING' AND charge_session_id IS NOT NULL`
   (`ChargingSessions.scala:13`).
2. Group by `charge_session_id, vin_hash, model_safe` (`ChargingSessions.scala:14`):
   - `session_start = min(to_timestamp(event_time))`, `session_end = max(…)`
   - `events = count(*)`
   - `energy_added_kwh = round(sum(energy_added_kwh), 3)`
   - `peak_power_kw = max(charging_power_kw)`
   - `soc_start = min_by(charge_percent, sequence_number)`
   - `soc_end = max_by(charge_percent, sequence_number)`
   - `end_state = max_by(charge_state, sequence_number)`
   - `had_fault = cast(max(cast(is_fault as int)) as boolean)`

   (`ChargingSessions.scala:15-25`)
3. `duration_minutes = round((cast(session_end as double) - cast(session_start as double)) / 60, 2)`.
   A timestamp cast to double is epoch seconds with the microsecond fraction
   (`ChargingSessions.scala:27-28`, `:31`).
4. `avg_power_kw = CASE WHEN duration_minutes > 0 THEN round(energy_added_kwh / (duration_minutes / 60), 2) END`.
   This uses the **already-rounded** energy and duration
   (`ChargingSessions.scala:32-36`).
5. `session_date = to_date(session_start)` in UTC (`ChargingSessions.scala:37`,
   `AnalyticsJob.scala:63`).

## Nulls and edge cases

*Verified* means run through the same Spark functions under PySpark 3.5.1
locally. Fixture counts are from `rust_pipeline/tests/fixtures/` (540
sessions).

| Case | Spark behaviour |
|---|---|
| Null `charge_session_id` | Row dropped before grouping. |
| Single-event session | `duration_minutes = 0`, so `avg_power_kw` is NULL: 194 fixture sessions. |
| Very short session | Duration rounds to 0.0 below 0.3 s. That is not > 0, so `avg_power_kw` is NULL. |
| `charge_percent` NULL on the first or last event | `min_by` / `max_by` return NULL. They do not skip to a non-null reading (*verified*): 15 fixture sessions have a NULL `soc_start`. Rows with a NULL `sequence_number` are skipped. Ties resolve arbitrarily (*verified*). |
| Session crosses midnight UTC | `session_date` is the start date: 136 fixture sessions. |
| Unparseable `event_time` | `to_timestamp` gives NULL (*verified*), and `min` / `max` ignore it. If every event is unparseable, start, end, duration, `avg_power_kw` and `session_date` are all NULL. |
| Accepted `event_time` forms | `…T…Z`, with or without fraction, space separator, `+hh:mm` offsets (converted to UTC), minute precision and date only all parse. More than 6 fractional digits are truncated to µs (*verified*). The fixture only uses `yyyy-MM-ddTHH:mm:ss.SSSZ`. |
| A session id seen with two `vin_hash` or `model_safe` values | Two rows with the same parity key. The fixture has none. |
| `had_fault` | Never NULL, because `is_fault` is never NULL. An all-NULL group would give NULL (*verified*). |

## Hardest Spark behaviour to reproduce

1. **`to_timestamp(string)`.** Spark accepts the range of ISO forms above and
   returns `Timestamp(µs, UTC)`. DataFusion 43.1.0's `to_timestamp` returns
   `Timestamp(ns)` with no time zone (checked locally), which fails the type
   check (`rust_pipeline/src/validate.rs:185-212`). Open PR #20 proposes a
   shared `event_time` helper.
2. **Timestamp → double seconds** with the microsecond fraction, then chained
   HALF_UP rounding: energy (3 dp), then duration (2 dp), then
   `avg_power_kw` computed from those rounded values (2 dp). Any difference
   in an earlier rounding step carries into `avg_power_kw`.
3. **`min_by` / `max_by` semantics:** keep NULL values, skip NULL keys, ties
   arbitrary. See [battery_health_daily.md](battery_health_daily.md#hardest-spark-behaviour-to-reproduce)
   for how DataFusion's `first_value` / `last_value` differ.
4. **`CASE` without `ELSE`** must give NULL, not 0 or NaN.
5. **UTC `to_date`** and partitioning by `session_date` (Date32).
