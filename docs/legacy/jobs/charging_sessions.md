# charging_sessions

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala`. Rust stub: `rust_pipeline/src/analytics/charging_sessions.rs`.
Fixture numbers measured with DuckDB over `rust_pipeline/tests/fixtures/input/telemetry` and `rust_pipeline/tests/fixtures/expected/charging_sessions`.

## Purpose
One row per charge session: duration, energy, peak and average power, SoC gained and how it ended (`README.md:77`, `spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala:11-38`).

## Inputs
| Table | Columns used | Cited in |
|---|---|---|
| `vehicle_telemetry` (Rust: `vehicle_telemetry_rust`) | `event_type`, `charge_session_id`, `vin_hash`, `model_safe`, `event_time` (string), `energy_added_kwh`, `charging_power_kw`, `charge_percent`, `charge_state`, `sequence_number`, `is_fault` | `spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala:13-24,40-41` |

Sessions are generated in `data_generator/generator.py:87-107`; `is_fault` is derived null-safely in `spark_jobs/transforms.py:133-142`.

## Output schema
Partitioned by `session_date` (`:9`), overwritten (`spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala:87-91`). Key `charge_session_id` (`README.md:77`).

| # | Column | Spark type | Source |
|---|---|---|---|
| 1 | `charge_session_id` | string | key, `:14` |
| 2 | `vin_hash` | string | key, `:14` |
| 3 | `model_safe` | string | key, `:14` |
| 4 | `session_start` | timestamp (Parquet `timestamp[us, tz=UTC]`) | `min(to_timestamp(event_time))`, `:16` |
| 5 | `session_end` | timestamp (Parquet `timestamp[us, tz=UTC]`) | `max(to_timestamp(event_time))`, `:17` |
| 6 | `events` | bigint | `count(*)`, `:18` |
| 7 | `energy_added_kwh` | double | `round(sum,3)`, `:19` |
| 8 | `peak_power_kw` | double | `max`, `:20` |
| 9 | `soc_start` | double | `min_by(charge_percent, sequence_number)`, `:21` |
| 10 | `soc_end` | double | `max_by(charge_percent, sequence_number)`, `:22` |
| 11 | `end_state` | string | `max_by(charge_state, sequence_number)`, `:23` |
| 12 | `had_fault` | boolean | `max(cast int).cast(boolean)`, `:24` |
| 13 | `duration_minutes` | double | `:27-31` |
| 14 | `avg_power_kw` | double | `:32-36` |
| 15 | `session_date` | date | `to_date(session_start)`, `:37` |

Timestamps are written as microsecond Parquet timestamps in the fixtures (`spark_jobs/analytics/src/main/scala/telemetry/analytics/BuildParityFixtures.scala:51`).

## Calculation
1. Keep `event_type = 'CHARGING'` and `charge_session_id IS NOT NULL` (`:13`).
2. Group by (`charge_session_id`, `vin_hash`, `model_safe`) (`:14`) and aggregate per the table above (`:16-24`).
3. `duration_minutes` = round((epoch-seconds(end) − epoch-seconds(start)) / 60, 2), using `timestamp → double` casts that keep fractional seconds (`:27-28`).
4. `avg_power_kw` = round(energy_added_kwh / (duration_minutes / 60), 2) **only when duration_minutes > 0**, else null (`when` without `otherwise`, `:32-36`). It uses the already-rounded energy and duration.
5. `session_date` = UTC date of `session_start` (session TZ UTC, `spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala:63`).

## Nulls and edge cases
- **Single-event sessions:** start = end → duration 0 → `avg_power_kw` null. 194 of the fixture sessions have one event.
- **Null `charge_percent` on the chosen row:** `min_by`/`max_by` return the value of the row with min/max `sequence_number` even if that value is null — they do not skip to the next row. Fixture: 15 null `charge_percent` inputs → 15 null `soc_start`.
- `min`/`max`/`sum` skip nulls; a session whose energy values are all null gets null `energy_added_kwh` (none in fixture).
- **Session spanning midnight:** dated by its start (`:37`).
- **Same session id across vehicles or models** would produce several rows with the same key; the fixture has none.
- `had_fault` is never null because `is_fault` is null-safe (`spark_jobs/transforms.py:133-142`).

## Hardest Spark behaviour to reproduce
1. **`to_timestamp(string)` and the timestamp type**: parsing `yyyy-MM-ddTHH:mm:ss.SSSZ` (`data_generator/generator.py:58-59`) in UTC and producing a column whose Arrow type passes the exact type check (`rust_pipeline/src/validate.rs:185-212`).
2. **`min_by`/`max_by` null semantics** (three columns) — DataFusion's `first_value`/`last_value` with `ORDER BY` must not ignore nulls.
3. HALF_UP rounding on four derived doubles, where step 4 compounds already-rounded inputs (`rust_pipeline/src/transforms/rounding.rs:1-7`).
4. Writing `session_date` as a partition column (`rust_pipeline/src/analytics/mod.rs:98-111`).
