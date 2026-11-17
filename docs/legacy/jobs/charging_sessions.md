# `charging_sessions`: functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala` (abbreviated `ChargingSessions.scala`).
Rust target: `rust_pipeline/src/analytics/charging_sessions.rs` (currently a stub, `rust_pipeline/src/analytics/charging_sessions.rs:14-17`).

## Purpose

One row per charging session: duration, energy added, peak and average power, state of charge at start and end, the final charge
state, and whether any fault occurred (`ChargingSessions.scala:6`, `dashboard/app.py:185-191`). The Fleet → Charging view reads it
(`dashboard/app.py:693-712`).

## Inputs

| Table | Rows used | Columns used | Source |
|-------|-----------|--------------|--------|
| `vehicle_telemetry` / `vehicle_telemetry_rust` | `event_type = 'CHARGING' AND charge_session_id IS NOT NULL` | `charge_session_id`, `vin_hash`, `model_safe`, `event_time` (string), `energy_added_kwh`, `charging_power_kw`, `charge_percent`, `charge_state`, `sequence_number`, `is_fault` | `ChargingSessions.scala:12-24` |

## Output

Delta `analytics/charging_sessions`, overwritten, **partitioned by `session_date`** (`ChargingSessions.scala:9`). Key:
`charge_session_id` (`rust_pipeline/src/analytics/charging_sessions.rs:8-12`). Schema from
`rust_pipeline/tests/fixtures/expected/charging_sessions/*.parquet`; the fixture has 540 sessions:

| # | Column | Type | Nullable | Definition |
|---|--------|------|----------|-----------|
| 1–3 | `charge_session_id`, `vin_hash`, `model_safe` | string | yes | Group keys (`ChargingSessions.scala:14`) |
| 4 | `session_start` | timestamp[us, UTC] | yes | `min(to_timestamp(event_time))` |
| 5 | `session_end` | timestamp[us, UTC] | yes | `max(to_timestamp(event_time))` |
| 6 | `events` | int64 | **no** | `count(*)` |
| 7 | `energy_added_kwh` | double | yes | `round(sum(energy_added_kwh), 3)` |
| 8 | `peak_power_kw` | double | yes | `max(charging_power_kw)` |
| 9 | `soc_start` | double | yes | `min_by(charge_percent, sequence_number)` |
| 10 | `soc_end` | double | yes | `max_by(charge_percent, sequence_number)` |
| 11 | `end_state` | string | yes | `max_by(charge_state, sequence_number)` |
| 12 | `had_fault` | bool | yes | `max(cast(is_fault as int))` cast to boolean |
| 13 | `duration_minutes` | double | yes | `round((double(session_end) - double(session_start)) / 60, 2)` |
| 14 | `avg_power_kw` | double | yes | `round(energy_added_kwh / (duration_minutes / 60), 2)` when `duration_minutes > 0`, else null |
| 15 | `session_date` | date32 | yes | `to_date(session_start)` in UTC |

Sources: `ChargingSessions.scala:15-37`.

## Calculation, step by step

1. Filter to `CHARGING` rows with a non-null session id (`ChargingSessions.scala:13`).
2. Group by `(charge_session_id, vin_hash, model_safe)` (`ChargingSessions.scala:14`). The generator creates a fresh UUID per
   session and vehicle (`data_generator/generator.py:89-90`). In the fixture no session id spans more than one vehicle or model.
3. Compute the aggregates in the output table (`ChargingSessions.scala:15-25`). `event_time` is an ISO string such as
   `2026-11-05T10:00:00.123Z` (`data_generator/generator.py:58-59`). `to_timestamp` parses it as UTC, because the session time zone
   is UTC (`spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala:63`).
4. Compute `duration_minutes` from the two timestamps cast to **seconds as a double**, with fractional milliseconds kept
   (`ChargingSessions.scala:27-28`).
5. Compute `avg_power_kw` from the **already rounded** `energy_added_kwh` and `duration_minutes`. It is null when the duration is not
   positive (`ChargingSessions.scala:32-36`).
6. Set `session_date` to the UTC date of `session_start`, so a session that crosses midnight belongs to its start date
   (`ChargingSessions.scala:37`). 136 of the 540 fixture sessions end on a later date than they start.

## Nulls and edge cases

Checked with a local PySpark 3.5.1 probe unless a file is cited.

| Case | Spark result | In the fixture? |
|------|--------------|-----------------|
| Single-event session | `duration_minutes = 0.0`, `avg_power_kw` null (the `when` has no `otherwise`) | Yes, 194 sessions |
| `charge_percent` null on the lowest-`sequence_number` row | `min_by` returns the value at the minimum key **even if it is null**, so `soc_start` is null and Spark does not fall back to the next row | Yes, 15 sessions (`soc_start` null) |
| `charge_percent` null on the highest-`sequence_number` row | Same for `max_by`: `soc_end` is null | No (0 null `soc_end`) |
| `sequence_number` null | `min_by`/`max_by` ignore rows whose ordering key is null | No |
| Ties on `sequence_number` | `min_by`/`max_by` pick one row, and which row is not defined | No |
| `is_fault` null for every row | `had_fault` null (`max` of all-null) | No (`is_fault` is never null; T4 coalesces it, `spark_jobs/transforms.py:133-137`) |
| Unparseable `event_time` | `to_timestamp` returns null (non-ANSI). Min/max ignore nulls; if every row is null, start, end, duration and `session_date` are all null | No (all 5,763 `event_time` values are 24-character ISO strings) |
| `energy_added_kwh` all null | `sum` null, so `energy_added_kwh` and `avg_power_kw` are null | No |

## Spark behaviour that is hardest to reproduce in Rust

1. **`min_by` / `max_by`.** DataFusion 43 has no `max_by` ("Invalid function 'max_by'", probe). The usual substitutes,
   `first_value`/`last_value(… ORDER BY …)`, sort null keys **first** for `DESC` and do not drop null keys. They therefore differ from
   Spark when `sequence_number` is null. Spark also returns a null *value* at the extreme key, which the port must keep and not skip.
   Three columns depend on this.
2. **Timestamp parsing and arithmetic.**
   - DataFusion's `to_timestamp` returns nanoseconds (`timestamp[ns]`). Casting it to double gives **nanoseconds**, while Spark gives
     seconds with a fraction (`1.793872800123e18` vs `1793872800.123`, probe).
   - DataFusion's `to_timestamp('garbage')` raises an error, where Spark returns null (probe).
   - The output must be `timestamp[us, UTC]`, and parity compares the unit (`rust_pipeline/src/validate.rs:204-211`).
3. **HALF_UP `round` at three scales** (3, 2, 2). DataFusion's `round` differs from Spark's (see `ota_rollout.md`). `avg_power_kw`
   rounds a value computed from two rounded values, so the port must round at each step exactly like Spark.
4. **`to_date` in UTC** of a timestamp, used as the partition column (`ChargingSessions.scala:9,37`). DataFusion 43's `to_date` does
   not accept a timestamp (probe: "Unsupported data type Timestamp(Nanosecond, None)"). Use `CAST(... AS DATE)` instead.
