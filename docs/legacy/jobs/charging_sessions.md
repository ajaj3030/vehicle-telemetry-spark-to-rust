# `charging_sessions` functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala` (cited as `ChargingSessions.scala:N`).
Rust target: `rust_pipeline/src/analytics/charging_sessions.rs` (stub).

## Purpose
One row per charging session: duration, energy, peak and average power, start and end
state of charge, end state, faults (`ChargingSessions.scala:6`, `README.md:77`). Shown on the
dashboard charging tab (`dashboard/app.py:693-712`).

## Inputs
`vehicle_telemetry` (`ChargingSessions.scala:40-41`), rows with `event_type = 'CHARGING'` and
non-null `charge_session_id` (`:13`). Columns used: `charge_session_id`, `vin_hash`,
`model_safe`, `event_time` (string, ISO-8601 with `Z`, e.g. `2026-11-05T23:23:51.318Z`
in `rust_pipeline/tests/fixtures/input/telemetry/`), `energy_added_kwh`,
`charging_power_kw`, `charge_percent`, `charge_state`, `is_fault`, `sequence_number`
(`:14-24`). `is_fault` is derived in T4 (`spark_jobs/transforms.py:133-137`).

## Output schema
Table `analytics/charging_sessions`, overwritten each run, partitioned by `session_date`
(`ChargingSessions.scala:9`, `AnalyticsJob.scala:75-97`). Key: `charge_session_id`
(`rust_pipeline/src/analytics/charging_sessions.rs:10`). Types from
`rust_pipeline/tests/fixtures/expected/charging_sessions/`:

| Column | Type | Nullable |
|---|---|---|
| `charge_session_id`, `vin_hash`, `model_safe` | string | yes |
| `session_start`, `session_end` | timestamp (µs, UTC) | yes |
| `events` | long | no |
| `energy_added_kwh`, `peak_power_kw`, `soc_start`, `soc_end` | double | yes |
| `end_state` | string | yes |
| `had_fault` | boolean | yes |
| `duration_minutes`, `avg_power_kw` | double | yes |
| `session_date` | date | yes |

## Calculation
1. Keep `CHARGING` rows with a session id (`:13`).
2. Group by `(charge_session_id, vin_hash, model_safe)` (`:14`) and compute:
   - `session_start` / `session_end` = min / max of `to_timestamp(event_time)` (`:16-17`);
   - `events` = row count (`:18`);
   - `energy_added_kwh` = `round(sum(energy_added_kwh), 3)` (`:19`);
   - `peak_power_kw` = `max(charging_power_kw)` (`:20`);
   - `soc_start` / `soc_end` = `charge_percent` at the lowest / highest `sequence_number` (`min_by`/`max_by`, `:21-22`);
   - `end_state` = `charge_state` at the highest `sequence_number` (`:23`);
   - `had_fault` = `max(cast(is_fault as int))` cast back to boolean (`:24`).
3. `duration_minutes` = `round((epoch_seconds(session_end) - epoch_seconds(session_start)) / 60, 2)`, with fractional seconds (`:27-28`, `:31`).
4. `avg_power_kw` = `round(energy_added_kwh / (duration_minutes / 60), 2)` only when `duration_minutes > 0`, using the already rounded energy and duration (`:32-36`).
5. `session_date` = `to_date(session_start)` in the UTC session time zone (`:37`, `AnalyticsJob.scala:55-68`).

## Nulls and edge cases
- **Single-event or same-timestamp sessions:** `duration_minutes = 0`, so `avg_power_kw` is null (`when` has no `otherwise`, `:34-35`). 194 of 540 expected rows (`rust_pipeline/tests/fixtures/expected/charging_sessions/`).
- **Unparseable `event_time`:** `to_timestamp` returns null in non-ANSI mode; min/max skip nulls. If every row is unparseable, start, end, duration, average power and `session_date` are all null.
- **Aggregates skip nulls:** `sum`, `max`, and the `is_fault` max are null only when every input is null; `events` counts all rows (`:18-24`).
- **`min_by`/`max_by` return the value at the extreme ordering key even when that value is null**; rows with a null `sequence_number` are skipped.
- **One session id under two `vin_hash` or `model_safe` values** yields two rows with the same key `charge_session_id` (`:14`), which the parity key cannot tell apart (`rust_pipeline/src/analytics/charging_sessions.rs:10`). Not checked against the fixture.
- **Rounding compounds:** step 4 divides rounded values, so `avg_power_kw` is not the unrounded energy/duration. Port as is.

## Spark behaviour hardest to reproduce in Rust
1. **`to_timestamp` on strings**: Spark's lenient ISO parser, UTC session zone, microsecond precision, and null on failure. Output must be `Timestamp(Microsecond, "UTC")`. Open PR #20 adds a shared helper (`rust_pipeline/src/transforms/event_time.rs` on its branch).
2. **`cast(timestamp as double)`**: seconds since epoch including the microsecond fraction, before the `/ 60` and HALF_UP round.
3. **`min_by` / `max_by` semantics** (null values kept, null keys skipped), not available as built-ins with the same rules in every DataFusion version.
4. **Order of floating-point `sum`** before `round(..., 3)`: Spark's partial aggregation order can differ from Rust's, which can flip a value that is exactly on a rounding boundary.
5. **`round`** HALF_UP on the decimal value (`rust_pipeline/src/transforms/rounding.rs`).
6. **Partitioned Delta write by `session_date`** (`rust_pipeline/src/analytics/mod.rs:98-111`).
