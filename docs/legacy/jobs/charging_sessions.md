# `charging_sessions` — functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala` (cited as `ChargingSessions.scala`).
Rust target: `rust_pipeline/src/analytics/charging_sessions.rs` (stub, `:15-16`).

## Purpose

One row per charging session: start/end, duration, energy, peak and average power, start/end
state of charge, end state, whether a fault occurred (`README.md:69-78`, `dashboard/app.py:693-713`).

## Inputs

| Table | Rows used | Columns read |
|---|---|---|
| `vehicle_telemetry` (`AnalyticsJob.scala:49`) | `event_type = 'CHARGING'` and `charge_session_id IS NOT NULL` (`ChargingSessions.scala:13`) | `charge_session_id`, `vin_hash`, `model_safe`, `event_time`, `sequence_number`, `energy_added_kwh`, `charging_power_kw`, `charge_percent`, `charge_state`, `is_fault` (`ChargingSessions.scala:14-24`) |

`is_fault` comes from the stream (`spark_jobs/transforms.py:127-157`); for CHARGING rows it is driven
by `charge_state ∈ FAULT_CHARGE_STATES` (`spark_jobs/transforms.py:24-31`). Session ids are generated
per vehicle and cleared when a non-charging state is seen (`data_generator/generator.py:87-106`).

## Output schema

Table `<analytics base>/charging_sessions`, overwrite, partitioned by **`session_date`**
(`ChargingSessions.scala:9`, `rust_pipeline/src/analytics/charging_sessions.rs:8-12`).
Parity key: `charge_session_id`. Types from `rust_pipeline/tests/fixtures/expected/charging_sessions`:

| Column | Type | Definition |
|---|---|---|
| `charge_session_id` | string | Group key |
| `vin_hash` | string | Group key |
| `model_safe` | string | Group key |
| `session_start` | timestamp[us, UTC] | `min(to_timestamp(event_time))` |
| `session_end` | timestamp[us, UTC] | `max(to_timestamp(event_time))` |
| `events` | int64 | `count(*)` |
| `energy_added_kwh` | double | `round(sum(energy_added_kwh), 3)` |
| `peak_power_kw` | double | `max(charging_power_kw)` |
| `soc_start` | double | `min_by(charge_percent, sequence_number)` |
| `soc_end` | double | `max_by(charge_percent, sequence_number)` |
| `end_state` | string | `max_by(charge_state, sequence_number)` |
| `had_fault` | bool | `max(is_fault::int)::boolean` |
| `duration_minutes` | double | `round((end − start) seconds / 60, 2)` |
| `avg_power_kw` | double | `round(energy_added_kwh / (duration_minutes/60), 2)` when `duration_minutes > 0`, else null |
| `session_date` | date | `to_date(session_start)` (partition) |

(Exact rounding scales: `ChargingSessions.scala:19`, `:27-36`.)

## Calculation

1. Filter to CHARGING rows with non-null `charge_session_id` (`ChargingSessions.scala:13`).
2. `groupBy(charge_session_id, vin_hash, model_safe)` and aggregate as in the table (`ChargingSessions.scala:14-25`).
3. `duration_minutes` from `session_end.cast(double) − session_start.cast(double)` (epoch seconds with
   fractional ms) ÷ 60, rounded (`ChargingSessions.scala:27-31`).
4. `avg_power_kw` from the **already rounded** `energy_added_kwh` and `duration_minutes`
   (`ChargingSessions.scala:32-36`); `when` without `otherwise` gives null.
5. `session_date = to_date(session_start)` in the UTC session timezone (`ChargingSessions.scala:37`, `AnalyticsJob.scala:63`).

## Nulls and edge cases

| Case | Spark result |
|---|---|
| Single-event session | start = end, `duration_minutes = 0`, `avg_power_kw` null. Fixture: 194 of 540 sessions. |
| `charge_percent` null on the min/max-sequence row | `min_by`/`max_by` return that row's null (they ignore only null **ordering** values). Fixture: 15 null `soc_start`. |
| `sequence_number` null | Row ignored by `min_by`/`max_by`; all-null → null. |
| `is_fault` all null | `max` → null → `had_fault` null. |
| Unparseable `event_time` | `to_timestamp` returns null; min/max skip it. |
| Session spanning midnight | Partitioned under the start date only. Fixture: 136 sessions span > 1 event date. |
| Same session id on two `model_safe` values | Two rows (model is a group key) — would duplicate the parity key. |

Measured on `rust_pipeline/tests/fixtures/expected/charging_sessions` and `rust_pipeline/tests/fixtures/input/telemetry`.

## Hardest Spark behaviour to reproduce

1. **`to_timestamp(event_time)`** on ISO-8601 strings with `Z` and milliseconds → microsecond UTC
   timestamp; output must be `Timestamp(Microsecond, "UTC")` exactly (`rust_pipeline/src/validate.rs:185-212`).
2. **`min_by` / `max_by` null semantics** (×3) — not built into DataFusion 43; needs `first_value`/`last_value`
   with ordering that skips null sequence numbers but keeps null values.
3. **Timestamp → double cast** for duration, including fractional seconds.
4. **Rounding of rounded values** and HALF_UP (`rust_pipeline/src/transforms/rounding.rs`).
5. **Float `sum` order** — Spark's summation order differs from DataFusion's; covered by the `1e-9`
   tolerance in `rust_pipeline/src/validate.rs:23` only after rounding to 3 dp.
6. **Boolean max** via int cast.
