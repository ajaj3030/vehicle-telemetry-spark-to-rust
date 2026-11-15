# `battery_health_daily` — functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala` (cited as `BatteryHealthDaily.scala`).
Rust target: `rust_pipeline/src/analytics/battery_health_daily.rs` (stub).

## Purpose

Daily battery summary per vehicle, a rolling 3-day peak cell temperature, and day-over-day change in
battery health (`README.md:69-78`, `dashboard/app.py:662-692`).

## Inputs

| Table | Rows used | Columns read |
|---|---|---|
| `vehicle_telemetry` (`AnalyticsJob.scala:49`) | `event_type = 'BATTERY'` (`BatteryHealthDaily.scala:16`) | `vin_hash`, `event_date`, `model_safe`, `charge_percent`, `max_cell_temp`, `cell_temp_spread`, `thermal_alert`, `battery_health_level`, `sequence_number` (`BatteryHealthDaily.scala:17-26`) |

`event_date`, `cell_temp_spread`, `thermal_alert` are stream-derived (`spark_jobs/transforms.py:82-157`).

## Output schema

Table `<analytics base>/battery_health_daily`, overwrite, partitioned by **`event_date`**
(`BatteryHealthDaily.scala:10`, `rust_pipeline/src/analytics/battery_health_daily.rs`). Parity key:
`vin_hash`, `event_date`. Types from `rust_pipeline/tests/fixtures/expected/battery_health_daily`:

| Column | Type | Definition |
|---|---|---|
| `vin_hash` | string | Group key |
| `event_date` | date | Group key / partition |
| `model_safe` | string | Group key |
| `readings` | int64 | `count(*)` |
| `avg_charge_percent` | double | `round(avg(charge_percent), 2)` |
| `min_charge_percent` | double | `min(charge_percent)` |
| `peak_cell_temp` | double | `max(max_cell_temp)` |
| `avg_cell_temp_spread` | double | `round(avg(cell_temp_spread), 3)` |
| `thermal_alert_count` | int64 | `sum(thermal_alert::int)` |
| `health_level_end_of_day` | double | `max_by(battery_health_level, sequence_number)` |
| `peak_cell_temp_3d` | double | max of `peak_cell_temp` over this and the previous 2 calendar days |
| `health_change` | double | `round(health_level_end_of_day − lag(…, 1), 2)` |

(`BatteryHealthDaily.scala:17-40`.)

## Calculation

1. Filter to BATTERY (`BatteryHealthDaily.scala:16`).
2. `groupBy(vin_hash, event_date, model_safe)` and aggregate (`BatteryHealthDaily.scala:17-26`).
3. `daySeconds = event_date.cast(timestamp).cast(long)` (UTC midnight epoch seconds) (`BatteryHealthDaily.scala:12`, `:28`).
4. Window per `vin_hash` ordered by `daySeconds`, `rangeBetween(-2 × 86400, 0)` →
   `peak_cell_temp_3d = max(peak_cell_temp)` (`BatteryHealthDaily.scala:29-36`).
5. Window per `vin_hash` ordered by `event_date` → `health_change = round(h − lag(h), 2)` (`BatteryHealthDaily.scala:33`, `:37-40`).

## Nulls and edge cases

| Case | Spark result |
|---|---|
| Date gaps | The 3-day window is by **calendar day** (range), so a gap excludes older days; `lag` is by **row**, so `health_change` spans the gap. Fixture: 5 VINs have gaps. |
| First day per VIN | `health_change` null. |
| `battery_health_level` null on the max-sequence row | `health_level_end_of_day` null (and the next day's `health_change` null). Fixture: 15 null `health_level_end_of_day`, 161 null `health_change`. |
| All `charge_percent` null | `avg`/`min` null. |
| All `thermal_alert` null | `sum` null, not 0. |
| VIN appearing with two `model_safe` on one day | Two rows with the same parity key; both windows partition by `vin_hash` only, so they mix. |

Measured on `rust_pipeline/tests/fixtures/expected/battery_health_daily`.

## Hardest Spark behaviour to reproduce

1. **`max_by`** with Spark's null semantics (not in DataFusion 43).
2. **`RANGE BETWEEN` on epoch seconds** derived from a date cast through the UTC session timezone
   (`AnalyticsJob.scala:63`) — DataFusion supports range frames but the order key must be numeric seconds.
3. **`lag` across gaps** combined with a range window in the same query.
4. **HALF_UP rounding** at 2 and 3 dp (`rust_pipeline/src/transforms/rounding.rs`) and float `avg` order.
5. **Partitioned date output** must stay `Date32` (`rust_pipeline/src/validate.rs:185-212`).
