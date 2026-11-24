# `battery_health_daily` functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala` (cited as `BatteryHealthDaily.scala:N`).
Rust target: `rust_pipeline/src/analytics/battery_health_daily.rs` (stub).

## Purpose
Battery health per vehicle per day, for warranty and service: daily summary, 3-day peak
cell temperature and day-over-day health change (`BatteryHealthDaily.scala:7`,
`README.md:76`). Shown on the dashboard battery tab (`dashboard/app.py:662-691`).

## Inputs
`vehicle_telemetry` (`BatteryHealthDaily.scala:43-44`), rows with `event_type = 'BATTERY'`
(`:16`). Columns used: `vin_hash`, `event_date`, `model_safe` (`:17`), `charge_percent`,
`max_cell_temp`, `cell_temp_spread`, `thermal_alert`, `battery_health_level`,
`sequence_number` (`:19-25`). Derived upstream: `event_date` (T2,
`spark_jobs/transforms.py:82-96`), `cell_temp_spread` (T3, `spark_jobs/transforms.py:99-124`),
`thermal_alert` (T4, `spark_jobs/transforms.py:127-156`).

## Output schema
Table `analytics/battery_health_daily`, overwritten each run, partitioned by `event_date`
(`BatteryHealthDaily.scala:10`, `AnalyticsJob.scala:75-97`). Key: `vin_hash, event_date`
(`rust_pipeline/src/analytics/battery_health_daily.rs:10`). Types from
`rust_pipeline/tests/fixtures/expected/battery_health_daily/`:

| Column | Type | Nullable |
|---|---|---|
| `vin_hash` | string | yes |
| `event_date` | date | yes |
| `model_safe` | string | yes |
| `readings` | long | no |
| `avg_charge_percent`, `min_charge_percent`, `peak_cell_temp`, `avg_cell_temp_spread` | double | yes |
| `thermal_alert_count` | long | yes |
| `health_level_end_of_day`, `peak_cell_temp_3d`, `health_change` | double | yes |

## Calculation
1. Keep `BATTERY` rows (`:16`).
2. Group by `(vin_hash, event_date, model_safe)` (`:17`) and compute: `readings` = row count;
   `avg_charge_percent` = `round(avg(charge_percent), 2)`; `min_charge_percent`;
   `peak_cell_temp` = `max(max_cell_temp)`; `avg_cell_temp_spread` = `round(avg(cell_temp_spread), 3)`;
   `thermal_alert_count` = `sum(cast(thermal_alert as int))`; `health_level_end_of_day` =
   `battery_health_level` at the highest `sequence_number` of the day (`max_by`) (`:18-26`).
3. `peak_cell_temp_3d` = max of `peak_cell_temp` over the vehicle's rows whose day is between
   2 days before and the current day: a `RANGE` frame of `[-172800, 0]` seconds over
   `cast(cast(event_date as timestamp) as long)`, partitioned by `vin_hash` (`:12`, `:28-32`, `:36`).
4. `health_change` = `round(health_level_end_of_day - lag(health_level_end_of_day, 1), 2)`, where
   `lag` is the previous row for the same `vin_hash` ordered by `event_date` (`:33`, `:37-40`).

## Nulls and edge cases
- **Gaps in days:** the 3-day frame is calendar-based, so a missing day shrinks the window rather
  than reaching further back (`:29-32`). `lag` is row-based, so after a gap `health_change`
  compares with the last day that had readings, not the calendar previous day (`:33`, `:39`).
- **First day of each vehicle:** `health_change` is null. It is also null when either day's
  `health_level_end_of_day` is null. 161 of 396 expected rows have null `health_change`
  (`rust_pipeline/tests/fixtures/expected/battery_health_daily/`).
- **`max_by` with a null value:** returns null if the highest-`sequence_number` reading has a null
  `battery_health_level`, even if earlier readings that day have a value. 15 input BATTERY rows
  have null `battery_health_level`, and 15 expected rows have a null `health_level_end_of_day`
  (`rust_pipeline/tests/fixtures/input/telemetry/`, `.../expected/battery_health_daily/`).
- **Aggregates skip nulls:** `avg`, `min`, `max`, `sum` are null only when every input is null.
- **Two `model_safe` values for one vehicle and day** would give two rows sharing a key and an
  `event_date`; both fall in each other's 3-day frame (RANGE includes peers) and their `lag` order is
  undefined (`:17`, `:29-33`). `model_safe` comes from the event's `model` (`spark_jobs/transforms.py:69-79`).
- **Date to seconds** uses the UTC session time zone, so each day is a multiple of 86400
  (`AnalyticsJob.scala:55-68`).

## Spark behaviour hardest to reproduce in Rust
1. **`RANGE BETWEEN 172800 PRECEDING AND CURRENT ROW` over a derived long**, including peers with
   the same value. DataFusion supports range frames, but the ordering key must be the same
   seconds value, and the result must match with gaps.
2. **`max_by` null semantics** (value kept even if null; null ordering keys ignored).
3. **`lag` over `event_date` with possible ties**.
4. **`round` HALF_UP** on doubles, including on a difference of two doubles (`:39`)
   (`rust_pipeline/src/transforms/rounding.rs`).
5. **`sum(cast(boolean as int))` returns a nullable long**, and `count(*)` a non-null long; the parity
   check compares types (`rust_pipeline/src/validate.rs:185-212`).
6. **Partitioned Delta write by `event_date`** (`rust_pipeline/src/analytics/mod.rs:98-111`).
