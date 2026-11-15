# battery_health_daily

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala`. Rust stub: `rust_pipeline/src/analytics/battery_health_daily.rs`.
Fixture numbers measured with DuckDB over `rust_pipeline/tests/fixtures/input/telemetry` and `rust_pipeline/tests/fixtures/expected/battery_health_daily`.

## Purpose
Per vehicle per day: charge, temperature, thermal alerts and end-of-day health, plus a 3-day rolling peak temperature and the day-over-day health change (`README.md:76`, `spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala:14-41`).

## Inputs
| Table | Columns used | Cited in |
|---|---|---|
| `vehicle_telemetry` (Rust: `vehicle_telemetry_rust`) | `event_type`, `vin_hash`, `event_date`, `model_safe`, `charge_percent`, `max_cell_temp`, `cell_temp_spread`, `thermal_alert`, `battery_health_level`, `sequence_number` | `:16-25,43-44` |

`event_date`, `cell_temp_spread` and `thermal_alert` are computed upstream in `spark_jobs/transforms.py:87,117-118,129-141`.

## Output schema
Partitioned by `event_date` (`:10`), overwritten (`spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala:87-91`). Key `vin_hash, event_date` (`README.md:76`).

| # | Column | Spark type | Source |
|---|---|---|---|
| 1 | `vin_hash` | string | key, `:17` |
| 2 | `event_date` | date | key, `:17` |
| 3 | `model_safe` | string | key, `:17` |
| 4 | `readings` | bigint | `count(*)`, `:19` |
| 5 | `avg_charge_percent` | double | `round(avg,2)`, `:20` |
| 6 | `min_charge_percent` | double | `:21` |
| 7 | `peak_cell_temp` | double | `max(max_cell_temp)`, `:22` |
| 8 | `avg_cell_temp_spread` | double | `round(avg,3)`, `:23` |
| 9 | `thermal_alert_count` | bigint | `sum(cast int)`, `:24` |
| 10 | `health_level_end_of_day` | double | `max_by(battery_health_level, sequence_number)`, `:25` |
| 11 | `peak_cell_temp_3d` | double | `:28-32,36` |
| 12 | `health_change` | double | `:33,37-40` |

## Calculation
1. Keep `event_type = 'BATTERY'` (`:16`).
2. Group by (`vin_hash`, `event_date`, `model_safe`) and aggregate (`:17-26`).
3. `peak_cell_temp_3d` = max of `peak_cell_temp` over rows of the same vehicle whose `event_date` (cast to timestamp, then epoch seconds) is within **2×86400 s before** the current day, inclusive — i.e. today and the two previous *calendar* days that have data (`:12,28-32,36`).
4. `health_change` = round(today's `health_level_end_of_day` − the previous **row's** value ordered by `event_date`, 2) (`:33,37-40`).

## Nulls and edge cases
- **First day of a vehicle:** `lag` is null → `health_change` null.
- **Null end-of-day health:** `max_by` returns the latest row's value even if null; null propagates to that day's and the next day's `health_change`. Fixture: 15 null `battery_health_level` inputs → 15 null `health_level_end_of_day`; 161 null `health_change` across 150 vehicles.
- **Gap days:** the range window is calendar-based (a 3-day gap leaves only today in the frame), but `lag` is row-based (compares with the last day that had data, however long ago).
- **Vehicle with two `model_safe` values on one day** would make two rows for the same key and both would be in the same window partition (partitioned by `vin_hash` only, `:30,33`).
- `avg`/`min`/`max` skip nulls; `thermal_alert` is never null (`spark_jobs/transforms.py:141`).
- Days are UTC (`spark_jobs/transforms.py:87`, `spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala:63`).

## Hardest Spark behaviour to reproduce
1. **`rangeBetween` over date→timestamp→long seconds** (`:28-32`): DataFusion needs a RANGE frame on a numeric/interval order key; a ROWS frame gives different answers on gap days.
2. **`lag` vs. range window mixing** in one output, with different gap semantics.
3. **`max_by` null semantics** and HALF_UP rounding of averages (`rust_pipeline/src/transforms/rounding.rs:1-7`).
4. `event_date` as partition column (`rust_pipeline/src/analytics/mod.rs:98-111`).
