# `battery_health_daily` — functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala`
(cited as `BatteryHealthDaily.scala:<line>`). Rust stub:
`rust_pipeline/src/analytics/battery_health_daily.rs`.

## Purpose

Battery health per vehicle per day, for warranty and service
(`BatteryHealthDaily.scala:7`): charge, cell temperatures, thermal alerts,
end-of-day health, a 3-day peak cell temperature and the day-over-day health
change (`dashboard/app.py:179-185`). Shown on the dashboard's Battery Health
tab (`dashboard/app.py:662-691`).

## Inputs

| Table | Columns used | Filter |
|-------|--------------|--------|
| enriched telemetry (`AnalyticsJob.scala:49`) | `event_type`, `vin_hash`, `event_date`, `model_safe`, `charge_percent`, `max_cell_temp`, `cell_temp_spread`, `thermal_alert`, `battery_health_level`, `sequence_number` | `event_type = 'BATTERY'` (`BatteryHealthDaily.scala:16`) |

Derived inputs: `event_date` is `to_date(event_time)` in UTC
(`spark_jobs/transforms.py:87`), `cell_temp_spread` is
`round(max_cell_temp − min_cell_temp, 2)` (`spark_jobs/transforms.py:117-118`),
`thermal_alert` is the null-safe T4 flag (`spark_jobs/transforms.py:129-132`,
`spark_jobs/transforms.py:141`).

## Output

Table `analytics/battery_health_daily`, overwritten every run, **partitioned by
`event_date`** (`BatteryHealthDaily.scala:9-10`). Parity key `vin_hash`,
`event_date` (`rust_pipeline/src/analytics/battery_health_daily.rs:8-12`).
Types from `rust_pipeline/tests/fixtures/expected/battery_health_daily/`:

| Column | Type | Definition |
|--------|------|------------|
| `vin_hash` | string | group key |
| `event_date` | date | group key |
| `model_safe` | string | group key |
| `readings` | bigint | `count(*)` |
| `avg_charge_percent` | double | `round(avg(charge_percent), 2)` |
| `min_charge_percent` | double | `min(charge_percent)` |
| `peak_cell_temp` | double | `max(max_cell_temp)` |
| `avg_cell_temp_spread` | double | `round(avg(cell_temp_spread), 3)` |
| `thermal_alert_count` | bigint | `sum(cast(thermal_alert as int))` |
| `health_level_end_of_day` | double | `battery_health_level` of the day's highest `sequence_number` |
| `peak_cell_temp_3d` | double | max `peak_cell_temp` over this day and the two calendar days before |
| `health_change` | double | `round(health_level_end_of_day − previous row's value, 2)` |

## Calculation, step by step

1. Filter BATTERY events (`BatteryHealthDaily.scala:16`).
2. Group by `vin_hash`, `event_date`, `model_safe` and aggregate
   (`BatteryHealthDaily.scala:17-26`).
3. `peak_cell_temp_3d`: window per `vin_hash` ordered by
   `cast(cast(event_date as timestamp) as long)` (epoch seconds of midnight
   UTC) with `rangeBetween(-2 × 86400, 0)` (`BatteryHealthDaily.scala:12`,
   `BatteryHealthDaily.scala:28-32`, `BatteryHealthDaily.scala:36`). This is a
   **calendar** window: a missing day shrinks it, it does not reach further back.
4. `health_change`: `lag(health_level_end_of_day, 1)` per `vin_hash` ordered by
   `event_date` (`BatteryHealthDaily.scala:33`, `BatteryHealthDaily.scala:37-40`).
   This is a **row** offset: after a gap it compares with the last day that has
   data, not with the previous calendar day.

## Nulls and edge cases

*Probe* = checked with local PySpark 3.5.1 (not committed). Fixture counts are
DuckDB queries over `rust_pipeline/tests/fixtures/`.

| Case | Spark result | In the fixture? |
|------|--------------|-----------------|
| First day for a vehicle | `health_change` null | yes: 150 vehicles |
| Day's last reading has `battery_health_level` null | `max_by` returns that null, so `health_level_end_of_day` is **null** even if earlier readings that day had a value (*probe*) | yes: 15 null readings, all the last of their day → 15 null end-of-day values |
| A null end-of-day value | `health_change` null on that day **and** the next day (lag of null) (*probe*) | yes: 161 null `health_change` in total |
| Missing days (gap) | `peak_cell_temp_3d` only covers dates within 2 calendar days; `health_change` compares with the last present day (*probe*) | yes: 5 gaps of 3 days |
| `charge_percent` null on some readings | ignored by `avg` and `min`; `readings` still counts the row | no: 0 null on BATTERY rows |
| `max_cell_temp` / `cell_temp_spread` null | ignored by `max`/`avg` | no |
| `thermal_alert` | never null (`spark_jobs/transforms.py:129-132`), so the count is never null | yes |
| Same vehicle and day under two `model_safe` values | two rows with the same parity key | no |
| Ties on `sequence_number` | `max_by` picks one arbitrarily | no: unique per VIN (`data_generator/generator.py:150-154`) |
| `peak_cell_temp_3d` ≠ `peak_cell_temp` | an earlier day in the window was hotter | yes: 104 rows |

## Spark behaviour hardest to reproduce in Rust

1. **`max_by` null semantics.** DataFusion 43 has no `max_by`. The
   replacement (e.g. `first_value(battery_health_level ORDER BY sequence_number DESC)`)
   must return the null at the top rather than the last non-null value, which
   is what Spark does and what 15 fixture rows check.
2. **Range window over days vs. row lag.** Two different windows over the same
   partition: a range frame in seconds (calendar-aware) and a row offset. A port
   that uses one for both, or fills missing days, changes results on the 5
   gaps. DataFusion accepts `RANGE BETWEEN INTERVAL '2 days' PRECEDING AND CURRENT ROW`
   on a date column (checked with the `datafusion==43.1.0` Python bindings).
3. **`round` HALF_UP** on three columns, including the difference of two
   doubles (`health_change`), where binary noise decides the result
   (`rust_pipeline/src/transforms/rounding.rs:1-28`).
4. **Partitioned output** by `event_date` (`rust_pipeline/src/analytics/mod.rs:98-111`).
