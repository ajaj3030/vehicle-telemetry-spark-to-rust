# `battery_health_daily`

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala` (below `BatteryHealthDaily.scala`). Runner: `spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala` (below `AnalyticsJob.scala`).

## Purpose

One row per vehicle per day summarising battery readings (charge, cell temperature, thermal alerts, end-of-day health), plus a rolling three-day peak temperature and the day-over-day health change (`BatteryHealthDaily.scala:14-41`). Shown on the dashboard's battery view (`dashboard/app.py:662-691`).

## Inputs

- `vehicle_telemetry`, rows with `event_type = 'BATTERY'` (`BatteryHealthDaily.scala:16,43-44`). Columns used: `vin_hash`, `event_date`, `model_safe`, `charge_percent`, `max_cell_temp`, `cell_temp_spread`, `thermal_alert`, `battery_health_level`, `sequence_number` (`BatteryHealthDaily.scala:17-25`).
- Upstream derivations: `event_date` from `event_time` in the stream's time-dimension transform (`spark_jobs/transforms.py:82-96`); `cell_temp_spread = round(max_cell_temp - min_cell_temp, 2)` (`spark_jobs/transforms.py:117-118`); `thermal_alert` (`spark_jobs/transforms.py:127-132`); `model_safe` (`spark_jobs/transforms.py:69-73`).

## Output schema

Delta `<analytics.output_base>/battery_health_daily`, overwrite, **partitioned by `event_date`** (`BatteryHealthDaily.scala:10`, `AnalyticsJob.scala:87-91`). Types from `rust_pipeline/tests/fixtures/expected/battery_health_daily/` (396 rows):

| Column | Type | Definition |
|--------|------|------------|
| `vin_hash` | string | group key |
| `event_date` | date32 | group key, partition column |
| `model_safe` | string | group key |
| `readings` | int64 | `count(*)` |
| `avg_charge_percent` | double | `round(avg(charge_percent), 2)` |
| `min_charge_percent` | double | `min(charge_percent)` |
| `peak_cell_temp` | double | `max(max_cell_temp)` |
| `avg_cell_temp_spread` | double | `round(avg(cell_temp_spread), 3)` |
| `thermal_alert_count` | int64 | `sum(cast(thermal_alert as int))` |
| `health_level_end_of_day` | double | `max_by(battery_health_level, sequence_number)` |
| `peak_cell_temp_3d` | double | max `peak_cell_temp` over this and the previous two calendar days |
| `health_change` | double | `round(health_level_end_of_day - previous row's value, 2)` |

Sources: `BatteryHealthDaily.scala:17-40`.

## Calculation

1. Filter `event_type = 'BATTERY'` (`BatteryHealthDaily.scala:16`).
2. Group by `(vin_hash, event_date, model_safe)` and compute the seven aggregates in the table (`BatteryHealthDaily.scala:17-26`).
3. Window `lastThreeDays`: per `vin_hash`, order by `event_date` cast to timestamp then to long (epoch seconds, midnight UTC), frame `RANGE BETWEEN 2*86400 PRECEDING AND CURRENT ROW` (`BatteryHealthDaily.scala:12,28-32`). `peak_cell_temp_3d = max(peak_cell_temp)` over that frame (`BatteryHealthDaily.scala:36`).
4. Window `byDay`: per `vin_hash`, order by `event_date` (`BatteryHealthDaily.scala:33`). `health_change = round(health_level_end_of_day - lag(health_level_end_of_day, 1), 2)` (`BatteryHealthDaily.scala:37-40`).

## Nulls and edge cases

*(probe)* = confirmed with `pyspark==3.5.1`, session time zone UTC (`AnalyticsJob.scala:63`).

- **`avg`/`min`/`max` skip nulls**; if all inputs in a group are null the result is null *(probe)*.
- **`max_by` returns the value at the highest `sequence_number` even when that value is null** (it does not fall back to an older non-null reading); rows with a null `sequence_number` are ignored *(probe)*. The fixture has 15 BATTERY rows with null `battery_health_level` (`rust_pipeline/tests/fixtures/input/telemetry/`).
- **`thermal_alert`** is built from `_is_true(...) | _is_true(...)` (`spark_jobs/transforms.py:127-132`), so it is never null and `thermal_alert_count` counts `true` rows.
- **Missing days:** the 3-day frame is by calendar value, so a gap shortens the frame (days D-2..D that exist). `lag` takes the previous **row**, not the previous calendar day, so after a gap `health_change` compares across more than one day. 5 vehicles in the expected fixture have such gaps (`rust_pipeline/tests/fixtures/expected/battery_health_daily/`).
- **First day per vehicle:** `lag` is null, so `health_change` is null; a null on either side also gives null.
- **`model_safe` changing within a day** would split the day into two rows, while both windows partition only by `vin_hash` (`BatteryHealthDaily.scala:17,30,33`), so the two rows share frames.
- **Ties on `sequence_number`** in `max_by` are non-deterministic.
- `event_date` is derived in UTC upstream (`spark_jobs/transforms.py:87`, `spark_jobs/streaming_job.py:135-136`).

## Hardest Spark behaviour to reproduce in Rust

1. **`max_by` null semantics.** DataFusion's `last_value(x ORDER BY sequence_number)` matches only if it does not skip nulls (no `IGNORE NULLS`) and ignores rows whose ordering key is null.
2. **`RANGE` frame over epoch seconds of a date.** DataFusion supports `RANGE BETWEEN` on numeric or interval orders; the port must order by the same day-number (or seconds) and use a 2-day/172800-second range, not `ROWS BETWEEN 2 PRECEDING`, to match gaps.
3. **`lag` by row with the partition only on `vin_hash`**, including the `model_safe` split case above.
4. **HALF_UP `round` at three scales** (2 and 3, `BatteryHealthDaily.scala:20,23,39`) via `rust_pipeline/src/transforms/rounding.rs:7`.
5. **Partitioned Delta write by `event_date`**, passed through `Job.partition_cols` (`rust_pipeline/src/analytics/mod.rs:28-32,98-111`).
