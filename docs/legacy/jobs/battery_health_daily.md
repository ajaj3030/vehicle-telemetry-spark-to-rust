# battery_health_daily — functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala` (line refs to this file unless stated).
Rust placeholder: `rust_pipeline/src/analytics/battery_health_daily.rs`.
Expected output fixture: `rust_pipeline/tests/fixtures/expected/battery_health_daily/`.

## Purpose

Battery health per vehicle per day, for warranty and service (line 7): daily charge and cell temperature
stats, a rolling 3-day peak cell temperature, end-of-day state of health and its change since the
vehicle's previous reporting day. Dashboard: Battery Health tab (`dashboard/app.py:662-691`), which ranks
the largest day-over-day drops from `health_change` (`dashboard/app.py:686-691`).

## Inputs

| Table | Rows used | Columns used |
|---|---|---|
| telemetry | `event_type = 'BATTERY'` (line 16) | `vin_hash`, `event_date`, `model_safe`, `charge_percent`, `max_cell_temp`, `cell_temp_spread`, `thermal_alert`, `battery_health_level`, `sequence_number` |

Upstream derivations it relies on: `event_date = to_date(event_time)` in UTC (`spark_jobs/transforms.py:87`,
`spark_jobs/streaming_job.py:136`); `cell_temp_spread = round(max_cell_temp - min_cell_temp, 2)` (T3,
`spark_jobs/transforms.py:117-118`); `thermal_alert` is null-safe boolean, never null
(`spark_jobs/transforms.py:128-141`).

## Output

Partitioned by `event_date` (line 10); key (`vin_hash`, `event_date`) (`rust_pipeline/src/analytics/battery_health_daily.rs:8-12`).

| Column | Type | Definition |
|---|---|---|
| vin_hash | string | group key |
| event_date | date32 | group key, partition column |
| model_safe | string | group key |
| readings | int64 | `count(*)` |
| avg_charge_percent | double | `round(avg(charge_percent), 2)` |
| min_charge_percent | double | `min(charge_percent)` |
| peak_cell_temp | double | `max(max_cell_temp)` |
| avg_cell_temp_spread | double | `round(avg(cell_temp_spread), 3)` |
| thermal_alert_count | int64 | `sum(cast(thermal_alert as int))` |
| health_level_end_of_day | double | `max_by(battery_health_level, sequence_number)` |
| peak_cell_temp_3d | double | `max(peak_cell_temp)` over this and the previous 2 calendar days |
| health_change | double | `round(health_level_end_of_day - lag(health_level_end_of_day), 2)` |

## Calculation

1. Filter `event_type = 'BATTERY'` (line 16).
2. `groupBy(vin_hash, event_date, model_safe)` and aggregate (lines 17-26) as in the table above.
3. Rolling 3-day window (lines 28-32): `PARTITION BY vin_hash ORDER BY cast(cast(event_date as timestamp) as long)`
   (seconds since epoch of the day's midnight, UTC) `RANGE BETWEEN 2*86400 PRECEDING AND CURRENT ROW`
   (`SecondsPerDay`, line 12). `peak_cell_temp_3d = max(peak_cell_temp)` over it (line 36). It is a
   **calendar** range: a day with no readings shortens the window rather than reaching further back.
4. `health_change` (lines 37-40): `lag(health_level_end_of_day, 1)` over `PARTITION BY vin_hash ORDER BY event_date`
   (line 33) — the previous **reporting** day, however many calendar days back — subtracted from today's,
   rounded to 2 dp.

## Nulls and edge cases

Confirmed with a local PySpark 3.5.1 probe unless marked "fixture".

| Case | Spark result |
|---|---|
| first reporting day of a vehicle | `health_change` null (no lag row); 161 of 396 fixture rows (fixture) |
| missing days (5 fixture vehicles have date gaps) | `peak_cell_temp_3d` only looks back 2 calendar days (a 3-day gap → just today's value); `health_change` compares with the last reporting day across the gap |
| `battery_health_level` null on the day's last reading (15 fixture rows) | `max_by` returns the value of the max-`sequence_number` row **even when that value is null** → `health_level_end_of_day` null (15 fixture rows); next day's `health_change` is null too |
| `sequence_number` null | `max_by` ignores rows with null ordering (impossible upstream) |
| ties on `sequence_number` within a day | `max_by` picks arbitrarily; no ties in fixtures |
| `charge_percent` null | ignored by `avg`/`min`; all-null day → null (no null BATTERY `charge_percent` in the fixture) |
| `thermal_alert` all null | `sum` → null, not 0 (cannot happen: upstream coalesces to false) |
| vehicle's `model_safe` changes within one day | two rows with the same (vin_hash, event_date) key — breaks the key contract; both are peers in the windows (not in fixtures, 0 such days) |
| rounding | HALF_UP on the decimal value (`round(1.005, 2) = 1.01`) |

## Spark behaviour hardest to reproduce in Rust

1. **`max_by`** (line 25): DataFusion has no built-in `max_by`; must reproduce "value of the row with max
   ordering, nulls in the *value* kept, nulls in the *ordering* ignored". `FIRST_VALUE(x ORDER BY seq DESC)`
   is close but must ignore null `seq` rows.
2. **`rangeBetween` on a cast-to-seconds date** (lines 28-32): needs a RANGE frame on a numeric key
   equivalent to `event_date` as epoch seconds; a ROWS frame would be wrong whenever days are missing.
3. **`lag` over reporting days**, not calendar days (lines 33, 37-40).
4. **`round` HALF_UP** at 2 and 3 dp (lines 20, 23, 39) — use `rust_pipeline/src/transforms/rounding.rs:7-28`.
5. **Partitioned Delta write** by `event_date` — already supported by the shared writer
   (`rust_pipeline/src/analytics/mod.rs:97-111`, test at `rust_pipeline/src/analytics/mod.rs:134-161`).
