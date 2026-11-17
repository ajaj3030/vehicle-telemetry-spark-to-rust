# `battery_health_daily`: functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala` (abbreviated `BatteryHealthDaily.scala`).
Rust target: `rust_pipeline/src/analytics/battery_health_daily.rs` (currently a stub, `rust_pipeline/src/analytics/battery_health_daily.rs:14-17`).

## Purpose

Battery health per vehicle per day, for warranty and service: charge, cell temperatures, thermal alerts, end-of-day health, a
3-calendar-day peak cell temperature and the change in health since the previous reported day (`BatteryHealthDaily.scala:7`,
`dashboard/app.py:178-184`). The Fleet → Battery view reads it (`dashboard/app.py:662-691`).

## Inputs

| Table | Rows used | Columns used | Source |
|-------|-----------|--------------|--------|
| `vehicle_telemetry` / `vehicle_telemetry_rust` | `event_type = 'BATTERY'` | `vin_hash`, `event_date` (date, from T2 `spark_jobs/transforms.py:87`), `model_safe`, `charge_percent`, `max_cell_temp`, `cell_temp_spread` (T3), `thermal_alert` (T4), `battery_health_level`, `sequence_number` | `BatteryHealthDaily.scala:15-26` |

## Output

Delta `analytics/battery_health_daily`, overwritten, **partitioned by `event_date`** (`BatteryHealthDaily.scala:10`). Key:
`vin_hash, event_date` (`rust_pipeline/src/analytics/battery_health_daily.rs:8-12`). Schema from
`rust_pipeline/tests/fixtures/expected/battery_health_daily/*.parquet`; the fixture has 396 rows for 150 vehicles:

| # | Column | Type | Nullable | Definition (`BatteryHealthDaily.scala`) |
|---|--------|------|----------|------------|
| 1–3 | `vin_hash`, `event_date`, `model_safe` | string, date32, string | yes | Group keys (`:17`) |
| 4 | `readings` | int64 | **no** | `count(*)` (`:19`) |
| 5 | `avg_charge_percent` | double | yes | `round(avg(charge_percent), 2)` (`:20`) |
| 6 | `min_charge_percent` | double | yes | `min(charge_percent)` (`:21`) |
| 7 | `peak_cell_temp` | double | yes | `max(max_cell_temp)` (`:22`) |
| 8 | `avg_cell_temp_spread` | double | yes | `round(avg(cell_temp_spread), 3)` (`:23`) |
| 9 | `thermal_alert_count` | int64 | yes | `sum(cast(thermal_alert as int))` (`:24`) |
| 10 | `health_level_end_of_day` | double | yes | `max_by(battery_health_level, sequence_number)` (`:25`) |
| 11 | `peak_cell_temp_3d` | double | yes | `max(peak_cell_temp)` over this day and the 2 previous **calendar** days (`:28-32,36`) |
| 12 | `health_change` | double | yes | `round(health_level_end_of_day − lag(health_level_end_of_day, 1), 2)` over the previous **row** by date (`:33,37-40`) |

## Calculation, step by step

1. Filter `BATTERY` rows (`BatteryHealthDaily.scala:16`).
2. Group by `(vin_hash, event_date, model_safe)` and compute aggregates 4–10 (`BatteryHealthDaily.scala:17-26`). `event_date` is the
   UTC date of `event_time`, computed upstream by the stream job (`spark_jobs/transforms.py:87`).
3. Compute `peak_cell_temp_3d`: partition by `vin_hash`, order by `event_date` cast to timestamp, then to long (**epoch seconds**),
   with `RANGE BETWEEN 2*86400 PRECEDING AND CURRENT ROW` (`BatteryHealthDaily.scala:28-32`). This is a calendar window: missing days
   simply contribute nothing.
4. Compute `health_change`: partition by `vin_hash`, order by `event_date`, `lag(…, 1)`. This is the previous *row*, however many
   days earlier it is (`BatteryHealthDaily.scala:33,37-40`).

Note: the window partitions by `vin_hash` only, while the grouping also includes `model_safe`. If one vehicle had two `model_safe`
values on the same day, it would have two rows for that day in the same window. That does not happen in the fixture.

## Nulls and edge cases

Checked with a local PySpark 3.5.1 probe unless a file is cited.

| Case | Spark result | In the fixture? |
|------|--------------|-----------------|
| A vehicle's first day | `health_change` null (no previous row) | Yes. 161 null `health_change` in total, including 150 first days |
| Missing calendar days | `peak_cell_temp_3d` only covers days within 2 calendar days (11-07 after 11-04 sees only itself); `health_change` still compares with the previous row (11-07 vs 11-04) | Yes. 5 vehicles have gaps; 104 rows have `peak_cell_temp_3d ≠ peak_cell_temp` |
| `battery_health_level` null on the day's highest-`sequence_number` row | `max_by` returns null, without falling back to an earlier row. That day's `health_change` and the next day's are null | Yes. 15 days have null `health_level_end_of_day` |
| `peak_cell_temp` null on a day | `max` over the window ignores it | No |
| `charge_percent` all null on a day | `avg` and `min` null | No |
| `thermal_alert` null for every row | `thermal_alert_count` null, not 0 | No (`thermal_alert` is never null, `spark_jobs/transforms.py:129-132`) |
| Ties on `sequence_number` | `max_by` picks one row, and which row is not defined | No |

## Spark behaviour that is hardest to reproduce in Rust

1. **The `RANGE` frame over epoch seconds.** In DataFusion, `CAST(CAST(date AS TIMESTAMP) AS BIGINT)` gives **nanoseconds**
   (`1793836800000000000`), not Spark's seconds (DataFusion 43 probe). A literal `2*86400` frame would then cover 0.17 ms instead of
   2 days. Order by the `Date32` value with `RANGE BETWEEN 2 PRECEDING`, or scale by 10⁹. Either way, the fixture's gap vehicles must
   stay calendar-based, not row-based.
2. **`max_by`**, with Spark's null semantics. DataFusion 43 has no `max_by`; see `charging_sessions.md`.
3. **HALF_UP `round`** at 2 and 3 dp, including `round` of a difference of two doubles (`health_change`).
4. **Int64 widths:** `count(*)` is non-null int64 and `sum(cast(bool as int))` is nullable int64, as in the fixture schema.
5. Partitioned output by `event_date`. This is already handled by `write_output` (`rust_pipeline/src/analytics/mod.rs:98-111`).
