# battery_health_daily

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala`
(cited below as `BatteryHealthDaily.scala:<line>`). Rust stub:
`rust_pipeline/src/analytics/battery_health_daily.rs`.

## Purpose

Battery health per vehicle per day, for warranty and service
(`BatteryHealthDaily.scala:7`): daily charge and cell-temperature summary,
thermal alerts, end-of-day state of health, its day-over-day change and a
3-day peak cell temperature. The dashboard's Battery Health tab reads it
(`dashboard/app.py:662-691`).

## Inputs

One table, `vehicle_telemetry` (`BatteryHealthDaily.scala:43-44`), BATTERY
rows only. Columns used: `event_type`, `vin_hash`, `event_date`,
`model_safe`, `charge_percent`, `max_cell_temp`, `cell_temp_spread`,
`thermal_alert`, `battery_health_level`, `sequence_number`. Three of these
are enrichment outputs: `event_date` is `to_date(event_time)` in UTC
(`spark_jobs/transforms.py:87`, `spark_jobs/streaming_job.py:136`),
`cell_temp_spread` is `round(max_cell_temp - min_cell_temp, 2)`
(`spark_jobs/transforms.py:117-118`), and `thermal_alert` is a null-safe
boolean (`spark_jobs/transforms.py:129-132`).

## Output schema

Table `analytics/battery_health_daily`, overwritten, **partitioned by
`event_date`** (`BatteryHealthDaily.scala:10`, `AnalyticsJob.scala:87-91`).
Rust parity key: `vin_hash`, `event_date`
(`rust_pipeline/src/analytics/battery_health_daily.rs:10`). Types from
`rust_pipeline/tests/fixtures/expected/battery_health_daily/`.

| Column | Type | Meaning |
|---|---|---|
| `vin_hash` | string | Pseudonymised vehicle |
| `event_date` | date | UTC day |
| `model_safe` | string | Model |
| `readings` | bigint, not null | BATTERY events that day |
| `avg_charge_percent` | double | Mean state of charge, 2 dp |
| `min_charge_percent` | double | Lowest state of charge |
| `peak_cell_temp` | double | Max of `max_cell_temp` |
| `avg_cell_temp_spread` | double | Mean `cell_temp_spread`, 3 dp |
| `thermal_alert_count` | bigint | Events with `thermal_alert` |
| `health_level_end_of_day` | double | `battery_health_level` of the day's highest `sequence_number` |
| `peak_cell_temp_3d` | double | Max `peak_cell_temp` over this day and the 2 calendar days before |
| `health_change` | double | Change from the previous recorded day, 2 dp |

## Calculation

1. Keep `event_type = 'BATTERY'` (`BatteryHealthDaily.scala:16`).
2. Group by `vin_hash, event_date, model_safe` (`BatteryHealthDaily.scala:17`):
   - `readings = count(*)`
   - `avg_charge_percent = round(avg(charge_percent), 2)`
   - `min_charge_percent = min(charge_percent)`
   - `peak_cell_temp = max(max_cell_temp)`
   - `avg_cell_temp_spread = round(avg(cell_temp_spread), 3)`
   - `thermal_alert_count = sum(cast(thermal_alert as int))`
   - `health_level_end_of_day = max_by(battery_health_level, sequence_number)`

   (`BatteryHealthDaily.scala:18-26`)
3. `peak_cell_temp_3d = max(peak_cell_temp)` over `PARTITION BY vin_hash
   ORDER BY <event_date as epoch seconds> RANGE BETWEEN 172800 PRECEDING AND
   CURRENT ROW`. The epoch seconds are midnight UTC, because the session time
   zone is UTC (`BatteryHealthDaily.scala:12`, `:28-32`, `:36`,
   `AnalyticsJob.scala:63`).
4. `health_change = round(health_level_end_of_day - lag(health_level_end_of_day, 1), 2)`
   over `PARTITION BY vin_hash ORDER BY event_date` (`BatteryHealthDaily.scala:33`, `:37-40`).

## Nulls and edge cases

*Verified* means run through the same Spark functions under PySpark 3.5.1
locally. Fixture counts are from `rust_pipeline/tests/fixtures/` (396 output
rows for 150 vehicles: 98 with 3 days, 50 with 2, 2 with 1).

| Case | Spark behaviour |
|---|---|
| Null `battery_health_level` on the day's last reading | `max_by` returns **NULL**. It does not fall back to an earlier non-null reading (*verified*). The fixture has 15 such days and 15 NULL `health_level_end_of_day`. |
| Null `sequence_number` | That row is ignored by `max_by` (*verified*). The column is never null in practice. |
| Ties on `sequence_number` within a day | `max_by` returns one of the tied values, depending on input order (*verified*). The fixture has none. |
| First recorded day of a vehicle | `health_change` is NULL. So is any day where either side is NULL: 161 NULLs in the fixture. |
| Missing calendar days | `lag` compares with the **previous recorded day**, not yesterday. The 3-day window covers only the days that exist within the last 2 calendar days (*verified*). 5 fixture vehicles have gaps. |
| Null `peak_cell_temp` | Ignored by the window `max`. The window still returns the max of the other days (*verified*). |
| `avg` / `min` / `max` over only NULLs | NULL. `charge_percent` and the cell temperatures have no NULLs in the fixture's BATTERY rows. |
| `thermal_alert` all NULL | `sum` would be NULL (*verified*), but `thermal_alert` is never NULL because of the `coalesce` in `spark_jobs/transforms.py:60-62`. |
| `model_safe` changes within a day | Two rows with the same `(vin_hash, event_date)`, which collide on the parity key. The fixture has none. |

## Hardest Spark behaviour to reproduce

1. **`max_by` semantics.** It keeps a NULL value at the max ordering key,
   skips NULL ordering keys, and resolves ties arbitrarily. The closest
   DataFusion form is `last_value(x ORDER BY sequence_number)`. In DataFusion
   43.1.0 (checked locally) that keeps a NULL value as Spark does, but a row
   with a NULL ordering key **wins**, because NULLs sort last, while Spark
   skips it. Filter `sequence_number IS NOT NULL` or order with NULLS FIRST.
2. **The RANGE frame over epoch seconds.** The frame must be 2 calendar
   days back over sparse dates, not 2 rows. Order by the same
   `Date32 → timestamp → seconds` value, or use an interval range on the date.
3. **Rounding after aggregation.** `round(avg(…), 2/3)` is Spark HALF_UP on
   the decimal value (`rust_pipeline/src/transforms/rounding.rs:1-7`). A
   different summation order can shift an average by float noise and flip a
   value near a rounding boundary, for example x.xx5 → x.xx or x.xx+0.01.
   The 1e-9 tolerance (`rust_pipeline/src/validate.rs:23`) does not absorb a
   0.01 flip.
4. **Partitioned Delta output by a Date32 column.** `write_output` already
   supports this (`rust_pipeline/src/analytics/mod.rs:98-111`, `:134-161`).
