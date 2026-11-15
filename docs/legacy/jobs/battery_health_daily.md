# battery_health_daily

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala`
(below, `BatteryHealthDaily.scala:N`). Rust placeholder: `rust_pipeline/src/analytics/battery_health_daily.rs`.
`AnalyticsJob.scala` and `BuildParityFixtures.scala` are in the same directory as the job.

## Purpose

Battery health per vehicle per day, for warranty and service
(`BatteryHealthDaily.scala:7`; `dashboard/app.py:180-185`). Shown on the Fleet
page's Battery Health tab (`dashboard/app.py:662-691`).

## Inputs

Enriched telemetry (`BatteryHealthDaily.scala:43-44`), rows with
`event_type = 'BATTERY'` (`BatteryHealthDaily.scala:16`). Columns:
`vin_hash`, `event_date` (date, `to_date(event_time)` in UTC,
`spark_jobs/transforms.py:87`, `AnalyticsJob.scala:63`), `model_safe`,
`charge_percent`, `max_cell_temp`, `cell_temp_spread`, `thermal_alert`,
`battery_health_level`, `sequence_number`.

## Output

`analytics/battery_health_daily`, Delta overwrite, **partitioned by
`event_date`** (`BatteryHealthDaily.scala:10`, `AnalyticsJob.scala:86-91`).
One row per `(vin_hash, event_date, model_safe)`. Types from
`rust_pipeline/tests/fixtures/expected/battery_health_daily/`.

| Column | Type | Definition |
|---|---|---|
| `vin_hash` | string | group key |
| `event_date` | date | group key, partition column |
| `model_safe` | string | group key |
| `readings` | bigint not null | `count(*)` (`:19`) |
| `avg_charge_percent` | double | `round(avg(charge_percent), 2)` (`:20`) |
| `min_charge_percent` | double | `min(charge_percent)` (`:21`) |
| `peak_cell_temp` | double | `max(max_cell_temp)` (`:22`) |
| `avg_cell_temp_spread` | double | `round(avg(cell_temp_spread), 3)` (`:23`) |
| `thermal_alert_count` | bigint | `sum(cast(thermal_alert as int))` (`:24`) |
| `health_level_end_of_day` | double | `max_by(battery_health_level, sequence_number)` (`:25`) |
| `peak_cell_temp_3d` | double | max of `peak_cell_temp` over this and the previous 2 calendar days (`:28-32,36`) |
| `health_change` | double | `round(health_level_end_of_day − lag(…,1), 2)` by `event_date` (`:33,37-40`) |

## Calculation

1. Keep BATTERY events (`:16`).
2. Group by `(vin_hash, event_date, model_safe)` and compute the aggregates
   above (`:17-26`).
3. Per `vin_hash`, order by `event_date` cast to timestamp (UTC midnight) then
   to epoch seconds, and take `max(peak_cell_temp)` over `RANGE BETWEEN
   172800 PRECEDING AND CURRENT ROW` (`:12,28-32,36`).
4. Per `vin_hash`, ordered by `event_date`, subtract the previous **row's**
   end-of-day health and round to 2 dp (`:33,37-40`).

## Nulls and edge cases

- **Date gaps:** step 3 is a *range* in seconds, so a day with no readings
  shrinks the window (a row two days after a gap sees only itself and the
  day before). Step 4 is *row*-based, so `health_change` compares with the
  last day that had readings, however long ago. Five fixture vehicles have
  gaps (pyarrow on `rust_pipeline/tests/fixtures/input/telemetry/`).
- **Null `battery_health_level`:** `max_by` takes the value at the max
  `sequence_number` even if that value is null (Spark skips only null
  *orderings*). 15 BATTERY inputs and 15 output rows have it null; a null
  then makes `health_change` null on that day and the next.
- **First day per vehicle:** `lag` is null → `health_change` null. With
  the nulls above, 161 of 396 expected rows have null `health_change`.
- **All-null averages / mins:** Spark `avg`, `min`, `max`, `sum` ignore nulls
  and return null for all-null groups; `count(*)` counts rows.
  `thermal_alert` is never null (`spark_jobs/transforms.py:127-132`).
- **`sum` type:** `sum` of int is bigint in Spark (hence `thermal_alert_count` bigint).
- **Same vehicle, two `model_safe` values on one day:** two rows with the
  same `(vin_hash, event_date)`; both are in the same window partition
  (`vin_hash`), and with equal `event_date` the `lag` order between them is
  arbitrary. The parity key `vin_hash, event_date`
  (`rust_pipeline/src/analytics/battery_health_daily.rs:8-12`) assumes this does not happen.

## Hardest Spark behaviour to reproduce

1. **The RANGE window over epoch seconds** across date gaps, with Spark's
   date→timestamp cast in the session time zone (UTC, `AnalyticsJob.scala:63`).
   DataFusion supports RANGE frames, but the ordering expression must be
   numeric seconds, not the date, to keep `-2 * 86400` meaningful.
2. **`max_by` null semantics** — returns the value of the row with the
   greatest non-null ordering even when that value is null; DataFusion has
   no exact built-in (`last_value … ORDER BY` skips or keeps nulls
   differently depending on options).
3. **`round` HALF_UP on the decimal value**, on averages and on a difference
   of doubles (`rust_pipeline/src/transforms/rounding.rs:1-6`).
4. **Window `ORDER BY` ASC defaults to NULLS FIRST** in Spark (relevant only
   if `event_date` were null, which requires an unparseable `event_time`).
5. Writing the output partitioned by `event_date` (checked by
   `rust_pipeline/src/analytics/mod.rs:120-132`).
