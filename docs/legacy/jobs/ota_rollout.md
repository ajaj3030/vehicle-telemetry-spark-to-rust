# ota_rollout

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala`
(cited below as `OtaRollout.scala:<line>`). Rust stub:
`rust_pipeline/src/analytics/ota_rollout.rs`.

## Purpose

OTA rollout status per target software version and vehicle model: how many
vehicles are in each update state, based on each vehicle's latest update
event, plus success and failure rates (`OtaRollout.scala:7`,
`dashboard/app.py:193-199`). The dashboard's OTA Rollout tab reads it
(`dashboard/app.py:714-730`).

## Inputs

One table, `vehicle_telemetry` (`OtaRollout.scala:46-47`), OTA rows only.
Columns used: `event_type`, `vin_hash`, `target_software_version`,
`sequence_number`, `model_safe`, `update_state`. `model_safe` is never null
(`spark_jobs/transforms.py:73`).

## Output schema

Table `analytics/ota_rollout`, overwritten on every run, not partitioned
(`AnalyticsJob.scala:77`, `:87-91`). Rust parity key: `target_software_version`,
`model_safe` (`rust_pipeline/src/analytics/ota_rollout.rs:10`). Types from
`rust_pipeline/tests/fixtures/expected/ota_rollout/`.

| Column | Type | Meaning |
|---|---|---|
| `target_software_version` | string (nullable) | Version being rolled out |
| `model_safe` | string | Vehicle model, `MODEL_UNKNOWN` if absent |
| `UPDATE_STATE_IN_PROGRESS` | bigint, not null | Vehicles whose latest state is this value |
| `UPDATE_STATE_WAITING_ON_BCM` | bigint, not null | same |
| `UPDATE_STATE_SUCCESS` | bigint, not null | same |
| `UPDATE_SUCCESS_WITH_WARNINGS` | bigint, not null | same |
| `UPDATE_STATE_FAILED` | bigint, not null | same |
| `UPDATE_FAILED_DRIVE_ALLOWED` | bigint, not null | same |
| `UPDATE_FAILED_NO_ACTION` | bigint, not null | same |
| `vehicles` | bigint, not null | Sum of the seven state columns |
| `success_rate_pct` | double | % of `vehicles` that succeeded, 2 dp |
| `failure_rate_pct` | double | % of `vehicles` that failed, 2 dp |

## Calculation

1. Keep rows with `event_type = 'OTA'` (`OtaRollout.scala:32`).
2. **Latest event per vehicle per target version:** `row_number()` over
   `PARTITION BY vin_hash, target_software_version ORDER BY sequence_number DESC`,
   keep row 1 (`OtaRollout.scala:27-34`). A vehicle is counted once per
   target version, under the `model_safe` of that latest row.
3. **Pivot:** group by `target_software_version, model_safe`, pivot
   `update_state` over the **fixed** list of seven states in the order above,
   and count (`OtaRollout.scala:11-19`, `:36-39`). Fill missing counts with 0
   (`OtaRollout.scala:40`).
4. `vehicles` = sum of the seven state columns, not the number of rows
   (`OtaRollout.scala:23`, `:41`).
5. `success_rate_pct = round((SUCCESS + SUCCESS_WITH_WARNINGS) / vehicles * 100, 2)`
   (`OtaRollout.scala:20`, `:24`, `:42`).
6. `failure_rate_pct = round((STATE_FAILED + FAILED_DRIVE_ALLOWED + FAILED_NO_ACTION) / vehicles * 100, 2)`
   (`OtaRollout.scala:21`, `:24`, `:43`).
   `IN_PROGRESS` and `WAITING_ON_BCM` count in `vehicles` but in neither rate,
   so the rates do not add up to 100. For example `2.6.1 / MODEL_GRAVITY` has
   77.27 and 4.55 in the fixture.

## Nulls and edge cases

Cases marked *verified* were run through the same Spark functions under
PySpark 3.5.1 locally. That replica of all four jobs reproduces every
committed expected fixture row for row. The fixture itself contains none of
these cases: its 587 OTA events (218 vehicle/version pairs, up to 8 events
each) have no null or unknown states, no null target versions and no
sequence ties (`rust_pipeline/tests/fixtures/input/telemetry/`). **The parity
test will not catch a port that gets these wrong.**

| Case | Spark behaviour |
|---|---|
| Latest event has `update_state = NULL` | The vehicle is counted in no column and not in `vehicles`. Its earlier non-null state is not used (*verified*). |
| Latest event has a state outside the seven | Same: dropped from every count (*verified*). |
| Every vehicle in a (version, model) group is null or unknown | The row is **still emitted** with all seven counts 0, `vehicles = 0`, and both rates **NULL** (*verified*). Division by zero gives NULL in Spark 3.5 with ANSI mode off, the default. |
| `target_software_version` is NULL | It forms its own window partition and its own output row with a NULL key (*verified*). The parity join matches NULL keys through a sentinel (`rust_pipeline/src/validate.rs:232-243`). |
| Two events with the same `sequence_number` for one vehicle and version | `row_number` keeps one of them arbitrarily. Per-VIN sequence numbers are unique by design (`README.md:96-99`). |
| Rate rounding | HALF_UP on the decimal value: `2/3*100` gives 66.67, `round(1.005, 2)` gives 1.01 (*verified*). |

## Hardest Spark behaviour to reproduce

1. **Pivot with a fixed value list.** DataFusion has no `pivot`. Emulate it
   with one conditional count per state over **all** latest rows, for example
   `SUM(CASE WHEN update_state = '…' THEN 1 ELSE 0 END)`. Filtering
   `update_state IN (…)` before grouping would drop the all-zero rows that
   Spark emits. Column names are the raw state strings in this exact set.
   The parity check compares names and types, not column order
   (`rust_pipeline/src/validate.rs:185-203`).
2. **Division.** Spark's `/` always returns double and gives NULL on zero.
   In DataFusion 43.1.0 (Python bindings, checked locally) `bigint / bigint`
   is integer division (`2/3 = 0`), integer division by zero is an error,
   and `double / 0.0` gives `inf` or `NaN`. Cast to double and use
   `NULLIF(vehicles, 0)`.
3. **Rounding.** DataFusion's `round(1.005, 2)` returns 1.0, Spark returns
   1.01 (both checked locally). Use `spark_round`
   (`rust_pipeline/src/transforms/rounding.rs:7`) through a UDF. On `main`
   there is no UDF wrapper yet (open PR #20 adds one).
4. **Types.** The seven counts and `vehicles` must be Int64. DataFusion's
   `SUM` of an integer `CASE` returns Int64. Window `row_number()` returns
   UInt64 in DataFusion, which is fine here because it is only used as a
   filter.

## Notes

- No timestamps, no partitioned output, and no second input.
- Open [PR #6](https://github.com/ajaj3030/vehicle-telemetry-spark-to-rust/pull/6)
  (against `demo-privacy-base`, not `main`) would add a raw `vin` column to
  this table. That conflicts with the privacy contract and with
  `FORBIDDEN_PII_COLUMNS` (`rust_pipeline/src/validate.rs:19`,
  `README.md:107-113`), and its CI is failing. This spec describes `main`.
