# ota_rollout — functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala` (all line refs below are to this file unless stated).
Rust placeholder: `rust_pipeline/src/analytics/ota_rollout.rs`.
Expected output fixture: `rust_pipeline/tests/fixtures/expected/ota_rollout/`.

## Purpose

OTA rollout status per target software version and model (line 7): for every vehicle that has
reported on an update to a given target version, take its **latest** reported update state, then count
vehicles per state and derive success and failure rates. Shown on the dashboard's OTA Rollout tab
(`dashboard/app.py:714-730`), which also recomputes fleet-wide weighted rates from
`success_rate_pct * vehicles` (`dashboard/app.py:719-720`).

## Inputs

| Table | Rows used | Columns used |
|---|---|---|
| telemetry (Spark: `spark.delta_output_path`; Rust: `vehicle_telemetry_rust`) | `event_type = 'OTA'` (line 32) | `vin_hash`, `target_software_version`, `sequence_number`, `model_safe`, `update_state` |

Only one input table, one event type. The telemetry table is partitioned by `event_type`
(`spark_jobs/streaming_job.py:245`), so the filter prunes to the OTA partition.
Upstream guarantees: `model_safe` is never null (`coalesce(model, 'MODEL_UNKNOWN')`,
`spark_jobs/transforms.py:73`); `sequence_number` is non-null and per-VIN monotonic
(`spark_jobs/streaming_job.py:88`, `data_generator/generator.py:143-154`).

## Output

Not partitioned; key (`target_software_version`, `model_safe`) (`rust_pipeline/src/analytics/ota_rollout.rs:8-12`).
Column order and types must match exactly (`rust_pipeline/src/validate.rs:183-203`):

| # | Column | Type | Meaning |
|---|---|---|---|
| 1 | target_software_version | string | group key |
| 2 | model_safe | string | group key |
| 3–9 | UPDATE_STATE_IN_PROGRESS, UPDATE_STATE_WAITING_ON_BCM, UPDATE_STATE_SUCCESS, UPDATE_SUCCESS_WITH_WARNINGS, UPDATE_STATE_FAILED, UPDATE_FAILED_DRIVE_ALLOWED, UPDATE_FAILED_NO_ACTION | int64 | vehicles whose latest state is that value; in the order of `UpdateStates` (lines 11-19) |
| 10 | vehicles | int64 | sum of the seven state columns |
| 11 | success_rate_pct | double | `round((SUCCESS + SUCCESS_WITH_WARNINGS) / vehicles * 100, 2)` |
| 12 | failure_rate_pct | double | `round((FAILED + FAILED_DRIVE_ALLOWED + FAILED_NO_ACTION) / vehicles * 100, 2)` |

## Calculation

1. Filter telemetry to `event_type = 'OTA'` (line 32).
2. Window `PARTITION BY vin_hash, target_software_version ORDER BY sequence_number DESC` (lines 27-29);
   `row_number()` over it, keep `rn = 1` (lines 33-34) → one row per (vehicle, target version): its newest
   OTA event. Note `rn` stays as a column but is dropped by the pivot's groupBy.
3. `groupBy(target_software_version, model_safe).pivot(update_state, UpdateStates).count()` (lines 37-39):
   one count column per listed state, in list order.
4. `na.fill(0L, UpdateStates)` (line 40): absent combinations become 0, not null.
5. `vehicles = 0 + IN_PROGRESS + … + FAILED_NO_ACTION` (`total`, lines 23, 41).
6. `success_rate_pct`, `failure_rate_pct` = `round(numerator / vehicles * 100, 2)` (`pct`, lines 24, 42-43);
   Succeeded/Failed lists at lines 20-21. `UPDATE_STATE_IN_PROGRESS` and `UPDATE_STATE_WAITING_ON_BCM`
   count toward `vehicles` but toward neither rate.

On the fixtures this gives 6 rows (3 target versions × 2 models) totalling 218 vehicles, equal to the
218 distinct (vin_hash, target_software_version) pairs in the OTA input.

## Nulls and edge cases

Spark behaviour below was confirmed with a local PySpark 3.5.1 probe (`spark.sql.ansi.enabled=false`,
the default the job runs with — `spark_jobs/analytics/src/main/scala/telemetry/analytics/AnalyticsJob.scala:55-68` does not set it).

| Case | Spark result | In fixtures? |
|---|---|---|
| `update_state` null or not in `UpdateStates` | row is counted in no column, **but its (version, model) group is still emitted** with all seven counts 0 | no — every OTA row has one of the 7 states (generator only emits those, `data_generator/schemas.py:89-97`) |
| a group whose latest states are all unlisted → `vehicles = 0` | `x / 0` → **null** rate (non-ANSI division), not an error or NaN | no |
| `target_software_version` null | null is a valid window-partition value and a valid group key; emitted as its own row | no (0 OTA rows with null target) |
| a vehicle's latest event per target has the max `sequence_number` tie | `row_number` picks one arbitrarily (non-deterministic) | no — no (vin_hash, sequence_number) duplicates in the fixture |
| `sequence_number` null | sorts **last** under Spark's `.desc` (NULLS LAST) | impossible upstream (non-null in the parsed schema) |
| vehicle's model changes between versions | each latest row carries its own `model_safe`; grouping uses that | not exercised |
| rounding | `round` is HALF_UP on the decimal value: `round(2.675, 2) = 2.68` | yes (values like 77.27, 4.55) |

## Spark behaviour hardest to reproduce in Rust

1. **`pivot` with a fixed value list** (line 38): the output columns are *data values* in a fixed order,
   non-listed values are dropped, and their groups still appear with zeros. DataFusion has no `PIVOT`; the
   port needs one `SUM(CASE WHEN update_state = '…' THEN 1 ELSE 0 END)` per state (which naturally keeps
   the all-zero groups and yields int64). Column order must follow `UpdateStates` exactly because the schema
   check compares names and types (`rust_pipeline/src/validate.rs:183-203`) and the dashboard selects
   columns by the `UPDATE_` prefix (`dashboard/app.py:715`).
2. **Division by zero → null** (lines 24, 42-43) under non-ANSI Spark; Rust must return null, not NaN/inf.
3. **`round(…, 2)` HALF_UP on the shortest decimal form**; a binary `(x*100).round()/100` differs on values
   like 1.005. `rust_pipeline/src/transforms/rounding.rs:1-28` (`spark_round`) already implements Spark's rule.
4. **`row_number` ordering with `.desc` = NULLS LAST** (line 29). DataFusion's default for `DESC` is
   NULLS FIRST, so the sort must say `NULLS LAST` explicitly (harmless today since the column is non-null).

Fixture caveat: the expected output has only 6 rows and none of the null / unlisted / zero-vehicle cases
above, so the parity test alone cannot prove them; a port should add unit tests for cases 1–2.
