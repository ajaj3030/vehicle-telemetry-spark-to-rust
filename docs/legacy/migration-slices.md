# Migration slices

Built on `dependencies.md`. `SCALA` = `spark_jobs/analytics/src/main/scala/telemetry/analytics`. Each job slice replaces a stub `summarize` and removes the `#[ignore]` on its parity test; the stub shape is `rust_pipeline/src/analytics/ota_rollout.rs:8-22`. A job counts as ported once its file no longer contains `#[ignore` (`dashboard/app.py:461-468`). The shared plumbing (`JOBS`, dispatcher, `run`, `write_output`, parity harness) already exists (`rust_pipeline/src/analytics/mod.rs:28-111`, `rust_pipeline/src/analytics/parity.rs:22-58`), so job slices do not need to touch it.

## Slices

| Slice | Files touched | Why the boundary is safe |
|-------|---------------|--------------------------|
| **0. Shared Spark-semantics helpers** | new shared module under `rust_pipeline/src/analytics/` (or `rust_pipeline/src/transforms/rounding.rs`) exposing `spark_round` as a DataFusion UDF, plus a Spark-compatible `event_time` → `Timestamp(µs, UTC)` parser; `rust_pipeline/src/analytics/mod.rs` (`pub mod`) | No job calls it yet, so no output changes. Wraps the existing tested scalar (`rust_pipeline/src/transforms/rounding.rs:7-39`). Needed by every job's `round` (`dependencies.md`) and by the two `to_timestamp` jobs (`SCALA/ChargingSessions.scala:16-17`, `SCALA/DiagnosticAlerts.scala:26-27`). |
| **1. `ota_rollout`** (recommended first) | `rust_pipeline/src/analytics/ota_rollout.rs` | Reads only telemetry (`SCALA/OtaRollout.scala:46-47`); writes only `ota_rollout_rust`, which only the dashboard OTA view and `validate` read (`dashboard/app.py:714-730`, `rust_pipeline/src/config.rs:230-232`); no other job reads its output (`SCALA/BuildParityFixtures.scala:19-24`). Unpartitioned (`SCALA/AnalyticsJob.scala:77`). |
| **2. `battery_health_daily`** | `rust_pipeline/src/analytics/battery_health_daily.rs` | Reads only telemetry (`SCALA/BatteryHealthDaily.scala:43-44`); writes only `battery_health_daily_rust` (dashboard `dashboard/app.py:662-691`). Shares nothing with other jobs except slice 0. |
| **3. `charging_sessions`** | `rust_pipeline/src/analytics/charging_sessions.rs` | Reads only telemetry (`SCALA/ChargingSessions.scala:40-41`); writes only `charging_sessions_rust` (dashboard `dashboard/app.py:693-712`). Depends on slice 0's timestamp parser. |
| **4. `diagnostic_alerts`** | `rust_pipeline/src/analytics/diagnostic_alerts.rs` | Writes only `diagnostic_alerts_rust` (dashboard `dashboard/app.py:732-748`). The only job with a second input, `vehicle_state` (`SCALA/DiagnosticAlerts.scala:13-14,36-40`), so it should start only after `vehicle_state_rust` passes `validate --state` parity (`rust_pipeline/src/main.rs:50`, `rust_pipeline/src/validate.rs:44-50`). Depends on slice 0's timestamp parser. |
| **5. Retire Scala** | `spark_jobs/analytics/**`, `.github/workflows/ci.yml:50-65`, `dashboard/app.py:178-207,249-285`, `README.md`, and a new way to regenerate `rust_pipeline/tests/fixtures/expected/` | Only safe last: `BuildParityFixtures` needs all four Scala jobs to produce the Rust tests' expected outputs (`SCALA/BuildParityFixtures.scala:19-24`, `rust_pipeline/src/analytics/parity.rs:37-58`), and the dashboard still launches the Scala classes (`dashboard/app.py:275-284`). |

Slices 1-4 are independent of each other (`dependencies.md`), so after slice 0 they can land in any order or in parallel. Slice 0 is optional for slices 1-2 if the job file wraps `rust_pipeline/src/transforms/rounding.rs:7` itself, but then every later job repeats the wrapper.

## Choosing the first slice

| | `ota_rollout` | `battery_health_daily` | `charging_sessions` | `diagnostic_alerts` |
|---|---|---|---|---|
| Input tables | 1 | 1 | 1 | 2 (`SCALA/DiagnosticAlerts.scala:36-40`) |
| Timestamp parsing | no | no | yes (`SCALA/ChargingSessions.scala:16-17`) | yes (`SCALA/DiagnosticAlerts.scala:26-27`) |
| Partitioned write | no | `event_date` | `session_date` | no |
| Spark behaviours to reproduce (detail in `jobs/`) | 4: `row_number` latest-row pick, fixed-list `pivot` + `na.fill(0)`, null on `x / 0`, HALF_UP `round` | 6: `max_by` null semantics, `rangeBetween` over epoch seconds, `lag` across missing days, null-skipping `avg`/`min`/`max`, HALF_UP `round` ×3, date partitioning | 7: `to_timestamp`, timestamp → double seconds, `min_by`/`max_by` ×3, boolean `max` via int cast, conditional `when` → null, `to_date` in UTC, HALF_UP `round` ×3 | 7: `sha2` hex join key, broadcast left join with null keys, null grouping key, `countDistinct`, eager `count()` literal, `dense_rank` as int32, `to_timestamp` |
| Expected fixture rows | 6 | 396 | 540 | 235 |

**Recommendation: slice 1, `ota_rollout`** (after or together with slice 0's `spark_round` UDF).

- It has the fewest inputs (tied with two others) and the fewest Spark-specific behaviours (4 against 6-7; table above).
- It is the only single-input job with neither timestamps nor a partitioned write, so the end-to-end path (`analytics ota_rollout` → `ota_rollout_rust` → `validate --job ota_rollout` → dashboard) is exercised with the smallest surface (`rust_pipeline/src/main.rs:37-55`, `dashboard/app.py:501-521`).
- `battery_health_daily` is the closest alternative (also one input, no timestamps). The OTA team asked to go first (stated in the request; no ownership metadata exists in the repository), which settles the choice towards `ota_rollout`.

**Risks to carry into slice 1:**
- With only 6 expected rows, the fixture does not cover the null-handling paths: it has no OTA rows with null `update_state` or `target_software_version`, and no `sequence_number` ties (checked against `rust_pipeline/tests/fixtures/input/telemetry/`). Parity on the fixture will not prove the `vehicles = 0` → null-rate path described in `jobs/ota_rollout.md`, so the port should reproduce it from the spec rather than from the fixture.
- Open PR #6 (branch `demo-privacy-gate`) adds a raw VIN to `ota_rollout`. That conflicts with the analytics privacy contract (`rust_pipeline/src/validate.rs:19`, `rust_pipeline/src/analytics/mod.rs:42`), so the port should follow `SCALA/OtaRollout.scala` as it is on this branch.
