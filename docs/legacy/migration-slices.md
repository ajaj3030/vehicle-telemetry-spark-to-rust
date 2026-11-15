# Migration slices

Each slice ports one job behind the existing parity gate. Boundaries come from `dependencies.md`: no job reads another job's output, Rust writes to separate `<job>_rust` tables (`rust_pipeline/src/config.rs:225-232`), and the Spark job keeps writing its own table until cutover, so a slice cannot change what any existing consumer reads.

Definition of done for a slice: its parity test runs without `#[ignore]` and passes in CI (`README.md:115-145`, `.github/workflows/ci.yml:14-36`).

## Comparison

| Slice | Inputs | Spark behaviours to reproduce (from `jobs/*.md`) | Partition | Count |
|---|---|---|---|---|
| **1. ota_rollout** | 1 | fixed-list pivot + zero fill, `row_number` latest-row, HALF_UP round, long/long divide (÷0 → null) | none | 4 |
| 2. charging_sessions | 1 | `to_timestamp` + `timestamp[us, UTC]` type, timestamp→double casts, UTC `to_date`, `min_by`/`max_by` ×3 (null-preserving), HALF_UP round ×4, `when` without `otherwise`, bool max | `session_date` | 7 |
| 3. battery_health_daily | 1 | `rangeBetween` on date→epoch seconds, `lag` (row-based), `max_by`, avg + HALF_UP round ×3, bool sum | `event_date` | 5 + partition |
| 4. diagnostic_alerts | 2 | `sha2` join key, left join with null key groups, eager `count()` denominator, `countDistinct`, `dense_rank` as int32, `to_timestamp` ×2, HALF_UP round | none | 7 |

Counts are this document's grouping of the behaviours cited in each spec (`jobs/ota_rollout.md`, `jobs/charging_sessions.md`, `jobs/battery_health_daily.md`, `jobs/diagnostic_alerts.md`).

## Slice 1 — ota_rollout (recommended first)

Files touched:
- `rust_pipeline/src/analytics/ota_rollout.rs` — implement `summarize` (replace the `bail!`, `:14-17`) and remove `#[ignore]` (`:22`).
- New shared helper module under `rust_pipeline/src/analytics/` (or `rust_pipeline/src/transforms/rounding.rs`) exposing Spark HALF_UP `round` as a DataFusion UDF, plus its `mod` line in `rust_pipeline/src/analytics/mod.rs` — see "Move first" in `dependencies.md`.

Read but not changed: `rust_pipeline/tests/fixtures/input/telemetry`, `rust_pipeline/tests/fixtures/expected/ota_rollout`, `spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala`.

Why the boundary is safe: reads only `vehicle_telemetry_rust` (`rust_pipeline/src/analytics/mod.rs:84-87`); writes only `analytics/ota_rollout_rust`, which no job reads; its registry/dispatch entries already exist (`rust_pipeline/src/analytics/mod.rs:49-78`); the Scala job and its consumers (`dashboard/app.py:714`) are untouched. The dashboard picks up the port automatically (`dashboard/app.py:464-468`).

Why first: tied for fewest inputs (1) and has the fewest Spark-specific behaviours (4), with no timestamps, no partition column and no `max_by`/window frames (`jobs/ota_rollout.md`). It still exercises the whole pattern: DataFusion query over the Rust telemetry table, exact names and `int64`/`double` types, HALF_UP rounding, Delta write via `write_output` (`rust_pipeline/src/analytics/mod.rs:98-111`), parity in CI and the dashboard status flip. It also lands the shared rounding helper every later slice needs. The OTA tie-breaker points the same way: the user reports the OTA team owns this job and asked to go first (the repo itself has no ownership metadata; `README.md:5-7` only names OTA systems as consumers).

Risks: the fixture never hits null/unknown `update_state` or `vehicles = 0` (`jobs/ota_rollout.md`), so add unit tests for those paths; resolve open PR #6, which edits the same file to add a raw `vin` column (`rust_pipeline/src/validate.rs:19` forbids it).

## Slice 2 — charging_sessions

Files: `rust_pipeline/src/analytics/charging_sessions.rs`; extend the shared helper with Spark `to_timestamp` parsing and null-preserving `min_by`/`max_by` if not done inline.
Safe: single input (`spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala:40-41`), writes only `charging_sessions_rust`, partition handling already generic (`rust_pipeline/src/analytics/mod.rs:98-111`). Can run in parallel with slice 3 once slice 1's helper has landed.

## Slice 3 — battery_health_daily

Files: `rust_pipeline/src/analytics/battery_health_daily.rs` (reuses `max_by` from slice 2 if available).
Safe: single input (`spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala:43-44`), writes only `battery_health_daily_rust`.

## Slice 4 — diagnostic_alerts

Files: `rust_pipeline/src/analytics/diagnostic_alerts.rs`.
Safe: the only slice with a second input, `vehicle_state_rust` (`spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala:36-40`); still writes only `diagnostic_alerts_rust`. Do it last so the shared helpers are proven and the `vehicle_state_rust` → `sha2` path is the only new risk.

## Slice 5 — cutover and Scala retirement (after 1–4)

Files: `dashboard/app.py` (point Fleet at `<job>_rust`, `:178-207,651`), `scripts/load_demo_data.py:35-40`, `spark_jobs/analytics/src/main/scala/telemetry/analytics/*.scala` incl. `AnalyticsJob.scala` and `BuildParityFixtures.scala`, `spark_jobs/analytics/build.sbt`, `.github/workflows/ci.yml:50-65`, `README.md:55-92`.
Safe only once all four parity tests pass: `AnalyticsJob.scala` is shared by every job and `BuildParityFixtures.scala:19-24` needs every Scala `summarize` to regenerate fixtures. External consumers (`README.md:5-7`) must be moved off the Spark tables first.
