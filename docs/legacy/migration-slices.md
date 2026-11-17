# Proposed migration slices

Built on [dependencies.md](dependencies.md). A slice is one PR that can land on
its own without changing any other job's output. Every port slice follows the
pattern the repo already sets up: implement `summarize` in the job's file,
remove `#[ignore]` from its parity test, and pass `cargo test` in CI
(`rust_pipeline/src/analytics/mod.rs:1-10`, `README.md:226-227`,
`.github/workflows/ci.yml:35-36`). Spark keeps running and stays the source of
truth until the cutover slice.

Why any port slice is safe, for all four jobs:

- It writes only `analytics/<job>_rust` (`rust_pipeline/src/config.rs:230-232`),
  never the Spark table that the dashboard and external systems read by default
  (`dashboard/app.py:222-223`).
- No other analytics job reads its output (dependencies.md, "Independent").
- The registry, dispatcher and partition test already include the job
  (`rust_pipeline/src/analytics/mod.rs:49-54`, `:70-78`, `:121-132`), so no
  shared Rust file changes except an optional shared helper module.
- The parity test compares against Spark's committed output with exact
  schema (`rust_pipeline/src/analytics/parity.rs:37-58`,
  `rust_pipeline/src/validate.rs:185-203`), so a wrong port fails CI instead of
  shipping.
- Rollback is reverting one file.

## Comparison

| | `ota_rollout` | `battery_health_daily` | `charging_sessions` | `diagnostic_alerts` |
|---|---|---|---|---|
| Inputs | 1 (telemetry) | 1 | 1 | **2** (telemetry + vehicle_state) |
| Spark behaviours to reproduce (dependencies.md) | **4** | 5 | 5 | 6 |
| Parses timestamps | no | no | yes | yes |
| Partitioned output | no | yes | yes | no |
| Fixture output rows | 6 | 396 | 540 | 235 |
| Edge cases covered by the fixture | weak (no null state, no zero-vehicle group) | strong (null end-of-day, gaps) | strong (single-event, null SoC, midnight) | medium (no-state vehicles, rank ties) |
| Owner recorded in repo | none (no CODEOWNERS; request says OTA team) | none | none | none |

## Slice 1 — `ota_rollout` (recommended first)

**Files:**

- `rust_pipeline/src/analytics/ota_rollout.rs`: `summarize`, un-ignore the
  parity test (`rust_pipeline/src/analytics/ota_rollout.rs:14-25`), and unit
  tests for the cases the fixture lacks (null/unlisted latest `update_state`,
  a `vehicles = 0` group with null rates, a vehicle in two target versions),
  using the Spark results in [jobs/ota_rollout.md](jobs/ota_rollout.md#nulls-and-edge-cases).
- New `rust_pipeline/src/analytics/spark_functions.rs` plus one `pub mod` line
  in `rust_pipeline/src/analytics/mod.rs:12-15`: a DataFusion UDF wrapping
  `spark_round` (`rust_pipeline/src/transforms/rounding.rs:7-28`) and a
  Spark-style division (double, null on zero). All four jobs need the round
  (dependencies.md), so it is shared from the start.

**Boundary:** reads one table (`analytics/OtaRollout.scala:46-47`), no
timestamps, no partitions, output not read by another job. The shared helper
module only adds code; no existing caller changes.

**Before it lands:** close or rescope
[PR #6](https://github.com/ajaj3030/vehicle-telemetry-spark-to-rust/pull/6),
which edits the same file to add a raw `vin` column that both the Spark
contract and `FORBIDDEN_PII_COLUMNS` reject (`rust_pipeline/src/validate.rs:19`).

## Slice 2 — `battery_health_daily`

**Files:** `rust_pipeline/src/analytics/battery_health_daily.rs`; add a
`max_by` equivalent (keep the null at the top) to
`rust_pipeline/src/analytics/spark_functions.rs`.

**Boundary:** one input (`analytics/BatteryHealthDaily.scala:43-44`); the
partitioned write is already handled by `write_output`
(`rust_pipeline/src/analytics/mod.rs:98-111`).

## Slice 3 — `charging_sessions`

**Files:** `rust_pipeline/src/analytics/charging_sessions.rs`; add `min_by`
(reusing slice 2's `max_by` pattern) and string → µs UTC timestamp helpers to
`spark_functions.rs`.

**Boundary:** one input (`analytics/ChargingSessions.scala:40-41`). It reuses
slice 2's helper, so it lands after slice 2, or adds the helper itself if run
in parallel.

## Slice 4 — `diagnostic_alerts`

**Files:** `rust_pipeline/src/analytics/diagnostic_alerts.rs`; reuse slice 3's
timestamp helper; hex SHA-256 via `sha256_hex`
(`rust_pipeline/src/transforms/anonymized.rs:20-24`) or `encode(sha256(vin), 'hex')`.

**Boundary:** the only job with a second input, `vehicle_state_rust`
(`rust_pipeline/src/analytics/mod.rs:86-87`), which already has its own parity
gate (`rust_pipeline/src/main.rs:106-112`). It goes last among ports because it
has the most Spark behaviours and depends on a second upstream table.

## Slice 5 — cutover and Scala retirement (per job, then once)

Only after a port passes `validate --job <job>` on live tables
(`rust_pipeline/src/main.rs:80-96`), not just on fixtures.

**Files, per job:** a schedule for `rust-pipeline analytics <job>` (none
exists: `deploy/k8s/kustomization.yaml:9-13`; new manifest under `deploy/k8s/`),
pointing readers at the Rust output (`rust_pipeline/src/config.rs:225-232`,
`dashboard/app.py:212-225`), and removing the Spark job from the dashboard's
runner (`dashboard/app.py:275-284`).

**Files, once all four are cut over:** `spark_jobs/analytics/` (all Scala,
including `BuildParityFixtures`), `.github/workflows/ci.yml:50-65`,
`dashboard/app.py:244-246`, `README.md`.

**Boundary:** this is the only slice that changes what external systems read
(`README.md:5-7`), and it must be last because `BuildParityFixtures` needs all
four Scala `summarize` functions (`analytics/BuildParityFixtures.scala:19-24`).

## Recommendation

Start with **slice 1, `ota_rollout`**. It has the fewest inputs (1, tied with
battery and charging) and the fewest Spark behaviours to reproduce (4, vs 5–6),
with no timestamps, no date windows and no partitioned write. It also lands the shared HALF_UP
`round` every later slice needs, which proves the full pattern: shared helper,
port, parity test in CI, `<job>_rust` table, dashboard Migration page.

`battery_health_daily` is close behind (5 behaviours, 1 input) but adds
`max_by` null semantics, two kinds of window and a partitioned write. Because
the two are close, the OTA team's request to go first applies as the
tie-breaker, and it points to `ota_rollout` as well. Team ownership is not
recorded in the repo (no CODEOWNERS file), so that preference comes only from
the request.

Main risk: the `ota_rollout` fixture has only 6 rows and none of its null or
zero-vehicle edge cases, so passing the parity test proves less than for the
other jobs. Slice 1 should add the unit tests listed above.
