# Migration slices

Each slice ports one Scala analytics job to Rust, proven by its parity test against the committed Spark
fixtures (`rust_pipeline/src/analytics/parity.rs:37-45`) and then by `rust-pipeline validate` against live
tables (`README.md:138-147`). Dependencies: [dependencies.md](dependencies.md).

## Definition of done (all slices)

- `summarize` implemented and the parity test's `#[ignore]` removed, which is also what the dashboard reads as
  "ported" (`README.md:226-227`, `dashboard/app.py:461-468`).
- Exact output names and Arrow types (`rust_pipeline/src/validate.rs:185-212`), output written to `<job>_rust`
  with Spark's partition columns (`rust_pipeline/src/analytics/mod.rs:97-110`, `rust_pipeline/src/config.rs:230-232`).
- CI green: `cargo fmt`, `clippy`, `cargo test` (`.github/workflows/ci.yml:35-36`).
- Scala job left in place until the decommission slice (S5).

## Slices

### S1 — `ota_rollout`

- **Touches:** `rust_pipeline/src/analytics/ota_rollout.rs` only (implementation, un-ignore parity test, unit
  tests for the edge cases the fixture lacks — see [jobs/ota_rollout.md](jobs/ota_rollout.md)). Reads, does not
  change, `rust_pipeline/src/transforms/rounding.rs`, `rust_pipeline/tests/fixtures/expected/ota_rollout`.
- **Why the boundary is safe:** single input (telemetry OTA rows, 5 columns,
  `spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala:26-43`); no other job reads its output
  (`dependencies.md`); registry and dispatch already exist (`rust_pipeline/src/analytics/mod.rs:9-10`); writes a
  separate `ota_rollout_rust` table, so the Spark table and its consumers are unaffected
  (`rust_pipeline/src/config.rs:230-232`); unpartitioned output (`rust_pipeline/src/analytics/ota_rollout.rs:8-12`).

### S2 — `charging_sessions`

- **Touches:** `rust_pipeline/src/analytics/charging_sessions.rs`; if `min_by`/`max_by` and Spark timestamp parsing
  are written as shared helpers, a new `rust_pipeline/src/analytics/spark_functions.rs` plus one `mod` line in
  `rust_pipeline/src/analytics/mod.rs`.
- **Why safe:** single input (`spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala:13`);
  output read only by the dashboard and external consumers; separate `_rust` table. The helper module is additive.

### S3 — `battery_health_daily`

- **Touches:** `rust_pipeline/src/analytics/battery_health_daily.rs`; reuses the S2 `max_by` helper if present
  (otherwise adds it, as in S2).
- **Why safe:** single input (`spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala:16`);
  windows are per `vin_hash` within this job only; separate `_rust` table.

### S4 — `diagnostic_alerts`

- **Touches:** `rust_pipeline/src/analytics/diagnostic_alerts.rs` (SHA-256 UDF over `vehicle_state.vin`, likely
  reusing `rust_pipeline/src/transforms/anonymized.rs`).
- **Why safe:** both inputs are already registered for every job (`rust_pipeline/src/analytics/mod.rs:82-87`) and
  both upstream Rust streams are ported (`README.md:55-61`); separate `_rust` table. Widest boundary: parity also
  depends on `vehicle_state_rust` matching Spark (`README.md:58-60`), and it is the only job handling raw VINs
  (`spark_jobs/vehicle_state_job.py:58-73`).

### S5 — Decommission Scala analytics (after S1–S4)

- **Touches:** `spark_jobs/analytics/` (all jobs, `AnalyticsJob.scala`, `BuildParityFixtures.scala`, `build.sbt`),
  `.github/workflows/ci.yml:55-65` (Scala job), `dashboard/app.py:178-205` and `:995` (Spark class/jar launch),
  `README.md:55-93`, consumer path switch to `<job>_rust` or a rename, and a new Rust analytics schedule if one is
  wanted (none exists, `deploy/k8s/kustomization.yaml:9-13`).
- **Why last:** fixture regeneration runs all four Scala jobs (`BuildParityFixtures.scala:19-61`), so it is needed
  until no Scala job remains. A fixture-regeneration replacement must be decided before deleting it.

## Comparison

"Spark behaviours" counts the items in each spec's "Hardest Spark behaviour" list that have no direct DataFusion
equivalent.

| | S1 `ota_rollout` | S2 `charging_sessions` | S3 `battery_health_daily` | S4 `diagnostic_alerts` |
|---|---|---|---|---|
| Input tables | 1 | 1 | 1 | 2 |
| Input columns | 5 | 10 | 9 | 6 + 2 state |
| Spark behaviours to reproduce | row_number DESC null order, fixed pivot keeping non-matching groups, null-on-zero divide, HALF_UP round | `to_timestamp`, `min_by`/`max_by` ×3, timestamp→double, nested rounding, bool max, µs UTC type | `max_by`, epoch-seconds RANGE window, `lag` across gaps, HALF_UP round | SHA-256 UDF, left join with misses, `countDistinct`, `dense_rank` Int32 cast, timestamps |
| Timestamp output | no | yes | no | yes |
| Partitioned output | no | yes | yes | no |
| New shared helpers needed | none | `min_by`/`max_by`, timestamps | `max_by` | SHA-256 UDF |
| Fixture rows (expected) | 6 | 540 | 396 | 235 |
| Owner (per request) | OTA team | — | — | — |

Sources: [jobs/ota_rollout.md](jobs/ota_rollout.md), [jobs/charging_sessions.md](jobs/charging_sessions.md),
[jobs/battery_health_daily.md](jobs/battery_health_daily.md), [jobs/diagnostic_alerts.md](jobs/diagnostic_alerts.md);
counts measured on `rust_pipeline/tests/fixtures/expected/`. The repo has no ownership metadata (no CODEOWNERS);
OTA ownership of `ota_rollout` is from the migration request, consistent with OTA systems consuming the tables
(`README.md:6`).

## Recommendation: start with S1 `ota_rollout`

- **Fewest inputs:** one table, five columns — tied with S2/S3 on tables, lowest on columns.
- **Fewest Spark-specific behaviours:** no timestamps, no `min_by`/`max_by`, no range window, no join, no
  partitioned output; every behaviour it needs is expressible in plain DataFusion SQL plus the existing
  `spark_round` (`rust_pipeline/src/transforms/rounding.rs`).
- **Touches one file** (`rust_pipeline/src/analytics/ota_rollout.rs`), so it proves the pattern end to end — port,
  parity test, `<job>_rust` output, dashboard status, `validate` on live tables — with no shared-code change.
- **OTA team asked to go first**, which agrees with the risk ranking rather than overriding it.

Main risk and mitigation: the fixture has only 6 expected rows and none of the pivot/zero-division/null-key edge
cases ([jobs/ota_rollout.md](jobs/ota_rollout.md)), so the parity test alone under-tests it. The slice should add
Rust unit tests for out-of-list `update_state`, `vehicles = 0`, null `target_software_version` and null
`sequence_number`, with expected values taken from Spark's behaviour documented in the spec.

Suggested order after S1: S3 or S2 (S2 first if the shared `min_by`/`max_by` helper is built there for S3 to reuse),
then S4, then S5.
