# Proposed migration slices

Based on [dependencies.md](dependencies.md) and the per-job specs in [jobs/](jobs/). Paths are relative to
the repo root. A slice is "done" when its parity test is un-`#[ignore]`d and passes in CI
(`.github/workflows/ci.yml:31-36`), which is also how the dashboard counts it as ported (`dashboard/app.py:462-468`).

## Slices

| Slice | Files it touches | Why the boundary is safe |
|---|---|---|
| **1. ota_rollout** (+ shared helpers) | `rust_pipeline/src/analytics/ota_rollout.rs`; new `rust_pipeline/src/analytics/spark_functions.rs` (Spark-compatible `round` UDF wrapping `rust_pipeline/src/transforms/rounding.rs`, null-safe divide); one `pub mod` line in `rust_pipeline/src/analytics/mod.rs` | Reads one table, one partition (`OTA`); writes only `ota_rollout_rust`; no job reads `ota_rollout`; registry/dispatcher/dashboard already wired (`rust_pipeline/src/analytics/mod.rs:49-54,70-78`, `dashboard/app.py:178-206`); Scala side untouched, so fixtures can still be regenerated |
| **2. battery_health_daily** | `rust_pipeline/src/analytics/battery_health_daily.rs`; adds `max_by` to `spark_functions.rs` | One input (BATTERY partition); own output `battery_health_daily_rust`; partitioned writer already exists and is tested (`rust_pipeline/src/analytics/mod.rs:97-111,134-161`) |
| **3. charging_sessions** | `rust_pipeline/src/analytics/charging_sessions.rs`; adds `min_by`, Spark `to_timestamp` → `timestamp[us, UTC]` to `spark_functions.rs` | One input (CHARGING partition); own output; no readers among jobs |
| **4. diagnostic_alerts** | `rust_pipeline/src/analytics/diagnostic_alerts.rs`; adds a `sha2` UDF over `rust_pipeline/src/transforms/anonymized.rs:20-24` and an int32 `dense_rank` cast to `spark_functions.rs` | Own output; but two inputs — needs `vehicle_state_rust` parity (already gated by `validate --state`) and handles raw VINs, so review privacy |
| **5. Rust batch runtime** | new `deploy/k8s/analytics-<job>-cronjob.yaml`, `deploy/k8s/kustomization.yaml`, `deploy/k8s/storage.yaml` (shared storage), `deploy/k8s/app_config.yaml` | Additive: new resources only; streams' Deployments unchanged (`deploy/k8s/telemetry-stream.yaml`, `deploy/k8s/vehicle-state-stream.yaml`). Required before any production cutover (`deploy/k8s/kustomization.yaml:9-13`, `deploy/k8s/storage.yaml:1-22`) |
| **6. Cutover + Scala retirement** (per job, then final) | per job: point readers at `<job>_rust` (or have Rust write `<job>`), `dashboard/app.py` defaults; final: delete `spark_jobs/analytics/`, CI `scala` job (`.github/workflows/ci.yml:50-65`), Spark analytics runners (`dashboard/app.py:244-246,275-284`), `BuildParityFixtures.scala` | Only after slices 1–5; `BuildParityFixtures` couples all Scala jobs (`spark_jobs/analytics/src/main/scala/telemetry/analytics/BuildParityFixtures.scala:19-24`) |

Slices 2–4 are independent of each other once slice 1 has landed the shared helper module; they only
append functions to `spark_functions.rs`, so run them in parallel with care for that one file.

## Comparison for choosing the first slice

| | ota_rollout | battery_health_daily | charging_sessions | diagnostic_alerts |
|---|---|---|---|---|
| Input tables | **1** (telemetry) | 1 | 1 | 2 (telemetry + vehicle_state) |
| Event types / rows in fixture | OTA / 587 | BATTERY / 2,867 | CHARGING / 1,412 | DIAGNOSTIC / 897 + 147 state |
| Output partitioned | **no** | yes (`event_date`) | yes (`session_date`) | no |
| Spark behaviours to reproduce (see each spec) | **4**: fixed-value `pivot`, null on ÷0, HALF_UP `round`, `row_number` DESC NULLS LAST | 5: `max_by`, calendar `rangeBetween`, `lag` over reporting days, HALF_UP `round`, partitioned write | 6: `to_timestamp`→µs UTC, `min_by`/`max_by`, ts→double, round-before-reuse, bool max/cast, `to_date` + partitioned write | 6: in-query `sha2`, int32 `dense_rank`, `countDistinct` with null key, eager fleet count, `to_timestamp`, HALF_UP `round` |
| Raw-VIN exposure | no | no | no | yes (`spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala:12-13`) |
| Owner | OTA team (per your request; no CODEOWNERS or owner mapping exists in the repo) | warranty/service (`spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala:7`) | not stated | service triage (`spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala:7`) |

## Recommendation: slice 1, ota_rollout

- **Fewest inputs:** one table, one event-type partition (`spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala:31-32`);
  tied with battery_health_daily and charging_sessions, ahead of diagnostic_alerts.
- **Fewest Spark-specific behaviours:** no `max_by`/`min_by`, no range windows, no timestamp parsing, no
  hashing, no partitioned output — the four it has are all expressible in plain DataFusion SQL plus the
  existing `spark_round` (`rust_pipeline/src/transforms/rounding.rs:7-28`). battery_health_daily is the
  closest alternative (also one input) but needs `max_by` and a calendar `RANGE` frame.
- **Tie-break:** the two closest candidates are ota_rollout and battery_health_daily; ota_rollout is the OTA
  team's job, and they have asked to go first.
- **Proves the pattern end to end** with existing tooling: port → parity test on fixtures
  (`rust_pipeline/src/analytics/parity.rs:37-57`) → `rust-pipeline analytics ota_rollout` writing
  `ota_rollout_rust` (`rust_pipeline/src/analytics/mod.rs:82-95`) → `validate --job ota_rollout`
  (`rust_pipeline/src/main.rs:80-96`) → dashboard Migration page and Fleet "Rust" toggle
  (`dashboard/app.py:501-520`, `dashboard/app.py:644-656`). It also lands the shared helper module that
  slices 2–4 reuse, which is the main ordering constraint in [dependencies.md](dependencies.md).

**Risks to manage in slice 1:**

1. The expected fixture has only 6 rows and none of the null/unlisted-state/zero-vehicle cases
   ([jobs/ota_rollout.md](jobs/ota_rollout.md#nulls-and-edge-cases)), so parity passing proves less here than for
   the other jobs. Add unit tests in `ota_rollout.rs` for: an unlisted or null `update_state` (group still
   emitted with zeros) and `vehicles = 0` (null rates). Optionally extend the shared fixture input while Spark
   still exists — but that regenerates all four expected outputs (`README.md:126-136`).
2. Column order and int64 types of the seven pivot columns are part of the contract
   (`rust_pipeline/src/validate.rs:183-203`).
3. "End to end" here stops at parity and the CLI; production scheduling needs slice 5.
