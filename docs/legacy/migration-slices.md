# Proposed migration slices

Built on [dependencies.md](dependencies.md) and the specs in [jobs/](jobs/). `…/analytics/` means
`spark_jobs/analytics/src/main/scala/telemetry/analytics/`.

Every port slice uses the same pattern:

1. Replace the job's stub `summarize` (`rust_pipeline/src/analytics/<job>.rs:14-17`).
2. Remove `#[ignore]` from its `matches_spark_output` test (`rust_pipeline/src/analytics/<job>.rs:21-25`). That is the definition of
   "ported" (`README.md:226-227`), and the dashboard checks for it (`dashboard/app.py:461-468`).
3. Pass the parity test in CI (`.github/workflows/ci.yml:36`).
4. Run `rust-pipeline analytics <job>` and `rust-pipeline validate --job <job>`, either from the CLI
   (`rust_pipeline/src/main.rs:77-96`) or with the dashboard's "Run Rust & validate" (`dashboard/app.py:501-510`).

None of these slices changes the Spark jobs, the fixtures, the parity harness, or what any consumer reads.

## Slices

| # | Slice | Files touched | Why the boundary is safe |
|---|-------|---------------|--------------------------|
| 1 | **Port `ota_rollout`** | `rust_pipeline/src/analytics/ota_rollout.rs`. Also the HALF_UP-round helper: either local to that file, or a new `rust_pipeline/src/analytics/spark_functions.rs` plus one `pub mod` line in `rust_pipeline/src/analytics/mod.rs` | One input (`vehicle_telemetry_rust`). No other job reads its output. It writes a new table, `ota_rollout_rust` (`rust_pipeline/src/config.rs:229-231`), while Spark keeps writing `ota_rollout`. The registry, dispatcher and partition test already include it (`rust_pipeline/src/analytics/mod.rs:49-78`, `121-132`). The dashboard reads it by name and column prefix and needs no change (`dashboard/app.py:212-229`, `714-722`). |
| 2 | **Port `charging_sessions`** | `rust_pipeline/src/analytics/charging_sessions.rs`; adds `max_by`/`min_by` and ISO-timestamp helpers to the shared module | One input. Its output has no readers except the dashboard and parity. The new helpers become a dependency for slices 3 and 4, which come later. |
| 3 | **Port `battery_health_daily`** | `rust_pipeline/src/analytics/battery_health_daily.rs`; reuses `max_by` from slice 2 | One input, no downstream jobs. The partitioned write is already handled by `write_output` (`rust_pipeline/src/analytics/mod.rs:98-111`). |
| 4 | **Port `diagnostic_alerts`** | `rust_pipeline/src/analytics/diagnostic_alerts.rs`; reuses the timestamp helper and adds a hex-SHA-256 UDF (wrapping `rust_pipeline/src/transforms/anonymized.rs:20`) | Depends only on the two stream tables. The state stream is already ported, and `validate --state` gates live parity (`rust_pipeline/src/main.rs:106-113`). No other job reads its output. The raw `vin` must not reach the output (`rust_pipeline/src/validate.rs:19`). |
| 5 | **Cutover, one job at a time** | `dashboard/app.py:644-648` (default source); a new schedule for `rust-pipeline analytics <job>` (none exists for either engine, `deploy/k8s/kustomization.yaml:9-13`); stop that job's `spark-submit` runs | Only readers change. Each job has its own table pair, so the switch can be made per job. **Outside this map:** the external service, warranty and OTA systems (`README.md:5-7`) must be repointed too, and the repository does not record which tables they read. A Kubernetes batch job would also need access to the streams' `ReadWriteOnce` volumes (`deploy/k8s/storage.yaml:9,19`). |
| 6 | **Retire Scala** | `…/analytics/*.scala`, `spark_jobs/analytics/build.sbt`, `spark_jobs/analytics/project/`, the CI `scala` job (`.github/workflows/ci.yml:50-65`), the Spark analytics entries in `dashboard/app.py:245-280`, `README.md` | Only after slices 1–5 for all four jobs, because `BuildParityFixtures` couples them (`…/analytics/BuildParityFixtures.scala:19-24`). The fixtures are committed, so the Rust tests keep passing. But expected outputs can no longer be regenerated, so freeze the Spark logic first. |

Slices 1–4 touch disjoint job files and can be developed in parallel. The only ordering comes from the shared helper module:
whichever slice merges first introduces it, and the others rebase onto it.

## Recommended first slice: `ota_rollout`

The criteria are to prove the pattern end to end with the least risk, using the fewest inputs and the fewest Spark-specific
behaviours. The counts below come from the "hardest to reproduce" sections of each spec:

| Job | Inputs | Spark-specific behaviours to reproduce | Partitioned output | New shared helpers needed |
|-----|--------|----------------------------------------|--------------------|---------------------------|
| **`ota_rollout`** | **1** | **4**: fixed-list pivot + `na.fill(0)` as non-null int64; HALF_UP round; double `/` with null on `/0`; `DESC NULLS LAST` ([spec](jobs/ota_rollout.md)) | no | **round only** |
| `charging_sessions` | 1 | 4 groups, 6+ details: `min_by`/`max_by` ×3; `to_timestamp` units and ns-vs-seconds double cast; round at 3 scales, chained; `to_date` of a timestamp ([spec](jobs/charging_sessions.md)) | yes | round, `max_by`/`min_by`, timestamp |
| `battery_health_daily` | 1 | 5: `RANGE` frame over epoch seconds (DataFusion casts to ns); `max_by`; round ×2; int64 widths; partitioned write ([spec](jobs/battery_health_daily.md)) | yes | round, `max_by` |
| `diagnostic_alerts` | **2** | 7: hex `sha2` join on the raw VIN; eager fleet count; integer division; `dense_rank` int32 and null order; timestamps; round; live state parity ([spec](jobs/diagnostic_alerts.md)) | no | round, timestamp, SHA-256 |

`ota_rollout` ties with two jobs on inputs and has the fewest Spark behaviours. It is the only port that needs no timestamp
handling, no missing aggregate function (`max_by`), no window frame other than `row_number`, and no partitioned write. It still
covers the whole pattern: Rust stream table → `summarize` → `write_output` → `ota_rollout_rust` → `validate --job` → the dashboard's
Rust source switch, plus the CI parity test.

The OTA team tie-break was not needed, because `ota_rollout` already leads on the stated criteria. It also matches the OTA team's
request to go first. Ownership is not recorded in the repository (there is no CODEOWNERS file); it comes from the migration request.

### Risks to cover inside slice 1

- **The fixture covers little of the job's edge cases.** `expected/ota_rollout` has 6 rows, and the input has no null or unlisted
  `update_state`, no null `target_software_version`, no `vehicles = 0` group and no null `sequence_number`
  ([spec](jobs/ota_rollout.md#nulls-and-edge-cases)). Add Rust unit tests for each, using the Spark results listed in the spec, or
  the parity test will not catch regressions there.
- **Decision: where the round helper lives.** If it is local to `ota_rollout.rs`, the "only touch your own file" rule
  (`rust_pipeline/src/analytics/mod.rs:9-10`) holds, but slice 2 must extract it. If it goes in a shared `spark_functions.rs`, the
  rule is broken once, but slices 2–4 reuse it from the start.
- **Running against live or demo data needs `vehicle_state_rust` to exist**, even though this job does not use it
  (`rust_pipeline/src/analytics/mod.rs:86-87`). `scripts/load_demo_data.py` creates it.
- **Possible Delta reader incompatibility (not verified on `main`).** `main` pins `deltalake` 0.22, which uses parquet 53
  (`rust_pipeline/Cargo.toml:13`, `rust_pipeline/Cargo.lock`). The demo loader uses an unpinned Python `deltalake`
  (`dashboard/requirements.txt:3`). On another branch, demo tables written by a newer Python `deltalake` failed to read in Rust with
  `Parquet error: External: bad data`, and that was fixed by upgrading to `deltalake` 0.25
  ([PR #13](https://github.com/ajaj3030/vehicle-telemetry-spark-to-rust/pull/13)). If slice 1's end-to-end run hits this, the upgrade
  becomes a shared prerequisite (`Cargo.toml` / `Cargo.lock`). The fixture-based CI test is not affected, because Spark wrote those
  fixtures.
- **Open [PR #6](https://github.com/ajaj3030/vehicle-telemetry-spark-to-rust/pull/6)** (`demo-privacy-gate` → `demo-privacy-base`)
  changes `rust_pipeline/src/analytics/ota_rollout.rs`. It adds a raw `vin` column and a `vehicle_state` lookup. That breaks the
  Spark contract (Spark's output has no `vin`), makes this a two-input job, and emits a column that `validate` forbids
  (`rust_pipeline/src/validate.rs:19`). Slice 1 should port the Spark job as written and not take that change.
