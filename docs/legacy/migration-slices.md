# Migration slices

Proposed order for porting the four Scala analytics jobs, based on
[dependencies.md](dependencies.md) and the per-job specs in [jobs/](jobs/).
Each job slice follows the existing pattern: edit only the job's
`rust_pipeline/src/analytics/<job>.rs`, implement `summarize`, and un-ignore
its parity test. The registry, dispatcher, writer and fixtures already exist
(`rust_pipeline/src/analytics/mod.rs:1-10`, `README.md:63-65`).

## Comparison

"Spark behaviours" counts the Spark-specific semantics listed in each spec's
"Hardest Spark behaviour" section.

| Job | Inputs | Columns read | Spark behaviours to reproduce | Partitioned output | Timestamps | Fixture output rows |
|---|---|---|---|---|---|---|
| `ota_rollout` | 1 | 6 | 4: latest row per key, fixed-list pivot that keeps zero rows, double division with NULL on zero, HALF_UP round | no | no | 6 |
| `battery_health_daily` | 1 | 10 | 4, each harder: `max_by` NULL/tie rules, RANGE window in epoch seconds over gaps, `lag` across gaps, rounding after `avg` | yes (`event_date`) | date only | 396 |
| `charging_sessions` | 1 | 11 | 5: `to_timestamp` parsing to µs UTC, timestamp→double, chained rounding, `min_by`/`max_by`, UTC `to_date` | yes (`session_date`) | yes | 540 |
| `diagnostic_alerts` | 2 | 7 + 2 | 6: second input and eager count, SHA-256 hex, `dense_rank` Int32, integer→double division, `to_timestamp`, NULL keys from a left join | no | yes | 235 |

Sources: [ota_rollout.md](jobs/ota_rollout.md), [battery_health_daily.md](jobs/battery_health_daily.md),
[charging_sessions.md](jobs/charging_sessions.md), [diagnostic_alerts.md](jobs/diagnostic_alerts.md).

## Slices

### Slice 0: shared Spark-semantics helpers (precursor)

- **Files:** `rust_pipeline/src/transforms/rounding.rs` (a DataFusion UDF
  around `spark_round`), a new `rust_pipeline/src/transforms/event_time.rs`,
  and `rust_pipeline/src/transforms/mod.rs`. Open
  [PR #20](https://github.com/ajaj3030/vehicle-telemetry-spark-to-rust/pull/20)
  already does this.
- **Why the boundary is safe:** no job uses the helpers yet and the parity
  tests stay ignored, so no output changes. `spark_round`'s existing callers
  in the telemetry stream keep their behaviour. Every job needs Spark `round`
  ([dependencies.md](dependencies.md#what-can-move-independently-and-what-has-to-move-first)).

### Slice 1: `ota_rollout` (recommended first)

- **Files:** `rust_pipeline/src/analytics/ota_rollout.rs` only. It needs a
  callable Spark `round`, from Slice 0 or registered in this file from
  `rust_pipeline/src/transforms/rounding.rs`.
- **Why the boundary is safe:** it reads only `vehicle_telemetry_rust`,
  which is already validated, and writes only `analytics/ota_rollout_rust`
  (`rust_pipeline/src/config.rs:229-232`). No other job reads its output. The
  registry and dispatcher already include it (`rust_pipeline/src/analytics/mod.rs:49-54`, `:70-78`).
  Spark's `analytics/ota_rollout` is untouched, so the dashboard and
  external readers see no change until someone switches the source.

### Slice 2: `battery_health_daily`

- **Files:** `rust_pipeline/src/analytics/battery_health_daily.rs`.
- **Why the boundary is safe:** one input, its own output table, no readers
  among the other jobs. Partitioned writes by `event_date` are already
  supported and tested (`rust_pipeline/src/analytics/mod.rs:121-161`).

### Slice 3: `charging_sessions`

- **Files:** `rust_pipeline/src/analytics/charging_sessions.rs`. It needs the
  `event_time` helper from Slice 0.
- **Why the boundary is safe:** one input and its own output. Its only
  coupling is the `is_fault` definition in the stream transforms
  (`spark_jobs/transforms.py:133-137`), which this slice reads but does not
  change.

### Slice 4: `diagnostic_alerts`

- **Files:** `rust_pipeline/src/analytics/diagnostic_alerts.rs`. It needs
  the `event_time` helper and SHA-256 hex
  (`rust_pipeline/src/transforms/anonymized.rs:20`).
- **Why the boundary is safe:** it is the only reader of `vehicle_state`, so
  its extra dependency touches no other slice. Run `validate --state` before
  validating this job live (`README.md:60`).

### Slice 5: run Rust analytics in production

- **Files:** new, for example a Kubernetes CronJob in `deploy/k8s/` and
  `deploy/k8s/kustomization.yaml`.
- **Why it is separate:** analytics have no scheduler today, on either side
  (`deploy/k8s/kustomization.yaml:9-13`, `README.md:86-91`). Picking a
  schedule and deciding when readers switch from `<job>` to `<job>_rust` are
  operational decisions, not port work.

### Slice 6: retire the Scala jobs

- **Files:** `spark_jobs/analytics/**`, the analytics entries in
  `dashboard/app.py:178-209` and `:275-284`, `README.md`, and
  `.github/workflows/ci.yml:50-65`.
- **Why it is last:** `BuildParityFixtures` regenerates every job's
  expected output at once (`spark_jobs/analytics/src/main/scala/telemetry/analytics/BuildParityFixtures.scala:19-24`).
  After this slice the fixtures can no longer be regenerated.

## Recommendation: start with `ota_rollout`

It proves the pattern end to end with the least risk: read a Rust table,
port with DataFusion, write a Delta table, pass the parity test in CI, then
run `analytics` and `validate --job` live.

- **Fewest inputs:** 1 table, tied with `battery_health_daily` and
  `charging_sessions`. It reads the fewest columns (6).
- **Fewest Spark-specific behaviours,** and the simplest ones: no
  timestamps, no `max_by`/`min_by`, no range windows, no partitioned
  output, no second input. Its only shared need is Spark `round`, which
  every other job also needs, so building it here unblocks the rest.
- **Smallest output** (6 rows), so parity failures are easy to read.
- **OTA team:** the job's purpose is OTA rollout (`OtaRollout.scala:7`), and
  OTA systems are named downstream readers (`README.md:5-7`). The repository
  has no ownership metadata (no CODEOWNERS), so ownership comes from the
  request, not from the code. The tie-break was not needed: `ota_rollout`
  ranks first on the criteria anyway.

**Risks to handle in Slice 1:**

- The fixture has no NULL or unknown `update_state`, no NULL
  `target_software_version`, and no zero-vehicle groups
  ([ota_rollout.md](jobs/ota_rollout.md#nulls-and-edge-cases)), so the
  parity test cannot catch the pivot mistake described there. Port those
  behaviours verbatim from the spec. Adding such rows would mean
  regenerating the fixtures with `BuildParityFixtures`, which is outside a
  port slice.
- Open [PR #6](https://github.com/ajaj3030/vehicle-telemetry-spark-to-rust/pull/6)
  would add a raw `vin` to this table, against the privacy gate
  (`rust_pipeline/src/validate.rs:19`). Resolve it before the port, or the
  Spark contract the port targets may change.
