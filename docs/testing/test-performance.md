# Test runtime history

Every normal pytest run records per-case timing through the common root pytest
plugin. The checkout-local Docker unit/E2E scripts and live-stage Web wrapper
persist it in `.e2e-artifacts/test-performance/`. Files are ignored by Git and
are not automatically uploaded or deleted. Collection-only runs write no samples.

Each pytest process uses a separate JSONL file. Retries, repeated commands and
parallel processes cannot overwrite earlier attempts. The run header records
the test checkout revision, candidate revision, dirty state, Python/platform,
runner image and optional provider/model labels. The candidate revision describes
the selected checkout; it is not proof that a running stage service loaded it.
Unknown metadata remains unknown.

Each test measurement includes:

- Exact parameterized test ID, attempt and run/session identity.
- Workflow category, execution kind and local/stage lane.
- Setup, call and teardown durations; their complete-case total and wall elapsed time.
- Passed, failed, skipped, expected failure, unexpected pass, rerun or interrupted outcome.
- Test-source fingerprint and numeric test-specific properties, such as measured
  user-visible response seconds. Arbitrary report payloads and failure text are not copied.

Measurements are appended after each case, so an interrupted batch retains
completed cases. Gracefully interrupted cases are marked incomplete with elapsed time,
without inventing a missing call duration. A hard process kill preserves already
written cases but cannot provide a final duration for the pending case. Partial
JSONL records are counted and skipped without discarding other measurements.
Setup/teardown failures remain
failures. A timing-storage error is reported in the test summary and does not
turn a successful product test into a failure.

## Comparing runs

```sh
python3 scripts/test_performance.py --lane stage
python3 scripts/test_performance.py --test tests/e2e/test_live_stage_code_mode.py::test_live_stage_visual_worker_closes_its_tabs --json
python3 scripts/test_performance.py --kind live_workflow --window 3
python3 scripts/test_performance.py --metric total_seconds --limit 20
```

The default metric is pytest call time, excluding setup/teardown. Total time
includes fixture work and is available only for completed native measurements.
The report compares the latest successful sample window with the immediately
preceding window of the same size, up to three samples each by default. It
shows both medians, percentage change, observed p95 and sample counts. A negative
change means faster. Fewer than two successful samples means no comparison;
one sample never becomes a fabricated baseline.

Failures and incomplete/skipped/retried attempts remain in history and counts.
The latest outcome is shown even if a previous success was faster. Successful
latency statistics are separate from reliability; they do not erase a timeout.

Local, live and replay kinds, lanes, provider/model labels, runner platform,
Python version, image, parallel-worker count and measurement sources are compared separately. Revisions
may differ, since the point is to observe changes over time. A changed test-source
fingerprint is flagged. Record configured provider/model names through
`NULLION_TEST_PROVIDER` and `NULLION_TEST_MODEL` when known; do not mix unknown
labels with an assumption that providers were identical. Relative dates, external
sites, account contents, model load and machine load can still change the work.
Observed timing changes alone do not prove which implementation caused them.

The reports only read local files. They never rerun a test, call a model or
contact stage. The history volume is excluded from candidate copies, so the
growing archive does not become test startup work.

## Existing measured results

Measured legacy live results can be imported without repeating their workflows:

```sh
python3 scripts/test_performance.py --import-live-log path/to/case-results.jsonl
```

Imports retain recorded call durations and outcomes. They never invent setup
time, an unknown revision or a successful outcome for a failed attempt. Imports
are idempotent, and copied or incrementally imported rows are deduplicated by
their measurement identity. Historical inputs may include several revisions;
use `--revision` only for a source known to belong to that revision. Legacy
measurements remain a separate comparison group from native phase measurements.

Retired duplicate test IDs remain readable in historical files. Their canonical
replacements and original categories are saved in the stage catalogue. History
is not rewritten or merged across changed assertions; existing canonical IDs
continue accumulating their own measurements.

The initial archive includes the original stage batch, its continuation and the
completed browser-cleanup replay. This is historical timing evidence, not a
replacement for the saved acceptance status or evidence of a fresh test run.

## Overrides

`NULLION_TEST_PERFORMANCE_DIR` selects a destination for direct pytest runs.
The repository Docker/live wrappers use the persistent history mount. The
optional `NULLION_TEST_REVISION`, `NULLION_TEST_CANDIDATE_REVISION`,
`NULLION_TEST_RUN_ID` and `NULLION_TEST_LANE` metadata are useful for controlled
benchmarks. `NULLION_TEST_PERFORMANCE_DISABLE=1` disables direct-run capture;
normal wrapper runs keep capture enabled. No application/runtime state or
production setting is changed by timing collection.
