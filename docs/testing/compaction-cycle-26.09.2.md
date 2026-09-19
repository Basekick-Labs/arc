# Compaction cycle acceptance for 26.09.2

PR #922 adds a configurable cycle budget. The follow-up fixes address recovery
scope, error propagation, partial recovery accounting, and cancellation logs.

## Reproduce the native acceptance run

```sh
go build -tags=duckdb_arrow -o /tmp/arc-cycle-test ./cmd/arc
python3 scripts/compaction_cycle_acceptance.py \
  --arc /tmp/arc-cycle-test --output /tmp/arc-cycle-acceptance
```

The output directory must not already exist. The script starts Arc bound to
loopback, uses isolated storage/configuration, and retains process logs and a
UTC-stamped `results.json`. It needs Python's standard library only.

The fixture uses real Line Protocol ingestion, Parquet files, the HTTP query and
compaction APIs, and the actual compaction subprocess. Fixture filenames are
aged to satisfy the existing hourly ingestion-safety guard without waiting an
hour. The three hourly-tier cycles are manually triggered; they do not represent
three hours of cron execution.

Memory limits remain 512 MB for the main database and each compaction subprocess;
threads remain 2, concurrency 1, and batch size 7. Only the cycle budget changes
between the deliberate 100 ms interruption and the 30 s retry. Process restarts
for changing configuration are planned and recorded separately from unexpected
exits.

The script checks:

- Three completed hourly-tier cycles with zero batch/discovery failures, each
  reducing 14 files to 2 while preserving all 7,000 rows.
- An active batch interrupted by the 100 ms cycle deadline, reported as
  interrupted rather than failed, with remaining batches unstarted.
- A 30 s retry completing all 84,000 rows in the interrupted workload, reducing
  42 original files to 6 outputs without lost or duplicate rows.
- A durable post-upload/pre-deletion recovery fixture assembled from actual
  compacted Parquet and its original raw inputs. An unrelated scoped cycle must
  leave it untouched; the selected recovery must preserve the 3,500 rows and
  remove its seven consumed raw inputs and manifest.
- Exact timestamp, tag, value, and multiplicity equality before/after operations.
- Zero unexpected process exits and no increase in observed local cgroup
  `max`, `oom`, or `oom_kill` events when these counters are available.

This is native development validation. It does not assert production-scale
throughput, remote object-store recovery, Kubernetes restart behavior, or safety
under a particular container memory limit. The post-upload fixture is an
explicit reconstruction of a durable interruption state; the short-deadline
case separately kills a real running compaction subprocess.

## Adversarial checks

| Challenge | Protection and test |
|---|---|
| Recovery fails but cycle reports success | Recovery errors propagate; healthy manifests can still complete and failures remain visible |
| Eligibility lookup fails vs. normal delivery deferral | Error-returning gate separates infrastructure failure from valid deferral |
| Narrow scope modifies unrelated data | Recovery verifies database and measurement metadata before mutation; an undecodable manifest is matched on the database in its path |
| Similar names or namespace paths bypass scope | Tests cover database and measurement prefixes, empty scope, and spoke namespaces |
| Single-tier scheduled cycle leaves another tier's orphan | Recovery spans all tiers; the hourly scheduler's cycle recovers a daily orphan |
| Cancellation discards completed recovery progress | Manager and recovery metrics record successful work before returning cancellation |
| Independent operation timeout looks like a cycle timeout | Cycle status and interruption counts consult the parent context; remaining batches still run |
| Transient manifest read failure exposes files to another compaction | Recovery retains it; cache construction fails closed instead of publishing partial state |
| Undecodable manifest blocks every candidate on the node | Recovery parks it under `.quarantined` (path preserved for the operator); the cache skips it and still honors healthy manifests |
| Cache hit forgets an in-flight output | Input and output keys are retained in both cache paths |
| Cancellation attempts adaptive retries | Known cancellation returns before error classification/splitting; subprocess test asserts no misleading retry/failure log |
| Worker survives cycle exclusion release | Existing worker-wait and overlap tests remain in place; native retry verifies the next cycle can proceed |

## Test commands

```sh
go test -tags=duckdb_arrow -race ./...
go test -tags=duckdb_arrow -race ./internal/compaction \
  -run 'Cycle|RecoveryScope|RecoveryTransient|Unparseable|RecoveryAggregates|RecoveryEmpty|ManifestCacheProtects' -count=20
go vet -tags=duckdb_arrow ./...
git diff --check
```

The recovery error contract intentionally changed: callers now receive
per-manifest errors that were previously only logged. Existing retention and
receipt-marking tests retain their assertions that failed work preserves the
manifest, and additionally assert the propagated error.
