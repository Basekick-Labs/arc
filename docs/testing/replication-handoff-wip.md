# Replication handoff — work in progress

This branch extends PR #1116 to preserve live WAL reader freshness while avoiding
counting those writes again when primary Parquet files arrive. It is a checkpoint,
not a merge-ready implementation. Production startup now enables the new view and
recovery callbacks for licensed local-storage replication without cold tiering.
Shared/cold storage remains unfinished. No final performance claim or release
note is made.

The base combines contributor PR #1116 (`c5b43416`) and main (`ac616f48`). The
initial implementation checkpoint is `3c6a8e39`. Backup branch:
`Basekick-Labs/arc:wip/pr1116-replication-handoff`.

## Invariants

- Identity is the primary WAL's existing instance/sequence pair, not a content
  hash. Two accepted identical writes remain two writes.
- Replication publishes after ingestion admission. A rejected write must not
  appear only on readers.
- Received WAL keeps its original identity and separate provenance; it does not
  become a new originating write during live apply or recovery.
- A manifest advertisement cannot withdraw streamed rows. Only verified,
  available canonical bytes can cover the corresponding replica rows.
- Replica and primary flush boundaries may differ, including across hours.
- Query snapshots hold immutable file versions until their response ends.
- Compaction and partial DELETE preserve full input coverage, including rows
  deliberately removed, so replay cannot resurrect them.
- Intentional deletions require durable retirement evidence before hiding data.
- WAL reclamation requires durable data or a durable retirement decision.

## Implemented foundations

The WAL changes reuse the existing 17-byte tracked envelope. Received entries use
marker `0x04` locally, avoiding a second record or a new originating identity.
Recovery requires a provenance-aware callback and a durable flush barrier. Purge
retains received entries and their checkpoints across process restarts.

Ingestion writes replica Parquet into a separate namespace with per-entry row
segments. Canonical footers hold compressed identity coverage. The view combines
canonical files with uncovered replica segments; SQL ordinality is privately
aliased to preserve colliding user column names. Query requests own snapshot
leases. Verified local hard links can pin immutable file versions independently
of compaction subprocess and retention unlinks.

Raft manifest entries carry partition coverage and replacement paths. Intentional
deletes persist a retirement ledger in Raft snapshots. Compaction preserves
coverage through row deduplication and multi-hour outputs. Partial DELETE now
preserves it in the rewritten footer and records its source in the manifest.

The coordinator authenticates the tracked-entry capability, streams the existing
identity, and refuses incompatible payload modes. The receiver checks the agreed
mode before changing its sequence position. Sender queue publication is serialized
to prevent sequence inversion between concurrent ingestion workers.

The file puller has a publication callback carrying the exact requested version.
The callback must verify and pin local bytes, including already-local files whose
size alone cannot prove identity. Publication failures keep catch-up incomplete.
Own-origin registrations and startup/reconciliation entries also reach that
callback, and queued coverage is deeply copied to preserve its version.
The local production publication service is connected before WAL recovery. Queries
remain unavailable until a leader barrier and coherent manifest/retirement
observation complete. Pulled bytes are pinned and footer metadata is verified
before publication; missing replacements return retryable HTTP 503.

Replay checks use an index limited to current replica materializations. Covered
replicas can be withdrawn without invalidating query leases; local filesystem cleanup
now runs through the coordinator lifecycle and waits for snapshot leases. Obsolete
canonical pins are reclaimed without deleting ordinary canonical storage keys. Durable duplicate retries checkpoint their new
local WAL record, including the race with an in-progress flush.

## Validation and limits

Regression tests exercise real Parquet and DuckDB for unequal flush boundaries,
identical accepted writes, multiple hours, user column name collisions, and
query visibility before and after canonical publication. Other tests cover WAL
recovery/purge barriers, sender ordering, authenticated capability negotiation,
received-WAL-before-apply ordering, file pin lifetime, snapshot retirement,
compaction coverage, DELETE rewrite coverage, replay-index reclamation, and
publication failure/readiness behavior.

The focused handoff tests copy files and invoke publication explicitly. The
licensed local integration benchmark additionally exercises production startup,
real file pulls and live queries. The newer licensed restart results below supersede the initial steady-state-only checkpoint; they still do not establish correctness at every crash boundary. The full race matrix passed for `internal/wal`, `internal/replicaview`,
`internal/ingest`, `internal/cluster/...`, `internal/compaction`, `internal/api`,
and `cmd/arc` with `duckdb_arrow` enabled. After the own-origin publication and
queued-metadata-copy follow-up, the complete file-replication race suite also
passed. These results cover this checkpoint, not the remaining production work.

The earlier 1M-record MessagePack benchmark measured only carrying the existing
identity through the stream. Its readers still counted both materializations.
Those timings are not the completed fix's performance result.

## Remaining work before contributor push or merge

1. Review the integrated PR #1118 recovery barriers at the remaining crash boundaries, including pending originating files and active-buffer replay.
2. Complete legacy provenance migration and unmanifested origin-file recovery.
   Never infer provenance from the node's current role or payload equality.
3. Integrate shared storage, cold-tier sources, failover, and immutable remote
   object lifetimes. Local hard links do not establish those guarantees.
4. Establish safe retirement-ledger reclamation and bound manifest reconciliation
   cost as the number of files grows.
5. Exercise real licensed crash/restart boundaries, delayed/corrupt pulls, offline
   DELETE, compaction, streamed-query lifetime, failover, and mixed versions.
6. Resolve the measured throughput regression and rerun the before/after matrix
   on the completed implementation, including serial and four-client ingestion.
7. Complete the final review, update `RELEASE_NOTES_2027.01.1.md`, verify the
   contributor's latest head, and push the completed fix with contributor credit.

The WIP backup push is explicitly authorized. It is distinct from publishing an
incomplete implementation to the contributor branch or merging it.

## Local integration checkpoint and recovery dependency

The local service recovers pins and replica footers before WAL replay; canonical
publication and Raft reconciliation install one source set. A replacement that
has not arrived blocks its measurement rather than returning deleted rows or
silently hiding surviving rows. Tests cover restart after normal-source unlink,
partial DELETE catch-up, leased canonical pin collection, manifest revision and
copy isolation, legacy provenance refusal, private namespace enumeration, and
retryable query availability errors.

The full `duckdb_arrow` race matrix passed after this integration: WAL, replica
view, ingest, all cluster packages, compaction, API, storage, and cmd/arc. Log:
`/private/tmp/arc-replication-handoff-local-race.log`.

### Measured local performance (unfinished implementation)

Same pinned 1M-record MessagePack fixture, 1,000-record batches, 100k warm-up,
licensed writer/reader, three trials per configuration. Fixture SHA-256:
`a286d26162c5fb1ebe818ac71d004b89246df30f045e36e305542287c3970a7d`.

| Build | Clients | Median records/sec | Reader total/distinct |
| --- | ---: | ---: | --- |
| Baseline ac616f48 | 1 | 1,084,942 | 2M / 1M (duplicates) |
| Local handoff with durable-directory cache | 1 | 777,631 | 1M / 1M |
| Same handoff build | 4 | 881,696 | 1M / 1M |

All six handoff trials acknowledged every write, delivered all 1M rows through
the live stream, matched originating and received WAL identities, and recorded
zero drops/flush failures. Serial median throughput is 28.3% below the fresh
baseline; this regression is unresolved. Runs lasted roughly 1–1.5 seconds on a
shared development host, so these are limited measurements, not production
capacity estimates. Hard-link paths in the harness's file-size totals count the
same inode twice; those totals are not physical disk usage.

The benchmark binary predates the subsequent canonical-pin cleanup and HTTP 503
classification changes. Raw reports and binary hashes:
`/private/tmp/arc-msgpack-1m.Zp5NTf/{before-local-handoff-control,handoff-local-pins-serial,handoff-local-pins-concurrent}/results.json`.
The original baseline harness is unchanged; the handoff harness recognizes the
received marker and requires exact row counts. The abrupt-restart harness was subsequently run; see the integration checkpoint below.

### Do not duplicate PR #1118

At the user's request, open/merged WAL PRs were checked before adding a generic
originating-recovery barrier. PR #1118 (`e855b6d74bfa14f845e7be7ae9ca1abbee26f360`),
<https://github.com/Basekick-Labs/arc/pull/1118>, already implements it for #1009:
startup and maintenance pass `NewRecoveryFlushBarrier` to `BeforeDelete`, with
queued/direct task fencing and flush-failure-generation checks. It also provides
tracked row-range replay and a real two-process-kill regression test.

That work is now integrated in this branch, retaining its commit history. Its CI is green at the inspected head, but its author explicitly
leaves performance attribution and live licensed-cluster validation open. Received recovery now uses the same stronger `BeforeDelete` barrier. Handoff-mode row replay preserves the whole originating identity; existing partial row-range checkpoints are refused with the WAL retained, pending a migration design.


## PR #1118 integration and licensed recovery validation

Integrated contributor head `e855b6d74bfa14f845e7be7ae9ca1abbee26f360`, including
its newer main ancestry, with origin/received provenance kept separate. The
local pin publication now syncs the file inode as well as its directory before
it can justify WAL reclamation. This adds no per-ingestion journal record.

Two recovery defects were reproduced and corrected during integration:

- Startup replay previously flushed originating files before the cluster file
  registrar existed. A recovered writer could query 1M rows while advertising
  zero event files; restoring an empty reader failed. Local handoff startup now
  waits for a reconciled manifest, installs the registrar, replays WAL, and only
  then starts live WAL replication. Repeating the same probe advertised the
  recovered file and restored all 1M rows to the empty reader.
- Retrying an originating entry after one hour had persisted could write that
  hour again. A real Parquet regression returned five rows instead of two.
  Recovery now omits durably covered or retired hours, preserving the original
  identity for the remaining rows. The same regression passes with two rows,
  including retry and subsequent retirement checks.

Compaction crash recovery also needed integration: its newly introduced recovery
path reconstructed completion records without replication coverage/replacement
metadata. A real Job.Run regression forces completion publication failure after
upload, then invokes recovery. It failed with empty coverage before the fix.
The existing pre-upload recovery manifest now carries that metadata, and the
regression passes through source deletion with coverage preserved.

Licensed native cluster probes use the development license from an external
private file, authentication, real Raft, HTTP MessagePack ingestion, live WAL
streaming and local file pulls. Each ingests the same pinned 1M-row fixture:

| Probe | Result |
| --- | --- |
| Settled reader, writer, then both abruptly restarted | Each returns exactly 1M total / 1M distinct IDs |
| Both killed with 1M received rows buffered and no event Parquet files | WAL recovery returns exactly 1M on both; repeated restarts pass |
| Reader offline during partial DELETE, then full DELETE | Both converge to 500k, then zero rows |
| Empty reader data directory after unflushed writer recovery | Restores exactly 1M from the advertised recovered origin file |
| Writer/reader/compactor raw-to-compacted handoff | 20 event files become one; both retain exactly 1M, including reader restart |

The compaction probe invokes the real `arc compact --job-stdin` child because
newly flushed files are intentionally ineligible for hourly scheduling for one
hour. The live completion watcher, Raft transition, output pulls, queries and
reader restart remain exercised; this is not a scheduler-selection test.

External artifacts live under `/private/tmp/arc-msgpack-1m.Zp5NTf/`:
`handoff-1118-recovery-01`, `handoff-1118-offline-delete-01`,
`handoff-1118-unflushed-02`, `handoff-1118-empty-restore-02`, and
`handoff-1118-compaction-02`. Failed setup/reproduction runs are retained too.
Functional probe ingest timings are not the before/after performance comparison.
The unflushed probe uses a deliberately enlarged buffer, and process restarts
reset counters: its cross-restart metric deltas must not be used as throughput
or delivery evidence. Its pre-crash buffer counts, matched WAL identities, lack
of event Parquet files, and post-restart query counts establish the tested case.

### Still not merge-ready

The startup ordering fix does not solve a crash after originating file pinning
and checkpointing but before asynchronous manifest registration. Existing
unannounced pins need durable ownership and a safe registration retry; blindly
registering all recovered pins could resurrect a deleted or foreign file.
Other remaining boundaries include active-buffer replay, shared/cold storage,
legacy migration, failover, mixed versions, network corruption/delay, streamed
query leases, and bounded retirement/reconciliation costs. These successful
local probes do not clear those requirements. The contributor release note
remains credited; a release claim for the completed handoff is intentionally
pending completion of this work.


The final integrated race matrix passed all 13 packages (`wal`, `replicaview`,
`ingest`, `cluster/...`, `compaction`, `api`, `storage`, and `cmd/arc`) with
`duckdb_arrow`, after the startup, origin-hour replay, inode sync, and compaction
recovery fixes. Log: `/private/tmp/arc-handoff-1118-final-race.log`.
