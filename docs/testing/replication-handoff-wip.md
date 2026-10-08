# Replication handoff — work in progress

This branch extends PR #1116 to preserve live WAL reader freshness while avoiding
counting those writes again when primary Parquet files arrive. It is a checkpoint,
not a merge-ready implementation. Production startup does not yet enable the new
view or recovery callbacks. No final performance claim or release note is made.

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
The production publication service still needs to be connected.

Replay checks use an index limited to current replica materializations. Covered
replicas can be withdrawn without invalidating query leases; filesystem cleanup
still needs its service lifecycle. Durable duplicate retries checkpoint their new
local WAL record, including the race with an in-progress flush.

## Validation and limits

Regression tests exercise real Parquet and DuckDB for unequal flush boundaries,
identical accepted writes, multiple hours, user column name collisions, and
query visibility before and after canonical publication. Other tests cover WAL
recovery/purge barriers, sender ordering, authenticated capability negotiation,
received-WAL-before-apply ordering, file pin lifetime, snapshot retirement,
compaction coverage, DELETE rewrite coverage, replay-index reclamation, and
publication failure/readiness behavior.

The handoff tests currently copy files and invoke publication explicitly. They do
not establish that production startup, real file pulls, crash recovery, and live
queries work together. The full race matrix passed for `internal/wal`, `internal/replicaview`,
`internal/ingest`, `internal/cluster/...`, `internal/compaction`, `internal/api`,
and `cmd/arc` with `duckdb_arrow` enabled. After the own-origin publication and
queued-metadata-copy follow-up, the complete file-replication race suite also
passed. These results cover this checkpoint, not the remaining production work.

The earlier 1M-record MessagePack benchmark measured only carrying the existing
identity through the stream. Its readers still counted both materializations.
Those timings are not the completed fix's performance result.

## Remaining work before contributor push or merge

1. Initialize the view/service before received-WAL recovery; hydrate durable
   replica files, canonical manifest state, pins, and retirement decisions.
   Configure startup and periodic recovery, ingestion, queries, and coordinator.
2. Connect file publication to that service, including own-origin compaction
   output and files already present during startup/reconciliation. Keep Raft
   callbacks nonblocking and outside coordinator-lock acquisition.
3. Reconcile replacement transitions atomically and retain missing predecessors
   until replacement bytes are available. Partial DELETE catch-up must never
   silently hide surviving rows. Offline readers and snapshot restore must
   apply durable retirements before queries can expose stale materializations.
4. Connect covered-shadow/pin cleanup to query leases and durable coverage.
   Establish a safe retirement-ledger reclamation policy; passage of time alone
   does not prove that an offline replica cannot replay an old entry.
5. Exclude the private replica/pin trees from ordinary recursive storage,
   maintenance, backup, and compaction enumeration, with explicit internal
   enumeration for recovery.
6. Define and test legacy provenance migration and unmanifested origin-file
   recovery. Do not infer provenance from the node's current role or content
   equality, and do not discard an accepted write's only copy.
7. Integrate shared storage, cold-tier sources, failover, and immutable remote
   object lifetimes. Local hard links do not establish those guarantees.
8. Exercise a real licensed writer/reader through MessagePack HTTP, unequal
   flushes, delayed/corrupt pulls, crash/restart at handoff boundaries, offline
   DELETE, compaction, streamed-query lifetime, failover, mixed versions, and
   row/columnar formats.
9. Rerun the exact 1M-record fixture before/after, with serial and four-client
   ingestion. Require all HTTP acknowledgements, complete live stream delivery,
   no drops or sequence errors, and exactly 1M query rows with 1M distinct IDs.
10. Complete the final review, update `RELEASE_NOTES_2027.01.1.md`, verify the
    contributor's latest head, and push the completed fix with contributor credit.

The WIP backup push is explicitly authorized. It is distinct from publishing an
incomplete implementation to the contributor branch or merging it.
