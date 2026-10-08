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
real file pulls and live queries. Abrupt process restart testing remains pending;
these successful steady-state runs do not establish crash correctness. The full race matrix passed for `internal/wal`, `internal/replicaview`,
`internal/ingest`, `internal/cluster/...`, `internal/compaction`, `internal/api`,
and `cmd/arc` with `duckdb_arrow` enabled. After the own-origin publication and
queued-metadata-copy follow-up, the complete file-replication race suite also
passed. These results cover this checkpoint, not the remaining production work.

The earlier 1M-record MessagePack benchmark measured only carrying the existing
identity through the stream. Its readers still counted both materializations.
Those timings are not the completed fix's performance result.

## Remaining work before contributor push or merge

1. Integrate PR #1118's recovery barriers and tracked row replay, reconciling
   their identity/checkpoint semantics with received-WAL provenance and coverage.
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
received marker and requires exact row counts. A separate abrupt-restart harness
has been prepared but has NOT been run.

### Do not duplicate PR #1118

At the user's request, open/merged WAL PRs were checked before adding a generic
originating-recovery barrier. PR #1118 (`e855b6d74bfa14f845e7be7ae9ca1abbee26f360`),
<https://github.com/Basekick-Labs/arc/pull/1118>, already implements it for #1009:
startup and maintenance pass `NewRecoveryFlushBarrier` to `BeforeDelete`, with
queued/direct task fencing and flush-failure-generation checks. It also provides
tracked row-range replay and a real two-process-kill regression test.

Reuse/integrate that work rather than create an independent originating-WAL fix
in this branch. Its CI is green at the inspected head, but its author explicitly
leaves performance attribution and live licensed-cluster validation open. The
existing received-WAL `ReplicationFlush` must be reconciled with its stronger
barrier; a plain `FlushAll` call is not sufficient proof that all earlier queued
or already-failed asynchronous tasks persisted. Until that integration, no
complete crash-durability claim is justified. Row-range identities also require
compatibility review with this branch's originating-identity coverage model.
