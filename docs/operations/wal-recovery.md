# WAL recovery monitoring

Arc exports `arc_wal_dir_bytes`, the current size of `*.wal` files and
quarantined `*.failed` WAL files. The gauge is sampled every 15 seconds while
WAL maintenance is running. Quarantined files remain on disk for operator
inspection and are included in the size.

Two useful starting alerts are:

```promql
# Tune the absolute limit to the capacity reserved for this WAL filesystem.
arc_wal_dir_bytes > 8589934592

# A sustained increase can indicate that flush throughput is below ingest.
delta(arc_wal_dir_bytes[15m]) > 1073741824

# A file failed recovery repeatedly and was moved aside for investigation.
increase(arc_wal_quarantined_files_total[5m]) > 0
```

The examples use 8 GiB as an absolute threshold and 1 GiB of growth over 15
minutes; set them to match the WAL volume and the amount of temporary backlog
the deployment can safely hold. A growing WAL usually means its downstream
flush path is not draining as quickly as data arrives. A larger threshold
provides more recovery time but does not correct a sustained throughput gap.

WAL recovery only removes replayed files after a flush barrier confirms their
data reached storage. Files that remain unrecoverable are kept for retry and,
after repeated replay failures, renamed with a `.failed` suffix. Investigate
`arc_wal_quarantined_files_total` and preserve those files until their contents
have been accounted for.

An incomplete final entry after an interrupted append is treated as a truncated
tail, not as a poison entry. Complete preceding entries still pass the flush
barrier before their file is removed. A checksum or decoding failure in a
complete entry remains a recovery failure, even at the end of the file.

Quarantined WAL files are not replayed, but their valid flush checkpoints are
still scanned on every recovery pass. A checkpoint in one file can cover data
in an earlier file that is still awaiting recovery. Preserve quarantined files
together with the remaining WAL directory; deleting or moving one away can
remove the proof that prevents already-flushed records from being replayed.

## Reclaiming retained and quarantined files

There is no age-based fallback for retained files. A file's age, a successful
health check, zero `kept_files`, or a falling WAL gauge does not establish that
the records in a quarantined file reached storage. Recovery skips quarantined
data, so even a successful recovery pass is insufficient for that conclusion.

Use this maintenance procedure for one node's private WAL directory:

1. Stop or divert all input to the node, including replication and background
   producers. Restore healthy downstream storage and let normal recovery and
   flushing finish. Resolve reported replay/barrier errors. Keep all
   quarantined files in place throughout this drain: their checkpoints may be
   the only proof that records in earlier files were already persisted.
2. Stop Arc and confirm that no process or container can write this WAL
   directory. Take a complete offline copy on a separate filesystem, including
   normal WAL, quarantined WAL, and attempt sidecars. Record exact filenames,
   sizes and SHA-256 hashes; verify the copied bytes and preserve timestamps.
   Record the Arc revision, configuration and corresponding durable-storage
   backup or snapshot. A copy on the same full filesystem does not free space.
3. Inspect the stopped directory. **If any ordinary `*.wal` file contains an
   entry, is malformed, or cannot be read, stop manual reclamation.** Retain the
   complete checkpoint set and resume normal recovery after fixing the cause.
   The conservative check below allows only valid empty headers; it also
   refuses checkpoint-only files. It does not inspect quarantined records or
   authorize deleting them.
4. Account for every quarantined file's records against durable storage and
   the producer's authoritative record set, or recover missing records through
   a separately validated repair on an isolated copy. Counts alone cannot
   distinguish missing records from duplicates. If contents cannot be decoded
   or reconciled, preserve the file and escalate for repair; do not assume the
   missing data is expendable. Renaming `.failed` back to `.wal` or resending
   its entire contents can replay already-persisted records.
5. Only after steps 2–4 succeed, archive/remove the **explicitly inventoried
   quarantined filenames** from the stopped node's WAL directory, keeping the
   verified offline copy and the reconciliation record. Do not use a wildcard
   deletion, delete normal WAL files, or act on files that appeared after the
   inventory. An orphan attempt sidecar may be removed only when its associated
   normal WAL no longer exists; preserve it in the archive as well.
6. Start Arc, check recovery/flush errors, and repeat the affected record
   reconciliation before restoring input. On a discrepancy, stop and investigate
   using the archived set and the matching storage snapshot. Never restore an
   arbitrary subset of old WAL/checkpoints into a directory or storage state
   that has advanced since the snapshot; that can introduce duplicate replay.

This read-only check implements the conservative ordinary-WAL condition in
step 3 on Linux and macOS (Python 3). Set `WAL_DIR` to the stopped node's exact
directory. It neither proves quarantined data is durable nor removes files.

```sh
python3 - "$WAL_DIR" <<'PY'
from pathlib import Path
import sys

root = Path(sys.argv[1])
if not root.is_dir():
    raise SystemExit("STOP: WAL directory does not exist")
blocked = []
try:
    candidates = sorted(p for p in root.iterdir() if p.name.endswith(".wal"))
except OSError:
    raise SystemExit("STOP: cannot list the WAL directory")
for path in candidates:
    try:
        # Read one extra byte: an entry, torn tail, or unexpected header blocks.
        with path.open("rb") as source:
            header = source.read(8)
        if path.is_symlink() or header != b"ARCW\x00\x01\x01":
            blocked.append(path.name)
    except OSError:
        blocked.append(path.name)
if blocked:
    print("STOP: ordinary WAL needs recovery/inspection:", *blocked, sep="\n")
    raise SystemExit(1)
print("No ordinary WAL entries found. Backup and quarantine reconciliation remain required.")
PY
```

If free space is insufficient to complete recovery, first reduce admission or
provide capacity. A stopped node's **entire** WAL directory can be relocated to
a larger volume using a verified copy with original names/timestamps, updating
`wal.directory` before restart. Keep every quarantined checkpoint and sidecar
with it. Verify recovery from the new location before retiring the old copy.
Moving only the largest `.failed` file while retained files remain is unsafe.

## Replay attempts across restarts

Completed failed replay passes are recorded in a small `<file>.wal.recovery`
sidecar. Its update is written to a temporary file, synced, renamed atomically,
and the directory is synced before quarantine can proceed. Restarting Arc no
longer resets the threshold (three failed passes by default). The sidecar stores
only a format version, attempt count, file size and modification timestamp;
it contains no records. A changed size or timestamp starts a new attempt series
for an operator-repaired file.

Cancellation of recovery and failure of the flush barrier do not add strikes.
Successful replay clears prior strikes. Attempts interrupted before their
failure is recorded are not counted. Quarantine preserves the original WAL
bytes and their checkpoints; it does not declare their records disposable.

Each node must exclusively own its WAL directory. Keep attempt sidecars with
the WAL when restoring a stopped node, preserving file timestamps. If attempt
metadata cannot be read or written, recovery reports an error and retains the
WAL rather than guessing a count. Investigate filesystem permissions, free
space or malformed metadata before retrying. Sidecars are removed after a
successful replay or quarantine; cleanup failures are logged. A process crash
may leave an unreferenced `.wal-recovery-*` temporary file, which is not replayed
or used as a checkpoint.

## Recovery wait budget

Startup and maintenance recovery barriers submit buffered measurements to the
existing `ingest.flush_workers` pool. They do not hold shard locks while waiting
for queue space or storage completion. Each barrier waits for at most
`ingest.flush_timeout_seconds` (default 30 seconds), or an earlier caller
deadline. This is a budget for the barrier's wait, not a new timeout started at
enqueue for each storage write. It does not bound file scanning or the entire
healthy recovery pass.

On deadline expiry or the first observed flush failure, recovery retains the
affected WAL files and retries through normal maintenance. Records not yet
submitted remain in their buffers. Admitted tasks remain owned by the workers
and retain their own storage-write deadlines; they may complete after the
barrier returns. This also lets the barrier return when a storage implementation
ignores cancellation, without discarding an in-flight write. A later recovery
pass must settle those tasks and read their checkpoints before replaying again.

A short barrier budget can defer reclamation on a healthy but backlogged node.
Inspect flush errors, queue depth, throughput and WAL growth before increasing
the budget. Increasing it also increases how long recovery waits during an
outage; it does not improve the downstream drain rate.
