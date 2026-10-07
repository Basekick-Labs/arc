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
