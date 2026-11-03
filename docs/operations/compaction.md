# Hourly compaction and ingest flushes

Hourly compaction evaluates each hour partition separately. By default it runs
at five minutes past each hour, waits until a partition is at least one hour
old, and requires at least 10 uncompacted input files in that partition. The
`compaction.hourly_min_files` setting controls that last threshold; it is a
fixed minimum, not a target file count or an estimate derived from ingest
configuration.

Each measurement's ingest buffer flushes when it reaches the configured row
limit, reaches its maximum age, or must flush for a schema change. The defaults
are 50,000 rows and 5 seconds. A flush usually writes one Parquet file for each
hour partition it touches. The age setting triggers a flush attempt after a
non-empty buffer reaches that age; it is not a guaranteed interval between
completed files. Idle gaps produce fewer files, while row-limit flushes and
schema changes can produce more, and queue or storage backpressure can delay
completion.

For example, a continuously active measurement that stays below 50,000 rows per
5 seconds and has no schema changes can produce about 720 files in an hour
partition (`3,600 / 5`). Measurements with gaps can produce far fewer. The age
setting therefore gives a useful upper-bound estimate only for sustained
traffic, not a general file-count prediction.

Do not derive `hourly_min_files` from `max_buffer_age_ms` alone. The observed
file count also depends on traffic gaps, row throughput, schema changes, and
partition boundaries. The default of 10 remains an independent floor; choose a
different value only after measuring file counts and compaction/query costs for
the workload and storage backend. A higher floor can leave sparse hourly
partitions for the daily tier, provided the daily file threshold is met.
