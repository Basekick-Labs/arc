# Arc v2026.09.4 Release Notes

> **Status:** Release candidate.

A patch release on 26.09.3 with three fixes. Everything else on `main` ships
in 27.01.1.

## Fixed: compaction no longer deletes rows that differ only in their field values ([#1232](https://github.com/Basekick-Labs/arc/issues/1232))

Compaction deduplicates data that carries tag metadata (line protocol) and
continuous-query output. Up to 26.09.3 it kept **one row per (tags, time)**,
or one row per time for tagless continuous-query output, and deleted the rest
whatever their field values. Which row survived was arbitrary, and in
practice it was usually the oldest. So compaction silently deleted:

- **Corrections:** a point re-sent with the same tags and time and a new
  value. The old value usually survived.
- **Partial writes:** two writes of the same series and time carrying
  different fields (`usage=1`, then `temp=5`). One of them was dropped.
- **Re-run continuous-query windows:** the stale aggregate usually survived.

Compaction now removes a row only when another row is identical to it in
**every** column (time, every tag and every field).

**Behaviour change:** a re-sent point with a new value no longer replaces the
old one. Both rows are stored, and queries count both. Exact duplicates, such
as a client retrying the same write, are still collapsed.

**Clusters:** only the node holding the compaction lease compacts, and every
other node pulls its output. Upgrading the compactor node protects the data,
whatever version the writers and readers run. With
`cluster.failover_enabled = true`, a writer takes over compaction when the
compactor is down, so upgrade writers too. Until then, a 26.09.3 writer
holding the lease still compacts the old way.

Continuous-query output without tag columns is now marked
`arc:dedup_exact`. The old key, `arc:dedup_time`, is still read but no longer
written, because 26.09.3 deduplicates files carrying it on time alone. A file
written by 26.09.4 is therefore never deduplicated by 26.09.3 after a
rollback.

**Cost:** rows are compared in full only where their (tags, time) key
repeats, so compaction cost is close to before. A partition that is mostly
duplicates takes more memory to compact.

## Fixed: the Linux packages run on Debian 12, Ubuntu 22.04, RHEL 9 and Amazon Linux 2023 ([#1579](https://github.com/Basekick-Labs/arc/issues/1579))

The 26.09.3 Linux binary needed glibc 2.38, because it was built on the newest
GitHub runner:

- On Debian 12 and Ubuntu 22.04 the `.deb` installed and then failed to start
  (``version `GLIBC_2.38' not found``).
- On RHEL/Rocky/Alma 9 and Amazon Linux 2023 the RPM refused to install. On
  RHEL 9 it also needed a newer libstdc++ (`GLIBCXX_3.4.30`) than the system
  ships.

The binaries are now built on Ubuntu 22.04 with libstdc++ linked in, so they
need only glibc 2.34 or newer and the system's `libgcc_s`.

- **Supported:** Ubuntu 22.04+, Debian 12+, RHEL/Rocky/Alma 9+, Amazon Linux
  2023, Fedora 35+.
- **Not supported:** Ubuntu 20.04 and RHEL 8.

The `.deb` declares `Depends: libc6 (>= 2.34), libgcc-s1`, and the RPM
declares `Requires(pre): shadow-utils`. Each release now installs and starts
its packages on Debian 12, Ubuntu 22.04, Rocky 9 and Amazon Linux 2023 before
publishing.

## Changed: continuous queries are enabled in the packaged configuration

The `arc.toml` shipped in the `.deb`, RPM, Arch package and Docker image set
`[continuous_query] enabled = false`, unlike the code default and the
documentation. On a fresh package install, every continuous-query API call
answered 404 and every `arcli cq` command failed with "continuous queries are
disabled on this server". It is now `enabled = true`. No continuous query runs
until one is created.

The 26.09.x packages replace `/etc/arc/arc.toml` on upgrade, so an upgrade to
26.09.4 enables continuous queries unless the file is edited back. To keep them
off, set `[continuous_query] enabled = false` or
`ARC_CONTINUOUS_QUERY_ENABLED=false`.

## Security: Go 1.26.10 and dependency updates

- **A crafted zstd request body no longer crashes Arc.**
  `klauspost/compress` up to 1.20.1 panics on a crafted ~27 KB zstd frame
  ([GHSA-fpmh-frq8-7rj8](https://github.com/advisories/GHSA-fpmh-frq8-7rj8),
  GO-2026-6865). Arc decoded zstd write bodies on a goroutine of its own,
  where the panic cannot be recovered, so one POST to a write endpoint
  stopped the process. Fixed by `klauspost/compress` 1.20.4.
- **Go 1.26.10** fixes vulnerabilities in `net/http`, `net/textproto`,
  `crypto/tls`, `crypto/x509` and `html/template`.
- **`golang.org/x/net` 0.60.0** fixes HTTP/2 server issues.
- **`andybalholm/brotli` 1.2.7** fixes excessive allocation on crafted
  streams.

**Performance note (arm64):** `klauspost/compress` 1.20 encodes snappy on
arm64 with new assembly, and every Parquet file Arc writes is
snappy-compressed. On an Apple M3 Max with 1.20.3, line-protocol ingest was
3.2% slower and MessagePack+gzip unchanged, with files 2-4% larger
([klauspost/compress#1271](https://github.com/klauspost/compress/issues/1271)).
1.20.2 is the first release with the security fix.
