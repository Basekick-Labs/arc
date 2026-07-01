# Arc v2026.06.3 Release Notes

> **Status:** In progress.

> **This is a patch release.** A reliability fix plus macOS/Homebrew packaging — no breaking API changes and no schema migrations. Drop-in upgrade from v2026.06.2.

## Reliability fix

**Improved database routing under heavy concurrent writes.** This release fixes an edge case where, under sustained high-throughput concurrent ingest, a write tagged with one database (via the `x-arc-database` header or the `db`/`bucket` query parameter) could occasionally be stored under a different database. The affected name showed up shortened to the length of the intended one — for example, writes meant for `energy_grid` (11 characters) landing under `vessels_tra` or `system_moni`, the leading 11 characters of other databases being written at the same moment.

The cause was a subtle string-lifetime detail at the request boundary. Arc's write handlers read the target database (and, on the TLE and import paths, the measurement) using Fiber's `c.Get` / `c.Query`, which hand back lightweight strings that point into the request buffer and are only meant to be used while the handler runs. Arc's ingest path keeps that string a little longer — the background Arrow-buffer flush uses it to pick the storage directory for the database — and if another request reused the buffer in between, the value could change out from under it. Copying the value at the boundary resolves it completely.

The behavior only showed up under concurrency with buffer reuse, so a steady or moderate write stream was very unlikely to see it, while a rapid burst of large payloads alongside other concurrent writers could hit it more often. It touched the MessagePack, line-protocol (native and InfluxDB-compatible), TLE, and bulk-import (CSV / Parquet / line-protocol / TLE) write paths. Reads and queries were never affected. There are no configuration, API, or on-disk format changes, and the fix is a drop-in upgrade.

If you happen to find rows under an unexpected database or measurement directory after running heavy concurrent ingest on an earlier version, those files are ordinary, intact Parquet and can simply be re-ingested once you're on 26.06.3.

## Deployment

**Install Arc on macOS with Homebrew.** Arc is now available through a Homebrew tap, so macOS users can install and update the `arc` binary with one command:

```bash
brew install basekick-labs/tap/arc
```

or, equivalently:

```bash
brew tap basekick-labs/tap
brew install arc
```

Then start the server:

```bash
arc            # listens on http://localhost:8000
arc --help
```

The release now builds native **macOS binaries** for both Apple Silicon (`arc-darwin-arm64`) and Intel (`arc-darwin-amd64`), alongside the existing Linux binaries, `.deb`/`.rpm` packages, container images, and Helm chart. Like every other release binary, the macOS binaries are built on GitHub-hosted runners and **cosign-signed** with keyless OIDC (a `.bundle` signature sidecar is attached to the release for offline verification). DuckDB is statically linked into the binary, so the Homebrew install has **no runtime dependencies** — it drops a single self-contained `arc` executable into your Homebrew prefix.

The formula installs the standard (non-FIPS) build, which is the right choice for local development and evaluation on macOS. The FIPS variant remains a Linux/CMVP-focused build distributed as a container image and Linux packages; see the FIPS deployment guide. Linux users should continue to use the `.deb`/`.rpm`, container, or Helm artifacts from the [release page](https://github.com/basekick-labs/arc/releases).

The tap lives at [github.com/basekick-labs/homebrew-tap](https://github.com/basekick-labs/homebrew-tap); the formula is updated automatically on each Arc release.

## Upgrade notes

1. **No configuration change required.** Drop in the new binary; existing `arc.toml` and license keys work as-is.
2. **macOS Homebrew users:** if you previously installed Arc by downloading a binary manually, you can switch to the tap with `brew install basekick-labs/tap/arc`. Homebrew will manage the binary and future updates from then on.
3. **Active licenses keep working.** No re-activation or license-key reissuance is required.

## Dependencies

No product dependency changes in this release. The macOS build uses the same Go toolchain, build tags (`duckdb_arrow`), and static-DuckDB linkage as the existing Linux builds.
