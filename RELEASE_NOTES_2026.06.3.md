# Arc v2026.06.3 Release Notes

> **Status:** In development — targeted for release. Draft.

> **This is a patch release.** No breaking API changes and no schema migrations. Drop-in upgrade from v2026.06.2.

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

No product dependency changes in this release. The macOS build uses the same Go toolchain (Go 1.26.4), build tags (`duckdb_arrow`), and static-DuckDB linkage as the existing Linux builds.

---

_Maintainer notes: keep this file at the repo root (per [memory/project_release_strategy.md](memory/project_release_strategy.md)); do not write to `docs/RELEASE_NOTES_*` (that path is stale). This is a working draft — fold in any additional 26.06.3 changes (bug fixes, security items) under the standard `## Security hardening` / `## Bug fixes` / `## Performance improvements` sections as they land, matching the 26.06.2 structure._
