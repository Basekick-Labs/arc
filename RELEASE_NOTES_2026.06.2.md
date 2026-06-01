# Arc v2026.06.2 Release Notes

> **Status:** In progress (planned release: July 2026).

> **This is a patch release.** Bug fixes and hardening only — no new features, no breaking API changes, no schema migrations. Drop-in upgrade from v2026.06.1.

## Security hardening

This release strengthens internal verification routines on the Arc Enterprise activation path. The change is internal to Arc; existing license keys, activation flows, and `arc.toml` configurations continue to work unchanged.

Operators on 26.06.1 should plan to upgrade.

## Upgrade notes

1. **No configuration change required.** Drop in the new binary; existing `arc.toml` and license keys work as-is.
2. **Active licenses keep working.** Arc binaries running against `enterprise.basekick.net` continue to operate normally; no re-activation or license-key reissuance is required.
3. **OSS-only deployments** (no `[license]` block in `arc.toml`) are unaffected by this release.

## Dependencies

No dependency changes from 26.06.1.

---

_Maintainer notes: keep this file at the repo root (per [memory/project_release_strategy.md](memory/project_release_strategy.md)); do not write to `docs/RELEASE_NOTES_*` (that path is stale)._
