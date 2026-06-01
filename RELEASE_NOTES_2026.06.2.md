# Arc v2026.06.2 Release Notes

> **Status:** In progress (planned release: July 2026).

> **This is a patch release.** Bug fixes and hardening only — no new features, no breaking API changes, no schema migrations. Drop-in upgrade from v2026.06.1.

## Security hardening

This release strengthens internal verification routines on the Arc Enterprise activation path and tightens authorization on database-management API endpoints. The changes are internal to Arc; existing license keys, activation flows, tokens, and `arc.toml` configurations continue to work unchanged.

`POST /api/v1/databases` and `DELETE /api/v1/databases/:name` now consistently require admin-permission tokens (matching the established convention on every other mutating endpoint in Arc). Read-only routes — listing databases, listing measurements, fetching database metadata — remain accessible to any authenticated token.

Operators on 26.06.1 should plan to upgrade.

## Upgrade notes

1. **No configuration change required.** Drop in the new binary; existing `arc.toml` and license keys work as-is.
2. **Active licenses keep working.** Arc binaries running against `enterprise.basekick.net` continue to operate normally; no re-activation or license-key reissuance is required.
3. **OSS-only deployments** (no `[license]` block in `arc.toml`) are unaffected by the license-verification change. The database-API authorization change applies only when authentication is enabled in `arc.toml`.
4. **Token review for operators using non-admin tokens for database management**: if any automation provisions databases via `POST /api/v1/databases`, ensure the token it uses has the `admin` permission. Auto-create-on-write (databases that come into existence as a side-effect of line-protocol or msgpack writes) is unaffected — write tokens continue to work for ingestion.

## Dependencies

No dependency changes from 26.06.1.

---

_Maintainer notes: keep this file at the repo root (per [memory/project_release_strategy.md](memory/project_release_strategy.md)); do not write to `docs/RELEASE_NOTES_*` (that path is stale)._
