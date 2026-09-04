# Upgrading pre-26.06.1 API tokens

Applies to Arc Enterprise clusters with API-created tokens from before 26.06.1.
Plan a maintenance window and preserve administrative access. Bootstrap tokens
configured identically on every node are unaffected.

## Why legacy tokens need local cleanup

Legacy tokens exist only in individual nodes' SQLite auth databases. They were
not migrated into the replicated FSM. The cluster revoke endpoint looks up the
ID in the FSM: an unknown ID is an idempotent no-op even if the API returns
success. If a legacy AUTOINCREMENT ID collides with a replicated token ID, the
same request can revoke the replacement token instead. Never use the replicated
revoke endpoint to remove a legacy token.

## Maintenance procedure

1. Inventory active tokens on **every node** before upgrade: record node, local
   token identity, name, permissions, owner and consumers. Preserve enough
   non-secret identity to distinguish legacy rows from replacements; a numeric
   ID alone is insufficient. Do not export hashes or plaintext into the inventory.
2. Stop each node before backing up the file configured by `auth.db_path`,
   including SQLite `-wal` and `-shm` sidecars when present. Preserve the backup
   with restricted access. Never copy or delete the live database during writes.
3. Upgrade all nodes to 26.06.1 or later. Restore stable leadership and full
   membership before issuing replicated tokens.
4. Create a short-lived validation token through `POST /api/v1/auth/tokens`.
   Confirm authentication on every node and matching increments of
   `arc_cluster_auth_apply_create_total` on every node. Revoke **this new token**
   through `POST /api/v1/auth/tokens/:id/revoke`; verify rejection on every node.
5. Re-issue each inventoried token through `POST /api/v1/auth/tokens` on any node,
   preserving its scope and permissions. Capture the returned plaintext once
   into the approved secret store. Verify it on every node before rotating any
   consumer. If an ID collision prevents materialisation, stop and resolve the
   divergence as described below before proceeding.
6. Rotate downstream CI secrets, SDKs, dashboards and other consumers to the
   replacement. Verify each consumer against multiple nodes.
7. Remove the old token **locally on every node that holds it**. Stop the node,
   take a fresh backup, and remove only the positively identified legacy rows
   from its local auth database, or use the supported rebuild from replicated
   FSM state. Do not select rows by ID alone or remove a replicated replacement.
   If identity cannot be established, stop and seek operator support rather than
   guessing. Restart and verify that replacement tokens still authenticate and
   legacy tokens do not. Keep quorum available during node maintenance, or use
   a planned full-cluster outage. Repeat steps 5–7 one token at a time.
8. Verify completion: every active consumer uses a replacement, all apply
   counters converge, `arc_cluster_auth_rejected_total` is stable, and every
   legacy token is rejected on every node.

## Divergence and rollback

On a collision or growing rejected counter, stop the affected node and preserve
its backup. Reconcile only the conflicting legacy rows, or rebuild its local
auth database from authoritative FSM state before rejoining. Never remove the
Raft state to repair the SQLite cache. Do not invent SQL cleanup by token ID.

Restoring a pre-upgrade database can re-enable legacy credentials and diverge
from replicated state. Treat rollback as a coordinated maintenance operation:
keep the affected node out of client traffic until its state and credentials
have been reconciled and verified. Retain backups until the operator accepts
the completed migration.

See [26.06.1 release notes](../RELEASE_NOTES_2026.06.1.md) for replication,
collision handling and metrics. The source issue is
[#457](https://github.com/Basekick-Labs/arc/issues/457); its original suggestion
to revoke legacy tokens through the API does not apply to unmigrated rows.
