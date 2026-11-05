# License time, expiration and cluster lifecycle

Online-key installations require a fresh signed time response after every
process start before enabling Enterprise features. A cached license proves its
contents and machine binding, not today's date. A temporary server failure during
startup waits and remains cancellable; it must not bootstrap a persisted cluster
as independent OSS writers. An already certified standalone OSS survivor can
continue serving without licensing-server availability, after one bounded fresh
verification attempt; Enterprise features still require fresh proof. Existing running processes advance their authority
anchor using monotonic elapsed time and preserve entitlement on transient errors.

Each activation/verification request includes a cryptographic 256-bit `time_nonce`.
The response includes `time_file` and `time_signature`, verified with Arc's pinned
RSA key over the exact decoded bytes. The signed statement binds purpose/version,
nonce, configured key hash, machine fingerprint, exact signed-license digest, and
UTC server time. Replays, mismatches, backward authority refreshes, malformed
proofs and requests exceeding the freshness budget are rejected. No operator
configuration can substitute a licensing clock. HTTP Date and license IssuedAt
are not trusted as current time. Application/data timestamps are not changed.

Protocol 2 also requires a signed `expiry_policy` containing its own version,
paid/trial kind, nominal `term_ends_at` and grace days. The server derives this
from its nominal term record, not an ambiguous legacy signed file. Signed
`expires_at` is the final cutoff. Legacy files do not silently receive fourteen
additional days. A protocol-2 response cannot be downgraded to an old policy by
stripping negotiation parameters or signed fields.

For paid licenses, UTC grace day 1 begins at midnight after the expiration date.
Days 1–7 retain licensed behavior; day 8 reduces active services to one per role;
day 9 makes RBAC management read-only; day 10 stops admitting new tier migrations;
D+15 midnight requires one standalone OSS survivor. Trials end at their exact
signed deadline and have no grace. License state must preserve data, metadata,
configuration and existing access-control enforcement. Cold-data reading is
separate from migration eligibility. Renewed/replacement keys must respect their
own feature and capacity limits and reconcile changes before reopening peers.
The existing admin-only membership-removal recovery route remains available;
new token memberships are blocked with other RBAC management changes on day 9.

The lifecycle controller commits the day-8 assignment in Raft. Only the selected
node for each configured role serves; automatic writer and compactor replacement
are disabled. A missing selected node does not promote a spare. Nodes retain
their configuration and metadata. `health.desired_policy` describes the requested
phase; the cluster lifecycle state and actual readiness describe enforcement.

At the final cutoff, the controller fences mutations, drains acknowledged ingest,
WAL flushes, registration outboxes, admitted MQTT work and background mutations,
and synchronizes the manifest. It reserves the actual tiering cycle and scan
guards through process replacement, rather than treating historical job records
as current activity. WAL maintenance is paused between complete passes and resumes
if a drain fails, preserving retry recovery. A checked final auth/RBAC
materialization must succeed before a participant reports drained; stale revoked
credentials cannot become standalone authority. Each participant materializes the exact hot manifest into a separate
generation namespace with content-hash verification. Cold objects remain readable
through the configured backend. A durable sealed certificate is committed before
process replacement. Local `.sealed` durability and global Raft-off authorization
are separate: only after all participants report durable sealing does the
controller publish boot authority and release peers. A crash before that point
restarts fenced Raft coordination, retaining quorum; a crash afterwards resumes
authenticated release. All participants must persist it before release. If a peer,
quorum, drain or copy fails, the controller keeps writes fenced and retries; it
does not guess that data is safe or erase the original storage.

Every subsequent start honors the certificate even if the operator keeps the old
cluster configuration. A separate authenticated control channel elects one OSS
process using fresh process incarnations and unanimous participant promises.
Ordinary Raft transport and node roles are disabled. Other processes remain
parked. Missing participants prevent a new election; a serving owner is retained.
This transition requires an already established Raft cluster and reachable
participants running the compatible lifecycle protocol. The prepared record
persists the resolved local node identity, including generated IDs, before process
replacement. Startup recovers that identity even if a crash happened before the
local seal persisted; prepared state alone never authorizes leaving Raft.
Namespace paths remain inside configured writable roots, isolated by
cluster, participant and generation; predecessor listings exclude those copies.
If the controller restarts between persistence and release, bootstrap resumes
authenticated generation-bound releases of still-sealed Raft peers. Their replies
never substitute for the subsequent unanimous Raft-off election.

On a coordinated restart with unchanged expired configuration, another retained
node can become the OSS owner. The sealed cluster dataset is available on every
participant; writes subsequently accepted in OSS remain in that node's namespace.
They are preserved physically but are not automatically transferred to a different
owner. Operators must renew or explicitly reconcile this stale configuration
before relying on cross-node continuity for those later writes. Automatic
license-reactivation process replacement pins the current owner instead.

Renewal re-enables only the loaded license's capabilities and capacities. A
process initially booted without entitlement, or receiving different capabilities
or limits, defers all feature gates until a fresh process reconstructs licensed
components. Time-only verification refreshes do not restart the runtime. A
retained topology exceeding a replacement key's machine or aggregate core limit remains fenced;
the authenticated membership-removal route remains available for reconciliation.
Retained metadata is not deleted.
Retained shared-storage mode without its feature grant also stays fenced and
enters the safe standalone handoff; a recovery exception cannot authorize it.

When Iceberg export is configured, the OSS generation uses an independent catalog
beside its sealed storage and a local warehouse under that storage root. The
predecessor catalog and warehouse remain unchanged; their snapshots do not become
the publication target for later OSS writes. This avoids reopening absolute local
or S3 metadata locations against a different backend or credentials context.
Startup logs the new warehouse. Directory readers must explicitly switch to it
and have access to the selected node's local data. Each retained node keeps its
own OSS export history, just as it keeps its own OSS writes.
Reactivation of a sealed OSS owner restores supported standalone features; it
does not automatically merge divergent OSS namespaces or rebuild the former
multi-node cluster. That inverse reconciliation remains separate work.
Read-serving tier scans retain the configured scan deadline and do not remove
redundant cold objects when migration permission is disabled.
Definitive rejection also invalidates the accepted cached-payload digest while
retaining elapsed authority time, so failed cache deletion cannot restore the
rejected entitlement in the same process.

Rollout requires the companion activation-server changes before the new client:
old servers cannot produce the required time proof/policy. Existing natural-expiry
soak installations remain on their original image, while accelerated acceptance
will use isolated test signers and clocks. No production time override, host-clock
change, server deployment or real-key re-signing is part of the test fixture.

Clock rollback protection applies to the honest online client. Offline permission
and legacy file compatibility are being resolved separately. A fully offline
machine or restored whole-VM snapshot cannot prove current external time without
fresh authority or a separate trusted hardware clock; this protocol must not be
described as protection against a modified binary or compromised license server.
