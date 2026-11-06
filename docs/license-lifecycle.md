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

The production licensing base URL remains `https://enterprise.basekick.net`.
Local/Atila endpoints and trust roots are test-only external overlays and must
never be included in a production build or published source change.

Online clients use `POST /api/v2/activate` and `GET /api/v2/verify`. The endpoint
selects protocol 2; requests do not send a `license_protocol` selector. Both
endpoints require a fresh nonce. A missing v2 endpoint or an invalid response
never triggers fallback to v1. The authority's `/api/v1` endpoints retain their
existing request validation, response payloads and legacy expiration behavior;
introducing v2 does not change the contract for older consumers.
An unavailable v2 route, including a router's JSON 404, is transient: a running
process retains its current grant and cache, and a fresh process waits for
authority rather than enabling Enterprise or starting independent OSS services.
An explicit `license not found` verdict remains a definitive rejection.

Protocol 2 distinguishes an installation's admission from the validity of its
key. `maximum machine activations reached` and unrecognized protocol refusals
fence this installation; they do not erase signed evidence, invalidate the key
for peers, or prove that grace has ended. Retained cache bytes cannot reopen a
refused installation. A configured cluster can recover coordination while data
and maintenance remain fenced. A fresh successful proof clears the refusal and
uses the existing runtime-rebuild path before newly licensed components serve.
An absent grant or unavailable clock is nonterminal. A final OSS handoff needs
a recognized terminal cause on every participant, such as the signed cutoff or
an explicit key-wide rejection. Actual cutoffs still apply to the preserved
trusted policy while admission is blocked; admission never extends grace.

Online installations persist `license_installation.json` beside the auth database
on the node's own persistent volume. The first identity uses the normal machine
fingerprint and remains stable across subsequent pod/network replacements.
Creation is atomic and uses owner-private permissions, accepting private group
read/write modes applied by Kubernetes fsGroup on remount. A corrupt identity or
failed persistence does not silently create a different installation. Keep this
file with the node's data across restarts and key changes. A copied license
cache alone never supplies an identity or authorizes an online boot. An older
installation without this file establishes its current machine identity on its
first upgraded start; this does not automatically recover activations already
consumed by earlier identity churn. Migration/reassignment of those existing
activations requires explicit validation, not automatic slot release or a quota
increase. Cloning an entire node volume, including its identity, is outside the
cache-only copy boundary and must not be used to run a second logical node.
Changing only the request URL to v1 does not satisfy protocol 2: a legacy signed
license without its policy and fresh time proof cannot enable Enterprise at
startup. A process with an already trusted grant keeps that grant's deadlines
and cumulative restrictions when such a response fails validation, including
day-7 writer stopping and the four-hour restart policy. Separating API versions
does not prevent someone from modifying the client's verification code.

Protocol 2 also requires a signed `expiry_policy` containing its own version,
paid/trial kind, nominal `term_ends_at` and grace days. The server derives this
from its nominal term record, not an ambiguous legacy signed file. Signed
`expires_at` is the final cutoff. Legacy files do not silently receive fourteen
additional days. A protocol-2 response cannot be downgraded to an old policy by
stripping signed fields or supplying a legacy version selector.

For paid licenses, UTC grace day 1 begins at midnight after the expiration date.
Restrictions are cumulative throughout the fourteen complete grace days:

| Paid grace day | Four-hour restarts | New writer ingestion |
| --- | --- | --- |
| 1 | No | Enabled |
| 2 | No | Enabled |
| 3 | No | Enabled |
| 4 | Yes | Enabled |
| 5 | Yes | Enabled |
| 6 | Yes | Enabled |
| 7 | Yes | Stopped |
| 8 | Yes | Stopped |
| 9 | Yes | Stopped |
| 10 | Yes | Stopped |
| 11 | Yes | Stopped |
| 12 | Yes | Stopped |
| 13 | Yes | Stopped |
| 14 | Yes | Stopped |

D+15 midnight requires one standalone OSS survivor. The four-hour interval starts
when a running process first observes day-4 eligibility, or starts already in
that period. The timer uses monotonic process uptime; ordinary license refreshes
and the day-7 transition do not reset it. Restarts gracefully drain accepted work,
retain Raft membership and replace the process only after successful shutdown.
An in-progress final handoff takes priority over a periodic restart. Stopping
writers refuses new live ingestion, including MQTT and forwarded imports, before
WAL writes; already admitted finite batches finish and recovery/flushes remain
available. Reads, RBAC management and tier migrations retain their licensed
behavior during grace. There are no separate day-8, day-9 or day-10 restrictions.

Trials retain full Enterprise behavior for their fourteen-day term, then switch
directly to OSS at the signed deadline (the beginning of trial day 15). They have
no additional grace, four-hour restarts or day-7 writer stop. A production license
uses a separate new key; the trial key is not converted. License state must preserve data, metadata,
configuration and existing access-control enforcement. Cold-data reading is
separate from migration eligibility. Renewed/replacement keys must respect their
own feature and capacity limits and reconcile changes before reopening peers.
The existing admin-only membership-removal recovery route remains available.

Nodes retain their configuration and metadata throughout grace. Writer failover
cannot reopen ingestion once writers are stopped. The lifecycle controller
chooses serving owners only when preparing the final OSS handoff; retained older
`reduced` records remain recoverable. `health.desired_policy` describes the requested
phase, cumulative restart interval and writer-stop flag; actual write readiness
describes enforcement.

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

Assigning a new production key or renewing re-enables only the loaded license's
capabilities and capacities, preserving configuration and stored data. A
process initially booted without entitlement, or receiving different capabilities
or limits, defers all feature gates until a fresh process reconstructs licensed
components. Time-only verification refreshes do not restart the runtime. A
retained topology exceeding a replacement key's machine or aggregate core limit
automatically receives a durable serving allocation once participants agree on
the new key and constructed runtime. The designated writer is retained; remaining
capacity is assigned deterministically to readers, compactors and additional
writers within both limits. Excess nodes retain their data, configuration, Raft
membership and recovery/control services, but cannot serve data or run licensed
data work. Serving checks revalidate the selected nodes' current core counts;
a stale allocation cannot exceed a newly applied limit. A designated writer that
cannot yet fit remains fenced until its runtime applies the new core limit;
selection never bypasses the ownership/promotion protocol. The admin membership
recovery route remains available. Retained metadata is not deleted.
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

The revised cumulative paid-grace schedule has focused Go regression coverage.
Earlier Docker acceptance predates that revision; real-server integration and
multi-node restart and production-key replacement acceptance must be repeated for
this schedule before rollout. Focused fixtures exercise signed key replacement,
retained configuration/data bytes and serving-allocation enforcement; they do not
establish full live-cluster continuity after replacing a real key.
