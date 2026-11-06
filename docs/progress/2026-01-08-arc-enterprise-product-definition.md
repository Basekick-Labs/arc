# Arc Enterprise product definition — license lifecycle

Updated 2026-11-05. This licensing section accompanies the current implementation
work; the README and activation-server tier definitions describe other features.

Paid licenses use a signed nominal term and fourteen UTC calendar grace days
starting the next date. Days 1–3 preserve operation. From day 4, processes restart
gracefully every four hours. From day 7 through day 14, writers also stop accepting
new ingestion; the four-hour restarts continue. These are cumulative restrictions.
Reads and existing recovery continue; there are no separate day-8 role reduction,
day-9 RBAC management or day-10 tiering restrictions. After day 14 one authoritative
OSS node remains. Trials run as Enterprise for their full fourteen-day term and
switch directly to OSS on day 15, without an additional grace or paid-grace
degradation. A production license is a separate new key; accepting it preserves
configuration and data and enforces the replacement key's capabilities and limits.

Transitions must preserve data, configuration, metadata, logs, recovery state and
existing data authorization. Replacement keys restore only their capabilities
and limits after safe reconciliation. Online-key Enterprise startup requires
fresh signed server time; a cached license cannot establish the date after a
restart. Application timestamps are not changed.

The controller persists ownership for the final OSS handoff in Raft,
fences and drains writers, and retains a verified generation namespace on every
participant. Every restart runs an authenticated unanimous OSS-owner election;
other processes remain parked and Raft is disabled. Missing participants block a
new election. Generated identities are persisted before replacement. A controller
restart resumes incomplete peer release. Compatible participants are required.
Prepared identity is recovered across PID changes even before local sealing;
prepared state alone never authorizes an OSS boot.
See [the protocol and operational limits](../license-lifecycle.md).
Read-serving tier synchronization retains scan budgets and never admits cold
cleanup after migration permission ends.
Synthetic OSS Docker acceptance confirms thirty cold rows remain queryable after
restart with no license and a non-default S3 prefix. Retained expired cluster
configuration may elect another node after a coordinated restart. OSS-period
writes remain on their accepting node and are not merged into another owner's
namespace. Automatic license-reactivation process replacement retains the current
owner. Restoring a former multi-node cluster after sealed OSS operation requires
separate reconciliation; feature reactivation alone does not perform that merge.
Changed capabilities or resource limits require process reconstruction before
feature gates reopen. A replacement key with smaller machine/core limits selects
a supported serving subset by consensus; excess nodes remain fenced with their
data/configuration retained and control/recovery membership intact. The designated
writer remains authoritative, and current selected core counts must fit. A missing
shared-storage capability still requires the safe handoff. Local seal
durability does not authorize leaving Raft until all participants persist it.
Final drains join live MQTT admissions and actual tiering work and
materialize current auth/RBAC revocations before sealing.
Configured Iceberg export uses a separate catalog and warehouse for the OSS
generation. Predecessor metadata is retained, and directory readers must switch
to the logged new warehouse with access to the selected node's local files.

Earlier accelerated Docker acceptance covers two paid profiles and one trial, shared S3
and local hot storage, generated IDs, repeated OSS restarts, and injected
controller crashes before global release in two- and three-voter clusters.
These finite fixtures do not certify an activation-server rollout or automatic
reconstruction of the previous multi-node cluster.
That acceptance predates the revised day-4/day-7 schedule, which requires renewed
multi-node restart and real-server verification.
