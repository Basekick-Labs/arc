package main

// A local-storage cluster without file replication (#1599).
//
// Neither shipped cluster pattern looks like this. Pattern 1 is per-node local
// storage *with* peer replication; Pattern 2 is shared object storage. Without
// replication no node reads another's disk: a query never reaches the node that
// holds the data, readers see only the cold tier, and a promoted writer starts
// from an empty disk. It is still what a local-storage cluster gets by default,
// because cluster.replication_enabled defaults to false.
//
// The cold tier is the one thing these nodes do share: the cluster's cold mark
// requires every node to use the same cold store (#1552). Tiering there used to
// be ungated on every node, so a reader recorded the writers' cold objects,
// kept their rows after a writer's DELETE or retention removed them, and
// reported them as lost data (#1551).

// localClusterWithoutReplication reports whether this node runs a cluster on
// local storage with neither shared storage nor file replication.
func localClusterWithoutReplication(hasCoordinator bool, storageBackend string, sharedStorage, replication bool) bool {
	return hasCoordinator && storageBackend == "local" && !sharedStorage && !replication
}

// localClusterWithoutReplicationWarning is the startup warning for that
// configuration. Writes are spread across the writers and each keeps only what
// it ingested. One primary writer is elected whenever Raft runs, failover or
// not (#872): it runs retention, DELETE and continuous queries, but reaches
// only its own files plus the shared cold tier. The writers stay ungated and
// keep reporting a peer's cold deletes as lost (#1551); the warning says so.
func localClusterWithoutReplicationWarning() string {
	return "This cluster runs on local storage with cluster.replication_enabled=false, which is neither supported cluster pattern: " +
		"no node reads another's disk, so each writer keeps only the data it ingested, a query sees only the receiving node's own files (plus the cold tier, which every node shares), readers hold no hot data, " +
		"and retention, DELETE and continuous queries on the primary writer reach only its own files. Writers here also report cold rows another node deleted as lost data. " +
		"Set cluster.replication_enabled=true on every node (per-node local storage with peer replication), or use cluster.shared_storage_mode with an object store"
}

// gateTieringInLocalClusterWithoutReplication reports whether a node of that
// configuration gets the writer gate on tiering. Writer-role nodes keep their
// own hot files and stay ungated: each migrates what is on its disk. A node
// that does not ingest has no hot files to migrate. Gated, it only syncs its
// tier metadata, as a reader does in the shipped patterns, and so retires the
// cold rows a writer's DELETE or retention removed instead of reporting them
// as lost (#1563). Roles are static and failover promotes only writers, so
// such a node stays gated.
func gateTieringInLocalClusterWithoutReplication(role string) bool {
	return role == "reader" || role == "compactor"
}

// A cluster on an object-store backend without cluster.shared_storage_mode
// (#1603) is the single-writer form of Pattern 2: every node reads and writes
// one bucket, one primary writer is elected through Raft, and the router and
// the ingest fence send writes to it. It needs no multi-writer licence
// (internal/license: "Single-writer Pattern 2 deployments do NOT require this
// flag"). The bucket is shared, so tiering is gated as in shared-storage mode:
// main.go decides that from the backend it built, as reconciliation and the
// manifest delete callbacks already do, not from the flag.

// objectStoreCluster reports a cluster whose hot storage is an object store,
// whatever cluster.shared_storage_mode says. backend is the configured
// storage.backend, lowercased at load. "Not local" is safe as a test because
// the set is closed: any other value is refused at load, so every non-local
// value is an object store.
func objectStoreCluster(clusterEnabled bool, backend string) bool {
	return clusterEnabled && backend != "" && backend != "local"
}

// objectStoreClusterStandaloneRoleError is the startup refusal for a
// standalone-role node in an object-store cluster without shared-storage
// mode, which refuses that role itself. Such a node never passes the
// primary-writer gate, so tiering, retention, continuous queries and deletes
// would run on no node.
const objectStoreClusterStandaloneRoleError = "a cluster on an object-store backend requires cluster.role to be writer, reader or compactor; " +
	"a standalone-role node never passes the primary-writer gate, so tiering migration, retention, continuous queries and deletes would run on no node"

// objectStoreClusterNoRaftWarning is logged when such a cluster has no Raft
// (cluster.raft_data_dir empty): no primary writer is elected, so every
// writer-role node accepts writes and runs tiering, retention, continuous
// queries and deletes against the one bucket.
const objectStoreClusterNoRaftWarning = "This cluster shares an object-store bucket but cluster.raft_data_dir is empty, so no primary writer is elected: " +
	"every writer-role node accepts writes and runs tiering migration, retention, continuous queries and deletes against the shared bucket. " +
	"Set cluster.raft_data_dir on every node"

// objectStoreClusterNoCoordinatorWarning is logged when cluster.enabled is set
// on an object-store backend but the coordinator is not running (no
// clustering licence, or it failed to start, see the errors above). Shared-
// storage mode refuses to start in that state; this configuration runs, with
// nothing gating it.
const objectStoreClusterNoCoordinatorWarning = "cluster.enabled is set on an object-store backend but the cluster coordinator is not running (see the log above): " +
	"this node runs tiering migration, retention, continuous queries and deletes against the shared bucket unconditionally, as if no other node used it"

// tieringGateWired reports whether a cluster node's tiering gets the writer
// gate. sharedObjectStore is cluster.IsSharedObjectStore of the backend that
// was built: a hot bucket is shared whether or not shared_storage_mode is set
// (#1603). role is the node's resolved role.
func tieringGateWired(sharedMode, replication, sharedObjectStore bool, backend, role string) bool {
	return sharedMode || replication || sharedObjectStore ||
		(localClusterWithoutReplication(true, backend, sharedMode, replication) &&
			gateTieringInLocalClusterWithoutReplication(role))
}
