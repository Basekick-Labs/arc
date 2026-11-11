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
