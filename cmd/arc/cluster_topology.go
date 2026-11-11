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
// configuration. Without writer failover every writer-role node is a primary,
// with failover a promoted writer starts empty; the advice is the same.
func localClusterWithoutReplicationWarning(failover bool) string {
	what := "every writer-role node acts as a primary writer, and each answers queries from its own disk only"
	if failover {
		what = "a writer promoted by failover starts from an empty disk"
	}
	return "This cluster runs on local storage with cluster.replication_enabled=false, which is neither supported cluster pattern: " +
		"no node reads another's disk, so queries see only the receiving node's own files plus the shared cold tier, readers hold no hot data, and " +
		what + ". Set cluster.replication_enabled=true on every node (per-node local storage with peer replication), or use cluster.shared_storage_mode with an object store"
}

// gateTieringInLocalClusterWithoutReplication reports whether a node of that
// configuration gets the writer gate on tiering. Writer-role nodes keep their
// own hot files and stay ungated: each migrates what is on its disk. A node
// that does not ingest has no hot files to migrate. Gated, it only syncs its
// tier metadata, as a reader does in the shipped patterns, and so retires the
// cold rows a writer's DELETE or retention removed instead of reporting them
// as lost (#1563). The gate is the writer gate, so a reader that failover
// promotes to writer becomes ungated with no restart.
func gateTieringInLocalClusterWithoutReplication(role string) bool {
	return role == "reader" || role == "compactor"
}
