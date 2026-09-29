package tiering

import "errors"

// ClusterGate is the minimal role check the tiering manager needs to decide
// whether the local node may run a migration cycle. Only the primary writer
// migrates and deletes; every other node still keeps its tier metadata in
// sync (see Manager.ScanTiers) because the query layer routes from that
// metadata. It lives here rather than in the cluster package so tiering
// has no compile-time dependency on it — the same shape as
// compaction.ClusterGate and scheduler.WriterGate. A nil gate means "no
// check, allow": OSS, standalone and per-node-storage clusters, where each
// node's metadata is authoritative for its own storage.
type ClusterGate interface {
	// IsPrimaryWriter reports whether the local node may mutate storage.
	IsPrimaryWriter() bool
	// Role returns the node's role for log messages and API responses only;
	// IsPrimaryWriter is authoritative.
	Role() string
}

// ErrMigrationRoleGated is returned by RunMigrationCycle and TriggerMigration
// when a gate is wired and the node is not the primary writer. The scheduler
// treats it as a quiet skip; the API maps it to 409 Conflict.
var ErrMigrationRoleGated = errors.New("tiering: node is not the primary writer; migration runs on the primary only")

// ErrMigrationCycleRunning is returned when a cycle is asked for while one
// is still running on this node. Two overlapping cycles would let the
// second one's metadata sync observe the first one's half-finished copy.
var ErrMigrationCycleRunning = errors.New("tiering: a migration cycle is already running on this node")
