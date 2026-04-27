package reconciliation

// Gate is the minimal interface the reconciler needs to decide whether the
// local node may run reconciliation work this tick. The two halves of a run
// (storage scan, manifest sweep) can have different gating rules depending on
// the storage backend topology:
//
//   - Shared storage (S3, Azure, MinIO): one node sweeps the bucket. Gate
//     returns true on both halves only on the active compactor.
//   - Local storage (per-node disks): every node walks its own backend
//     filtered by OriginNodeID == self. Gate returns true on both halves
//     on every node; the per-file OriginNodeID filter happens inside the
//     reconciler.
//   - Standalone (no cluster): no Raft, no roles. Gate returns true on
//     both halves unconditionally when reconciliation is enabled.
//
// nil gate is treated as "always allowed" — the standalone fallback. The
// gate is consulted at every tick (and at every chunk boundary inside a
// run) so role transitions take effect without a restart, mirroring the
// retention/compaction scheduler pattern.
type Gate interface {
	// ShouldRunStorageScan reports whether the local node may walk the
	// storage backend looking for orphan files this tick.
	ShouldRunStorageScan() bool

	// ShouldRunManifestSweep reports whether the local node may issue
	// manifest deletes for orphan manifest entries this tick.
	// BatchFileOpsInManifest already leader-forwards, so this is usually
	// the same as ShouldRunStorageScan — but we keep them separable so the
	// dual-mode local-storage case (every node scans, only one writes the
	// manifest summary) can be expressed cleanly if needed.
	ShouldRunManifestSweep() bool

	// Role returns a human-readable role string for log messages only.
	Role() string
}
