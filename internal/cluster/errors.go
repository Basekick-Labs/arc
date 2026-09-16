package cluster

import "errors"

// Cluster-specific errors.
var (
	// ErrLicenseRequired indicates that an enterprise license is required for clustering.
	ErrLicenseRequired = errors.New("enterprise license required for clustering")

	// ErrClusteringFeatureRequired indicates the license doesn't include the clustering feature.
	ErrClusteringFeatureRequired = errors.New("license does not include clustering feature")

	// ErrInvalidRole indicates an invalid node role was specified.
	ErrInvalidRole = errors.New("invalid cluster role")

	// ErrAlreadyRunning indicates the coordinator is already running.
	ErrAlreadyRunning = errors.New("cluster coordinator already running")

	// ErrNotRunning indicates the coordinator is not running.
	ErrNotRunning = errors.New("cluster coordinator not running")

	// ErrNodeNotFound indicates the requested node was not found in the registry.
	ErrNodeNotFound = errors.New("node not found")

	// ErrNodeAlreadyExists indicates a node with the same ID already exists.
	ErrNodeAlreadyExists = errors.New("node already exists")

	// ErrClusterNotEnabled indicates clustering is not enabled in configuration.
	ErrClusterNotEnabled = errors.New("clustering is not enabled")

	// ErrIngestNotAllowed indicates this node role cannot accept writes.
	ErrIngestNotAllowed = errors.New("this node role does not accept writes")

	// ErrQueryNotAllowed indicates this node role cannot execute queries.
	ErrQueryNotAllowed = errors.New("this node role does not execute queries")

	// ErrCompactionNotAllowed indicates this node role cannot run compaction.
	ErrCompactionNotAllowed = errors.New("this node role does not run compaction")

	// ErrTooManyNodes indicates the registry has reached its maximum node capacity.
	ErrTooManyNodes = errors.New("maximum number of cluster nodes reached")

	// ErrCoreLimitExceeded indicates adding this node would exceed the licensed core limit.
	ErrCoreLimitExceeded = errors.New("cluster core limit exceeded")

	// ErrCompactorFailoverInProgress indicates a compactor lease change is
	// already running, so a manual assignment would race it.
	ErrCompactorFailoverInProgress = errors.New("a compactor lease change is already in progress")

	// ErrCompactorLeaseNotManaged indicates this cluster has no compactor
	// failover manager and no lease, so assigning one would switch every
	// node from the static role check to a lease nothing maintains.
	ErrCompactorLeaseNotManaged = errors.New("this cluster does not manage a compactor lease")

	// ErrAlreadyCompactorLeaseHolder indicates the target already holds the
	// lease, so the assignment would be a no-op reported as a move.
	ErrAlreadyCompactorLeaseHolder = errors.New("node already holds the compactor lease")

	// ErrCannotHoldCompactorLease indicates the target's role may not hold
	// the compactor lease (readers have no write access to storage).
	ErrCannotHoldCompactorLease = errors.New("this node role cannot hold the compactor lease")

	// ErrNodeNotHealthy indicates the target node is not in a healthy state.
	ErrNodeNotHealthy = errors.New("node is not healthy")

	// ErrVoterConvergeUnsafe indicates converging the voter set would risk
	// quorum, so nothing was changed.
	ErrVoterConvergeUnsafe = errors.New("converging the voter set would risk quorum")

	// ErrVoterConvergeNotReady indicates the Raft barrier did not complete, so
	// roles could not be read against an applied FSM.
	ErrVoterConvergeNotReady = errors.New("cluster state is not settled enough to converge the voter set")

	// ErrVoterConvergeNeedsLeadershipMove indicates this node holds a vote its
	// own role does not grant, so leadership has to move before it can be
	// demoted.
	ErrVoterConvergeNeedsLeadershipMove = errors.New("leadership must move before this node's vote can be revoked")

	// ErrVoterConvergeLeadershipMoved is returned when leadership was
	// transferred and the caller must re-run against the new leader. It is not
	// a failure.
	ErrVoterConvergeLeadershipMoved = errors.New("leadership moved; re-run against the new leader")

	// ErrClusterRaftNotConfigured indicates clustering is enabled but no Raft
	// data directory was configured, so there is no consensus layer to apply
	// a topology change through.
	ErrClusterRaftNotConfigured = errors.New("clustering is not configured with Raft")
)
