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
)
