package cluster

// NodeRole represents the role of a node in the cluster.
// Each role has specific capabilities that determine what operations the node can perform.
type NodeRole string

const (
	// RoleStandalone is the default single-node deployment mode (OSS-compatible).
	// Can perform all operations: ingest, query, and compact.
	RoleStandalone NodeRole = "standalone"

	// RoleWriter handles ingestion, WAL, and flushes to shared storage.
	// Can also execute queries for monitoring and validation.
	RoleWriter NodeRole = "writer"

	// RoleReader is a query-only node that reads from shared storage.
	// Cannot ingest data or run compaction jobs.
	RoleReader NodeRole = "reader"

	// RoleCompactor runs background compaction and maintenance tasks.
	// Cannot ingest data or serve queries.
	RoleCompactor NodeRole = "compactor"
)

// RoleCapabilities defines what operations each role can perform.
type RoleCapabilities struct {
	CanIngest     bool // Accept write requests (LineProtocol, MessagePack)
	CanQuery      bool // Execute SQL queries
	CanCompact    bool // Run compaction jobs
	CanCoordinate bool // Participate in cluster coordination (leader election)
}

// GetCapabilities returns the capabilities for this role.
func (r NodeRole) GetCapabilities() RoleCapabilities {
	switch r {
	case RoleWriter:
		return RoleCapabilities{
			CanIngest:     true,
			CanQuery:      true,  // Writers can query for monitoring
			CanCompact:    false, // Compaction runs on dedicated nodes
			CanCoordinate: true,  // Writers participate in leader election
		}
	case RoleReader:
		return RoleCapabilities{
			CanIngest:     false,
			CanQuery:      true,
			CanCompact:    false,
			CanCoordinate: false,
		}
	case RoleCompactor:
		return RoleCapabilities{
			CanIngest:     false,
			CanQuery:      false, // Compactors don't serve queries
			CanCompact:    true,
			CanCoordinate: false,
		}
	case RoleStandalone:
		return RoleCapabilities{
			CanIngest:     true,
			CanQuery:      true,
			CanCompact:    true,
			CanCoordinate: false, // Standalone doesn't coordinate
		}
	default:
		// Fail-safe: no capabilities for unknown roles
		return RoleCapabilities{}
	}
}

// String returns the string representation of the role.
func (r NodeRole) String() string {
	return string(r)
}

// IsValid returns true if the role is a recognized value.
func (r NodeRole) IsValid() bool {
	switch r {
	case RoleStandalone, RoleWriter, RoleReader, RoleCompactor:
		return true
	default:
		return false
	}
}

// ValidRole returns true if the role string represents a valid role.
func ValidRole(role string) bool {
	return NodeRole(role).IsValid()
}

// ParseRole parses a string into a NodeRole.
// Returns RoleStandalone if the role is empty or invalid.
func ParseRole(role string) NodeRole {
	if role == "" {
		return RoleStandalone
	}
	r := NodeRole(role)
	if r.IsValid() {
		return r
	}
	return RoleStandalone
}

// AllRoles returns all valid node roles.
func AllRoles() []NodeRole {
	return []NodeRole{RoleStandalone, RoleWriter, RoleReader, RoleCompactor}
}
