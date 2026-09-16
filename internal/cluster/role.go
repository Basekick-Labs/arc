package cluster

import "strings"

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

// VotesInElections reports whether a node with this role should be a Raft
// VOTER rather than a non-voting member.
//
// The rule is: a node votes iff it can ingest. Writers and standalone nodes
// vote; readers and compactors replicate the log and serve their own work but
// never campaign (#862).
//
// Why CanIngest and not CanCoordinate, which sounds like the right field:
// CanCoordinate is false for RoleStandalone, and standalone is the DEFAULT
// cluster.role. A CanCoordinate predicate would leave a default-configured
// cluster with zero voters, which hashicorp/raft rejects outright.
//
// Call this on a ParseRole-normalised role, never on a raw string cast. The
// capabilities table's default branch returns the zero value, so an unknown
// role would NOT vote, while ParseRole maps the same input to standalone,
// which does. They disagree on exactly the input a migration meets — a record
// written before role validation — and the safe answer there is to keep the
// vote, not to silently strip it.
func (r NodeRole) VotesInElections() bool {
	return r.GetCapabilities().CanIngest
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

// ParseRoleStrict parses a role string and reports whether it was recognised.
//
// It is ParseRole without the fallback. ParseRole answers "what role should
// this node have", and standalone is the right answer for an unset value; that
// makes it wrong for the two callers that need to answer "is this a role at
// all", because a typo and a deliberate standalone become the same node. An
// empty string is not recognised here either: the caller that wants the
// unset-means-standalone default has to say so.
func ParseRoleStrict(role string) (NodeRole, bool) {
	r := NodeRole(role)
	if !r.IsValid() {
		return "", false
	}
	return r, true
}

// RoleNames renders the valid roles for an operator-facing error message, so
// the set is spelled once rather than in every message that lists it.
func RoleNames() string {
	names := make([]string, 0, len(AllRoles()))
	for _, r := range AllRoles() {
		names = append(names, string(r))
	}
	return strings.Join(names, ", ")
}

// AllRoles returns all valid node roles.
func AllRoles() []NodeRole {
	return []NodeRole{RoleStandalone, RoleWriter, RoleReader, RoleCompactor}
}
