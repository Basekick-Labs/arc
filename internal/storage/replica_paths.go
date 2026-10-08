package storage

import "strings"

// Private replication files are addressed explicitly by the handoff service.
// Ordinary storage enumeration must not treat them as independent user data
// or offer them to orphan cleanup. Read/Write still accept their exact keys.
func isReplicaPrivateKey(key string) bool {
	first, _, _ := strings.Cut(key, "/")
	return first == ".replica" || first == ".replica-pins"
}
