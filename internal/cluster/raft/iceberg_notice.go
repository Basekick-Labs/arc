package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Iceberg path notices (#1728).
//
// A cluster's writer removes cold-tier objects (retention and a cold DELETE
// on the primary writer; the migrator dropping a cold copy on any writer
// that runs tiering). The Iceberg export runs on
// another node, lists the cold tier itself and caches that listing, and
// nothing else tells it: tier rows are node-local. The writer therefore
// proposes CommandIcebergPathsChanged after the objects are gone, naming
// the measurement directories it touched, and every node hands them to its
// export, which drops its cached cold listing and reconciles them.
//
// The command changes no state: Apply only validates the payload and calls
// the observer, so it is deterministic, snapshots are unaffected, and an
// older build that does not know it answers "unknown command type" without
// diverging. Proposers still wait for CapabilityIcebergNotice on every node
// (a precaution: no node should log an apply error for it). A log replayed
// at restart mostly applies before the export wires its observer, so old
// notices are dropped; one that does arrive asks for a reconcile of a known
// measurement, which is fingerprint-gated.

// CapabilityIcebergNotice is advertised by builds that apply
// CommandIcebergPathsChanged.
const CapabilityIcebergNotice = "iceberg_notice_v1"

// Bounds of one notice: keys, each key's length, and the payload's total key
// bytes (a Raft entry stays small).
const (
	MaxIcebergNoticeKeys     = 1000
	MaxIcebergNoticeKeyLen   = 1024
	MaxIcebergNoticeKeyBytes = 64 << 10
)

// IcebergPathsChangedPayload names storage directories ("db/m/", or
// "spoke/db/m/" on an edge-sync hub) whose cold objects were removed.
type IcebergPathsChangedPayload struct {
	Keys []string `json:"keys"`
}

// ValidIcebergNoticeKey reports whether key is a relative directory key the
// export can map: slash-separated, ending in "/", no leading "/", no empty,
// "." or ".." segment, no backslash or NUL.
func ValidIcebergNoticeKey(key string) bool {
	if key == "" || len(key) > MaxIcebergNoticeKeyLen || !strings.HasSuffix(key, "/") ||
		strings.HasPrefix(key, "/") || strings.ContainsAny(key, "\\\x00") {
		return false
	}
	for _, seg := range strings.Split(strings.TrimSuffix(key, "/"), "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

// ValidateIcebergPathsChanged checks a notice the way Apply does.
func ValidateIcebergPathsChanged(p IcebergPathsChangedPayload) error {
	if len(p.Keys) == 0 || len(p.Keys) > MaxIcebergNoticeKeys {
		return fmt.Errorf("iceberg notice: %d keys (1..%d)", len(p.Keys), MaxIcebergNoticeKeys)
	}
	total := 0
	for _, k := range p.Keys {
		if !ValidIcebergNoticeKey(k) {
			return errors.New("iceberg notice: invalid key")
		}
		total += len(k)
	}
	if total > MaxIcebergNoticeKeyBytes {
		return fmt.Errorf("iceberg notice: %d key bytes (max %d)", total, MaxIcebergNoticeKeyBytes)
	}
	return nil
}

// SetIcebergPathsCallback sets the observer of applied notices; nil clears it.
// It must not block: it runs on the apply path.
func (f *ClusterFSM) SetIcebergPathsCallback(fn func(keys []string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onIcebergPaths = fn
}

// applyIcebergPathsChanged validates the notice and calls the observer. No
// state changes, valid or not.
func (f *ClusterFSM) applyIcebergPathsChanged(payload []byte) interface{} {
	var p IcebergPathsChangedPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal iceberg notice: %w", err)
	}
	if err := ValidateIcebergPathsChanged(p); err != nil {
		return err
	}
	f.mu.RLock()
	fn := f.onIcebergPaths
	f.mu.RUnlock()
	if fn != nil {
		fn(p.Keys)
	}
	return nil
}

// Iceberg database-drop notices (#1731).
//
// A database DELETE runs on the primary writer, which drops its own Iceberg
// tables. With node-local storage every node has its own warehouse and
// catalog, and the node that exports (or exported before a lease move) keeps
// tables naming the deleted files. After a DELETE completes (hot files, cold
// objects and its own catalog), the writer proposes
// CommandIcebergDatabaseDropped; Raft orders it after every manifest delete
// of the database, and each node drops its own tables once its delete workers
// have unlinked that database's files. Like CommandIcebergPathsChanged it
// changes no state: Apply validates and calls the observer.

// CapabilityIcebergDatabaseDropped is advertised by builds that apply
// CommandIcebergDatabaseDropped.
const CapabilityIcebergDatabaseDropped = "iceberg_db_dropped_v1"

// IcebergDatabaseDroppedPayload names the deleted database: one path segment
// (a plain database, or a spoke on an edge-sync hub).
type IcebergDatabaseDroppedPayload struct {
	Database string `json:"database"`
}

// ValidateIcebergDatabaseDropped checks a drop notice the way Apply does: one
// non-empty segment, no separator, NUL, "." or "..".
func ValidateIcebergDatabaseDropped(p IcebergDatabaseDroppedPayload) error {
	d := p.Database
	if d == "" || len(d) > MaxIcebergNoticeKeyLen || d == "." || d == ".." || strings.ContainsAny(d, "/\\\x00") {
		return errors.New("iceberg database-drop notice: invalid database name")
	}
	return nil
}

// SetIcebergDatabaseDroppedCallback sets the observer of applied drop
// notices; nil clears it. It must not block: it runs on the apply path.
func (f *ClusterFSM) SetIcebergDatabaseDroppedCallback(fn func(database string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onIcebergDatabaseDropped = fn
}

func (f *ClusterFSM) applyIcebergDatabaseDropped(payload []byte) interface{} {
	var p IcebergDatabaseDroppedPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal iceberg database-drop notice: %w", err)
	}
	if err := ValidateIcebergDatabaseDropped(p); err != nil {
		return err
	}
	f.mu.RLock()
	fn := f.onIcebergDatabaseDropped
	f.mu.RUnlock()
	if fn != nil {
		fn(p.Database)
	}
	return nil
}
