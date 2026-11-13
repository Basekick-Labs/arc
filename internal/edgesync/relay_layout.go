package edgesync

// Relay bucket layout (edge sync 3d, #1685). Keys are relative to the relay
// backend, whose own prefix (s3_prefix, azure_prefix, or local_path) is the
// relay's root: there is no second prefix to compose with it, so a key is
// built in exactly one place.
//
//	<spoke_id>/outbox/<bundle_id>/manifest.json   spoke writes, hub reads
//	<spoke_id>/outbox/<bundle_id>/entries.jsonl
//	<spoke_id>/outbox/<bundle_id>/data/<path>
//	<spoke_id>/acks/<bundle_id>.json              hub writes, spoke reads
//	<spoke_id>/rejected/<bundle_id>.json          hub writes, spoke reads
//
// A bundle is complete only once its manifest exists: the writer puts it
// last. Every function here takes IDs the caller has validated
// (validateSpokeID, ValidateBundleID); they are joined, not sanitized.
const (
	relayOutbox   = "outbox"
	relayAcks     = "acks"
	relayRejected = "rejected"
)

// RelaySpokePrefix is one spoke's root in the relay bucket, with a trailing
// separator.
func RelaySpokePrefix(spokeID string) string { return spokeID + "/" }

// RelayOutboxDir is the prefix listing a spoke's outbox bundles.
func RelayOutboxDir(spokeID string) string { return spokeID + "/" + relayOutbox + "/" }

// RelayOutboxPrefix is one bundle's prefix in a spoke's outbox, with a
// trailing separator, so a prefix match cannot reach a sibling bundle.
func RelayOutboxPrefix(spokeID, bundleID string) string {
	return RelayOutboxDir(spokeID) + bundleID + "/"
}

// RelayAckDir is the prefix listing a spoke's acknowledgments.
func RelayAckDir(spokeID string) string { return spokeID + "/" + relayAcks + "/" }

// RelayAckKey is one bundle's acknowledgment object.
func RelayAckKey(spokeID, bundleID string) string { return RelayAckDir(spokeID) + bundleID + ".json" }

// RelayRejectedDir is the prefix listing a spoke's refused bundles.
func RelayRejectedDir(spokeID string) string { return spokeID + "/" + relayRejected + "/" }

// RelayRejectedKey is the hub's refusal note for one bundle.
func RelayRejectedKey(spokeID, bundleID string) string {
	return RelayRejectedDir(spokeID) + bundleID + ".json"
}
