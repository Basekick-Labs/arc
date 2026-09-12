package storage_test

// One corpus, every path validator in the tree, asserted by DIRECTION.
//
// #746 was filed because there were several spellings of "is this path safe"
// and they disagreed with each other. Collapsing them into one function is not
// possible: raft.ValidateManifestPath runs inside FSM Apply including log
// REPLAY, so tightening it would make a node reject an entry an older binary
// accepted and two versions would build different state from one log (#743).
//
// So the property that can be asserted is not equality but ORDERING. Each
// validator either accepts a superset of the contract or a subset of it, and
// which one it is follows from what the validator is for. An exception LIST
// cannot express this: raft is looser on six independent axes, so the list
// degenerates into an allowlist of the inputs someone happened to think of.
//
// This test lives in storage_test (not storage) because it imports the cluster
// and api packages, which import storage.

import (
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/storage"
)

// pathCorpus is shared by every direction check below. It is deliberately
// weighted toward inputs the contract ACCEPTS: a corpus that is mostly
// rejections asserts almost nothing about a superset relation (#741, lesson 5).
func pathCorpus() []string {
	c := []string{
		// Accepted by the contract.
		"db/cpu/2026/09/12/13/f.parquet",
		"db/cpu", "db", "a/b/c", "single",
		"db/a..b/f.parquet", "db/..foo/f.parquet", "db/.hidden/f.parquet",
		"a.b", "a..b", "..foo", ".hidden", "a-b_c.parquet",
		"db/cpu/2026/09/12/13/f.part",
		strings.Repeat("a", 255), "db/" + strings.Repeat("a", 255),
		"a*", "db/a*/f.parquet", "a?", "a[0-9]", "a{1,2}",
		"C:/x", "mailto:foo",

		// Rejected by the contract.
		"", "/", "/db/cpu", "db/cpu/", "db//cpu", "db/./cpu", "db/../cpu",
		".", "..", "db\\cpu", "db/cpu\x00", "s3://bucket/x",
		strings.Repeat("a", 256), strings.Repeat("ab/", 400) + "f",
	}
	return c
}

// TestManifestValidatorAcceptsSupersetOfContract.
//
// Direction: raft.ValidateManifestPath must accept everything the key contract
// accepts. If it rejected a contract-valid path, a writer could store a file
// the FSM refuses to register, and that file would be invisible to the cluster.
//
// The converse is NOT asserted, and that is the documented design: raft is
// looser on purpose (it runs inside FSM Apply including log replay), and the
// gap is closed at the consumer instead (#747).
//
// ONE axis breaks the superset relation, and it is a real divergence rather
// than a corpus artifact: raft rejects any ":" outside a Windows drive prefix,
// as an anti-worm-primitive guard (a scheme in a manifest path is the
// "s3://attacker-bucket/..." shape), while the key contract has no colon rule.
// It is reachable: edgesync.validateSpokeID accepts "rocket:01", a spoke ID is
// the first segment of everything that spoke writes, and storage accepts those
// keys. Such a file is stored and then cannot be registered in the manifest.
// Tracked separately; this test pins the divergence so it cannot widen
// silently, and asserts the superset relation holds everywhere else.
func TestManifestValidatorAcceptsSupersetOfContract(t *testing.T) {
	checked := 0
	for _, p := range pathCorpus() {
		if storage.ValidateKey(p) != nil {
			continue
		}
		err := raft.ValidateManifestPath(p)
		if strings.Contains(p, ":") {
			// The known divergence. Assert it still IS one, so that closing it
			// elsewhere shows up here as a failing expectation rather than as
			// silently dead code.
			if err == nil {
				t.Errorf("path %q now passes the manifest validator; the colon divergence is closed, update this test", p)
			}
			continue
		}
		checked++
		if err != nil {
			t.Errorf("path %q is a valid storage key but the manifest validator rejects it: %v", p, err)
		}
	}
	if checked < 15 {
		t.Fatalf("only %d contract-valid paths in the corpus; it no longer exercises the relation", checked)
	}
}

// TestListPrefixIsLooserThanKeyOnlyWhereDocumented.
//
// ValidateListPrefix must accept every key (a key is a usable prefix), and may
// additionally accept exactly the three shapes that cannot name an object: "",
// one trailing separator, and the reserved write-staging suffix. Anything else
// it accepts and ValidateKey rejects is a divergence, not a relaxation.
//
// The third is the reason this test earns its keep: #744 reserved PartSuffix in
// ValidateKey and left this function's doc claiming it was looser "in exactly
// two ways", which had silently stopped being true.
func TestListPrefixIsLooserThanKeyOnlyWhereDocumented(t *testing.T) {
	documentedRelaxation := func(p string) bool {
		switch {
		case p == "":
			return true
		case strings.HasSuffix(p, "/") && storage.ValidateKey(strings.TrimSuffix(p, "/")) == nil:
			return true
		case strings.HasSuffix(p, storage.PartSuffix):
			return true
		}
		return false
	}

	for _, p := range pathCorpus() {
		keyOK := storage.ValidateKey(p) == nil
		prefixOK := storage.ValidateListPrefix(p) == nil

		if keyOK && !prefixOK {
			t.Errorf("%q is a valid key but not a valid list prefix", p)
		}
		if prefixOK && !keyOK && !documentedRelaxation(p) {
			t.Errorf("%q is accepted as a list prefix but rejected as a key, which is not a documented relaxation", p)
		}
	}
}

// TestKeySegmentAgreesWithWholeKeyValidation.
//
// ValidateKeySegment is factored out of the whole-key walk, so the two must
// agree by construction: a key made of accepted segments is accepted, and a key
// containing a rejected segment is rejected. This is what stops the segment
// rule from drifting into a sixth spelling.
func TestKeySegmentAgreesWithWholeKeyValidation(t *testing.T) {
	segments := []string{
		"db", "cpu", "a..b", "..foo", ".hidden", "a.b", "f.parquet", "a*",
		"", ".", "..", "a/b", "a\\b", "a\x00b",
		strings.Repeat("a", 255), strings.Repeat("a", 256),
	}
	for _, seg := range segments {
		segOK := storage.ValidateKeySegment(seg) == nil
		keyOK := storage.ValidateKey("db/"+seg+"/f.parquet") == nil

		// The one legitimate disagreement: a segment containing "/" is not one
		// segment, but joining it still yields a well-formed key.
		if strings.Contains(seg, "/") {
			if segOK {
				t.Errorf("segment %q contains a separator and must be rejected", seg)
			}
			continue
		}
		if segOK != keyOK {
			t.Errorf("segment %q: ValidateKeySegment=%v but ValidateKey of a key containing it=%v", seg, segOK, keyOK)
		}
	}
}

// TestGlobSafetyIsOrthogonalToTheKeyContract.
//
// ValidateGlobSafe is deliberately NOT part of the contract, and this pins the
// distinction that #746 turns on: a glob metacharacter is a perfectly good key
// (one object) and an unacceptable read path (a pattern over many).
func TestGlobSafetyIsOrthogonalToTheKeyContract(t *testing.T) {
	for _, s := range []string{"a*", "a?", "a[0-9]", "a{1,2}"} {
		if err := storage.ValidateKey("db/" + s); err != nil {
			t.Errorf("the key contract must keep accepting %q for writes: %v", s, err)
		}
		if err := storage.ValidateGlobSafe(s); err == nil {
			t.Errorf("%q must not be glob-safe", s)
		}
	}
	for _, s := range []string{"db/cpu", "a..b", ".hidden", "f.parquet"} {
		if err := storage.ValidateGlobSafe(s); err != nil {
			t.Errorf("%q is an ordinary name and must be glob-safe: %v", s, err)
		}
	}
}
