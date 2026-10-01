package compaction

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
)

func parkedCount() int64 {
	return metrics.Get().Snapshot()["compaction_manifests_parked_unparseable_total"].(int64)
}

// refuseQuarantineWrites makes every park fail at the copy step while the
// original stays in place.
type refuseQuarantineWrites struct {
	storage.Backend
}

func (b refuseQuarantineWrites) Write(ctx context.Context, path string, data []byte) error {
	if strings.HasSuffix(path, ManifestQuarantineSuffix) {
		return errors.New("simulated: quarantine write refused")
	}
	return b.Backend.Write(ctx, path, data)
}

// The park counter moves once per successful park and not at all for a
// park that failed and left the manifest for the next cycle (#926).
func TestParkedUnparseableManifestCounterIssue926(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	ctx := context.Background()
	for _, path := range []string{ManifestBasePath + "/hourly/db/empty.json", ManifestBasePath + "/daily/db/garbage.json"} {
		body := []byte(nil)
		if strings.Contains(path, "garbage") {
			body = []byte("{corrupt")
		}
		if err := b.Write(ctx, path, body); err != nil {
			t.Fatal(err)
		}
	}
	// A park that cannot complete counts nothing and keeps the manifest.
	before := parkedCount()
	refusing := NewManifestManager(refuseQuarantineWrites{b}, zerolog.Nop())
	if _, err := refusing.recoverOrphanedManifests(ctx, recoveryScope{}, nil, nil); err == nil {
		t.Fatal("a refused park must be reported")
	}
	if got := parkedCount(); got != before {
		t.Fatalf("failed park counted: %d -> %d", before, got)
	}
	if ok, _ := b.Exists(ctx, ManifestBasePath+"/hourly/db/empty.json"); !ok {
		t.Fatal("manifest must survive a failed park")
	}
	// Successful parks count exactly once each; a second pass finds nothing
	// left to park and counts nothing.
	if _, err := m.ManifestManager.recoverOrphanedManifests(ctx, recoveryScope{}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if got := parkedCount(); got != before+2 {
		t.Fatalf("two parks must count two: %d -> %d", before, got)
	}
	if _, err := m.ManifestManager.recoverOrphanedManifests(ctx, recoveryScope{}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if got := parkedCount(); got != before+2 {
		t.Fatalf("nothing to park must count nothing: %d", got)
	}
}
