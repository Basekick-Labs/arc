package cluster

import (
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
)

// #1105 part 4: a GCS backend is shared object storage for every cluster
// path that asks (staged compaction, the finisher, manifest repair, pending
// registrations, reconciliation's BackendShared). This rests on GCSBackend
// implementing ObjectCopier and reporting Type() "gcs".
func TestGCSIsSharedObjectStore(t *testing.T) {
	if !IsSharedObjectStore(&storage.GCSBackend{}) {
		t.Fatal("a GCS backend must be shared object storage in cluster mode")
	}
	if (&storage.GCSBackend{}).Type() != "gcs" {
		t.Fatal("cluster paths branch on Type(); GCS must report \"gcs\"")
	}
}
