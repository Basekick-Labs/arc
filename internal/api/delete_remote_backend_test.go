package api

import (
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// objectStoreOf is a backend that reports an object-store type the DELETE
// handler has no concrete case for, the shape every new backend has on the
// day it is added (#1105).
type objectStoreOf struct {
	*storage.LocalBackend
	typ string
}

func (o objectStoreOf) Type() string { return o.typ }

// Every backend that is not local takes the remote rewrite. Before #1105 the
// predicate listed *S3Backend and *AzureBlobBackend, so any other object store
// took rewriteLocalFile: the object URL became a relative local directory, the
// surviving rows were written there, and the original object was deleted from
// the bucket.
func TestDeleteRewritesEveryNonLocalBackendRemotely(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name    string
		backend storage.Backend
		remote  bool
	}{
		{"local", local, false},
		// A wrapper that forwards Type() is still local: the concrete type is
		// not the test, the reported type is.
		{"wrapped local", objectStoreOf{local, "local"}, false},
		{"gcs", objectStoreOf{local, "gcs"}, true},
		{"an unknown object store", objectStoreOf{local, "x-future"}, true},
		{"s3", objectStoreOf{local, "s3"}, true},
		{"azure", objectStoreOf{local, "azure"}, true},
		{"no backend", nil, false},
	} {
		h := &DeleteHandler{storage: c.backend}
		if got := h.isRemoteBackend(); got != c.remote {
			t.Errorf("%s: isRemoteBackend = %v, want %v", c.name, got, c.remote)
		}
		q := &QueryHandler{storage: c.backend}
		if got := q.readsObjectStore(); got != c.remote {
			t.Errorf("%s: readsObjectStore = %v, want %v", c.name, got, c.remote)
		}
	}
}
