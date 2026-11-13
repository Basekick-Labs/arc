//go:build objectstore

package storage

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// The conditional Put Blob (If-None-Match: *) against a real Blob API: the
// second writer gets created=false and the blob keeps the first body. Runs
// against Azurite when ARC_TEST_AZURITE_ENDPOINT is set (the well-known
// development account), else against the account the contract tests use.
func TestAzureWriteIfAbsent(t *testing.T) {
	prefix := fmt.Sprintf("wia-%d", time.Now().UnixNano())
	var b *AzureBlobBackend
	if ep := os.Getenv("ARC_TEST_AZURITE_ENDPOINT"); ep != "" {
		container := os.Getenv("ARC_TEST_AZURE_CONTAINER")
		if container == "" {
			container = "arc-test"
		}
		var err error
		b, err = NewAzureBlobBackend(&AzureBlobConfig{
			AccountName: "devstoreaccount1", AccountKey: azuriteAccountKey,
			ContainerName: container, Endpoint: ep, Prefix: prefix,
		}, zerolog.Nop())
		if err != nil {
			t.Fatalf("azurite backend: %v", err)
		}
		t.Cleanup(func() { _ = b.Close() })
	} else {
		b = azureBackend(t, prefix)
	}
	ctx := context.Background()
	key := "relay/spoke-a/acks/x.json"
	t.Cleanup(func() { _ = b.Delete(ctx, key) })
	var _ ConditionalWriter = b

	created, err := b.WriteIfAbsent(ctx, key, []byte("first"))
	if err != nil || !created {
		t.Fatalf("first write: created=%v err=%v", created, err)
	}
	created, err = b.WriteIfAbsent(ctx, key, []byte("second"))
	if err != nil || created {
		t.Fatalf("second write: created=%v err=%v, want false and nil", created, err)
	}
	got, err := b.Read(ctx, key)
	if err != nil || string(got) != "first" {
		t.Fatalf("read back %q err=%v, want the first body", got, err)
	}
}
