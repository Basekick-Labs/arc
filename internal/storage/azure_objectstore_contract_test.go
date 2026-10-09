//go:build objectstore

// The Azure half of the key contract, verified against a real storage account.
//
// Tagged `objectstore` alongside objectstore_contract_test.go, which does the
// same for S3 against SeaweedFS. Run with:
//
//	ARC_TEST_AZURE_ACCOUNT=<account> \
//	ARC_TEST_AZURE_KEY=<key> \
//	ARC_TEST_AZURE_CONTAINER=<container> \
//	go test -tags='duckdb_arrow objectstore' ./internal/storage/
//
// The container must already exist: writing to a missing container stores
// nothing and reports success on some paths, which once made an A/B comparison
// look identical when neither half had written anything.
package storage

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// azureBackend builds a backend against the account named by the environment,
// rooted at prefix (pass "" for the container root).
//
// It SKIPS when the three variables are absent, and that is deliberate rather
// than the anti-pattern B1 removed. B1's finding was about tests that
// hard-failed because of the HOST's ambient AWS configuration, which a test
// should isolate itself from rather than depend on; an Azure storage account is
// not ambient — it is paid infrastructure that genuinely does not exist on most
// machines, including CI. There is nothing to isolate and nothing to stand in:
// skipping is the honest answer. Do not "fix" this into a hard failure.
//
// The account key is read from the environment and never written down: not
// logged, not in an error message, not in a t.Logf. If this helper ever needs
// to report an authentication failure, report that it failed, not with what.
func azureBackend(t *testing.T, prefix string) *AzureBlobBackend {
	t.Helper()
	account := os.Getenv("ARC_TEST_AZURE_ACCOUNT")
	key := os.Getenv("ARC_TEST_AZURE_KEY")
	container := os.Getenv("ARC_TEST_AZURE_CONTAINER")
	if account == "" || key == "" || container == "" {
		t.Skip("ARC_TEST_AZURE_ACCOUNT / ARC_TEST_AZURE_KEY / ARC_TEST_AZURE_CONTAINER not set")
	}
	b, err := NewAzureBlobBackend(&AzureBlobConfig{
		AccountName:   account,
		AccountKey:    key,
		ContainerName: container,
		Prefix:        prefix,
	}, zerolog.Nop())
	if err != nil {
		// No %v on err: an Azure SDK authentication error can echo request
		// headers. Name the account and the container, never the credential.
		t.Fatalf("could not build an Azure backend for account %q container %q", account, container)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// A full round trip through a prefixed backend, and the proof that nothing
// lands outside the prefix.
//
// The last assertion is the one a unit test cannot make: it lists the
// CONTAINER ROOT through a second, unprefixed backend and checks that every
// object this test wrote is under the prefix. A backend that prefixed its
// writes but not its listings, or the reverse, passes every same-backend
// round trip and fails here.
func TestAzurePrefixRoundTrip(t *testing.T) {
	const prefix = "arc-contract-prefix"
	b := azureBackend(t, prefix)
	root := azureBackend(t, "")
	ctx := context.Background()

	keys := []string{
		"contractdb/cpu/2026/10/06/14/a.parquet",
		"contractdb/cpu/2026/10/06/14/b.parquet",
		"contractdb/mem/2026/10/06/15/c.parquet",
	}
	payload := func(k string) []byte { return []byte("payload-" + k) }

	defer func() {
		for _, k := range keys {
			_ = b.Delete(ctx, k)
		}
	}()

	for _, k := range keys {
		if err := b.WriteReader(ctx, k, bytes.NewReader(payload(k)), int64(len(payload(k)))); err != nil {
			t.Fatalf("WriteReader(%s): %v", k, err)
		}
	}

	// Read, Stat, Exists all address the same blob the write created.
	for _, k := range keys {
		got, err := b.Read(ctx, k)
		if err != nil {
			t.Fatalf("Read(%s): %v", k, err)
		}
		if !bytes.Equal(got, payload(k)) {
			t.Errorf("Read(%s) = %q, want %q", k, got, payload(k))
		}
		size, err := b.StatFile(ctx, k)
		if err != nil {
			t.Fatalf("StatFile(%s): %v", k, err)
		}
		if size != int64(len(payload(k))) {
			t.Errorf("StatFile(%s) = %d, want %d", k, size, len(payload(k)))
		}
		ok, err := b.Exists(ctx, k)
		if err != nil || !ok {
			t.Errorf("Exists(%s) = %v, %v", k, ok, err)
		}
	}

	// A listing returns UNPREFIXED keys, so its output feeds straight back
	// into Read.
	listed, err := b.List(ctx, "contractdb/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	seen := map[string]bool{}
	for _, k := range listed {
		if strings.HasPrefix(k, prefix) {
			t.Errorf("List returned %q with the backend prefix still attached", k)
		}
		seen[k] = true
	}
	for _, k := range keys {
		if !seen[k] {
			t.Errorf("List did not return %q; it returned %v", k, listed)
		}
	}

	// ListObjects and ListDirectories agree with it.
	objs, err := b.ListObjects(ctx, "contractdb/")
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	for _, o := range objs {
		if strings.HasPrefix(o.Path, prefix) {
			t.Errorf("ListObjects returned %q with the backend prefix still attached", o.Path)
		}
	}
	dirs, err := b.ListDirectories(ctx, "contractdb")
	if err != nil {
		t.Fatalf("ListDirectories: %v", err)
	}
	wantDirs := map[string]bool{"cpu": true, "mem": true}
	for _, d := range dirs {
		delete(wantDirs, d)
		if strings.Contains(d, "/") || strings.HasPrefix(d, prefix) {
			t.Errorf("ListDirectories returned %q; it must return a bare directory name", d)
		}
	}
	if len(wantDirs) != 0 {
		t.Errorf("ListDirectories missed %v; it returned %v", wantDirs, dirs)
	}

	if has, err := b.HasObjectsUnderPrefix(ctx, "contractdb/"); err != nil || !has {
		t.Errorf("HasObjectsUnderPrefix = %v, %v; want true", has, err)
	}

	// Nothing landed outside the prefix. Checked from the container root, so
	// a backend that prefixed one direction only fails here.
	fromRoot, err := root.ListObjects(ctx, "")
	if err != nil {
		t.Fatalf("ListObjects from the container root: %v", err)
	}
	for _, k := range keys {
		if containsPath(fromRoot, k) {
			t.Errorf("%q exists at the CONTAINER ROOT; the write escaped the prefix", k)
		}
		if !containsPath(fromRoot, prefix+"/"+k) {
			t.Errorf("%q does not exist at %q; the write did not land under the prefix", k, prefix+"/"+k)
		}
	}

	// Delete addresses the prefixed blob too, in both the single and the batch
	// form. The batch form is the one whose keys never pass through
	// prefixedKey (#1102).
	if err := b.Delete(ctx, keys[0]); err != nil {
		t.Fatalf("Delete(%s): %v", keys[0], err)
	}
	if ok, err := b.Exists(ctx, keys[0]); err != nil || ok {
		t.Errorf("after Delete, Exists(%s) = %v, %v", keys[0], ok, err)
	}
	if err := b.DeleteBatch(ctx, keys[1:]); err != nil {
		t.Fatalf("DeleteBatch: %v", err)
	}
	for _, k := range keys[1:] {
		if ok, err := b.Exists(ctx, k); err != nil || ok {
			t.Errorf("after DeleteBatch, Exists(%s) = %v, %v", k, ok, err)
		}
	}
	// And the batch did not leave the blobs sitting at the container root
	// under their unprefixed names, which is what a validate-without-prefix
	// batch would have deleted instead.
	fromRoot, err = root.ListObjects(ctx, prefix+"/")
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	for _, k := range keys {
		if containsPath(fromRoot, prefix+"/"+k) {
			t.Errorf("%q survived the delete under the prefix", prefix+"/"+k)
		}
	}
}

// An empty prefix must leave the blob name byte-identical to the key, which is
// the pre-#1102 behaviour every existing Azure deployment depends on.
func TestAzureNoPrefixAddressesTheBareKey(t *testing.T) {
	b := azureBackend(t, "")
	ctx := context.Background()
	key := fmt.Sprintf("contractdb/noprefix/2026/10/06/14/%d.parquet", os.Getpid())
	defer func() { _ = b.Delete(ctx, key) }()

	if err := b.Write(ctx, key, []byte("x")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	objs, err := b.ListObjects(ctx, "contractdb/noprefix/")
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if !containsPath(objs, key) {
		t.Errorf("ListObjects did not return %q; it returned %v", key, objs)
	}
}

func containsPath(objs []ObjectInfo, path string) bool {
	for _, o := range objs {
		if o.Path == path {
			return true
		}
	}
	return false
}
