package storage

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// The key arithmetic of the Azure prefix (#1102), proven without credentials.
//
// The live half is azure_objectstore_contract_test.go, which needs a real
// account. This half is what CI without Azure can still prove: that the
// mapping between a caller's key and the blob name is the one S3 already has,
// and that an empty prefix leaves every key byte-identical to the pre-#1102
// behaviour.
func TestAzureBackendPrefixedKey(t *testing.T) {
	backend := &AzureBlobBackend{containerName: "c", prefix: "instances/abc123/"}

	const key = "mydb/cpu/2025/01/file.parquet"
	got, err := backend.prefixedKey(key)
	if err != nil {
		t.Fatalf("prefixedKey(%q) = %v", key, err)
	}
	if want := "instances/abc123/" + key; got != want {
		t.Errorf("prefixedKey(%q) = %q, want %q", key, got, want)
	}

	// "" is not a key. It named the prefix directory itself, which is an
	// object nothing can address (#743). It remains valid as a LIST prefix.
	if _, err := backend.prefixedKey(""); err == nil {
		t.Error(`prefixedKey("") was accepted; an empty key names no object`)
	}
	if got, err := backend.prefixedListPrefix(""); err != nil || got != "instances/abc123/" {
		t.Errorf(`prefixedListPrefix("") = %q, %v; want the configured prefix`, got, err)
	}
	if got, err := backend.prefixedListPrefix("mydb/"); err != nil || got != "instances/abc123/mydb/" {
		t.Errorf(`prefixedListPrefix("mydb/") = %q, %v`, got, err)
	}

	// Azure treats a backslash as a path separator, so the contract must still
	// refuse one even though the prefix now sits in front of it.
	if _, err := backend.prefixedKey(`mydb\cpu/x.parquet`); err == nil {
		t.Error("prefixedKey accepted a backslash; on Azure that IS a separator")
	}

	// No prefix configured: byte-identical to what blobKey returned before.
	noPrefix := &AzureBlobBackend{containerName: "c"}
	got, err = noPrefix.prefixedKey(key)
	if err != nil {
		t.Fatalf("prefixedKey: %v", err)
	}
	if got != key {
		t.Errorf("prefixedKey with no prefix = %q, want the key unchanged (%q)", got, key)
	}
	if got, err := noPrefix.prefixedListPrefix(""); err != nil || got != "" {
		t.Errorf(`prefixedListPrefix("") with no prefix = %q, %v; want ""`, got, err)
	}
}

// NOTE: there is deliberately no unit test here for the STRIP direction.
// One existed and it was worthless: it re-implemented
// strings.TrimPrefix(name, b.prefix) in the test body and compared that
// against prefixedKey, so deleting every strip in azure.go left it passing.
// The strip is now covered where it can actually fail, through the real
// methods against a canned endpoint: see azure_listing_offline_test.go.

// A prefix that cannot form usable keys must stop the backend from being
// built. These are the four shapes the old repairing sanitiser turned into
// something that either broke every write or silently relocated the
// deployment to the container root; the S3 path has refused them since #746
// and the Azure path must refuse the same set.
//
// Construction fails before any client is built or any request is made, so
// this needs no credentials and no network.
func TestNewAzureBlobBackendRefusesAnUnusablePrefix(t *testing.T) {
	for _, prefix := range []string{"/", "a//b", ".", "a b", `a\b`, "a/../b"} {
		b, err := NewAzureBlobBackend(&AzureBlobConfig{
			ContainerName: "c",
			Prefix:        prefix,
			// Deliberately no credentials: the prefix check runs first, so a
			// reachable account is not needed and the auth switch is never
			// reached. If the check ever moves after it, this test fails with
			// the authentication error instead and says so.
		}, zerolog.Nop())
		if err == nil {
			_ = b.Close()
			t.Errorf("prefix %q was accepted", prefix)
			continue
		}
		if !strings.Contains(err.Error(), "storage prefix") {
			t.Errorf("prefix %q failed with %v; want the prefix validator to be the one that refused it", prefix, err)
		}
	}
}

// Every field ConfigJSON emits must be parsed by the compaction subprocess.
// This pins the emitting half; internal/compaction owns the round trip.
func TestAzureBackendConfigJSONCarriesThePrefix(t *testing.T) {
	b := &AzureBlobBackend{containerName: "cont", prefix: "arc/", accountName: "acct", endpoint: "https://acct.blob.core.windows.net"}

	var got map[string]any
	if err := json.Unmarshal([]byte(b.ConfigJSON()), &got); err != nil {
		t.Fatalf("ConfigJSON is not valid JSON: %v", err)
	}
	if got["prefix"] != "arc/" {
		t.Errorf("ConfigJSON prefix = %v, want %q; the subprocess reconstructs the backend from this", got["prefix"], "arc/")
	}
	if got["container"] != "cont" {
		t.Errorf("ConfigJSON container = %v", got["container"])
	}
	// It must stay credential-free: the subprocess reads the account key from
	// the environment the parent sets.
	for _, k := range []string{"account_key", "connection_string", "sas_token"} {
		if _, ok := got[k]; ok {
			t.Errorf("ConfigJSON emits %q; it is serialised into a job config and must carry no credentials", k)
		}
	}

	if b.GetPrefix() != "arc/" {
		t.Errorf("GetPrefix() = %q, want %q", b.GetPrefix(), "arc/")
	}
}

// backendRoot feeds ObjectURI and GetStoragePath, which are what DuckDB's
// read_parquet consumes. An Azure primary with a prefix that reads from the
// container root reads a location nothing was written to.
func TestAzureBackendRootCarriesThePrefix(t *testing.T) {
	const key = "db/cpu/2026/09/12/13/f.parquet"
	for _, tc := range []struct{ prefix, want string }{
		{"", "azure://c/"},
		{"arc/", "azure://c/arc/"},
		{"a/b/", "azure://c/a/b/"},
	} {
		b := &AzureBlobBackend{containerName: "c", prefix: tc.prefix}
		if got := backendRoot(b); got != tc.want {
			t.Errorf("backendRoot(prefix=%q) = %q, want %q", tc.prefix, got, tc.want)
		}
		uri, err := ObjectURI(b, key)
		if err != nil {
			t.Fatalf("ObjectURI: %v", err)
		}
		if want := tc.want + key; uri != want {
			t.Errorf("ObjectURI(prefix=%q) = %q, want %q", tc.prefix, uri, want)
		}
		glob, err := GetStoragePath(b, "db", "cpu")
		if err != nil {
			t.Fatalf("GetStoragePath: %v", err)
		}
		if want := tc.want + "db/cpu/**/*.parquet"; glob != want {
			t.Errorf("GetStoragePath(prefix=%q) = %q, want %q", tc.prefix, glob, want)
		}
	}
}
