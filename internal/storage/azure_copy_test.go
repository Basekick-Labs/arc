package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/rs/zerolog"
)

// #1207 d2a2b: how an Azure copy authorises its source, and when it falls
// back to copying through this process.

type fakeToken struct{}

func (fakeToken) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "tok", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

// offline builds a backend's client without contacting the store.
func offline(t *testing.T, cfg AzureBlobConfig) *AzureBlobBackend {
	t.Helper()
	endpoint := "https://acct.blob.core.windows.net"
	var client *azblob.Client
	var err error
	if cfg.AccountKey != "" {
		cred, cerr := azblob.NewSharedKeyCredential(cfg.AccountName, cfg.AccountKey)
		if cerr != nil {
			t.Fatal(cerr)
		}
		client, err = azblob.NewClientWithSharedKeyCredential(endpoint, cred, nil)
	} else {
		client, err = azblob.NewClientWithNoCredential(endpoint+"?"+cfg.SASToken, nil)
	}
	if err != nil {
		t.Fatal(err)
	}
	return &AzureBlobBackend{client: client, containerName: "c", accountName: cfg.AccountName, logger: zerolog.Nop()}
}

func TestAzureCopySource(t *testing.T) {
	ctx := context.Background()
	key := "a2V5a2V5a2V5a2V5a2V5a2V5a2V5a2V5a2V5a2V5a2V5a2V5a2V5a2V5a2V5a2V5a2V5a2V5a2V5a2V5a2V5a2V5"
	// Account key: a SAS for the source, no bearer.
	shared := offline(t, AzureBlobConfig{AccountName: "acct", AccountKey: key})
	cc := shared.client.ServiceClient().NewContainerClient("c")
	u, bearer, err := shared.copySource(ctx, cc.NewBlobClient("_compaction_staged/j/o.parquet"), time.Hour)
	if err != nil || !strings.Contains(u, "sig=") || bearer != "" {
		t.Fatalf("shared key: %q %q %v", u, bearer, err)
	}
	// SAS-token auth: the client's own SAS travels in the URL.
	sasB := offline(t, AzureBlobConfig{AccountName: "acct", SASToken: "sv=2024&sig=abc"})
	cc = sasB.client.ServiceClient().NewContainerClient("c")
	u, bearer, err = sasB.copySource(ctx, cc.NewBlobClient("_compaction_staged/j/o.parquet"), time.Hour)
	if err != nil || !strings.Contains(u, "sig=abc") || bearer != "" {
		t.Fatalf("sas token: %q %q %v", u, bearer, err)
	}
	// Entra: the plain URL with a bearer token.
	sasB.tokenCred = fakeToken{}
	u, bearer, err = sasB.copySource(ctx, cc.NewBlobClient("_compaction_staged/j/o.parquet"), time.Hour)
	if err != nil || bearer != "Bearer tok" {
		t.Fatalf("entra: %q %q %v", u, bearer, err)
	}
}

func TestAzureCopyRefused(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{&azcore.ResponseError{StatusCode: http.StatusForbidden}, true},
		{&azcore.ResponseError{StatusCode: http.StatusUnauthorized}, true},
		{&azcore.ResponseError{StatusCode: http.StatusBadRequest}, false},
		{errors.New("network"), false},
	} {
		if got := copyRefused(tc.err); got != tc.want {
			t.Fatalf("%v: %v", tc.err, got)
		}
	}
}

// The fallback copies the bytes through this process.
func TestStreamCopy(t *testing.T) {
	ctx := context.Background()
	b, err := NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("row "), 10000)
	if err := b.Write(ctx, "src.parquet", body); err != nil {
		t.Fatal(err)
	}
	if err := streamCopy(ctx, b, func(r io.Reader) error {
		return b.WriteReader(ctx, "dst.parquet", r, int64(len(body)))
	}, "src.parquet"); err != nil {
		t.Fatal(err)
	}
	got, err := b.Read(ctx, "dst.parquet")
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("copy differs: %v", err)
	}
	// A source that cannot be read fails the copy.
	if err := streamCopy(ctx, b, func(r io.Reader) error {
		return b.WriteReader(ctx, "dst2.parquet", r, 10)
	}, "absent.parquet"); err == nil {
		t.Fatal("copied from an absent source")
	}
}

// A copy source escapes each key segment, "+" included.
func TestEscapeKey(t *testing.T) {
	if got := escapeKey("p/db/cpu/a+b c.parquet"); got != "p/db/cpu/a%2Bb%20c.parquet" {
		t.Fatalf("got %q", got)
	}
}
