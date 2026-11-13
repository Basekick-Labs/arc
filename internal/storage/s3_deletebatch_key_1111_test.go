package storage

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog"
)

// A per-object DeleteObjects failure is reported under the caller's key, not
// the prefixed object name the service answers with (#1111): an operator
// matching the error against the manifest or a listing must find the key.
// The request itself still carries the prefixed name.
func TestS3DeleteBatchReportsCallerKeys(t *testing.T) {
	const prefix = "tenant-a/arc/"
	var sent []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		sent = sent[:0]
		for _, m := range regexp.MustCompile(`<Key>([^<]*)</Key>`).FindAllSubmatch(body, -1) {
			sent = append(sent, string(m[1]))
		}
		var out bytes.Buffer
		out.WriteString(`<?xml version="1.0" encoding="UTF-8"?><DeleteResult>`)
		for _, k := range sent {
			out.WriteString(`<Error><Key>` + k + `</Key><Code>AccessDenied</Code><Message>denied</Message></Error>`)
		}
		out.WriteString(`</DeleteResult>`)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write(out.Bytes())
	}))
	t.Cleanup(server.Close)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("key", "secret", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	b := &S3Backend{
		client: s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(server.URL)
			o.UsePathStyle = true
		}),
		bucket: "b",
		prefix: prefix,
		logger: zerolog.Nop(),
	}

	keys := []string{"prod/cpu/2026/01/01/00/a.parquet", "prod/cpu/2026/01/01/00/b.parquet"}
	err = b.DeleteBatch(context.Background(), keys)
	if err == nil {
		t.Fatal("DeleteBatch: want the per-object failures")
	}
	if len(sent) != 2 || sent[0] != prefix+keys[0] || sent[1] != prefix+keys[1] {
		t.Fatalf("sent %q, want the prefixed object names", sent)
	}
	for _, k := range keys {
		if !strings.Contains("\n"+err.Error(), "\n"+k+": AccessDenied") && !strings.Contains(err.Error(), ": "+k+": AccessDenied") {
			t.Errorf("error %q does not name the caller's key %q", err, k)
		}
	}
	if strings.Contains(err.Error(), prefix) {
		t.Errorf("error %q leaks the object prefix", err)
	}
}
