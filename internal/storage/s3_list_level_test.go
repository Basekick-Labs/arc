package storage

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog"
)

// TestS3ListLevel: with a configured object prefix, across two pages, the
// level's directories and its own objects come back as bare names. The
// directory marker object, and a deeper key a non-conforming store returned
// despite the delimiter, are not files of the level.
func TestS3ListLevel(t *testing.T) {
	const objPrefix = "tenant-a/"
	const day = objPrefix + "db/cpu/2026/01/15/"

	var mu sync.Mutex
	var asked []string // "prefix|delimiter|token"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		mu.Lock()
		asked = append(asked, q.Get("prefix")+"|"+q.Get("delimiter")+"|"+q.Get("continuation-token"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/xml")
		head := `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>b</Name><Prefix>` + q.Get("prefix") + `</Prefix><Delimiter>/</Delimiter>`
		if q.Get("continuation-token") == "" {
			_, _ = w.Write([]byte(head + `<IsTruncated>true</IsTruncated><NextContinuationToken>page2</NextContinuationToken>` +
				`<Contents><Key>` + day + `</Key><Size>0</Size></Contents>` +
				`<Contents><Key>` + day + `cpu_daily_1.parquet</Key><Size>4</Size></Contents>` +
				`<CommonPrefixes><Prefix>` + day + `10/</Prefix></CommonPrefixes>` +
				`</ListBucketResult>`))
			return
		}
		_, _ = w.Write([]byte(head + `<IsTruncated>false</IsTruncated>` +
			`<Contents><Key>` + day + `cpu_daily_2.parquet</Key><Size>4</Size></Contents>` +
			`<Contents><Key>` + day + `12/deeper.parquet</Key><Size>4</Size></Contents>` +
			`<CommonPrefixes><Prefix>` + day + `11/</Prefix></CommonPrefixes>` +
			`</ListBucketResult>`))
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
		prefix: objPrefix,
		logger: zerolog.Nop(),
	}
	ctx := context.Background()

	// No trailing separator from the caller: the method adds one.
	dirs, files, err := b.ListLevel(ctx, "db/cpu/2026/01/15")
	assertLevel(t, dirs, files, err, "10,11", "cpu_daily_1.parquet,cpu_daily_2.parquet")

	mu.Lock()
	got := append([]string(nil), asked...)
	asked = nil
	mu.Unlock()
	if len(got) != 2 || got[0] != day+"|/|" || got[1] != day+"|/|page2" {
		t.Errorf("requests = %q, want two delimiter listings of %q, the second with the continuation token", got, day)
	}

	// ListDirectories is the same listing's directories.
	only, err := b.ListDirectories(ctx, "db/cpu/2026/01/15/")
	assertLevel(t, only, nil, err, "10,11", "")
}
