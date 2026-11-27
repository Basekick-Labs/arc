package storage

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog"
)

// lengthlessStore answers a HEAD 200 with the given headers and no
// Content-Length (written raw: net/http would add one), and a listing with
// one entry (listKey, listSize) or none when listKey is empty.
func lengthlessStore(t *testing.T, headHeaders map[string]string, listXML func(prefix string) string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(listXML(r.URL.Query().Get("prefix"))))
			return
		}
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Error("no hijacker")
			return
		}
		conn, buf, err := hj.Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		writeRawHead(buf, headHeaders)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func writeRawHead(buf *bufio.ReadWriter, headers map[string]string) {
	_, _ = buf.WriteString("HTTP/1.1 200 OK\r\nConnection: close\r\n")
	for k, v := range headers {
		_, _ = buf.WriteString(k + ": " + v + "\r\n")
	}
	_, _ = buf.WriteString("\r\n")
	_ = buf.Flush()
}

func s3BackendAt(t *testing.T, url string) *S3Backend {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("key", "secret", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	return &S3Backend{
		client: s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(url)
			o.UsePathStyle = true
			o.RetryMaxAttempts = 1
		}),
		bucket: "b",
		logger: zerolog.Nop(),
	}
}

func s3ListXML(key string, size int64) func(string) string {
	return func(prefix string) string {
		contents := ""
		if key != "" {
			contents = fmt.Sprintf(`<Contents><Key>%s</Key><Size>%d</Size></Contents>`, key, size)
		}
		return `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
			`<Name>b</Name><Prefix>` + prefix + `</Prefix><MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated>` + contents + `</ListBucketResult>`
	}
}

const lengthlessKey = "db/m/f.parquet"

// A HEAD with no length takes the size from the listing of that exact key;
// with none there either it is ErrNoContentLength, never 0 (#1812).
func TestS3SizeWithoutContentLength(t *testing.T) {
	head := map[string]string{"ETag": `"abc"`, "Last-Modified": "Mon, 02 Jan 2006 15:04:05 GMT"}
	for _, c := range []struct {
		name     string
		listKey  string
		wantSize int64
		wantErr  bool
	}{
		{"listed", lengthlessKey, 42, false},
		{"listed empty object", lengthlessKey, 0, false},
		{"not listed", "", 0, true},
		{"another key listed", lengthlessKey + "2", 7, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			size := c.wantSize
			if c.wantErr {
				size = 7
			}
			b := s3BackendAt(t, lengthlessStore(t, head, s3ListXML(c.listKey, size)).URL)
			got, err := b.StatFile(context.Background(), lengthlessKey)
			got2, _, err2 := b.ObjectSHA(context.Background(), lengthlessKey)
			if c.wantErr {
				if !errors.Is(err, ErrNoContentLength) || !errors.Is(err2, ErrNoContentLength) {
					t.Fatalf("StatFile = %d, %v; ObjectSHA = %d, %v; want ErrNoContentLength", got, err, got2, err2)
				}
				return
			}
			if err != nil || got != c.wantSize || err2 != nil || got2 != c.wantSize {
				t.Fatalf("StatFile = %d, %v; ObjectSHA = %d, %v; want %d", got, err, got2, err2, c.wantSize)
			}
		})
	}
}

func azureBackendAt(t *testing.T, url string) *AzureBlobBackend {
	t.Helper()
	const fakeKey = "dGhpcy1pcy1ub3QtYS1yZWFsLWtleQ==" // never a real credential
	b, err := NewAzureBlobBackend(&AzureBlobConfig{
		ContainerName: "cont",
		AccountName:   "arctestaccount",
		AccountKey:    fakeKey,
		Endpoint:      url,
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func azureListXML(key string, size int64) func(string) string {
	return func(prefix string) string {
		blobs := ""
		if key != "" {
			blobs = fmt.Sprintf(`<Blob><Name>%s</Name><Properties><Content-Length>%d</Content-Length><BlobType>BlockBlob</BlobType></Properties></Blob>`, key, size)
		}
		return `<?xml version="1.0" encoding="utf-8"?><EnumerationResults ServiceEndpoint="http://stub/" ContainerName="cont">` +
			`<Prefix>` + prefix + `</Prefix><MaxResults>1</MaxResults><Blobs>` + blobs + `</Blobs><NextMarker/></EnumerationResults>`
	}
}

func TestAzureSizeWithoutContentLength(t *testing.T) {
	head := map[string]string{"ETag": `"abc"`, "x-ms-blob-type": "BlockBlob"}
	for _, c := range []struct {
		name     string
		listKey  string
		wantSize int64
		wantErr  bool
	}{
		{"listed", lengthlessKey, 42, false},
		{"not listed", "", 0, true},
		{"another key listed", lengthlessKey + "2", 7, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			size := c.wantSize
			if c.wantErr {
				size = 7
			}
			b := azureBackendAt(t, lengthlessStore(t, head, azureListXML(c.listKey, size)).URL)
			got, err := b.StatFile(context.Background(), lengthlessKey)
			got2, _, err2 := b.ObjectSHA(context.Background(), lengthlessKey)
			if c.wantErr {
				if !errors.Is(err, ErrNoContentLength) || !errors.Is(err2, ErrNoContentLength) {
					t.Fatalf("StatFile = %d, %v; ObjectSHA = %d, %v; want ErrNoContentLength", got, err, got2, err2)
				}
				return
			}
			if err != nil || got != c.wantSize || err2 != nil || got2 != c.wantSize {
				t.Fatalf("StatFile = %d, %v; ObjectSHA = %d, %v; want %d", got, err, got2, err2, c.wantSize)
			}
		})
	}
}

// The control: a HEAD that carries the length is used as it is, 0 included.
func TestS3SizeWithContentLength(t *testing.T) {
	for _, n := range []string{"0", "42"} {
		srv := lengthlessStore(t, map[string]string{"Content-Length": n, "ETag": `"abc"`}, s3ListXML("", 0))
		b := s3BackendAt(t, srv.URL)
		size, err := b.StatFile(context.Background(), lengthlessKey)
		if err != nil || fmt.Sprint(size) != n {
			t.Fatalf("Content-Length %s: StatFile = %d, %v", n, size, err)
		}
	}
}
