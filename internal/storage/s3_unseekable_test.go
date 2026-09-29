package storage

// WriteReader with a body the SDK cannot rewind (an io.Pipe from a streaming
// copy, an HTTP request body) against a plain-HTTP endpoint. The SDK's
// refusal is client-side — it never sends a request — so an httptest server
// that answers every S3 call with success reproduces it without an object
// store, and records what actually reached the wire.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"
)

type stubPut struct {
	key           string
	bodyLen       int64
	contentLength int64
}

type stubState struct {
	puts       []stubPut
	partsTotal int64
	initiated  int
	completed  int
	aborted    int
}

// s3Stub is the minimum of S3 the SDK needs to complete a PutObject or a
// multipart upload: 200 for HEAD, an UploadId for the multipart initiate,
// an ETag for each part, and a non-empty completion document (the SDK
// treats an empty 200 on CompleteMultipartUpload as an error).
type s3Stub struct {
	srv       *httptest.Server
	mu        sync.Mutex
	puts      []stubPut
	parts     map[string][]int64
	completed []string
	aborted   []string
	uploads   int
}

func newS3Stub(t *testing.T) *s3Stub {
	t.Helper()
	s := &s3Stub{parts: map[string][]int64{}}
	s.srv = httptest.NewServer(http.HandlerFunc(s.handle))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *s3Stub) handle(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	key := strings.TrimPrefix(r.URL.Path, "/")
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case r.Method == http.MethodHead:
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodPost && q.Has("uploads"):
		s.uploads++
		id := fmt.Sprintf("upload-%d", s.uploads)
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult><Bucket>b</Bucket><Key>%s</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>`, key, id)
	case r.Method == http.MethodPut && q.Has("uploadId"):
		n, _ := io.Copy(io.Discard, r.Body)
		id := q.Get("uploadId")
		s.parts[id] = append(s.parts[id], n)
		w.Header().Set("ETag", fmt.Sprintf(`"part-%d"`, len(s.parts[id])))
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodPost && q.Has("uploadId"):
		_, _ = io.Copy(io.Discard, r.Body)
		s.completed = append(s.completed, key)
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult><Location>%s</Location><Bucket>b</Bucket><Key>%s</Key><ETag>"done"</ETag></CompleteMultipartUploadResult>`, r.URL.Path, key)
	case r.Method == http.MethodDelete && q.Has("uploadId"):
		s.aborted = append(s.aborted, key)
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPut:
		n, _ := io.Copy(io.Discard, r.Body)
		s.puts = append(s.puts, stubPut{key: key, bodyLen: n, contentLength: r.ContentLength})
		w.Header().Set("ETag", `"obj"`)
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusOK)
	}
}

func (s *s3Stub) snapshot() stubState {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := stubState{initiated: s.uploads, completed: len(s.completed), aborted: len(s.aborted)}
	st.puts = append(st.puts, s.puts...)
	for _, sizes := range s.parts {
		for _, n := range sizes {
			st.partsTotal += n
		}
	}
	return st
}

func stubBackend(t *testing.T, s *s3Stub) *S3Backend {
	t.Helper()
	b, err := NewS3Backend(&S3Config{
		Bucket: "b", Region: "us-east-1", Endpoint: s.srv.URL,
		AccessKey: "k", SecretKey: "s", PathStyle: true, UseSSL: false,
	}, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewS3Backend: %v", err)
	}
	return b
}

const unseekableKey = "db1/cpu/2024/03/15/cpu_20240315_daily.parquet"

const (
	mib      = 1024 * 1024
	largeMiB = 20 * mib // above one 16 MiB part, below the 100 MiB threshold
)

// pipeOf streams data through an io.Pipe, the shape the tiering migrator's
// copyFileStreaming hands to WriteReader. The test closes the reader so a
// writer left with unread bytes (the long-body cases) does not block forever.
func pipeOf(t *testing.T, data []byte) *io.PipeReader {
	t.Helper()
	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write(data)
		_ = pw.Close()
	}()
	t.Cleanup(func() { _ = pr.Close() })
	return pr
}

func repeat(b byte, n int) []byte { return bytes.Repeat([]byte{b}, n) }

func TestWriteReader_UnseekableSmallBodyOverHTTP(t *testing.T) {
	stub := newS3Stub(t)
	b := stubBackend(t, stub)
	data := repeat('a', 1024)

	if err := b.WriteReader(context.Background(), unseekableKey, pipeOf(t, data), int64(len(data))); err != nil {
		t.Fatalf("WriteReader with an io.Pipe over http: %v", err)
	}
	st := stub.snapshot()
	if len(st.puts) != 1 || st.puts[0].bodyLen != 1024 || st.puts[0].key != "b/"+unseekableKey {
		t.Fatalf("puts = %+v, want one PutObject of 1024 bytes at b/%s", st.puts, unseekableKey)
	}
	if st.puts[0].contentLength != 1024 {
		t.Fatalf("Content-Length = %d, want 1024 (spooled body is sent with its exact length)", st.puts[0].contentLength)
	}
	if st.initiated != 0 || st.completed != 0 {
		t.Fatalf("a 1 KiB body must be a single PutObject, got multipart initiated=%d completed=%d", st.initiated, st.completed)
	}
}

// Exactly one part is still spooled: the Uploader would have made a
// multipart upload of it (it only takes the single-part path on EOF within
// the first read), and a fresh 16 MiB pool buffer besides.
func TestWriteReader_UnseekableOnePartBodyIsSpooled(t *testing.T) {
	stub := newS3Stub(t)
	b := stubBackend(t, stub)
	data := repeat('p', 16*mib)

	if err := b.WriteReader(context.Background(), unseekableKey, pipeOf(t, data), int64(len(data))); err != nil {
		t.Fatalf("WriteReader with a 16 MiB io.Pipe: %v", err)
	}
	st := stub.snapshot()
	if len(st.puts) != 1 || st.puts[0].bodyLen != 16*mib || st.initiated != 0 {
		t.Fatalf("puts=%+v initiated=%d, want one PutObject of 16 MiB", st.puts, st.initiated)
	}
}

// Above one part the body becomes a multipart upload. 32 MiB is an exact
// multiple of the part size: the Uploader asks for a third part, the
// length wrapper answers EOF, and no empty part may be sent.
func TestWriteReader_UnseekableLargeBodyOverHTTP(t *testing.T) {
	for _, size := range []int{largeMiB, 32 * mib} {
		t.Run(fmt.Sprintf("%dMiB", size/mib), func(t *testing.T) {
			stub := newS3Stub(t)
			b := stubBackend(t, stub)
			data := repeat('b', size)

			if err := b.WriteReader(context.Background(), unseekableKey, pipeOf(t, data), int64(size)); err != nil {
				t.Fatalf("WriteReader with a %d-byte io.Pipe over http: %v", size, err)
			}
			st := stub.snapshot()
			if len(st.puts) != 0 || st.initiated != 1 || st.completed != 1 || st.aborted != 0 || st.partsTotal != int64(size) {
				t.Fatalf("puts=%d initiated=%d completed=%d aborted=%d parts=%d bytes, want one multipart upload of %d bytes",
					len(st.puts), st.initiated, st.completed, st.aborted, st.partsTotal, size)
			}
		})
	}
}

// A declared length is a contract for an unrewindable body: a short body
// commits nothing, on either path.
func TestWriteReader_UnseekableShortBodyFails(t *testing.T) {
	for _, tc := range []struct {
		name          string
		declared      int64
		sent          int
		wantInitiated int
		wantAborted   int
	}{
		{"single PutObject", 2048, 1024, 0, 0},
		{"multipart", largeMiB, 18 * mib, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := newS3Stub(t)
			b := stubBackend(t, stub)

			err := b.WriteReader(context.Background(), unseekableKey, pipeOf(t, repeat('s', tc.sent)), tc.declared)
			if !errors.Is(err, ErrBodyLength) {
				t.Fatalf("err = %v, want ErrBodyLength", err)
			}
			st := stub.snapshot()
			if len(st.puts) != 0 || st.completed != 0 || st.initiated != tc.wantInitiated || st.aborted != tc.wantAborted {
				t.Fatalf("puts=%d initiated=%d completed=%d aborted=%d, want nothing committed (initiated %d, aborted %d)",
					len(st.puts), st.initiated, st.completed, st.aborted, tc.wantInitiated, tc.wantAborted)
			}
		})
	}
}

// A body that continues past the declared length is refused rather than
// truncated: the old PutObject branch silently cut it at ContentLength.
func TestWriteReader_UnseekableLongBodyFails(t *testing.T) {
	for _, tc := range []struct {
		name        string
		declared    int64
		sent        int
		wantAborted int
	}{
		{"single PutObject", 1024, 2048, 0},
		{"multipart", largeMiB, 21 * mib, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := newS3Stub(t)
			b := stubBackend(t, stub)

			err := b.WriteReader(context.Background(), unseekableKey, pipeOf(t, repeat('l', tc.sent)), tc.declared)
			if !errors.Is(err, ErrBodyLength) {
				t.Fatalf("err = %v, want ErrBodyLength", err)
			}
			st := stub.snapshot()
			if len(st.puts) != 0 || st.completed != 0 || st.aborted != tc.wantAborted {
				t.Fatalf("puts=%d completed=%d aborted=%d, want nothing committed (aborted %d)",
					len(st.puts), st.completed, st.aborted, tc.wantAborted)
			}
		})
	}
}

// failingReader yields data and then its own error, the way edge sync's
// shortBodyGuard yields errShortBody; the caller must still see that error.
type failingReader struct {
	data []byte
	err  error
}

func (f *failingReader) Read(p []byte) (int, error) {
	if len(f.data) == 0 {
		return 0, f.err
	}
	n := copy(p, f.data)
	f.data = f.data[n:]
	return n, nil
}

func TestWriteReader_UnseekableInnerErrorPassesThrough(t *testing.T) {
	sentinel := errors.New("the source's own error")
	for _, tc := range []struct {
		name     string
		declared int64
		sent     int
	}{
		{"single PutObject", 2048, 1024},
		{"multipart", largeMiB, 17 * mib},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := newS3Stub(t)
			b := stubBackend(t, stub)

			err := b.WriteReader(context.Background(), unseekableKey, &failingReader{data: repeat('e', tc.sent), err: sentinel}, tc.declared)
			if !errors.Is(err, sentinel) {
				t.Fatalf("err = %v, want the source's error to pass through", err)
			}
			if errors.Is(err, ErrBodyLength) {
				t.Fatalf("err = %v, the source's error must not be reclassified as a length mismatch", err)
			}
			if st := stub.snapshot(); len(st.puts) != 0 || st.completed != 0 {
				t.Fatalf("puts=%d completed=%d, want nothing committed", len(st.puts), st.completed)
			}
		})
	}
}

// brokenSeeker claims io.Seeker but cannot seek — the SDK probes Seek before
// trusting it, and so does the backend.
type brokenSeeker struct{ io.Reader }

func (brokenSeeker) Seek(int64, int) (int64, error) { return 0, errors.New("not seekable") }

func TestWriteReader_SeekProbeRoutesBrokenSeekerToSpool(t *testing.T) {
	stub := newS3Stub(t)
	b := stubBackend(t, stub)
	data := repeat('k', 1024)

	if err := b.WriteReader(context.Background(), unseekableKey, brokenSeeker{pipeOf(t, data)}, int64(len(data))); err != nil {
		t.Fatalf("WriteReader with a body whose Seek fails: %v", err)
	}
	if st := stub.snapshot(); len(st.puts) != 1 || st.puts[0].bodyLen != 1024 {
		t.Fatalf("puts = %+v, want one PutObject of 1024 bytes", st.puts)
	}
}

// A cancelled context stops a spool before anything is sent.
func TestWriteReader_UnseekableHonoursCancelledContext(t *testing.T) {
	stub := newS3Stub(t)
	b := stubBackend(t, stub)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := b.WriteReader(ctx, unseekableKey, pipeOf(t, repeat('c', 1024)), 1024)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if st := stub.snapshot(); len(st.puts) != 0 {
		t.Fatalf("puts = %+v, want none", st.puts)
	}
}

// The seekable path (ingest flush via Write, compaction, backup) keeps its
// single PutObject with an explicit Content-Length.
func TestWriteReader_SeekableSmallBodyUnchanged(t *testing.T) {
	stub := newS3Stub(t)
	b := stubBackend(t, stub)
	data := repeat('c', 1024)

	if err := b.WriteReader(context.Background(), unseekableKey, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("WriteReader with a bytes.Reader: %v", err)
	}
	st := stub.snapshot()
	if len(st.puts) != 1 || st.puts[0].bodyLen != 1024 || st.puts[0].contentLength != 1024 || st.initiated != 0 {
		t.Fatalf("puts = %+v initiated=%d, want one PutObject with Content-Length 1024", st.puts, st.initiated)
	}
}
