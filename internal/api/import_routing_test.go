package api

// #1070: an import that reaches a node which may not take it (a reader, a
// compactor, a standby writer) is forwarded to a writer, once, with the body
// as received and no overall timeout.

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

var importPaths = []string{
	"/api/v1/import/csv",
	"/api/v1/import/parquet",
	"/api/v1/import/lp",
	"/api/v1/import/tle",
}

// importApp is an app built from the production Fiber configuration, so an
// upload stays in the request body the way it does in a running server.
func importApp(t *testing.T, h *ImportHandler) *fiber.App {
	t.Helper()
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	h.RegisterRoutes(app)
	return app
}

func newImportForwardingRouter(t *testing.T, peerURL string, role cluster.NodeRole, writeTimeout time.Duration) *cluster.Router {
	return newImportRouter(t, peerURL, role, writeTimeout, 0)
}

func newImportRouter(t *testing.T, peerURL string, role cluster.NodeRole, writeTimeout, importTimeout time.Duration) *cluster.Router {
	t.Helper()

	localNode := cluster.NewNode("local", "Local", role, "test-cluster")
	registry := cluster.NewRegistry(&cluster.RegistryConfig{
		LocalNode: localNode,
		Logger:    zerolog.Nop(),
	})
	writer := cluster.NewNode("writer", "Writer", cluster.RoleWriter, "test-cluster")
	writer.UpdateState(cluster.StateHealthy)
	writer.APIAddress = strings.TrimPrefix(peerURL, "http://")
	require.NoError(t, registry.Register(writer))

	return cluster.NewRouter(&cluster.RouterConfig{
		Timeout:       time.Second,
		Retries:       0,
		Registry:      registry,
		LocalNode:     localNode,
		Logger:        zerolog.Nop(),
		WriteTimeout:  writeTimeout,
		ImportTimeout: importTimeout,
	})
}

// multipartUpload is a curl -F style upload of one file.
func multipartUpload(t *testing.T, content []byte) (body []byte, contentType string) {
	t.Helper()
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	fw, err := w.CreateFormFile("file", "data.bin")
	require.NoError(t, err)
	_, err = fw.Write(content)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return b.Bytes(), w.FormDataContentType()
}

type forwardedImport struct {
	path        string
	body        []byte
	contentType string
	forwarded   string
}

func recordingWriter(t *testing.T, status int, delay time.Duration) (*httptest.Server, chan forwardedImport) {
	t.Helper()
	requests := make(chan forwardedImport, 16)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		time.Sleep(delay)
		requests <- forwardedImport{
			path:        r.URL.RequestURI(),
			body:        body,
			contentType: r.Header.Get("Content-Type"),
			forwarded:   r.Header.Get(ForwardedByHeader),
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte("forwarded"))
	}))
	t.Cleanup(peer.Close)
	return peer, requests
}

// A reader or a compactor forwards every import route, the multipart upload
// byte for byte with its boundary.
func TestImportRoutesForwardWritesFromNonIngestNodes(t *testing.T) {
	peer, requests := recordingWriter(t, http.StatusAccepted, 0)
	for _, role := range []cluster.NodeRole{cluster.RoleReader, cluster.RoleCompactor} {
		handler := NewImportHandler(zerolog.Nop())
		handler.SetRouter(newImportForwardingRouter(t, peer.URL, role, 0))
		app := importApp(t, handler)

		for _, path := range importPaths {
			t.Run(string(role)+path, func(t *testing.T) {
				body, contentType := multipartUpload(t, []byte("cpu,host=a value=1 1700000000000000000\n"))
				req := httptest.NewRequest(http.MethodPost, path+"?db=sample", bytes.NewReader(body))
				req.Header.Set("Content-Type", contentType)
				resp, err := app.Test(req, testRequestTimeoutMS)
				require.NoError(t, err)
				defer resp.Body.Close()
				responseBody, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.Equal(t, http.StatusAccepted, resp.StatusCode)
				require.Equal(t, "forwarded", string(responseBody))

				got := <-requests
				require.Equal(t, path+"?db=sample", got.path)
				require.Equal(t, body, got.body, "the upload was not forwarded as received")
				require.Equal(t, contentType, got.contentType)
				require.Equal(t, "local", got.forwarded)
			})
		}
	}
}

func TestImportRoutesRejectAlreadyForwardedWrites(t *testing.T) {
	var peerCalls atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		peerCalls.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer peer.Close()

	handler := NewImportHandler(zerolog.Nop())
	handler.SetRouter(newImportForwardingRouter(t, peer.URL, cluster.RoleCompactor, 0))
	app := importApp(t, handler)

	for _, path := range importPaths {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("payload"))
			req.Header.Set(ForwardedByHeader, "another-node")
			resp, err := app.Test(req, testRequestTimeoutMS)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusLoopDetected, resp.StatusCode)
		})
	}
	require.Zero(t, peerCalls.Load(), "loop-marked imports must not be forwarded again")
}

// A writer that cannot be reached: nothing was written, 503 marked as a
// refusal so the client retries. Not 502.
func TestImportForwardUnreachableWriterIsRetryable(t *testing.T) {
	_, _ = captureRefusals(t)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	dead := "http://" + l.Addr().String()
	require.NoError(t, l.Close())

	handler := NewImportHandler(zerolog.Nop())
	handler.SetRouter(newImportForwardingRouter(t, dead, cluster.RoleReader, 0))
	app := importApp(t, handler)
	for _, path := range importPaths {
		body, contentType := multipartUpload(t, []byte("x"))
		req := httptest.NewRequest(http.MethodPost, path+"?db=sample", bytes.NewReader(body))
		req.Header.Set("Content-Type", contentType)
		resp, err := app.Test(req, testRequestTimeoutMS)
		require.NoError(t, err)
		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, path)
		require.NotEmpty(t, resp.Header.Get(IngestFencedHeader), path)
		require.NotEmpty(t, resp.Header.Get(fiber.HeaderRetryAfter), path)
	}
}

// An import takes as long as it takes on the writer: the write-forward
// timeout does not apply to it (a 504 while the writer is still importing
// would invite a re-run that duplicates rows). Sent once.
func TestImportForwardHasNoWriteTimeout(t *testing.T) {
	peer, requests := recordingWriter(t, http.StatusOK, 400*time.Millisecond)
	handler := NewImportHandler(zerolog.Nop())
	handler.SetRouter(newImportForwardingRouter(t, peer.URL, cluster.RoleReader, 100*time.Millisecond))
	app := importApp(t, handler)

	body, contentType := multipartUpload(t, []byte("x"))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/import/csv?db=sample", bytes.NewReader(body))
	req.Header.Set("Content-Type", contentType)
	resp, err := app.Test(req, testRequestTimeoutMS)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	<-requests
	require.Empty(t, requests, "an import was sent more than once")
}

// Past cluster.import_forward_timeout_ms the forward gives up: 504, it may
// have been written; sent once.
func TestImportForwardBackstopIsAmbiguous(t *testing.T) {
	peer, requests := recordingWriter(t, http.StatusOK, 400*time.Millisecond)
	handler := NewImportHandler(zerolog.Nop())
	handler.SetRouter(newImportRouter(t, peer.URL, cluster.RoleReader, time.Minute, 100*time.Millisecond))
	app := importApp(t, handler)

	body, contentType := multipartUpload(t, []byte("x"))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/import/parquet?db=sample", bytes.NewReader(body))
	req.Header.Set("Content-Type", contentType)
	resp, err := app.Test(req, testRequestTimeoutMS)
	require.NoError(t, err)
	require.Equal(t, http.StatusGatewayTimeout, resp.StatusCode)
	require.Empty(t, resp.Header.Get(IngestFencedHeader))
	<-requests
	require.Empty(t, requests, "an import was sent more than once")
}

// An upload too large for the writer to accept is refused before it crosses
// the cluster network, on both forward paths.
func TestImportTooLargeIsNotForwarded(t *testing.T) {
	_, _ = captureRefusals(t)
	old := maxForwardedImportBody
	maxForwardedImportBody = 64
	t.Cleanup(func() { maxForwardedImportBody = old })
	peer, requests := recordingWriter(t, http.StatusOK, 0)

	handler := NewImportHandler(zerolog.Nop())
	handler.SetRouter(newImportForwardingRouter(t, peer.URL, cluster.RoleReader, 0))
	reader := importApp(t, handler)
	standby := standbyImportApp(t, primaryAt(strings.TrimPrefix(peer.URL, "http://")), 0)
	for name, app := range map[string]*fiber.App{"reader": reader, "standby": standby} {
		body, contentType := multipartUpload(t, bytes.Repeat([]byte("x"), 128))
		req := httptest.NewRequest(http.MethodPost, "/api/v1/import/csv?db=sample", bytes.NewReader(body))
		req.Header.Set("Content-Type", contentType)
		resp, err := app.Test(req, testRequestTimeoutMS)
		require.NoError(t, err)
		require.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode, name)
	}
	require.Empty(t, requests, "an oversize upload was forwarded")
}

// standbyImportApp is a standby writer: its own writes are fenced, and the
// cluster names a primary through target.
func standbyImportApp(t *testing.T, target func() (*cluster.Node, bool), writeTimeout time.Duration) *fiber.App {
	t.Helper()
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	require.NoError(t, err)
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 1000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
	}, backend, zerolog.Nop())
	t.Cleanup(func() { buf.Close() })
	buf.SetIngestGate(func() error { return fencedErr(t) })
	router := cluster.NewRouter(&cluster.RouterConfig{
		Registry:           cluster.NewRegistry(&cluster.RegistryConfig{Logger: zerolog.Nop()}),
		LocalNode:          cluster.NewNode("w-5", "w-5", cluster.RoleWriter, "c"),
		Logger:             zerolog.Nop(),
		StandbyWriteTarget: target,
		WriteTimeout:       writeTimeout,
	})
	h := NewImportHandler(zerolog.Nop())
	h.SetArrowBuffer(buf)
	h.SetRouter(router)
	return importApp(t, h)
}

// A standby writer forwards every import route to the primary, with no
// write timeout, and answers a second hop itself (one hop at most).
func TestStandbyForwardsImportsToThePrimary(t *testing.T) {
	_, _ = captureRefusals(t)
	peer, requests := recordingWriter(t, http.StatusOK, 400*time.Millisecond)
	app := standbyImportApp(t, primaryAt(strings.TrimPrefix(peer.URL, "http://")), 100*time.Millisecond)

	lp := []byte("cpu,host=a value=1 1700000000000000000\n")
	for _, path := range importPaths {
		body, contentType := multipartUpload(t, lp)
		req := httptest.NewRequest(http.MethodPost, path+"?db=sample", bytes.NewReader(body))
		req.Header.Set("Content-Type", contentType)
		resp, err := app.Test(req, testRequestTimeoutMS)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode, path)
		got := <-requests
		require.Equal(t, body, got.body, path)
		require.Equal(t, "w-5", got.forwarded, path)
	}

	body, contentType := multipartUpload(t, lp)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/import/lp?db=sample", bytes.NewReader(body))
	req.Header.Set("Content-Type", contentType)
	req.Header.Set(ForwardedByHeader, "w-9")
	resp, err := app.Test(req, testRequestTimeoutMS)
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, "second hop must answer locally")
	require.Empty(t, requests, "a forwarded import was forwarded again")
}

// A node that ingests imports itself (guard: the old behaviour).
func TestImportOnWriterIsNotForwarded(t *testing.T) {
	peer, requests := recordingWriter(t, http.StatusAccepted, 0)
	handler := NewImportHandler(zerolog.Nop())
	handler.SetRouter(newImportForwardingRouter(t, peer.URL, cluster.RoleWriter, 0))
	app := importApp(t, handler)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/import/lp", strings.NewReader("x"))
	resp, err := app.Test(req, testRequestTimeoutMS)
	require.NoError(t, err)
	require.NotEqual(t, http.StatusAccepted, resp.StatusCode)
	require.Empty(t, requests)
}

// An LP import refused after some measurements were written answers a
// non-retryable error naming them, without the refusal headers: those promise
// nothing was written, and a re-run would write those rows again.
func TestLPImportRefusedPartWayIsNotRetryable(t *testing.T) {
	_, _ = captureRefusals(t)
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	require.NoError(t, err)
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 1000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
	}, backend, zerolog.Nop())
	t.Cleanup(func() { buf.Close() })
	// The gate passes `allowed` checks (admission, then one per measurement),
	// then the node is fenced.
	var calls, allowed atomic.Int32
	fenced, closingErr := fencedErr(t), ingest.ErrBufferClosing
	var gateErr atomic.Pointer[error]
	gateErr.Store(&fenced)
	buf.SetIngestGate(func() error {
		if calls.Add(1) > allowed.Load() {
			return *gateErr.Load()
		}
		return nil
	})
	h := NewImportHandler(zerolog.Nop())
	h.SetArrowBuffer(buf)
	app := importApp(t, h)

	allowed.Store(2) // admission and the first measurement
	body, contentType := multipartUpload(t, []byte("cpu,host=a value=1 1700000000000000000\nmem,host=a used=2 1700000000000000000\n"))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/import/lp?db=sample", bytes.NewReader(body))
	req.Header.Set("Content-Type", contentType)
	resp, err := app.Test(req, testRequestTimeoutMS)
	require.NoError(t, err)
	out, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode, string(out))
	require.Empty(t, resp.Header.Get(IngestFencedHeader))
	require.Empty(t, resp.Header.Get(IngestRefusedHeader))
	require.Empty(t, resp.Header.Get(fiber.HeaderRetryAfter))
	var partial struct {
		Written []string `json:"written_measurements"`
		Stopped string   `json:"stopped_at_measurement"`
	}
	require.NoError(t, json.Unmarshal(out, &partial))
	require.Len(t, partial.Written, 1)
	require.NotEmpty(t, partial.Stopped)
	require.NotEqual(t, partial.Written[0], partial.Stopped)

	// The same for a buffer that starts closing part-way (was a retryable 503).
	gateErr.Store(&closingErr)
	calls.Store(0)
	allowed.Store(2)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/import/lp?db=sample", bytes.NewReader(body))
	req.Header.Set("Content-Type", contentType)
	resp, err = app.Test(req, testRequestTimeoutMS)
	require.NoError(t, err)
	out, _ = io.ReadAll(resp.Body)
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode, string(out))
	require.Contains(t, string(out), "written_measurements")
	gateErr.Store(&fenced)

	// Refused before anything was written, at admission or at the first
	// measurement: still the retryable 503.
	for _, n := range []int32{0, 1} {
		calls.Store(0)
		allowed.Store(n)
		req = httptest.NewRequest(http.MethodPost, "/api/v1/import/lp?db=sample", bytes.NewReader(body))
		req.Header.Set("Content-Type", contentType)
		resp, err = app.Test(req, testRequestTimeoutMS)
		require.NoError(t, err)
		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, "allowed=%d", n)
		require.NotEmpty(t, resp.Header.Get(IngestFencedHeader), "allowed=%d", n)
	}
}
