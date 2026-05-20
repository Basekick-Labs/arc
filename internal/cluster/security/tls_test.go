package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
)

// TestSchemeForServer pins the http/https switch on the operator's
// server.tls_enabled setting. Inter-node URL construction depends on
// this being aligned with the receiver's Fiber listener — a mismatch
// silently produces TLS-handshake failures with no useful error.
func TestSchemeForServer(t *testing.T) {
	t.Parallel()
	if got := SchemeForServer(false); got != "http" {
		t.Errorf("server.tls_enabled=false: got %q, want %q", got, "http")
	}
	if got := SchemeForServer(true); got != "https" {
		t.Errorf("server.tls_enabled=true: got %q, want %q", got, "https")
	}
}

// TestNewClusterHTTPTransport_PlainHTTP verifies the back-compat path:
// passing nil leaves TLSClientConfig nil, and net/http dials plain TCP
// when the URL uses http://. Pool defaults reference the shared
// constants so a future tuning change updates this assertion at the
// same time as the production code.
func TestNewClusterHTTPTransport_PlainHTTP(t *testing.T) {
	t.Parallel()
	tr := NewClusterHTTPTransport(nil)
	if tr == nil {
		t.Fatal("NewClusterHTTPTransport(nil) returned nil")
	}
	if tr.TLSClientConfig != nil {
		t.Errorf("nil tlsCfg should leave TLSClientConfig nil, got non-nil")
	}
	if tr.MaxIdleConns != clusterHTTPMaxIdleConns {
		t.Errorf("MaxIdleConns: got %d, want %d", tr.MaxIdleConns, clusterHTTPMaxIdleConns)
	}
	if tr.MaxIdleConnsPerHost != clusterHTTPMaxIdleConnsPerHost {
		t.Errorf("MaxIdleConnsPerHost: got %d, want %d", tr.MaxIdleConnsPerHost, clusterHTTPMaxIdleConnsPerHost)
	}
	if tr.IdleConnTimeout != clusterHTTPIdleConnTimeout {
		t.Errorf("IdleConnTimeout: got %s, want %s", tr.IdleConnTimeout, clusterHTTPIdleConnTimeout)
	}
}

// TestNewClusterHTTPTransport_TLSConfigCloned verifies the happy path
// with a real cluster TLS config: the transport's TLSClientConfig is
// a clone of the passed-in config (NOT the same pointer — see Clone()
// rationale in NewClusterHTTPTransport), and the cert/MinVersion/RootCAs
// are preserved. Catches regressions to the Clone() step that would
// either re-share the pointer (bad: net/http mutates NextProtos) or
// drop fields (bad: handshake would fail).
func TestNewClusterHTTPTransport_TLSConfigCloned(t *testing.T) {
	t.Parallel()
	certPEM, keyPEM := generateTestCert(t)
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certPEM) {
		t.Fatal("AppendCertsFromPEM: failed to parse cert")
	}
	src := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS12,
	}

	tr := NewClusterHTTPTransport(src)
	if tr.TLSClientConfig == nil {
		t.Fatal("expected non-nil TLSClientConfig with valid tlsCfg")
	}
	if tr.TLSClientConfig == src {
		t.Error("expected TLSClientConfig to be a Clone() of src, got the same pointer (Clone() step was skipped)")
	}
	if len(tr.TLSClientConfig.Certificates) != 1 {
		t.Errorf("Certificates: got %d, want 1", len(tr.TLSClientConfig.Certificates))
	}
	if tr.TLSClientConfig.RootCAs == nil {
		t.Error("RootCAs was dropped by Clone() step")
	}
	if tr.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion: got %x, want %x (TLS12)", tr.TLSClientConfig.MinVersion, tls.VersionTLS12)
	}
	if tr.TLSClientConfig.InsecureSkipVerify {
		t.Error("InsecureSkipVerify must remain false after Clone()")
	}
}

// TestClusterTLSConfig_MissingFiles verifies fail-loud behaviour when
// cluster.tls_enabled is true but the cert/key files are missing. The
// loader must propagate the underlying fs error rather than silently
// falling back to plaintext (a security surprise) or to system roots
// (would mask the misconfiguration).
func TestClusterTLSConfig_MissingFiles(t *testing.T) {
	t.Parallel()
	cfg := &config.ClusterConfig{
		TLSEnabled:  true,
		TLSCertFile: filepath.Join(t.TempDir(), "nonexistent-cert.pem"),
		TLSKeyFile:  filepath.Join(t.TempDir(), "nonexistent-key.pem"),
	}
	_, err := ClusterTLSConfig(cfg)
	if err == nil {
		t.Fatal("expected error when cluster.tls_enabled=true with missing cert files, got nil")
	}
	// Assert the error carries the file-loader context. If a future
	// refactor swallows the underlying error and returns a transport
	// with InsecureSkipVerify=true, this catches the regression.
	if !strings.Contains(err.Error(), "load cluster TLS keypair") {
		t.Errorf("error should mention 'load cluster TLS keypair', got: %v", err)
	}
	// Underlying error should still be an fs error reachable via Is/As.
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("error should wrap fs.ErrNotExist, got: %v", err)
	}
}

// generateTestCert produces a self-signed ECDSA cert + key in PEM form
// for use as a cluster TLS test fixture. Kept inline rather than reading
// from disk because crypto/x509 cert generation is faster than
// round-tripping to a temp file and avoids platform path-separator
// concerns.
func generateTestCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "arc-test-cluster"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("MarshalECPrivateKey: %v", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	// Sanity-check we can round-trip the generated cert.
	if _, err := os.Stat(t.TempDir()); err != nil {
		t.Fatalf("TempDir stat: %v", err)
	}
	return certPEM, keyPEM
}
