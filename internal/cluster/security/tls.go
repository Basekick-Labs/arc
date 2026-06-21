package security

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/fips"
)

// ClusterTLSConfig creates a tls.Config from ClusterConfig.
// Returns nil if TLS is not enabled.
func ClusterTLSConfig(cfg *config.ClusterConfig) (*tls.Config, error) {
	if !cfg.TLSEnabled {
		return nil, nil
	}

	cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load cluster TLS keypair: %w", err)
	}

	// Check certificate expiration — warn but don't block startup
	if len(cert.Certificate) > 0 {
		leaf, parseErr := x509.ParseCertificate(cert.Certificate[0])
		if parseErr == nil {
			now := time.Now()
			if now.After(leaf.NotAfter) {
				fmt.Fprintf(os.Stderr, "WARNING: cluster TLS certificate EXPIRED on %s — connections may fail until certificate is renewed\n",
					leaf.NotAfter.Format(time.RFC3339))
			} else if leaf.NotAfter.Sub(now) < 30*24*time.Hour {
				fmt.Fprintf(os.Stderr, "WARNING: cluster TLS certificate expires in %d days (%s)\n",
					int(leaf.NotAfter.Sub(now).Hours()/24), leaf.NotAfter.Format(time.RFC3339))
			}
		}
	}

	// HardenTLSConfig sets the TLS 1.2 floor in both build variants (the
	// pre-existing behavior, formerly an explicit MinVersion literal here) and,
	// in the fips build only, restricts cipher suites and curves to the
	// FIPS-approved set. The default build keeps Go's default ciphers/curves.
	// Certificates are passed in so they are preserved.
	tlsCfg := fips.HardenTLSConfig(&tls.Config{
		Certificates: []tls.Certificate{cert},
	})

	if cfg.TLSCAFile != "" {
		caCert, err := os.ReadFile(cfg.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("read cluster CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse cluster CA certificate")
		}
		tlsCfg.RootCAs = pool
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return tlsCfg, nil
}

// Connection-pool defaults shared by every cluster-internal HTTP
// transport (request router, scatter-gather scaffolding, post-compaction
// cache-invalidate fan-out). Kept in one place so a tuning change lands
// everywhere atomically; the same numeric defaults previously lived in
// three separate inline literals across router.go, sharding/router.go,
// and the cache-invalidate sender.
const (
	clusterHTTPMaxIdleConns        = 100
	clusterHTTPMaxIdleConnsPerHost = 10
	clusterHTTPIdleConnTimeout     = 90 * time.Second
)

// NewClusterHTTPTransport returns an *http.Transport suitable for
// inter-node HTTP calls.
//
// Pass a non-nil tlsCfg when the receiving peers serve HTTPS — the
// transport will use it (cloned, see below) as its TLSClientConfig. Pass
// nil for plaintext clusters; net/http then uses its default
// *tls.Config (system root CAs) on the rare path where the URL is
// https://, which is the right behaviour when an operator runs with
// server.tls_enabled and a publicly-trusted public-API cert.
//
// Callers loading cluster TLS config repeatedly should call
// ClusterTLSConfig once at startup and pass the resulting *tls.Config
// here. That avoids re-reading the cert/key/CA on every consumer (a
// previous shape called ClusterTLSConfig from each consumer, which
// triggered the "certificate expires in N days" warning multiple times
// per process start).
//
// Callers MUST pair this with SchemeForServer when building URLs;
// using "http" against a TLS-serving peer (or "https" against a
// plaintext peer) produces a connection failure that has no useful
// error.
func NewClusterHTTPTransport(tlsCfg *tls.Config) *http.Transport {
	t := &http.Transport{
		MaxIdleConns:        clusterHTTPMaxIdleConns,
		MaxIdleConnsPerHost: clusterHTTPMaxIdleConnsPerHost,
		IdleConnTimeout:     clusterHTTPIdleConnTimeout,
	}
	if tlsCfg != nil {
		// Clone() is a shallow copy: it duplicates the struct but
		// shares pointers to RootCAs / Certificates / ClientSessionCache.
		// Sharing the session cache across goroutines is what enables
		// TLS session resumption for inter-node connections. The clone
		// itself is defensive: net/http mutates TLSClientConfig.NextProtos
		// during h2 negotiation (see net/http/transport.go) and we don't
		// want that to leak back into the cluster-TLS source config that
		// other subsystems (Raft, peer-fetch) also reference.
		t.TLSClientConfig = tlsCfg.Clone()
	}
	return t
}

// SchemeForServer returns "http" or "https" based on the operator's
// server.tls_enabled setting. All cluster nodes are expected to be
// configured identically (a heterogeneous cluster is unsupported), so
// the local node's server.tls_enabled correctly predicts what every
// peer's Fiber listener is serving.
func SchemeForServer(serverTLSEnabled bool) string {
	if serverTLSEnabled {
		return "https"
	}
	return "http"
}

// Listen creates a net.Listener, wrapping with TLS if tlsCfg is non-nil.
func Listen(network, addr string, tlsCfg *tls.Config) (net.Listener, error) {
	if tlsCfg != nil {
		return tls.Listen(network, addr, tlsCfg)
	}
	return net.Listen(network, addr)
}

// Dial connects to addr, wrapping with TLS if tlsCfg is non-nil.
func Dial(network, addr string, timeout time.Duration, tlsCfg *tls.Config) (net.Conn, error) {
	if tlsCfg != nil {
		dialer := &net.Dialer{Timeout: timeout}
		return tls.DialWithDialer(dialer, network, addr, tlsCfg)
	}
	return net.DialTimeout(network, addr, timeout)
}
