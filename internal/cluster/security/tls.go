package security

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/basekick-labs/arc/internal/config"
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

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

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
