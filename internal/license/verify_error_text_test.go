package license

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// A verify request that fails in transport (server unreachable) is reported
// and logged without the licence key: the request carries it in its address,
// which the transport error quotes.
func TestVerifyTransportErrorOmitsTheKey(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens: connection refused

	var logs bytes.Buffer
	const key = "ARC-ENT-TESTKEY-0123456789ABCDEF"
	c, err := NewClient(&ClientConfig{LicenseKey: key, Logger: zerolog.New(&logs)})
	if err != nil {
		t.Fatal(err)
	}
	c.serverURL = "http://" + addr
	_, err = c.Verify(context.Background())
	if err == nil {
		t.Fatal("verify against a closed port succeeded")
	}
	if strings.Contains(err.Error(), key) || strings.Contains(err.Error(), "0123456789ABCDEF") {
		t.Fatalf("error carries the key: %v", err)
	}
	if !strings.Contains(err.Error(), addr) {
		t.Fatalf("error should still name the server: %v", err)
	}
	if strings.Contains(logs.String(), "0123456789ABCDEF") {
		t.Fatalf("log carries the key: %s", logs.String())
	}
}
