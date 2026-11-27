package license

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
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

// A proxy's error page that echoes the request (whose address carries the
// key on verify) does not put the key into the returned error.
func TestVerifyErrorPageOmitsTheKey(t *testing.T) {
	const key = "ARC-ENT-TESTKEY-FEDCBA9876543210"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("<html>Bad gateway for " + r.URL.RequestURI() + "</html>"))
	}))
	defer srv.Close()
	c, err := NewClient(&ClientConfig{LicenseKey: key, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	c.serverURL = srv.URL
	_, err = c.Verify(context.Background())
	if err == nil || strings.Contains(err.Error(), "FEDCBA9876543210") || !strings.Contains(err.Error(), "502") {
		t.Fatalf("error: %v", err)
	}
}
