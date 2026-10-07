//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package api

import (
	"errors"
	"testing"

	"golang.org/x/sys/unix"
)

func TestClassifyPeekError(t *testing.T) {
	tests := []struct {
		name             string
		err              error
		wantDisconnected bool
		wantSupported    bool
	}{
		{name: "connection reset", err: unix.ECONNRESET, wantDisconnected: true, wantSupported: true},
		{name: "unknown error stops watching", err: errors.New("unexpected socket error")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			disconnected, supported := classifyPeekError(tt.err)
			if disconnected != tt.wantDisconnected || supported != tt.wantSupported {
				t.Fatalf("classifyPeekError(%v) = (%v, %v), want (%v, %v)", tt.err, disconnected, supported, tt.wantDisconnected, tt.wantSupported)
			}
		})
	}
}
