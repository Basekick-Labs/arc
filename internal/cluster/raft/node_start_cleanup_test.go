package raft

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// A Start that fails after the transport is up must release the Raft port;
// before, the listener (and, since #1431, the auth layer's accept goroutine)
// stayed alive for the life of the process.
func TestNodeStartFailureReleasesRaftPort(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	probe.Close()

	dataDir := t.TempDir()
	// A directory where the log store's file belongs makes NewBoltStore fail
	// after the transport has bound the port.
	if err := os.Mkdir(filepath.Join(dataDir, "raft-log.db"), 0700); err != nil {
		t.Fatal(err)
	}
	node, err := NewNode(&NodeConfig{
		NodeID:           "start-fail",
		SharedSecret:     "test-cluster-secret-32-bytes-long!",
		DataDir:          dataDir,
		BindAddr:         addr,
		Bootstrap:        true,
		ElectionTimeout:  500 * time.Millisecond,
		HeartbeatTimeout: 500 * time.Millisecond,
		Logger:           zerolog.Nop(),
	}, NewClusterFSM(zerolog.Nop()))
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Start(); err == nil {
		node.Stop()
		t.Fatal("Start succeeded with an unopenable log store")
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("Raft port still bound after a failed Start: %v", err)
	}
	ln.Close()
}
