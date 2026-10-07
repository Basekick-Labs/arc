package main

import (
	"errors"
	"testing"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/wal"
)

type recordingClusterReplicationStarter struct {
	walSet     bool
	bufferSet  bool
	startCalls int
	startErr   error
}

func (r *recordingClusterReplicationStarter) SetWAL(*wal.Writer) {
	r.walSet = true
}

func (r *recordingClusterReplicationStarter) SetIngestBuffer(*ingest.ArrowBuffer) {
	r.bufferSet = true
}

func (r *recordingClusterReplicationStarter) StartReplication() error {
	r.startCalls++
	return r.startErr
}

func TestStartClusterWALReplicationStartsWithoutLocalWAL(t *testing.T) {
	coordinator := &recordingClusterReplicationStarter{}

	if err := startClusterWALReplication(coordinator, nil, nil); err != nil {
		t.Fatalf("startClusterWALReplication() error = %v", err)
	}
	if coordinator.walSet {
		t.Fatal("SetWAL called without a local WAL")
	}
	if !coordinator.bufferSet {
		t.Fatal("SetIngestBuffer was not called for a node without a local WAL")
	}
	if coordinator.startCalls != 1 {
		t.Fatalf("StartReplication calls = %d, want 1", coordinator.startCalls)
	}
}

func TestStartClusterWALReplicationPropagatesStartError(t *testing.T) {
	wantErr := errors.New("start failed")
	coordinator := &recordingClusterReplicationStarter{startErr: wantErr}

	if err := startClusterWALReplication(coordinator, nil, nil); !errors.Is(err, wantErr) {
		t.Fatalf("startClusterWALReplication() error = %v, want %v", err, wantErr)
	}
}
