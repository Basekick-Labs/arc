package cluster

import (
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
)

func TestStartReplicationWriterRequiresLocalWAL(t *testing.T) {
	coordinator := &Coordinator{
		cfg:       &config.ClusterConfig{ReplicationEnabled: true},
		localNode: NewNode("writer-1", "writer-1", RoleWriter, "test-cluster"),
		logger:    zerolog.Nop(),
	}

	err := coordinator.StartReplication()
	if err == nil || !strings.Contains(err.Error(), "WAL is disabled") {
		t.Fatalf("StartReplication() error = %v, want a WAL-disabled misconfiguration", err)
	}
}

func TestReceiverWALWriterReturnsNilInterfaceWhenWALDisabled(t *testing.T) {
	var walWriter *wal.Writer
	if got := receiverWALWriter(walWriter); got != nil {
		t.Fatalf("receiverWALWriter(nil) = %#v, want nil interface", got)
	}
}
