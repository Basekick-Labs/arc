package ingest

import (
	"context"
	"testing"

	"github.com/basekick-labs/arc/internal/replicaview"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

type becomingDurablePublisher struct {
	checks     int
	knownAfter int
}

func (p *becomingDurablePublisher) PublishReplicationFile(context.Context, string, replicaview.FileMetadata, int64, string) error {
	return nil
}
func (p *becomingDurablePublisher) HasEntry(string, string, string) bool {
	p.checks++
	return p.checks >= p.knownAfter
}

func TestReplicatedRetryCheckpointsAlreadyDurableRows(t *testing.T) {
	for _, duringFlush := range []bool{false, true} {
		t.Run(map[bool]string{false: "already_durable", true: "publication_raced_with_apply"}[duringFlush], func(t *testing.T) {
			w := &checkpointRecordingWAL{}
			publisher := &becomingDurablePublisher{knownAfter: 1}
			buffer := &ArrowBuffer{wal: w, logger: zerolog.Nop(), replicationPublisher: publisher}
			const id = "00000000000000010000000000000001"
			if duringFlush {
				publisher.knownAfter = 2
				buffer.replicaInFlight.Store("db\x00cpu\x00"+id, struct{}{})
			}
			require.NoError(t, buffer.WriteReplicatedColumnar(context.Background(), "db", "cpu", map[string][]interface{}{"time": {int64(1700000000000000)}}, id))
			require.Equal(t, []string{id}, w.flushed, "retry WAL must not remain pinned after durable duplicate suppression")
		})
	}
}
