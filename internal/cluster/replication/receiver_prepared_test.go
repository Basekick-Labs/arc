package replication

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

type preparedWALFixture struct {
	trackedLocalWALFixture
	drop     bool
	payloads []string
}

func (w *preparedWALFixture) AppendRawTracked(payload []byte) ([]string, error) {
	w.trackedCalls++
	w.payloads = append(w.payloads, string(payload))
	if w.drop {
		return nil, wal.ErrWALDropped
	}
	return []string{fmt.Sprintf("part-%d", w.trackedCalls)}, nil
}

func TestPreparedReplicationReleasesOnlyRejectedPart(t *testing.T) {
	w := &preparedWALFixture{}
	var firstOwner []string
	r := &Receiver{ctx: context.Background(), logger: zerolog.Nop(), cfg: &ReceiverConfig{LocalWAL: w,
		IngestHandler: PreparingIngestHandlerFunc(func(context.Context, []byte) ([]PreparedWALIngest, error) {
			return []PreparedWALIngest{
				{Payload: []byte("first"), Apply: func(_ context.Context, ids []string) error { firstOwner = ids; return nil }},
				{Payload: []byte("second"), Apply: func(context.Context, []string) error { return errors.New("rejected before buffering") }},
			}, nil
		})}}
	require.Error(t, r.applyEntry(&ReplicateEntry{Payload: []byte("original")}))
	require.Equal(t, []string{"part-1"}, firstOwner)
	require.Equal(t, []string{"part-2"}, w.forgotten, "must not release a part already owned by another buffer")
	require.Equal(t, []string{"first", "second"}, w.payloads)
	require.Zero(t, r.totalEntriesApplied.Load())
}

func TestPreparedReplicationDropStillAppliesWithoutIdentity(t *testing.T) {
	w := &preparedWALFixture{drop: true}
	applied := false
	r := &Receiver{ctx: context.Background(), logger: zerolog.Nop(), cfg: &ReceiverConfig{LocalWAL: w,
		IngestHandler: PreparingIngestHandlerFunc(func(context.Context, []byte) ([]PreparedWALIngest, error) {
			return []PreparedWALIngest{{Payload: []byte("part"), Apply: func(_ context.Context, ids []string) error {
				require.Empty(t, ids)
				applied = true
				return nil
			}}}, nil
		})}}
	require.NoError(t, r.applyEntry(&ReplicateEntry{Sequence: 1}))
	require.True(t, applied)
	require.Equal(t, int64(1), r.totalLocalWALDropped.Load())
	require.Equal(t, int64(1), r.totalEntriesApplied.Load())
}

func TestPreparedReplicationNoOwnerNeverAppends(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			w := &preparedWALFixture{}
			r := &Receiver{ctx: context.Background(), logger: zerolog.Nop(), cfg: &ReceiverConfig{LocalWAL: w,
				IngestHandler: PreparingIngestHandlerFunc(func(context.Context, []byte) ([]PreparedWALIngest, error) {
					if fail {
						return nil, errors.New("invalid payload")
					}
					return nil, nil
				})}}
			err := r.applyEntry(&ReplicateEntry{})
			require.Equal(t, fail, err != nil)
			require.Zero(t, w.trackedCalls)
			require.Zero(t, w.untrackedCalls)
		})
	}
}
