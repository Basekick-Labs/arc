package replication

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"
)

type trackedLocalWALFixture struct {
	trackedCalls   int
	untrackedCalls int
	forgotten      []string
	hashes         []string
}

func (w *trackedLocalWALFixture) AppendRaw([]byte) error {
	w.untrackedCalls++
	return nil
}

func (w *trackedLocalWALFixture) AppendRawTracked([]byte) ([]string, error) {
	w.trackedCalls++
	return append([]string(nil), w.hashes...), nil
}

func (w *trackedLocalWALFixture) ForgetTracked(hashes []string) {
	w.forgotten = append(w.forgotten, hashes...)
}

func TestApplyEntryTracksFollowerWALThroughIngestFlushIdentity(t *testing.T) {
	localWAL := &trackedLocalWALFixture{hashes: []string{"tracked-id"}}
	var appliedHashes []string
	receiver := &Receiver{
		ctx:    context.Background(),
		logger: zerolog.Nop(),
		cfg: &ReceiverConfig{
			LocalWAL: localWAL,
			IngestHandler: IngestHandlerWithWALFunc(func(_ context.Context, _ []byte, hashes []string) error {
				appliedHashes = append([]string(nil), hashes...)
				return nil
			}),
		},
	}

	if err := receiver.applyEntry(&ReplicateEntry{Payload: []byte("payload")}); err != nil {
		t.Fatalf("applyEntry: %v", err)
	}
	if localWAL.trackedCalls != 1 || localWAL.untrackedCalls != 0 {
		t.Fatalf("WAL calls: tracked=%d untracked=%d, want tracked only", localWAL.trackedCalls, localWAL.untrackedCalls)
	}
	if len(appliedHashes) != 1 || appliedHashes[0] != "tracked-id" {
		t.Fatalf("ingest hashes = %v, want [tracked-id]", appliedHashes)
	}
	if len(localWAL.forgotten) != 0 {
		t.Fatalf("ForgetTracked called on successful ingest: %v", localWAL.forgotten)
	}
}

func TestApplyEntryReleasesFollowerWALIdentityWhenIngestRejects(t *testing.T) {
	localWAL := &trackedLocalWALFixture{hashes: []string{"tracked-id"}}
	receiver := &Receiver{
		ctx:    context.Background(),
		logger: zerolog.Nop(),
		cfg: &ReceiverConfig{
			LocalWAL: localWAL,
			IngestHandler: IngestHandlerWithWALFunc(func(context.Context, []byte, []string) error {
				return errors.New("ingest rejected")
			}),
		},
	}

	if err := receiver.applyEntry(&ReplicateEntry{Payload: []byte("payload")}); err == nil {
		t.Fatal("applyEntry returned nil after ingest rejection")
	}
	if len(localWAL.forgotten) != 1 || localWAL.forgotten[0] != "tracked-id" {
		t.Fatalf("ForgetTracked hashes = %v, want [tracked-id]", localWAL.forgotten)
	}
}
