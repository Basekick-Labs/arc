package replication

import (
	"sync"
	"testing"

	"github.com/rs/zerolog"
)

// Simultaneous ingest handlers must never expose a backwards sequence to the
// receiver. Drain the actual publication queue after concurrent writers have
// finished so no scheduling of the consumer can conceal an inversion.
func TestSenderConcurrentPublicationOrder(t *testing.T) {
	const workers, perWorker = 32, 1000
	sender := NewSender(&SenderConfig{BufferSize: workers * perWorker, Logger: zerolog.Nop()})
	sender.running.Store(true)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < perWorker; i++ {
				sender.Replicate(&ReplicateEntry{Payload: []byte{1}})
			}
		}()
	}
	close(start)
	wg.Wait()
	for want := uint64(1); want <= workers*perWorker; want++ {
		select {
		case entry := <-sender.entryChan:
			if entry.Sequence != want {
				t.Fatalf("wire publication sequence = %d, want %d", entry.Sequence, want)
			}
		default:
			t.Fatalf("missing publication %d", want)
		}
	}
	if dropped := sender.totalEntriesDropped.Load(); dropped != 0 {
		t.Fatalf("dropped %d entries", dropped)
	}
}
