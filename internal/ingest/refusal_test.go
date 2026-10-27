package ingest

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func refusal(reason string) error {
	return fmt.Errorf("%w: %w", ErrIngestFenced, &RefusalError{Reason: reason, Text: reason + " text"})
}

func TestRefusalReason(t *testing.T) {
	for want, err := range map[string]error{
		RefusalNotDesignated: refusal(RefusalNotDesignated),
		RefusalContact:       refusal(RefusalContact),
		RefusalOther:         ErrIngestFenced,
		"":                   errors.New("disk full"),
	} {
		if got := RefusalReason(err); got != want {
			t.Errorf("RefusalReason(%v) = %q, want %q", err, got, want)
		}
	}
}

// #1268: one line per reason per window, with the count suppressed since the
// previous line, restarting after each line; a new reason logs at once.
func TestRefusalLogThrottlesPerReason(t *testing.T) {
	buf := &lockedBuf{}
	now := time.Unix(1_000_000, 0)
	defer CaptureRefusalLog(zerolog.New(buf), func() time.Time { return now })()
	lines := func() int { return strings.Count(buf.String(), "Writes refused") }
	err := refusal(RefusalStandby)
	for i := 0; i < 1000; i++ {
		LogRefusal("/w", err)
	}
	if lines() != 1 {
		t.Fatalf("1000 refusals logged %d lines", lines())
	}
	LogRefusal("/w", refusal(RefusalContact))
	if lines() != 2 || !strings.Contains(buf.String(), `"reason":"contact"`) {
		t.Fatalf("a new reason waited for another reason's window:\n%s", buf.String())
	}
	now = now.Add(RefusalLogEvery - time.Second)
	LogRefusal("/w", err)
	if lines() != 2 {
		t.Fatal("logged again inside the window")
	}
	now = now.Add(2 * time.Second)
	LogRefusal("/w", err)
	if lines() != 3 || !strings.Contains(buf.String(), `"suppressed":1000`) {
		t.Fatalf("second standby line missing or wrong count:\n%s", buf.String())
	}
	for i := 0; i < 5; i++ {
		LogRefusal("/w", err)
	}
	now = now.Add(RefusalLogEvery)
	LogRefusal("/w", err)
	if !strings.Contains(buf.String(), `"suppressed":5`) {
		t.Fatalf("the count did not restart after a line:\n%s", buf.String())
	}
	if strings.Contains(buf.String(), `"level":"error"`) {
		t.Fatal("refusals must log at WARN")
	}
}
