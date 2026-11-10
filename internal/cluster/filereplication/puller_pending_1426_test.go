package filereplication

import "testing"

// Pending names a file enqueued, being pulled, or waiting behind its pull: a
// DELETE with allow_unreachable keeps waiting for it (#1426).
func TestPullerPending(t *testing.T) {
	p := &Puller{inflight: map[string]*pullRequest{"a": nil}, pending: map[string]*pullRequest{"b": nil}}
	for path, want := range map[string]bool{"a": true, "b": true, "c": false} {
		if got := p.Pending(path); got != want {
			t.Errorf("Pending(%q) = %v, want %v", path, got, want)
		}
	}
}
