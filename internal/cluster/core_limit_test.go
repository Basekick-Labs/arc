package cluster

import (
	"errors"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// #869: an unlimited Enterprise license carries max_cores = -1, and the join
// validator was the only reader in the system that treated a non-positive
// limit as a real limit. Every join was then rejected with "cluster core limit
// exceeded ... license limit=-1" and the cluster could never form.
//
// These drive coreLimitError, which is the entire admission policy — the
// method above it only supplies the two numbers. Testing the predicate alone
// would not have caught the bug: the sign convention has to be exercised
// through the decision that uses it.
func TestCoreLimitError(t *testing.T) {
	tests := []struct {
		name         string
		maxCores     int
		currentTotal int
		coreCount    int
		wantErr      bool
	}{
		// The bug. Any node at all was refused on an unlimited license.
		{"unlimited tier (-1) admits the first peer", -1, 14, 14, false},
		{"unlimited tier (-1) admits a large node", -1, 512, 512, false},
		{"unlimited tier (-1) admits the bootstrap node", -1, 0, 8, false},
		{"any other negative is unlimited too", -128, 1000, 1000, false},

		// Zero has always meant unlimited, and still does.
		{"zero admits any node", 0, 4096, 4096, false},

		// A real limit is still a real limit.
		{"under a real limit", 128, 32, 64, false},
		{"exactly at a real limit", 128, 64, 64, false},
		{"one core over a real limit", 128, 64, 65, true},
		{"far over a real limit", 128, 70, 70, true},
		{"a one-core license admits one core", 1, 0, 1, false},
		{"a one-core license refuses two", 1, 0, 2, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := coreLimitError(tt.maxCores, tt.currentTotal, tt.coreCount)
			if (err != nil) != tt.wantErr {
				t.Fatalf("coreLimitError(%d, %d, %d) = %v, wantErr %v",
					tt.maxCores, tt.currentTotal, tt.coreCount, err, tt.wantErr)
			}
			if !tt.wantErr {
				return
			}
			if !errors.Is(err, ErrCoreLimitExceeded) {
				t.Errorf("error should wrap ErrCoreLimitExceeded, got %v", err)
			}
		})
	}
}

// The rejection message is what an operator sees in the log when a cluster
// will not form, so it has to name all four numbers.
func TestCoreLimitErrorMessage(t *testing.T) {
	err := coreLimitError(128, 70, 70)
	if err == nil {
		t.Fatal("expected a rejection")
	}
	msg := err.Error()
	for _, want := range []string{"current cluster cores=70", "new node cores=70", "projected total=140", "license limit=128"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message should contain %q, got: %s", want, msg)
		}
	}
}

// The method itself, not just the policy it delegates to. A nil raftFSM means
// a current total of zero, which is the bootstrap leader admitting the first
// peer — the exact moment #869 refused.
func TestCheckCoreLimit(t *testing.T) {
	c := &Coordinator{logger: zerolog.Nop()}

	if err := c.checkCoreLimit(-1, "n2", 14); err != nil {
		t.Errorf("an unlimited license must admit a joining node, got: %v", err)
	}
	if err := c.checkCoreLimit(0, "n2", 14); err != nil {
		t.Errorf("a zero limit must admit a joining node, got: %v", err)
	}
	if err := c.checkCoreLimit(128, "n2", 14); err != nil {
		t.Errorf("a node well under the limit must be admitted, got: %v", err)
	}
	err := c.checkCoreLimit(8, "n2", 14)
	if err == nil {
		t.Fatal("a node over the limit must be refused")
	}
	if !errors.Is(err, ErrCoreLimitExceeded) {
		t.Errorf("refusal should wrap ErrCoreLimitExceeded, got %v", err)
	}
}
