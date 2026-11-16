package raft

import (
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// #1728: a notice calls the observer and changes nothing; an invalid one is
// refused without calling it.
func TestIcebergNoticeApply(t *testing.T) {
	f := NewClusterFSM(zerolog.Nop())
	var got []string
	f.SetIcebergPathsCallback(func(k []string) { got = append(got, k...) })
	if res := applyCmd(t, f, 1, CommandIcebergPathsChanged, IcebergPathsChangedPayload{Keys: []string{"db/cpu/", "spoke/db/m/"}}); res != nil {
		t.Fatalf("valid notice: %v", res)
	}
	if strings.Join(got, ",") != "db/cpu/,spoke/db/m/" {
		t.Fatalf("observer got %v", got)
	}
	got = nil
	big := make([]string, MaxIcebergNoticeKeys+1)
	for i := range big {
		big[i] = "db/m/"
	}
	long := strings.Repeat("a/", 600) // 1200 bytes
	many := make([]string, 100)
	for i := range many {
		many[i] = strings.Repeat("x", 700) + "/"
	}
	for name, keys := range map[string][]string{
		"none": nil, "too many": big, "traversal": {"db/../m/"}, "absolute": {"/db/m/"},
		"no slash": {"db/m"}, "empty segment": {"db//m/"}, "dot": {"./m/"}, "backslash": {"db\\m/"},
		"nul": {"db/\x00/"}, "long key": {long}, "too many bytes": many,
	} {
		if res := applyCmd(t, f, 2, CommandIcebergPathsChanged, IcebergPathsChangedPayload{Keys: keys}); res == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if len(got) != 0 {
		t.Fatalf("observer called for an invalid notice: %v", got)
	}
	if n := len(f.GetAllNodes()); n != 0 {
		t.Fatalf("a notice changed state: %d nodes", n)
	}
}
