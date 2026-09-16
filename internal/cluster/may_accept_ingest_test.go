package cluster

// MayAcceptIngest answers "should a load balancer send writes here", which is
// a different question from IsPrimaryWriter. Pointing a write pool at
// IsPrimaryWriter would collapse a shared-storage cluster of N writers onto
// the single node that runs singleton work (#857).

import (
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

func ingestCoordinator(t *testing.T, role NodeRole, shared bool, withFailover bool, primary bool) *Coordinator {
	t.Helper()
	local := NewNode("node-1", "node-1", role, "test-cluster")
	local.UpdateState(StateHealthy)
	if primary {
		local.SetWriterState(WriterStatePrimary)
	}
	reg := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: zerolog.Nop()})
	c := &Coordinator{
		cfg:       &config.ClusterConfig{SharedStorageMode: shared},
		registry:  reg,
		localNode: local,
		logger:    zerolog.Nop(),
	}
	if withFailover {
		c.writerFailoverMgr = NewWriterFailoverManager(&WriterFailoverConfig{Registry: reg, Logger: zerolog.Nop()})
	}
	return c
}

func TestMayAcceptIngest_ByRoleAndMode(t *testing.T) {
	cases := []struct {
		name         string
		role         NodeRole
		shared       bool
		withFailover bool
		primary      bool
		want         bool
	}{
		{name: "reader never takes writes", role: RoleReader, want: false},
		{name: "compactor never takes writes", role: RoleCompactor, want: false},
		{name: "standalone is the whole deployment", role: RoleStandalone, want: true},
		{name: "writer without a failover manager", role: RoleWriter, want: true},
		{name: "shared storage: every writer takes writes", role: RoleWriter, shared: true, withFailover: false, want: true},
		{name: "shared storage: a non-leader writer still takes writes", role: RoleWriter, shared: true, withFailover: true, want: true},
		{name: "shared storage: a reader still does not", role: RoleReader, shared: true, want: false},
		{name: "local storage: the elected primary takes writes", role: RoleWriter, withFailover: true, primary: true, want: true},
		{name: "local storage: a standby writer does not", role: RoleWriter, withFailover: true, primary: false, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := ingestCoordinator(t, tc.role, tc.shared, tc.withFailover, tc.primary)
			if got := c.MayAcceptIngest(); got != tc.want {
				t.Errorf("MayAcceptIngest() = %v; want %v", got, tc.want)
			}
		})
	}
}

// The two predicates must differ exactly where the pattern needs them to: in
// shared storage every healthy writer takes writes while only one runs the
// singleton work.
func TestMayAcceptIngest_DiffersFromIsPrimaryWriterInSharedStorage(t *testing.T) {
	c := ingestCoordinator(t, RoleWriter, true, true, false)
	if !c.MayAcceptIngest() {
		t.Error("a shared-storage writer must accept writes")
	}
	if c.IsPrimaryWriter() {
		t.Error("precondition: this node is not the Raft leader, so it is not the singleton runner")
	}
}

// A nil local node must not panic the health endpoint.
func TestMayAcceptIngest_NilLocalNode(t *testing.T) {
	c := &Coordinator{cfg: &config.ClusterConfig{}, logger: zerolog.Nop()}
	if c.MayAcceptIngest() {
		t.Error("a coordinator with no local node reported itself a write target")
	}
}
