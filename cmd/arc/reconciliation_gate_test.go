package main

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/reconciliation"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

type fakeGateCoordinator struct {
	active bool
	ready  bool
}

func (f fakeGateCoordinator) IsActiveCompactor() bool   { return f.active }
func (f fakeGateCoordinator) ReplicationReady() bool    { return f.ready }
func (f fakeGateCoordinator) GetRole() cluster.NodeRole { return cluster.RoleWriter }

// On per-node storage the manifest sweep waits for file replication to
// converge on this node (#959); the storage scan never waits. The hold is
// armed only when replication and its catch-up walker are both enabled,
// since with the walker off readiness is never reached. Shared storage keeps
// its one-node-sweeps rule for both halves.
func TestReconciliationGate_LocalHoldsManifestSweepUntilCaughtUp(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		shared               bool
		replication, catchUp bool
		active, ready        bool
		wantScan, wantSweep  bool
	}{
		{"local, replicating, not caught up", false, true, true, false, false, true, false},
		{"local, replicating, caught up", false, true, true, false, true, true, true},
		{"local, replicating, walker disabled, not caught up", false, true, false, false, false, true, true},
		{"local, no replication, not ready", false, false, true, false, false, true, true},
		{"shared, active compactor, not caught up", true, true, true, true, false, true, true},
		{"shared, not the active compactor, caught up", true, true, true, false, true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newReconciliationClusterGate(fakeGateCoordinator{active: tc.active, ready: tc.ready}, tc.shared, tc.replication, tc.catchUp)
			if got := g.ShouldRunStorageScan(); got != tc.wantScan {
				t.Errorf("ShouldRunStorageScan = %v, want %v", got, tc.wantScan)
			}
			if got := g.ShouldRunManifestSweep(); got != tc.wantSweep {
				t.Errorf("ShouldRunManifestSweep = %v, want %v", got, tc.wantSweep)
			}
		})
	}
}

// fakeObjectStore answers every request with 200. Both object-store
// constructors probe their bucket or container before returning; a local
// server keeps the probe offline and instant.
func fakeObjectStore(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// The reconciler's topology comes from the backend main.go built, for every
// spelling storage.backend accepts. It used to come from the config string
// through a list that knew "azure" but not "azblob", so every node of an
// azblob cluster swept the one container as if it were its own disk, without
// the pending-registration guard the publisher side had switched on.
func TestReconciliationTopology_EverySpellingOfASharedStoreIsShared(t *testing.T) {
	// NewS3Backend loads the AWS default config; keep the host's out of it.
	absent := t.TempDir()
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_DEFAULT_PROFILE", "")
	t.Setenv("AWS_CA_BUNDLE", "")
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(absent, "no-such-aws-config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(absent, "no-such-aws-credentials"))

	s3Spec := func(typ string) storage.BackendSpec {
		return storage.BackendSpec{Type: typ, S3: storage.S3Config{
			Bucket: "arc-rec-test", Region: "us-east-1", Endpoint: fakeObjectStore(t),
			AccessKey: "rectest", SecretKey: "rectest", PathStyle: true,
		}}
	}
	azureSpec := func(typ string) storage.BackendSpec {
		return storage.BackendSpec{Type: typ, Azure: storage.AzureBlobConfig{
			AccountName: "devstoreaccount1",
			// The public Azurite development key; it only has to decode.
			AccountKey:    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
			ContainerName: "arc-rec-test",
			Endpoint:      fakeObjectStore(t),
		}}
	}
	for _, tc := range []struct {
		spec       storage.BackendSpec
		wantShared bool
	}{
		{storage.BackendSpec{Type: "local", LocalPath: t.TempDir()}, false},
		{s3Spec("s3"), true},
		{s3Spec("minio"), true},
		{azureSpec("azure"), true},
		{azureSpec("azblob"), true},
	} {
		t.Run(tc.spec.Type, func(t *testing.T) {
			backend, err := storage.NewBackend(tc.spec, zerolog.Nop())
			if err != nil {
				t.Fatalf("NewBackend(%q): %v", tc.spec.Type, err)
			}
			// Replication and catch-up on: a backend misread as local would
			// also arm the per-node replication hold.
			gate, kind := newReconciliationTopology(fakeGateCoordinator{}, backend, true, true)
			wantKind := reconciliation.BackendLocal
			if tc.wantShared {
				wantKind = reconciliation.BackendShared
			}
			if kind != wantKind {
				t.Errorf("backend kind = %q, want %q", kind, wantKind)
			}
			if gate.shared != tc.wantShared {
				t.Errorf("gate.shared = %v, want %v", gate.shared, tc.wantShared)
			}
			if gate.holdSweepUntilCaughtUp == tc.wantShared {
				t.Errorf("holdSweepUntilCaughtUp = %v on shared=%v", gate.holdSweepUntilCaughtUp, tc.wantShared)
			}
			// Not the active compactor: a shared store must not scan here.
			if got := gate.ShouldRunStorageScan(); got == tc.wantShared {
				t.Errorf("ShouldRunStorageScan = %v on a non-compactor node, shared=%v", got, tc.wantShared)
			}
		})
	}
}

// Shared storage reconciles only on the compactor lease holder, and only
// compactor failover assigns a lease, so without it nothing ever reconciles.
// The scheduler logs a gated tick at Debug, so startup has to say so.
func TestReconciliationStartupWarnings(t *testing.T) {
	for _, tc := range []struct {
		name                                   string
		shared, replication, catchUp, failover bool
		wantLease, wantCatchUp                 bool
	}{
		{"shared without failover", true, false, false, false, true, false},
		{"shared with failover", true, false, false, true, false, false},
		{"shared, replication without catch-up", true, true, false, true, false, false},
		{"local, replication without catch-up", false, true, false, false, false, true},
		{"local, replication with catch-up", false, true, true, false, false, false},
		{"local, no replication", false, false, false, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gotLease, gotCatchUp bool
			for _, msg := range reconciliationStartupWarnings(tc.shared, tc.replication, tc.catchUp, tc.failover) {
				switch {
				case strings.Contains(msg, "cluster.failover_enabled"):
					gotLease = true
				case strings.Contains(msg, "cluster.replication_catchup_enabled=false"):
					gotCatchUp = true
				default:
					t.Errorf("unexpected warning %q", msg)
				}
			}
			if gotLease != tc.wantLease || gotCatchUp != tc.wantCatchUp {
				t.Errorf("lease warning %v (want %v), catch-up warning %v (want %v)", gotLease, tc.wantLease, gotCatchUp, tc.wantCatchUp)
			}
		})
	}
}
