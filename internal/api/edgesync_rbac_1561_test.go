package api

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/edgesync"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// #1561: the edge-sync hub endpoints gated on the coarse write bit and checked
// no RBAC grant, so a team-member token was not narrowed by its grants there.
// A pushed file lands at {spoke_id}/{spoke db}/..., served as database
// {spoke_id} with the spoke's database as the measurement; that is the pair
// RBAC is now checked against, after the request MAC.
//
// Real AuthManager, RBACManager and auth middleware; signed requests.

type syncRBACRig struct {
	rig    *syncTestRig
	tokens map[string]string
}

func newSyncRBACRig(t *testing.T) *syncRBACRig {
	t.Helper()
	ctx := context.Background()
	am, err := auth.NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), 5*time.Minute, 100, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { am.Close() })
	rm := auth.NewRBACManager(&auth.RBACManagerConfig{DB: am.GetDB(), Logger: zerolog.Nop()})
	t.Cleanup(func() { rm.Close() })
	org, err := rm.CreateOrganization(ctx, &auth.CreateOrganizationRequest{Name: "o"})
	if err != nil {
		t.Fatal(err)
	}
	team := func(name, dbPattern string, measPattern string) int64 {
		t.Helper()
		tm, err := rm.CreateTeam(ctx, org.ID, &auth.CreateTeamRequest{Name: name})
		if err != nil {
			t.Fatal(err)
		}
		role, err := rm.CreateRole(ctx, tm.ID, &auth.CreateRoleRequest{DatabasePattern: dbPattern, Permissions: []string{"write"}})
		if err != nil {
			t.Fatal(err)
		}
		if measPattern != "" {
			if _, err := rm.CreateMeasurementPermission(ctx, role.ID, &auth.CreateMeasurementPermissionRequest{
				MeasurementPattern: measPattern, Permissions: []string{"write"},
			}); err != nil {
				t.Fatal(err)
			}
		}
		return tm.ID
	}
	tokens := map[string]string{}
	token := func(name, perms string, teamID int64) {
		t.Helper()
		tok, err := am.CreateToken(ctx, name, name, perms, nil)
		if err != nil {
			t.Fatal(err)
		}
		if teamID != 0 {
			info := am.VerifyToken(tok)
			if _, err := rm.AddTokenToTeam(ctx, info.ID, teamID); err != nil {
				t.Fatal(err)
			}
		}
		tokens[name] = tok
	}
	token("coarse-no-team", "write", 0)
	token("rbac-spoke", auth.PermissionsNone, team("spoke", testSpokeID, ""))
	token("rbac-spoke-metrics", auth.PermissionsNone, team("meas", testSpokeID, "metrics"))
	token("rbac-other-db", auth.PermissionsNone, team("other", "otherdb", ""))
	// The issue's token: holds the coarse bit, but its team grants another db.
	token("coarse-team-other-db", "write", team("other2", "otherdb", ""))
	token("none", auth.PermissionsNone, 0)
	rm.InvalidateAllCache()

	rig := buildSyncTestRig(t, StaticSpokeSecrets(map[string]string{testSpokeID: testSecret}), am, rm)
	return &syncRBACRig{rig: rig, tokens: tokens}
}

func (r *syncRBACRig) push(t *testing.T, token, path string, body []byte) (int, map[string]any) {
	t.Helper()
	req := defaultReq(body)
	req.path = path
	req.token = r.tokens[token]
	resp := req.do(t, r.rig)
	return resp.StatusCode, decodeBody(t, resp)
}

func TestEdgeSyncPushIsDecidedByRBACOnTheSpokeNamespace(t *testing.T) {
	r := newSyncRBACRig(t)
	// Each case pushes its own file (the case index is in the name), so one
	// case's stored file cannot answer for another's.
	const metricsPath = "metrics/cpu/2026/08/07/14/cpu_%d.parquet"
	const logsPath = "logs/app/2026/08/07/14/app_%d.parquet"
	for i, c := range []struct {
		token, path string
		status      int
	}{
		{"coarse-no-team", metricsPath, 200},
		{"rbac-spoke", metricsPath, 200},
		{"rbac-spoke-metrics", metricsPath, 200},
		// Measurement grant on "metrics" only: the spoke's "logs" database
		// is another measurement of the {spoke_id} database.
		{"rbac-spoke-metrics", logsPath, 403},
		{"rbac-other-db", metricsPath, 403},
		{"coarse-team-other-db", metricsPath, 403},
		// Refused at the door: no write grant anywhere.
		{"none", metricsPath, 403},
	} {
		path := fmt.Sprintf(c.path, i)
		t.Run(c.token+" "+path, func(t *testing.T) {
			body := []byte("payload " + c.token + path)
			status, got := r.push(t, c.token, path, body)
			if status != c.status {
				t.Fatalf("status %d, want %d: %v", status, c.status, got)
			}
			stored, err := r.rig.backend.Exists(context.Background(), edgesync.NamespacedPath(testSpokeID, path))
			if err != nil {
				t.Fatal(err)
			}
			if c.status != 200 && stored {
				t.Fatal("a refused push was stored")
			}
		})
	}
}

// RBAC is not consulted before the MAC: a token without a grant and a forged
// MAC gets the MAC's 401, never a 403 that would answer a question about
// grants to a caller without the spoke's secret.
func TestEdgeSyncRBACCheckRunsAfterTheMAC(t *testing.T) {
	r := newSyncRBACRig(t)
	req := defaultReq([]byte("x"))
	req.token = r.tokens["rbac-other-db"]
	req.secret = "not-the-spoke-secret"
	if resp := req.do(t, r.rig); resp.StatusCode != fiber.StatusUnauthorized {
		t.Fatalf("status %d, want 401", resp.StatusCode)
	}
}

// A single-segment spoke path names no measurement to check a grant for.
func TestEdgeSyncRefusesAPathWithNoMeasurement(t *testing.T) {
	r := newSyncRBACRig(t)
	if status, got := r.push(t, "coarse-no-team", "x.parquet", []byte("x")); status != fiber.StatusBadRequest {
		t.Fatalf("status %d, want 400: %v", status, got)
	}
}

// Reconcile answers entries the token may not write as missing, without
// looking them up, and keeps answering the rest: one ungranted measurement
// must not stall the spoke's queue.
func TestEdgeSyncReconcileHidesUngrantedEntries(t *testing.T) {
	r := newSyncRBACRig(t)
	metrics := []byte("metrics payload")
	logs := []byte("logs payload")
	const metricsPath = "metrics/cpu/2026/08/07/14/cpu_1.parquet"
	const logsPath = "logs/app/2026/08/07/14/app_1.parquet"
	for path, body := range map[string][]byte{metricsPath: metrics, logsPath: logs} {
		if status, got := r.push(t, "coarse-no-team", path, body); status != 200 {
			t.Fatalf("seed %s: %d %v", path, status, got)
		}
	}
	entries := []edgesync.ReconcileEntry{
		{Path: metricsPath, SHA256: testDigest(metrics)},
		{Path: logsPath, SHA256: testDigest(logs)},
	}
	reconcile := func(token string) (int, map[string]any) {
		req := defaultReconcileReq(entries)
		req.token = r.tokens[token]
		resp := req.do(t, r.rig)
		return resp.StatusCode, decodeBody(t, resp)
	}
	status, got := reconcile("rbac-spoke-metrics")
	if status != 200 {
		t.Fatalf("status %d: %v", status, got)
	}
	present, _ := got["present"].([]any)
	missing, _ := got["missing"].([]any)
	if len(present) != 1 || present[0] != metricsPath {
		t.Errorf("present = %v, want only the granted measurement's file", present)
	}
	if len(missing) != 1 || missing[0] != logsPath {
		t.Errorf("missing = %v, want the ungranted file answered as missing", missing)
	}
	// A token granted nothing on the spoke learns nothing about it.
	if status, got := reconcile("coarse-team-other-db"); status != 200 || len(got["present"].([]any)) != 0 {
		t.Fatalf("token with no grant on the spoke: %d %v; want 200 with nothing present", status, got)
	}
	// Without a team, the coarse bit decides, as before.
	if status, got := reconcile("coarse-no-team"); status != 200 || len(got["present"].([]any)) != 2 {
		t.Fatalf("coarse token: %d %v; want both present", status, got)
	}
}

// unavailableRBAC is a checker whose store cannot be read.
type unavailableRBAC struct{ *mockRBACChecker }

func (unavailableRBAC) CheckPermission(*auth.PermissionCheckRequest) *auth.PermissionCheckResult {
	return &auth.PermissionCheckResult{Allowed: false, Source: auth.SourceUnavailable, Reason: "store down"}
}

func (unavailableRBAC) HasPermissionAnywhere(*auth.TokenInfo, string) (bool, error) { return true, nil }

// Grants that cannot be read are 503 (a hub-side condition), not a 403 that
// says the token lacks a grant.
func TestEdgeSyncUnreadableGrantsAre503(t *testing.T) {
	am, err := auth.NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), 5*time.Minute, 100, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { am.Close() })
	tok, err := am.CreateToken(context.Background(), "w", "w", "write", nil)
	if err != nil {
		t.Fatal(err)
	}
	rig := buildSyncTestRig(t, StaticSpokeSecrets(map[string]string{testSpokeID: testSecret}), am, unavailableRBAC{&mockRBACChecker{}})
	push := defaultReq([]byte("x"))
	push.token = tok
	if resp := push.do(t, rig); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("push: status %d, want 503", resp.StatusCode)
	}
	rec := defaultReconcileReq([]edgesync.ReconcileEntry{{Path: testSyncPth, SHA256: testDigest([]byte("x"))}})
	rec.token = tok
	if resp := rec.do(t, rig); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("reconcile: status %d, want 503", resp.StatusCode)
	}
}
