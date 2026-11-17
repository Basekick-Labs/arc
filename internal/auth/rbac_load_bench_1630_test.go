package auth

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkLoadTokenRBACData measures the RBAC load on a cache miss for a
// token in five teams with a role each (#1630 PR 2 adds a join on
// api_tokens to the membership query).
func BenchmarkLoadTokenRBACData(b *testing.B) {
	rm, am, cleanup := setupTestRBACManager(b)
	defer cleanup()
	ctx := context.Background()
	org, err := rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "org"})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := am.CreateToken(ctx, "bench", "", "read", nil); err != nil {
		b.Fatal(err)
	}
	var id int64
	if err := am.db.QueryRow(`SELECT id FROM api_tokens WHERE name = 'bench'`).Scan(&id); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		team, err := rm.CreateTeam(ctx, org.ID, &CreateTeamRequest{Name: fmt.Sprintf("t%d", i)})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := rm.CreateRole(ctx, team.ID, &CreateRoleRequest{DatabasePattern: fmt.Sprintf("db%d", i), Permissions: []string{"read"}}); err != nil {
			b.Fatal(err)
		}
		if _, err := rm.AddTokenToTeam(ctx, id, team.ID); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := rm.loadTokenRBACData(id); err != nil {
			b.Fatal(err)
		}
	}
}
