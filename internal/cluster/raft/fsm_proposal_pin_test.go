package raft

import (
	"testing"

	"github.com/basekick-labs/arc/internal/auth"
)

// TestProposalCommandTypesMatchFSM pins the wire-format contract between
// internal/auth's ProposalCommand* constants and internal/cluster/raft's
// CommandType enum.
//
// internal/auth deliberately does NOT import internal/cluster/raft (would
// cycle via internal/cluster importing internal/auth), so the constants
// are mirrored by hand. A silent drift would route an apply to the wrong
// FSM handler or be rejected as unknown. This test runs on every CI run
// and fails loudly the moment the two sides disagree.
//
// If you add a new CommandType to fsm.go, add the matching
// ProposalCommand* in raft_proposer.go and an entry to the table below.
func TestProposalCommandTypesMatchFSM(t *testing.T) {
	cases := []struct {
		name     string
		fsmCmd   CommandType
		authCmd  uint8
		expected uint8
	}{
		// Phase A — tokens (5)
		{"CreateToken", CommandCreateToken, auth.ProposalCommandCreateToken, 12},
		{"UpdateToken", CommandUpdateToken, auth.ProposalCommandUpdateToken, 13},
		{"RevokeToken", CommandRevokeToken, auth.ProposalCommandRevokeToken, 14},
		{"DeleteToken", CommandDeleteToken, auth.ProposalCommandDeleteToken, 15},
		{"RotateToken", CommandRotateToken, auth.ProposalCommandRotateToken, 16},

		// Phase A.1 — RBAC (13)
		{"CreateOrganization", CommandCreateOrganization, auth.ProposalCommandCreateOrganization, 17},
		{"UpdateOrganization", CommandUpdateOrganization, auth.ProposalCommandUpdateOrganization, 18},
		{"DeleteOrganization", CommandDeleteOrganization, auth.ProposalCommandDeleteOrganization, 19},
		{"CreateTeam", CommandCreateTeam, auth.ProposalCommandCreateTeam, 20},
		{"UpdateTeam", CommandUpdateTeam, auth.ProposalCommandUpdateTeam, 21},
		{"DeleteTeam", CommandDeleteTeam, auth.ProposalCommandDeleteTeam, 22},
		{"CreateRole", CommandCreateRole, auth.ProposalCommandCreateRole, 23},
		{"UpdateRole", CommandUpdateRole, auth.ProposalCommandUpdateRole, 24},
		{"DeleteRole", CommandDeleteRole, auth.ProposalCommandDeleteRole, 25},
		{"CreateMeasurementPermission", CommandCreateMeasurementPermission, auth.ProposalCommandCreateMeasurementPermission, 26},
		{"DeleteMeasurementPermission", CommandDeleteMeasurementPermission, auth.ProposalCommandDeleteMeasurementPermission, 27},
		{"AddTokenToTeam", CommandAddTokenToTeam, auth.ProposalCommandAddTokenToTeam, 28},
		{"RemoveTokenFromTeam", CommandRemoveTokenFromTeam, auth.ProposalCommandRemoveTokenFromTeam, 29},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if uint8(tc.fsmCmd) != tc.expected {
				t.Errorf("FSM CommandType for %s = %d, want %d", tc.name, uint8(tc.fsmCmd), tc.expected)
			}
			if tc.authCmd != tc.expected {
				t.Errorf("auth.ProposalCommand for %s = %d, want %d", tc.name, tc.authCmd, tc.expected)
			}
			if uint8(tc.fsmCmd) != tc.authCmd {
				t.Errorf("FSM(%d) and auth(%d) disagree for %s", uint8(tc.fsmCmd), tc.authCmd, tc.name)
			}
		})
	}
}
