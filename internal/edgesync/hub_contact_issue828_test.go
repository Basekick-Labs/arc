package edgesync

import (
	"context"
	"testing"
)

func TestAgentHubContactRequiresReconcileIssue828(t *testing.T) {
	rig := newAgentRig(t)

	empty, err := rig.agent.Run(context.Background())
	if err != nil {
		t.Fatalf("empty pass: %v", err)
	}
	if empty.HubContacted {
		t.Fatal("empty backlog incorrectly reported hub contact")
	}

	rig.writeFile(t, agentPath, []byte("isolated test payload"))

	synced, err := rig.agent.Run(context.Background())
	if err != nil {
		t.Fatalf("sync pass: %v", err)
	}
	if !synced.HubContacted || synced.Sent != 1 {
		t.Fatalf("contact=%v sent=%d, want true/1",
			synced.HubContacted, synced.Sent)
	}

	emptyAgain, err := rig.agent.Run(context.Background())
	if err != nil {
		t.Fatalf("subsequent empty pass: %v", err)
	}
	if emptyAgain.HubContacted {
		t.Fatal("subsequent empty pass incorrectly reported hub contact")
	}
}
