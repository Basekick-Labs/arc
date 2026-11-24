package cluster

import (
	"encoding/json"
	"fmt"

	clusterraft "github.com/basekick-labs/arc/internal/cluster/raft"
)

// isRehomeDriveCommand: the commands a re-home job's driver proposes (#1825):
// the primary writer stages, and the finisher advances, claims and ends.
func isRehomeDriveCommand(t clusterraft.CommandType) bool {
	return t == clusterraft.CommandStageRehome ||
		t == clusterraft.CommandAdvanceRehome ||
		t == clusterraft.CommandClaimRehomeFinisher ||
		t == clusterraft.CommandEndRehome
}

// rehomeCommandSpeaksFor checks a forwarded re-home command names the
// requesting node: the stage's origin, the driver's by, an ack's node. A
// consistency check, not sender authentication (#1510).
func rehomeCommandSpeaksFor(cmd *clusterraft.Command, nodeID string) error {
	var who string
	switch cmd.Type {
	case clusterraft.CommandStageRehome:
		var p clusterraft.StageRehomePayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return err
		}
		who = p.Origin
	case clusterraft.CommandAdvanceRehome:
		var p clusterraft.AdvanceRehomePayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return err
		}
		who = p.By
	case clusterraft.CommandClaimRehomeFinisher:
		var p clusterraft.ClaimRehomeFinisherPayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return err
		}
		who = p.By
	case clusterraft.CommandEndRehome:
		var p clusterraft.EndRehomePayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return err
		}
		who = p.By
	case clusterraft.CommandAckRehome:
		var p clusterraft.AckRehomePayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return err
		}
		who = p.Node
	default:
		return fmt.Errorf("command %d is not a re-home command", cmd.Type)
	}
	if who == "" || who != nodeID {
		return fmt.Errorf("re-home command names %q, not the requesting node", who)
	}
	return nil
}
