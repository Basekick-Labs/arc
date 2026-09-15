package raft

// Node.Stop must not hold n.mu while it joins the Raft goroutines (#813).
// Every accessor takes n.mu, so an FSM callback that reads the node through
// one of them while Stop held the lock across Shutdown().Error() blocked the
// FSM goroutine that Shutdown was waiting for: a deadlock. No production FSM
// callback reads the node synchronously today (the compactor callback
// spawns goroutines), so this is a guard for the contract, not a repro of a
// shipped bug.

import (
	"os"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

func TestNodeStop_ReturnsWhileAnFSMCallbackReadsRaftState(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raft-stop-lock-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	fsm := NewClusterFSM(zerolog.Nop())
	cfg := &NodeConfig{
		NodeID:           "stop-lock-1",
		SharedSecret:     "test-cluster-secret-32-bytes-long!",
		DataDir:          tmpDir,
		BindAddr:         "127.0.0.1:0",
		Bootstrap:        true,
		ElectionTimeout:  500 * time.Millisecond,
		HeartbeatTimeout: 500 * time.Millisecond,
		Logger:           zerolog.Nop(),
	}
	node, err := NewNode(cfg, fsm)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if err := node.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := node.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	// The callback parks on the FSM goroutine until the gate opens, then
	// reads the node through an accessor that takes n.mu.
	entered := make(chan struct{})
	gate := make(chan struct{})
	callbackDone := make(chan bool, 1)
	fsm.SetCallbacks(func(*NodeInfo) {
		close(entered)
		<-gate
		callbackDone <- node.IsLeader()
	}, nil, nil)

	applyDone := make(chan error, 1)
	go func() {
		applyDone <- node.AddNode(&NodeInfo{ID: "peer", Role: "reader", Address: "127.0.0.1:1"}, 10*time.Second)
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the AddNode callback never ran")
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- node.Stop() }()
	// Stop is inside its Raft join once the instance reports Shutdown;
	// hashicorp sets that state synchronously in Shutdown(). On the pre-fix
	// code this poll blocks on n.mu (Stop holds it across the join), the
	// gate never opens, and Stop never returns: the deadlock shows up as
	// the budget below expiring. The poll runs on its own goroutine so the
	// test reports that instead of hanging with it.
	go func() {
		for node.State() != hraft.Shutdown {
			time.Sleep(5 * time.Millisecond)
		}
		close(gate)
	}()

	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Stop did not return within 15s: n.mu was held across the Raft join while an FSM callback waited for it (#813); rig leaked on purpose")
	}
	select {
	case isLeader := <-callbackDone:
		if isLeader {
			t.Error("IsLeader() reported true during shutdown")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the callback never finished after Stop returned")
	}
	if err := <-applyDone; err != nil {
		t.Logf("AddNode resolved with %v once the callback was released", err)
	}
	if node.IsLeader() {
		t.Error("IsLeader() true after Stop")
	}
	// A restart on the same node is still refused only while stopping; after
	// Stop returned it is allowed, as before.
	if err := node.Start(); err != nil {
		t.Fatalf("Start after Stop: %v", err)
	}
	_ = node.Stop()
}
