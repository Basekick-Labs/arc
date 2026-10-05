package main

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/shutdown"
	"github.com/rs/zerolog"
)

// TestClusterShutdownOrderKeepsRaftAliveForTheFinalFlush models the cluster
// node's shutdown sequence with the same registration helpers main.go uses,
// in main.go's registration order, and checks the invariants the sequence
// exists for:
//
//   - the replication receiver (a hook) is gone before the Arrow buffer
//     closes (#853);
//   - the file registrar drains after the Arrow buffer's final flush and
//     while the coordinator — Raft — is still up, so the final files reach
//     the manifest before the WAL that covers them is purged (#1014);
//   - the coordinator stops before tiering, so its delete drain reports to a
//     live tiering drainer (#1062).
//
// Every hook runs before every component, so the coordinator and tiering
// have to be components for any of this to hold; registering either as a
// hook fails this test.
func TestClusterShutdownOrderKeepsRaftAliveForTheFinalFlush(t *testing.T) {
	coordinator := shutdown.New(5*time.Second, zerolog.Nop())
	var order []string
	receiverRunning := true
	raftAlive := true
	var queued, manifest []string

	// main.go registration order: arrow-buffer, wal-purge, the scheduler
	// hooks, the cluster block, tiering.
	coordinator.Register("arrow-buffer", shutdownFunc(func() error {
		order = append(order, "arrow-buffer")
		if receiverRunning {
			return fmt.Errorf("arrow-buffer closed with the replication receiver still applying into it")
		}
		queued = append(queued, "shutdown.parquet")
		return nil
	}), shutdown.PriorityBuffer)

	coordinator.Register("wal-purge", shutdownFunc(func() error {
		order = append(order, "wal-purge")
		if !reflect.DeepEqual(manifest, []string{"shutdown.parquet"}) {
			return fmt.Errorf("WAL purged before the manifest held the final file: manifest = %v", manifest)
		}
		return nil
	}), shutdown.PriorityBuffer+5)

	coordinator.RegisterHook("retention-scheduler", func(context.Context) error {
		order = append(order, "retention-scheduler")
		if !raftAlive {
			return fmt.Errorf("scheduler ticked after the coordinator it gates on stopped")
		}
		return nil
	}, shutdown.PriorityScheduler)

	coordinator.RegisterHook("cluster-replication-receiver", func(context.Context) error {
		order = append(order, "cluster-replication-receiver")
		receiverRunning = false
		return nil
	}, shutdown.PriorityIngest)
	registerClusterCoordinatorShutdown(coordinator, func() error {
		order = append(order, "cluster-coordinator")
		raftAlive = false
		return nil
	})
	registerFileRegistrarShutdown(coordinator, func() {
		order = append(order, "file-registrar")
		if !raftAlive {
			// What a stopped coordinator does with the drain: every apply
			// fails, nothing reaches the manifest.
			return
		}
		manifest = append(manifest, queued...)
	})

	registerTieringShutdown(coordinator, func() error {
		order = append(order, "tiering")
		if raftAlive {
			return fmt.Errorf("tiering stopped before the coordinator: the delete drain would report to a dead drainer")
		}
		return nil
	})

	if err := coordinator.Shutdown(); err != nil {
		t.Fatalf("Shutdown() error = %v (order %v)", err, order)
	}
	if !reflect.DeepEqual(manifest, []string{"shutdown.parquet"}) {
		t.Fatalf("manifest after shutdown = %v, want [shutdown.parquet] (order %v)", manifest, order)
	}
	want := []string{
		"cluster-replication-receiver", "retention-scheduler",
		"arrow-buffer", "file-registrar", "wal-purge", "cluster-coordinator", "tiering",
	}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("shutdown order = %v, want %v", order, want)
	}
}
