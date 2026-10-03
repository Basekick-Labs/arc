package main

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/shutdown"
	"github.com/rs/zerolog"
)

func TestFileRegistrarShutdownOrder(t *testing.T) {
	coordinator := shutdown.New(5*time.Second, zerolog.Nop())
	var order []string
	var queuedFiles []string
	var registeredFiles []string

	coordinator.Register("arrow-buffer", shutdownFunc(func() error {
		queuedFiles = append(queuedFiles, "shutdown.parquet")
		order = append(order, "arrow-buffer")
		return nil
	}), shutdown.PriorityBuffer)

	registerFileRegistrarShutdown(coordinator, func() {
		registeredFiles = append(registeredFiles, queuedFiles...)
		order = append(order, "file-registrar")
	})

	coordinator.Register("wal-purge", shutdownFunc(func() error {
		if !reflect.DeepEqual(registeredFiles, []string{"shutdown.parquet"}) {
			return fmt.Errorf("registered files before WAL cleanup = %v", registeredFiles)
		}
		order = append(order, "wal-purge")
		return nil
	}), 35)
	coordinator.Register("cluster-coordinator", shutdownFunc(func() error {
		order = append(order, "cluster-coordinator")
		return nil
	}), shutdown.PriorityCompaction)

	if err := coordinator.Shutdown(); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if !reflect.DeepEqual(registeredFiles, []string{"shutdown.parquet"}) {
		t.Fatalf("registered files = %v, want [shutdown.parquet]", registeredFiles)
	}

	want := []string{"arrow-buffer", "file-registrar", "wal-purge", "cluster-coordinator"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("shutdown order = %v, want %v", order, want)
	}
}
