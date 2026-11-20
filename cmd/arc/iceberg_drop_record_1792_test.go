package main

import "testing"

// The database drop records of a shared bucket (internal/iceberg
// shared_drop.go, #1792) are read by reconcile passes only. That is enough
// because wherever they are kept (the barrier) there is a coordinator, and
// with one the ingest-triggered export, which appends without a pass, is
// off. If either rule changes, the append path needs the record too.
func TestIcebergDropRecordsAreOnlyKeptWhereIngestAppendsAreOff(t *testing.T) {
	bools := []bool{false, true}
	for _, sharedMode := range bools {
		for _, sharedBucket := range bools {
			for _, hasCoordinator := range bools {
				for _, compactorFailover := range bools {
					for _, compactionGate := range bools {
						topo := icebergTopologyFor(sharedMode, sharedBucket, hasCoordinator, compactorFailover, compactionGate)
						if !topo.barrier {
							continue
						}
						if got := icebergIngestInterval(5, hasCoordinator); got != 0 {
							t.Errorf("sharedMode=%v sharedBucket=%v coordinator=%v: drop records are kept and ingest appends run every %v",
								sharedMode, sharedBucket, hasCoordinator, got)
						}
					}
				}
			}
		}
	}
}
