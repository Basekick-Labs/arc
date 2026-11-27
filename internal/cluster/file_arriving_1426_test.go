package cluster

import "testing"

// Without a puller or a shared store nothing delivers files here.
func TestFileArrivingWithoutPuller(t *testing.T) {
	var none *Coordinator
	if none.FileArriving("db/m/x.parquet") {
		t.Fatal("a nil coordinator reported a file arriving")
	}
	if (&Coordinator{}).FileArriving("db/m/x.parquet") {
		t.Fatal("a coordinator with no puller reported a file arriving")
	}
}
