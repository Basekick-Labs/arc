package metrics

import "testing"

// #1268: an unknown refusal reason is counted under "other", not dropped.
func TestIngestFencedWritesUnknownReasonCountsAsOther(t *testing.T) {
	m := Get()
	before := m.IngestFencedWritesFor("other")
	m.IncIngestFencedWrites("no-such-reason")
	if d := m.IngestFencedWritesFor("other") - before; d != 1 {
		t.Fatalf("unknown reason added %d to other", d)
	}
	beforeTotal := m.ingestFencedWritesTotal()
	m.IncIngestFencedWrites("standby")
	if d := m.ingestFencedWritesTotal() - beforeTotal; d != 1 {
		t.Fatalf("one refusal added %d to the total", d)
	}
}
