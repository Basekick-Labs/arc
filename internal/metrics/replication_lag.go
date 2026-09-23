package metrics

import "sort"

// ReplicationLagSample is one active reader's lag at scrape time.
// HasSeconds=false means the oldest outstanding entry's timestamp is
// unavailable. Absence is preferable to reporting a false zero.
type ReplicationLagSample struct {
	Peer       string
	Entries    uint64
	Seconds    float64
	HasSeconds bool
}

type replicationLagRegistration struct {
	collect func() []ReplicationLagSample
}

// RegisterReplicationLagProvider makes the current writer's active peers
// available to the metrics endpoints. A later registration replaces it.
// The returned cleanup only clears its own registration.
func (m *Metrics) RegisterReplicationLagProvider(collect func() []ReplicationLagSample) func() {
	registration := &replicationLagRegistration{collect: collect}

	m.replicationLagMu.Lock()
	m.replicationLagProvider = registration
	m.replicationLagMu.Unlock()

	return func() {
		m.replicationLagMu.Lock()
		if m.replicationLagProvider == registration {
			m.replicationLagProvider = nil
		}
		m.replicationLagMu.Unlock()
	}
}

func (m *Metrics) replicationLagSamples() []ReplicationLagSample {
	m.replicationLagMu.RLock()
	registration := m.replicationLagProvider
	m.replicationLagMu.RUnlock()

	if registration == nil {
		return nil
	}

	samples := registration.collect()
	sort.Slice(samples, func(i, j int) bool {
		return samples[i].Peer < samples[j].Peer
	})
	return samples
}

// appendReplicationLagMetric escapes the dynamic label according to
// Prometheus text exposition rules. The peer ID comes from the active
// replication connection, never from arbitrary historical metric input.
func appendReplicationLagMetric(b []byte, name, peer string, value float64) []byte {
	b = append(b, name...)
	b = append(b, `{peer="`...)

	for i := 0; i < len(peer); i++ {
		switch peer[i] {
		case '\\':
			b = append(b, '\\', '\\')
		case '"':
			b = append(b, '\\', '"')
		case '\n':
			b = append(b, '\\', 'n')
		case '\r':
			b = append(b, '\\', 'n')
		default:
			b = append(b, peer[i])
		}
	}

	b = append(b, '"', '}', ' ')
	b = appendFloat(b, value)
	return append(b, '\n')
}
