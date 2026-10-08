package wal

// DeferTrackedReplication is configured before ingestion starts. ArrowBuffer
// then publishes only entries whose rows it has actually admitted. A rejected
// conversion, full WAL queue, or failed multi-chunk append cannot create a
// replica-only write that no primary file will ever cover.
func (w *Writer) DeferTrackedReplication() { w.deferTrackedReplication.Store(true) }

func (w *Writer) stageOrPublishTracked(identity string, entry *ReplicationEntry) {
	w.mu.Lock()
	hook := w.replicationHook
	w.mu.Unlock()
	if hook == nil {
		return
	}
	if w.deferTrackedReplication.Load() {
		w.pendingMu.Lock()
		if w.stagedReplication == nil {
			w.stagedReplication = make(map[string]*ReplicationEntry)
		}
		w.stagedReplication[identity] = entry
		w.pendingMu.Unlock()
		return
	}
	w.publishReplication(entry)
}

func (w *Writer) publishReplication(entry *ReplicationEntry) {
	w.mu.Lock()
	hook := w.replicationHook
	if hook != nil {
		w.sequence++
		entry.Sequence = w.sequence
	}
	w.mu.Unlock()
	if hook != nil {
		hook(entry)
	}
}

// PublishTracked is called after buffer admission, and outside the shard lock.
// It releases the existing serialized payload after handing ownership to the
// sender; there is no extra WAL append, serialization, or file sync here.
func (w *Writer) PublishTracked(identities []string) {
	for _, identity := range identities {
		w.pendingMu.Lock()
		entry := w.stagedReplication[identity]
		delete(w.stagedReplication, identity)
		w.pendingMu.Unlock()
		if entry != nil {
			w.publishReplication(entry)
		}
	}
}
