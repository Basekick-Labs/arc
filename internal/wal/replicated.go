package wal

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

// TrackedPayload returns the originating identity and logical payload. It
// accepts both originating and received on-disk entries; callers decide which
// provenance is permitted at their boundary. It never copies the payload.
func TrackedPayload(payload []byte) (identity string, logical []byte, err error) {
	if len(payload) <= walTrackedHeaderSize || (payload[0] != WALTrackedMarker && payload[0] != WALReplicatedMarker) {
		return "", nil, fmt.Errorf("replicated WAL entry has no originating identity")
	}
	return hex.EncodeToString(payload[1:walTrackedHeaderSize]), payload[walTrackedHeaderSize:], nil
}

// AppendReplicated persists an originating tracked entry without assigning a
// new identity or invoking this writer's replication hook. The provenance bit
// occupies the existing marker byte, so neither the entry size nor the number
// of WAL writes increases. The caller's authenticated payload is immutable.
func (w *Writer) AppendReplicated(payload []byte) error {
	if len(payload) <= walTrackedHeaderSize || payload[0] != WALTrackedMarker {
		return fmt.Errorf("replicated WAL entry must carry an originating tracked identity")
	}
	if len(payload) > MaxWALPayloadSize {
		return oversizedPayloadError(fmt.Errorf("replicated size %d exceeds limit %d", len(payload), MaxWALPayloadSize))
	}
	data := make([]byte, WALEntryHeaderSize+len(payload))
	binary.BigEndian.PutUint32(data[:4], uint32(len(payload)))
	binary.BigEndian.PutUint64(data[4:12], uint64(time.Now().UnixMicro()))
	copy(data[WALEntryHeaderSize:], payload)
	data[WALEntryHeaderSize] = WALReplicatedMarker
	binary.BigEndian.PutUint32(data[12:16], crc32.ChecksumIEEE(data[WALEntryHeaderSize:]))
	identity, _, err := TrackedPayload(payload)
	if err != nil {
		return err
	}
	w.pendingMu.Lock()
	seq := atomic.AddUint64(&w.trackedSequence, 1)
	w.pendingSeqs[seq] = struct{}{}
	if w.receivedSeqs == nil {
		w.receivedSeqs = make(map[string][]uint64)
	}
	w.receivedSeqs[identity] = append(w.receivedSeqs[identity], seq)
	w.pendingMu.Unlock()
	if err := w.tryEnqueueEntry(walEntry{data: data, seq: seq}); err != nil {
		w.pendingMu.Lock()
		delete(w.pendingSeqs, seq)
		seqs := w.receivedSeqs[identity]
		for i, candidate := range seqs {
			if candidate == seq {
				seqs = append(seqs[:i], seqs[i+1:]...)
				break
			}
		}
		if len(seqs) == 0 {
			delete(w.receivedSeqs, identity)
		} else {
			w.receivedSeqs[identity] = seqs
		}
		w.pendingMu.Unlock()
		return err
	}
	return nil
}

// Only a durable checkpoint can release received data. ForgetTracked must not
// release it: abandoning local ingestion does not make an origin write invalid.
func (w *Writer) releaseReceived(identities []string) {
	w.pendingMu.Lock()
	defer w.pendingMu.Unlock()
	for _, identity := range identities {
		for _, seq := range w.receivedSeqs[identity] {
			delete(w.pendingSeqs, seq)
		}
		delete(w.receivedSeqs, identity)
	}
}

// containsReceivedEntries scans framing and checksums without decoding MessagePack. Unknown or
// truncated framing fails closed. It is used only by legacy/foreign purges;
// current-process reclamation uses the normal flush sequence floor.
func containsReceivedEntries(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return false, err
	}
	var header [WALFileHeaderSize]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		return false, err
	}
	if string(header[:4]) != string(WALMagic) || binary.BigEndian.Uint16(header[4:6]) != WALVersion || header[6] != WALChecksumCRC32 {
		return false, fmt.Errorf("invalid WAL header")
	}
	offset := int64(WALFileHeaderSize)
	for offset < info.Size() {
		var entry [WALEntryHeaderSize]byte
		if _, err := io.ReadFull(f, entry[:]); err != nil {
			return false, err
		}
		size := int64(binary.BigEndian.Uint32(entry[:4]))
		offset += WALEntryHeaderSize + size
		if size == 0 || size > MaxWALPayloadSize || offset > info.Size() {
			return false, fmt.Errorf("invalid WAL entry framing")
		}
		var marker [1]byte
		if _, err := io.ReadFull(f, marker[:]); err != nil {
			return false, err
		}
		if marker[0] == WALReplicatedMarker {
			return true, nil
		}
		checksum := crc32.NewIEEE()
		_, _ = checksum.Write(marker[:])
		if _, err := io.CopyN(checksum, f, size-1); err != nil {
			return false, err
		}
		if checksum.Sum32() != binary.BigEndian.Uint32(entry[12:16]) {
			return false, fmt.Errorf("invalid WAL entry checksum")
		}

	}
	return false, nil
}

// A previous process's received data can be checkpointed into a file created
// by this process during recovery. Until recovery removes that data file, keep
// later checkpoints too; a local sequence floor cannot describe foreign data.
func (w *Writer) hasForeignReceivedWAL() bool {
	paths, err := filepath.Glob(filepath.Join(w.config.WALDir, "*.wal"))
	if err != nil {
		return true
	}
	w.mu.Lock()
	known := make(map[string]bool, len(w.fileSeqs)+1)
	for path := range w.fileSeqs {
		known[path] = true
	}
	known[w.currentPath] = true
	w.mu.Unlock()
	for _, path := range paths {
		if known[path] {
			continue
		}
		protected, err := containsReceivedEntries(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil || protected {
			return true
		}
	}
	return false
}
