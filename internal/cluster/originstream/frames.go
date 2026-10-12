// Package originstream is the #1207 origin stream: a Pattern 1 primary
// writer sends every ingest batch it commits to its standby writers, which
// keep a copy on disk until the batch's files are registered and present on
// the standby. The copy is never applied to a buffer; a later stage replays
// it when a standby takes over from a primary that died.
//
// After the hello exchange (protocol.MsgOriginStreamHello / Ack) the primary
// sends frames on the same connection:
//
//	[len u32][type u8][body]
//
// Every body starts with a sequence number that increases by exactly one per
// frame of the session, and ends with a MAC over the type and everything
// before it, keyed by the session key. A replayed, reordered, dropped or
// altered frame fails the check and ends the session.
package originstream

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// ProtocolVersion is the origin stream protocol this build speaks.
const ProtocolVersion = 1

const (
	frameEntry  byte = 0x01
	frameFloors byte = 0x02

	macLen = 16

	// maxFrame bounds a frame: an ingest batch's WAL payload is at most
	// 100 MiB, plus coverage and headers.
	maxFrame = 100<<20 + 64<<10

	entryFixed = 8 + 8 + 8 + 8 + 2 + 2 + 2 // seq ts instance cseq chunk chunks keyLen
)

var errMAC = errors.New("origin stream frame failed authentication")

// Entry is one committed ingest batch, or one chunk of one.
type Entry struct {
	TimestampUS uint64
	Instance    uint64
	Key         string
	CSeq        uint64
	Chunk       uint16
	Chunks      uint16
	Payload     []byte
}

// KeyFloor is what the primary knows about one buffer key's batches: every
// cseq at or below Floor, and every cseq in Settled, is in a registered
// file; the cseqs in Lost settled without one (the standby may hold the
// only copy).
type KeyFloor struct {
	Floor   uint64      `json:"f"`
	Settled [][2]uint64 `json:"s,omitempty"`
	Lost    []uint64    `json:"l,omitempty"`
}

// Floors is the primary's registration progress for its current instance.
// AppliedIndex is the primary's applied Raft index read after the floors:
// every registration they describe is at or below it.
type Floors struct {
	Instance     uint64              `json:"i"`
	AppliedIndex uint64              `json:"a"`
	Keys         map[string]KeyFloor `json:"k"`
}

func frameMAC(key []byte, typ byte, body []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte{typ})
	m.Write(body)
	return m.Sum(nil)[:macLen]
}

// writeFrame writes [len][type][body-without-mac][mac]. head and payload are
// written separately so a large payload is not copied.
func writeFrame(w io.Writer, key []byte, typ byte, head, payload []byte) error {
	m := hmac.New(sha256.New, key)
	m.Write([]byte{typ})
	m.Write(head)
	m.Write(payload)
	mac := m.Sum(nil)[:macLen]
	total := 1 + len(head) + len(payload) + macLen
	if total > maxFrame {
		return fmt.Errorf("origin stream frame too large: %d", total)
	}
	var hdr [5]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(total))
	hdr[4] = typ
	for _, b := range [][]byte{hdr[:], head, payload, mac} {
		if len(b) == 0 {
			continue
		}
		if _, err := w.Write(b); err != nil {
			return err
		}
	}
	return nil
}

// readFrame reads one frame, checks its MAC and returns its type and body
// without the MAC.
func readFrame(r io.Reader, key []byte) (byte, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:4])
	if n < 1+8+macLen || n > maxFrame {
		return 0, nil, fmt.Errorf("origin stream frame length %d out of range", n)
	}
	buf := make([]byte, n-1)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, nil, err
	}
	body, mac := buf[:len(buf)-macLen], buf[len(buf)-macLen:]
	if !hmac.Equal(mac, frameMAC(key, hdr[4], body)) {
		return 0, nil, errMAC
	}
	return hdr[4], body, nil
}

// maxKeyLen bounds a coverage key on the wire: its length travels as a u16.
const maxKeyLen = 1<<16 - 1

func entryHead(seq uint64, e *Entry) []byte {
	h := make([]byte, entryFixed+len(e.Key)) // len(e.Key) <= maxKeyLen: checked by validEntry
	binary.BigEndian.PutUint64(h[0:], seq)
	binary.BigEndian.PutUint64(h[8:], e.TimestampUS)
	binary.BigEndian.PutUint64(h[16:], e.Instance)
	binary.BigEndian.PutUint64(h[24:], e.CSeq)
	binary.BigEndian.PutUint16(h[32:], e.Chunk)
	binary.BigEndian.PutUint16(h[34:], e.Chunks)
	binary.BigEndian.PutUint16(h[36:], uint16(len(e.Key)))
	copy(h[entryFixed:], e.Key)
	return h
}

// validEntry reports whether e can be framed: its key fits the u16 length
// field and the whole frame fits maxFrame. Checked before any allocation
// sized from e.
func validEntry(e *Entry) bool {
	return len(e.Key) <= maxKeyLen && len(e.Payload) <= maxFrame-(1+entryFixed+maxKeyLen+macLen)
}

func parseEntry(body []byte) (uint64, *Entry, error) {
	if len(body) < entryFixed {
		return 0, nil, errors.New("origin stream entry frame too short")
	}
	keyLen := int(binary.BigEndian.Uint16(body[36:]))
	if len(body) < entryFixed+keyLen {
		return 0, nil, errors.New("origin stream entry key overruns the frame")
	}
	e := &Entry{
		TimestampUS: binary.BigEndian.Uint64(body[8:]),
		Instance:    binary.BigEndian.Uint64(body[16:]),
		CSeq:        binary.BigEndian.Uint64(body[24:]),
		Chunk:       binary.BigEndian.Uint16(body[32:]),
		Chunks:      binary.BigEndian.Uint16(body[34:]),
		Key:         string(body[entryFixed : entryFixed+keyLen]),
		Payload:     body[entryFixed+keyLen:],
	}
	if e.Chunks == 0 || e.Chunk >= e.Chunks || e.CSeq == 0 || e.Key == "" {
		return 0, nil, errors.New("origin stream entry has invalid coverage")
	}
	return binary.BigEndian.Uint64(body[0:]), e, nil
}

func floorsHead(seq uint64, f *Floors) ([]byte, error) {
	js, err := json.Marshal(f)
	if err != nil {
		return nil, err
	}
	// Bounded before the allocation: a floors report this large cannot be
	// framed, and the size arithmetic must not overflow.
	if len(js) > maxFrame-(1+8+macLen) {
		return nil, fmt.Errorf("origin stream floors report too large: %d bytes for %d keys", len(js), len(f.Keys))
	}
	h := make([]byte, 8+len(js))
	binary.BigEndian.PutUint64(h, seq)
	copy(h[8:], js)
	return h, nil
}

func parseFloors(body []byte) (uint64, *Floors, error) {
	if len(body) < 8 {
		return 0, nil, errors.New("origin stream floors frame too short")
	}
	var f Floors
	if err := json.Unmarshal(body[8:], &f); err != nil {
		return 0, nil, fmt.Errorf("origin stream floors frame: %w", err)
	}
	return binary.BigEndian.Uint64(body), &f, nil
}
