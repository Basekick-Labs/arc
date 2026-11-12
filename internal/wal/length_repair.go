package wal

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
)

// A damaged length field (#1641).
//
// A frame is a 16-byte header (payload length, timestamp, CRC32 of the
// payload) and the payload. A length the reader cannot follow used to read
// as a torn tail when it pointed past the end of the file, and every later
// frame of the file was dropped without a trace: recovery replayed the frames
// before it and deleted the file.
//
// The header's checksum still identifies the payload the writer wrote. If
// only the length is damaged, the payload is intact, so the CRC32 of the
// bytes after the header equals the stored checksum exactly at the true
// length. A torn tail (a prefix of one frame) matches only by coincidence, so
// a match is not trusted on its own: what follows the candidate end must
// confirm it. It is the end of the file (the frame was the last one), the
// start of a frame that checks out itself, or a torn tail after it. Mid-file
// a wrong repair needs two coincident CRC matches (about 2^-64).
//
// Not repaired, and reported as before: a length damaged together with the
// payload or checksum of the same frame (no prefix matches), and a length
// damaged to be SHORTER than the payload (the frame reads as a checksum
// mismatch and the reads after it are misaligned).

// repairFrameLength looks for the true payload length of a frame whose
// payload starts at payloadStart, given the bytes read from there (all of
// them up to the end of the file or the claimed length, whichever is
// shorter). exclusive bounds the search: only lengths below it are tried (the
// claimed length, when it was read whole and its checksum did not match).
// It returns the length, or 0 when no candidate is confirmed.
func repairFrameLength(f *os.File, fileSize, payloadStart int64, expected uint32, have []byte, exclusive int) int {
	limit := len(have)
	if exclusive > 0 && exclusive-1 < limit {
		limit = exclusive - 1
	}
	tab := crc32.IEEETable
	c := ^uint32(0)
	for i := 0; i < limit; i++ {
		c = tab[byte(c)^have[i]] ^ (c >> 8)
		if ^c != expected {
			continue
		}
		if frameEndConfirmed(f, fileSize, payloadStart+int64(i+1)) {
			return i + 1
		}
	}
	return 0
}

// frameEndConfirmed reports whether next can be the end of a frame: the end
// of the file, a torn header, or a header with a plausible length whose
// payload either checks out or runs past the end of the file (a torn tail).
func frameEndConfirmed(f *os.File, fileSize, next int64) bool {
	if next == fileSize {
		return true
	}
	if next > fileSize {
		return false
	}
	if fileSize-next < WALEntryHeaderSize {
		return true // a torn header
	}
	var header [WALEntryHeaderSize]byte
	if _, err := f.ReadAt(header[:], next); err != nil {
		return false
	}
	n := binary.BigEndian.Uint32(header[0:4])
	if n == 0 || n > MaxWALPayloadSize {
		return false // 16 zero bytes are not a frame (CRC32 of nothing is 0)
	}
	if next+WALEntryHeaderSize+int64(n) > fileSize {
		return true // a torn tail after the repaired frame
	}
	payload := make([]byte, n)
	if _, err := f.ReadAt(payload, next+WALEntryHeaderSize); err != nil && err != io.EOF {
		return false
	}
	return crc32.ChecksumIEEE(payload) == binary.BigEndian.Uint32(header[12:16])
}
