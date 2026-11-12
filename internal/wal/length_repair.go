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
// confirm it. Either the candidate ends exactly at the end of the file (the
// frame was the last one: one position, about 2^-32 for a torn tail), or the
// next 16 bytes are the header of a frame that fits in the file and checks
// out itself (two coincident CRC matches: about L x 2^-64 over L candidates).
// A torn tail or torn header after the candidate does NOT confirm it: random
// bytes read as a plausible torn header about 2^-5 of the time.
//
// Only a length that points past the end of the file or exceeds
// MaxWALPayloadSize is repaired, and once per file. Not repaired, and
// reported as before:
//   - a length damaged to be longer but still inside the file: the payload
//     read fails its checksum (a damaged entry). Repairing it in ReadAll would
//     disagree with the checkpoint scan, which skips data frames by their
//     stated length without reading them, and an entry the scan missed the
//     checkpoint of would be replayed twice;
//   - a length damaged together with the payload or checksum of the frame
//     (no prefix matches), or followed directly by a torn tail;
//   - a length damaged to be SHORTER than the payload (checksum mismatch,
//     misaligned reads after it).

// repairFrameLength looks for the true payload length of a frame whose
// payload starts at payloadStart, given the bytes read from there (all of
// them up to the end of the file or the claimed length, whichever is
// shorter). It returns the length, or 0 when no candidate is confirmed.
func repairFrameLength(f *os.File, fileSize, payloadStart int64, expected uint32, have []byte) int {
	limit := len(have)
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

// frameEndConfirmed reports whether next is the end of a frame: the end of
// the file, or the start of a frame that fits in the file and checks out.
func frameEndConfirmed(f *os.File, fileSize, next int64) bool {
	if next == fileSize {
		return true
	}
	if fileSize-next < WALEntryHeaderSize {
		return false
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
		return false
	}
	payload := make([]byte, n)
	if _, err := f.ReadAt(payload, next+WALEntryHeaderSize); err != nil && err != io.EOF {
		return false
	}
	return crc32.ChecksumIEEE(payload) == binary.BigEndian.Uint32(header[12:16])
}
