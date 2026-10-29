package ingest

import "encoding/binary"

// EXPERIMENTAL: a msgpack framing walker. It reads type headers only and never
// allocates: msgpackValueEnd finds where a value ends, refusing any length or
// element count larger than the bytes left. Two uses:
//
//   - the generic decode checks a payload with it first: the msgpack library
//     allocates a bin/str value's declared length before reading it, so five
//     bytes (c6 7f ff ff ff, "2 GB of bin follow") made it allocate 2 GB;
//   - the typed path finds the items of a top-level array or of a "batch"
//     array with it, so each item can be decoded from (and keep, for the WAL)
//     its own bytes.
//
// It also caps nesting depth (msgpackMaxDepth).

// msgpackMaxDepth is the deepest nesting the walker accepts. The library's
// generic decode recurses once per level, so millions of nested arrays (a few
// MB of 0x91 bytes) overflowed the goroutine stack, which no recover catches.
// Real payloads nest four or five levels.
const msgpackMaxDepth = 64

// msgpackValueEnd is the offset just past the value at data[pos:], or false
// if the value is truncated, uses the reserved code 0xc1, declares more bytes
// or elements than are left, or nests deeper than msgpackMaxDepth.
func msgpackValueEnd(data []byte, pos int) (int, bool) {
	// open[i] is how many values are still to walk at nesting level i.
	var openBuf [msgpackMaxDepth + 1]int
	open := openBuf[:1]
	open[0] = 1
	for len(open) > 0 {
		if open[len(open)-1] == 0 {
			open = open[:len(open)-1]
			continue
		}
		open[len(open)-1]--
		if pos >= len(data) {
			return 0, false
		}
		c := data[pos]
		pos++
		var n uint64     // payload bytes after the header
		var elems uint64 // child values (arrays: n, maps: 2n)
		switch {
		case c <= 0x7f, c >= 0xe0, c == 0xc0, c == 0xc2, c == 0xc3:
		case c >= 0x80 && c <= 0x8f:
			elems = 2 * uint64(c&0x0f)
		case c >= 0x90 && c <= 0x9f:
			elems = uint64(c & 0x0f)
		case c >= 0xa0 && c <= 0xbf:
			n = uint64(c & 0x1f)
		default:
			var size int // length field bytes
			switch c {
			case 0xc4, 0xd9: // bin8, str8
				size = 1
			case 0xc5, 0xda: // bin16, str16
				size = 2
			case 0xc6, 0xdb: // bin32, str32
				size = 4
			case 0xc7, 0xc8, 0xc9: // ext8/16/32: length, then a type byte
				size = 1 << (c - 0xc7) // 1, 2, 4
			case 0xca:
				n = 4
			case 0xcb:
				n = 8
			case 0xcc, 0xd0:
				n = 1
			case 0xcd, 0xd1:
				n = 2
			case 0xce, 0xd2:
				n = 4
			case 0xcf, 0xd3:
				n = 8
			case 0xd4, 0xd5, 0xd6, 0xd7, 0xd8: // fixext 1/2/4/8/16, plus a type byte
				n = 1 + 1<<(c-0xd4)
			case 0xdc, 0xde: // array16, map16
				size = 2
			case 0xdd, 0xdf: // array32, map32
				size = 4
			default: // 0xc1: never used
				return 0, false
			}
			if size > 0 {
				if len(data)-pos < size {
					return 0, false
				}
				var l uint64
				switch size {
				case 1:
					l = uint64(data[pos])
				case 2:
					l = uint64(binary.BigEndian.Uint16(data[pos:]))
				case 4:
					l = uint64(binary.BigEndian.Uint32(data[pos:]))
				}
				pos += size
				switch c {
				case 0xdc, 0xdd:
					elems = l
				case 0xde, 0xdf:
					elems = 2 * l
				case 0xc7, 0xc8, 0xc9:
					n = l + 1
				default:
					n = l
				}
			}
		}
		left := uint64(len(data) - pos)
		if n > left || elems > left {
			return 0, false
		}
		pos += int(n)
		if elems > 0 {
			if len(open) > msgpackMaxDepth {
				return 0, false
			}
			open = append(open, int(elems))
		}
	}
	return pos, true
}

// msgpackFramingOK reports whether data starts with one complete value whose
// declared lengths all fit in data.
func msgpackFramingOK(data []byte) bool {
	_, ok := msgpackValueEnd(data, 0)
	return ok
}

// msgpackHeader reads an array or map header at data[pos]: its element count
// (map: entries) and the offset after it.
func msgpackHeader(data []byte, pos int, array bool) (n, next int, ok bool) {
	if pos >= len(data) {
		return 0, 0, false
	}
	c := data[pos]
	fix, b16, b32 := byte(0x80), byte(0xde), byte(0xdf)
	if array {
		fix, b16, b32 = 0x90, 0xdc, 0xdd
	}
	switch {
	case c >= fix && c <= fix+0x0f:
		return int(c & 0x0f), pos + 1, true
	case c == b16 && len(data)-pos >= 3:
		return int(binary.BigEndian.Uint16(data[pos+1:])), pos + 3, true
	case c == b32 && len(data)-pos >= 5:
		l := binary.BigEndian.Uint32(data[pos+1:])
		if uint64(l) > uint64(len(data)-pos-5) {
			return 0, 0, false
		}
		return int(l), pos + 5, true
	}
	return 0, 0, false
}
