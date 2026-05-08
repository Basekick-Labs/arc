//go:build !(linux && cgo)

package memtrim

// ReleaseToOS is a no-op outside Linux+cgo. The bug it addresses (glibc malloc
// arenas holding pages after CGo frees) does not exist on darwin's allocator.
func ReleaseToOS() bool { return false }
