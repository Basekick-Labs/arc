// Package diskspace reads the size and free space of the filesystem holding a
// path. Shared by the WAL disk pressure gate (#676) and the standby store's
// free-space reserve (#1266), so both read the volume the same way.
package diskspace
