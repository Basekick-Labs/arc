//go:build !unix

package cluster

import "os"

func fileInode(os.FileInfo) uint64 { return 0 }
