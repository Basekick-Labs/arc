//go:build arc_seam_1938

// Test-only build (never shipped; no Makefile, Dockerfile or workflow sets
// this tag): pauses the periodic WAL recovery pass right after its
// pre-barrier for ARC_TEST_PASS_PAUSE_MS, so a binary run can make a flush
// fail inside #1938's W3 window. Each pause is announced on stderr before and
// after.
package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

func init() {
	ms, err := strconv.Atoi(os.Getenv("ARC_TEST_PASS_PAUSE_MS"))
	if err != nil || ms <= 0 {
		return
	}
	walRecoveryAfterPreBarrier = func() {
		fmt.Fprintf(os.Stderr, "{\"message\":\"seam1938 pass paused\",\"pause_ms\":%d}\n", ms)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		fmt.Fprintln(os.Stderr, "{\"message\":\"seam1938 pass resumed\"}")
	}
}
