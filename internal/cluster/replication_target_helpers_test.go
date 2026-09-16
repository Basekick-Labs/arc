package cluster

import "time"

// timeoutAfterSeconds is a small readability wrapper so the blocking tests read
// as "did this finish" rather than as channel plumbing.
func timeoutAfterSeconds(n int) <-chan time.Time {
	return time.After(time.Duration(n) * time.Second)
}
