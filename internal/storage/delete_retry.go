package storage

import (
	"context"
	"time"
)

// DeleteRetryBackoff spaces DeleteWithRetry's attempts (a variable so tests
// can shrink it).
var DeleteRetryBackoff = []time.Duration{100 * time.Millisecond, 400 * time.Millisecond}

// DeleteWithRetry deletes path, trying again on an error after each
// DeleteRetryBackoff step. It is for removing a file something else already
// replaced (a DELETE rewrite's source), whose survival means reading its rows
// twice: a transient storage error should not leave it behind. A path already
// gone is success.
func DeleteWithRetry(ctx context.Context, b Backend, path string) error {
	var err error
	for attempt := 0; ; attempt++ {
		if err = b.Delete(ctx, path); err == nil || IsNotFound(err) {
			return nil
		}
		if attempt >= len(DeleteRetryBackoff) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(DeleteRetryBackoff[attempt]):
		}
	}
}
