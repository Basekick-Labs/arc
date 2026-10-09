package compaction

import (
	"strings"
	"testing"
)

// ClassifySubprocessError must search BOTH streams for every permanent marker.
// Before this, "no files found that match" checked both while the other four
// checked stderr only — so a permanent failure arriving through the parent's
// wrapped error was classified "unknown" and spent the whole retry budget on a
// batch that could never succeed.
func TestClassifySubprocessErrorChecksBothStreamsForEveryMarker(t *testing.T) {
	for _, marker := range permanentErrorMarkers {
		t.Run(marker, func(t *testing.T) {
			// via err only
			if recoverable, reason := ClassifySubprocessError(errString(marker), ""); recoverable || reason != "permanent_error" {
				t.Errorf("marker %q in err: recoverable=%v reason=%q, want false/permanent_error", marker, recoverable, reason)
			}
			// via stderr only
			if recoverable, reason := ClassifySubprocessError(errString("subprocess failed"), strings.ToUpper(marker)); recoverable || reason != "permanent_error" {
				t.Errorf("marker %q in stderr: recoverable=%v reason=%q, want false/permanent_error", marker, recoverable, reason)
			}
		})
	}
}

// The control: an unrecognised failure must stay retryable, or one transient
// blip becomes a permanently abandoned partition.
func TestClassifySubprocessErrorKeepsUnknownRecoverable(t *testing.T) {
	recoverable, reason := ClassifySubprocessError(errString("connection reset by peer"), "")
	if !recoverable || reason != "unknown" {
		t.Errorf("recoverable=%v reason=%q, want true/unknown", recoverable, reason)
	}
}

type stringErr string

func (e stringErr) Error() string { return string(e) }

func errString(s string) error { return stringErr(s) }
