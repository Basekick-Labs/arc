package api

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
)

// receiptRecorder records which marker a DELETE called, and what storage
// held at that moment.
type receiptRecorder struct {
	r          *leftoverRig
	removed    []string
	rewritten  []string
	filesAtRun [][]string
	err        error
}

func (rr *receiptRecorder) wire() {
	rr.r.h.SetReceiptMarkers(
		func(_ context.Context, paths []string) error {
			rr.filesAtRun = append(rr.filesAtRun, rr.r.files())
			rr.removed = append(rr.removed, paths...)
			return rr.err
		},
		func(paths []string) error {
			rr.filesAtRun = append(rr.filesAtRun, rr.r.files())
			rr.rewritten = append(rr.rewritten, paths...)
			return rr.err
		},
	)
}

// A partial rewrite marks the original's receipt compacted (its surviving
// rows live in the new file), before anything changes (#1666). Marking after
// the rename left a window in which the leftover sweep removed the original
// unmarked, and the spoke re-sent it.
func TestDeleteRewriteMarksReceiptCompactedFirst(t *testing.T) {
	r := newLeftoverRig(t)
	rr := &receiptRecorder{r: r}
	rr.wire()
	old := leftoverHour + "cpu_20261007_204851_558252000.parquet"
	r.parquet(old, "h1", "h2")

	n, err := r.h.rewriteFileWithoutDeletedRows(context.Background(), filepath.Join(r.root, old), old, "host = 'h1'", "")
	if err != nil || n != 1 {
		t.Fatalf("rewrite: %d %v", n, err)
	}
	if !slices.Equal(rr.rewritten, []string{old}) || len(rr.removed) != 0 {
		t.Fatalf("rewritten %v removed %v, want the original marked compacted only", rr.rewritten, rr.removed)
	}
	if !slices.Equal(rr.filesAtRun[0], []string{old}) {
		t.Fatalf("storage at the mark = %v, want only the untouched original", rr.filesAtRun[0])
	}
	if files := r.files(); len(files) != 1 || files[0] == old {
		t.Fatalf("files after = %v, want the rewritten file only", files)
	}
}

// A file whose rows all match is marked removed while it still exists, then
// removed.
func TestDeleteWholeFileMarksReceiptRemovedFirst(t *testing.T) {
	r := newLeftoverRig(t)
	rr := &receiptRecorder{r: r}
	rr.wire()
	old := leftoverHour + "cpu_20261007_204851_558252000.parquet"
	r.parquet(old, "h1", "h1")

	n, err := r.h.rewriteFileWithoutDeletedRows(context.Background(), filepath.Join(r.root, old), old, "host = 'h1'", "")
	if err != nil || n != 2 {
		t.Fatalf("delete: %d %v", n, err)
	}
	if !slices.Equal(rr.removed, []string{old}) || len(rr.rewritten) != 0 {
		t.Fatalf("removed %v rewritten %v, want the original marked removed only", rr.removed, rr.rewritten)
	}
	if !slices.Equal(rr.filesAtRun[0], []string{old}) {
		t.Fatalf("storage at the mark = %v, want the original still there", rr.filesAtRun[0])
	}
	if files := r.files(); len(files) != 0 {
		t.Fatalf("files after = %v, want none", files)
	}
}

// A marking failure leaves the file exactly as it was, partial or whole.
func TestDeleteLeavesFileWhenReceiptMarkFails(t *testing.T) {
	for name, hosts := range map[string][]string{"partial": {"h1", "h2"}, "whole": {"h1", "h1"}} {
		t.Run(name, func(t *testing.T) {
			r := newLeftoverRig(t)
			rr := &receiptRecorder{r: r, err: errors.New("database is locked")}
			rr.wire()
			old := leftoverHour + "cpu_20261007_204851_558252000.parquet"
			r.parquet(old, hosts...)
			if _, err := r.h.rewriteFileWithoutDeletedRows(context.Background(), filepath.Join(r.root, old), old, "host = 'h1'", ""); err == nil {
				t.Fatal("rewrite succeeded although the receipt could not be marked")
			}
			if files := r.files(); !slices.Equal(files, []string{old}) {
				t.Fatalf("files = %v, want the original untouched", files)
			}
		})
	}
}

// Nothing matching: no mark at all.
func TestDeleteMatchingNothingMarksNoReceipt(t *testing.T) {
	r := newLeftoverRig(t)
	rr := &receiptRecorder{r: r}
	rr.wire()
	old := leftoverHour + "cpu_20261007_204851_558252000.parquet"
	r.parquet(old, "h1", "h2")
	if _, err := r.h.rewriteFileWithoutDeletedRows(context.Background(), filepath.Join(r.root, old), old, "host = 'h9'", ""); err != nil {
		t.Fatal(err)
	}
	if len(rr.removed)+len(rr.rewritten) != 0 {
		t.Fatalf("marked %v / %v for a DELETE that changed nothing", rr.removed, rr.rewritten)
	}
}

// A cold file whose rows all match is marked removed before it is deleted;
// a marking failure keeps it.
func TestDeleteColdWholeFileMarksReceiptRemoved(t *testing.T) {
	for _, fail := range []bool{false, true} {
		r := newColdDeleteRig(t)
		r.coldFile(coldWhole, "h0", "h0")
		var marked []string
		existedAtMark := false
		r.h.SetReceiptMarkers(func(_ context.Context, paths []string) error {
			existedAtMark = r.coldExists(coldWhole)
			marked = append(marked, paths...)
			if fail {
				return errors.New("database is locked")
			}
			return nil
		}, func([]string) error { return nil })
		r.delete(`{"database":"db","measurement":"cpu","where":"host = 'h0'","confirm":true}`)
		if !slices.Equal(marked, []string{coldWhole}) || !existedAtMark {
			t.Fatalf("fail=%v: marked %v (existed at mark %v), want the cold file marked while present", fail, marked, existedAtMark)
		}
		if r.coldExists(coldWhole) != fail {
			t.Fatalf("fail=%v: cold file present = %v", fail, r.coldExists(coldWhole))
		}
	}
}
