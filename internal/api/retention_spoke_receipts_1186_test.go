package api

import (
	"context"
	"errors"
	"slices"
	"testing"
)

// Retention marks the edge-sync receipts of the files it deletes before it
// deletes them, hot and cold (#1186): without the mark the spoke re-sends the
// files and the next pass deletes them again.
func TestRetentionMarksSpokeReceiptsBeforeDeleting(t *testing.T) {
	r := newColdRetentionRig(t)
	ctx := context.Background()
	hotKey := "testdb/logs/2020/01/01/00/hot.parquet"
	coldKey := "testdb/cpu/2020/01/01/00/cold.parquet"
	hot := r.parquet(t, r.hotDir, hotKey, expiredTS)
	cold := r.cold(t, "cpu", coldKey, expiredTS)

	var marked []string
	r.h.SetOnSpokeFilesRemoved(func(_ context.Context, paths []string) error {
		// Each path must still exist when it is marked.
		for _, p := range paths {
			if p == hotKey && !exists(hot) || p == coldKey && !exists(cold) {
				t.Errorf("%s was deleted before its receipt was marked", p)
			}
		}
		marked = append(marked, paths...)
		return nil
	})
	if _, err := r.h.ExecutePolicy(ctx, r.policy(t, "")); err != nil {
		t.Fatalf("ExecutePolicy: %v", err)
	}
	if exists(hot) || exists(cold) {
		t.Fatalf("hot=%v cold=%v after retention, want both deleted", exists(hot), exists(cold))
	}
	slices.Sort(marked)
	if !slices.Equal(marked, []string{coldKey, hotKey}) {
		t.Fatalf("marked %v, want the cold and the hot file", marked)
	}
}

// A marking failure deletes nothing: hot retention aborts the cycle like a
// manifest failure, cold retention keeps the file for the next run.
func TestRetentionKeepsFilesWhenReceiptMarkFails(t *testing.T) {
	r := newColdRetentionRig(t)
	ctx := context.Background()
	hot := r.parquet(t, r.hotDir, "testdb/logs/2020/01/01/00/hot.parquet", expiredTS)
	cold := r.cold(t, "cpu", "testdb/cpu/2020/01/01/00/cold.parquet", expiredTS)
	r.h.SetOnSpokeFilesRemoved(func(context.Context, []string) error { return errors.New("database is locked") })

	_, _ = r.h.ExecutePolicy(ctx, r.policy(t, ""))
	if !exists(hot) || !exists(cold) {
		t.Fatalf("hot=%v cold=%v, want both kept when receipts could not be marked", exists(hot), exists(cold))
	}
}
