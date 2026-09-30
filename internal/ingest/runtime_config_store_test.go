package ingest

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestRuntimeIngestConfigStorePersistsAndDeletesOverride(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store, err := NewRuntimeIngestConfigStore(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Load(); err != nil || found {
		t.Fatalf("initial Load() = found %v, err %v; want no override", found, err)
	}

	want := RuntimeIngestConfig{MaxBufferSize: 1234, MaxBufferAgeMS: 5678}
	if err := store.Save(want); err != nil {
		t.Fatal(err)
	}
	got, found, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !found || got != want {
		t.Fatalf("Load() = (%+v, %v), want (%+v, true)", got, found, want)
	}

	if err := store.Delete(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Load(); err != nil || found {
		t.Fatalf("Load() after Delete = found %v, err %v; want no override", found, err)
	}
}

func TestRuntimeIngestConfigStoreRejectsNonPositiveValues(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store, err := NewRuntimeIngestConfigStore(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, cfg := range []RuntimeIngestConfig{
		{MaxBufferSize: 0, MaxBufferAgeMS: 1},
		{MaxBufferSize: 1, MaxBufferAgeMS: 0},
	} {
		if err := store.Save(cfg); err == nil {
			t.Errorf("Save(%+v) succeeded; want validation error", cfg)
		}
	}

	maxInt := int(^uint(0) >> 1)
	maxDurationMS := int64(^uint64(0)>>1) / int64(time.Millisecond)
	if int64(maxInt) > maxDurationMS {
		if err := store.Save(RuntimeIngestConfig{MaxBufferSize: 1, MaxBufferAgeMS: maxInt}); err == nil {
			t.Fatal("Save() with overflowing max_buffer_age_ms succeeded; want validation error")
		}
	}
}

func TestRuntimeElasticReserveStorePersistsIndependently(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	store, err := NewRuntimeIngestConfigStore(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.LoadElasticReserve(); err != nil || found {
		t.Fatalf("initial LoadElasticReserve = found %v, err %v; want no override", found, err)
	}
	want := RuntimeElasticReserveConfig{Enabled: true, CapacityRecords: 12345}
	if err := store.SaveElasticReserve(want); err != nil {
		t.Fatal(err)
	}
	got, found, err := store.LoadElasticReserve()
	if err != nil {
		t.Fatal(err)
	}
	if !found || got != want {
		t.Fatalf("LoadElasticReserve = (%+v, %v), want (%+v, true)", got, found, want)
	}
	if err := store.Save(RuntimeIngestConfig{MaxBufferSize: 11, MaxBufferAgeMS: 22}); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(); err != nil {
		t.Fatal(err)
	}
	if got, found, err := store.LoadElasticReserve(); err != nil || !found || got != want {
		t.Fatalf("buffer Delete changed reserve = (%+v, %v, %v)", got, found, err)
	}
	if err := store.DeleteElasticReserve(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.LoadElasticReserve(); err != nil || found {
		t.Fatalf("LoadElasticReserve after reserve delete = found %v, err %v; want no override", found, err)
	}
}

func TestRuntimeElasticReserveStoreRejectsInvalidCapacity(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	store, err := NewRuntimeIngestConfigStore(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, cfg := range []RuntimeElasticReserveConfig{
		{Enabled: true, CapacityRecords: 0},
		{Enabled: true, CapacityRecords: -1},
		{Enabled: false, CapacityRecords: -1},
	} {
		if err := store.SaveElasticReserve(cfg); err == nil {
			t.Errorf("SaveElasticReserve(%+v) succeeded; want validation error", cfg)
		}
	}
}
