package config

import (
	"strings"
	"testing"
)

// #1429: the import write concurrency and queue timeout keys.
func TestLoad_ImportSlotDefaults(t *testing.T) {
	t.Chdir(t.TempDir())
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Ingest.ImportWriteConcurrency != 0 || cfg.Ingest.ImportQueueTimeoutMS != 30000 {
		t.Fatalf("defaults: concurrency %d (want 0 = auto), queue timeout %d (want 30000)",
			cfg.Ingest.ImportWriteConcurrency, cfg.Ingest.ImportQueueTimeoutMS)
	}
}

func TestLoad_ImportSlotOverrides(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("ARC_INGEST_IMPORT_WRITE_CONCURRENCY", "3")
	t.Setenv("ARC_INGEST_IMPORT_QUEUE_TIMEOUT_MS", "0")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Ingest.ImportWriteConcurrency != 3 || cfg.Ingest.ImportQueueTimeoutMS != 0 {
		t.Fatalf("got concurrency %d, queue timeout %d", cfg.Ingest.ImportWriteConcurrency, cfg.Ingest.ImportQueueTimeoutMS)
	}
}

func TestLoad_ImportSlotNegativesRefused(t *testing.T) {
	for _, key := range []string{"ARC_INGEST_IMPORT_WRITE_CONCURRENCY", "ARC_INGEST_IMPORT_QUEUE_TIMEOUT_MS"} {
		t.Run(key, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv(key, "-1")
			_, err := Load()
			want := strings.ToLower(strings.TrimPrefix(key, "ARC_INGEST_"))
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("Load() with %s=-1: err = %v, want a refusal naming %s", key, err, want)
			}
		})
	}
}

func TestLoad_ImportWriteConcurrencyCapped(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("ARC_INGEST_IMPORT_WRITE_CONCURRENCY", "1025")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "import_write_concurrency") {
		t.Fatalf("Load() with 1025: err = %v, want a refusal", err)
	}
	t.Setenv("ARC_INGEST_IMPORT_WRITE_CONCURRENCY", "1024")
	if _, err := Load(); err != nil {
		t.Fatalf("Load() with 1024: %v", err)
	}
}
