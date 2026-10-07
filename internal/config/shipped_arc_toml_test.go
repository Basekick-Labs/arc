package config

import (
	"os"
	"path/filepath"
	"testing"
)

// The arc.toml at the repo root ships inside the container image, so any
// connection value set there becomes the effective configuration for every
// deployment that does not override it — and an empty environment variable
// does NOT override it. Viper's AutomaticEnv treats an unset-or-empty env var
// as absent (AllowEmptyEnv is off, and turning it on globally would make every
// ARC_X="" in a values.yaml or compose file start overriding this file), so
// ARC_TIERED_STORAGE_COLD_S3_ENDPOINT="" leaves a shipped endpoint in place.
//
// A cold tier meant for AWS would then keep talking to whatever was shipped.
// That happened: the file carried s3_endpoint = "localhost:9000" plus static
// MinIO credentials, so an operator following its own "AWS S3: leave empty"
// advice from the environment got localhost, every cold listing failed, and
// every migration cycle skipped.
//
// Arc's built-in defaults are already the AWS-correct ones, so the fix is for
// the file to set nothing here. This test is the guard that keeps it that way.
func TestShippedArcTomlHasNoLiveColdTierConnectionValues(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	tomlPath := filepath.Join(root, "arc.toml")
	if _, err := os.Stat(tomlPath); err != nil {
		t.Skipf("arc.toml not present at %s: %v", tomlPath, err)
	}

	t.Chdir(root)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() the shipped arc.toml: %v", err)
	}

	cold := cfg.TieredStorage.Cold

	if cold.S3Endpoint != "" {
		t.Errorf("arc.toml sets tiered_storage.cold.s3_endpoint = %q; it must be unset so the "+
			"AWS default applies, because an empty env var cannot clear it", cold.S3Endpoint)
	}
	if cold.S3AccessKey != "" || cold.S3SecretKey != "" {
		t.Error("arc.toml sets static cold-tier S3 credentials; configured keys take precedence " +
			"over the AWS credential chain, so they silently disable IRSA and instance-role detection")
	}
	if !cold.S3UseSSL {
		t.Error("arc.toml sets tiered_storage.cold.s3_use_ssl = false; against real S3 that is " +
			"plaintext HTTP and every read fails with AccessDenied")
	}
	if cold.S3PathStyle {
		t.Error("arc.toml sets tiered_storage.cold.s3_path_style = true; against real S3 that is " +
			"path-style addressing and every read fails with AccessDenied")
	}
	if cold.S3Bucket != "" {
		t.Errorf("arc.toml sets tiered_storage.cold.s3_bucket = %q; the built-in default is empty "+
			"so enabling the cold tier without a bucket is refused at startup rather than writing "+
			"into whatever that name means in the operator's account", cold.S3Bucket)
	}
	if cold.AzureConnectionString != "" || cold.AzureAccountKey != "" || cold.AzureSASToken != "" {
		t.Error("arc.toml sets Azure cold-tier credentials; same hazard as the S3 keys above")
	}
}
