package config

import (
	"errors"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/fips"
	"github.com/basekick-labs/arc/internal/storage"
)

// skipGCSInFIPSBuild skips a test of the native GCS backend's configuration
// in the FIPS build, which refuses that backend at load (#1587).
func skipGCSInFIPSBuild(t *testing.T) {
	t.Helper()
	if fipsBuild {
		t.Skip("the FIPS build has no native GCS backend (#1587)")
	}
}

// asFIPSBuild runs the test as the FIPS build's config validation would.
func asFIPSBuild(t *testing.T) {
	t.Helper()
	prev := fipsBuild
	fipsBuild = true
	t.Cleanup(func() { fipsBuild = prev })
}

// TestFIPSBuildFlagFollowsTheBuildTag: the refusal is on exactly in the
// build that is tagged fips.
func TestFIPSBuildFlagFollowsTheBuildTag(t *testing.T) {
	if fipsBuild != fips.BuildTagged {
		t.Fatalf("fipsBuild = %v, want fips.BuildTagged = %v", fipsBuild, fips.BuildTagged)
	}
}

// TestFIPSBuildRefusesEveryGCSBackendAtLoad is the #1587 regression: the FIPS
// build cannot link the native GCS backend, and each place one can be
// configured is refused at config load, naming the key. Left to the factory,
// a cold tier or backup target would fail to build and leave the node running
// without it.
func TestFIPSBuildRefusesEveryGCSBackendAtLoad(t *testing.T) {
	cases := []struct {
		name, toml, key string
	}{
		{"primary", "[storage]\nbackend = \"gcs\"\ngcs_bucket = \"b\"\n", "storage.backend"},
		{"cold tier", "[tiered_storage]\nenabled = true\n[tiered_storage.cold]\nenabled = true\nbackend = \"gcs\"\ngcs_bucket = \"c\"\n",
			"tiered_storage.cold.backend"},
		{"backup target", "[backup]\ndefault_target = \"audit\"\n[backup.targets.audit]\ntype = \"gcs\"\ngcs_bucket = \"bk\"\n",
			"backup.targets.audit.type"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			asFIPSBuild(t)
			writeArcToml(t, c.toml)
			_, err := Load()
			if !errors.Is(err, storage.ErrGCSNotInFIPSBuild) {
				t.Fatalf("Load() = %v, want ErrGCSNotInFIPSBuild", err)
			}
			if !strings.HasPrefix(err.Error(), c.key+` is "gcs"`) {
				t.Errorf("error %q does not name %s", err, c.key)
			}
		})
	}
}

// TestFIPSBuildKeepsGCSThroughTheS3Endpoint: the alternative the refusal
// names, GCS's S3-compatible endpoint, stays available in the FIPS build, and
// so does a cold block that names gcs but is disabled.
func TestFIPSBuildKeepsGCSThroughTheS3Endpoint(t *testing.T) {
	cases := []struct{ name, toml string }{
		{"primary over the S3 endpoint", "[storage]\nbackend = \"s3\"\ns3_bucket = \"b\"\ns3_endpoint = \"storage.googleapis.com\"\n"},
		{"disabled gcs cold tier", "[tiered_storage]\nenabled = true\n[tiered_storage.cold]\nenabled = false\nbackend = \"gcs\"\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			asFIPSBuild(t)
			writeArcToml(t, c.toml)
			if _, err := Load(); err != nil {
				t.Fatalf("Load() = %v, want success", err)
			}
		})
	}
}
