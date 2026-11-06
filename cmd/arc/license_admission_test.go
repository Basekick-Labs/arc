package main

import (
	"errors"
	"testing"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/rs/zerolog"
)

func TestLicenseConfiguredOnlineInstallationCannotFallThroughToStandaloneData(t *testing.T) {
	c, err := license.NewClient(&license.ClientConfig{LicenseKey: "ARC-ENT-UNADMITTED", CacheDir: t.TempDir(), Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	for _, client := range []*license.Client{nil, c} {
		if licenseDataAllowed(client, true, false) {
			t.Fatal("configured online initialization failure/missing grant enabled data services")
		}
		if !errors.Is(licenseRuntimeIngestGate(client, true, false)(), ingest.ErrIngestFenced) {
			t.Fatal("configured online installation enabled direct ingestion")
		}
	}
	if !licenseDataAllowed(nil, false, false) || licenseRuntimeIngestGate(nil, false, false)() != nil {
		t.Fatal("unconfigured OSS was fenced")
	}
	if !licenseDataAllowed(c, true, true) || licenseRuntimeIngestGate(c, true, true)() != nil {
		t.Fatal("already authorized sealed OSS owner became dependent on new admission")
	}
}

func TestLicenseStandaloneMaintenanceRemainsArmedButRespectsAdmission(t *testing.T) {
	allowed := false
	g := &standaloneLicenseCompactionGate{allowed: func() bool { return allowed }}
	if !g.CanScheduleCompaction() || g.CanCompact() || (icebergWriterGate{inner: g}).CanRun() {
		t.Fatal("unadmitted standalone executed maintenance or could not later resume")
	}
	allowed = true
	if !g.CanCompact() || !(icebergWriterGate{inner: g}).CanRun() {
		t.Fatal("fresh admission did not resume maintenance")
	}
}
