package storage

import (
	"path/filepath"
	"strings"
	"testing"
)

// s3At is a Destination for an S3 bucket+prefix, built through
// DestinationFromSpec so the tests exercise the same normalisation the
// production caller gets.
func s3At(t *testing.T, bucket, prefix string) Destination {
	t.Helper()
	d, err := DestinationFromSpec(BackendSpec{
		Type: "s3",
		S3:   S3Config{Bucket: bucket, Prefix: prefix},
	})
	if err != nil {
		t.Fatalf("DestinationFromSpec(bucket=%q prefix=%q): %v", bucket, prefix, err)
	}
	return d
}

// TestObjectDestinationsOverlapWhenOnePrefixContainsTheOther is the refusal
// half: equal prefixes, a parent prefix, and the bucket root all overlap, in
// both argument orders.
//
// Every spelling of the same prefix is listed on purpose. "arc" and "arc/"
// differ by one byte and name one location, and a check that treated them as
// different would clear exactly the configuration an operator is most likely
// to write (#534's corollary).
func TestObjectDestinationsOverlapWhenOnePrefixContainsTheOther(t *testing.T) {
	cases := []struct{ a, b string }{
		// The same prefix, at each of its three spellings.
		{"", ""},
		{"arc", "arc"},
		{"arc/", "arc/"},
		{"arc", "arc/"},
		{"arc/", "arc"},
		// A parent, at each spelling of the parent and the child.
		{"arc", "arc/backups"},
		{"arc/", "arc/backups"},
		{"arc", "arc/backups/"},
		{"arc/", "arc/backups/"},
		{"arc/backups", "arc/backups/daily"},
		// The bucket root contains every prefix in the bucket.
		{"", "arc"},
		{"", "arc/"},
		{"", "arc/backups/daily"},
	}
	for _, c := range cases {
		a, b := s3At(t, "acme", c.a), s3At(t, "acme", c.b)
		if !a.Overlaps(b) {
			t.Errorf("s3://acme/%q .Overlaps(s3://acme/%q) = false, want true", c.a, c.b)
		}
		if !b.Overlaps(a) {
			t.Errorf("s3://acme/%q .Overlaps(s3://acme/%q) = false, want true (reverse order)", c.b, c.a)
		}
	}
}

// TestObjectPrefixContainmentMatchesAtASeparatorBoundary is the #534 shape
// whose own fix shipped the bug: a bare HasPrefix makes "arc-other" look like
// a child of "arc".
//
// Separate from the containment test above because it is a separate claim
// about the same function, and because the fix for one is not the fix for the
// other: a check that only ever compared for equality would pass this test and
// fail that one.
func TestObjectPrefixContainmentMatchesAtASeparatorBoundary(t *testing.T) {
	siblings := []struct{ a, b string }{
		{"arc", "arc-other"},
		{"arc", "arcx"},
		{"arc/", "arc-other/"},
		{"arc/backups", "arc/backups-old"},
		{"wh", "wh-other"},
	}
	for _, c := range siblings {
		a, b := s3At(t, "acme", c.a), s3At(t, "acme", c.b)
		if a.Overlaps(b) {
			t.Errorf("s3://acme/%q .Overlaps(s3://acme/%q) = true, want false (sibling prefix, not a child)", c.a, c.b)
		}
		if b.Overlaps(a) {
			t.Errorf("s3://acme/%q .Overlaps(s3://acme/%q) = true, want false (sibling prefix, reverse order)", c.b, c.a)
		}
	}
}

// TestDisjointObjectDestinationsDoNotOverlap is the false-positive half. Same
// bucket with unrelated prefixes is the legitimate shape an operator picks on
// purpose, and a check that refused it would be a check nobody believes.
func TestDisjointObjectDestinationsDoNotOverlap(t *testing.T) {
	if a, b := s3At(t, "acme", "backups"), s3At(t, "acme", "cold"); a.Overlaps(b) || b.Overlaps(a) {
		t.Error("s3://acme/backups and s3://acme/cold overlap, want disjoint (same bucket, unrelated prefixes)")
	}
	if a, b := s3At(t, "acme", "arc/backups"), s3At(t, "acme", "arc/cold"); a.Overlaps(b) || b.Overlaps(a) {
		t.Error("s3://acme/arc/backups and s3://acme/arc/cold overlap, want disjoint (shared parent, unrelated children)")
	}
}

// TestDifferentObjectStoresDoNotOverlap: a different bucket, a different
// endpoint and a different backend kind are each enough to make two locations
// distinct even when the prefixes are identical.
func TestDifferentObjectStoresDoNotOverlap(t *testing.T) {
	if a, b := s3At(t, "acme", "arc"), s3At(t, "other", "arc"); a.Overlaps(b) || b.Overlaps(a) {
		t.Error("s3://acme/arc and s3://other/arc overlap, want disjoint (different buckets)")
	}

	aws := s3At(t, "acme", "arc")
	minio, err := DestinationFromSpec(BackendSpec{
		Type: "minio",
		S3:   S3Config{Bucket: "acme", Prefix: "arc", Endpoint: "http://localhost:9000"},
	})
	if err != nil {
		t.Fatalf("DestinationFromSpec(minio): %v", err)
	}
	if aws.Overlaps(minio) || minio.Overlaps(aws) {
		t.Error("an AWS bucket and a MinIO bucket of the same name overlap, want disjoint (different endpoints)")
	}

	azure, err := DestinationFromSpec(BackendSpec{
		Type:  "azure",
		Azure: AzureBlobConfig{ContainerName: "acme", Prefix: "arc", AccountName: "acct"},
	})
	if err != nil {
		t.Fatalf("DestinationFromSpec(azure): %v", err)
	}
	if aws.Overlaps(azure) || azure.Overlaps(aws) {
		t.Error("an S3 bucket and an Azure container of the same name overlap, want disjoint (different backends)")
	}
	if aws.Overlaps(LocalDestination("/data/arc")) {
		t.Error("an S3 bucket overlaps a local directory, want disjoint (different kinds)")
	}

	// The DEGENERATE pair, which is the only one the Kind check itself
	// decides. Every other cross-kind pair above is separated by the bucket
	// or the store as well, so deleting the Kind check would not change
	// their answer and they prove nothing about it. An object destination at
	// the root of an unnamed bucket collides with a local destination on
	// every other field — empty store, empty bucket, root prefix — so Kind is
	// all that keeps them apart. config.Load requires a bucket and a
	// container, so this is not a reachable configuration today; the
	// backup-target check calls Overlaps N-squared times over inputs this
	// package does not control (#1085 stage B2b-2), and a primitive that is
	// only correct for the inputs one caller happens to pass is the shape
	// that breaks when the second caller arrives.
	rootless, err := DestinationFromSpec(BackendSpec{Type: "s3"})
	if err != nil {
		t.Fatalf("DestinationFromSpec(s3, no bucket): %v", err)
	}
	here := LocalDestination(".")
	if rootless.Overlaps(here) || here.Overlaps(rootless) {
		t.Error("an unnamed S3 bucket root overlaps a local directory, want disjoint (only Kind separates them)")
	}
	azureRootless, err := DestinationFromSpec(BackendSpec{Type: "azure"})
	if err != nil {
		t.Fatalf("DestinationFromSpec(azure, no container): %v", err)
	}
	if azureRootless.Overlaps(here) || here.Overlaps(azureRootless) {
		t.Error("an unnamed Azure container root overlaps a local directory, want disjoint (only Kind separates them)")
	}
}

// TestMinioAndS3AreTheSameStore: the two type spellings address one store, so
// a target spelled "minio" and a cold tier spelled "s3" at the same endpoint
// and bucket must still be refused.
func TestMinioAndS3AreTheSameStore(t *testing.T) {
	spec := func(kind string) BackendSpec {
		return BackendSpec{
			Type: kind,
			S3:   S3Config{Bucket: "acme", Prefix: "arc", Endpoint: "localhost:9000"},
		}
	}
	a, err := DestinationFromSpec(spec("minio"))
	if err != nil {
		t.Fatalf("DestinationFromSpec(minio): %v", err)
	}
	// Scheme-stripped on one side only: the S3 backend accepts both spellings
	// and adds the scheme itself from UseSSL, so they are one endpoint.
	b, err := DestinationFromSpec(BackendSpec{
		Type: "s3",
		S3:   S3Config{Bucket: "acme", Prefix: "arc", Endpoint: "http://LOCALHOST:9000/"},
	})
	if err != nil {
		t.Fatalf("DestinationFromSpec(s3): %v", err)
	}
	if !a.Overlaps(b) {
		t.Error("minio and s3 at one endpoint, bucket and prefix do not overlap, want true (the spellings and the scheme name one store)")
	}
}

// TestAzureDestinationsCompareByAccount: two accounts may each hold a
// container of the same name, so the container alone is not an identity — and
// a connection-string deployment leaves AccountName empty, so the account has
// to be read out of the string or every such config would compare as
// "unknown" and be refused.
func TestAzureDestinationsCompareByAccount(t *testing.T) {
	azure := func(t *testing.T, cfg AzureBlobConfig) Destination {
		t.Helper()
		cfg.ContainerName = "arcdata"
		cfg.Prefix = "arc"
		d, err := DestinationFromSpec(BackendSpec{Type: "azure", Azure: cfg})
		if err != nil {
			t.Fatalf("DestinationFromSpec(azure): %v", err)
		}
		return d
	}

	one := azure(t, AzureBlobConfig{AccountName: "acctone"})
	two := azure(t, AzureBlobConfig{AccountName: "accttwo"})
	if one.Overlaps(two) || two.Overlaps(one) {
		t.Error("containers of the same name on two different accounts overlap, want disjoint")
	}

	fromString := azure(t, AzureBlobConfig{
		ConnectionString: "DefaultEndpointsProtocol=https;AccountName=acctone;AccountKey=Zm9v;EndpointSuffix=core.windows.net",
	})
	if !fromString.Overlaps(one) {
		t.Error("a connection-string config and an account-name config for the same account do not overlap, want true (the account is in the string)")
	}
	if fromString.Overlaps(two) {
		t.Error("a connection-string config for acctone overlaps acttwo, want disjoint (the account must be read from the string, not treated as unknown)")
	}
}

// TestLocalDestinationsOverlapInBothDirections: the containment test has to
// run both ways round, because the two hazards are different and each has only
// one direction. A backup directory INSIDE the storage root is copied into the
// next backup and swept by reconciliation; a backup directory that CONTAINS
// the storage root backs up its own earlier backups.
func TestLocalDestinationsOverlapInBothDirections(t *testing.T) {
	root := t.TempDir()
	inside := filepath.Join(root, "data", "arc")
	outer := filepath.Join(root, "data")

	if !LocalDestination(inside).Overlaps(LocalDestination(outer)) {
		t.Error("a directory does not overlap its own parent, want true")
	}
	if !LocalDestination(outer).Overlaps(LocalDestination(inside)) {
		t.Error("a directory does not overlap its own child, want true (reverse direction)")
	}
	if !LocalDestination(inside).Overlaps(LocalDestination(inside + "/")) {
		t.Error("two spellings of one directory do not overlap, want true")
	}

	sibling := filepath.Join(root, "data", "arc-other")
	if LocalDestination(sibling).Overlaps(LocalDestination(inside)) {
		t.Errorf("%q overlaps %q, want disjoint (sibling directory, not a child)", sibling, inside)
	}
	if LocalDestination(inside).Overlaps(LocalDestination(sibling)) {
		t.Errorf("%q overlaps %q, want disjoint (sibling directory, reverse order)", inside, sibling)
	}
}

// TestDestinationStringNamesTheLocationWithoutCredentials: the refusal message
// is printed before the logger exists and read by an operator who has to edit
// a config file, so it has to name the place — and must never name a key.
func TestDestinationStringNamesTheLocationWithoutCredentials(t *testing.T) {
	d, err := DestinationFromSpec(BackendSpec{
		Type: "s3",
		S3: S3Config{
			Bucket:    "acme-arc-audit-backups",
			Prefix:    "arc/",
			Endpoint:  "https://minio.internal:9000",
			AccessKey: "AKIAEXAMPLEKEY",
			SecretKey: "s3cr3t-value",
		},
	})
	if err != nil {
		t.Fatalf("DestinationFromSpec: %v", err)
	}
	got := d.String()
	const want = "s3://acme-arc-audit-backups/arc at minio.internal:9000"
	if got != want {
		t.Errorf("Destination.String() = %q, want %q", got, want)
	}
	for _, secret := range []string{"AKIAEXAMPLEKEY", "s3cr3t-value"} {
		if strings.Contains(got, secret) {
			t.Errorf("Destination.String() = %q, must not contain the credential %q", got, secret)
		}
	}
}

// s3AtEndpoint is s3At with an explicit endpoint.
func s3AtEndpoint(t *testing.T, bucket, prefix, endpoint string) Destination {
	t.Helper()
	d, err := DestinationFromSpec(BackendSpec{
		Type: "s3",
		S3:   S3Config{Bucket: bucket, Prefix: prefix, Endpoint: endpoint},
	})
	if err != nil {
		t.Fatalf("DestinationFromSpec(bucket=%q endpoint=%q): %v", bucket, endpoint, err)
	}
	return d
}

// TestEverySpellingOfOneAWSEndpointIsOneStore is the missed-refusal case.
//
// An operator leaves s3_endpoint empty for AWS in one config block and writes
// it out in another — or uses virtual-hosted addressing in one of them. Those
// are the SAME bucket at the SAME prefix, and comparing the raw endpoint
// strings reported them as different stores and let the configuration boot.
//
// The consequence is not cosmetic: the backup destination then sits inside the
// storage root's listing, so each backup re-copies the previous one, and with
// reconciliation enabled the sweep deletes the backups (a backup data key has
// nine segments, over its seven-segment managed-path heuristic).
func TestEverySpellingOfOneAWSEndpointIsOneStore(t *testing.T) {
	const bucket = "acme"
	spellings := []string{
		"",
		"s3.amazonaws.com",
		"https://s3.amazonaws.com",
		"s3.amazonaws.com/",
		"s3.amazonaws.com:443",
		"S3.AMAZONAWS.COM",
		"s3.us-east-1.amazonaws.com",
		"s3-us-east-1.amazonaws.com",
		"https://s3.dualstack.us-east-1.amazonaws.com",
		"acme.s3.amazonaws.com",
		"https://acme.s3.us-east-1.amazonaws.com",
	}
	for _, a := range spellings {
		for _, b := range spellings {
			da, db := s3AtEndpoint(t, bucket, "arc", a), s3AtEndpoint(t, bucket, "arc", b)
			if !da.Overlaps(db) {
				t.Errorf("endpoint %q and %q report different stores for one bucket and prefix, want one store", a, b)
			}
			if !db.Overlaps(da) {
				t.Errorf("endpoint %q and %q report different stores (reverse order), want one store", b, a)
			}
		}
	}
}

// TestANonAWSEndpointIsNotFoldedOntoAWS is the other side: folding must not go
// so far that a MinIO bucket and an AWS bucket of the same name become one
// store. That regression is exactly what treating an empty Store as a wildcard
// would have caused, and it is why the fix is canonicalisation.
func TestANonAWSEndpointIsNotFoldedOntoAWS(t *testing.T) {
	aws := s3AtEndpoint(t, "acme", "arc", "")
	for _, other := range []string{
		"localhost:9000",
		"http://minio.internal:9000",
		"seaweedfs.svc.cluster.local:8333",
		// The China partition is a separate partition with separate buckets,
		// so a bucket of the same name there is a different bucket.
		"s3.cn-north-1.amazonaws.com.cn",
		// A host that merely CONTAINS the AWS suffix is not AWS.
		"s3.amazonaws.com.evil.example",
	} {
		d := s3AtEndpoint(t, "acme", "arc", other)
		if aws.Overlaps(d) || d.Overlaps(aws) {
			t.Errorf("endpoint %q folds onto the AWS default, want a distinct store", other)
		}
	}
	// And two different non-AWS hosts stay distinct, including two ports on
	// one machine, which is an ordinary development layout.
	a := s3AtEndpoint(t, "acme", "arc", "localhost:9000")
	b := s3AtEndpoint(t, "acme", "arc", "localhost:9001")
	if a.Overlaps(b) || b.Overlaps(a) {
		t.Error("two ports on one host report one store, want two")
	}
}

// TestEverySpellingOfOneAzureAccountIsOneStore is the Azure twin of the missed
// refusal: an account name in one block and the equivalent
// "<account>.blob.core.windows.net" endpoint in another name one account.
func TestEverySpellingOfOneAzureAccountIsOneStore(t *testing.T) {
	azure := func(t *testing.T, cfg AzureBlobConfig) Destination {
		t.Helper()
		cfg.ContainerName = "arcdata"
		cfg.Prefix = "arc"
		d, err := DestinationFromSpec(BackendSpec{Type: "azure", Azure: cfg})
		if err != nil {
			t.Fatalf("DestinationFromSpec(azure): %v", err)
		}
		return d
	}
	spellings := map[string]AzureBlobConfig{
		"account name":                    {AccountName: "acmearc"},
		"account name, mixed case":        {AccountName: "AcmeArc"},
		"endpoint":                        {Endpoint: "https://acmearc.blob.core.windows.net"},
		"endpoint, no scheme":             {Endpoint: "acmearc.blob.core.windows.net"},
		"endpoint with a trailing slash":  {Endpoint: "https://acmearc.blob.core.windows.net/"},
		"connection string account":       {ConnectionString: "DefaultEndpointsProtocol=https;AccountName=acmearc;AccountKey=Zm9v;EndpointSuffix=core.windows.net"},
		"connection string blob endpoint": {ConnectionString: "BlobEndpoint=https://acmearc.blob.core.windows.net;SharedAccessSignature=sv=2021"},
	}
	for nameA, cfgA := range spellings {
		for nameB, cfgB := range spellings {
			da, db := azure(t, cfgA), azure(t, cfgB)
			if !da.Overlaps(db) {
				t.Errorf("azure %s and %s report different stores for one container and prefix, want one store", nameA, nameB)
			}
			if !db.Overlaps(da) {
				t.Errorf("azure %s and %s report different stores (reverse order), want one store", nameB, nameA)
			}
		}
	}

	// And a DIFFERENT account still does not overlap, however it is spelled.
	mine := azure(t, AzureBlobConfig{AccountName: "acmearc"})
	theirs := azure(t, AzureBlobConfig{Endpoint: "https://otherarc.blob.core.windows.net"})
	if mine.Overlaps(theirs) || theirs.Overlaps(mine) {
		t.Error("two different Azure accounts report one store, want two")
	}
}
