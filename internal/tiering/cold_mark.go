package tiering

// The cluster's cold mark (#1552), tiering side. See
// internal/cluster/raft/cold_mark.go for the invariant: per
// "database/measurement" scope, the latest UTC day that may hold a cold
// object. Tiering raises it before every cold write (GuardColdWrite, used by
// the migrator and by a restore) and seeds it from a complete cold listing.

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/config"
)

// coldMarkUnknownDay mirrors raft.MarkUnknownDay (neither package imports the
// other): the mark of a scope holding an object whose day cannot be parsed.
const coldMarkUnknownDay = int32(math.MaxInt32)

// ColdMarker is the cluster's cold mark, implemented by the manifest adapter
// in cmd/arc. Absent on a standalone node.
type ColdMarker interface {
	// ColdWrite raises each scope's mark to its day and then runs write. When
	// the cluster requires the mark and it cannot be raised, write does not
	// run and the error is returned.
	ColdWrite(ctx context.Context, marks map[string]int32, write func() error) error
	// ColdMarkSeedState: fenced means a listing started now may seed the
	// mark; seeded that one already did.
	ColdMarkSeedState() (fenced, seeded bool)
	// SeedColdMarks folds a complete cold listing into the marks.
	SeedColdMarks(ctx context.Context, marks map[string]int32, backend string) error
}

func (m *Manager) coldMarker() ColdMarker {
	if m == nil || m.manifest == nil {
		return nil
	}
	cm, _ := m.manifest.(ColdMarker)
	return cm
}

// coldMarkDay returns the scope a query reads key under (its first two
// segments) and the UTC day, as days since the epoch, of its partition. A key
// under a scope whose partition cannot be parsed gets coldMarkUnknownDay; one
// under no scope is read by no query and gets ok=false.
func (m *Manager) coldMarkDay(key string) (scope string, day int32, ok bool) {
	scope, ok = measurementScope(key)
	if !ok {
		return "", 0, false
	}
	info, err := m.parseFilePath(key)
	if err != nil {
		return scope, coldMarkUnknownDay, true
	}
	return scope, int32(info.PartitionTime.UTC().Unix() / int64(24*time.Hour/time.Second)), true
}

// addColdMark raises marks[scope] to day.
func addColdMark(marks map[string]int32, scope string, day int32) {
	if cur, ok := marks[scope]; !ok || day > cur {
		marks[scope] = day
	}
}

// GuardColdWrite runs write, which writes the cold objects at keys, after
// raising the cluster's cold mark for them. On a standalone node it just runs
// write. backup.Manager calls it for a restore's cold writes.
func (m *Manager) GuardColdWrite(ctx context.Context, keys []string, write func() error) error {
	cm := m.coldMarker()
	if cm == nil {
		return write()
	}
	marks := make(map[string]int32, 1)
	for _, k := range keys {
		if scope, day, ok := m.coldMarkDay(k); ok {
			addColdMark(marks, scope, day)
		}
	}
	return cm.ColdWrite(ctx, marks, write)
}

// coldBackendIdentity names the cold store a listing read, so marks seeded
// from two different stores are never trusted.
func coldBackendIdentity(c *config.ColdTierConfig) string {
	if c == nil {
		return ""
	}
	var bucket, prefix string
	switch c.Backend {
	case "s3":
		bucket, prefix = c.S3Bucket, c.S3Prefix
	case "azure":
		bucket, prefix = c.AzureContainer, c.AzurePrefix
	case "gcs":
		bucket, prefix = c.GCSBucket, c.GCSPrefix
	default:
		return ""
	}
	if bucket == "" {
		return ""
	}
	return c.Backend + "://" + bucket + "/" + strings.Trim(prefix, "/")
}

// coldMarkSeed is the seed a cold sync collects: started only when the mark
// was fenced and not seeded before the listing began.
type coldMarkSeed struct {
	marker ColdMarker
	marks  map[string]int32
}

// beginColdMarkSeed is called before the cold listing. It returns nil unless
// this listing may seed the mark: a cluster node whose tiering is not
// role-gated, with the mark fenced and not yet seeded.
func (m *Manager) beginColdMarkSeed() *coldMarkSeed {
	cm := m.coldMarker()
	if cm == nil || m.roleGated() {
		return nil
	}
	if fenced, seeded := cm.ColdMarkSeedState(); !fenced || seeded {
		return nil
	}
	return &coldMarkSeed{marker: cm, marks: map[string]int32{}}
}

// note folds one listed key into the seed.
func (s *coldMarkSeed) note(m *Manager, key string) {
	if s == nil {
		return
	}
	if scope, day, ok := m.coldMarkDay(key); ok {
		addColdMark(s.marks, scope, day)
	}
}

// finish proposes the seed after a complete sync.
func (s *coldMarkSeed) finish(ctx context.Context, m *Manager) {
	if s == nil {
		return
	}
	backend := coldBackendIdentity(&m.config.Cold)
	if backend == "" {
		m.logger.Warn().Msg("Cold mark not seeded: the cold tier has no bucket identity")
		return
	}
	if err := s.marker.SeedColdMarks(ctx, s.marks, backend); err != nil {
		m.logger.Warn().Err(err).Msg("Seeding the cold mark failed; the next cold sync retries")
		return
	}
	m.logger.Info().Int("scopes", len(s.marks)).Msg("Cold mark seeded from the cold listing")
}
