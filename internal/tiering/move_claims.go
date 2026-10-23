package tiering

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// moveClaims keeps a migration and a DELETE off the same hot file at once.
// A DELETE that rewrites a file the migrator is copying leaves the copy
// holding the rows it deleted, and the cold sync records any cold object
// without a row, so those rows would be read again for good. One mutex,
// two sets: a path is either moving or being deleted, never both.
type moveClaims struct {
	mu       sync.Mutex
	moving   map[string]struct{}
	deleting map[string]int
}

func (c *moveClaims) claimMove(path string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, busy := c.deleting[path]; busy {
		return false
	}
	if _, busy := c.moving[path]; busy {
		return false
	}
	if c.moving == nil {
		c.moving = map[string]struct{}{}
	}
	c.moving[path] = struct{}{}
	return true
}

func (c *moveClaims) releaseMove(path string) {
	c.mu.Lock()
	delete(c.moving, path)
	c.mu.Unlock()
}

func (c *moveClaims) claimDelete(path string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, busy := c.moving[path]; busy {
		return false
	}
	if c.deleting == nil {
		c.deleting = map[string]int{}
	}
	c.deleting[path]++
	return true
}

func (c *moveClaims) releaseDelete(path string) {
	c.mu.Lock()
	if c.deleting[path] <= 1 {
		delete(c.deleting, path)
	} else {
		c.deleting[path]--
	}
	c.mu.Unlock()
}

// ErrFileMoving reports a hot file a DELETE must not touch: it is being
// copied to cold storage, or a cold copy of it already exists (a move that
// flipped and has not released the hot copy, one whose flip failed, or a copy
// left over in a database that has since stopped tiering). A rewrite of the
// hot copy would leave the cold one holding the deleted rows. Once the move
// finishes, the file is in cold storage only: a DELETE then removes it if all
// of its rows match and reports it otherwise (#1350); until then a retry meets
// the same answer.
var ErrFileMoving = errors.New("file is being moved to cold storage")

// ErrColdCheckFailed reports a hot file a DELETE left alone because whether
// cold storage holds a copy of it could not be checked.
var ErrColdCheckFailed = errors.New("could not check cold storage for a copy of the file")

// ClaimForDelete keeps the migrator off a hot file while a DELETE rewrites or
// removes it, and refuses one the migrator has already started on, or one
// with a copy in cold storage. The returned release must be called once the
// DELETE has finished with the file, its tier metadata included. A nil
// manager, or one with no usable cold tier, claims nothing.
func (m *Manager) ClaimForDelete(ctx context.Context, path string) (release func(), err error) {
	if m == nil || !m.coldTierUsable() {
		return func() {}, nil
	}
	if !m.claims.claimDelete(path) {
		return nil, fmt.Errorf("%w: %s", ErrFileMoving, path)
	}
	release = func() { m.claims.releaseDelete(path) }
	// Taken after the claim: a migration cannot start a copy between this
	// check and the DELETE's own work.
	if has, cerr := m.hasColdCopy(ctx, path); cerr != nil || has {
		release()
		if cerr != nil {
			return nil, fmt.Errorf("%w: %s: %v", ErrColdCheckFailed, path, cerr)
		}
		return nil, fmt.Errorf("%w: %s", ErrFileMoving, path)
	}
	return release, nil
}

// hasColdCopy reports an object in cold storage at a hot file's path. Only a
// file the migrator could have copied is asked about: a daily compaction
// output (or a rewrite of one). A DELETE of recent hourly data asks cold
// storage nothing.
//
// Not by the database's CURRENT policy (#1350 review): a database made
// hot_only, or given a longer hot age, after a copy landed in cold still has
// that copy, and cold sync records it as soon as the hot file is rewritten
// away. The rewrite would then bring the deleted rows back and count the kept
// ones twice. One HEAD per rewritten daily file is the price.
func (m *Manager) hasColdCopy(ctx context.Context, path string) (bool, error) {
	if !isDailyCompactedPath(path) {
		return false, nil
	}
	cold := m.GetBackendForTier(TierCold)
	if cold == nil {
		return false, nil
	}
	size, err := cold.StatFile(ctx, path)
	if err != nil {
		return false, err
	}
	return size >= 0, nil
}
