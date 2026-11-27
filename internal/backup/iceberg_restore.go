package backup

import (
	"bytes"
	"context"
	"path"
	"strings"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// IcebergExportPauser stops the Iceberg export while a restore stages a
// catalog (#1813): iceberg.Exporter.PauseExportForRestore.
type IcebergExportPauser interface {
	PauseExportForRestore(ctx context.Context, reason string) (resume func(), err error)
}

// SetIcebergExportPauser wires the export a restore that stages a catalog
// pauses until the restart (nil when Iceberg export is off).
func (m *Manager) SetIcebergExportPauser(p IcebergExportPauser) {
	m.icebergPauser = p
}

// isIcebergDiscoveryFile reports a key a directory-based Iceberg reader
// resolves a table through: version-hint.text, or a v<N>.metadata.json copy
// (N digits only), directly in a table's metadata directory.
func isIcebergDiscoveryFile(key string) bool {
	if path.Base(path.Dir(key)) != "metadata" {
		return false
	}
	base := path.Base(key)
	if base == icebergHintName {
		return true
	}
	digits, ok := strings.CutPrefix(base, "v")
	if !ok {
		return false
	}
	digits, ok = strings.CutSuffix(digits, ".metadata.json")
	if !ok || digits == "" {
		return false
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// discoveryGuard decides, per table metadata directory, whether a restore
// may write the backup's discovery files there (#1813). A directory that
// already holds a version-hint.text keeps all of its own: the live hint names
// the version this node's table is at, and the backup's would move every
// directory-based reader back to the backup's version until the next commit
// or restart; and a v<N> copy restored beside a live hint can belong to
// another lineage (the table dropped and recreated since the backup) and
// trip the export's publish fence. A restored catalog row is moved onto the
// live published version at the restart (adoption, #1613). A directory with
// no hint gets all of them, as before. Kept across legs: a directory's
// files can come from more than one.
type discoveryGuard struct {
	backend storage.Backend
	logger  zerolog.Logger
	live    map[string]bool // metadata dir → holds a live hint (or could not be checked)
}

func newDiscoveryGuard(b storage.Backend, logger zerolog.Logger) *discoveryGuard {
	return &discoveryGuard{backend: b, logger: logger, live: make(map[string]bool)}
}

// keep reports that the discovery file at key must not be restored. A
// failed check keeps it: overwriting a live hint is the harm this exists to
// prevent, and a skipped file is counted.
func (g *discoveryGuard) keep(ctx context.Context, key string) bool {
	dir := path.Dir(key)
	if live, seen := g.live[dir]; seen {
		return live
	}
	exists, err := g.backend.Exists(ctx, dir+"/"+icebergHintName)
	if err != nil {
		g.logger.Warn().Err(err).Str("dir", dir).
			Msg("Could not check for a live Iceberg version-hint.text; the backup's version-hint.text and v<N>.metadata.json for this table are not restored. If the table has no hint here, directory-based readers resolve it again at its next export")
	}
	live := err != nil || exists
	g.live[dir] = live
	return live
}

// restoreDiscoveryFile writes the backup's discovery file at destPath unless
// the guard keeps the directory's own, and then only where nothing is
// (WriteIfAbsent where the store has it): another node's export can publish
// into a shared bucket between the check and the write. written is false
// when the file was left as it was. These files are a few hundred bytes.
func (m *Manager) restoreDiscoveryFile(ctx context.Context, g *discoveryGuard, src backupTarget, srcPath, destPath string) (written bool, n int64, err error) {
	if g.keep(ctx, destPath) {
		return false, 0, nil
	}
	var buf bytes.Buffer
	if err := src.backend.ReadTo(ctx, srcPath, &buf); err != nil {
		return false, 0, classifyReadTo(srcPath, err, nil)
	}
	if cw, ok := m.dataStorage.(storage.ConditionalWriter); ok {
		created, err := cw.WriteIfAbsent(ctx, destPath, buf.Bytes())
		if err != nil {
			return false, 0, err
		}
		return created, int64(buf.Len()), nil
	}
	if exists, err := m.dataStorage.Exists(ctx, destPath); err != nil || exists {
		return false, 0, err
	}
	if err := m.dataStorage.Write(ctx, destPath, buf.Bytes()); err != nil {
		return false, 0, err
	}
	return true, int64(buf.Len()), nil
}
