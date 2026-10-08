package storage

import (
	"context"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestReplicaPrivateFilesExcludedFromOrdinaryEnumeration(t *testing.T) {
	root := t.TempDir()
	backend, err := NewLocalBackend(root, zerolog.Nop())
	require.NoError(t, err)
	defer backend.Close()
	ctx := context.Background()
	ordinary := "db/cpu/data.parquet"
	require.NoError(t, backend.Write(ctx, ordinary, []byte("data")))
	for _, prefix := range []string{".replica", ".replica-pins"} {
		key := prefix + "/db/cpu/shadow.parquet"
		require.NoError(t, backend.Write(ctx, key, []byte("private")))
		// Even malformed private filenames belong to handoff recovery, never to
		// the orphan/unusable-object maintenance job.
		require.NoError(t, os.WriteFile(filepath.Join(root, prefix, "db/cpu/bad\\name.parquet"), []byte("private"), 0600))
		body, err := backend.Read(ctx, key)
		require.NoError(t, err)
		require.Equal(t, "private", string(body))
		for _, queryPrefix := range []string{prefix, prefix + "/db"} {
			keys, err := backend.List(ctx, queryPrefix)
			require.NoError(t, err)
			require.Empty(t, keys)
			objects, err := backend.ListObjects(ctx, queryPrefix)
			require.NoError(t, err)
			require.Empty(t, objects)
			dirs, err := backend.ListDirectories(ctx, queryPrefix)
			require.NoError(t, err)
			require.Empty(t, dirs)
			unusable, err := backend.ListUnusable(ctx, queryPrefix)
			require.NoError(t, err)
			require.Empty(t, unusable)
		}
	}
	keys, err := backend.List(ctx, "")
	require.NoError(t, err)
	require.Equal(t, []string{ordinary}, keys)
	objects, err := backend.ListObjects(ctx, "")
	require.NoError(t, err)
	require.Len(t, objects, 1)
	require.Equal(t, ordinary, objects[0].Path)
	dirs, err := backend.ListDirectories(ctx, "")
	require.NoError(t, err)
	require.Equal(t, []string{"db"}, dirs)
	unusable, err := backend.ListUnusable(ctx, "")
	require.NoError(t, err)
	require.Empty(t, unusable)
}
