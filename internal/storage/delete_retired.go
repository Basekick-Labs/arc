package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
)

// DeleteRetiredRoot holds the standalone DELETE's retired journal: one JSON
// object per file a row DELETE replaced with its rewrite and has not removed
// yet (internal/api delete_leftovers.go). A reserved root
// (IsReservedRootDir), so no database walker reads it as data.
const DeleteRetiredRoot = "_delete_retired"

// RetiredKey is the journal key of the record naming path.
func RetiredKey(path string) string {
	sum := sha256.Sum256([]byte(path))
	return DeleteRetiredRoot + "/" + hex.EncodeToString(sum[:16]) + ".json"
}

// ParseRetired returns the file a journal record at key names. Only a record
// stored under its own path's key, naming a parquet file under a database,
// counts: anything else is not trusted to hide a file.
func ParseRetired(key string, data []byte) (string, bool) {
	var rec struct {
		Path string `json:"path"`
	}
	if json.Unmarshal(data, &rec) != nil || !retiredPathValid(rec.Path) || RetiredKey(rec.Path) != key {
		return "", false
	}
	return rec.Path, true
}

func retiredPathValid(p string) bool {
	if !strings.HasSuffix(p, ".parquet") || strings.HasPrefix(p, "/") || strings.Contains(p, "\\") {
		return false
	}
	parts := strings.Split(p, "/")
	if len(parts) < 3 || IsReservedRootDir(parts[0]) {
		return false
	}
	for _, s := range parts {
		if s == "" || s == "." || s == ".." {
			return false
		}
	}
	return true
}

// ReadRetired returns the files the journal in b names.
func ReadRetired(ctx context.Context, b Backend) (map[string]bool, error) {
	keys, err := b.List(ctx, DeleteRetiredRoot+"/")
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, k := range keys {
		if !strings.HasSuffix(k, ".json") {
			continue
		}
		data, err := b.Read(ctx, k)
		if err != nil {
			if IsNotFound(err) {
				continue
			}
			return nil, err
		}
		if p, ok := ParseRetired(k, data); ok {
			out[p] = true
		}
	}
	return out, nil
}
