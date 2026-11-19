package pruning

import "context"

// The forms below are only used by tests. They store what they list with no
// measurement generation (context.Background carries none), so an entry they
// make is never dropped when a file is published: production code must go
// through the scoped forms (publish.go).

// set stores a glob-cache entry that belongs to no measurement.
func (c *globCache) set(pattern string, matches []string) {
	c.setScoped(pattern, matches, listScope{})
}

func (p *PartitionPruner) filterExistingPaths(paths []string, basePath string) []string {
	existing, _, _ := p.filterExistingPathsOpts(context.Background(), paths, basePath, 0)
	return existing
}

func (p *PartitionPruner) filterExistingLocalPaths(paths []string) []string {
	existing, _, _ := p.filterExistingLocalPathsOpts(context.Background(), paths, 0)
	return existing
}

func (p *PartitionPruner) filterExistingRemotePaths(paths []string, basePath string) []string {
	existing, _, _ := p.filterExistingRemotePathsOpts(context.Background(), paths, basePath, 0)
	return existing
}
