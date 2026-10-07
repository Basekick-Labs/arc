//go:build !linux && !darwin

package sysmem

// limit reports nothing on platforms Arc does not build for. Callers must keep
// their existing behaviour when ok is false rather than substituting a guess.
func limit() (uint64, Source, bool) {
	return 0, SourceUnknown, false
}
