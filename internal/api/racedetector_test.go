//go:build !race

package api

// raceDetectorEnabled reports whether this binary was built with -race.
// See the //go:build race variant for why it exists.
const raceDetectorEnabled = false
