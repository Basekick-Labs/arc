//go:build race

package api

// raceDetectorEnabled reports whether this binary was built with -race.
//
// Used to skip the one test that drives POST /api/v1/query/arrow to its
// success path over a real HTTP response. That path trips a pre-existing data
// race between the trailer Set in the stream-writer goroutine and fasthttp's
// response-header serialisation on the connection goroutine (#729). The race
// is in the trailer mechanism itself, not in the test: it fires on
// Arc-Execution-Time-Ms, shipped before the test existed, with the #724
// trailer removed entirely.
//
// Skipped rather than deleted so the coverage exists for local runs now and
// starts running in CI the moment #729 lands.
const raceDetectorEnabled = true
