//go:build race

package auth

// testTimeoutMS is the per-request budget for app.Test in this package.
//
// Under the race detector every PBKDF2 verification costs ~1.07s on its own
// (600,000 iterations, measured; ~19x the ~56ms non-race cost), which exceeds
// Fiber's 1000ms app.Test default before the handler has done anything else.
// The result was a guaranteed "test: timeout error 1000ms" for any test whose
// handler verifies a token — read as a flake, but deterministic.
//
// The budget is raised rather than disabled (Test accepts -1 for "no timeout")
// so a genuinely hung handler still fails the suite instead of blocking until
// the go test binary's own timeout.
const testTimeoutMS = 30_000
