//go:build !race

package auth

// testTimeoutMS is the per-request budget for app.Test in this package.
//
// Without the race detector a PBKDF2 verification costs ~56ms, so Fiber's
// 1000ms default is already comfortable. Kept slightly above it to absorb a
// loaded CI machine without masking a hung handler. See the //go:build race
// variant for why this is a constant rather than the default.
const testTimeoutMS = 5_000
