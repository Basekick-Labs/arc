package api

// testRequestTimeoutMS bounds a single app.Test request, in milliseconds.
//
// Fiber's default is 1000ms, which is too tight under `-race`: the race
// detector's instrumentation slows every handler enough that auth-heavy tests
// (bcrypt token verification, SQLite round-trips) exceed it and fail with
// "timeout error 1000ms" — a harness artifact that looks exactly like a
// deadlock in the code under test. Four tests in this package failed that way
// under -race while passing without it.
//
// Generous on purpose. This is not a latency assertion; it is the point at
// which a genuinely stuck handler is declared hung rather than left to block
// the suite. A test that cares about latency should measure it directly.
//
// Pass it to every app.Test call — the no-timeout form inherits the 1000ms
// default and will flake here sooner or later.
const testRequestTimeoutMS = 30_000
