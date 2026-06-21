//go:build fips

package fips

// BuildTagged reports whether this binary was compiled with the "fips" build
// tag (the arc-fips variant). This is a compile-time constant, distinct from
// Enabled() which is the runtime GODEBUG check. Code that must fail closed on
// non-approved paths (legacy token-hash verification, InsecureSkipVerify)
// keys off BuildTagged so the policy is decided at build time and cannot be
// toggled at runtime.
const BuildTagged = true
