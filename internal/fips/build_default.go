//go:build !fips

package fips

// BuildTagged reports whether this binary was compiled with the "fips" build
// tag. False in the default (non-FIPS) build variant. See build_fips.go for
// the full contract.
const BuildTagged = false
