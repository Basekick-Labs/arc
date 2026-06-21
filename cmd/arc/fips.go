//go:build fips

// This file is compiled only into the arc-fips build variant (-tags=fips).
//
// The //go:debug directive below bakes GODEBUG=fips140=only into the binary's
// default settings, so the arc-fips binary always runs the Go Cryptographic
// Module in FIPS-only mode — non-approved stdlib crypto calls fail closed, and
// an operator cannot silently downgrade the posture by omitting a GODEBUG env
// var. Pair this with building under GOFIPS140=v1.0.0 (the CMVP-certified
// module snapshot) so the compiled crypto IS the validated module source.
//
//go:debug fips140=only
package main
