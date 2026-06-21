// Package fips centralizes Arc's FIPS 140-3 posture: detecting whether the
// process is running against the Go Cryptographic Module in FIPS mode, and
// producing TLS configurations restricted to FIPS-approved primitives.
//
// Arc ships two build variants. The default build is unchanged. The "fips"
// build (built with -tags=fips and GOFIPS140=v1.0.0) bakes in
// GODEBUG=fips140=only via a //go:debug directive (see fips_enabled.go), runs
// against the CMVP-certified Go Cryptographic Module, and fails closed on
// non-approved code paths (legacy token hashes, InsecureSkipVerify, etc.).
//
// IMPORTANT: the Go FIPS module only covers the standard library crypto/*
// tree. golang.org/x/crypto packages (bcrypt, hkdf, …) are OUTSIDE the
// boundary and are NOT rejected by fips140=only — they must be removed by
// code change. This package exists so the policy lives in one place rather
// than being re-derived at each call site.
package fips

import (
	"crypto/fips140"
	"crypto/tls"
)

// Enabled reports whether the process is running with the Go Cryptographic
// Module in FIPS mode (GODEBUG=fips140=on or =only). It returns true only in
// the fips build variant run with the proper GODEBUG; it is a runtime check,
// not a build-tag check. Use BuildTagged for the compile-time variant.
func Enabled() bool {
	return fips140.Enabled()
}

// approvedCipherSuites is the set of TLS 1.2 cipher suites permitted under
// FIPS 140-3 (SP 800-52r2: AES-GCM with ECDHE key exchange and ECDSA/RSA
// auth). TLS 1.3 cipher suites are not configurable in Go (the stack selects
// among AES-GCM / ChaCha20 itself, and under fips140 mode ChaCha20 is
// disabled automatically), so this list governs only the TLS 1.2 floor.
var approvedCipherSuites = []uint16{
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
}

// approvedCurves are the NIST-approved elliptic curves for FIPS mode.
// X25519 is excluded (not a FIPS-approved curve for key establishment).
var approvedCurves = []tls.CurveID{
	tls.CurveP256,
	tls.CurveP384,
	tls.CurveP521,
}

// HardenTLSConfig applies Arc's TLS hardening to a config. It mutates and
// returns the passed config (or allocates one if nil) so it composes with
// builders that have already populated Certificates / RootCAs / ClientAuth
// (e.g. ClusterTLSConfig).
//
// The TLS 1.2 minimum-version floor is applied in BOTH build variants — that
// is the pre-existing behavior (every call site previously set
// MinVersion: tls.VersionTLS12 explicitly) and is plain hardening, not a
// FIPS-specific restriction. Setting it explicitly (rather than relying on
// Go's MinVersion==0 default) keeps the floor pinned across Go-version default
// changes and ignores the GODEBUG=tls10server escape hatch.
//
// The FIPS-approved cipher-suite and curve restrictions are applied ONLY in
// the fips build (BuildTagged). The default build keeps Go's default cipher
// suites and curves — notably X25519, which the FIPS-approved set excludes —
// so standard-build TLS behavior (beyond the long-standing 1.2 floor) is
// unchanged. The two build variants do not mix TLS posture.
func HardenTLSConfig(cfg *tls.Config) *tls.Config {
	if cfg == nil {
		cfg = &tls.Config{}
	}
	// TLS 1.2 floor — both build variants (pre-existing behavior).
	if cfg.MinVersion < tls.VersionTLS12 {
		cfg.MinVersion = tls.VersionTLS12
	}
	if !BuildTagged {
		// Default build: keep Go's default cipher suites / curves.
		return cfg
	}
	// FIPS build: restrict to the FIPS-approved cipher suites and curves.
	cfg.CipherSuites = approvedCipherSuites
	cfg.CurvePreferences = approvedCurves
	return cfg
}
