package license

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Failure taxonomy for license-server interactions (#license-boot-resilience).
//
// The boot path routes on ONE question: did the server SPEAK THE PROTOCOL and
// say "no" (definitive — fall back to OSS, never use the cache), or could we
// simply not get a protocol answer (transient — retry, then fall back to the
// locally cached, signature-verified license)?
//
// The classification table (from the reviewed plan; server behavior enumerated
// from enterprise_activation_server handlers):
//
//	transport error (DNS/refused/timeout/TLS)        → transient
//	HTTP 5xx                                          → transient
//	HTTP 429 (server's own rate limiter)              → transient
//	HTTP 4xx WITHOUT a protocol JSON body (ingress /
//	  deploy-window 404s — the field incident)        → transient
//	HTTP 4xx WITH protocol JSON ("license not found",
//	  "license is not active", max machines, ...)     → definitive
//	200 envelope valid:false / success:false          → definitive
//	  (server spoke protocol and said no; unknown
//	   error strings default definitive on purpose)
//	200 malformed / non-protocol body (proxy default) → transient
//	signature or fingerprint-binding failure          → transient
//	  (protocol violation or MITM; the cache is
//	   independently verified, so falling back to it
//	   cannot be worse than trusting this response)
//
// "machine not activated" and "activation has been revoked" are NOT terminal:
// ActivateOrVerify attempts Activate and the classification of ITS outcome
// governs. (The revoked case matters because the server's stale-activation
// reaper revokes any activation without a heartbeat for 72h — a routine
// condition for stable machines, not an operator decision.)

// FailureClass says how a license-server failure should be treated.
type FailureClass int

const (
	// ClassTransient: no protocol answer was obtained. Retry; if retries
	// exhaust, the verified cache may serve.
	ClassTransient FailureClass = iota
	// ClassDefinitive: the server spoke the protocol and rejected. OSS
	// fallback; the cache MUST NOT override an explicit rejection.
	ClassDefinitive
)

// errActivationMissing marks the two verify rejections for which activation is
// the intended recovery ("machine not activated for this license",
// "activation has been revoked"); ActivateOrVerify routes on it with
// errors.Is. Matched exactly at the envelope, never by substring on wrapped
// messages.
var errActivationMissing = errors.New("no live activation for this machine")

// classifiedError carries the class through error wrapping.
type classifiedError struct {
	class FailureClass
	err   error
}

func (e *classifiedError) Error() string { return e.err.Error() }
func (e *classifiedError) Unwrap() error { return e.err }

func transientErr(format string, args ...any) error {
	return &classifiedError{class: ClassTransient, err: fmt.Errorf(format, args...)}
}

func definitiveErr(format string, args ...any) error {
	return &classifiedError{class: ClassDefinitive, err: fmt.Errorf(format, args...)}
}

// FailureClassOf extracts the class from an error chain. Unclassified errors
// (client-side marshal bugs etc.) default to transient: they carry no server
// verdict, and the cache path independently re-verifies everything it loads.
func FailureClassOf(err error) FailureClass {
	var ce *classifiedError
	if errors.As(err, &ce) {
		return ce.class
	}
	return ClassTransient
}

// isProtocolJSONBody reports whether an HTTP error body is the license
// protocol's own JSON error shape (`{"error": "..."}`), as opposed to an
// ingress/proxy artifact (HTML error page, empty body, plain text). Only a
// protocol-shaped body makes a 4xx definitive: the field incident was a
// deploy-window 404 served by the ingress, not by the handler.
func isProtocolJSONBody(preview string) bool {
	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(preview), &body); err != nil {
		return false
	}
	return body.Error != ""
}
