// Router operating mode. Cgo-free and shared by every build (stub and tagged) so
// the mode surface is identical regardless of the arcx_engine tag.

package arcxrouter

import "strings"

// Mode is the router's operating mode, from the ARC_ROUTER env (parsed once at
// handler construction and passed into Run).
type Mode int

const (
	ModeOff    Mode = iota // never touch arcx; Decide short-circuits to decline
	ModeShadow             // run arcx alongside DuckDB, compare, always serve DuckDB
	ModeServe              // serve arcx for green shapes, fall back on decline/error
)

// ParseMode maps the ARC_ROUTER env value to a Mode. Unknown/empty → shadow (the
// safe default when the engine is built in: observe, never serve, until a human
// flips to serve). "off" is the runtime kill switch.
func ParseMode(s string) Mode {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "off":
		return ModeOff
	case "serve":
		return ModeServe
	default:
		return ModeShadow
	}
}
