package auth

import "testing"

// #1630 review: the matcher the auth middleware and the audit coverage check
// share. An empty prefix, or "/" (emptied by the trailing-slash trim), is
// ignored rather than matching every path; prefixes match at a segment
// boundary.
func TestIsPublicPath(t *testing.T) {
	routes := []string{"/health", "/api/v1/auth/verify"}
	for _, tc := range []struct {
		path     string
		prefixes []string
		want     bool
	}{
		{"/health", nil, true},
		{"/healthz", nil, false},
		{"/api/v1/auth/tokens", []string{""}, false},
		{"/api/v1/auth/tokens", []string{"/"}, false},
		{"/metrics", []string{"/metrics/"}, true},
		{"/metrics/x", []string{"/metrics"}, true},
		{"/metricsfoo", []string{"/metrics"}, false},
	} {
		if got := IsPublicPath(tc.path, routes, tc.prefixes); got != tc.want {
			t.Errorf("IsPublicPath(%q, %q) = %v, want %v", tc.path, tc.prefixes, got, tc.want)
		}
	}
}
