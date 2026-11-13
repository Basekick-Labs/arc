package edgesync

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// HeldKnown follows the presence of the "held" key: a hub that predates
// verification answers an ordinary reconcile without it, and the spoke must
// then delete nothing. An empty array is a hub that verifies and holds none.
func TestHTTPTransportVerifyHeldKnown(t *testing.T) {
	for _, tc := range []struct {
		body      string
		heldKnown bool
		held      int
	}{
		{`{"missing":[],"present":["a"],"conflicts":[]}`, false, 0},
		{`{"missing":[],"present":[],"conflicts":[],"held":null}`, false, 0},
		{`{"missing":[],"present":[],"conflicts":[],"held":[]}`, true, 0},
		{`{"missing":[],"present":[],"conflicts":[],"held":["m/a.parquet"]}`, true, 1},
	} {
		var sawVerify bool
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			sawVerify = strings.Contains(string(b), `"verify":true`)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(tc.body))
		}))
		tr, err := NewHTTPTransport(HTTPTransportConfig{BaseURL: srv.URL, SpokeID: "rocket-01", Secret: "0123456789abcdef0123456789abcdef"})
		if err != nil {
			t.Fatal(err)
		}
		ans, err := tr.Verify(context.Background(), DefaultHubID, []*LedgerEntry{{Path: "m/a.parquet", SHA256: sha256Hex([]byte("a")), SizeBytes: 1}})
		srv.Close()
		if err != nil {
			t.Fatal(err)
		}
		if !sawVerify {
			t.Error("the request did not ask for verify")
		}
		if ans.HeldKnown != tc.heldKnown || len(ans.Held) != tc.held {
			t.Errorf("%s: HeldKnown=%v held=%v, want %v %d", tc.body, ans.HeldKnown, ans.Held, tc.heldKnown, tc.held)
		}
	}
}
