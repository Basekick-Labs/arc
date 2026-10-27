package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/rs/zerolog"
)

const (
	testSigMarker = "ZZSIGMARKER0123456789"
	testKeyMarker = "ZZACCOUNTKEYMARKER0123456789"
)

func testSAS() string {
	return "sv=2021-12-02&ss=b&srt=sco&sp=rwdl&se=2030-01-01T00%3A00%3A00Z&sig=" + testSigMarker
}

func noRetries(t *testing.T) {
	t.Helper()
	old := azureClientOptions
	azureClientOptions = &azblob.ClientOptions{ClientOptions: azcore.ClientOptions{
		Retry: policy.RetryOptions{MaxRetries: -1},
	}}
	t.Cleanup(func() { azureClientOptions = old })
}

func TestRedactCredentialText(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{`Get "https://acct.blob.core.windows.net/c?comp=list&sig=abc": EOF`, `Get "https://acct.blob.core.windows.net/c": EOF`},
		{`parse "http://h/c/k?sv=1&sig=abc\n": net/url: invalid control character in URL`, `parse "http://h/c/k\n": net/url: invalid control character in URL`},
		{"body: <Url>https://h/c?x=1&sig=abc</Url>", "body: <Url>https://h/c</Url>"},
		{"BlobEndpoint=https://h;SharedAccessSignature=sv=1&sig=abc;", "BlobEndpoint=https://h;SharedAccessSignature=REDACTED;"},
		{"DefaultEndpointsProtocol=https;AccountName=a;AccountKey=abc==", "DefaultEndpointsProtocol=https;AccountName=a;AccountKey=REDACTED"},
		{"status 403 for https://h/c/db/m/f.parquet", "status 403 for https://h/c/db/m/f.parquet"},
		// JSON-escaped (as stored in an audit record's detail): the escaping
		// survives, so the document stays valid.
		{`{"abort_message":"Get \"https://h/c?comp=list\u0026sig=abc\": EOF"}`, `{"abort_message":"Get \"https://h/c\u0026sig=REDACTED\": EOF"}`},
		{`{"m":"x\u0026sig=abc\u0026sp=r"}`, `{"m":"x\u0026sig=REDACTED\u0026sp=r"}`},
		{"SIG=abc at the start", "SIG=REDACTED at the start"},
	} {
		if got := RedactCredentialText(c.in); got != c.want {
			t.Errorf("RedactCredentialText(%q)\n got %q\nwant %q", c.in, got, c.want)
		}
	}
	// A JSON document stays valid after redaction.
	doc, _ := json.Marshal(map[string]string{"abort_message": `Get "https://h/c?comp=list&sig=SECRETABC": EOF`, "n": `x&sig=SECRETDEF&sp=r`})
	var back map[string]string
	if out := RedactCredentialText(string(doc)); strings.Contains(out, "SECRET") || json.Unmarshal([]byte(out), &back) != nil {
		t.Errorf("JSON document: %s", out)
	}
	if got := RedactCredentialText("token secretvalue123 here", "secretvalue123"); strings.Contains(got, "secretvalue123") {
		t.Errorf("literal secret kept: %q", got)
	}
}

// A credential that ends in a newline (read from a file) is trimmed and
// works; one with a control character inside is refused without echoing it.
func TestAzureCredentialControlCharacters(t *testing.T) {
	noRetries(t)
	b, err := NewAzureBlobBackend(&AzureBlobConfig{
		AccountName: "acct", ContainerName: "c", Endpoint: "http://127.0.0.1:1",
		SASToken: "?" + testSAS() + "\n",
	}, zerolog.Nop())
	if err != nil {
		t.Fatalf("trailing newline: %v", err)
	}
	if got := b.Credentials().SASToken; got != testSAS() {
		t.Fatalf("stored SAS %q, want it trimmed", got)
	}
	for _, cfg := range []AzureBlobConfig{
		{AccountName: "acct", ContainerName: "c", SASToken: "sv=1&sig=" + testSigMarker + "\x01x"},
		{ContainerName: "c", ConnectionString: "BlobEndpoint=http://h;SharedAccessSignature=sig=" + testSigMarker + "\x7f"},
		{AccountName: "acct", ContainerName: "c", AccountKey: testKeyMarker + "\x02"},
	} {
		_, err := NewAzureBlobBackend(&cfg, zerolog.Nop())
		if err == nil || strings.Contains(err.Error(), testSigMarker) || strings.Contains(err.Error(), testKeyMarker) {
			t.Fatalf("control character: err %v", err)
		}
	}
}

// Every exported operation of an Azure backend returns errors with no
// credential in their text: against a server that drops the connection (the
// client's own error quotes the request address) and one that echoes the
// request back in an error body. The host still appears, so the error still
// says where it failed, and not-found behaviour is unchanged.
func TestAzureErrorsCarryNoCredential(t *testing.T) {
	noRetries(t)
	drop := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && !strings.Contains(r.URL.RawQuery, "comp=") && strings.Count(r.URL.Path, "/") <= 1 {
			w.WriteHeader(http.StatusOK) // container probe
			return
		}
		if h, ok := w.(http.Hijacker); ok {
			if conn, _, err := h.Hijack(); err == nil {
				_ = conn.Close()
			}
		}
	}))
	t.Cleanup(drop.Close)
	echo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-ms-error-code", "InvalidQueryParameterValue")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("<Error><Code>InvalidQueryParameterValue</Code><Url>http://" + r.Host + r.URL.RequestURI() + "</Url></Error>"))
	}))
	t.Cleanup(echo.Close)

	for _, srv := range []*httptest.Server{drop, echo} {
		host := strings.TrimPrefix(srv.URL, "http://")
		for _, cfg := range []AzureBlobConfig{
			{AccountName: "acct", ContainerName: "c", Prefix: "p", Endpoint: srv.URL, SASToken: testSAS()},
			{ContainerName: "c", ConnectionString: "BlobEndpoint=" + srv.URL + "/acct;SharedAccessSignature=" + testSAS()},
		} {
			b, err := NewAzureBlobBackend(&cfg, zerolog.Nop())
			if err != nil {
				t.Fatalf("backend: %v", err)
			}
			ctx := context.Background()
			ops := map[string]error{}
			ops["Write"] = b.Write(ctx, "db/m/f.parquet", []byte("x"))
			ops["WriteReader"] = b.WriteReader(ctx, "db/m/f.parquet", bytes.NewReader([]byte("x")), 1)
			ops["WriteReaderWithSHA"] = b.WriteReaderWithSHA(ctx, "db/m/f.parquet", bytes.NewReader([]byte("x")), 1, "abc")
			_, ops["Read"] = b.Read(ctx, "db/m/f.parquet")
			ops["ReadTo"] = b.ReadTo(ctx, "db/m/f.parquet", &bytes.Buffer{})
			ops["ReadToAt"] = b.ReadToAt(ctx, "db/m/f.parquet", &bytes.Buffer{}, 0)
			_, ops["StatFile"] = b.StatFile(ctx, "db/m/f.parquet")
			_, ops["Exists"] = b.Exists(ctx, "db/m/f.parquet")
			_, ops["List"] = b.List(ctx, "db/")
			_, ops["ListObjects"] = b.ListObjects(ctx, "db/")
			_, ops["ListDirectories"] = b.ListDirectories(ctx, "")
			_, ops["HasObjectsUnderPrefix"] = b.HasObjectsUnderPrefix(ctx, "db/")
			_, ops["ListUnusable"] = b.ListUnusable(ctx, "")
			_, _, ops["ObjectSHA"] = b.ObjectSHA(ctx, "db/m/f.parquet")
			ops["Delete"] = b.Delete(ctx, "db/m/f.parquet")
			ops["DeleteBatch"] = b.DeleteBatch(ctx, []string{"db/m/a.parquet", "db/m/b.parquet"})
			ops["CopyObject"] = b.CopyObject(ctx, "db/m/a.parquet", "db/m/b.parquet", 1, "abc")
			for op, err := range ops {
				if err == nil {
					// Exists and Delete may legitimately absorb a not-found;
					// every other operation must fail against these servers.
					if op != "Exists" && op != "Delete" {
						t.Errorf("%s via %s: no error, the check would be vacuous", op, host)
					}
					continue
				}
				text := err.Error()
				if strings.Contains(text, testSigMarker) || strings.Contains(text, "sig=") || strings.Contains(text, "sv=2021") {
					t.Errorf("%s via %s: credential in error text: %s", op, host, text)
				}
				// A caller that reaches the transport error itself and
				// prints it gets the trimmed address too.
				var ue *url.Error
				if errors.As(err, &ue) && (strings.Contains(ue.Error(), testSigMarker) || strings.Contains(ue.URL, "?")) {
					t.Errorf("%s via %s: *url.Error keeps the credential: %s", op, host, ue.Error())
				}
			}
		}
	}
}

// Redaction keeps the error chain: not-found and store-not-found checks, and
// the underlying types, still answer through it.
func TestRedactedErrorKeepsTheChain(t *testing.T) {
	respErr := &azcore.ResponseError{StatusCode: 404, ErrorCode: "BlobNotFound"}
	err := redactError(errors.Join(respErr, ErrStoreNotFound), "secretvalue123")
	if !isAzureNotFoundError(err) || !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("chain lost: %v", err)
	}
	var re *azcore.ResponseError
	if !errors.As(err, &re) || re != respErr {
		t.Fatal("errors.As no longer finds the response error")
	}
	if redactError(nil) != nil {
		t.Fatal("nil must stay nil")
	}
}
