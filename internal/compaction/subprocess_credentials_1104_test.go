package compaction

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// #1104: the compaction subprocess rebuilds the parent's backend with the
// parent's authentication. ConfigJSON carries no secrets and says nothing about
// which authentication was used, and construction succeeds for any mode
// (managed identity is lazy, the container probe only warns), so these
// assertions are on the rebuilt spec's credential fields, one by one.

const fakeAzureKey = "dGhpcy1pcy1ub3QtYS1yZWFsLWtleQ==" // base64("this-is-not-a-real-key")

func azureStub(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "AuthenticationFailed", http.StatusForbidden)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// roundTrip sends a job built for parent through JSON, as RunJobInSubprocess
// does over stdin, and returns the spec the subprocess would build.
func roundTrip(t *testing.T, parent storage.Backend, getenv func(string) string) storage.BackendSpec {
	t.Helper()
	job := &SubprocessJobConfig{
		StorageType:   parent.Type(),
		StorageConfig: parent.ConfigJSON(),
		Credentials:   subprocessCredentials(parent),
	}
	raw, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	var received SubprocessJobConfig
	if err := json.Unmarshal(raw, &received); err != nil {
		t.Fatal(err)
	}
	spec, err := backendSpecFromJob(&received, getenv)
	if err != nil {
		t.Fatalf("backendSpecFromJob: %v", err)
	}
	return spec
}

func noEnv(string) string { return "" }

func authOf(c storage.AzureBlobConfig) storage.AzureBlobConfig {
	return storage.AzureBlobConfig{
		ConnectionString:   c.ConnectionString,
		AccountKey:         c.AccountKey,
		SASToken:           c.SASToken,
		UseManagedIdentity: c.UseManagedIdentity,
	}
}

// Every Azure authentication a real parent backend can be built with reaches
// the subprocess intact. Before #1104 only the account key did: a connection
// string or a SAS token turned into managed identity in the subprocess.
func TestSubprocessGetsTheParentsAzureAuthentication(t *testing.T) {
	endpoint := azureStub(t)
	connStr := "DefaultEndpointsProtocol=http;AccountName=arctestaccount;AccountKey=" + fakeAzureKey + ";BlobEndpoint=" + endpoint + "/arctestaccount;"
	for _, tc := range []struct {
		name string
		cfg  storage.AzureBlobConfig
	}{
		{"account key", storage.AzureBlobConfig{AccountName: "arctestaccount", AccountKey: fakeAzureKey, Endpoint: endpoint}},
		{"connection string", storage.AzureBlobConfig{ConnectionString: connStr}},
		{"connection string with account name and key", storage.AzureBlobConfig{ConnectionString: connStr, AccountName: "arctestaccount", AccountKey: fakeAzureKey}},
		{"SAS token", storage.AzureBlobConfig{AccountName: "arctestaccount", SASToken: "sv=2023-01-03&sig=fake", Endpoint: endpoint}},
		{"managed identity", storage.AzureBlobConfig{AccountName: "arctestaccount", UseManagedIdentity: true, Endpoint: endpoint}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			cfg.ContainerName = "arc-data"
			cfg.Prefix = "instances/abc123/"
			parent, err := storage.NewAzureBlobBackend(&cfg, zerolog.Nop())
			if err != nil {
				t.Fatalf("NewAzureBlobBackend: %v", err)
			}
			defer parent.Close()

			// An unrelated AZURE_STORAGE_KEY in the inherited environment must
			// not change which authentication the subprocess uses.
			spec := roundTrip(t, parent, func(k string) string {
				if k == "AZURE_STORAGE_KEY" {
					return "aW5oZXJpdGVkLWtleQ=="
				}
				return ""
			})
			if got, want := authOf(spec.Azure), authOf(cfg); got != want {
				t.Errorf("subprocess authentication = %+v, want the parent's %+v", got, want)
			}
			if spec.Azure.ContainerName != "arc-data" || spec.Azure.Prefix != "instances/abc123/" {
				t.Errorf("container/prefix = %q/%q", spec.Azure.ContainerName, spec.Azure.Prefix)
			}
			// And the spec builds through the shared constructor.
			rebuilt, err := storage.NewBackend(spec, zerolog.Nop())
			if err != nil {
				t.Fatalf("NewBackend: %v", err)
			}
			rebuilt.Close()
		})
	}
}

// Managed identity is carried as a flag, not inferred from a missing key, so
// an inherited AZURE_STORAGE_KEY cannot replace it. Checked on the job alone:
// building a real managed-identity backend would reach for a token endpoint.
func TestSubprocessKeepsManagedIdentity(t *testing.T) {
	job := &SubprocessJobConfig{
		StorageType:   "azure",
		StorageConfig: `{"container":"arc-data","prefix":"","account_name":"arctestaccount","endpoint":""}`,
		Credentials:   &SubprocessCredentials{AzureUseManagedIdentity: true},
	}
	spec, err := backendSpecFromJob(job, func(k string) string {
		if k == "AZURE_STORAGE_KEY" {
			return "aW5oZXJpdGVkLWtleQ=="
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	if !spec.Azure.UseManagedIdentity || spec.Azure.AccountKey != "" {
		t.Errorf("auth = %+v, want managed identity and no key", authOf(spec.Azure))
	}
}

// A job from a parent that predates the credentials field (an upgrade while
// the parent kept running) is read the old way: the Azure key from the
// environment, managed identity when there is none.
func TestSubprocessReadsAnOlderParentsEnvironment(t *testing.T) {
	job := &SubprocessJobConfig{
		StorageType:   "azure",
		StorageConfig: `{"container":"arc-data","prefix":"","account_name":"arctestaccount","endpoint":""}`,
	}
	withKey, err := backendSpecFromJob(job, func(k string) string {
		if k == "AZURE_STORAGE_KEY" {
			return fakeAzureKey
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	if withKey.Azure.AccountKey != fakeAzureKey || withKey.Azure.UseManagedIdentity {
		t.Errorf("with AZURE_STORAGE_KEY: auth = %+v, want the key", authOf(withKey.Azure))
	}
	withoutKey, err := backendSpecFromJob(job, noEnv)
	if err != nil {
		t.Fatal(err)
	}
	if withoutKey.Azure.AccountKey != "" || !withoutKey.Azure.UseManagedIdentity {
		t.Errorf("without AZURE_STORAGE_KEY: auth = %+v, want managed identity", authOf(withoutKey.Azure))
	}
}

// The S3 keys the parent resolved, from its configuration or from AWS_* in its
// environment, reach the subprocess in the job. When it resolved none, the
// spec carries none and NewS3Backend in the subprocess falls back to the
// environment and SDK chain it inherits, as the parent's did.
func TestSubprocessGetsTheParentsS3Keys(t *testing.T) {
	for _, tc := range []struct{ name, access, secret string }{
		{"configured keys", "AKIAFAKE", "fake-secret"},
		{"environment credentials", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent, err := storage.NewS3Backend(&storage.S3Config{
				Bucket: "arc-data", Prefix: "instances/abc123/", Region: "us-west-2",
				Endpoint: "http://127.0.0.1:1", PathStyle: true,
				AccessKey: tc.access, SecretKey: tc.secret,
			}, zerolog.Nop())
			if err != nil {
				t.Skipf("cannot construct S3 backend in this environment: %v", err)
			}
			defer parent.Close()
			spec := roundTrip(t, parent, noEnv)
			if spec.S3.AccessKey != parent.GetAccessKey() || spec.S3.SecretKey != parent.GetSecretKey() {
				t.Errorf("subprocess keys = %q/%q, want the parent's", spec.S3.AccessKey, spec.S3.SecretKey)
			}
			if spec.S3.Prefix != "instances/abc123/" || spec.S3.Endpoint != "http://127.0.0.1:1" || !spec.S3.PathStyle {
				t.Errorf("S3 location = %+v", spec.S3)
			}
		})
	}
}

// The parent always sends a credentials object, even an empty one, so the
// subprocess never mistakes a current parent for an older one.
func TestSubprocessCredentialsAlwaysReachTheJob(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	raw, err := json.Marshal(&SubprocessJobConfig{
		StorageType: local.Type(), StorageConfig: local.ConfigJSON(),
		Credentials: subprocessCredentials(local),
	})
	if err != nil {
		t.Fatal(err)
	}
	var received SubprocessJobConfig
	if err := json.Unmarshal(raw, &received); err != nil {
		t.Fatal(err)
	}
	if received.Credentials == nil {
		t.Fatalf("job %s reached the subprocess without a credentials object", raw)
	}
}

// The manager attaches the credentials to the job it starts: the wiring the
// rest of this file assumes. A job without them would be read as one from an
// older parent and fall back to the environment.
func TestManagerJobCarriesTheBackendsCredentials(t *testing.T) {
	endpoint := azureStub(t)
	connStr := "DefaultEndpointsProtocol=http;AccountName=arctestaccount;AccountKey=" + fakeAzureKey + ";BlobEndpoint=" + endpoint + "/arctestaccount;"
	backend, err := storage.NewAzureBlobBackend(&storage.AzureBlobConfig{ConnectionString: connStr, ContainerName: "arc-data"}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	m := &Manager{StorageBackend: backend}
	job := m.subprocessJob(Candidate{Database: "db", Measurement: "cpu", PartitionPath: "db/cpu/2026/01/01/00", BatchNumber: 1}, "job-1")
	if job.Credentials == nil || job.Credentials.AzureConnectionString != connStr {
		t.Fatalf("job credentials = %+v, want the backend's connection string", job.Credentials)
	}
}
