// Command duckdbseed pre-installs the DuckDB extensions Arc loads at startup
// into the local extension cache (~/.duckdb/extensions/<version>/<platform>/).
//
// It exists so the container image can bake the extensions in at build time.
// Without it, Arc's first run performs an INSTALL that fetches from
// extensions.duckdb.org — which fails outright in an air-gapped or
// egress-restricted deployment, and costs a network round-trip on first boot
// everywhere else. See ensureHTTPFSLoaded in internal/database/duckdb.go.
//
// Building this against the same go.mod as the server keeps the cached
// extensions in step with the bundled DuckDB version automatically: extension
// binaries are DuckDB-version- and platform-specific.
package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/duckdb/duckdb-go/v2"
)

// extensions must stay in sync with ensureHTTPFSLoaded.
var extensions = []string{"httpfs", "aws"}

func main() {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "open duckdb: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	for _, ext := range extensions {
		if _, err := db.Exec("INSTALL " + ext); err != nil {
			fmt.Fprintf(os.Stderr, "install %s: %v\n", ext, err)
			os.Exit(1)
		}
		// LOAD verifies the cached binary is actually usable, so a corrupt or
		// platform-mismatched download fails the image build rather than the
		// first query in production.
		if _, err := db.Exec("LOAD " + ext); err != nil {
			fmt.Fprintf(os.Stderr, "load %s: %v\n", ext, err)
			os.Exit(1)
		}
		fmt.Printf("seeded duckdb extension: %s\n", ext)
	}
}
