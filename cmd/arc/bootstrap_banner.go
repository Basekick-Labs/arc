package main

import (
	"fmt"
	"io"
)

// printBootstrapBanner writes the startup banner for the admin token Arc
// installed at bootstrap.
//
// The token value is printed only when Arc generated it (configured is
// false): the operator has no other way to see it. When the value came from
// configuration (auth.bootstrap_token / ARC_AUTH_BOOTSTRAP_TOKEN) the operator
// already has it, and printing it would only copy a secret into the logs, on
// every start of a deployment whose auth database is not persistent. In that
// case the banner says the configured token was installed and leaves the value
// out. force is auth.force_bootstrap, the recovery path.
func printBootstrapBanner(w io.Writer, token string, configured, force bool) {
	const (
		cyan   = "\033[96m"
		yellow = "\033[93m"
		bold   = "\033[1m"
		reset  = "\033[0m"
	)
	banner := cyan + "======================================================================" + reset

	fmt.Fprintln(w)
	fmt.Fprintln(w, banner)
	switch {
	case force:
		fmt.Fprintln(w, cyan+bold+"  RECOVERY TOKEN ADDED - EXISTING TOKENS PRESERVED"+reset)
	case configured:
		fmt.Fprintln(w, cyan+bold+"  INITIAL ADMIN TOKEN INSTALLED FROM CONFIGURATION"+reset)
	default:
		fmt.Fprintln(w, cyan+bold+"  FIRST RUN - INITIAL ADMIN TOKEN GENERATED"+reset)
	}
	fmt.Fprintln(w, banner)
	if configured {
		fmt.Fprintln(w, cyan+"  The token from auth.bootstrap_token (ARC_AUTH_BOOTSTRAP_TOKEN) was installed."+reset)
		fmt.Fprintln(w, cyan+"  Its value is not printed here. Use the one you configured."+reset)
	} else {
		fmt.Fprintln(w, yellow+bold+"  Admin API token: "+token+reset)
		fmt.Fprintln(w, banner)
		fmt.Fprintln(w, cyan+"  SAVE THIS TOKEN! It will not be shown again."+reset)
		fmt.Fprintln(w, cyan+"  Use this token to login to the web UI or API."+reset)
	}
	if force {
		fmt.Fprintln(w, cyan+"  Use the API to revoke any tokens you no longer need."+reset)
		fmt.Fprintln(w, cyan+"  Remove ARC_AUTH_FORCE_BOOTSTRAP after recovery."+reset)
	} else if !configured {
		fmt.Fprintln(w, cyan+"  You can create additional tokens after logging in."+reset)
	}
	fmt.Fprintln(w, banner)
	fmt.Fprintln(w)
}
