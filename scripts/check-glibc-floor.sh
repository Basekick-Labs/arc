#!/usr/bin/env bash
# check-glibc-floor.sh BINARY [FLOOR]
#
# Prints the highest GLIBC_ version BINARY requires. With FLOOR, fails (exit 1)
# when that is above FLOOR and names the symbols responsible (#1579: 26.09.3,
# built on ubuntu-latest, needed 2.38 for fmod/fmodf alone and would not start
# on Debian 12 or Ubuntu 22.04, nor install on RHEL 9).
#
# The versions come from the version-needed table (readelf -V), which is what
# the loader enforces; a need with no symbol attached (GLIBC_ABI_DT_RELR, from
# -z pack-relative-relocs) appears only there. A non-numeric GLIBC_ need other
# than GLIBC_PRIVATE is treated as above any floor. readelf, not objdump: the
# .deb job inspects arm64 binaries on an amd64 runner.
set -euo pipefail
bin=${1:?usage: check-glibc-floor.sh BINARY [FLOOR]}
floor=${2:-}

needs=$(readelf -V -W "$bin" 2>/dev/null | grep -o 'Name: GLIBC_[A-Za-z0-9_.]*' | sed 's/Name: GLIBC_//' | sort -u || true)
if [ -z "$needs" ]; then
	echo "no GLIBC_ version needs in $bin (not a dynamically linked glibc binary?)" >&2
	exit 1
fi
numeric=$(grep -E '^[0-9]+(\.[0-9]+)*$' <<<"$needs" || true)
other=$(grep -vE '^[0-9]+(\.[0-9]+)*$' <<<"$needs" | grep -v '^PRIVATE$' || true)
need=$(sort -V <<<"$numeric" | tail -1)

above() { # $1 > floor
	[ "$1" != "$floor" ] && [ "$(printf '%s\n%s\n' "$1" "$floor" | sort -V | tail -1)" = "$1" ]
}
if [ -n "$floor" ]; then
	bad=()
	for v in $numeric; do above "$v" && bad+=("GLIBC_$v"); done
	for v in $other; do bad+=("GLIBC_$v"); done
	if [ ${#bad[@]} -gt 0 ]; then
		echo "::error::$bin requires ${bad[*]}, above the supported glibc floor $floor. Symbols:" >&2
		syms=$(readelf --dyn-syms -W "$bin" | grep -o '[A-Za-z0-9_.]*@\{1,2\}GLIBC_[A-Za-z0-9_.]*' | sort -u || true)
		for b in "${bad[@]}"; do
			grep -E "@{1,2}${b}\$" <<<"$syms" | sed 's/^/  /' >&2 || echo "  (no symbol: a version need only, $b)" >&2
		done
		exit 1
	fi
fi
echo "$need"
