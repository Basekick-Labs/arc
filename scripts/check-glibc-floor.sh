#!/usr/bin/env bash
# check-glibc-floor.sh BINARY [FLOOR]
#
# Prints the highest GLIBC_ symbol version BINARY requires. With FLOOR, fails
# (exit 1) when that is above FLOOR and names the symbols responsible (#1579:
# 26.09.3, built on ubuntu-latest, needed 2.38 for fmod/fmodf alone and would
# not start on Debian 12 or Ubuntu 22.04, nor install on RHEL 9).
#
# readelf, not objdump: the .deb job inspects arm64 binaries on an amd64
# runner, and readelf reads any ELF.
set -euo pipefail
bin=${1:?usage: check-glibc-floor.sh BINARY [FLOOR]}
floor=${2:-}

# "symbol@GLIBC_x.y" (or @@) for every dynamic symbol carrying a glibc version.
syms=$(readelf --dyn-syms -W "$bin" | grep -o '[A-Za-z0-9_.]*@\{1,2\}GLIBC_[0-9.]*' | sort -u || true)
if [ -z "$syms" ]; then
	echo "no GLIBC_ symbol versions in $bin (not a dynamically linked glibc binary?)" >&2
	exit 1
fi
need=$(sed 's/.*GLIBC_//' <<<"$syms" | sort -Vu | tail -1)
if [ -n "$floor" ] && [ "$need" != "$floor" ] &&
	[ "$(printf '%s\n%s\n' "$need" "$floor" | sort -V | tail -1)" = "$need" ]; then
	echo "::error::$bin requires glibc $need, above the supported floor $floor. Symbols above the floor:" >&2
	while read -r s; do
		v=${s##*GLIBC_}
		if [ "$v" != "$floor" ] && [ "$(printf '%s\n%s\n' "$v" "$floor" | sort -V | tail -1)" = "$v" ]; then
			echo "  $s" >&2
		fi
	done <<<"$syms"
	exit 1
fi
echo "$need"
