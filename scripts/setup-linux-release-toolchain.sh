#!/usr/bin/env bash
# setup-linux-release-toolchain.sh
#
# Prepares a GitHub Actions Ubuntu 22.04 runner to link the Linux release
# binary so it runs on every supported distro (#1579), and exports the
# environment for the go build through $GITHUB_ENV.
#
# - glibc: the binary takes the runner's (2.35), which caps what it needs at
#   GLIBC_2.34; check-glibc-floor.sh enforces it.
# - libstdc++: DuckDB's prebuilt archives need GCC 12's (GLIBCXX_3.4.30), which
#   RHEL 9 and Amazon Linux 2023 do not ship (GCC 11). It is linked in
#   statically instead. duckdb-go-bindings hardcodes -lstdc++, so
#   -static-libstdc++ has no effect; a directory holding only GCC 12's
#   libstdc++.a, first on the search path, makes -lstdc++ resolve to it.
# - libgcc stays dynamic (libgcc_s.so.1, on every distro): GCC 12's static
#   unwinder calls _dl_find_object, which needs glibc 2.35.
set -euo pipefail
sudo apt-get update -qq
sudo apt-get install -y -qq gcc-12 g++-12
dir="${RUNNER_TEMP:-/tmp}/static-libstdcxx"
mkdir -p "$dir"
ln -sf "$(g++-12 -print-file-name=libstdc++.a)" "$dir/libstdc++.a"
test -e "$dir/libstdc++.a"
{
	echo "CC=gcc-12"
	echo "CXX=g++-12"
	echo "CGO_LDFLAGS=-L$dir"
} >> "${GITHUB_ENV:?not running under GitHub Actions}"
