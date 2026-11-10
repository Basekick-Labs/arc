#!/usr/bin/env bash
# generate-rpm-signing-key.sh — one-off: create the Basekick Labs RPM signing
# key and store it as the RPM_SIGNING_KEY / RPM_SIGNING_PASSPHRASE secrets of
# the repos that publish RPMs. Run on a trusted machine, interactively.
#
# Rotation: the key expires 5 years after creation (the first one on
# 2031-11-09). Extend it before then with gpg --quick-set-expire on the
# backed-up private key and re-publish the public key; every user must
# re-import it (rpm --import), so announce it in the release notes.
#
# Leaves two files in a new private directory (printed at the end):
#   RPM-GPG-KEY-basekick         the public key: commit it as
#                                deploy/packaging/RPM-GPG-KEY-basekick
#   basekick-rpm-signing-SECRET.asc
#                                the private key: put it in the team's
#                                password manager / offline backup, then
#                                delete it. Losing it means a key rotation.
set -euo pipefail

REPOS=(Basekick-Labs/arc Basekick-Labs/arcli)
UID_STR="Basekick Labs Package Signing <contact@basekick.com>"

command -v gpg >/dev/null || { echo "gpg is required" >&2; exit 1; }
command -v gh >/dev/null || { echo "gh is required" >&2; exit 1; }

read -rsp "Passphrase for the new signing key: " PASS; echo
read -rsp "Repeat passphrase: " PASS2; echo
[ -n "$PASS" ] && [ "$PASS" = "$PASS2" ] || { echo "passphrases empty or different" >&2; exit 1; }

GNUPGHOME=$(mktemp -d); export GNUPGHOME
trap 'rm -rf "$GNUPGHOME"' EXIT
# Outside any checkout, so the private key cannot be committed by accident.
OUT=$(mktemp -d "${TMPDIR:-/tmp}/basekick-rpm-key.XXXXXX")
# The passphrase goes to gpg through a file, never argv (visible in ps).
PASSFILE="$GNUPGHOME/passphrase"
( umask 077; printf '%s' "$PASS" > "$PASSFILE" )

# RSA 4096, sign-only, 5 years: verifiable by every supported rpm
# (RHEL 9, Amazon Linux 2023, Fedora).
gpg --batch --pinentry-mode loopback --passphrase-file "$PASSFILE" \
	--quick-gen-key "$UID_STR" rsa4096 sign 5y
FPR=$(gpg --list-keys --with-colons | awk -F: '/^fpr:/ {print $10; exit}')

gpg --armor --export "$FPR" > "$OUT/RPM-GPG-KEY-basekick"
umask 077
gpg --armor --batch --pinentry-mode loopback --passphrase-file "$PASSFILE" \
	--export-secret-keys "$FPR" > "$OUT/basekick-rpm-signing-SECRET.asc"

for repo in "${REPOS[@]}"; do
	gh secret set RPM_SIGNING_KEY -R "$repo" < "$OUT/basekick-rpm-signing-SECRET.asc"
	gh secret set RPM_SIGNING_PASSPHRASE -R "$repo" < "$PASSFILE"
	echo "secrets set on $repo"
done

echo
echo "Fingerprint: $FPR"
echo "Public key:  $OUT/RPM-GPG-KEY-basekick  <- commit as deploy/packaging/RPM-GPG-KEY-basekick"
echo "Private key: $OUT/basekick-rpm-signing-SECRET.asc  <- back it up, then delete it"
