#!/usr/bin/env bash
# generate-rpm-signing-key.sh — one-off: create the Basekick Labs RPM signing
# key and store it as the RPM_SIGNING_KEY / RPM_SIGNING_PASSPHRASE secrets of
# the repos that publish RPMs. Run on a trusted machine, interactively.
#
# Leaves two files in the current directory:
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

# RSA 4096, sign-only, 5 years: verifiable by every supported rpm
# (RHEL 9, Amazon Linux 2023, Fedora).
gpg --batch --pinentry-mode loopback --passphrase "$PASS" \
	--quick-gen-key "$UID_STR" rsa4096 sign 5y
FPR=$(gpg --list-keys --with-colons | awk -F: '/^fpr:/ {print $10; exit}')

gpg --armor --export "$FPR" > RPM-GPG-KEY-basekick
umask 077
gpg --armor --batch --pinentry-mode loopback --passphrase "$PASS" \
	--export-secret-keys "$FPR" > basekick-rpm-signing-SECRET.asc

for repo in "${REPOS[@]}"; do
	gh secret set RPM_SIGNING_KEY -R "$repo" < basekick-rpm-signing-SECRET.asc
	printf '%s' "$PASS" | gh secret set RPM_SIGNING_PASSPHRASE -R "$repo"
	echo "secrets set on $repo"
done

echo
echo "Fingerprint: $FPR"
echo "Public key:  $(pwd)/RPM-GPG-KEY-basekick"
echo "Private key: $(pwd)/basekick-rpm-signing-SECRET.asc  <- back it up, then delete it"
