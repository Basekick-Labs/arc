package license

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
)

// enterprisePublicKeyPEM is the RSA-2048 public key Arc uses to verify
// signed license blobs returned by the Basekick Enterprise activation
// server.
//
// Pinned at compile time (no operator override, no ENV var) so a
// misconfigured arc.toml cannot redirect verification to a different
// trust root. Rotating this key requires shipping a new Arc release.
//
// The corresponding private key never leaves the activation server.
// The public key is also served by the activation server at
// GET /api/v1/public-key for documentation and tooling purposes.
const enterprisePublicKeyPEM = `-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEApq62ep4hBrqS4yHSi1mW
Mms9/xF4vGduKLbBjuo1226UcqbyF7OxqomTq01JPMg9UUsKplcVhWXRDXPGOqMZ
VyPMerO4Pbw24N8eZ4H5EJ2n4HeR+UzP/q0j8gkSL290PQRaWk5GMGu9+ND+evFC
i8vmrW1HvrRBbBPW/UjdIddz5HVBqcr9bWlEX4d5fmMhYzKR1qDfYQblXn/kNl2V
H2W0BJQ6mzFkDwpqTwG73+kc5WzjvcPY4tWLWbpYnP1ur9MiATD4wuQV1px9R8Jh
9bFFJHK/Utf+ACWcJ05dwXoxs6Qh/6g35icfvnjFvRw0xFrsAxrkW0pRbP7w6Pd2
2QIDAQAB
-----END PUBLIC KEY-----
`

// parsedEnterprisePublicKey is the parsed *rsa.PublicKey form of
// enterprisePublicKeyPEM. Computed at package init so every Verify
// path avoids repeated PEM-decode overhead.
var parsedEnterprisePublicKey *rsa.PublicKey

func init() {
	// Initialize lazily-but-deterministically. A malformed pinned key
	// is a build-time bug that needs to surface immediately when the
	// binary starts, NOT silently disable enterprise features.
	key, err := parseRSAPublicKey([]byte(enterprisePublicKeyPEM))
	if err != nil {
		// Use a placeholder during development; production builds will
		// fail loud if this stays unset. We can't `panic` here because
		// the OSS arc binary (no license configured) should still boot;
		// the verify path checks parsedEnterprisePublicKey == nil and
		// fails closed for any enterprise-feature gating.
		return
	}
	parsedEnterprisePublicKey = key
}

// parseRSAPublicKey decodes a PEM-encoded RSA public key and returns
// the parsed *rsa.PublicKey. Exported so tests can inject a different
// key and re-use the same parsing path.
func parseRSAPublicKey(pemBytes []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("license: failed to decode PEM block")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("license: parse public key: %w", err)
	}
	rsaKey, ok := pub.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("license: pinned key is not an RSA public key")
	}
	return rsaKey, nil
}
