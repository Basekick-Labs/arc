package license

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// AuthorityTime is signed independently of the reusable license. A new nonce
// proves the statement belongs to the current request, not a cached response.
type AuthorityTime struct {
	Version            int       `json:"version"`
	Purpose            string    `json:"purpose"`
	Nonce              string    `json:"nonce"`
	LicenseKeySHA256   string    `json:"license_key_sha256"`
	MachineFingerprint string    `json:"machine_fingerprint"`
	LicenseSHA256      string    `json:"license_sha256"`
	ServerTime         time.Time `json:"server_time"`
}

func timeNonce() (string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value[:]), nil
}

func verifyAuthorityTime(file, signature, nonce, key, fingerprint, licenseFile string) (*AuthorityTime, error) {
	if file == "" || signature == "" {
		return nil, fmt.Errorf("signed server time missing")
	}
	if parsedEnterprisePublicKey == nil {
		return nil, ErrNoPublicKey
	}
	raw, err := base64.StdEncoding.DecodeString(file)
	if err != nil {
		return nil, fmt.Errorf("malformed server time")
	}
	sig, err := base64.StdEncoding.DecodeString(signature)
	if err != nil {
		return nil, fmt.Errorf("malformed server time signature")
	}
	digest := sha256.Sum256(raw)
	if rsa.VerifyPKCS1v15(parsedEnterprisePublicKey, crypto.SHA256, digest[:], sig) != nil {
		return nil, fmt.Errorf("server time signature rejected")
	}
	var grant AuthorityTime
	if err := json.Unmarshal(raw, &grant); err != nil {
		return nil, fmt.Errorf("malformed signed server time")
	}
	payload, err := base64.StdEncoding.DecodeString(licenseFile)
	if err != nil {
		return nil, fmt.Errorf("malformed bound license")
	}
	keyHash, payloadHash := sha256.Sum256([]byte(key)), sha256.Sum256(payload)
	challenge, err := base64.RawURLEncoding.DecodeString(nonce)
	if err != nil || len(challenge) != 32 || base64.RawURLEncoding.EncodeToString(challenge) != nonce ||
		grant.Version != 1 || grant.Purpose != "arc-license-time" || grant.Nonce != nonce ||
		grant.LicenseKeySHA256 != hex.EncodeToString(keyHash[:]) || grant.MachineFingerprint != fingerprint ||
		grant.LicenseSHA256 != hex.EncodeToString(payloadHash[:]) || grant.ServerTime.IsZero() {
		return nil, fmt.Errorf("server time binding rejected")
	}
	return &grant, nil
}

// authorityClock never consults the wall clock after construction. A signed
// server sample replaces the local origin, and elapsed monotonic time advances
// it during authority outages. The local origin is explicitly untrusted.
type authorityClock struct {
	mu            sync.Mutex
	tick          func() time.Time
	origin        time.Time
	at            time.Time
	lastServer    time.Time
	trusted       bool
	unavailable   bool
	licenseDigest string
}

func newAuthorityClock() *authorityClock {
	tick := newLicenseTicker()
	return &authorityClock{tick: tick, origin: time.Now().UTC(), at: tick()}
}

func (c *authorityClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	tick := c.tick()
	c.unavailable = tick.IsZero()
	if c.unavailable {
		return time.Time{}
	}
	return c.origin.Add(tick.Sub(c.at))
}

func (c *authorityClock) source() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.unavailable {
		return "unavailable"
	}
	if c.trusted {
		return "license_server"
	}
	return "local_unverified"
}

func (c *authorityClock) accept(grant *AuthorityTime, started time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	received := c.tick()
	if received.IsZero() || started.IsZero() {
		return fmt.Errorf("monotonic boot clock unavailable")
	}
	elapsed := received.Sub(started)
	if elapsed < 0 || elapsed > 30*time.Second {
		return fmt.Errorf("server time request exceeded freshness budget")
	}
	if !c.lastServer.IsZero() && grant.ServerTime.Before(c.lastServer) {
		return fmt.Errorf("server time moved backwards")
	}
	// Full request elapsed is an upper bound, rather than giving a delayed reply
	// extra licensed time. Once trusted, no refresh may move the clock backwards.
	next := grant.ServerTime.UTC().Add(elapsed)
	current := c.origin.Add(received.Sub(c.at))
	if c.trusted && next.Before(current) {
		next = current
	}
	c.origin, c.at, c.lastServer, c.trusted = next, received, grant.ServerTime.UTC(), true
	c.unavailable = false
	c.licenseDigest = grant.LicenseSHA256
	return nil
}

func (c *authorityClock) matchesLicense(file string) bool {
	payload, err := base64.StdEncoding.DecodeString(file)
	if err != nil {
		return false
	}
	digest := sha256.Sum256(payload)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.trusted && c.licenseDigest == hex.EncodeToString(digest[:])
}

// rejectLicense revokes cache eligibility without discarding elapsed authority
// time. A failed cache unlink must not make a rejected payload usable again.
func (c *authorityClock) rejectLicense() {
	c.mu.Lock()
	c.licenseDigest = ""
	c.mu.Unlock()
}

func (c *Client) runtimeClock() *authorityClock {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clock == nil {
		c.clock = newAuthorityClock()
	}
	return c.clock
}
