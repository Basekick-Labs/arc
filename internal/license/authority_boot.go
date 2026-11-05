package license

import (
	"context"
	"time"
)

var authorityBootRetryDelay = 5 * time.Second

// AwaitOnlineAuthority holds startup before components can serve as licensed or
// accidentally become independent OSS writers during an authority outage. A
// definitive rejection remains a rejection; temporary failures never authorize
// cache-only startup for online keys. The caller controls shutdown with ctx.
func (c *Client) AwaitOnlineAuthority(ctx context.Context) (*License, LicenseSource, error) {
	for {
		lic, source, err := c.ActivateOrVerifyResilient(ctx)
		if err == nil || FailureClassOf(err) == ClassDefinitive || c.offline {
			return lic, source, err
		}
		if ctx.Err() != nil {
			return nil, SourceServer, ctx.Err()
		}
		c.logger.Warn().Err(err).Msg("Fresh signed license-server time unavailable; waiting before starting data services")
		timer := time.NewTimer(authorityBootRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, SourceServer, ctx.Err()
		case <-timer.C:
		}
	}
}
