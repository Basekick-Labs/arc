package edgesync

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// AttestedDeleter is delete_after_sync for files a signed air-gap ack
// attested as held (edge sync 3c, #1684). ApplyAck records the attestation;
// this deletes the local copies it still covers, under the 3a rules: the
// delay since sync, the size check and tiering claim in the LocalDeleter, and
// held_compacted only when accepted. Plus the ack's own bound: an
// attestation older than MaxAge on the spoke's clock (measured from the
// bundle's export) no longer licenses a delete, since an ack in transit
// cannot see hub retention or forget.
type AttestedDeleter struct {
	Ledger          *Ledger
	HubID           string
	Delete          LocalDeleter
	AfterDeletes    func() // once per page that deleted files (query caches)
	Delay           time.Duration
	MaxAge          time.Duration
	AcceptCompacted bool
	Logger          zerolog.Logger

	mu sync.Mutex // one run at a time: post-ack and the timer can overlap
}

// attestedPage bounds one candidate page.
const attestedPage = 1000

// Run deletes what the attestations still cover and reports how many local
// copies went. Stops, deleting nothing more, when the deleter is not ready.
func (d *AttestedDeleter) Run(ctx context.Context) (int, error) {
	if d == nil || d.Delete == nil {
		return 0, nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	states := []string{HubStateAckHeld}
	if d.AcceptCompacted {
		states = append(states, HubStateAckHeldCompacted)
	}
	now := time.Now().UTC()
	exportedAfter := now.Add(-d.MaxAge)
	syncedBefore := now.Add(-d.Delay)
	var (
		afterID int64
		total   int
	)
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		rows, err := d.Ledger.AttestedCandidates(ctx, d.HubID, states, exportedAfter, syncedBefore, afterID, attestedPage)
		if err != nil {
			return total, err
		}
		if len(rows) == 0 {
			return total, nil
		}
		afterID = rows[len(rows)-1].ID
		deleted := 0
		stop := false
		for _, e := range rows {
			hubState := e.HubState
			claimed, err := d.Ledger.ClaimAttested(ctx, d.HubID, e.Path, hubState)
			if err != nil {
				return total + deleted, err
			}
			if !claimed {
				// A network verify answered in between: it wins.
				continue
			}
			err = d.Delete(ctx, e.Path, e.SizeBytes)
			switch {
			case errors.Is(err, ErrDeleterNotReady):
				if rerr := d.Ledger.ReleaseAttested(ctx, d.HubID, e.Path, hubState); rerr != nil {
					return total + deleted, rerr
				}
				stop = true
			case errors.Is(err, ErrLocalChanged):
				d.Logger.Warn().Err(err).Str("path", e.Path).Msg("delete_after_sync: an ack attests this file is held but the local copy changed; kept")
				if kerr := d.Ledger.KeepClaimed(ctx, d.HubID, e.Path); kerr != nil {
					return total + deleted, kerr
				}
			case err != nil:
				// Transient (a tiering move, an I/O error): the attestation
				// stays and the next run retries.
				d.Logger.Warn().Err(err).Str("path", e.Path).Msg("delete_after_sync: could not delete an attested file; the next run retries")
				if rerr := d.Ledger.ReleaseAttested(ctx, d.HubID, e.Path, hubState); rerr != nil {
					return total + deleted, rerr
				}
			default:
				deleted++
				if err := d.Ledger.MarkDeletedLocal(ctx, d.HubID, e.Path); err != nil {
					if deleted > 0 && d.AfterDeletes != nil {
						d.AfterDeletes()
					}
					return total + deleted, err
				}
			}
			if stop {
				break
			}
		}
		total += deleted
		if deleted > 0 && d.AfterDeletes != nil {
			d.AfterDeletes()
		}
		if stop {
			return total, nil
		}
	}
}
