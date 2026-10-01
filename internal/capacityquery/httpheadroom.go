// Package capacityquery lets heain-job's Stage B scheduler query each
// replica's own real, live capacity (RAM/GPU) over HTTP, rather than
// assuming a constant value. Every Layer 3 module exposes this same
// GET /capacity contract via heain-sdk's capacity package (the same
// probe heain-core's own P2 dispatcher uses to measure a node's live
// resources) -- heain-job never probes hardware itself, it only reads
// what each replica already measured and reported about itself.
package capacityquery

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/heainframework/heain-job/internal/jobapp"
	"github.com/heainframework/heain-job/internal/scheduler"
	"github.com/heainframework/heain-sdk/capacity"
)

// DefaultTimeout bounds how long Assign waits on one replica's
// /capacity call before treating it as unreachable. Kept short since
// Assign calls this once per replica, synchronously, before scoring --
// a slow/hung replica must not stall the whole scheduling decision.
const DefaultTimeout = 3 * time.Second

// HTTP returns a scheduler.CapacityHeadroom that queries each
// replica's own GET {base_url}/capacity endpoint (Bearer-token
// authenticated with the replica's own registered token, the same
// token already used for /split, /merge, /process-subunit) and
// reduces the result via capacity.Capacity.Score(). A replica that
// fails to respond (unreachable, times out, non-200, bad JSON) scores
// 0 -- the least favorable outcome, not an error that aborts the
// whole scheduling decision -- so one broken replica never blocks
// assigning work to its healthy siblings. client may be nil, in which
// case a default client with DefaultTimeout is used.
func HTTP(client *http.Client) scheduler.CapacityHeadroom {
	if client == nil {
		client = &http.Client{Timeout: DefaultTimeout}
	}
	return func(ep jobapp.ModuleEndpoint) float64 {
		ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ep.BaseURL+"/capacity", nil)
		if err != nil {
			return 0
		}
		req.Header.Set("Authorization", "Bearer "+ep.Token)

		resp, err := client.Do(req)
		if err != nil {
			return 0
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return 0
		}

		var c capacity.Capacity
		if err := json.NewDecoder(resp.Body).Decode(&c); err != nil {
			return 0
		}
		return c.Score()
	}
}
