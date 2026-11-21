package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/andreacoppini/crossbreeder/ap"
)

// Runner fans a host list out over a bounded pool of workers. This is the whole
// answer to "it only does one AP at a time": the per-AP work is pure network
// wait, so N in flight costs N sockets and a few kilobytes of stack, not N CPUs.
type Runner struct {
	Concurrency int
	Config      ap.Config
	// Stagger spaces out when each host's work starts, so a shared firmware
	// server is not asked to serve every AP at the same instant. It is a
	// start-time interval rather than a pause after each host: with Stagger set
	// to 20s the k-th host begins at least 20s after the (k-1)-th, whatever
	// Concurrency happens to be. Zero disables it.
	Stagger time.Duration
	// PerHostCredentials, if set, returns credentials to try ahead of
	// Config.Credentials for a given address — the RUCKUS One per-AP passwords.
	// It is read-only during the run, so it needs no locking.
	PerHostCredentials func(ip string) []ap.Credentials
	// OnResult is called once per finished AP, from a worker goroutine, in
	// completion order. It must be safe to call concurrently.
	OnResult func(index int, r ap.Result)
}

// Run blocks until every host has a Result. Results come back in host order.
func (rn *Runner) Run(ctx context.Context, hosts []string) []ap.Result {
	n := rn.Concurrency
	if n < 1 {
		n = 1
	}
	if n > len(hosts) {
		n = len(hosts)
	}

	results := make([]ap.Result, len(hosts))
	var next atomic.Int64
	var wg sync.WaitGroup

	// The next free start time. Workers claim a slot under the lock and sleep
	// until it, so the spacing holds even when several of them are running.
	var slotMu sync.Mutex
	slot := time.Now()
	takeSlot := func() time.Time {
		slotMu.Lock()
		defer slotMu.Unlock()
		at := slot
		if now := time.Now(); now.After(at) {
			at = now
		}
		slot = at.Add(rn.Stagger)
		return at
	}

	for w := 0; w < n; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= len(hosts) {
					return
				}
				if ctx.Err() != nil {
					results[i] = ap.Result{IP: hosts[i], Status: "Cancelled"}
					continue
				}
				if rn.Stagger > 0 && !sleepUntil(ctx, takeSlot()) {
					results[i] = ap.Result{IP: hosts[i], Status: "Cancelled"}
					continue
				}
				// Each worker writes only to its own slot, so no lock is needed
				// on results itself.
				cfg := rn.Config
				if rn.PerHostCredentials != nil {
					if extra := rn.PerHostCredentials(hosts[i]); len(extra) > 0 {
						// A fresh slice, R1 passwords first: never mutate the
						// shared Config.Credentials, and try the AP's own
						// password before the manual fallbacks.
						cfg.Credentials = append(append([]ap.Credentials{}, extra...), rn.Config.Credentials...)
					}
				}
				results[i] = ap.Run(ctx, hosts[i], cfg)
				if rn.OnResult != nil {
					rn.OnResult(i, results[i])
				}
			}
		}()
	}
	wg.Wait()
	return results
}

// sleepUntil waits for at, or reports false if the run was cancelled first.
func sleepUntil(ctx context.Context, at time.Time) bool {
	d := time.Until(at)
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
