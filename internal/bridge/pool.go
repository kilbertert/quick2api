// pool.go turns the single-lane Bridge into a rotating lane pool when
// config carries quick.accounts: each account is one session source,
// requests round-robin across live lanes, and a lane that answers
// 401/403/5xx sits out for cooldown_seconds before serving again.
//
// devin2api uses the same shape for its devin accounts.
package bridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/kilbertert/quick2api/internal/chat"
	"github.com/kilbertert/quick2api/internal/config"
)

// Lane wraps one upstream account with its own session + conn pool +
// cooldown state. Structurally identical to Bridge — we share the code.
type Lane struct {
	*Bridge

	name      string
	cooldownT time.Duration
	until     atomic.Int64 // unix nano; 0 = live
	lastErr   atomic.Value // error
}

// Pool is a fixed set of lanes rotated round-robin.
type Pool struct {
	lanes []*Lane
	next  atomic.Uint32
	turns conversationLocks
}

// BuildPool constructs one Lane per accounts entry. When accounts is
// empty it returns a single lane built from the top-level quick.* fields
// — the caller always gets a Pool, just of size 1.
func BuildPool(ctx context.Context, cfg *config.Config) (*Pool, error) {
	accounts := cfg.Quick.Accounts
	if len(accounts) == 0 {
		lane, err := newLane(ctx, cfg, config.AccountConfig{Name: "default"})
		if err != nil {
			return nil, err
		}
		return &Pool{lanes: []*Lane{lane}}, nil
	}
	p := &Pool{}
	for i, a := range accounts {
		lane, err := newLane(ctx, cfg, a)
		if err != nil {
			// Failed lanes still join the pool — they will answer 503
			// until their credentials recover, without dragging the rest
			// down. This matches devin2api's per-account state.
			slog.Warn("lane failed to come up, marking degraded", "lane", a.Name, "err", err)
			lane = &Lane{
				Bridge: &Bridge{cfg: cfg},
				name:   a.Name,
			}
			lane.lastErr.Store(err)
		}
		lane.cooldownT = laneCooldown(a, cfg)
		p.lanes = append(p.lanes, lane)
		slog.Info("lane ready", "lane", a.Name, "cooldown", lane.cooldownT)
		_ = i
	}
	return p, nil
}

// newLane builds a Bridge for one account config, then wraps it.
func newLane(ctx context.Context, cfg *config.Config, a config.AccountConfig) (*Lane, error) {
	// Per-account overrides: the top-level quick.* fields stay the
	// baseline; an accounts entry can redirect to a different outpost or
	// session file without touching them.
	sub := *cfg
	if a.OutpostURL != "" {
		sub.Quick.OutpostURL = a.OutpostURL
		sub.Quick.OutpostToken = a.OutpostToken
		sub.Quick.SessionFile = ""
	} else if a.SessionFile != "" {
		sub.Quick.SessionFile = a.SessionFile
		sub.Quick.OutpostURL = ""
		sub.Quick.OutpostToken = ""
	}
	b, err := New(ctx, &sub)
	if err != nil {
		return nil, err
	}
	return &Lane{Bridge: b, name: a.Name}, nil
}

// laneCooldown picks the per-lane cooldown override, else the pool default.
func laneCooldown(a config.AccountConfig, cfg *config.Config) time.Duration {
	if a.CooldownS > 0 {
		return time.Duration(a.CooldownS) * time.Second
	}
	if cfg.Quick.CooldownSecs > 0 {
		return time.Duration(cfg.Quick.CooldownSecs) * time.Second
	}
	return 60 * time.Second
}

// ownerOf returns the lane whose bridge still holds a live conn for id,
// or nil. Affinity is "the socket lives where it was opened" — no TTL
// map needed; once the conn dies the next request re-picks.
func (p *Pool) ownerOf(id string) *Lane {
	for _, l := range p.lanes {
		if l.Bridge != nil && l.Bridge.Owns(id) {
			return l
		}
	}
	return nil
}

// pick selects the next live lane round-robin, skipping cooled ones.
// Returns the lane plus its 1-based index for logging; nil when every
// lane is cooled (caller then waits or reports 503).
func (p *Pool) pick() (*Lane, int) {
	n := len(p.lanes)
	for i := 0; i < n; i++ {
		idx := int(p.next.Add(1)) % n
		l := p.lanes[idx]
		if l.until.Load() == 0 || time.Now().UnixNano() > l.until.Load() {
			return l, idx
		}
	}
	return nil, -1
}

// cool marks a lane down for its cooldown duration.
func (l *Lane) cool(err error) {
	l.lastErr.Store(err)
	l.until.Store(time.Now().Add(l.cooldownT).UnixNano())
	slog.Warn("lane cooled", "lane", l.name, "err", err, "for", l.cooldownT)
}

// RunTurn picks the lane holding a live conn for opts.ConversationID when
// one exists (affinity follows the socket, not a TTL map); otherwise it
// rotates live lanes. Auth-type failures cool the lane and retry once on
// another lane.
func (p *Pool) RunTurn(ctx context.Context, opts chat.TurnOpts) (*chat.TurnResult, error) {
	// Lock before lane selection: two first requests for one conversation
	// must not open independent connections on different lanes.
	release, err := p.turns.acquire(ctx, normalizeConvID(opts.ConversationID))
	if err != nil {
		return nil, err
	}
	defer release()
	if opts.ConversationID != "" {
		if l := p.ownerOf(normalizeConvID(opts.ConversationID)); l != nil {
			return l.Bridge.RunTurn(ctx, opts)
		}
	}
	l, _ := p.pick()
	if l == nil {
		var last any
		for _, ln := range p.lanes {
			if e := ln.lastErr.Load(); e != nil {
				last = e
			}
		}
		return nil, fmt.Errorf("no live lanes (all cooled): %v", last)
	}
	res, err := l.Bridge.RunTurn(ctx, opts)
	if err != nil && coolable(err) && opts.NativeRelay == "" {
		l.cool(err)
		// One retry on another lane — the request shouldn't die just
		// because the first pick went stale.
		if l2, _ := p.pick(); l2 != nil && l2 != l {
			slog.Info("retrying on alternate lane", "from", l.name, "to", l2.name)
			return l2.Bridge.RunTurn(ctx, opts)
		}
	}
	return res, err
}

// Start launches the lane health probe: every cfg.HealthProbeEvery() it
// re-fetches each lane's session, which forces a token mint — a lane
// whose credentials died cools itself before the next request lands.
// No-op when probing is disabled.
func (p *Pool) Start(ctx context.Context) {
	every := p.probeEvery()
	if every <= 0 {
		return
	}
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				p.probeAll()
			}
		}
	}()
}

func (p *Pool) probeEvery() time.Duration {
	for _, l := range p.lanes {
		if l.Bridge != nil && l.Bridge.cfg != nil {
			return l.Bridge.cfg.HealthProbeEvery()
		}
	}
	return 0
}

// probeAll re-fetches each lane's session once. A lane that errors is
// marked cooled (its lastErr is visible on /healthz); a lane that
// succeeds clears its cooldown.
func (p *Pool) probeAll() {
	for _, l := range p.lanes {
		if l.Bridge == nil {
			continue
		}
		if _, err := l.Bridge.Session(true); err != nil {
			slog.Warn("lane health probe failed", "lane", l.name, "err", err)
			l.cool(err)
		} else if l.until.Load() != 0 {
			slog.Info("lane recovered", "lane", l.name)
			l.until.Store(0)
		}
	}
}

// coolable reports whether an error justifies cooling the lane: auth
// rejections and 5xx mean the credential or endpoint is actually broken,
// whereas a plain upstream error or timeout is a transient per-request.
func coolable(err error) bool {
	var ue *chat.UpstreamError
	if errors.As(err, &ue) {
		switch ue.Code {
		case "unauthenticated", "token_expired", "forbidden", "upstream_error":
			return true
		}
	}
	return false
}

// Info reports lane state for /healthz.
func (p *Pool) Info() map[string]any {
	out := map[string]any{"lanes": len(p.lanes)}
	var lanes []map[string]any
	for _, l := range p.lanes {
		entry := map[string]any{"name": l.name}
		if l.until.Load() > 0 {
			entry["state"] = "cooled"
			entry["until"] = time.Unix(0, l.until.Load()).Format(time.RFC3339)
		} else {
			entry["state"] = "live"
		}
		if e := l.lastErr.Load(); e != nil {
			entry["last_error"] = fmt.Sprint(e)
		}
		if l.Bridge != nil {
			for k, v := range l.Bridge.Info() {
				if k == "session_source" || k == "pooled_conversations" {
					entry[k] = v
				}
			}
		}
		lanes = append(lanes, entry)
	}
	out["lanes_detail"] = lanes
	return out
}

// Close shuts every lane down.
func (p *Pool) Close() {
	for _, l := range p.lanes {
		if l.Bridge != nil {
			l.Bridge.Close()
		}
	}
}
