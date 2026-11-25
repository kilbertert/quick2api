// Package bridge glues the three pieces of the upstream chain into one
// RunTurn call: outpost (spawn or attach) → sncs-session → chat WSS.
package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/kilbertert/quick2api/internal/chat"
	"github.com/kilbertert/quick2api/internal/config"
	"github.com/kilbertert/quick2api/internal/session"
)

// Bridge owns the upstream session source and a pool of persistent
// upstream conversations keyed by client-provided ConversationID.
type Bridge struct {
	cfg *config.Config

	mu      sync.Mutex
	outpost *session.Outpost // spawned; nil when attached/file mode
	sess    *session.Session
	sessAt  time.Time
	sessErr error

	// conns holds live upstream conversations (one AIDP socket each),
	// keyed by client-provided ConversationID → its thread_id. Entries are
	// swept by a ticker: idle past cfg.ConnIdleTTL() they are closed, and
	// past cfg.MaxPooledConns() the oldest is evicted — both bounds keep a
	// long-running process from accumulating dead sockets.
	conns map[string]*connEntry
	turns conversationLocks
	// lastUpErr keeps the most recent upstream error for /healthz so the
	// endpoint can report degraded state instead of a bare "ok".
	lastUpErr error

	stopSweep chan struct{}
}

// connEntry tracks a pooled connection's last-use time for the sweeper.
type connEntry struct {
	conn   *chat.Conn
	lastAt time.Time
	busy   bool
	native bool
}

// New prepares the session source. It eagerly verifies the source once so a
// broken install fails at startup, not on the first request. The conn pool
// sweeper starts here; ctx cancels it alongside the spawned outpost.
func New(ctx context.Context, cfg *config.Config) (*Bridge, error) {
	b := &Bridge{cfg: cfg, conns: make(map[string]*connEntry), stopSweep: make(chan struct{})}
	done := ctx.Done()
	go b.sweepLoop(done)
	switch {
	case cfg.Quick.SessionFile != "":
		s, err := session.LoadFile(cfg.Quick.SessionFile)
		if err != nil {
			return nil, err
		}
		b.sess, b.sessAt = s, time.Now()
		slog.Info("session source: file", "ws_host", s.WSHost, "provider", s.Provider)
	case cfg.Quick.OutpostURL != "":
		o := session.Attach(cfg.Quick.OutpostURL, cfg.Quick.OutpostToken)
		s, err := o.FetchSession()
		if err != nil {
			return nil, fmt.Errorf("attached outpost: %w", err)
		}
		b.outpost, b.sess, b.sessAt = o, s, time.Now()
		slog.Info("session source: attached outpost", "url", o.URL, "ws_host", s.WSHost)
	default:
		o, err := session.Spawn(done, session.SpawnOpts{
			AgentPath:   cfg.Quick.AgentPath,
			Manifest:    cfg.Quick.Manifest,
			BuildStage:  cfg.Quick.BuildStage,
			ProfileRoot: cfg.Quick.ProfileRoot,
			Port:        cfg.Quick.OutpostPort,
			WorkDir:     "",
		})
		if err != nil {
			return nil, err
		}
		s, err := o.FetchSession()
		if err != nil {
			o.Close()
			return nil, err
		}
		b.outpost, b.sess, b.sessAt = o, s, time.Now()
		slog.Info("session source: spawned outpost",
			"url", o.URL, "ws_host", s.WSHost, "provider", s.Provider,
			"account", s.AWSAccountID, "usable_until", time.Unix(int64(s.UsableUntil), 0))
	}
	return b, nil
}

// Close releases the spawned outpost, every pooled conversation, and the
// sweeper goroutine.
func (b *Bridge) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	select {
	case <-b.stopSweep:
	default:
		close(b.stopSweep)
	}
	if b.outpost != nil {
		b.outpost.Close()
		b.outpost = nil
	}
	for k, e := range b.conns {
		e.conn.Close()
		delete(b.conns, k)
	}
}

// sweepLoop drops pooled conversations idle longer than ConnIdleTTL and
// evicts the oldest when the pool exceeds MaxPooledConns.
func (b *Bridge) sweepLoop(done <-chan struct{}) {
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-b.stopSweep:
			return
		case <-t.C:
			b.sweepOnce()
		}
	}
}

func (b *Bridge) sweepOnce() {
	idle := b.cfg.ConnIdleTTL()
	max := b.cfg.MaxPooledConns()
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	// Idle TTL first.
	for k, e := range b.conns {
		if e.busy {
			continue
		}
		if now.Sub(e.lastAt) > idle {
			e.conn.Close()
			delete(b.conns, k)
		}
	}
	// Capacity second: evict oldest entries until under the cap.
	for len(b.conns) > max {
		var oldestKey string
		var oldest time.Time
		for k, e := range b.conns {
			if e.busy {
				continue
			}
			if oldestKey == "" || e.lastAt.Before(oldest) {
				oldestKey, oldest = k, e.lastAt
			}
		}
		if oldestKey == "" {
			break
		}
		e := b.conns[oldestKey]
		e.conn.Close()
		delete(b.conns, oldestKey)
	}
}

// Session returns a session fresh enough to use, refreshing when the cache
// is older than SessionTTL or when force is set (after an auth failure).
func (b *Bridge) Session(force bool) (*session.Session, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sess == nil {
		return nil, errors.New("no session source")
	}
	if !force && time.Since(b.sessAt) < b.cfg.SessionTTL() {
		return b.sess, nil
	}
	// File source can't refresh — reload the file (it may have been rewritten).
	if b.cfg.Quick.SessionFile != "" {
		s, err := session.LoadFile(b.cfg.Quick.SessionFile)
		if err != nil {
			return nil, err
		}
		b.sess, b.sessAt = s, time.Now()
		return s, nil
	}
	if b.outpost == nil {
		return b.sess, nil // attached but no outpost handle — shouldn't happen
	}
	s, err := b.outpost.FetchSession()
	if err != nil {
		b.sessErr = err
		return nil, fmt.Errorf("refresh sncs-session: %w", err)
	}
	slog.Info("sncs-session refreshed", "token", s.MaskedIDToken(), "usable_until", time.Unix(int64(s.UsableUntil), 0))
	b.sess, b.sessAt = s, time.Now()
	return s, nil
}

// ChatClient builds the WebSocket client from config.
func (b *Bridge) ChatClient() *chat.Client {
	return &chat.Client{
		SurfaceType:      b.cfg.Quick.SurfaceType,
		Channel:          b.cfg.Quick.Channel,
		Origin:           b.cfg.Quick.Origin,
		HandshakeTimeout: b.cfg.HandshakeTimeout(),
		StrictHandshake:  b.cfg.Quick.StrictHandshake,
		Proxy:            b.cfg.Quick.Proxy,
	}
}

// RunTurn fetches a fresh-enough session then runs the turn.
//
// When opts.ConversationID is set the bridge first tries a live pooled
// connection on that upstream thread; if none exists it dials a new
// conversation and pools it for the next request on the same ID. A dead
// socket is dropped and redialed once. One retry on a mid-handshake auth
// failure refreshes the sncs-session and retries on a fresh conn.
func (b *Bridge) RunTurn(ctx context.Context, opts chat.TurnOpts) (*chat.TurnResult, error) {
	opts.ConversationID = normalizeConvID(opts.ConversationID)
	release, err := b.turns.acquire(ctx, opts.ConversationID)
	if err != nil {
		return nil, err
	}
	defer release()
	sess, err := b.Session(false)
	if err != nil {
		b.noteErr(err)
		return nil, err
	}
	if opts.NativeRelay != "" {
		return b.sendNative(ctx, sess, opts)
	}

	// Session reuse: keep one live conversation per client ConversationID.
	if opts.ConversationID != "" {
		if res, err := b.sendOnPool(ctx, sess, opts); err == nil || !retryable(err) {
			return res, err
		}
		// The pooled conn went stale or the send died mid-handshake —
		// drop it and fall through to a fresh dial.
		b.dropConn(opts.ConversationID)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// The one-shot fallback also reseeds the complete transcript. The client
	// id must never reopen an existing upstream thread without its watermark.
	opts.ConversationID = ""
	res, err := b.ChatClient().RunTurn(ctx, sess, opts)
	if err != nil {
		var ue *chat.UpstreamError
		if errors.As(err, &ue) && (ue.Code == "unauthenticated" || ue.Code == "token_expired") {
			slog.Warn("auth rejected; refreshing session and retrying once")
			if sess2, ferr := b.Session(true); ferr == nil {
				return b.ChatClient().RunTurn(ctx, sess2, opts)
			}
		}
		b.noteErr(err)
		return nil, err
	}
	return res, nil
}

// normalizeConvID turns a client-supplied conversation id into one the
// upstream accepts as session_id. Claude Code passes metadata.user_id as a
// JSON blob ({"device_id":...,"session_id":"<uuid>"}); the upstream
// rejects any non-uuid id (invalid_session_id), and the proceed-anyway
// handshake then leaves files/upload/<id> unregistered — every attachment
// deliver 404s and oversized prompts degrade to chunked turns. Anything
// that is not a uuid cannot name a live upstream thread, so drop it.
func normalizeConvID(id string) string {
	if id == "" {
		return ""
	}
	if id[0] == '{' {
		var v struct {
			SessionID string `json:"session_id"`
		}
		if json.Unmarshal([]byte(id), &v) == nil && v.SessionID != "" {
			id = v.SessionID
		}
	}
	if !looksUUID(id) {
		return ""
	}
	return id
}

func looksUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < 36; i++ {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
				return false
			}
		}
	}
	return true
}

// retryable reports whether a Send failure is worth retrying on a fresh
// connection — i.e. it died on the wire, not inside the model.
func retryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var policy *chat.AttachmentPolicyError
	if errors.As(err, &policy) {
		return false
	}
	var ue *chat.UpstreamError
	return !errors.As(err, &ue)
}

// sendOnPool sends on the pooled conversation for opts.ConversationID,
// creating it if missing. Returns (nil, err) when the pooled path can't
// serve this turn so the caller can fall back to a one-shot.
func (b *Bridge) sendOnPool(ctx context.Context, sess *session.Session, opts chat.TurnOpts) (*chat.TurnResult, error) {
	b.mu.Lock()
	e := b.conns[opts.ConversationID]
	if e != nil && !e.conn.CanContinue(opts) {
		e.conn.Close()
		delete(b.conns, opts.ConversationID)
		e = nil
	}
	b.mu.Unlock()

	if e == nil {
		var err error
		// The client id identifies a local transcript, not an upstream thread.
		// A fresh connection has no watermark and must seed a fresh thread;
		// reconnecting the old upstream id would append the entire history twice.
		cn, err := b.ChatClient().NewConversation(ctx, sess, "")
		if err != nil {
			b.noteErr(err)
			return nil, err
		}
		e = &connEntry{conn: cn, lastAt: time.Now()}
		b.mu.Lock()
		b.conns[opts.ConversationID] = e
		b.mu.Unlock()
		slog.Info("upstream conversation opened", "conversation", opts.ConversationID)
	}
	res, err := e.conn.Send(ctx, sess, opts)
	b.mu.Lock()
	e.lastAt = time.Now()
	b.mu.Unlock()
	return res, err
}

// Owns reports whether this bridge holds a live pooled connection for
// the given client ConversationID — used by Pool for lane affinity.
func (b *Bridge) Owns(id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.conns[id]
	return e != nil && e.conn.Alive()
}

// dropConn removes and closes the pooled conn for an ID (stale / errored).
func (b *Bridge) dropConn(id string) {
	b.mu.Lock()
	if e := b.conns[id]; e != nil {
		e.conn.Close()
		delete(b.conns, id)
	}
	b.mu.Unlock()
}

// noteErr records the latest upstream error for the health endpoint.
func (b *Bridge) noteErr(err error) {
	b.mu.Lock()
	b.lastUpErr = err
	b.mu.Unlock()
}

// Info reports the current session source for /healthz (no secrets).
func (b *Bridge) Info() map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := map[string]any{
		"session_source": "spawned",
	}
	if b.cfg.Quick.OutpostURL != "" {
		out["session_source"] = "attached"
	}
	if b.cfg.Quick.SessionFile != "" {
		out["session_source"] = "file"
	}
	if b.outpost != nil && b.outpost.URL != "" {
		out["outpost_url"] = b.outpost.URL
	}
	if b.sess != nil {
		out["ws_host"] = b.sess.WSHost
		out["provider"] = b.sess.Provider
		out["account"] = b.sess.AWSAccountID
		out["session_age_s"] = int(time.Since(b.sessAt).Seconds())
	}
	out["pooled_conversations"] = len(b.conns)
	if b.lastUpErr != nil {
		out["upstream_state"] = "degraded"
		out["upstream_error"] = b.lastUpErr.Error()
	} else {
		out["upstream_state"] = "ok"
	}
	return out
}
