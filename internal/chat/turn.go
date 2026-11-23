// turn.go is the state machine: dial → session_connect → session_create →
// user_message → collect until message_complete. The `turn` struct bundles
// the per-turn transport state (socket, inbound channel, event sink) so
// the prompt strategies in prompt.go don't carry a dozen callback params.
package chat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/kilbertert/quick2api/internal/session"
)

// handshakeGap is a measured settle delay between the handshake frames;
// the upstream occasionally drops the next frame when they arrive
// back-to-back (observed against the live probe).
const handshakeGap = 400 * time.Millisecond

// turn bundles the per-turn transport state: the socket, the inbound
// channel the pump feeds, and the caller's event sink. Prompt strategies
// (prompt.go) and the collector below all take *turn instead of passing
// the send/collect/inCh/emit closures around separately.
type turn struct {
	ws   *websocket.Conn
	in   chan inbound
	emit func(TurnEvent)
}

// dialer builds the configured WebSocket dialer (proxy resolution and
// buffer sizes live here so NewConversation and RunTurn share them).
func (c *Client) dialer() (*websocket.Dialer, error) {
	d := &websocket.Dialer{
		HandshakeTimeout: 15 * time.Second,
		ReadBufferSize:   256 * 1024,
		WriteBufferSize:  64 * 1024,
	}
	switch {
	case c.Proxy == "direct":
		// Explicit no-proxy, bypassing env.
	case c.Proxy != "":
		pu, err := url.Parse(c.Proxy)
		if err != nil {
			return nil, fmt.Errorf("quick.proxy: %w", err)
		}
		d.Proxy = http.ProxyURL(pu)
	default:
		d.Proxy = http.ProxyFromEnvironment
	}
	return d, nil
}

// dial opens the upstream WSS and maps handshake refusals to UpstreamError
// so the bridge can distinguish "expired id_token" from a real failure.
func (c *Client) dial(ctx context.Context, sess *session.Session, outpostID, workingSessionID string) (*websocket.Conn, error) {
	d, err := c.dialer()
	if err != nil {
		return nil, err
	}
	hdr := http.Header{}
	hdr.Set("Origin", c.Origin)
	ws, wresp, err := d.DialContext(ctx, c.wsURL(sess, outpostID, workingSessionID), hdr)
	if err != nil {
		// The dialer returns ErrBadHandshake with the HTTP response when the
		// server refuses the upgrade — surface the status so bridge can
		// refresh the session and retry (id_token is short-lived).
		if wresp != nil {
			body, _ := io.ReadAll(io.LimitReader(wresp.Body, 4096))
			wresp.Body.Close()
			slog.Warn("ws handshake refused", "status", wresp.StatusCode, "body", trunc(string(body), 300))
			code := "server"
			if wresp.StatusCode == 401 || wresp.StatusCode == 403 {
				code = "unauthenticated"
			} else if wresp.StatusCode == 429 {
				code = "rate_limited"
			}
			return nil, &UpstreamError{Code: code, Message: fmt.Sprintf("ws handshake %d: %s", wresp.StatusCode, trunc(string(body), 200))}
		}
		return nil, fmt.Errorf("upstream ws dial: %w", err)
	}
	return ws, nil
}

// handshake performs session_connect (+ federate_status) → session_established
// → session_create(session_id=conversationID) → session_created on an
// already-open turn. Reusing an existing conversationID makes the upstream
// continue that thread's server-side context instead of starting a fresh one.
func (c *Client) handshake(ctx context.Context, t *turn, sess *session.Session, outpostID, conversationID, title string) error {
	// 1. session_connect — renderer payload is
	// {actor_id, ...tf(), ...nf()}: tf() adds surfaceType+outpostId+
	// appVersion+clientInstanceId; nf() adds the capability flags the server
	// needs to consider the client functional. Missing flags made the server
	// close the stream right after session_established in testing.
	if err := t.send(map[string]any{
		"type": "session_connect",
		"payload": map[string]any{
			"actor_id":                    sess.ActorARN,
			"surfaceType":                 c.SurfaceType,
			"outpostId":                   outpostID,
			"appVersion":                  orElse(c.AppVersion, "1.2.4660"),
			"clientInstanceId":            newUUID(),
			"acp_first_text":              1,
			"user_notification":           1,
			"subtask_progress":            1,
			"external_agent_sign_in":      1,
			"recents_promotion":           1,
			"self_registering_connectors": 1,
		},
	}); err != nil {
		return fmt.Errorf("send session_connect: %w", err)
	}
	// federate_status — renderer sends it right after session_connect.
	if err := t.send(map[string]any{
		"type":    "federate_status",
		"payload": map[string]any{},
	}); err != nil {
		return fmt.Errorf("send federate_status: %w", err)
	}
	time.Sleep(handshakeGap)
	if err := t.waitFor(ctx, c.HandshakeTimeout, func(f *Frame) bool {
		return f.Type == "session_established" || f.Type == "session_connected"
	}); err != nil {
		if c.StrictHandshake {
			return err
		}
		slog.Warn("session_connect ack not received; proceeding", "error", err)
	}

	// 2. session_create. An existing session_id continues that thread;
	// a fresh uuid starts a new one (the upstream accepts both).
	time.Sleep(handshakeGap)
	if err := t.send(map[string]any{
		"type": "session_create",
		"payload": map[string]any{
			"session_id": conversationID,
			"title":      orElse(title, "quick2api"),
		},
	}); err != nil {
		return fmt.Errorf("send session_create: %w", err)
	}
	if err := t.waitFor(ctx, c.HandshakeTimeout, func(f *Frame) bool {
		return f.Type == "session_created"
	}); err != nil {
		if c.StrictHandshake {
			return err
		}
		slog.Warn("session_create ack not received; proceeding", "error", err)
	}
	time.Sleep(handshakeGap)
	return nil
}

// Conn is a persistent upstream conversation: one AIDP WebSocket that has
// completed the handshake and can serve many sequential user_message turns
// inside the same upstream thread — this is the session-reuse mechanism.
// One Conn serves one Send at a time; callers serialize via the bridge pool.
type Conn struct {
	c      *Client
	t      *turn
	convID string
	sess   *session.Session

	// dead flips when the read pump exits — after that the socket is gone
	// and the next Send would write into a closing channel.
	dead atomic.Bool

	// sentTail is the shadow transcript: the message-history tail this
	// conn's upstream thread already carries. It lives on the Conn because
	// its validity ends with the thread — a dropped conn reseeds on the
	// next Send. Serial per-conn Sends keep it race-free.
	sentTail string
}

// NewConversation dials and handshakes a persistent upstream conversation.
// conversationID may be empty (a fresh uuid is generated); reusing the id
// of a previous conversation continues its server-side thread.
func (c *Client) NewConversation(ctx context.Context, sess *session.Session, conversationID string) (*Conn, error) {
	if conversationID == "" {
		conversationID = newUUID()
	}
	outpostID := sess.OutpostID
	if outpostID == "" {
		outpostID = fmt.Sprintf("q2a-%d", time.Now().UnixNano()%0xffffff)
		slog.Warn("no registered outpost id; chat will likely close early", "outpostID", outpostID)
	}
	// The upstream conflates session_id == thread_id == conversationId:
	// session_create carries it as session_id, user_message as thread_id,
	// and the attachment-deliver path as files/upload/<id>.
	ws, err := c.dial(ctx, sess, outpostID, newUUID())
	if err != nil {
		return nil, err
	}
	t := &turn{
		ws:   ws,
		in:   make(chan inbound, 64),
		emit: func(TurnEvent) {},
	}
	cn := &Conn{c: c, t: t, convID: conversationID, sess: sess}
	go cn.pump(t)
	if err := c.handshake(ctx, t, sess, outpostID, conversationID, ""); err != nil {
		ws.Close()
		return nil, err
	}
	return cn, nil
}

// pump wraps turn.pump and marks the connection dead when the read loop
// exits for any reason (server close, network error).
func (cn *Conn) pump(t *turn) {
	t.pump()
	cn.dead.Store(true)
}

// Alive reports whether the socket's read pump is still running.
func (cn *Conn) Alive() bool { return !cn.dead.Load() }

// ConversationID returns the upstream session_id/thread_id of this conn.
func (cn *Conn) ConversationID() string { return cn.convID }

// Close shuts the underlying socket.
func (cn *Conn) Close() {
	cn.dead.Store(true)
	if cn.t != nil && cn.t.ws != nil {
		cn.t.ws.Close()
	}
}

// Send runs one user_message turn on this connection. The emit sink is
// rebound per call because each Send carries its own OnEvent callback.
func (cn *Conn) Send(ctx context.Context, sess *session.Session, opts TurnOpts) (*TurnResult, error) {
	if !cn.Alive() {
		return nil, errors.New("upstream connection closed")
	}
	cn.t.emit = func(ev TurnEvent) {
		if opts.OnEvent != nil {
			opts.OnEvent(ev)
		}
	}
	send, commit := deltaTail(cn.sentTail, opts.Text, opts.Tail)
	if send != opts.Text {
		slog.Info("delta turn", "send_bytes", len(send), "tail_bytes", len(opts.Tail))
	}
	opts.Text = send
	res, err := cn.c.sendPrompt(ctx, cn.t, cn.convID, opts, sess)
	if err == nil {
		cn.sentTail = commit
	}
	if err != nil && !cn.Alive() {
		// Connection died mid-turn — mark it so the pool drops it.
		cn.dead.Store(true)
	}
	return res, err
}

// deltaTail decides what a Send actually puts on the wire. sentTail is the
// watermark already appended to this thread; tail is the request's
// rendered history tail ("" = caller supplied no split info → send Text).
// When the new tail is an append-only continuation of the shadow, only the
// unsent suffix goes upstream. On a failed turn the watermark is not
// committed, so the next request re-derives the delta from the last good
// state (a bounded duplicate at worst).
func deltaTail(sentTail, text, tail string) (send, commit string) {
	if tail == "" {
		return text, sentTail
	}
	if sentTail != "" && strings.HasPrefix(tail, sentTail) {
		if delta := strings.TrimLeft(tail[len(sentTail):], "\n"); delta != "" {
			return delta, tail
		}
	}
	return text, tail
}

// RunTurn performs one complete turn on a fresh WebSocket connection and
// fresh session. Kept for callers that want a one-shot; the bridge pools
// Conns keyed by ConversationID for cross-request reuse.
func (c *Client) RunTurn(ctx context.Context, sess *session.Session, opts TurnOpts) (*TurnResult, error) {
	if opts.Text == "" {
		return nil, errors.New("empty prompt text")
	}
	cn, err := c.NewConversation(ctx, sess, opts.ConversationID)
	if err != nil {
		return nil, err
	}
	defer cn.Close()
	return cn.Send(ctx, sess, opts)
}

// waitFor consumes frames until want() returns true or the timeout fires.
func (t *turn) waitFor(ctx context.Context, d time.Duration, want func(*Frame) bool) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return fmt.Errorf("handshake timeout after %s", d)
		case in, ok := <-t.in:
			if !ok {
				return errors.New("ws closed during handshake")
			}
			if in.err != nil {
				return fmt.Errorf("ws closed during handshake: %w", in.err)
			}
			t.emit(TurnEvent{Kind: "frame", Frame: in.f})
			if in.f.Type == "error" {
				return fmt.Errorf("upstream error frame: %s", in.f.Payload)
			}
			if want(in.f) {
				return nil
			}
		}
	}
}

// collect reads inbound frames until message_complete, session end, or
// close; shared by the single-turn and chunked paths in prompt.go.
func (t *turn) collect(ctx context.Context, res *TurnResult) (*TurnResult, error) {
	var textSeen atomic.Bool
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case in, ok := <-t.in:
			if !ok || in.err != nil {
				if textSeen.Load() {
					res.PartialOnClose = true
					t.emit(TurnEvent{Kind: "done"})
					return res, nil
				}
				if !ok {
					return nil, errors.New("upstream ws closed before message_complete")
				}
				return nil, fmt.Errorf("upstream ws closed mid-turn: %w", in.err)
			}
			f := in.f
			t.emit(TurnEvent{Kind: "frame", Frame: f})
			switch f.Type {
			case "text_delta", "thinking_delta":
				var p struct {
					Delta string `json:"delta"`
				}
				_ = json.Unmarshal(f.Payload, &p)
				if p.Delta != "" {
					if f.Type == "thinking_delta" {
						t.emit(TurnEvent{Kind: "thinking", Text: p.Delta})
					} else {
						res.Text += p.Delta
						textSeen.Store(true)
						t.emit(TurnEvent{Kind: "delta", Text: p.Delta})
					}
				}
			case "turn_usage":
				var u TurnUsage
				if err := json.Unmarshal(f.Payload, &u); err == nil {
					if u.CacheReadTokens == 0 {
						u.CacheReadTokens = u.CacheRead
					}
					if u.CacheWriteTokens == 0 {
						u.CacheWriteTokens = u.CacheWrite
					}
					res.Usage = &u
					if u.ResolvedModelID != "" {
						res.ResolvedModel = u.ResolvedModelID
					}
					t.emit(TurnEvent{Kind: "usage", Usage: &u})
				}
			case "guardrail_blocked":
				res.FinishReason = "content_filter"
				t.emit(TurnEvent{Kind: "done"})
				return res, nil
			case "error":
				var p struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				}
				_ = json.Unmarshal(f.Payload, &p)
				msg := p.Message
				if msg == "" {
					msg = string(f.Payload)
				}
				return nil, &UpstreamError{Code: p.Code, Message: msg}
			case "message_complete":
				t.emit(TurnEvent{Kind: "done"})
				return res, nil
			case "session_end", "session_ended":
				if textSeen.Load() {
					res.PartialOnClose = true
					t.emit(TurnEvent{Kind: "done"})
					return res, nil
				}
				return nil, errors.New("upstream ended session before reply")
			}
		}
	}
}
