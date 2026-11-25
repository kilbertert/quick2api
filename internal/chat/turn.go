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
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/kilbertert/quick2api/internal/session"
)

// handshakeGap is a measured settle delay between the handshake frames;
// the upstream occasionally drops the next frame when they arrive
// back-to-back (observed against the live probe).
const handshakeGap = 400 * time.Millisecond

// Only a missing acknowledgement may be tolerated by a legacy handshake.
// Closed sockets, upstream errors and cancellation must never advance it.
var errHandshakeTimeout = errors.New("handshake acknowledgement timeout")

// turn bundles the per-turn transport state: the socket, the inbound
// channel the pump feeds, and the caller's event sink. Prompt strategies
// (prompt.go) and the collector below all take *turn instead of passing
// the send/collect/inCh/emit closures around separately.
type turn struct {
	ws      *websocket.Conn
	in      chan inbound
	emit    func(TurnEvent)
	done    chan struct{}
	once    sync.Once
	writeMu sync.Mutex
	native  *nativeRuntime
}

func (t *turn) close() {
	t.once.Do(func() {
		if t.done != nil {
			close(t.done)
		}
		if t.ws != nil {
			t.ws.Close()
		}
	})
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
			_, _ = io.Copy(io.Discard, io.LimitReader(wresp.Body, 4096))
			wresp.Body.Close()
			slog.Warn("ws handshake refused", "status", wresp.StatusCode)
			code := "server"
			if wresp.StatusCode == 401 || wresp.StatusCode == 403 {
				code = "unauthenticated"
			} else if wresp.StatusCode == 429 {
				code = "rate_limited"
			}
			return nil, &UpstreamError{Code: code, Message: fmt.Sprintf("ws handshake refused (HTTP %d)", wresp.StatusCode)}
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
		return &ConnectionSetupError{Stage: "session_connect", Err: err}
	}
	// federate_status — renderer sends it right after session_connect.
	if err := t.send(map[string]any{
		"type":    "federate_status",
		"payload": map[string]any{},
	}); err != nil {
		return &ConnectionSetupError{Stage: "session_connect", Err: err}
	}
	if err := waitContext(ctx, handshakeGap); err != nil {
		return &ConnectionSetupError{Stage: "session_connect", Err: err}
	}
	if err := c.waitForAck(ctx, t, "session_connected", func(f *Frame) bool {
		return f.Type == "session_established" || f.Type == "session_connected"
	}); err != nil {
		return &ConnectionSetupError{Stage: "session_connect", Err: err}
	}

	// 2. session_create. An existing session_id continues that thread;
	// a fresh uuid starts a new one (the upstream accepts both).
	if err := waitContext(ctx, handshakeGap); err != nil {
		return &ConnectionSetupError{Stage: "session_create", Err: err}
	}
	if err := t.send(map[string]any{
		"type": "session_create",
		"payload": map[string]any{
			"session_id": conversationID,
			"title":      orElse(title, "quick2api"),
		},
	}); err != nil {
		return &ConnectionSetupError{Stage: "session_create", Err: err}
	}
	if err := c.waitForAck(ctx, t, "session_created", func(f *Frame) bool {
		return f.Type == "session_created"
	}); err != nil {
		return &ConnectionSetupError{Stage: "session_create", Err: err}
	}
	if err := waitContext(ctx, handshakeGap); err != nil {
		return &ConnectionSetupError{Stage: "session_create", Err: err}
	}
	return nil
}

func (c *Client) waitForAck(ctx context.Context, t *turn, stage string, want func(*Frame) bool) error {
	err := t.waitFor(ctx, c.HandshakeTimeout, want)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		if c.StrictHandshake || !errors.Is(err, errHandshakeTimeout) {
			return err
		}
		slog.Warn("handshake ack timed out; proceeding without confirmation", "stage", stage, "error", err)
		stage += "_unconfirmed"
	}
	t.emit(TurnEvent{Kind: "stage", Text: stage})
	return nil
}

func waitContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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
	sentTail        string
	sentHead        string
	preparedContext *preparedContext
}

// NewConversation dials and handshakes a persistent upstream conversation.
// conversationID may be empty (a fresh uuid is generated); reusing the id
// of a previous conversation continues its server-side thread.
func (c *Client) NewConversation(ctx context.Context, sess *session.Session, conversationID string) (*Conn, error) {
	return c.newConversation(ctx, sess, conversationID, nil)
}

func (c *Client) newConversation(ctx context.Context, sess *session.Session, conversationID string, emit func(TurnEvent)) (*Conn, error) {
	if emit == nil {
		emit = func(TurnEvent) {}
	}
	if sess == nil {
		return nil, errors.New("no session bound")
	}
	if conversationID == "" {
		conversationID = newUUID()
	}
	// Do not overlap device setup across independent Client objects. Reused
	// sockets never enter this path, and the gate ends before any prompt send.
	emit(TurnEvent{Kind: "stage", Text: "connection_queued"})
	release, err := acquireHandshake(ctx, sess)
	if err != nil {
		return nil, &ConnectionSetupError{Stage: "connection_queue", Err: err}
	}
	defer release()
	emit(TurnEvent{Kind: "stage", Text: "connection_acquired"})
	outpostID := sess.OutpostID
	if outpostID == "" {
		outpostID = fmt.Sprintf("q2a-%d", time.Now().UnixNano()%0xffffff)
		slog.Warn("no registered outpost id; chat will likely close early", "outpostID", outpostID)
	}
	// The upstream conflates session_id == thread_id == conversationId:
	// session_create carries it as session_id, user_message as thread_id,
	// and the attachment-deliver path as files/upload/<id>.
	emit(TurnEvent{Kind: "stage", Text: "connection_started"})
	ws, err := c.dial(ctx, sess, outpostID, newUUID())
	if err != nil {
		return nil, &ConnectionSetupError{Stage: "websocket_dial", Err: err}
	}
	emit(TurnEvent{Kind: "stage", Text: "websocket_ready"})
	t := &turn{
		ws:   ws,
		in:   make(chan inbound, 64),
		emit: emit,
		done: make(chan struct{}),
	}
	cn := &Conn{c: c, t: t, convID: conversationID, sess: sess}
	go cn.pump(t)
	if err := c.handshake(ctx, t, sess, outpostID, conversationID, ""); err != nil {
		t.close()
		return nil, err
	}
	emit(TurnEvent{Kind: "stage", Text: "connection_ready"})
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

// MatchesSession permits token refresh, but never moves an existing native
// thread to another account, device, tenant or transport endpoint.
func (cn *Conn) MatchesSession(s *session.Session) bool {
	return cn.sess != nil && s != nil && cn.sess.ActorARN == s.ActorARN &&
		cn.sess.AWSAccountID == s.AWSAccountID && cn.sess.WSHost == s.WSHost &&
		cn.sess.OutpostID == s.OutpostID && cn.sess.TenantURL == s.TenantURL
}

// Close shuts the underlying socket.
func (cn *Conn) Close() {
	cn.dead.Store(true)
	if cn.t != nil {
		cn.t.close()
	}
}

// CanContinue is called under the bridge's conversation lock. A stateless
// transcript that diverges (fork, compaction, model probe, or suggestion) must
// be seeded into a fresh upstream thread, rather than appended to this one.
func (cn *Conn) CanContinue(opts TurnOpts) bool {
	if !cn.Alive() {
		return false
	}
	if opts.Tail == "" || cn.sentTail == "" {
		return true
	}
	return transcriptHead(opts) == cn.sentHead && len(opts.Tail) > len(cn.sentTail) &&
		strings.HasPrefix(opts.Tail, cn.sentTail)
}

func transcriptHead(opts TurnOpts) string {
	if opts.Tail != "" && strings.HasSuffix(opts.Text, opts.Tail) {
		return opts.Text[:len(opts.Text)-len(opts.Tail)]
	}
	return ""
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
	if opts.NativeRelay != "" {
		if !cn.MatchesSession(sess) {
			cn.Close()
			return nil, errors.New("native connection session identity changed")
		}
	}
	head := transcriptHead(opts)
	send, commit := deltaTail(cn.sentTail, opts.Text, opts.Tail)
	if send != opts.Text {
		allAttachments := len(opts.Attachments)
		opts.Attachments = deltaAttachments(opts.Text, send, opts.Attachments)
		slog.Info("delta turn", "send_bytes", len(send), "tail_bytes", len(opts.Tail),
			"attachments", allAttachments, "send_attachments", len(opts.Attachments))
	}
	opts.Text = send
	opts.preparedContext = cn.preparedContext
	cn.preparedContext = nil
	res, err := cn.c.sendPrompt(ctx, cn.t, cn.convID, opts, sess)
	if err == nil && res != nil && res.FinishReason == "stop" && !res.PartialOnClose {
		cn.sentTail = commit
		if opts.Tail != "" {
			cn.sentHead = head
		}
	} else {
		// Cancellation, errors, and guardrail termination do not establish a
		// clean message_complete boundary. Never let queued frames reach the
		// next HTTP request, and do not advance the history watermark.
		cn.Close()
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

func deltaAttachments(text, send string, atts []Attachment) []Attachment {
	if text == send || !strings.HasSuffix(text, send) {
		return atts
	}
	start := len(text) - len(send)
	var pending []Attachment
	for _, att := range atts {
		if att.TranscriptOffset == 0 || att.TranscriptOffset > start {
			pending = append(pending, att)
		}
	}
	return pending
}

// RunTurn performs one complete turn on a fresh WebSocket connection and
// fresh session. Kept for callers that want a one-shot; the bridge pools
// Conns keyed by ConversationID for cross-request reuse.
func (c *Client) RunTurn(ctx context.Context, sess *session.Session, opts TurnOpts) (*TurnResult, error) {
	if opts.Text == "" {
		return nil, errors.New("empty prompt text")
	}
	var cn *Conn
	var err error
	if opts.NativeRelay != "" {
		cn, err = c.NewNativeConversation(ctx, sess, opts)
	} else {
		cn, err = c.NewConversation(ctx, sess, opts.ConversationID)
	}
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
	consume := func(in inbound, ok bool) (bool, error) {
		if !ok {
			return false, errors.New("ws closed during handshake")
		}
		if in.err != nil {
			return false, fmt.Errorf("ws closed during handshake: %w", in.err)
		}
		t.emit(TurnEvent{Kind: "frame", Frame: in.f})
		if in.f.Type == "error" {
			var failure struct {
				Code      string `json:"code"`
				ErrorCode string `json:"error_code"`
			}
			_ = json.Unmarshal(in.f.Payload, &failure)
			code := orElse(failure.Code, failure.ErrorCode)
			switch code {
			case "unauthenticated", "token_expired", "rate_limited", "throttled", "content_filtered", "server":
			default:
				// Some adapters also expose Code. Never pass arbitrary frame
				// content through that field under the guise of a safe message.
				code = "server"
			}
			return false, &UpstreamError{Code: code, Message: "upstream rejected connection setup"}
		}
		return want(in.f), nil
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			// A fatal frame/close already queued at the deadline outranks a
			// missing ack. Bound the drain so unrelated traffic cannot extend
			// the handshake indefinitely. The extra read observes a closed
			// channel after its buffered frames have been consumed.
			for pending := len(t.in) + 1; pending > 0; pending-- {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				select {
				case in, ok := <-t.in:
					if matched, err := consume(in, ok); err != nil || matched {
						return err
					}
				default:
					return fmt.Errorf("%w after %s", errHandshakeTimeout, d)
				}
			}
			return fmt.Errorf("%w after %s", errHandshakeTimeout, d)
		case in, ok := <-t.in:
			if matched, err := consume(in, ok); err != nil || matched {
				return err
			}
		}
	}
}

// collect reads inbound frames until message_complete, session end, or
// close; shared by the single-turn and chunked paths in prompt.go.
func (t *turn) collect(ctx context.Context, res *TurnResult) (*TurnResult, error) {
	var textSeen atomic.Bool
	nativeCtx, stopNative := context.WithCancel(ctx)
	var nativeCalls sync.WaitGroup
	defer func() { stopNative(); nativeCalls.Wait() }()
	var nativeDone <-chan nativeReply
	var replies chan nativeReply
	seenNative := make(map[string]bool)
	activeNative := 0
	if t.native != nil {
		replies = make(chan nativeReply, 32)
		nativeDone = replies
	}
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case reply := <-nativeDone:
			activeNative--
			if err := t.send(map[string]any{"type": "tool_result", "payload": map[string]any{
				"tool_call_id": reply.id, "result": reply.result, "error": reply.err,
			}}); err != nil {
				return nil, &UpstreamError{Code: "native_relay_disconnected", Message: "native tool result delivery failed; execution state is uncertain"}
			}
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
			case "tool_call":
				if t.native == nil {
					continue
				}
				var call NativeCall
				if json.Unmarshal(f.Payload, &call) != nil || call.ID == "" || call.Name == "" || !json.Valid(call.Arguments) {
					return nil, &UpstreamError{Code: "invalid_native_call", Message: "malformed native tool call"}
				}
				if seenNative[call.ID] || len(seenNative) >= 512 || activeNative >= 32 {
					return nil, &UpstreamError{Code: "invalid_native_call", Message: "duplicate native call id or native call limit exceeded"}
				}
				seenNative[call.ID] = true
				activeNative++
				res.ToolTextOffset = len(res.Text)
				nativeCalls.Add(1)
				go func() {
					defer nativeCalls.Done()
					reply := t.native.invoke(nativeCtx, call)
					select {
					case replies <- reply:
					case <-nativeCtx.Done():
					}
				}()
			case "tool_start", "tool_result", "tool_error":
				// Native Quick tools (such as reading context.txt) separate
				// progress narration from the subsequent client-tool reply.
				// Do not search arbitrary prose for executable JSON.
				res.ToolTextOffset = len(res.Text)
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
					Code      string `json:"code"`
					Message   string `json:"message"`
					ErrorCode string `json:"error_code"`
					Error     string `json:"error"`
				}
				_ = json.Unmarshal(f.Payload, &p)
				if p.Code == "" {
					p.Code = p.ErrorCode
				}
				if p.Message == "" {
					p.Message = p.Error
				}
				// Observed on a real Desktop fork: the content filter also
				// uses error_code/error rather than guardrail_blocked. This
				// is a refusal, not a broken/unavailable model or a retry.
				if p.Code == "content_filtered" {
					res.FinishReason = "content_filter"
					if res.Text == "" {
						res.Text = orElse(p.Message, "Quick's content check blocked this response.")
						t.emit(TurnEvent{Kind: "delta", Text: res.Text})
					}
					t.emit(TurnEvent{Kind: "done"})
					return res, nil
				}
				msg := p.Message
				if msg == "" {
					msg = string(f.Payload)
				}
				return nil, &UpstreamError{Code: p.Code, Message: msg}
			case "message_complete":
				if activeNative != 0 {
					return nil, &UpstreamError{Code: "incomplete_native_turn", Message: "upstream completed while native tool calls were pending"}
				}
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
