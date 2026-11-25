package bridge

import (
	"context"
	"log/slog"
	"time"

	"github.com/kilbertert/quick2api/internal/chat"
	"github.com/kilbertert/quick2api/internal/session"
)

// sendNative never retries a Send. A checkpoint permits reuse only of its
// still-live owned socket; a cold socket seeds a fresh upstream UUID with the
// entire transcript before executing this explicitly submitted new turn.
func (b *Bridge) sendNative(ctx context.Context, sess *session.Session, opts chat.TurnOpts) (*chat.TurnResult, error) {
	if opts.ConversationID == "" {
		return b.ChatClient().RunTurn(ctx, sess, opts)
	}
	started := time.Now()
	b.mu.Lock()
	e := b.conns[opts.ConversationID]
	reused := e != nil && e.native && opts.NativeContinuation != nil && e.conn.Alive() && e.conn.MatchesSession(sess)
	if e != nil && !reused {
		e.conn.Close()
		delete(b.conns, opts.ConversationID)
		e = nil
	}
	if e != nil {
		e.busy = true
	}
	b.mu.Unlock()
	if e == nil {
		// The public conversation key is never an upstream UUID on a cold turn.
		initial := opts
		initial.ConversationID = ""
		initial.Tail = ""
		cn, err := b.ChatClient().NewNativeConversation(ctx, sess, initial)
		if err != nil {
			return nil, err
		}
		e = &connEntry{conn: cn, lastAt: time.Now(), busy: true, native: true}
		b.mu.Lock()
		b.conns[opts.ConversationID] = e
		b.mu.Unlock()
	}
	fullBytes := len(opts.Text)
	if opts.OnNativeSession != nil {
		opts.OnNativeSession(func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			if b.conns[opts.ConversationID] == e {
				e.conn.Close()
				delete(b.conns, opts.ConversationID)
			}
		})
	}
	if reused {
		opts.Text = opts.NativeContinuation.Text
		opts.Attachments = opts.NativeContinuation.Attachments
	}
	// Native checkpoints include delivered assistant messages and actual tool
	// results. The compatibility renderer's shadow-tail heuristic is not used.
	opts.Tail = ""
	slog.Info("native transport prepared", "reused", reused, "full_bytes", fullBytes, "send_bytes", len(opts.Text), "prepare_ms", time.Since(started).Milliseconds())
	res, err := e.conn.Send(ctx, sess, opts)
	b.mu.Lock()
	e.busy = false
	e.lastAt = time.Now()
	if err != nil || res == nil || res.PartialOnClose || res.FinishReason != "stop" {
		e.conn.Close()
		delete(b.conns, opts.ConversationID)
	}
	b.mu.Unlock()
	return res, err
}
