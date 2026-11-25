package chat

import (
	"context"
	"errors"
	"sync"

	"github.com/kilbertert/quick2api/internal/session"
)

type preparedContext struct {
	text, final string
	attachment  preparedAttachment
}

// NewNativeConversation overlaps raw context upload/policy approval with the
// fresh socket handshake. No conversation binding or inference happens here.
// Both preparations are joined before returning, including on cancellation.
func (c *Client) NewNativeConversation(ctx context.Context, sess *session.Session, opts TurnOpts) (*Conn, error) {
	return prepareNativeConversation(ctx, sess, opts, func(ctx context.Context, emit func(TurnEvent)) (*Conn, error) {
		return c.newConversation(ctx, sess, opts.ConversationID, emit)
	})
}

func prepareNativeConversation(ctx context.Context, sess *session.Session, opts TurnOpts, open func(context.Context, func(TurnEvent)) (*Conn, error)) (*Conn, error) {
	text := appendPromptSuffix(opts.Text, opts.Suffix)
	final := opts.Final
	if final != "" {
		final = appendPromptSuffix(final, opts.Suffix)
	}
	if opts.NativeRelay == "" || len(text) <= maxSingleText || sess == nil || sess.TenantURL == "" || sess.IDToken == "" {
		return open(ctx, opts.OnEvent)
	}
	setupCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// OnEvent remains serial even though independent setup now overlaps.
	var eventMu sync.Mutex
	emit := func(ev TurnEvent) {
		eventMu.Lock()
		defer eventMu.Unlock()
		if opts.OnEvent != nil {
			opts.OnEvent(ev)
		}
	}
	type uploadResult struct {
		attachment preparedAttachment
		err        error
	}
	ready := make(chan uploadResult, 1)
	go func() {
		data, name, _ := oversizedPrompt(text, final)
		emit(TurnEvent{Kind: "stage", Text: "context_prepare_started"})
		attachment, err := prepareAttachment(setupCtx, sess, name, "text/plain", data)
		emit(TurnEvent{Kind: "stage", Text: "context_prepare_finished"})
		if err != nil {
			cancel()
		}
		ready <- uploadResult{attachment, err}
	}()
	cn, connErr := open(setupCtx, emit)
	if connErr != nil {
		cancel()
	}
	upload := <-ready
	if connErr != nil || upload.err != nil || ctx.Err() != nil {
		if cn != nil {
			cn.Close()
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// Preserve the initiating failure rather than a sibling's cancellation.
		if upload.err != nil && !errors.Is(upload.err, context.Canceled) {
			return nil, upload.err
		}
		if connErr != nil {
			return nil, connErr
		}
		return nil, upload.err
	}
	cn.preparedContext = &preparedContext{text: text, final: final, attachment: upload.attachment}
	return cn, nil
}
