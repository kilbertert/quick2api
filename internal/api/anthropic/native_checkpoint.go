package anthropic

import (
	"crypto/sha256"
	"encoding/json"
	"time"

	"github.com/kilbertert/quick2api/internal/chat"
)

// Store fingerprints, not another full copy of a potentially 32 MiB history.
// The socket itself belongs to the bridge pool and is never reconnected to its
// previous upstream UUID when it dies or a checkpoint does not match.
type nativeCheckpoint struct {
	identity   [32]byte
	history    [32]byte
	messages   int
	sessionKey string
	until      time.Time
	discard    func()
}

func nativeHistoryDigest(messages []Message) ([32]byte, error) {
	// Hash the same canonical JSON array incrementally. A large history no
	// longer needs a second full JSON buffer and decoded object tree at once.
	if len(messages) == 0 {
		return sha256.Sum256([]byte("null")), nil
	}
	h := sha256.New()
	h.Write([]byte("["))
	for i, message := range messages {
		content, err := nativeContent(message.Content)
		if err != nil {
			return [32]byte{}, err
		}
		message.Content = content
		data, err := json.Marshal(message)
		if err != nil {
			return [32]byte{}, err
		}
		data, err = canonicalJSON(data)
		if err != nil {
			return [32]byte{}, err
		}
		if i > 0 {
			h.Write([]byte(","))
		}
		h.Write(data)
	}
	h.Write([]byte("]"))
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

func (c nativeCheckpoint) continuation(req *MessagesRequest, identity [32]byte) *chat.NativeContinuation {
	if !time.Now().Before(c.until) || c.identity != identity || len(req.Messages) <= c.messages {
		return nil
	}
	digest, err := nativeHistoryDigest(req.Messages[:c.messages])
	if err != nil || digest != c.history {
		return nil
	}
	// Completed histories may append a new user message plus environment
	// context, never an unobserved assistant turn or synthetic tool outcome.
	if req.Messages[c.messages].Role != "user" {
		return nil
	}
	for _, m := range req.Messages[c.messages+1:] {
		if m.Role != "system" {
			return nil
		}
	}
	deltaReq := &MessagesRequest{Messages: req.Messages[c.messages:]}
	text, _, _, atts, err := renderNativePrompt(deltaReq)
	if err != nil || text == "" {
		return nil
	}
	return &chat.NativeContinuation{Text: text, Attachments: atts}
}

// Called only after successful delivery of a clean final response. Failed
// writes, refusals, tools still awaiting a result and question parking cannot
// create a reusable checkpoint.
func (b *NativeBridge) remember(t *nativeTurn, content []ContentBlock) bool {
	if t.conversation == "" || t.unsentContext {
		return false
	}
	raw, err := json.Marshal(content)
	if err != nil {
		return false
	}
	messages := append(append([]Message(nil), t.messages...), Message{Role: "assistant", Content: raw})
	digest, err := nativeHistoryDigest(messages)
	if err != nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ctx.Err() != nil {
		return false
	}
	for id, c := range b.checkpoints {
		if !time.Now().Before(c.until) {
			delete(b.checkpoints, id)
			if c.discard != nil {
				c.discard()
			}
		}
	}
	if len(b.checkpoints) >= 32 {
		oldest := ""
		var until time.Time
		for id, c := range b.checkpoints {
			if oldest == "" || c.until.Before(until) {
				oldest, until = id, c.until
			}
		}
		if c := b.checkpoints[oldest]; c.discard != nil {
			c.discard()
		}
		delete(b.checkpoints, oldest)
	}
	t.socketMu.Lock()
	discard := t.discardSocket
	t.socketMu.Unlock()
	b.checkpoints[t.conversation] = nativeCheckpoint{identity: t.identity, history: digest, messages: len(messages), sessionKey: t.sessionKey, until: time.Now().Add(10 * time.Minute), discard: discard}
	return true
}
