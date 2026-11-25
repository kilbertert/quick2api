// types.go collects the shared value types of the chat package: the wire
// frame, the turn result/event/option shapes, the upstream error, and a few
// small helpers (UUID, string truncation) used across conn/turn/prompt.
package chat

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// Frame is one decoded inner protocol frame.
type Frame struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`

	// Envelope fields that appear on some inbound frames.
	MessageID string `json:"message_id,omitempty"`
	ThreadID  string `json:"thread_id,omitempty"`
}

// TurnUsage mirrors the turn_usage frame payload (defensive: all optional).
type TurnUsage struct {
	InputTokens      int64   `json:"input_tokens"`
	OutputTokens     int64   `json:"output_tokens"`
	CacheReadTokens  int64   `json:"cache_read_input_tokens"`
	CacheWriteTokens int64   `json:"cache_write_input_tokens"`
	Model            string  `json:"model"`
	ResolvedModelID  string  `json:"resolved_model_id"`
	CostUSD          float64 `json:"cost_usd"`
	ContextWindow    int64   `json:"context_window"`
	// Fallback spellings seen on some builds.
	CacheRead  int64 `json:"cache_read"`
	CacheWrite int64 `json:"cache_write"`
}

// TurnResult is the completed turn.
type TurnResult struct {
	Text string
	// ToolTextOffset marks the latest native tool boundary in Text. Quick
	// may narrate an attachment read before using its own tools, then emit
	// client tool JSON afterwards. Keep all text for display, but only the
	// final uninterrupted text span is eligible for client-tool parsing.
	ToolTextOffset int
	FinishReason   string // "stop" | "content_filter" | "error"
	Usage          *TurnUsage
	ResolvedModel  string
	PartialOnClose bool
}

func (r *TurnResult) ToolText() string {
	if r.ToolTextOffset >= 0 && r.ToolTextOffset <= len(r.Text) {
		return r.Text[r.ToolTextOffset:]
	}
	return r.Text
}

// TurnEvent is a streaming callback event.
type TurnEvent struct {
	Kind  string // "delta" | "thinking" | "usage" | "frame" | "done" | "error"
	Text  string
	Usage *TurnUsage
	Frame *Frame // raw frame when Kind=="frame"
	Err   error
}

// TurnOpts controls one Send / RunTurn call.
type TurnOpts struct {
	Text string
	// ConversationID pins this turn to an upstream thread (the upstream
	// conflates session_id==thread_id==conversationId). Empty starts a new
	// one-shot conversation; a reused id continues that thread server-side.
	ConversationID string
	AgentMode      string
	ThinkingEffort string
	UserTimezone   string
	UserLocale     string
	Title          string
	// Attachments are out-of-band references for multimodal input
	// (image/file blocks). Each entry is {name,mime,type,data}; the send
	// path uploads them via uploadAttachment and wires the returned
	// server path into payload.attachments.
	Attachments []Attachment
	// Tail is the rendered message-history portion of Text (Text's suffix
	// after the request preamble; "" disables delta turns). A pooled Conn
	// keeps the tail it last sent as a shadow transcript — when this
	// request's Tail continues that shadow, only the unsent suffix goes
	// upstream: the stateless-API full transcript becomes a delta-append
	// on the stateful thread. The bridge reseeds divergent transcripts on a
	// fresh upstream thread rather than replaying them into the old one.
	Tail string
	// Final is the rendered operative message and any trailing environment
	// context ("User: ...", "ToolResult: ..."). The oversized-prompt
	// path keeps it inline in the user_message so the model answers a real
	// turn instead of analyzing an uploaded transcript document.
	Final string
	// Suffix is appended to the actual text sent for this turn, after delta
	// selection, but is excluded from Tail's append-only watermark. This is
	// for short per-turn protocol reminders that must stay near the operative
	// message without forcing a full transcript resend on the next request.
	Suffix string
	// OnEvent is invoked synchronously from the read loop; implementations
	// should be fast or hand off to a channel.
	OnEvent func(TurnEvent)
	// NativeRelay enables only the explicitly registered MCP relay on the
	// proxy-owned outpost. It does not enable arbitrary local device execution.
	// This local registration/courier name is not the model's callable name;
	// Quick may expose the same relay with a device-qualified native identifier.
	NativeRelay string
	// NativeContinuation is supplied only after the native adapter validates a
	// delivered, completed transcript checkpoint. The full Text remains available
	// for cold seeding if its owned socket has expired before this new turn.
	NativeContinuation *NativeContinuation
	// OnNativeSession hands the adapter an idempotent discard callback for a
	// completed socket whose final response could not be delivered downstream.
	OnNativeSession func(discard func())
	preparedContext *preparedContext
}

type NativeContinuation struct {
	Text        string
	Attachments []Attachment
}

// Attachment is one multimodal input part to ship out-of-band.
type Attachment struct {
	Name     string // filename shown to the model
	MimeType string // e.g. "image/png"
	// TranscriptOffset is the one-based byte offset of this attachment's
	// reference in the full rendered Text. Zero means no transcript binding.
	// A pooled delta need not upload attachments whose references were sent
	// successfully on an earlier turn; a new connection still uploads all.
	TranscriptOffset int
	// Data is the raw bytes. Upload happens in sendPrompt — keep this under
	// the ~20MB practical ceiling the attachment service accepts.
	Data []byte
}

// Client wires the WebSocket connection.
type Client struct {
	SurfaceType      string
	Channel          string
	Origin           string
	AppVersion       string // quickwork desktop version, sent in session_connect
	HandshakeTimeout time.Duration
	StrictHandshake  bool
	Proxy            string // "", or "direct", or a proxy URL
}

// UpstreamError is an explicit error frame from the upstream.
type UpstreamError struct {
	Code    string
	Message string
}

func (e *UpstreamError) Error() string {
	msg := e.Message
	if msg == "" {
		// The upstream occasionally sends {"type":"error","payload":null};
		// keep a marker so the caller doesn't log a bare "upstream: ".
		msg = "(empty upstream error frame)"
	}
	if e.Code != "" {
		return "upstream " + e.Code + ": " + msg
	}
	return "upstream: " + msg
}

// inbound is one read result off the websocket.
type inbound struct {
	f   *Frame
	err error
}

// splitText cuts s into pieces of at most n bytes on rune boundaries.
func splitText(s string, n int) []string {
	if len(s) <= n {
		return []string{s}
	}
	var out []string
	for len(s) > 0 {
		cut := n
		if cut > len(s) {
			cut = len(s)
		}
		for cut > 0 && !utf8.ValidString(s[:cut]) {
			cut--
		}
		if cut == 0 {
			cut = n
			if cut > len(s) {
				cut = len(s)
			}
		}
		out = append(out, s[:cut])
		s = s[cut:]
	}
	return out
}

func orElse(v, def string) string {
	if v != "" {
		return v
	}
	return def
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

var uuidCounter uint64

// newUUID returns a random RFC-4122 v4 UUID without pulling a dependency.
func newUUID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err == nil {
		b[6] = (b[6] & 0x0f) | 0x40
		b[8] = (b[8] & 0x3f) | 0x80
		return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
			b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
	}
	// Extremely unlikely fallback: counter-based pseudo-uuid.
	n := atomic.AddUint64(&uuidCounter, 1)
	t := uint64(time.Now().UnixNano())
	return fmt.Sprintf("%08x-%04x-4%03x-8%03x-%012x",
		uint32(t>>32), uint16(t>>16), uint16(t)&0xfff, uint16(n)&0xfff, n&0xffffffffffff)
}
