// Package anthropic adapts Anthropic /v1/messages onto one Quick turn.
//
// Why it exists: Claude Code (and the wider Anthropic-SDK ecosystem)
// speaks /v1/messages + SSE, not /v1/chat/completions. Serving it here
// lets a `claude` CLI point ANTHROPIC_BASE_URL at quick2api and run on
// the Quick subscription — no second proxy hop.
//
// Scope: the text surface only. Client-declared tools are flattened
// into a leading instruction block (the upstream agent sees their
// schemas as text); tool_use content blocks in the request are
// rendered as "ToolUse <name>(<json>)" so history stays coherent.
// Stop sequences and max_tokens are passed positionally but the
// upstream turn doesn't honor them server-side — they're metadata.
package anthropic

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/kilbertert/quick2api/internal/api/common"
	"github.com/kilbertert/quick2api/internal/bridge"
	"github.com/kilbertert/quick2api/internal/chat"
	"github.com/kilbertert/quick2api/internal/config"
	"github.com/kilbertert/quick2api/internal/debuglog"
	"github.com/kilbertert/quick2api/internal/tools"
	"github.com/kilbertert/quick2api/internal/util"
)

// Handler serves Anthropic-compatible endpoints.
type Handler struct {
	Bridge *bridge.Pool
	Cfg    *config.Config
	Debug  *debuglog.Logger
	Sem    chan struct{}
	// Tools is the server-side executor registry; nil = advisory mode
	// (schemas go in the prompt, tool_use blocks come back as-is).
	// *Registry over Executor so execFor can ask Has() for auto-mode
	// passthrough of client-declared tools.
	Tools *tools.Registry
}

// ---------- request/response types (subset of the Messages schema) ----------

type ContentBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	Content   json.RawMessage `json:"content,omitempty"`
	IsError   bool            `json:"is_error,omitempty"`
	Source    json.RawMessage `json:"source,omitempty"` // image/document source
}

type Message struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
	// ID carries the client-supplied thread/conversation id when the
	// client puts one there (Claude Code uses it for session pinning).
	ID       string          `json:"id,omitempty"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

type MessagesRequest struct {
	Model         string          `json:"model"`
	Messages      []Message       `json:"messages"`
	System        json.RawMessage `json:"system,omitempty"`
	MaxTokens     int             `json:"max_tokens"`
	Temperature   *float64        `json:"temperature,omitempty"`
	Stream        bool            `json:"stream"`
	StopSequences []string        `json:"stop_sequences,omitempty"`
	Tools         json.RawMessage `json:"tools,omitempty"`
	ToolChoice    json.RawMessage `json:"tool_choice,omitempty"`
	Metadata      json.RawMessage `json:"metadata,omitempty"`
	Thinking      json.RawMessage `json:"thinking,omitempty"`
}

type anthropicUsage struct {
	InputTokens              int64 `json:"input_tokens"`
	OutputTokens             int64 `json:"output_tokens"`
	CacheReadInputTokens     int64 `json:"cache_read_input_tokens,omitempty"`
	CacheCreationInputTokens int64 `json:"cache_creation_input_tokens,omitempty"`
}

type anthropicMessage struct {
	ID           string         `json:"id"`
	Type         string         `json:"type"`
	Role         string         `json:"role"`
	Model        string         `json:"model"`
	Content      []ContentBlock `json:"content"`
	StopReason   string         `json:"stop_reason"`
	StopSequence string         `json:"stop_sequence"`
	Usage        anthropicUsage `json:"usage"`
}

// CountTokens handles POST /v1/messages/count_tokens.
//
// The upstream has no tokenize endpoint, so this returns a heuristic
// estimate (rendered prompt bytes / 4) and never touches the bridge —
// count_tokens must not spend a billable turn.
func (h *Handler) CountTokens(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "invalid_request_error", "use POST")
		return
	}
	var req MessagesRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_request_error", "bad JSON: "+err.Error())
		return
	}
	text, _, _, _, err := renderPrompt(&req)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"input_tokens": len(text) / 4})
}

// ---------- endpoint ----------

// Messages handles POST /v1/messages.
func (h *Handler) Messages(w http.ResponseWriter, r *http.Request) {
	reqID := common.ReqID(r)
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "invalid_request_error", "use POST")
		return
	}
	var req MessagesRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_request_error", "bad JSON: "+err.Error())
		return
	}
	if len(req.Messages) == 0 {
		writeErr(w, http.StatusBadRequest, "invalid_request_error", "messages is empty")
		return
	}

	text, tailAt, lastMsgAt, atts, err := renderPrompt(&req)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	var tail, final string
	if tailAt < len(text) {
		tail = text[tailAt:]
	}
	if lastMsgAt < len(text) {
		final = text[lastMsgAt:]
	}

	release, ok := common.Acquire(r.Context(), h.Sem)
	if !ok {
		if r.Context().Err() == nil {
			writeErr(w, http.StatusTooManyRequests, "rate_limit_error", "local concurrency limit reached")
		}
		return
	}
	defer release()

	model := req.Model
	if model == "" {
		model = common.FirstModel(h.Cfg)
	}
	agentMode := h.Cfg.AgentModeFor(model)
	thinking := h.Cfg.Quick.ThinkingEffort
	if len(req.Thinking) > 0 && string(req.Thinking) != "null" {
		var t struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(req.Thinking, &t) == nil && t.Type == "enabled" {
			thinking = "high"
		}
	}

	// Thread continuity: honor the client's conversation id when supplied
	// (Claude Code sends thread/session ids through X-Session-Id or the
	// first message's client-supplied id). Falls back to a fresh conv each
	// request — the bridge drops stale conns automatically.
	conversationID := conversationIDOf(r, &req)

	dlogDir := h.Debug.Dir(reqID)
	h.Debug.Write(dlogDir, "00-request", map[string]any{
		"id": reqID, "model": model, "agent_mode": agentMode,
		"stream": req.Stream, "messages": len(req.Messages),
		"conversation": conversationID,
	})

	ctx, cancel := context.WithTimeout(r.Context(), h.Cfg.TurnTimeout())
	defer cancel()

	clientTools := clientHasTools(req.Tools)
	exec := h.execFor(clientTools)
	if req.Stream {
		h.stream(w, ctx, reqID, model, agentMode, thinking, text, tail, final, atts, conversationID, exec, clientTools, dlogDir)
		return
	}
	h.complete(w, ctx, reqID, model, agentMode, thinking, text, tail, final, atts, conversationID, exec, dlogDir)
}

// clientHasTools reports whether the client declared its own tool
// schemas — those belong to the client's runtime in auto mode.
func clientHasTools(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) != "null" && string(raw) != "[]"
}

// conversationIDOf resolves the client-supplied conversation id. Sources,
// in order: the `X-Conversation-Id`/`X-Session-Id` header, then the
// first message's top-level `id` (Anthropic clients put a thread/convo
// id there), then metadata.user_id. Returns "" when none is usable —
// the bridge then starts a fresh upstream session per request.
func conversationIDOf(r *http.Request, req *MessagesRequest) string {
	if id := strings.TrimSpace(r.Header.Get("X-Conversation-Id")); id != "" {
		return id
	}
	if id := strings.TrimSpace(r.Header.Get("X-Session-Id")); id != "" {
		return id
	}
	for _, m := range req.Messages {
		if id := strings.TrimSpace(m.ID); id != "" {
			return id
		}
	}
	if len(req.Metadata) > 0 {
		var meta struct {
			UserID string `json:"user_id"`
		}
		if json.Unmarshal(req.Metadata, &meta) == nil && meta.UserID != "" {
			return meta.UserID
		}
	}
	return ""
}

// complete handles the non-streaming path.
func (h *Handler) complete(w http.ResponseWriter, ctx context.Context,
	reqID, model, agentMode, thinking, text, tail, final string,
	atts []chat.Attachment, conversationID string, exec bool, dlogDir string) {

	frames := common.NewFrames(h.Debug.LogFrames)
	res, err := h.runAgent(ctx, reqID, agentMode, thinking, text, tail, final, atts,
		conversationID, exec, frames, nil, nil)
	// Always write frames before returning — the error path is exactly
	// where they are needed to see the upstream's real error payload.
	frames.Write(h.Debug, dlogDir)
	if err != nil {
		h.finishDebug(dlogDir, nil, err)
		writeUpstream(w, err)
		return
	}

	content := parseToolCalls(res.Text)
	out := anthropicMessage{
		ID:      "msg_" + reqID,
		Type:    "message",
		Role:    "assistant",
		Model:   util.Or(res.ResolvedModel, model),
		Content: content,
		Usage:   mapUsage(res.Usage),
	}
	if len(content) == 1 && content[0].Type == "text" {
		out.StopReason = "end_turn"
	} else {
		out.StopReason = "tool_use"
	}
	h.finishDebug(dlogDir, &out, nil)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// execFor reports whether the server-side executor may intercept
// tool_use replies for this request:
//   - "passthrough" never intercepts — every tool_use goes back to the
//     client as a proper tool_use block;
//   - "server" always intercepts (executor wins even when the client
//     declared its own tools — client gets the final text only);
//   - "auto" (default) intercepts only when the client declared no
//     tools — client-declared tools belong to the client's own runtime.
func (h *Handler) execFor(clientTools bool) bool {
	if h.Tools == nil {
		return false
	}
	switch h.Cfg.Tools.Mode {
	case "server":
		return true
	case "passthrough":
		return false
	default: // "auto" / empty
		return !clientTools
	}
}

// runAgent performs the tool-use loop: sends the prompt, executes any
// tool_use blocks the model emits via h.Tools (only when exec is set —
// see execFor), and feeds the results back until the model stops calling
// tools or MaxToolRounds hits. Returns the final TurnResult — the last
// upstream turn's text — so the caller maps it to Anthropic blocks
// exactly like a single-shot call.
//
// onToolRound, when set, is invoked after each round that parsed as
// tool calls — streaming callers use it to emit progress events instead
// of the raw tool JSON.
func (h *Handler) runAgent(ctx context.Context,
	reqID, agentMode, thinking, text, tail, final string,
	atts []chat.Attachment, conversationID string, exec bool,
	frames *common.Frames, onDelta func(string),
	onToolRound func(round int, calls []common.ToolCallJSON)) (*chat.TurnResult, error) {

	maxRounds := h.Cfg.Tools.MaxToolRounds
	if maxRounds <= 0 {
		maxRounds = 4
	}
	if !exec {
		maxRounds = 1
	}

	prompt := text
	var res *chat.TurnResult
	var err error
	for round := 0; round < maxRounds; round++ {
		// Only round 0 is a full client transcript — later rounds send
		// server-generated tool results, which are appends, not state.
		var roundTail, roundFinal string
		if round == 0 {
			roundTail, roundFinal = tail, final
		}
		res, err = h.Bridge.RunTurn(ctx, chat.TurnOpts{
			Text:           prompt,
			Tail:           roundTail,
			Final:          roundFinal,
			AgentMode:      agentMode,
			ThinkingEffort: thinking,
			UserTimezone:   h.Cfg.Quick.UserTimezone,
			UserLocale:     h.Cfg.Quick.UserLocale,
			Title:          "quick2api " + reqID,
			ConversationID: conversationID,
			Attachments:    atts,
			OnEvent: func(ev chat.TurnEvent) {
				frames.OnEvent(ev)
				if onDelta != nil && ev.Kind == "delta" && ev.Text != "" {
					onDelta(ev.Text)
				}
			},
		})
		if err != nil {
			return nil, err
		}

		calls, _ := common.ParseToolCallsJSON(res.Text)
		if len(calls) == 0 || !exec {
			return res, nil
		}
		if onToolRound != nil {
			onToolRound(round, calls)
		}

		var next strings.Builder
		fmt.Fprintf(&next, "Assistant: %s\n\n", res.Text)
		for _, c := range calls {
			result, isErr := h.Tools.Exec(ctx, c.Name, c.Input)
			label := "ToolResult"
			if isErr {
				label += " (error)"
			}
			fmt.Fprintf(&next, "%s for %s(%s): %s\n\n",
				label, c.Name, string(c.Input), result)
		}
		prompt = strings.TrimSpace(next.String())
		if prompt == "" {
			return res, nil
		}
		// Subsequent turns don't need the attachments again.
		atts = nil
	}
	return res, nil
}

// stream emits Anthropic SSE: message_start → content_block_start →
// content_block_delta(text) ×N → content_block_stop → message_delta →
// message_stop. Tool-use rounds can't stream live (the JSON arrives
// whole); each completed tool round emits a `quick2api_tool` progress
// event, and the final non-tool round's text is emitted as one delta.
func (h *Handler) stream(w http.ResponseWriter, ctx context.Context,
	reqID, model, agentMode, thinking, text, tail, final string,
	atts []chat.Attachment, conversationID string, exec, clientTools bool, dlogDir string) {

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	fl, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "api_error", "streaming unsupported")
		return
	}
	w.WriteHeader(http.StatusOK)

	emit := func(event string, v any) bool {
		data, err := json.Marshal(v)
		if err != nil {
			return false
		}
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
		fl.Flush()
		return true
	}

	msgID := "msg_" + reqID
	started := false
	start := func() {
		if started {
			return
		}
		started = true
		emit("message_start", map[string]any{
			"message": map[string]any{
				"id": msgID, "type": "message", "role": "assistant",
				"model": model, "content": []any{}, "stop_reason": nil, "stop_sequence": nil,
				"usage": map[string]any{"input_tokens": 0, "output_tokens": 0},
			},
		})
		emit("content_block_start", map[string]any{
			"index":         0,
			"content_block": map[string]any{"type": "text", "text": ""},
		})
	}
	start()

	// Live deltas only when nothing will intercept the answer: no
	// executor on this request and no client-declared tools whose
	// passthrough would still arrive as a single tool_use block.
	liveDeltas := !exec && !clientTools

	frames := common.NewFrames(h.Debug.LogFrames)
	res, err := h.runAgent(ctx, reqID, agentMode, thinking, text, tail, final, atts,
		conversationID, exec, frames,
		func(delta string) {
			if liveDeltas {
				emit("content_block_delta", map[string]any{
					"index": 0,
					"delta": map[string]any{"type": "text_delta", "text": delta},
				})
			}
		},
		func(round int, calls []common.ToolCallJSON) {
			// Progress event per tool round — keeps the stream alive and
			// tells the client the server is working without leaking the
			// raw tool JSON.
			names := make([]string, len(calls))
			for i, c := range calls {
				names[i] = c.Name
			}
			emit("quick2api_tool", map[string]any{
				"round": round, "calls": names,
			})
		})
	frames.Write(h.Debug, dlogDir)

	if err == nil && res != nil && !liveDeltas {
		// The final text was buffered — emit it now as one delta, unless
		// it parses as tool calls (emitted as a tool_use block below).
		if calls, _ := common.ParseToolCallsJSON(res.Text); len(calls) == 0 {
			emit("content_block_delta", map[string]any{
				"index": 0,
				"delta": map[string]any{"type": "text_delta", "text": res.Text},
			})
		}
	}

	// close the text block even on error — clients expect a terminated stream
	emit("content_block_stop", map[string]any{"index": 0})

	if err != nil {
		h.finishDebug(dlogDir, nil, err)
		emit("message_delta", map[string]any{
			"delta": map[string]any{"stop_reason": "error"},
			"usage": map[string]any{"output_tokens": 0},
		})
		emit("error", map[string]any{"type": "error", "error": map[string]any{
			"type": "api_error", "message": err.Error(),
		}})
		emit("message_stop", map[string]any{})
		return
	}

	// If the whole answer is a tool call, translate it into a tool_use block
	// (still a single block; legal to emit after the text block closes).
	content := parseToolCalls(res.Text)
	stopReason := "end_turn"
	if len(content) == 1 && content[0].Type == "tool_use" {
		stopReason = "tool_use"
		emit("content_block_start", map[string]any{
			"index":         1,
			"content_block": content[0],
		})
		emit("content_block_delta", map[string]any{
			"index": 1,
			"delta": map[string]any{
				"type":         "input_json_delta",
				"partial_json": string(content[0].Input),
			},
		})
		emit("content_block_stop", map[string]any{"index": 1})
	}

	u := mapUsage(res.Usage)
	emit("message_delta", map[string]any{
		"delta": map[string]any{
			"stop_reason":   stopReason,
			"stop_sequence": nil,
		},
		"usage": map[string]any{
			"input_tokens":                u.InputTokens,
			"output_tokens":               u.OutputTokens,
			"cache_read_input_tokens":     u.CacheReadInputTokens,
			"cache_creation_input_tokens": u.CacheCreationInputTokens,
		},
	})
	emit("message_stop", map[string]any{})
	h.finishDebug(dlogDir, &anthropicMessage{
		ID: msgID, Type: "message", Role: "assistant",
		Model: util.Or(res.ResolvedModel, model), Content: content,
		StopReason: stopReason, Usage: u,
	}, nil)
}

func (h *Handler) finishDebug(dir string, out any, runErr error) {
	rec := map[string]any{"ts": time.Now().UTC()}
	if runErr != nil {
		rec["error"] = runErr.Error()
	}
	if out != nil {
		rec["response"] = out
	}
	h.Debug.Write(dir, "02-response", rec)
}

// ---------- helpers ----------

// renderPrompt flattens the Anthropic request into the single text the
// upstream user_message carries, plus any binary payload blocks (images /
// documents) converted to chat.Attachment for the out-of-band channel.
//
// When `withTools` is false the `tools` array is left out of the text —
// callers that run a real tool loop (executor) still want the schemas in
// the prompt, callers that just pass the advisory contract through prefer
// to keep them anyway; both produce identical rendered text today.
// tailAt is the byte offset inside the returned text where the rendered
// message history begins — everything before it is the request preamble
// (system prompt + tools instruction). Callers use text[tailAt:] as the
// delta-eligible Tail so preamble drift never poisons the prefix check.
func renderPrompt(req *MessagesRequest) (text string, tailAt, lastMsgAt int, atts []chat.Attachment, err error) {
	var b strings.Builder

	// system: string | [ {type:"text",text:…} ]
	if len(req.System) > 0 && string(req.System) != "null" {
		var sys string
		if err := json.Unmarshal(req.System, &sys); err == nil {
			if sys != "" {
				fmt.Fprintf(&b, "System: %s\n\n", sys)
			}
		} else {
			var parts []ContentBlock
			if err := json.Unmarshal(req.System, &parts); err == nil {
				for _, p := range parts {
					if p.Type == "text" && p.Text != "" {
						fmt.Fprintf(&b, "System: %s\n\n", p.Text)
					}
				}
			}
		}
	}

	// tools → instruction block so the agent can see its own schemas.
	if ins := common.ToolsInstruction(req.Tools); ins != "" {
		b.WriteString(ins)
	}

	// The builder's head writes never start with whitespace, so this offset
	// stays valid after the final TrimSpace.
	tailAt = b.Len()
	lastMsgAt = tailAt
	for _, m := range req.Messages {
		msgStart := b.Len()
		// content: string | [ blocks ]
		var s string
		if err := json.Unmarshal(m.Content, &s); err == nil {
			if s != "" {
				label := "User"
				if m.Role == "assistant" {
					label = "Assistant"
				}
				fmt.Fprintf(&b, "%s: %s\n\n", label, s)
				lastMsgAt = msgStart
			}
			continue
		}
		var blocks []ContentBlock
		if err := json.Unmarshal(m.Content, &blocks); err != nil {
			return "", 0, 0, nil, fmt.Errorf("%s message: content must be a string or blocks", m.Role)
		}
		for _, bl := range blocks {
			switch bl.Type {
			case "text":
				label := "User"
				if m.Role == "assistant" {
					label = "Assistant"
				}
				fmt.Fprintf(&b, "%s: %s\n\n", label, bl.Text)
			case "image":
				a, aerr := attachmentFromImageBlock(&bl)
				if aerr != nil {
					return "", 0, 0, nil, fmt.Errorf("image block: %w", aerr)
				}
				if a != nil {
					atts = append(atts, *a)
				}
			case "document":
				a, aerr := attachmentFromDocumentBlock(&bl)
				if aerr != nil {
					return "", 0, 0, nil, fmt.Errorf("document block: %w", aerr)
				}
				if a != nil {
					atts = append(atts, *a)
				}
			case "tool_use":
				fmt.Fprintf(&b, "ToolUse %s(%s)\n\n", bl.Name, string(bl.Input))
			case "tool_result":
				inner := ""
				if len(bl.Content) > 0 {
					var s string
					if json.Unmarshal(bl.Content, &s) == nil {
						inner = s
					} else {
						var parts []ContentBlock
						if json.Unmarshal(bl.Content, &parts) == nil {
							for _, p := range parts {
								if p.Type == "text" {
									inner += p.Text
								}
							}
						}
					}
				}
				label := "ToolResult"
				if bl.IsError {
					label += " (error)"
				}
				fmt.Fprintf(&b, "%s: %s\n\n", label, inner)
			default:
				// thinking/redacted_thinking/etc — dropped silently.
			}
		}
		if b.Len() > msgStart {
			lastMsgAt = msgStart
		}
	}

	out := strings.TrimSpace(b.String())
	if out == "" && len(atts) == 0 {
		return "", 0, 0, nil, errors.New("messages rendered to empty text")
	}
	return out, tailAt, lastMsgAt, atts, nil
}

// attachmentFromImageBlock converts an Anthropic `image` content block
// (base64 or URL source) into a chat.Attachment for the upstream upload
// channel. URL sources are fetched here so the bytes ride the same
// out-of-band path the app uses for pasted files.
func attachmentFromImageBlock(bl *ContentBlock) (*chat.Attachment, error) {
	if len(bl.Source) == 0 {
		return nil, errors.New("missing source")
	}
	var src struct {
		Type      string `json:"type"`
		MediaType string `json:"media_type"`
		Data      string `json:"data"`
		URL       string `json:"url"`
	}
	if err := json.Unmarshal(bl.Source, &src); err != nil {
		return nil, fmt.Errorf("bad source: %w", err)
	}
	switch src.Type {
	case "base64":
		raw, err := base64.StdEncoding.DecodeString(src.Data)
		if err != nil {
			return nil, fmt.Errorf("bad base64: %w", err)
		}
		mime := src.MediaType
		if mime == "" {
			mime = "image/png"
		}
		return &chat.Attachment{
			Name:     "image." + extForMime(mime),
			MimeType: mime,
			Data:     raw,
		}, nil
	case "url":
		raw, mime, err := fetchURL(src.URL)
		if err != nil {
			return nil, err
		}
		if mime == "" {
			mime = src.MediaType
		}
		if mime == "" {
			mime = "image/png"
		}
		return &chat.Attachment{
			Name:     "image." + extForMime(mime),
			MimeType: mime,
			Data:     raw,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported image source type %q", src.Type)
	}
}

// attachmentFromDocumentBlock converts a `document` block (PDF/txt/etc,
// base64 or URL) into a chat.Attachment.
func attachmentFromDocumentBlock(bl *ContentBlock) (*chat.Attachment, error) {
	if len(bl.Source) == 0 {
		return nil, errors.New("missing source")
	}
	var src struct {
		Type      string `json:"type"`
		MediaType string `json:"media_type"`
		Data      string `json:"data"`
		URL       string `json:"url"`
	}
	if err := json.Unmarshal(bl.Source, &src); err != nil {
		return nil, fmt.Errorf("bad source: %w", err)
	}
	name := "document.bin"
	mime := src.MediaType
	if mime == "" {
		mime = "application/octet-stream"
	}
	if t := extForMime(mime); t != "bin" {
		name = "document." + t
	}
	var raw []byte
	switch src.Type {
	case "base64":
		b, err := base64.StdEncoding.DecodeString(src.Data)
		if err != nil {
			return nil, fmt.Errorf("bad base64: %w", err)
		}
		raw = b
	case "url":
		b, got, err := fetchURL(src.URL)
		if err != nil {
			return nil, err
		}
		raw = b
		if got != "" {
			mime = got
		}
	default:
		return nil, fmt.Errorf("unsupported document source type %q", src.Type)
	}
	return &chat.Attachment{Name: name, MimeType: mime, Data: raw}, nil
}

// fetchURL downloads bytes from a URL with a 30s cap.
func fetchURL(u string) (data []byte, mime string, err error) {
	cli := &http.Client{Timeout: 30 * time.Second}
	resp, err := cli.Get(u) //nolint:gosec // client-supplied URL is the point
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, "", fmt.Errorf("fetch %s: status %d", u, resp.StatusCode)
	}
	mime = resp.Header.Get("Content-Type")
	if i := strings.IndexByte(mime, ';'); i >= 0 {
		mime = strings.TrimSpace(mime[:i])
	}
	const maxBytes = 20 << 20
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return nil, "", err
	}
	if len(raw) > maxBytes {
		return nil, "", fmt.Errorf("fetch %s: >%d bytes", u, maxBytes)
	}
	return raw, mime, nil
}

func extForMime(m string) string {
	switch {
	case strings.HasPrefix(m, "image/"):
		return strings.TrimPrefix(m, "image/")
	case m == "application/pdf":
		return "pdf"
	case strings.HasPrefix(m, "text/"):
		return "txt"
	case m == "application/json":
		return "json"
	case m == "application/octet-stream":
		return "bin"
	default:
		return "bin"
	}
}

// parseToolCalls detects a single tool_use JSON the model emitted as its
// whole answer. Multiple calls degrade to plain text (acceptable — the
// upstream agent is conversational, not a strict tool runtime).
func parseToolCalls(text string) []ContentBlock {
	trimmed := strings.TrimSpace(text)
	// strip a stray ```json fence if the model wrapped it
	trimmed = strings.TrimPrefix(trimmed, "```json")
	trimmed = strings.TrimPrefix(trimmed, "```")
	trimmed = strings.TrimSuffix(trimmed, "```")
	trimmed = strings.TrimSpace(trimmed)

	var blk ContentBlock
	if json.Unmarshal([]byte(trimmed), &blk) == nil &&
		blk.Type == "tool_use" && blk.Name != "" && len(blk.Input) > 0 {
		if blk.ID == "" {
			blk.ID = common.NewID("toolu")
		}
		return []ContentBlock{blk}
	}
	return []ContentBlock{{Type: "text", Text: text}}
}

func mapUsage(u *chat.TurnUsage) anthropicUsage {
	if u == nil {
		return anthropicUsage{}
	}
	return anthropicUsage{
		InputTokens:              u.InputTokens,
		OutputTokens:             u.OutputTokens,
		CacheReadInputTokens:     u.CacheReadTokens,
		CacheCreationInputTokens: u.CacheWriteTokens,
	}
}

func writeUpstream(w http.ResponseWriter, err error) {
	status, kind, msg := common.UpstreamError(err)
	typ := "api_error"
	switch kind {
	case common.KindUnauthenticated:
		typ = "authentication_error"
	case common.KindRateLimited:
		typ = "rate_limit_error"
	case common.KindTimeout:
		typ = "timeout"
	}
	writeErr(w, status, typ, msg)
}

func writeErr(w http.ResponseWriter, status int, typ, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type":  "error",
		"error": map[string]any{"type": typ, "message": msg},
	})
}
