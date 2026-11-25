// Package anthropic adapts Anthropic /v1/messages onto one Quick turn.
//
// Why it exists: Claude Code (and the wider Anthropic-SDK ecosystem)
// speaks /v1/messages + SSE, not /v1/chat/completions. Serving it here
// lets a `claude` CLI point ANTHROPIC_BASE_URL at quick2api and run on
// the Quick subscription — no second proxy hop.
//
// Scope: the text surface only. Client-declared tools are flattened
// into a leading instruction block (the upstream agent sees their
// schemas as text); historical tool calls and results are rendered as
// JSON envelopes retaining their correlation IDs and content blocks.
// Stop sequences and max_tokens are passed positionally but the
// upstream turn doesn't honor them server-side — they're metadata.
package anthropic

import (
	"context"
	"crypto/sha256"
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
	Native *NativeBridge
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
	if err := common.DecodeRequestBody(w, r, &req, h.Cfg.RequestBodyLimit()); err != nil {
		status, kind, message := common.RequestBodyError(err)
		writeErr(w, status, kind, message)
		return
	}
	render := renderPrompt
	if h.Native != nil {
		render = renderNativePrompt
	}
	text, _, _, atts, err := render(&req)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	bytes := len(text)
	for _, att := range atts {
		if strings.HasPrefix(att.MimeType, "text/") || att.MimeType == "application/json" {
			bytes += len(att.Data)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"input_tokens": bytes / 4})
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
	if err := common.DecodeRequestBody(w, r, &req, h.Cfg.RequestBodyLimit()); err != nil {
		status, kind, message := common.RequestBodyError(err)
		writeErr(w, status, kind, message)
		return
	}
	if len(req.Messages) == 0 {
		writeErr(w, http.StatusBadRequest, "invalid_request_error", "messages is empty")
		return
	}

	render := renderPrompt
	if h.Native != nil {
		render = renderNativePrompt
	}
	text, tailAt, lastMsgAt, atts, err := render(&req)
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
	if h.Native != nil {
		req.Model = model
		h.serveNative(w, r, reqID, model, conversationID, text, tail, final, atts, &req)
		return
	}
	clientTools := clientHasTools(req.Tools)
	toolCount := clientToolCount(req.Tools)

	dlogDir := h.Debug.Dir(reqID)
	h.Debug.Write(dlogDir, "00-request", map[string]any{
		"id": reqID, "model": model, "agent_mode": agentMode,
		"stream": req.Stream, "messages": len(req.Messages),
		"conversation": conversationID,
		"system_bytes": len(req.System), "tools_bytes": len(req.Tools),
		"tool_count": toolCount, "rendered_bytes": len(text),
		"tail_bytes": len(tail), "final_bytes": len(final),
		"client_tools": clientTools,
	})

	ctx, cancel := context.WithTimeout(r.Context(), h.Cfg.TurnTimeout())
	defer cancel()

	exec := h.execFor(clientTools)
	runtimeSuffix := common.CompactToolsInstruction(req.Tools)
	var catalog *common.ClientTools
	if !exec {
		catalog = common.NewClientTools(req.Tools, req.ToolChoice)
	}
	if req.Stream {
		h.stream(w, ctx, reqID, model, agentMode, thinking, text, tail, final, atts, conversationID, exec, runtimeSuffix, dlogDir, catalog)
		return
	}
	h.complete(w, ctx, reqID, model, agentMode, thinking, text, tail, final, atts, conversationID, exec, runtimeSuffix, dlogDir, catalog)
}

// clientHasTools reports whether the client declared its own tool
// schemas — those belong to the client's runtime in auto mode.
func clientHasTools(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) != "null" && string(raw) != "[]"
}

func clientToolCount(raw json.RawMessage) int {
	var tools []json.RawMessage
	if json.Unmarshal(raw, &tools) != nil {
		return 0
	}
	return len(tools)
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
	atts []chat.Attachment, conversationID string, exec bool, runtimeSuffix, dlogDir string, catalog *common.ClientTools) {

	frames := common.NewFrames(h.Debug.LogFrames)
	res, err := h.runAgent(ctx, reqID, agentMode, thinking, text, tail, final, atts,
		conversationID, exec, runtimeSuffix, frames, nil, nil)
	// Always write frames before returning — the error path is exactly
	// where they are needed to see the upstream's real error payload.
	frames.Write(h.Debug, dlogDir)
	if err != nil {
		h.finishDebug(dlogDir, nil, err)
		writeUpstream(w, err)
		return
	}

	content, stopReason := responseContent(res, catalog)
	out := anthropicMessage{
		ID:         "msg_" + reqID,
		Type:       "message",
		Role:       "assistant",
		Model:      util.Or(res.ResolvedModel, model),
		Content:    content,
		Usage:      mapUsage(res.Usage),
		StopReason: stopReason,
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
	atts []chat.Attachment, conversationID string, exec bool, runtimeSuffix string,
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
			Suffix:         runtimeSuffix,
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
		if res.PartialOnClose {
			return nil, &chat.UpstreamError{Code: "incomplete_response", Message: "upstream closed before message_complete"}
		}
		if res.FinishReason == "content_filter" {
			return res, nil
		}

		calls, _ := common.ParseToolCallsJSON(res.ToolText())
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
	atts []chat.Attachment, conversationID string, exec bool, runtimeSuffix, dlogDir string, catalog *common.ClientTools) {

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	_, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "api_error", "streaming unsupported")
		return
	}
	w.WriteHeader(http.StatusOK)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream := &messageStream{w: w, cancel: cancel}
	emit := stream.emit

	msgID := "msg_" + reqID
	if !emit("message_start", map[string]any{
		"message": map[string]any{
			"id": msgID, "type": "message", "role": "assistant",
			"model": model, "content": []any{}, "stop_reason": nil, "stop_sequence": nil,
			"usage": map[string]any{"input_tokens": 0, "output_tokens": 0},
		},
	}) {
		return
	}
	if !emit("content_block_start", map[string]any{
		"index":         0,
		"content_block": map[string]any{"type": "text", "text": ""},
	}) {
		return
	}
	stopKeepalive := stream.startKeepalive(ctx, streamPingInterval)
	defer stopKeepalive()

	// Live deltas only when nothing will intercept the answer: no
	// executor on this request and no client-declared tools whose
	// passthrough would still arrive as a single tool_use block.
	liveDeltas := !exec && runtimeSuffix == ""

	frames := common.NewFrames(h.Debug.LogFrames)
	res, err := h.runAgent(ctx, reqID, agentMode, thinking, text, tail, final, atts,
		conversationID, exec, runtimeSuffix, frames,
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
	stopKeepalive()
	frames.Write(h.Debug, dlogDir)

	var content []ContentBlock
	var stopReason string
	if err == nil && res != nil {
		content, stopReason = responseContent(res, catalog)
	}
	if !liveDeltas && len(content) > 0 && content[0].Type == "text" {
		// Narration and tool calls are distinct blocks. Never stream the
		// original JSON as text before classifying a buffered reply.
		if !emit("content_block_delta", map[string]any{
			"index": 0,
			"delta": map[string]any{"type": "text_delta", "text": content[0].Text},
		}) {
			return
		}
	}

	if !emit("content_block_stop", map[string]any{"index": 0}) {
		return
	}

	if err != nil {
		h.finishDebug(dlogDir, nil, err)
		_, errorType, message := upstreamErrorDetails(err)
		// An SSE error is terminal. "error" is not an Anthropic stop_reason,
		// and a normal message_stop must not advertise a failed turn as done.
		emit("error", map[string]any{"type": "error", "error": map[string]any{
			"type": errorType, "message": message,
		}})
		return
	}

	if !emitToolBlocks(emit, content) {
		return
	}

	u := mapUsage(res.Usage)
	if !emit("message_delta", map[string]any{
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
	}) {
		return
	}
	if !emit("message_stop", map[string]any{}) {
		return
	}
	h.finishDebug(dlogDir, &anthropicMessage{
		ID: msgID, Type: "message", Role: "assistant",
		Model: util.Or(res.ResolvedModel, model), Content: content,
		StopReason: stopReason, Usage: u,
	}, nil)
}

// A blocked answer must remain a refusal even if its partial text resembles
// tool-call JSON. Never execute it or advertise a normal successful turn.
func responseContent(res *chat.TurnResult, catalog *common.ClientTools) ([]ContentBlock, string) {
	if res.FinishReason == "content_filter" {
		return []ContentBlock{{Type: "text", Text: res.Text}}, "refusal"
	}
	if !res.PartialOnClose && res.FinishReason == "stop" {
		if catalog != nil {
			if narration, calls, ok := catalog.ParseReply(res.ToolText()); ok {
				content := make([]ContentBlock, 0, len(calls)+1)
				if narration != "" {
					content = append(content, ContentBlock{Type: "text", Text: narration})
				}
				for _, call := range calls {
					id := call.ID
					if id == "" {
						id = common.NewID("toolu")
					}
					content = append(content, ContentBlock{Type: "tool_use", ID: id, Name: call.Name, Input: call.Input})
				}
				return content, "tool_use"
			}
		} else {
			// Server-execution mode retains its strict whole-response parser;
			// mixed prose is eligible only for declared client tools.
			content := parseToolCalls(res.ToolText())
			if len(content) == 1 && content[0].Type == "tool_use" {
				return content, "tool_use"
			}
		}
	}
	return []ContentBlock{{Type: "text", Text: res.Text}}, "end_turn"
}

// Index 0 is the narration block started before the upstream turn. Each tool
// gets its own subsequent block, with input supplied exactly once via deltas.
func emitToolBlocks(emit func(string, map[string]any) bool, content []ContentBlock) bool {
	index := 1
	for _, block := range content {
		if block.Type != "tool_use" {
			continue
		}
		start := block
		start.Input = json.RawMessage(`{}`)
		if !emit("content_block_start", map[string]any{"index": index, "content_block": start}) {
			return false
		}
		if !emit("content_block_delta", map[string]any{"index": index, "delta": map[string]any{"type": "input_json_delta", "partial_json": string(block.Input)}}) {
			return false
		}
		if !emit("content_block_stop", map[string]any{"index": index}) {
			return false
		}
		index++
	}
	return true
}

// SDK stream accumulators dispatch on the JSON type, not only the SSE event
// header. Both must be present and match, including message_stop.
func writeSSE(w io.Writer, event string, data map[string]any) error {
	data["type"] = event
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b)
	return err
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
// lastMsgAt points to the start of the latest operative message. Its slice
// includes trailing system/environment messages, which must not displace
// the user's request from the inline portion of an oversized prompt.
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
		label := "User"
		switch m.Role {
		case "assistant":
			label = "Assistant"
		case "system":
			label = "System"
		}
		// content: string | [ blocks ]
		var s string
		if err := json.Unmarshal(m.Content, &s); err == nil {
			if s != "" {
				fmt.Fprintf(&b, "%s: %s\n\n", label, s)
				if m.Role != "system" {
					lastMsgAt = msgStart
				}
			}
			continue
		}
		var blocks []json.RawMessage
		if err := json.Unmarshal(m.Content, &blocks); err != nil {
			return "", 0, 0, nil, fmt.Errorf("%s message: content must be a string or blocks", m.Role)
		}
		for _, rawBlock := range blocks {
			var bl ContentBlock
			if err := json.Unmarshal(rawBlock, &bl); err != nil {
				return "", 0, 0, nil, fmt.Errorf("%s message block: %w", m.Role, err)
			}
			switch bl.Type {
			case "text":
				fmt.Fprintf(&b, "%s: %s\n\n", label, bl.Text)
			case "image", "document":
				var a *chat.Attachment
				var aerr error
				if bl.Type == "image" {
					a, aerr = attachmentFromImageBlock(&bl)
				} else {
					a, aerr = attachmentFromDocumentBlock(&bl)
				}
				if aerr != nil {
					return "", 0, 0, nil, fmt.Errorf("%s block: %w", bl.Type, aerr)
				}
				if a == nil {
					return "", 0, 0, nil, fmt.Errorf("missing %s attachment", bl.Type)
				}
				digest := sha256.Sum256(rawBlock)
				a.Name = fmt.Sprintf("input-%x.%s", digest[:8], extForMime(a.MimeType))
				reference, refErr := attachmentReference(rawBlock, a.Name)
				if refErr != nil {
					return "", 0, 0, nil, refErr
				}
				a.TranscriptOffset = b.Len() + 1
				atts = append(atts, *a)
				fmt.Fprintf(&b, "%s: %s\n\n", label, reference)
			case "tool_use":
				// Keep the correlation ID and escape embedded newlines. A tool
				// transcript is data, not a new User/System instruction.
				data, marshalErr := json.Marshal(ContentBlock{Type: bl.Type, ID: bl.ID, Name: bl.Name, Input: bl.Input})
				if marshalErr != nil {
					return "", 0, 0, nil, fmt.Errorf("tool_use: %w", marshalErr)
				}
				fmt.Fprintf(&b, "ToolUse: %s\n\n", data)
			case "tool_result":
				data, resultAtts, resultErr := renderToolResult(bl)
				if resultErr != nil {
					return "", 0, 0, nil, fmt.Errorf("tool_result: %w", resultErr)
				}
				for i := range resultAtts {
					resultAtts[i].TranscriptOffset = b.Len() + 1
				}
				atts = append(atts, resultAtts...)
				fmt.Fprintf(&b, "ToolResult: %s\n\n", data)
			default:
				// thinking/redacted_thinking/etc — dropped silently.
			}
		}
		if b.Len() > msgStart && m.Role != "system" {
			lastMsgAt = msgStart
		}
	}

	out := strings.TrimSpace(b.String())
	if out == "" && len(atts) == 0 {
		return "", 0, 0, nil, errors.New("messages rendered to empty text")
	}
	return out, tailAt, lastMsgAt, atts, nil
}

// renderToolResult preserves IDs, errors and content-block boundaries. Images
// and documents inside results use the same upload path as top-level content;
// base64 bytes are replaced by stable attachment references in the transcript.
func renderToolResult(bl ContentBlock) ([]byte, []chat.Attachment, error) {
	var atts []chat.Attachment
	var parts []json.RawMessage
	var text string
	if len(bl.Content) > 0 && string(bl.Content) != "null" && json.Unmarshal(bl.Content, &text) != nil {
		if err := json.Unmarshal(bl.Content, &parts); err != nil {
			return nil, nil, errors.New("content must be a string or blocks")
		}
		digest := sha256.Sum256(bl.Content)
		for i, raw := range parts {
			var part ContentBlock
			if err := json.Unmarshal(raw, &part); err != nil {
				return nil, nil, err
			}
			var att *chat.Attachment
			var err error
			switch part.Type {
			case "image":
				att, err = attachmentFromImageBlock(&part)
			case "document":
				att, err = attachmentFromDocumentBlock(&part)
			default:
				continue // retain other blocks verbatim, including text
			}
			if err != nil {
				return nil, nil, err
			}
			if att == nil {
				return nil, nil, errors.New("missing result attachment")
			}
			att.Name = fmt.Sprintf("tool-result-%x-%d.%s", digest[:8], i, extForMime(att.MimeType))
			atts = append(atts, *att)
			parts[i], err = attachmentReference(raw, att.Name)
			if err != nil {
				return nil, nil, err
			}
		}
		bl.Content, _ = json.Marshal(parts)
	}
	data, err := json.Marshal(ContentBlock{Type: "tool_result", ToolUseID: bl.ToolUseID, Content: bl.Content, IsError: bl.IsError})
	return data, atts, err
}

func attachmentReference(raw json.RawMessage, name string) ([]byte, error) {
	var reference map[string]json.RawMessage
	if err := json.Unmarshal(raw, &reference); err != nil {
		return nil, err
	}
	if reference == nil {
		return nil, errors.New("attachment block must be an object")
	}
	delete(reference, "source")
	reference["attachment"], _ = json.Marshal(name)
	return json.Marshal(reference)
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
// base64, plain-text or URL source) into a chat.Attachment.
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
	var raw []byte
	switch src.Type {
	case "text":
		if src.MediaType != "" && src.MediaType != "text/plain" {
			return nil, errors.New("text document source requires text/plain")
		}
		mime = "text/plain"
		raw = []byte(src.Data)
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
	if t := extForMime(mime); t != "bin" {
		name = "document." + t
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
	status, typ, msg := upstreamErrorDetails(err)
	writeErr(w, status, typ, msg)
}

func upstreamErrorDetails(err error) (int, string, string) {
	status, kind, msg := common.UpstreamError(err)
	typ := "api_error"
	switch kind {
	case common.KindUnauthenticated:
		typ = "authentication_error"
	case common.KindRateLimited:
		typ = "rate_limit_error"
	case common.KindTimeout:
		typ = "timeout_error"
	case common.KindPermissionDenied:
		typ = "permission_error"
	}
	return status, typ, msg
}

func writeErr(w http.ResponseWriter, status int, typ, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type":  "error",
		"error": map[string]any{"type": typ, "message": msg},
	})
}
