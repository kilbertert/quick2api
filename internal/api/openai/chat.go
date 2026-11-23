// Package openai adapts OpenAI /v1/chat/completions onto one Quick turn.
// v1 deliberately covers the text surface: system/developer messages render
// as a leading transcript block, multi-turn history flattens into a single
// user_message (Quick's agent keeps its own thread state).
package openai

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
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

// Handler serves OpenAI-compatible endpoints.
type Handler struct {
	Bridge *bridge.Pool
	Cfg    *config.Config
	Debug  *debuglog.Logger
	Sem    chan struct{} // concurrency ceiling
	// Tools is the server-side executor registry; nil = advisory mode.
	// *Registry so execFor can ask Has() for auto-mode passthrough.
	Tools *tools.Registry
}

// ---------- request/response types (subset of the OpenAI schema) ----------

type ChatRequest struct {
	Model           string          `json:"model"`
	Messages        []Message       `json:"messages"`
	Stream          bool            `json:"stream"`
	StreamOptions   *StreamOptions  `json:"stream_options"`
	MaxTokens       int             `json:"max_tokens"`
	Temperature     *float64        `json:"temperature"`
	ReasoningEffort string          `json:"reasoning_effort"`
	Metadata        json.RawMessage `json:"metadata"`
	ResponseFormat  json.RawMessage `json:"response_format"`
	Tools           json.RawMessage `json:"tools"`
	ToolChoice      json.RawMessage `json:"tool_choice"`
	Stop            json.RawMessage `json:"stop"`
	N               int             `json:"n"`
	// ConversationID pins the upstream thread (same contract as
	// /v1/messages): when set the bridge pools one conversation per id.
	ConversationID string `json:"conversation_id,omitempty"`
	User           string `json:"user,omitempty"` // fallback thread key
}

type StreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type Message struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
	Name    string          `json:"name"`
	// ToolCalls carries assistant-side function calls when the client
	// echoes its own history; we flatten them into the transcript.
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`
	// ToolCallID correlates a role=tool message to its call.
	ToolCallID string `json:"tool_call_id,omitempty"`
}

type choice struct {
	Index        int         `json:"index"`
	Message      *MessageOut `json:"message,omitempty"`
	Delta        *MessageOut `json:"delta,omitempty"`
	FinishReason string      `json:"finish_reason,omitempty"`
}

type MessageOut struct {
	Role      string     `json:"role,omitempty"`
	Content   string     `json:"content,omitempty"`
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`
}

// ToolCall is OpenAI's function-call shape. We translate the model's
// tool_use JSON into this so clients see their declared function names.
type ToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"` // always "function"
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"` // JSON-encoded
	} `json:"function"`
}

type completion struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []choice `json:"choices"`
	Usage   *usage   `json:"usage,omitempty"`
}

type usage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
	// Quick-specific extensions (all optional).
	CacheReadTokens  int64   `json:"cache_read_input_tokens,omitempty"`
	CacheWriteTokens int64   `json:"cache_write_input_tokens,omitempty"`
	ResolvedModelID  string  `json:"quick_resolved_model_id,omitempty"`
	CostUSD          float64 `json:"quick_cost_usd,omitempty"`
	ContextWindow    int64   `json:"quick_context_window,omitempty"`
}

// ---------- endpoint ----------

// ChatCompletions handles POST /v1/chat/completions.
func (h *Handler) ChatCompletions(w http.ResponseWriter, r *http.Request) {
	reqID := common.ReqID(r)
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST", "")
		return
	}
	var req ChatRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "bad JSON: "+err.Error(), "")
		return
	}
	if len(req.Messages) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "messages is empty", "")
		return
	}
	if req.N > 1 {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "n>1 unsupported: Quick runs one turn per request", "")
		return
	}
	text, atts, err := renderTranscript(req.Messages)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", err.Error(), "")
		return
	}
	// tools → same instruction contract the Anthropic adapter emits so the
	// upstream model sees one protocol regardless of which surface the
	// client speaks.
	if ins := common.ToolsInstruction(req.Tools); ins != "" {
		text = ins + text
	}
	// The whole transcript is the delta-eligible tail: OpenAI messages mix
	// roles inline, so system/developer lines are history content too.
	tail := text
	conversationID := strings.TrimSpace(req.ConversationID)
	if conversationID == "" {
		conversationID = strings.TrimSpace(r.Header.Get("X-Conversation-Id"))
	}
	if conversationID == "" {
		conversationID = strings.TrimSpace(r.Header.Get("X-Session-Id"))
	}
	if conversationID == "" {
		conversationID = strings.TrimSpace(req.User)
	}

	// concurrency ceiling
	release, ok := common.Acquire(r.Context(), h.Sem)
	if !ok {
		if r.Context().Err() == nil {
			writeError(w, http.StatusTooManyRequests, "rate_limit_error",
				"local concurrency limit reached", "")
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
	if req.ReasoningEffort != "" {
		thinking = req.ReasoningEffort
	}

	dlogDir := h.Debug.Dir(reqID)
	h.Debug.Write(dlogDir, "00-request", map[string]any{
		"id": reqID, "model": model, "agent_mode": agentMode,
		"stream": req.Stream, "messages": req.Messages,
	})

	ctx, cancel := context.WithTimeout(r.Context(), h.Cfg.TurnTimeout())
	defer cancel()

	clientTools := clientHasTools(req.Tools)
	exec := h.execFor(clientTools)
	if req.Stream {
		h.stream(w, r, ctx, reqID, model, agentMode, thinking, text, tail, atts,
			conversationID, exec, clientTools, dlogDir)
		return
	}
	h.complete(w, r, ctx, reqID, model, agentMode, thinking, text, tail, atts,
		conversationID, exec, dlogDir)
}

// clientHasTools mirrors the Anthropic handler's check: the OpenAI
// `tools` array is json.RawMessage of a []Tool schema.
func clientHasTools(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) != "null" && string(raw) != "[]"
}

// execFor reports whether the server-side executor may intercept
// tool_use replies for this request. Same contract as Anthropic's:
// "passthrough" never intercepts, "server" always does, "auto"
// intercepts only when the client declared no tools of its own.
func (h *Handler) execFor(clientTools bool) bool {
	if h.Tools == nil {
		return false
	}
	switch h.Cfg.Tools.Mode {
	case "server":
		return true
	case "passthrough":
		return false
	default:
		return !clientTools
	}
}

// complete handles the non-streaming path.
func (h *Handler) complete(w http.ResponseWriter, r *http.Request, ctx context.Context,
	reqID, model, agentMode, thinking, text, tail string,
	atts []chat.Attachment, conversationID string, exec bool, dlogDir string) {

	frames := common.NewFrames(h.Debug.LogFrames)
	res, err := h.runAgent(ctx, reqID, agentMode, thinking, text, tail, atts,
		conversationID, exec, frames, nil, nil)
	frames.Write(h.Debug, dlogDir)
	if err != nil {
		h.finishDebug(dlogDir, nil, err)
		writeUpstreamError(w, err)
		return
	}
	out := completion{
		ID:      "chatcmpl-" + reqID,
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   util.Or(res.ResolvedModel, model),
		Choices: []choice{{
			Index:        0,
			Message:      messageOutFor(res.Text),
			FinishReason: res.FinishReason,
		}},
		Usage: mapUsage(res.Usage),
	}
	// finish_reason=tool_calls when the answer is a tool call.
	if calls, ok := common.ParseToolCallsJSON(res.Text); ok && len(calls) > 0 {
		out.Choices[0].FinishReason = "tool_calls"
		out.Choices[0].Message = &MessageOut{
			Role:      "assistant",
			Content:   "",
			ToolCalls: openAIToolCalls(calls),
		}
	}
	h.finishDebug(dlogDir, &out, nil)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// runAgent mirrors the Anthropic handler's tool loop: when h.Tools is
// enabled and the reply parses as a tool call, execute it locally and
// feed the result back until the model stops calling tools.
func (h *Handler) runAgent(ctx context.Context,
	reqID, agentMode, thinking, text, tail string,
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
		var roundTail string
		if round == 0 {
			roundTail = tail
		}
		res, err = h.Bridge.RunTurn(ctx, chat.TurnOpts{
			Text:           prompt,
			Tail:           roundTail,
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
		atts = nil // subsequent turns don't need the attachments again
	}
	return res, nil
}

// messageOutFor picks the OpenAI message shape: a tool_call answer turns
// into tool_calls[], otherwise plain content.
func messageOutFor(text string) *MessageOut {
	if calls, ok := common.ParseToolCallsJSON(text); ok && len(calls) > 0 {
		return &MessageOut{
			Role:      "assistant",
			Content:   "",
			ToolCalls: openAIToolCalls(calls),
		}
	}
	return &MessageOut{Role: "assistant", Content: text}
}

// openAIToolCalls translates the model's tool_use JSON into OpenAI's
// tool_calls[] shape. Names/inputs come straight through (the schema
// already used Anthropic's spelling), only the envelope changes.
func openAIToolCalls(calls []common.ToolCallJSON) []ToolCall {
	out := make([]ToolCall, len(calls))
	for i, c := range calls {
		out[i].ID = c.ID
		if out[i].ID == "" {
			out[i].ID = fmt.Sprintf("call_%d", i)
		}
		out[i].Type = "function"
		out[i].Function.Name = c.Name
		out[i].Function.Arguments = string(c.Input)
	}
	return out
}

// stream handles the SSE path. Each tool round emits a `quick2api_tool`
// chunk-shaped event; the final round's text arrives as one delta.
func (h *Handler) stream(w http.ResponseWriter, r *http.Request, ctx context.Context,
	reqID, model, agentMode, thinking, text, tail string,
	atts []chat.Attachment, conversationID string, exec, clientTools bool, dlogDir string) {

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	fl, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "server_error", "streaming unsupported", "")
		return
	}
	w.WriteHeader(http.StatusOK)

	emit := func(v any) bool {
		data, err := json.Marshal(v)
		if err != nil {
			return false
		}
		_, err = fmt.Fprintf(w, "data: %s\n\n", data)
		fl.Flush()
		return err == nil
	}

	// role chunk first (OpenAI convention)
	emit(completion{
		ID: "chatcmpl-" + reqID, Object: "chat.completion.chunk",
		Created: time.Now().Unix(), Model: model,
		Choices: []choice{{Index: 0, Delta: &MessageOut{Role: "assistant"}}},
	})

	// Live deltas only when nothing intercepts the answer (no executor
	// on this request and no client-declared tools that would still
	// arrive as one tool_use block).
	liveDeltas := !exec && !clientTools

	frames := common.NewFrames(h.Debug.LogFrames)
	res, err := h.runAgent(ctx, reqID, agentMode, thinking, text, tail, atts,
		conversationID, exec, frames,
		func(delta string) {
			if liveDeltas {
				emit(completion{
					ID: "chatcmpl-" + reqID, Object: "chat.completion.chunk",
					Created: time.Now().Unix(), Model: model,
					Choices: []choice{{Index: 0, Delta: &MessageOut{Content: delta}}},
				})
			}
		},
		func(round int, calls []common.ToolCallJSON) {
			names := make([]string, len(calls))
			for i, c := range calls {
				names[i] = c.Name
			}
			// Chunk-shaped progress event — clients that read deltas see
			// the server is working without leaking the tool JSON.
			emit(completion{
				ID: "chatcmpl-" + reqID, Object: "chat.completion.chunk",
				Created: time.Now().Unix(), Model: model,
				Choices: []choice{{Index: 0, Delta: &MessageOut{
					Content: fmt.Sprintf("[tool round %d: %s]", round, strings.Join(names, ", ")),
				}}},
			})
		})
	frames.Write(h.Debug, dlogDir)
	if err != nil {
		h.finishDebug(dlogDir, nil, err)
		// OpenAI streams still terminate with [DONE]; send an error event
		// chunk so clients see a typed failure rather than a silent cut.
		emit(map[string]any{"error": map[string]any{
			"type": "upstream_error", "message": err.Error(),
		}})
		fmt.Fprint(w, "data: [DONE]\n\n")
		fl.Flush()
		return
	}
	result := res

	// Buffered reply: emit the final text as one delta, unless it parses
	// as tool calls (emitted below as a tool_calls delta).
	if !liveDeltas {
		if calls, ok := common.ParseToolCallsJSON(result.Text); !ok || len(calls) == 0 {
			emit(completion{
				ID: "chatcmpl-" + reqID, Object: "chat.completion.chunk",
				Created: time.Now().Unix(), Model: model,
				Choices: []choice{{Index: 0, Delta: &MessageOut{Content: result.Text}}},
			})
		}
	}

	fin := completion{
		ID: "chatcmpl-" + reqID, Object: "chat.completion.chunk",
		Created: time.Now().Unix(), Model: util.Or(result.ResolvedModel, model),
		Choices: []choice{{Index: 0, Delta: &MessageOut{}, FinishReason: result.FinishReason}},
		Usage:   mapUsage(result.Usage),
	}
	if calls, ok := common.ParseToolCallsJSON(result.Text); ok && len(calls) > 0 {
		fin.Choices[0].FinishReason = "tool_calls"
		fin.Choices[0].Delta = &MessageOut{
			Role:      "assistant",
			ToolCalls: openAIToolCalls(calls),
		}
	}
	emit(fin)
	fmt.Fprint(w, "data: [DONE]\n\n")
	fl.Flush()
	h.finishDebug(dlogDir, &completion{
		ID: "chatcmpl-" + reqID, Object: "chat.completion",
		Created: time.Now().Unix(), Model: util.Or(result.ResolvedModel, model),
		Choices: []choice{{Index: 0, Message: messageOutFor(result.Text), FinishReason: fin.Choices[0].FinishReason}},
		Usage:   mapUsage(result.Usage),
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

// Models handles GET /v1/models: every agent_modes key + extra `models` IDs.
func (h *Handler) Models(w http.ResponseWriter, r *http.Request) {
	ids := map[string]struct{}{}
	for k := range h.Cfg.Quick.AgentModes {
		ids[k] = struct{}{}
	}
	for _, m := range h.Cfg.Quick.Models {
		ids[m] = struct{}{}
	}
	if len(ids) == 0 {
		ids[common.FirstModel(h.Cfg)] = struct{}{}
	}
	list := make([]map[string]any, 0, len(ids))
	for id := range ids {
		entry := map[string]any{
			"id":       id,
			"object":   "model",
			"created":  time.Now().Unix(),
			"owned_by": "quick2api",
		}
		if target := h.Cfg.AgentModeFor(id); target != id {
			entry["agent_mode"] = target
		}
		list = append(list, entry)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": list})
}

// ---------- helpers ----------

// renderTranscript flattens OpenAI messages into the single text the
// upstream user_message carries, plus any binary parts (image_url/file)
// converted to chat.Attachment for the out-of-band upload path.
func renderTranscript(msgs []Message) (text string, atts []chat.Attachment, err error) {
	var b strings.Builder
	for _, m := range msgs {
		// assistant tool_calls → render into transcript so the model sees
		// its own history.
		for _, tc := range m.ToolCalls {
			fmt.Fprintf(&b, "ToolCall %s(%s)\n\n", tc.Function.Name, tc.Function.Arguments)
		}
		content, mAtts, err := flattenContent(m.Content)
		if err != nil {
			return "", nil, fmt.Errorf("%s message: %w", m.Role, err)
		}
		atts = append(atts, mAtts...)
		if content == "" && len(m.ToolCalls) == 0 && len(mAtts) == 0 {
			continue
		}
		label := ""
		switch m.Role {
		case "system", "developer":
			label = "System"
		case "user":
			label = "User"
		case "assistant":
			label = "Assistant"
		case "tool":
			label = "Tool"
		default:
			label = strings.Title(m.Role)
		}
		if content != "" {
			fmt.Fprintf(&b, "%s: %s\n\n", label, content)
		}
	}
	out := strings.TrimSpace(b.String())
	if out == "" && len(atts) == 0 {
		return "", nil, errors.New("messages rendered to empty text")
	}
	return out, atts, nil
}

// flattenContent accepts either a string or an array of content parts.
// image_url/file parts convert to chat.Attachment for the upload path;
// the caller ships them out-of-band instead of inlining.
func flattenContent(raw json.RawMessage) (text string, atts []chat.Attachment, err error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s, nil, nil
	}
	var parts []map[string]any
	if err := json.Unmarshal(raw, &parts); err != nil {
		return "", nil, fmt.Errorf("content must be a string or an array of parts")
	}
	var b strings.Builder
	for _, p := range parts {
		t, _ := p["type"].(string)
		switch t {
		case "text":
			if v, ok := p["text"].(string); ok {
				b.WriteString(v)
			}
		case "refusal":
			if v, ok := p["refusal"].(string); ok {
				b.WriteString("[refusal] " + v)
			}
		case "image_url":
			a, aerr := attachmentFromImageURLPart(p)
			if aerr != nil {
				return "", nil, aerr
			}
			if a != nil {
				atts = append(atts, *a)
			}
		case "file":
			a, aerr := attachmentFromFilePart(p)
			if aerr != nil {
				return "", nil, aerr
			}
			if a != nil {
				atts = append(atts, *a)
			}
		case "input_audio":
			return "", nil, fmt.Errorf("input_audio unsupported: Quick has no audio channel")
		default:
			return "", nil, fmt.Errorf("unsupported content part %q", t)
		}
	}
	return b.String(), atts, nil
}

// attachmentFromImageURLPart converts an OpenAI `image_url` content part
// into a chat.Attachment. image_url:{url:"data:…"} decodes inline; http(s)
// URLs are fetched here so the bytes ride the same out-of-band channel.
func attachmentFromImageURLPart(p map[string]any) (*chat.Attachment, error) {
	raw, _ := p["image_url"].(map[string]any)
	u, _ := raw["url"].(string)
	if u == "" {
		return nil, errors.New("image_url missing url")
	}
	return urlToAttachment(u, "image")
}

// attachmentFromFilePart converts an OpenAI `file` content part into a
// chat.Attachment. file:{file_data:"data:…",filename:"name"} or a plain
// http(s) URL.
func attachmentFromFilePart(p map[string]any) (*chat.Attachment, error) {
	raw, _ := p["file"].(map[string]any)
	data, _ := raw["file_data"].(string)
	name, _ := raw["filename"].(string)
	if name == "" {
		name = "file.bin"
	}
	if data != "" {
		if strings.HasPrefix(data, "data:") {
			a, err := urlToAttachment(data, "file")
			if err != nil {
				return nil, err
			}
			a.Name = name
			return a, nil
		}
		return nil, errors.New("file.file_data must be a data: URL")
	}
	u, _ := raw["url"].(string)
	if u == "" {
		return nil, errors.New("file missing file_data or url")
	}
	a, err := urlToAttachment(u, "file")
	if err != nil {
		return nil, err
	}
	a.Name = name
	return a, nil
}

// urlToAttachment decodes a data: URL or fetches an http(s) URL into an
// Attachment. kind is "image" or "file" — used to pick the fallback
// name when the source doesn't say one.
func urlToAttachment(u, kind string) (*chat.Attachment, error) {
	if strings.HasPrefix(u, "data:") {
		comma := strings.IndexByte(u, ',')
		if comma < 0 {
			return nil, errors.New("bad data: URL")
		}
		header, payload := u[5:comma], u[comma+1:]
		mime := "application/octet-stream"
		if i := strings.IndexByte(header, ';'); i >= 0 {
			mime = header[:i]
		}
		var raw []byte
		var err error
		if strings.HasSuffix(header, ";base64") {
			raw, err = base64.StdEncoding.DecodeString(payload)
		} else {
			raw, err = urlDecode(payload)
		}
		if err != nil {
			return nil, fmt.Errorf("decode data: URL: %w", err)
		}
		return &chat.Attachment{Name: nameFor(mime, kind), MimeType: mime, Data: raw}, nil
	}
	cli := &http.Client{Timeout: 30 * time.Second}
	resp, err := cli.Get(u) //nolint:gosec // client-supplied URL is the point
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("fetch %s: status %d", u, resp.StatusCode)
	}
	mime := resp.Header.Get("Content-Type")
	if i := strings.IndexByte(mime, ';'); i >= 0 {
		mime = strings.TrimSpace(mime[:i])
	}
	const maxBytes = 20 << 20
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > maxBytes {
		return nil, fmt.Errorf("fetch %s: >%d bytes", u, maxBytes)
	}
	return &chat.Attachment{Name: nameFor(mime, kind), MimeType: mime, Data: raw}, nil
}

// nameFor picks a sensible filename for a mime type so the upload
// channel gets something meaningful.
func nameFor(mime, kind string) string {
	ext := "bin"
	switch {
	case strings.HasPrefix(mime, "image/"):
		ext = strings.TrimPrefix(mime, "image/")
	case mime == "application/pdf":
		ext = "pdf"
	case strings.HasPrefix(mime, "text/"):
		ext = "txt"
	case mime == "application/json":
		ext = "json"
	}
	return kind + "." + ext
}

// urlDecode handles percent-encoded data: URLs (rare — most clients
// send base64, but the spec allows it).
func urlDecode(s string) ([]byte, error) {
	// percent-decoding by hand avoids pulling net/url just for this.
	var out []byte
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			v, err := strconv.ParseUint(s[i+1:i+3], 16, 8)
			if err != nil {
				return nil, err
			}
			out = append(out, byte(v))
			i += 2
		} else {
			out = append(out, s[i])
		}
	}
	return out, nil
}

func mapUsage(u *chat.TurnUsage) *usage {
	if u == nil {
		return nil
	}
	return &usage{
		PromptTokens:     u.InputTokens + u.CacheReadTokens + u.CacheWriteTokens,
		CompletionTokens: u.OutputTokens,
		TotalTokens:      u.InputTokens + u.OutputTokens + u.CacheReadTokens + u.CacheWriteTokens,
		CacheReadTokens:  u.CacheReadTokens,
		CacheWriteTokens: u.CacheWriteTokens,
		ResolvedModelID:  u.ResolvedModelID,
		CostUSD:          u.CostUSD,
		ContextWindow:    u.ContextWindow,
	}
}

func writeUpstreamError(w http.ResponseWriter, err error) {
	status, kind, msg := common.UpstreamError(err)
	typ := "upstream_error"
	switch kind {
	case common.KindUnauthenticated:
		typ = "authentication_error"
	case common.KindRateLimited:
		typ = "rate_limit_error"
	case common.KindTimeout:
		typ = "timeout"
	}
	// keep the upstream's own code in the "code" field when present.
	var ue *chat.UpstreamError
	code := ""
	if errors.As(err, &ue) {
		code = ue.Code
	}
	writeError(w, status, typ, msg, code)
}

func writeError(w http.ResponseWriter, status int, typ, msg, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"message": msg, "type": typ, "code": code, "param": nil,
		},
	})
}
