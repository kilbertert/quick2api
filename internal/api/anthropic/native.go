package anthropic

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kilbertert/quick2api/internal/api/common"
	"github.com/kilbertert/quick2api/internal/chat"
	"github.com/kilbertert/quick2api/internal/toolbridge"
)

// NativeBridge owns upstream jobs across HTTP requests. Each job pins its
// original request, catalog, upstream socket and account until completion.
// Human questions explicitly end that job; only their validated real answers
// start a new inference. Ordinary tool results always resume the same job.
type NativeBridge struct {
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	routes      map[string]*nativeTurn
	pending     map[string]*nativeTurn
	active      map[string]*nativeTurn
	seen        map[[32]byte]time.Time
	checkpoints map[string]nativeCheckpoint
	wg          sync.WaitGroup
	// run is injectable for local adapter/state tests, not simulated proof of
	// Quick registration or tool behavior.
	run             func(context.Context, chat.TurnOpts) (*chat.TurnResult, error)
	ttl             time.Duration
	questionTTL     time.Duration
	parkedQuestions int
}

type nativeTurn struct {
	mu            sync.Mutex
	exchange      *toolbridge.Exchange
	route         string
	identity      [32]byte
	conversation  string
	messages      []Message
	call          *toolbridge.Call
	delivered     []ContentBlock
	sessionKey    string
	unsentContext bool
	socketMu      sync.Mutex
	discardSocket func()
	relayMu       sync.Mutex
	receipts      map[string]*nativeReceipt
	receiptBytes  int
	validateRelay func(string, json.RawMessage) error
	parked        atomic.Bool
	stopRun       context.CancelFunc
	questionTimer *time.Timer // guarded by NativeBridge.mu
	questionUntil time.Time   // guarded by NativeBridge.mu
	requestKey    [32]byte
}

// Quick may deliver one invocation directly through MCP and later through its
// WebSocket courier. Both must join one actual client action, not execute twice.
type nativeReceipt struct {
	name   string
	input  json.RawMessage
	done   chan struct{}
	result toolbridge.Result
	err    error
}

var nativeInvocationID = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,128}$`)
var errNativeExchangeExpired = errors.New("native tool exchange is unknown or expired; send a new user message to continue, and do not blindly repeat prior external actions")

func (t *nativeTurn) relay(ctx context.Context, callID, name string, input json.RawMessage) (toolbridge.Result, error) {
	if !nativeInvocationID.MatchString(callID) {
		return toolbridge.Result{}, errors.New("invalid invocation id")
	}
	if t.validateRelay != nil {
		if err := t.validateRelay(name, input); err != nil {
			return toolbridge.Result{}, err
		}
	}
	t.relayMu.Lock()
	if receipt := t.receipts[callID]; receipt != nil {
		if receipt.name != name || !canonicalEqual(receipt.input, input) {
			t.relayMu.Unlock()
			return toolbridge.Result{}, errors.New("invocation id changed its arguments")
		}
		t.relayMu.Unlock()
		select {
		case <-receipt.done:
			return receipt.result, receipt.err
		case <-ctx.Done():
			return toolbridge.Result{}, ctx.Err()
		case <-t.exchange.Done():
			select {
			case <-receipt.done:
				return receipt.result, receipt.err
			default:
				return toolbridge.Result{}, toolbridge.ErrClosed
			}
		}
	}
	// Reserve the maximum encoded result too, so many large inputs cannot
	// turn the bounded receipt count into an unbounded memory allowance.
	receiptCost := len(input) + (32 << 10)
	if len(t.receipts) >= 128 || receiptCost > (4<<20)-t.receiptBytes {
		t.relayMu.Unlock()
		return toolbridge.Result{}, errors.New("native invocation receipt count or 4 MiB budget reached")
	}
	if t.receipts == nil {
		t.receipts = make(map[string]*nativeReceipt)
	}
	receipt := &nativeReceipt{name: name, input: append(json.RawMessage(nil), input...), done: make(chan struct{})}
	t.receipts[callID] = receipt
	t.receiptBytes += receiptCost
	t.relayMu.Unlock()
	receipt.result, receipt.err = t.exchange.Call(ctx, name, input)
	close(receipt.done)
	return receipt.result, receipt.err
}

func NewNativeBridge(parent context.Context, ttl time.Duration, run func(context.Context, chat.TurnOpts) (*chat.TurnResult, error)) *NativeBridge {
	ctx, cancel := context.WithCancel(parent)
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &NativeBridge{ctx: ctx, cancel: cancel, ttl: ttl, questionTTL: 30 * time.Minute, run: run, routes: make(map[string]*nativeTurn), pending: make(map[string]*nativeTurn), active: make(map[string]*nativeTurn), seen: make(map[[32]byte]time.Time), checkpoints: make(map[string]nativeCheckpoint)}
}

func (b *NativeBridge) Close() {
	b.cancel()
	b.mu.Lock()
	turns := make(map[*nativeTurn]bool)
	for _, t := range b.routes {
		turns[t] = true
	}
	for _, t := range b.pending {
		turns[t] = true
		if t.questionTimer != nil {
			t.questionTimer.Stop()
		}
	}
	for _, c := range b.checkpoints {
		if c.discard != nil {
			c.discard()
		}
	}
	clear(b.pending)
	clear(b.active)
	clear(b.checkpoints)
	b.parkedQuestions = 0
	b.mu.Unlock()
	for t := range turns {
		t.exchange.Close()
	}
	b.wg.Wait()
}

func canonicalJSON(data []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}
	if dec.Decode(new(any)) != io.EOF {
		return nil, errors.New("trailing JSON")
	}
	return json.Marshal(value)
}

// renderNativePrompt supplies descriptions and schemas as relay catalog data.
// It deliberately omits the prompt backend's tool JSON imitation contract.
func renderNativePrompt(req *MessagesRequest) (string, int, int, []chat.Attachment, error) {
	copy := *req
	copy.Tools = nil
	text, tailAt, lastAt, atts, err := renderPrompt(&copy)
	if err != nil {
		return "", 0, 0, nil, err
	}
	prefix := ""
	if clientHasTools(req.Tools) {
		var compact bytes.Buffer
		if err := json.Compact(&compact, req.Tools); err != nil {
			return "", 0, 0, nil, errors.New("invalid native client tool catalog")
		}
		prefix = "External client tool catalog (data for the registered native relay):\n" + compact.String() + "\n\n"
	}
	for i := range atts {
		atts[i].TranscriptOffset += len(prefix)
	}
	return prefix + text, tailAt + len(prefix), lastAt + len(prefix), atts, nil
}

// Keep the receiving client's presentation contract inline even when its full
// catalog/history is uploaded as context. This is guidance for real native
// calls, never a parser that promotes assistant markup into an executable call.
func nativePresentation(tools json.RawMessage) string {
	text := "\n\n[EXTERNAL CLIENT PRESENTATION]\nThe recipient renders Anthropic text and tool_use blocks, not Quick's interactive UI. Write ordinary answers in Markdown. Quick-specific <decision> and <option> tags do not create a choice dialog here; do not use them for interactive questions. Preserve literal markup when the user requests a code example or quotation. If a decision genuinely requires the user's input, use a declared client question tool through the native relay, following its description and schema, then wait for the actual answer. If no permitted question tool is available, ask in plain text. Never invent a user selection or use an unrelated tool to imitate a dialog."
	var catalog []struct {
		Name        string          `json:"name"`
		Description string          `json:"description"`
		Schema      json.RawMessage `json:"input_schema"`
	}
	if json.Unmarshal(tools, &catalog) != nil {
		return text
	}
	for _, tool := range catalog {
		if tool.Name != "AskUserQuestion" {
			continue
		}
		text += "\nThe client declares AskUserQuestion. When its usage criteria apply and tool_choice permits it, invoke it through the registered native relay; do not print its arguments as prose."
		// The exact request-scoped declaration avoids substituting a guessed
		// Desktop schema. Large declarations remain in the complete catalog.
		declaration, err := json.Marshal(map[string]any{"name": tool.Name, "description": tool.Description, "input_schema": tool.Schema})
		if err == nil && len(declaration) <= 8<<10 {
			text += "\nClient question tool declaration (catalog data):\n" + string(declaration)
		}
		break
	}
	return text
}

func nativeIdentity(req *MessagesRequest) [32]byte {
	// Streaming mode and token ceilings may vary between continuation requests;
	// the tool contract, system, selected model and thinking configuration may not.
	data, _ := json.Marshal(struct {
		Model                           string
		System, Tools, Choice, Thinking json.RawMessage
	}{req.Model, nativeSystemIdentity(req.System), nativeCatalogIdentity(req.Tools), req.ToolChoice, req.Thinking})
	canonical, _ := canonicalJSON(data)
	return sha256.Sum256(canonical)
}

func nativeSystemIdentity(raw json.RawMessage) json.RawMessage {
	if normalized, err := nativeContent(raw); err == nil {
		return normalized
	}
	return raw
}

func nativeCatalogIdentity(raw json.RawMessage) json.RawMessage {
	var declarations []map[string]json.RawMessage
	if json.Unmarshal(raw, &declarations) != nil {
		return raw
	}
	for _, declaration := range declarations {
		// Only the declaration-level hint is metadata. An identically named
		// property in input_schema is client data and must remain exact.
		delete(declaration, "cache_control")
	}
	normalized, err := json.Marshal(declarations)
	if err != nil {
		return raw
	}
	return normalized
}

func cloneMessages(messages []Message) []Message {
	data, _ := json.Marshal(messages)
	var copied []Message
	_ = json.Unmarshal(data, &copied)
	return copied
}

func canonicalEqual(a, b any) bool {
	left, err := json.Marshal(a)
	if err != nil {
		return false
	}
	right, err := json.Marshal(b)
	if err != nil {
		return false
	}
	left, le := canonicalJSON(left)
	right, re := canonicalJSON(right)
	return le == nil && re == nil && bytes.Equal(left, right)
}

// Anthropic clients can move cache breakpoints or switch a text content value
// from a string to one text block when they append a tool result. Those are
// transport representations of the same history, not permission to alter text,
// tool inputs, roles, identifiers or attachment sources.
func nativeContent(raw json.RawMessage) (json.RawMessage, error) {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return json.Marshal([]map[string]any{{"type": "text", "text": text}})
	}
	var blocks []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return nil, err
	}
	for _, block := range blocks {
		delete(block, "cache_control")
		var kind string
		_ = json.Unmarshal(block["type"], &kind)
		if kind == "tool_result" && len(block["content"]) > 0 {
			content, err := nativeContent(block["content"])
			if err != nil {
				return nil, err
			}
			block["content"] = content
		}
	}
	return json.Marshal(blocks)
}

func nativeHistoryEqual(left, right []Message) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		a, b := left[i], right[i]
		var err error
		a.Content, err = nativeContent(a.Content)
		if err != nil {
			return false
		}
		b.Content, err = nativeContent(b.Content)
		if err != nil {
			return false
		}
		if !canonicalEqual(a, b) {
			return false
		}
	}
	return true
}

func latestNativeResult(req *MessagesRequest) (*ContentBlock, error) {
	for i := len(req.Messages) - 1; i >= 0; i-- {
		m := req.Messages[i]
		if m.Role == "system" {
			continue
		}
		if m.Role != "user" {
			return nil, nil
		}
		var blocks []ContentBlock
		if json.Unmarshal(m.Content, &blocks) != nil {
			return nil, nil
		}
		var found *ContentBlock
		for j := range blocks {
			if blocks[j].Type != "tool_result" || !strings.HasPrefix(blocks[j].ToolUseID, "toolu_q2a_") {
				continue
			}
			if found != nil {
				return nil, errors.New("submit one native client tool result per continuation")
			}
			found = &blocks[j]
		}
		return found, nil
	}
	return nil, nil
}

// Desktop may merge an old question result and a newly submitted user message.
// Only this explicit new-message shape can start after a lost question handle;
// a result alone must never restart an expired job. Earlier actions stay history.
func nativeQuestionFollowup(req *MessagesRequest, result *ContentBlock) bool {
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == "system" {
			continue
		}
		var blocks []ContentBlock
		if req.Messages[i].Role != "user" || json.Unmarshal(req.Messages[i].Content, &blocks) != nil || len(blocks) < 2 || blocks[0].Type != "tool_result" || blocks[0].ToolUseID != result.ToolUseID {
			return false
		}
		hasText := false
		for _, block := range blocks[1:] {
			if block.Type != "text" {
				return false
			}
			hasText = hasText || strings.TrimSpace(block.Text) != ""
		}
		if !hasText {
			return false
		}
		for j := i - 1; j >= 0; j-- {
			if req.Messages[j].Role == "system" {
				continue
			}
			var calls []ContentBlock
			if req.Messages[j].Role != "assistant" || json.Unmarshal(req.Messages[j].Content, &calls) != nil {
				return false
			}
			// Native streaming may deliver narration before the question.
			// Accept only that exact presentation shape: optional text followed
			// by one matching question, with no other calls or content kinds.
			if len(calls) == 2 && calls[0].Type == "text" {
				calls = calls[1:]
			}
			if len(calls) != 1 || calls[0].Type != "tool_use" || calls[0].ID != result.ToolUseID || calls[0].Name != "AskUserQuestion" {
				return false
			}
			catalog := common.NewClientTools(req.Tools, nil)
			return catalog.ValidateCalls([]common.ToolCallJSON{{Type: "tool_use", Name: calls[0].Name, Input: calls[0].Input}}) == nil
		}
		return false
	}
	return false
}

func (b *NativeBridge) remove(t *nativeTurn) {
	b.mu.Lock()
	delete(b.routes, t.route)
	if !t.parked.Load() {
		for id, turn := range b.pending {
			if turn == t {
				delete(b.pending, id)
			}
		}
	}
	b.mu.Unlock()
}

// A human question is an API turn boundary, not a five-minute device RPC.
// Once its call is delivered, stop the upstream job without fabricating an
// answer. Retain only the validated client transcript/handle for 30 minutes.
// The actual answer starts a fresh inference with the complete real history;
// no previously delivered tool is executed or automatically retried here.
func (b *NativeBridge) parkQuestion(t *nativeTurn) {
	b.mu.Lock()
	if b.ctx.Err() != nil || b.pending[t.call.ID] != t || t.parked.Load() {
		b.mu.Unlock()
		return
	}
	t.parked.Store(true)
	b.parkedQuestions++
	delete(b.routes, t.route)
	id := t.call.ID
	t.questionUntil = time.Now().Add(b.questionTTL)
	b.seen[t.requestKey] = t.questionUntil.Add(b.ttl)
	t.questionTimer = time.AfterFunc(b.questionTTL, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.pending[id] == t {
			delete(b.pending, id)
			b.parkedQuestions--
			if b.active[t.conversation] == t {
				delete(b.active, t.conversation)
			}
		}
	})
	b.mu.Unlock()
	t.stopRun()
	t.exchange.Close()
}

// open validates a continuation before consuming any result. Forks, expired
// handles, altered catalogs and concurrent resumes fail without executing tools.
func (b *NativeBridge) open(req *MessagesRequest, conversation string, opts chat.TurnOpts) (*nativeTurn, error) {
	result, err := latestNativeResult(req)
	if err != nil {
		return nil, err
	}
	if result != nil {
		b.mu.Lock()
		t := b.pending[result.ToolUseID]
		b.mu.Unlock()
		if t == nil {
			if nativeQuestionFollowup(req, result) {
				if _, err := nativeResultContent(result.Content); err != nil {
					return nil, err
				}
				opts.Suffix = "\n\n[NEW CLIENT MESSAGE AFTER ENDED QUESTION]\nThe prior AskUserQuestion and its actual client result are completed history. The client appended new text after that result in its latest user message. Respond only to that new text; do not resume or repeat older external actions automatically. This is a fresh user submission, not a retry of the old Quick job."
				return b.start(req, conversation, opts, nil)
			}
			return nil, errNativeExchangeExpired
		}
		if !t.mu.TryLock() {
			return nil, errors.New("native tool exchange already has an active continuation")
		}
		valid := false
		defer func() {
			if !valid {
				t.mu.Unlock()
			}
		}()
		// Additional user content cannot be silently lost while resolving an
		// ordinary same-job tool call. A live question consumes it through its
		// validated new-inference boundary instead.
		var resultBlocks []ContentBlock
		for i := len(req.Messages) - 1; i >= 0; i-- {
			if req.Messages[i].Role != "system" {
				_ = json.Unmarshal(req.Messages[i].Content, &resultBlocks)
				break
			}
		}
		if len(resultBlocks) != 1 && !(t.parked.Load() && nativeQuestionFollowup(req, result)) {
			return nil, errors.New("native continuation cannot append user content before its ordinary pending tool result")
		}
		if nativeIdentity(req) != t.identity || conversation != t.conversation {
			return nil, errors.New("native tool continuation changed its session or tool contract")
		}
		n := len(t.messages)
		if len(req.Messages) < n+2 || !nativeHistoryEqual(req.Messages[:n], t.messages) {
			slog.Warn("native history mismatch", "expected_prefix_messages", n, "received_messages", len(req.Messages))
			return nil, errors.New("native tool continuation diverged from the original conversation")
		}
		assistant := req.Messages[n]
		want := []ContentBlock{{Type: "tool_use", ID: t.call.ID, Name: t.call.Name, Input: t.call.Input}}
		if t.delivered != nil {
			want = t.delivered
		}
		wantRaw, _ := json.Marshal(want)
		assistantContent, normalizeErr := nativeContent(assistant.Content)
		if assistant.Role != "assistant" || normalizeErr != nil || !canonicalEqual(assistantContent, json.RawMessage(wantRaw)) || req.Messages[n+1].Role != "user" {
			return nil, errors.New("native tool continuation does not match the delivered assistant call")
		}
		for _, m := range req.Messages[n+2:] {
			if m.Role != "system" {
				return nil, errors.New("native tool continuation cannot append a new task before its pending result")
			}
		}
		content, err := nativeResultContent(result.Content)
		if err != nil {
			return nil, err
		}
		if t.parked.Load() {
			next, err := b.start(req, conversation, opts, t)
			if err != nil {
				return nil, err
			}
			t.mu.Unlock()
			valid = true
			return next, nil
		}
		if err = t.exchange.Resolve(toolbridge.Result{ID: result.ToolUseID, Content: content, IsError: result.IsError}); err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, toolbridge.ErrClosed) || errors.Is(err, toolbridge.ErrCallCancelled) {
				return nil, errNativeExchangeExpired
			}
			return nil, err
		}
		b.mu.Lock()
		delete(b.pending, result.ToolUseID)
		b.mu.Unlock()
		t.messages = cloneMessages(req.Messages)
		if len(req.Messages) > n+2 {
			t.unsentContext = true
		}
		t.call = nil
		t.delivered = nil
		valid = true
		return t, nil
	}
	return b.start(req, conversation, opts, nil)
}

func (b *NativeBridge) start(req *MessagesRequest, conversation string, opts chat.TurnOpts, replacing *nativeTurn) (*nativeTurn, error) {
	boundaryNote := opts.Suffix
	history, err := nativeHistoryDigest(req.Messages)
	if err != nil {
		return nil, errors.New("native request contains invalid message content")
	}
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	route := hex.EncodeToString(nonce[:])
	catalog := common.NewClientTools(req.Tools, req.ToolChoice)
	t := &nativeTurn{route: route, identity: nativeIdentity(req), conversation: conversation, messages: cloneMessages(req.Messages)}
	t.validateRelay = func(name string, input json.RawMessage) error {
		return catalog.ValidateCalls([]common.ToolCallJSON{{Type: "tool_use", Name: name, Input: input}})
	}
	t.exchange = toolbridge.NewExchange(b.ctx, b.ttl, t.validateRelay)
	t.mu.Lock()
	requestData, _ := json.Marshal(struct {
		Identity     [32]byte
		Conversation string
		History      [32]byte
	}{t.identity, conversation, history})
	requestData, _ = canonicalJSON(requestData)
	requestKey := sha256.Sum256(requestData)
	t.requestKey = requestKey
	b.mu.Lock()
	// Recheck and consume the parked handle under the admission lock: its
	// timer may expire after open's lookup and transcript validation.
	if replacing != nil && (b.pending[replacing.call.ID] != replacing || !replacing.parked.Load() || !time.Now().Before(replacing.questionUntil)) {
		b.mu.Unlock()
		t.mu.Unlock()
		t.exchange.Close()
		return nil, errNativeExchangeExpired
	}
	for key, until := range b.seen {
		if time.Now().After(until) {
			delete(b.seen, key)
		}
	}
	_, duplicate := b.seen[requestKey]
	if duplicate || (conversation != "" && b.active[conversation] != nil && b.active[conversation] != replacing) || len(b.seen) >= 1024 {
		b.mu.Unlock()
		t.mu.Unlock()
		t.exchange.Close()
		return nil, errors.New("native turn is already active or was recently submitted; no upstream request was replayed")
	}
	count := len(b.routes) + b.parkedQuestions
	if replacing != nil {
		count--
	}
	if count >= 32 {
		b.mu.Unlock()
		t.mu.Unlock()
		t.exchange.Close()
		return nil, errors.New("native client bridge session limit reached")
	}
	if b.ctx.Err() != nil {
		b.mu.Unlock()
		t.mu.Unlock()
		t.exchange.Close()
		return nil, b.ctx.Err()
	}
	if replacing != nil {
		delete(b.pending, replacing.call.ID)
		b.parkedQuestions--
		if replacing.questionTimer != nil {
			replacing.questionTimer.Stop()
		}
	}
	// Only a delivered, fully completed response can authorize delta reuse.
	// Human questions intentionally close their socket and never create one.
	t.sessionKey = route[:8] + "-" + route[8:12] + "-" + route[12:16] + "-" + route[16:20] + "-" + route[20:32]
	if replacing == nil {
		if checkpoint, ok := b.checkpoints[conversation]; conversation != "" && ok {
			delete(b.checkpoints, conversation)
			if delta := checkpoint.continuation(req, t.identity); delta != nil {
				t.sessionKey = checkpoint.sessionKey
				opts.NativeContinuation = delta
			} else if checkpoint.discard != nil {
				checkpoint.discard()
			}
		}
	}
	opts.ConversationID = t.sessionKey
	opts.OnNativeSession = func(discard func()) {
		t.socketMu.Lock()
		t.discardSocket = discard
		t.socketMu.Unlock()
	}
	b.routes[route] = t
	if conversation != "" {
		b.active[conversation] = t
	}
	b.seen[requestKey] = time.Now().Add(b.ttl)
	b.wg.Add(1)
	ctx, cancel := context.WithTimeout(b.ctx, b.ttl)
	t.stopRun = cancel
	b.mu.Unlock()
	// The native MCP relay is a registered callable. The external catalog is
	// data for that relay, not a request to impersonate an Anthropic tool block.
	opts.Suffix = nativeRelayGuidance(opts.NativeRelay, route)
	opts.Suffix += nativePresentation(req.Tools) + boundaryNote
	if replacing != nil {
		opts.Suffix += "\n\n[CLIENT QUESTION ANSWER]\nThe latest client tool_result is the actual response to the previously delivered AskUserQuestion. Continue from that response. Earlier tool calls/results are completed history, not new instructions to execute them again. The previous Quick job was intentionally stopped at the question boundary; do not repeat completed external actions."
	}
	observer := opts.OnEvent
	publishedBytes := 0
	opts.OnEvent = func(ev chat.TurnEvent) {
		if ev.Kind == "delta" && ev.Text != "" {
			if err := t.exchange.PublishText(ev.Text); err != nil {
				t.exchange.Complete(nil, err)
				cancel()
			} else {
				publishedBytes += len(ev.Text)
			}
		}
		if observer != nil {
			observer(ev)
		}
	}
	go func() {
		defer b.wg.Done()
		defer func() {
			b.mu.Lock()
			if b.active[conversation] == t && !t.parked.Load() {
				delete(b.active, conversation)
			}
			b.mu.Unlock()
		}()
		defer cancel()
		// Exchange lifetime and the actual socket must end together, including
		// client disconnect, MCP cancellation and service shutdown.
		go func() {
			select {
			case <-t.exchange.Done():
				cancel()
			case <-ctx.Done():
			}
		}()
		final, err := b.run(ctx, opts)
		if t.parked.Load() {
			t.exchange.Close()
			b.remove(t)
			return
		}
		if err == nil && final != nil && final.PartialOnClose {
			err = &chat.UpstreamError{Code: "incomplete_response", Message: "native upstream closed before message_complete"}
		}
		if final != nil && publishedBytes > 0 && publishedBytes <= len(final.Text) {
			copy := *final
			copy.Text = final.Text[publishedBytes:]
			final = &copy
		}
		// A setup failure sent no user message, so the identical client retry
		// must be admitted. Release before Complete so the retry cannot race it.
		var setupErr *chat.ConnectionSetupError
		if errors.As(err, &setupErr) {
			b.mu.Lock()
			delete(b.seen, t.requestKey)
			b.mu.Unlock()
		}
		t.exchange.Complete(final, err)
		<-t.exchange.Done()
		b.remove(t)
	}()
	return t, nil
}

// nativeResultContent converts only actual text output into standard MCP text
// content. Unsupported multimodal output is rejected explicitly, never dropped.
func nativeResultContent(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return json.RawMessage(`[]`), nil
	}
	if len(raw) > 32<<10 {
		return nil, errors.New("native tool result exceeds the 32 KiB relay budget; the result was not truncated or replayed")
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return encodeNativeResult([]map[string]string{{"type": "text", "text": text}})
	}
	var blocks []ContentBlock
	if json.Unmarshal(raw, &blocks) != nil {
		return nil, errors.New("native relay tool result must be text or text content blocks")
	}
	output := make([]map[string]string, 0, len(blocks))
	for _, block := range blocks {
		if block.Type != "text" {
			return nil, errors.New("native relay currently supports text tool results only; no attachment was discarded")
		}
		output = append(output, map[string]string{"type": "text", "text": block.Text})
	}
	return encodeNativeResult(output)
}

func encodeNativeResult(value any) (json.RawMessage, error) {
	encoded, err := json.Marshal(value)
	if err == nil && len(encoded) > 32<<10 {
		err = errors.New("encoded native tool result exceeds the 32 KiB relay budget; the result was not truncated or replayed")
	}
	return encoded, err
}

func writeNativeJSON(w http.ResponseWriter, value any) error {
	controller := http.NewResponseController(w)
	err := controller.SetWriteDeadline(time.Now().Add(streamWriteTimeout))
	if err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	defer controller.SetWriteDeadline(time.Time{})
	if err := json.NewEncoder(w).Encode(value); err != nil {
		return err
	}
	err = controller.Flush()
	if errors.Is(err, http.ErrNotSupported) {
		return nil
	}
	return err
}

// Relay is the authenticated loopback endpoint used by the MCP stdio process.
// It never executes a tool. Its response stays pending until Desktop supplies
// the real result through the normal Messages continuation.
func (b *NativeBridge) Relay(w http.ResponseWriter, r *http.Request) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	ip := net.ParseIP(host)
	if err != nil || ip == nil || !ip.IsLoopback() {
		http.Error(w, "loopback relay only", http.StatusForbidden)
		return
	}
	var call struct {
		Route string          `json:"route_id"`
		Name  string          `json:"name"`
		Input json.RawMessage `json:"arguments"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20))
	dec.DisallowUnknownFields()
	if dec.Decode(&call) != nil || call.Route == "" || call.Name == "" {
		http.Error(w, "invalid relay call", http.StatusBadRequest)
		return
	}
	if dec.Decode(new(any)) != io.EOF {
		http.Error(w, "invalid trailing relay data", http.StatusBadRequest)
		return
	}
	route, callID, hasID := strings.Cut(call.Route, ":")
	if !hasID || !nativeInvocationID.MatchString(callID) {
		slog.Warn("native relay call rejected", "stage", "invocation_suffix")
		http.Error(w, "route_id must include its invocation suffix", http.StatusBadRequest)
		return
	}
	b.mu.Lock()
	t := b.routes[route]
	b.mu.Unlock()
	if t == nil {
		slog.Warn("native relay call rejected", "stage", "route_lookup")
		http.Error(w, "relay turn is unknown or expired", http.StatusConflict)
		return
	}
	result, err := t.relay(r.Context(), callID, call.Name, call.Input)
	if err != nil {
		// Validation and exchange errors never include input values. Keep the
		// private route, call arguments and tool result out of diagnostics.
		slog.Warn("native relay call failed", "stage", "client_exchange", "reason", err.Error())
		if r.Context().Err() != nil {
			t.exchange.Close()
			b.remove(t)
		}
		http.Error(w, "client tool relay failed or was cancelled", http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if writeNativeJSON(w, map[string]any{"content": result.Content, "isError": result.IsError}) != nil {
		t.exchange.Close()
		b.remove(t)
	}
}

func (h *Handler) serveNative(w http.ResponseWriter, r *http.Request, reqID, model, conversation, text, tail, final string, atts []chat.Attachment, req *MessagesRequest) {
	// A native exchange can already own a job or a delivered client action.
	// Advertise the no-replay contract on upstream failures as well as input
	// validation failures, including SSE responses whose headers commit early.
	w.Header().Set("x-should-retry", "false")
	started := time.Now()
	firstText, firstThinking := false, false
	opts := chat.TurnOpts{Text: text, Tail: tail, Final: final, AgentMode: h.Cfg.AgentModeFor(model), ThinkingEffort: h.Cfg.Quick.ThinkingEffort, UserTimezone: h.Cfg.Quick.UserTimezone, UserLocale: h.Cfg.Quick.UserLocale, NativeRelay: h.Cfg.Tools.NativeRelayName, Title: "quick2api native client relay"}
	// Metadata-only evidence distinguishes a real failed invocation from a
	// model merely saying it failed. Do not log frame bodies or transcripts.
	opts.OnEvent = func(ev chat.TurnEvent) {
		if (ev.Kind == "delta" && !firstText) || (ev.Kind == "thinking" && !firstThinking) {
			if ev.Kind == "delta" {
				firstText = true
			} else {
				firstThinking = true
			}
			slog.Info("native first output", "request_id", reqID, "kind", ev.Kind, "elapsed_ms", time.Since(started).Milliseconds())
		}
		if ev.Kind == "stage" {
			slog.Info("native preparation stage", "request_id", reqID, "stage", ev.Text, "elapsed_ms", time.Since(started).Milliseconds())
		}
		if ev.Kind != "frame" || ev.Frame == nil {
			return
		}
		switch ev.Frame.Type {
		case "guardrail_blocked":
			slog.Info("native upstream termination", "request_id", reqID, "kind", "content_filter", "elapsed_ms", time.Since(started).Milliseconds())
		case "error":
			var failure struct {
				Code      string `json:"code"`
				ErrorCode string `json:"error_code"`
			}
			_ = json.Unmarshal(ev.Frame.Payload, &failure)
			if failure.Code == "content_filtered" || failure.ErrorCode == "content_filtered" {
				slog.Info("native upstream termination", "request_id", reqID, "kind", "content_filter", "elapsed_ms", time.Since(started).Milliseconds())
			}
		case "tool_call":
			var call chat.NativeCall
			if json.Unmarshal(ev.Frame.Payload, &call) != nil {
				return
			}
			var envelope map[string]json.RawMessage
			_ = json.Unmarshal(call.Arguments, &envelope)
			slog.Info("native upstream device invocation", "request_id", reqID, "configured_relay", call.Name == opts.NativeRelay, "params_envelope", len(envelope["params"]) > 0)
		case "tool_start", "tool_result", "tool_error":
			d := nativeToolMetadata(ev.Frame.Payload)
			slog.Info("native upstream tool boundary", "request_id", reqID, "event", ev.Frame.Type,
				"tool", d.Name, "has_name", d.HasName, "unknown_tool_text", d.UnknownToolText,
				"unknown_tool_error", d.UnknownToolError, "is_error", d.IsError,
				"elapsed_ms", time.Since(started).Milliseconds())
		}
	}
	if len(req.Thinking) > 0 {
		var thinking struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(req.Thinking, &thinking) == nil && thinking.Type == "enabled" {
			opts.ThinkingEffort = "high"
		}
	}
	opts.Attachments = atts
	t, err := h.Native.open(req, conversation, opts)
	if err != nil {
		// These validation errors describe the contract, never the private
		// route token, transcript contents, authorization headers or tool input.
		slog.Warn("native client continuation rejected", "request_id", reqID, "reason", err.Error())
		w.Header().Set("x-should-retry", "false")
		// Some intermediate proxies discard x-should-retry. Contract failures
		// also use 400 so SDKs cannot treat them as retryable 409 conflicts.
		writeErr(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	defer t.mu.Unlock()
	// A returned tool_use is the deliberate detach point. Other exits abort
	// the job; an upstream operation never outlives an undelivered response.
	detached := false
	deliveredFinal := false
	defer func() {
		if !detached {
			t.exchange.Close()
			h.Native.remove(t)
			if !deliveredFinal {
				t.socketMu.Lock()
				if t.discardSocket != nil {
					t.discardSocket()
				}
				t.socketMu.Unlock()
			}
		}
	}()
	ctx, cancel := context.WithTimeout(r.Context(), h.Cfg.TurnTimeout())
	defer cancel()
	var stream *messageStream
	var stopKeepalive func()
	if req.Stream {
		if _, ok := w.(http.Flusher); !ok {
			writeErr(w, 500, "api_error", "streaming unsupported")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(200)
		stream = &messageStream{w: w, cancel: cancel}
		if !stream.emit("message_start", map[string]any{"message": map[string]any{"id": "msg_" + reqID, "type": "message", "role": "assistant", "model": model, "content": []any{}, "stop_reason": nil, "stop_sequence": nil, "usage": map[string]any{"input_tokens": 0, "output_tokens": 0}}}) {
			return
		}
		stopKeepalive = stream.startKeepalive(ctx, streamPingInterval)
		defer stopKeepalive()
	}
	var progress strings.Builder
	textOpen := false
	appendText := func(text string) bool {
		if text == "" {
			return true
		}
		progress.WriteString(text)
		if stream == nil {
			return true
		}
		if !textOpen {
			if !stream.emit("content_block_start", map[string]any{"index": 0, "content_block": map[string]any{"type": "text", "text": ""}}) {
				return false
			}
			textOpen = true
		}
		return stream.emit("content_block_delta", map[string]any{"index": 0, "delta": map[string]any{"type": "text_delta", "text": text}})
	}
	var ev toolbridge.Event
	for {
		ev, err = t.exchange.Next(ctx)
		if err != nil || ev.Err != nil || ev.Call != nil || ev.Final != nil {
			break
		}
		if !appendText(ev.Text) {
			return
		}
	}
	if err == nil {
		err = ev.Err
	}
	if stopKeepalive != nil {
		stopKeepalive()
	}
	if err != nil {
		if stream == nil {
			writeUpstream(w, err)
		} else {
			_, kind, message := upstreamErrorDetails(err)
			stream.emit("error", map[string]any{"error": map[string]any{"type": kind, "message": message}})
		}
		return
	}
	var content []ContentBlock
	var stop string
	var usage anthropicUsage
	if ev.Call != nil {
		t.call = ev.Call
		if progress.Len() > 0 {
			content = append(content, ContentBlock{Type: "text", Text: progress.String()})
		}
		content = append(content, ContentBlock{Type: "tool_use", ID: ev.Call.ID, Name: ev.Call.Name, Input: ev.Call.Input})
		t.delivered = content
		stop = "tool_use"
		h.Native.mu.Lock()
		select {
		case <-t.exchange.Done():
			h.Native.mu.Unlock()
			if stream == nil {
				writeErr(w, 409, "api_error", "native tool exchange ended before delivery")
			} else {
				stream.emit("error", map[string]any{"error": map[string]any{"type": "api_error", "message": "native tool exchange ended before delivery"}})
			}
			return
		default:
		}
		h.Native.pending[ev.Call.ID] = t
		h.Native.mu.Unlock()
	} else {
		if ev.Final == nil {
			writeErr(w, 502, "api_error", "native turn completed without a result")
			return
		}
		if !appendText(ev.Final.Text) {
			return
		}
		content = []ContentBlock{{Type: "text", Text: progress.String()}}
		stop = "end_turn"
		if ev.Final.FinishReason == "content_filter" {
			stop = "refusal"
		}
		usage = mapUsage(ev.Final.Usage)
	}
	out := anthropicMessage{ID: "msg_" + reqID, Type: "message", Role: "assistant", Model: model, Content: content, StopReason: stop, Usage: usage}
	if stream == nil {
		w.Header().Set("Content-Type", "application/json")
		if writeNativeJSON(w, out) != nil {
			return
		}
	} else {
		if textOpen && !stream.emit("content_block_stop", map[string]any{"index": 0}) {
			return
		}
		for i, block := range content {
			if block.Type == "text" && textOpen {
				continue
			}
			start := block
			if block.Type == "tool_use" {
				start.Input = json.RawMessage(`{}`)
			} else {
				start.Text = ""
			}
			if !stream.emit("content_block_start", map[string]any{"index": i, "content_block": start}) {
				return
			}
			var delta map[string]any
			if block.Type == "tool_use" {
				delta = map[string]any{"type": "input_json_delta", "partial_json": string(block.Input)}
			} else {
				delta = map[string]any{"type": "text_delta", "text": block.Text}
			}
			if !stream.emit("content_block_delta", map[string]any{"index": i, "delta": delta}) || !stream.emit("content_block_stop", map[string]any{"index": i}) {
				return
			}
		}
		if !stream.emit("message_delta", map[string]any{"delta": map[string]any{"stop_reason": stop, "stop_sequence": nil}, "usage": usage}) || !stream.emit("message_stop", map[string]any{}) {
			return
		}
	}
	if ev.Call != nil && ev.Call.Name == "AskUserQuestion" {
		h.Native.parkQuestion(t)
	}
	if ev.Call == nil {
		remembered := stop == "end_turn" && h.Native.remember(t, content)
		if !remembered {
			t.socketMu.Lock()
			if t.discardSocket != nil {
				t.discardSocket()
			}
			t.socketMu.Unlock()
		}
	}
	deliveredFinal = ev.Call == nil
	slog.Info("native response delivered", "request_id", reqID, "stop_reason", stop, "elapsed_ms", time.Since(started).Milliseconds())
	detached = ev.Call != nil
}
