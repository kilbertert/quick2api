package anthropic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kilbertert/quick2api/internal/chat"
	"github.com/kilbertert/quick2api/internal/config"
	"github.com/kilbertert/quick2api/internal/toolbridge"
)

const nativeCatalog = `[{"name":"Read","description":"Read a client file","input_schema":{"type":"object","properties":{"file_path":{"type":"string"}},"required":["file_path"],"additionalProperties":false}}]`
const nativeInput = `{"file_path":"D:/UserProgram/quick2api/captures/desktop-tool-check.txt"}`

func TestNativeMessagesAcceptsLargeHistoryWithoutTruncation(t *testing.T) {
	large := "BEGIN_LARGE_HISTORY\n" + strings.Repeat("x", 5<<20) + "\nEND_LARGE_HISTORY"
	cfg := config.Defaults()
	cfg.Tools.NativeRelayName = "quick2api_client_call"
	var runs atomic.Int32
	b := NewNativeBridge(context.Background(), time.Minute, func(_ context.Context, opts chat.TurnOpts) (*chat.TurnResult, error) {
		runs.Add(1)
		if !strings.Contains(opts.Text, large) {
			t.Error("history was truncated before upstream admission")
		}
		return &chat.TurnResult{Text: "LOCAL_ADAPTER_TEST_ONLY", FinishReason: "stop"}, nil
	})
	defer b.Close()
	h := &Handler{Cfg: cfg, Native: b}
	req := nativeRequest()
	encoded, err := json.Marshal(large)
	if err != nil {
		t.Fatal(err)
	}
	req.Messages = []Message{{Role: "user", Content: encoded}}
	out := nativeReply(t, nativeHTTP(t, h, req, "large-history-fixture"))
	if out.Content[0].Text != "LOCAL_ADAPTER_TEST_ONLY" || runs.Load() != 1 {
		t.Fatal("large history did not reach exactly one local test runner")
	}
}

func TestNativeReceiptBudgetRejectsBeforeClientAdmission(t *testing.T) {
	exchange := toolbridge.NewExchange(context.Background(), time.Minute, nil)
	defer exchange.Close()
	turn := &nativeTurn{exchange: exchange, receiptBytes: 4 << 20}
	if _, err := turn.relay(context.Background(), "over-budget", "Read", json.RawMessage(nativeInput)); err == nil {
		t.Fatal("receipt memory budget ignored")
	}
	if len(turn.receipts) != 0 {
		t.Fatal("over-budget receipt was admitted")
	}
}

func TestNativeDuplicateTransportsJoinOneClientAction(t *testing.T) {
	for _, isError := range []bool{false, true} {
		t.Run(fmt.Sprint(isError), func(t *testing.T) {
			exchange := toolbridge.NewExchange(context.Background(), time.Minute, nil)
			defer exchange.Close()
			turn := &nativeTurn{exchange: exchange}
			type answer struct {
				result toolbridge.Result
				err    error
			}
			answers := make(chan answer, 2)
			invoke := func() {
				result, err := turn.relay(context.Background(), "same-invocation", "Read", json.RawMessage(nativeInput))
				answers <- answer{result, err}
			}
			go invoke()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			event, err := exchange.Next(ctx)
			if err != nil || event.Call == nil {
				t.Fatalf("first action: %v", err)
			}
			go invoke()
			want := toolbridge.Result{ID: event.Call.ID, Content: json.RawMessage(`[{"type":"text","text":"ACTUAL_SINGLE_EXECUTION"}]`), IsError: isError}
			if err = exchange.Resolve(want); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				select {
				case got := <-answers:
					if got.err != nil || !canonicalEqual(got.result, want) {
						t.Fatalf("duplicate lost actual result: %+v", got)
					}
				case <-ctx.Done():
					t.Fatal("duplicate invocation stalled")
				}
			}
			cached, err := turn.relay(ctx, "same-invocation", "Read", json.RawMessage(nativeInput))
			if err != nil || !canonicalEqual(cached, want) {
				t.Fatal("completed duplicate did not return its receipt")
			}
			if _, err = turn.relay(ctx, "same-invocation", "Write", json.RawMessage(nativeInput)); err == nil {
				t.Fatal("invocation id accepted a changed tool")
			}
			if _, err = turn.relay(ctx, "same-invocation", "Read", json.RawMessage(`{"file_path":"different"}`)); err == nil {
				t.Fatal("invocation id accepted changed arguments")
			}
			probeCtx, probeCancel := context.WithTimeout(ctx, 20*time.Millisecond)
			defer probeCancel()
			if next, err := exchange.Next(probeCtx); err == nil || next.Call != nil {
				t.Fatal("duplicate delivery queued a second client action")
			}
			go func() {
				result, err := turn.relay(ctx, "new-intended-action", "Read", json.RawMessage(nativeInput))
				answers <- answer{result, err}
			}()
			next, err := exchange.Next(ctx)
			if err != nil || next.Call == nil || next.Call.ID == want.ID {
				t.Fatal("new intended invocation was suppressed")
			}
			want.ID = next.Call.ID
			if err = exchange.Resolve(want); err != nil {
				t.Fatal(err)
			}
			if got := <-answers; got.err != nil || got.result.ID != want.ID {
				t.Fatal("new invocation result failed")
			}
		})
	}
}

func nativeRequest() MessagesRequest {
	return MessagesRequest{Model: "quick", System: json.RawMessage(`"Fixture only"`), Tools: json.RawMessage(nativeCatalog), Messages: []Message{{Role: "user", Content: json.RawMessage(`"Read the approved test fixture"`)}}}
}

func nativeHTTP(t *testing.T, h *Handler, req MessagesRequest, session string) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/v1/messages", bytes.NewReader(data))
	r.Header.Set("X-Session-Id", session)
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	w := httptest.NewRecorder()
	h.Messages(w, r.WithContext(ctx))
	return w
}

func nativeReply(t *testing.T, w *httptest.ResponseRecorder) anthropicMessage {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("HTTP %d: %s", w.Code, w.Body.String())
	}
	var message anthropicMessage
	if err := json.Unmarshal(w.Body.Bytes(), &message); err != nil {
		t.Fatal(err)
	}
	return message
}

func continueNative(t *testing.T, req MessagesRequest, message anthropicMessage, isError bool) MessagesRequest {
	t.Helper()
	content, _ := json.Marshal(message.Content)
	result, _ := json.Marshal([]ContentBlock{{Type: "tool_result", ToolUseID: message.Content[0].ID, Content: json.RawMessage(`"LOCAL_STATE_FIXTURE"`), IsError: isError}})
	req.Messages = append(cloneMessages(req.Messages), Message{Role: "assistant", Content: content}, Message{Role: "user", Content: result})
	return req
}

// This fixture exercises actual local HTTP lifetimes with an injected runner.
// It is not a simulation of Quick registration, inference, or Desktop execution.
func nativeFixture(t *testing.T, count int, ttl time.Duration) (*Handler, *atomic.Int32) {
	return nativeToolFixture(t, count, ttl, "Read", json.RawMessage(nativeInput), "LOCAL_STATE_FIXTURE")
}

func nativeToolFixture(t *testing.T, count int, ttl time.Duration, name string, input json.RawMessage, resultText string) (*Handler, *atomic.Int32) {
	t.Helper()
	var url string
	var runs atomic.Int32
	b := NewNativeBridge(context.Background(), ttl, func(ctx context.Context, opts chat.TurnOpts) (*chat.TurnResult, error) {
		runs.Add(1)
		if name == "AskUserQuestion" && strings.Contains(opts.Text, `"tool_result"`) {
			if !strings.Contains(opts.Text, resultText) {
				return nil, errors.New("real question answer missing from resumed history")
			}
			return &chat.TurnResult{Text: "All local fixture results received", FinishReason: "stop"}, nil
		}
		if opts.NativeRelay != "quick2api_client_call" {
			return nil, errors.New("missing native relay option")
		}
		if strings.Contains(opts.Text, "Reply MUST be") {
			return nil, errors.New("prompt imitation contract reached native runner")
		}
		parts := strings.Split(opts.Suffix, "Its route_id base for this turn is ")
		if len(parts) != 2 {
			return nil, errors.New("missing route")
		}
		route := strings.SplitN(parts[1], ".", 2)[0]
		answers := make(chan error, count)
		for i := 0; i < count; i++ {
			go func(callID string) {
				data, _ := json.Marshal(map[string]any{"route_id": route + ":" + callID, "name": name, "arguments": input})
				req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(data))
				res, err := http.DefaultClient.Do(req)
				if err != nil {
					answers <- err
					return
				}
				defer res.Body.Close()
				var result struct {
					Content []ContentBlock `json:"content"`
					IsError bool           `json:"isError"`
				}
				if res.StatusCode != 200 {
					answers <- fmt.Errorf("relay status %d", res.StatusCode)
					return
				}
				if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
					answers <- err
					return
				}
				if len(result.Content) != 1 || result.Content[0].Text != resultText {
					answers <- errors.New("real result was changed")
					return
				}
				answers <- nil
			}(fmt.Sprintf("fixture-%d", i))
		}
		for i := 0; i < count; i++ {
			if err := <-answers; err != nil {
				return nil, err
			}
		}
		return &chat.TurnResult{Text: "All local fixture results received", FinishReason: "stop"}, nil
	})
	server := httptest.NewServer(http.HandlerFunc(b.Relay))
	url = server.URL
	cfg := config.Defaults()
	cfg.Tools.NativeRelayName = "quick2api_client_call"
	h := &Handler{Cfg: cfg, Native: b, Sem: make(chan struct{}, 1)}
	t.Cleanup(func() { b.Close(); server.Close() })
	return h, &runs
}

func TestNativeQuestionParksUpstreamUntilActualClientAnswer(t *testing.T) {
	h, runs, req, first := nativeQuestionFixture(t, 250*time.Millisecond, time.Minute)
	// Wait beyond the entire execution budget. The real question answer must
	// remain resumable without keeping the old upstream job/socket alive.
	time.Sleep(350 * time.Millisecond)
	h.Native.mu.Lock()
	turn := h.Native.pending[first.Content[0].ID]
	parked := h.Native.parkedQuestions
	routes := len(h.Native.routes)
	h.Native.mu.Unlock()
	if turn == nil || parked != 1 || routes != 0 {
		t.Fatal("human wait did not survive the execution deadline")
	}
	select {
	case <-turn.exchange.Done():
	default:
		t.Fatal("human wait retained the old upstream exchange")
	}
	assistant, _ := json.Marshal(first.Content)
	result, _ := json.Marshal([]ContentBlock{{Type: "tool_result", ToolUseID: first.Content[0].ID, Content: json.RawMessage(`"Blue"`)}})
	req.Messages = append(req.Messages, Message{Role: "assistant", Content: assistant}, Message{Role: "user", Content: result})
	altered := req
	altered.Tools = json.RawMessage(nativeCatalog)
	if w := nativeHTTP(t, h, altered, "question-session"); w.Code != 400 || runs.Load() != 1 {
		t.Fatal("changed question contract was accepted")
	}
	last := nativeReply(t, nativeHTTP(t, h, req, "question-session"))
	if last.StopReason != "end_turn" || runs.Load() != 2 || last.Content[0].Text != "All local fixture results received" {
		t.Fatal("actual question answer did not start exactly one answer continuation")
	}
	if w := nativeHTTP(t, h, req, "question-session"); w.Code != 400 || w.Header().Get("x-should-retry") != "false" || runs.Load() != 2 {
		t.Fatal("duplicate question answer started another inference")
	}
}

func nativeQuestionFixture(t *testing.T, ttl, questionTTL time.Duration) (*Handler, *atomic.Int32, MessagesRequest, anthropicMessage) {
	t.Helper()
	input := json.RawMessage(`{"questions":[{"question":"Which color?","header":"Color","multiSelect":false,"options":[{"label":"Blue","description":"Use blue"},{"label":"Green","description":"Use green"}]}]}`)
	h, runs := nativeToolFixture(t, 1, ttl, "AskUserQuestion", input, "Blue")
	h.Native.questionTTL = questionTTL
	req := nativeRequest()
	req.Tools = json.RawMessage(`[{"name":"AskUserQuestion","description":"Ask only for a decision that belongs to the user","input_schema":{"type":"object","properties":{"questions":{"type":"array","minItems":1,"maxItems":4,"items":{"type":"object","properties":{"question":{"type":"string"},"header":{"type":"string"},"multiSelect":{"type":"boolean"},"options":{"type":"array","minItems":2,"items":{"type":"object","properties":{"label":{"type":"string"},"description":{"type":"string"}},"required":["label","description"],"additionalProperties":false}}},"required":["question","header","multiSelect","options"],"additionalProperties":false}}},"required":["questions"],"additionalProperties":false}}]`)
	first := nativeReply(t, nativeHTTP(t, h, req, "question-session"))
	if first.StopReason != "tool_use" || len(first.Content) != 1 || first.Content[0].Name != "AskUserQuestion" || !canonicalEqual(first.Content[0].Input, input) {
		t.Fatal("question was not delivered as the exact declared client tool call")
	}
	h.Native.mu.Lock()
	pending := len(h.Native.pending)
	h.Native.mu.Unlock()
	if runs.Load() != 1 || pending != 1 {
		t.Fatal("question did not remain pending for the client's answer")
	}
	return h, runs, req, first
}

func TestNativeQuestionExpiryAndShutdownReleaseParkedState(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(fmt.Sprint("shutdown=", shutdown), func(t *testing.T) {
			h, runs, req, first := nativeQuestionFixture(t, time.Minute, 75*time.Millisecond)
			if shutdown {
				h.Native.Close()
			} else {
				time.Sleep(125 * time.Millisecond)
			}
			h.Native.mu.Lock()
			leaked := len(h.Native.routes)+len(h.Native.pending)+len(h.Native.active)+h.Native.parkedQuestions != 0
			h.Native.mu.Unlock()
			if leaked {
				t.Fatal("expired or closed question retained bridge state")
			}
			if w := nativeHTTP(t, h, continueNative(t, req, first, false), "question-session"); w.Code != 400 || w.Header().Get("x-should-retry") != "false" || runs.Load() != 1 {
				t.Fatal("expired question restarted inference")
			}
		})
	}
}

func TestNativeQuestionAdmissionRechecksDeadlineAndProtectsOriginalRequest(t *testing.T) {
	h, runs, req, first := nativeQuestionFixture(t, time.Minute, time.Minute)
	h.Native.mu.Lock()
	turn := h.Native.pending[first.Content[0].ID]
	// Even anonymous requests must retain their original receipt throughout
	// a human wait, after the original execution receipt would have expired.
	if !h.Native.seen[turn.requestKey].After(turn.questionUntil) {
		t.Fatal("question lost original request duplicate protection")
	}
	// Simulate validation finishing after deadline but before timer cleanup.
	turn.questionUntil = time.Now().Add(-time.Second)
	h.Native.mu.Unlock()
	if _, err := h.Native.start(&req, "question-session", chat.TurnOpts{}, turn); !errors.Is(err, errNativeExchangeExpired) || runs.Load() != 1 {
		t.Fatal("expired question admitted during timer race")
	}
}

func TestNativeNewTextAfterEndedQuestionIsFreshSubmission(t *testing.T) {
	old, _, initial, first := nativeQuestionFixture(t, time.Minute, time.Minute)
	old.Native.Close() // The new bridge has no retained handle after restart.
	req := continueNative(t, initial, first, false)
	result := ContentBlock{Type: "tool_result", ToolUseID: first.Content[0].ID, Content: json.RawMessage(`"Blue"`)}
	req.Messages[len(req.Messages)-1].Content, _ = json.Marshal([]ContentBlock{result, {Type: "text", Text: "Ask a fresh harmless color question; do nothing else"}})
	req.Messages = append(req.Messages, Message{Role: "system", Content: json.RawMessage(`"client runtime metadata"`)})
	var runs atomic.Int32
	b := NewNativeBridge(context.Background(), time.Minute, func(_ context.Context, opts chat.TurnOpts) (*chat.TurnResult, error) {
		runs.Add(1)
		if !strings.Contains(opts.Text, "fresh harmless color question") || !strings.Contains(opts.Text, `"Blue"`) || !strings.Contains(opts.Suffix, "NEW CLIENT MESSAGE AFTER ENDED QUESTION") {
			return nil, errors.New("fresh message, actual prior result or question boundary missing")
		}
		return &chat.TurnResult{Text: "Fresh request received", FinishReason: "stop"}, nil
	})
	defer b.Close()
	cfg := config.Defaults()
	cfg.Tools.NativeRelayName = "quick2api_client_call"
	h := &Handler{Cfg: cfg, Native: b}
	for _, change := range []string{"result-only", "wrong-id", "ordinary-tool", "empty-text", "image", "two-results"} {
		t.Run(change, func(t *testing.T) {
			bad := req
			bad.Messages = cloneMessages(req.Messages)
			blocks := []ContentBlock{result, {Type: "text", Text: "new message"}}
			switch change {
			case "result-only":
				blocks = blocks[:1]
			case "wrong-id":
				blocks[0].ToolUseID = "toolu_q2a_unknown"
			case "ordinary-tool":
				bad.Messages[len(bad.Messages)-3].Content, _ = json.Marshal([]ContentBlock{{Type: "tool_use", ID: result.ToolUseID, Name: "Read", Input: json.RawMessage(nativeInput)}})
			case "empty-text":
				blocks[1].Text = "  "
			case "image":
				blocks[1] = ContentBlock{Type: "image"}
			case "two-results":
				blocks = append(blocks, result)
			}
			bad.Messages[len(bad.Messages)-2].Content, _ = json.Marshal(blocks)
			if w := nativeHTTP(t, h, bad, "question-session"); w.Code != 400 || runs.Load() != 0 {
				t.Fatal("invalid ended-question recovery started inference or enabled retry")
			}
		})
	}
	got := nativeReply(t, nativeHTTP(t, h, req, "question-session"))
	if got.StopReason != "end_turn" || runs.Load() != 1 {
		t.Fatal("explicit new message after ended question was not accepted")
	}
	if w := nativeHTTP(t, h, req, "question-session"); w.Code != 400 || runs.Load() != 1 {
		t.Fatal("merged new-message retry started a second inference")
	}
}

func TestNativeOrdinaryContinuationCannotSilentlyDiscardNewText(t *testing.T) {
	h, runs := nativeFixture(t, 1, time.Minute)
	req := nativeRequest()
	first := nativeReply(t, nativeHTTP(t, h, req, "fixture-session"))
	next := continueNative(t, req, first, false)
	var blocks []ContentBlock
	_ = json.Unmarshal(next.Messages[len(next.Messages)-1].Content, &blocks)
	blocks = append(blocks, ContentBlock{Type: "text", Text: "new task"})
	next.Messages[len(next.Messages)-1].Content, _ = json.Marshal(blocks)
	if w := nativeHTTP(t, h, next, "fixture-session"); w.Code != 400 || runs.Load() != 1 {
		t.Fatal("ordinary tool silently lost appended new task")
	}
	_ = nativeReply(t, nativeHTTP(t, h, continueNative(t, req, first, false), "fixture-session"))
}

func TestNativeLiteralDecisionExampleRemainsText(t *testing.T) {
	want := "```xml\n<decision question=\"Example?\"><option>Example</option></decision>\n```"
	b := NewNativeBridge(context.Background(), time.Minute, func(context.Context, chat.TurnOpts) (*chat.TurnResult, error) {
		return &chat.TurnResult{Text: want, FinishReason: "stop"}, nil
	})
	defer b.Close()
	cfg := config.Defaults()
	cfg.Tools.NativeRelayName = "quick2api_client_call"
	h := &Handler{Cfg: cfg, Native: b}
	reply := nativeReply(t, nativeHTTP(t, h, nativeRequest(), "literal-example"))
	if reply.StopReason != "end_turn" || len(reply.Content) != 1 || reply.Content[0].Type != "text" || reply.Content[0].Text != want {
		t.Fatal("literal decision markup was rewritten or promoted into a tool call")
	}
}

func TestNativeHTTPPauseResumeKeepsOneJob(t *testing.T) {
	for _, count := range []int{1, 3} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			h, runs := nativeFixture(t, count, time.Minute)
			req := nativeRequest()
			for i := 0; i < count; i++ {
				message := nativeReply(t, nativeHTTP(t, h, req, "fixture-session"))
				if message.StopReason != "tool_use" || len(message.Content) != 1 || message.Content[0].Name != "Read" {
					t.Fatalf("expected one real relay call: %+v", message)
				}
				if len(h.Sem) != 0 {
					t.Fatal("paused native job held the HTTP concurrency slot")
				}
				req = continueNative(t, req, message, false)
			}
			message := nativeReply(t, nativeHTTP(t, h, req, "fixture-session"))
			if message.StopReason != "end_turn" || message.Content[0].Text != "All local fixture results received" || runs.Load() != 1 {
				t.Fatalf("resume started new inference: message=%+v runs=%d", message, runs.Load())
			}
			if got := nativeHTTP(t, h, req, "fixture-session"); got.Code != 400 || got.Header().Get("x-should-retry") != "false" {
				t.Fatal("duplicate result was accepted")
			}
		})
	}
}

func TestNativeRejectsDivergentContinuationWithoutConsuming(t *testing.T) {
	h, runs := nativeFixture(t, 1, time.Minute)
	initial := nativeRequest()
	message := nativeReply(t, nativeHTTP(t, h, initial, "fixture-session"))
	good := continueNative(t, initial, message, false)
	for _, change := range []string{"system", "model", "history", "call", "catalog", "new-task", "session"} {
		t.Run(change, func(t *testing.T) {
			data, _ := json.Marshal(good)
			var changed MessagesRequest
			_ = json.Unmarshal(data, &changed)
			session := "fixture-session"
			switch change {
			case "system":
				changed.System = json.RawMessage(`"Changed system"`)
			case "model":
				changed.Model = "other"
			case "history":
				changed.Messages[0].Content = json.RawMessage(`"Changed user"`)
			case "call":
				changed.Messages[1].Content = json.RawMessage(`[{"type":"tool_use","id":"wrong","name":"Read","input":{}}]`)
			case "catalog":
				changed.Tools = json.RawMessage(`[]`)
			case "new-task":
				changed.Messages = append(changed.Messages, Message{Role: "user", Content: json.RawMessage(`"new task"`)})
			case "session":
				session = "fork"
			}
			if w := nativeHTTP(t, h, changed, session); w.Code != 400 {
				t.Fatalf("changed %s returned %d: %s", change, w.Code, w.Body.String())
			}
		})
	}
	message = nativeReply(t, nativeHTTP(t, h, good, "fixture-session"))
	if message.StopReason != "end_turn" || runs.Load() != 1 {
		t.Fatal("invalid continuation consumed or replayed the pending tool")
	}
}

func TestNativeContinuationAcceptsTextRepresentationAndCacheHints(t *testing.T) {
	h, runs := nativeFixture(t, 1, time.Minute)
	initial := nativeRequest()
	message := nativeReply(t, nativeHTTP(t, h, initial, "fixture-session"))
	next := continueNative(t, initial, message, false)
	next.Messages[0].Content = json.RawMessage(`[{"type":"text","text":"Read the approved test fixture","cache_control":{"type":"ephemeral"}}]`)
	var echoed []map[string]json.RawMessage
	_ = json.Unmarshal(next.Messages[1].Content, &echoed)
	echoed[0]["cache_control"] = json.RawMessage(`{"type":"ephemeral"}`)
	next.Messages[1].Content, _ = json.Marshal(echoed)
	final := nativeReply(t, nativeHTTP(t, h, next, "fixture-session"))
	if final.StopReason != "end_turn" || runs.Load() != 1 {
		t.Fatal("transport normalization replayed the upstream job")
	}
}

func TestNativeHistoryNormalizationPreservesSemanticFields(t *testing.T) {
	left := []Message{{Role: "assistant", Content: json.RawMessage(`[{"type":"tool_use","id":"call-a","name":"Write","input":{"cache_control":"client-data","n":9007199254740993}}]`)}}
	for _, change := range []string{"client-data", "9007199254740993", "call-a", "Write"} {
		right := cloneMessages(left)
		replacement := "changed"
		if change == "9007199254740993" {
			replacement = "9007199254740992"
		}
		right[0].Content = json.RawMessage(strings.Replace(string(right[0].Content), change, replacement, 1))
		if nativeHistoryEqual(left, right) {
			t.Fatalf("changed semantic field %s accepted", change)
		}
	}
}

func TestNativeInitialDuplicateIsNotReplayed(t *testing.T) {
	for _, session := range []string{"", "fixture-session"} {
		t.Run(session, func(t *testing.T) {
			h, runs := nativeFixture(t, 1, time.Minute)
			req := nativeRequest()
			first := nativeReply(t, nativeHTTP(t, h, req, session))
			if w := nativeHTTP(t, h, req, session); w.Code != 400 {
				t.Fatal("active initial request replayed")
			}
			_ = nativeReply(t, nativeHTTP(t, h, continueNative(t, req, first, false), session))
			if w := nativeHTTP(t, h, req, session); w.Code != 400 || runs.Load() != 1 {
				t.Fatal("completed initial request replayed")
			}
		})
	}
}

func TestNativeFinalProseNeverBecomesToolCall(t *testing.T) {
	for _, finish := range []string{"stop", "content_filter"} {
		t.Run(finish, func(t *testing.T) {
			cfg := config.Defaults()
			cfg.Tools.NativeRelayName = "quick2api_client_call"
			const prose = `{"type":"tool_use","name":"Read","input":{"file_path":"not-a-call"}}`
			b := NewNativeBridge(context.Background(), time.Minute, func(context.Context, chat.TurnOpts) (*chat.TurnResult, error) {
				return &chat.TurnResult{Text: prose, FinishReason: finish}, nil
			})
			defer b.Close()
			h := &Handler{Cfg: cfg, Native: b}
			out := nativeReply(t, nativeHTTP(t, h, nativeRequest(), ""))
			if out.Content[0].Type != "text" || out.Content[0].Text != prose || out.StopReason == "tool_use" {
				t.Fatal("native prose became an executable tool")
			}
			if finish == "content_filter" && out.StopReason != "refusal" {
				t.Fatal("refusal was hidden")
			}
		})
	}
}

func TestNativeSSEToolArgumentsDeliveredOnce(t *testing.T) {
	h, _ := nativeFixture(t, 1, time.Minute)
	req := nativeRequest()
	req.Stream = true
	w := nativeHTTP(t, h, req, "fixture-session")
	if w.Code != 200 || strings.Count(w.Body.String(), "event: content_block_start") != 1 || strings.Count(w.Body.String(), "input_json_delta") != 1 || !strings.Contains(w.Body.String(), `"stop_reason":"tool_use"`) {
		t.Fatalf("bad native SSE: %s", w.Body.String())
	}
}

func TestNativeResultBudgetsAndLosslessIdentity(t *testing.T) {
	if canonicalEqual(json.RawMessage(`9007199254740992`), json.RawMessage(`9007199254740993`)) {
		t.Fatal("large numeric values became equal")
	}
	a, b := nativeRequest(), nativeRequest()
	a.Tools = json.RawMessage(`[{"limit":9007199254740992}]`)
	b.Tools = json.RawMessage(`[{"limit":9007199254740993}]`)
	if nativeIdentity(&a) == nativeIdentity(&b) {
		t.Fatal("catalog integer precision was lost")
	}
	for _, raw := range []json.RawMessage{json.RawMessage(`[{"type":"image"}]`), json.RawMessage(`"` + strings.Repeat("x", 33000) + `"`), json.RawMessage(`"` + strings.Repeat("<", 6000) + `"`)} {
		if _, err := nativeResultContent(raw); err == nil {
			t.Fatal("unsupported/oversized result accepted")
		}
	}
	if got, err := nativeResultContent(nil); err != nil || string(got) != "[]" {
		t.Fatal("empty tool result rejected")
	}
}

func TestNativeExpiryAndShutdownReleasePendingJobs(t *testing.T) {
	h, runs := nativeFixture(t, 1, 60*time.Millisecond)
	req := nativeRequest()
	first := nativeReply(t, nativeHTTP(t, h, req, "fixture-session"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	h.Native.mu.Lock()
	turn := h.Native.pending[first.Content[0].ID]
	h.Native.mu.Unlock()
	select {
	case <-turn.exchange.Done():
	case <-ctx.Done():
		t.Fatal("expired native job remained live")
	}
	if w := nativeHTTP(t, h, continueNative(t, req, first, false), "fixture-session"); w.Code != 400 || w.Header().Get("x-should-retry") != "false" || runs.Load() != 1 {
		t.Fatal("expired continuation started a fresh job")
	}
	h.Native.Close()
	h.Native.mu.Lock()
	defer h.Native.mu.Unlock()
	if len(h.Native.routes) != 0 || len(h.Native.pending) != 0 || len(h.Native.active) != 0 {
		t.Fatal("shutdown leaked native jobs")
	}
}

func TestNativeRelayRejectsRemoteAndUndeclaredTool(t *testing.T) {
	h, _ := nativeFixture(t, 1, time.Minute)
	req := nativeRequest()
	first := nativeReply(t, nativeHTTP(t, h, req, "fixture-session"))
	h.Native.mu.Lock()
	turn := h.Native.pending[first.Content[0].ID]
	h.Native.mu.Unlock()
	for _, remote := range []string{"198.51.100.1:1234", "127.0.0.1:1234"} {
		data, _ := json.Marshal(map[string]any{"route_id": turn.route + ":undeclared", "name": "Undeclared", "arguments": map[string]any{}})
		r := httptest.NewRequest("POST", "/internal/client-tool-relay", bytes.NewReader(data))
		r.RemoteAddr = remote
		w := httptest.NewRecorder()
		h.Native.Relay(w, r)
		if w.Code == 200 {
			t.Fatal("remote or undeclared relay call accepted")
		}
	}
}

func TestNativeRelayCancellationAbortsExchange(t *testing.T) {
	h, _ := nativeFixture(t, 1, time.Minute)
	req := nativeRequest()
	first := nativeReply(t, nativeHTTP(t, h, req, "fixture-session"))
	h.Native.mu.Lock()
	turn := h.Native.pending[first.Content[0].ID]
	h.Native.mu.Unlock()
	data, _ := json.Marshal(map[string]any{"route_id": turn.route + ":cancelled-request", "name": "Read", "arguments": json.RawMessage(nativeInput)})
	r := httptest.NewRequest("POST", "/internal/client-tool-relay", bytes.NewReader(data))
	r.RemoteAddr = "127.0.0.1:1234"
	ctx, cancel := context.WithCancel(r.Context())
	cancel()
	w := httptest.NewRecorder()
	h.Native.Relay(w, r.WithContext(ctx))
	select {
	case <-turn.exchange.Done():
	case <-time.After(time.Second):
		t.Fatal("MCP cancellation left the upstream job running")
	}
}

type nativeFailedWriter struct{ header http.Header }

func (w nativeFailedWriter) Header() http.Header       { return w.header }
func (w nativeFailedWriter) WriteHeader(int)           {}
func (w nativeFailedWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestNativeJSONWriteFailureIsReported(t *testing.T) {
	if err := writeNativeJSON(nativeFailedWriter{header: make(http.Header)}, map[string]any{"x": true}); err == nil {
		t.Fatal("failed native response write was ignored")
	}
}
