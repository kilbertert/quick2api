package anthropic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kilbertert/quick2api/internal/chat"
	"github.com/kilbertert/quick2api/internal/config"
)

func TestNativeStreamsBeforeUpstreamCompletion(t *testing.T) {
	release := make(chan struct{})
	finished := make(chan struct{})
	b := NewNativeBridge(context.Background(), time.Minute, func(ctx context.Context, opts chat.TurnOpts) (*chat.TurnResult, error) {
		defer close(finished)
		opts.OnEvent(chat.TurnEvent{Kind: "delta", Text: "FIRST_NATIVE_TEXT"})
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		opts.OnEvent(chat.TurnEvent{Kind: "delta", Text: "_LAST"})
		return &chat.TurnResult{Text: "FIRST_NATIVE_TEXT_LAST", FinishReason: "stop"}, nil
	})
	defer b.Close()
	h := &Handler{Cfg: config.Defaults(), Native: b}
	server := httptest.NewServer(http.HandlerFunc(h.Messages))
	defer server.Close()
	req := nativeRequest()
	req.Stream = true
	body, _ := json.Marshal(req)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, "POST", server.URL, bytes.NewReader(body))
	res, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	reader := bufio.NewReader(res.Body)
	var all strings.Builder
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("no live text before completion: %v", err)
		}
		all.WriteString(line)
		if strings.Contains(line, "FIRST_NATIVE_TEXT") {
			break
		}
	}
	select {
	case <-finished:
		t.Fatal("text was buffered until completion")
	default:
	}
	close(release)
	rest, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	all.Write(rest)
	if strings.Count(all.String(), "FIRST_NATIVE_TEXT") != 1 || strings.Count(all.String(), `"text":"_LAST"`) != 1 || !strings.Contains(all.String(), "event: message_stop") {
		t.Fatalf("duplicated/missing native text or terminal boundary: %s", all.String())
	}
}

func TestNativeNarrationToolContinuationAndFinalText(t *testing.T) {
	h, _ := nativeFixture(t, 1, time.Minute)
	run := h.Native.run
	h.Native.run = func(ctx context.Context, opts chat.TurnOpts) (*chat.TurnResult, error) {
		opts.OnEvent(chat.TurnEvent{Kind: "delta", Text: "Before actual tool"})
		res, err := run(ctx, opts)
		if res != nil {
			opts.OnEvent(chat.TurnEvent{Kind: "delta", Text: res.Text})
			res.Text = "Before actual tool" + res.Text
		}
		return res, err
	}
	req := nativeRequest()
	first := nativeReply(t, nativeHTTP(t, h, req, "narrated-tool"))
	if len(first.Content) != 2 || first.Content[0].Text != "Before actual tool" || first.Content[1].Type != "tool_use" {
		t.Fatalf("narration/call ordering: %+v", first)
	}
	raw, _ := json.Marshal(first.Content)
	result, _ := json.Marshal([]ContentBlock{{Type: "tool_result", ToolUseID: first.Content[1].ID, Content: json.RawMessage(`"LOCAL_STATE_FIXTURE"`)}})
	req.Messages = append(req.Messages, Message{Role: "assistant", Content: raw}, Message{Role: "user", Content: result})
	// This accepted environment message is not delivered through the tool's
	// actual result, so a subsequent fresh turn must seed the full transcript.
	req.Messages = append(req.Messages, Message{Role: "system", Content: json.RawMessage(`"new environment context"`)})
	altered := req
	altered.Messages = cloneMessages(req.Messages)
	altered.Messages[1].Content = bytes.ReplaceAll(raw, []byte("Before actual tool"), []byte("Changed text"))
	if w := nativeHTTP(t, h, altered, "narrated-tool"); w.Code != 400 {
		t.Fatal("altered delivered narration was accepted")
	}
	last := nativeReply(t, nativeHTTP(t, h, req, "narrated-tool"))
	if last.Content[0].Text != "All local fixture results received" {
		t.Fatalf("old narration repeated after tool: %+v", last)
	}
	h.Native.mu.Lock()
	defer h.Native.mu.Unlock()
	if len(h.Native.checkpoints) != 0 {
		t.Fatal("unsent environment context was falsely checkpointed")
	}
}

func TestNativeNarratedQuestionMergedFreshText(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "parked", true: "ended-handle"}[restart], func(t *testing.T) {
			h, _ := nativeToolFixture(t, 1, time.Minute, "AskUserQuestion", json.RawMessage(`{"questions":[{"question":"Color?","header":"Color","options":[{"label":"Blue","description":"Blue"},{"label":"Green","description":"Green"}],"multiSelect":false}]}`), "Blue")
			run := h.Native.run
			h.Native.run = func(ctx context.Context, opts chat.TurnOpts) (*chat.TurnResult, error) {
				opts.OnEvent(chat.TurnEvent{Kind: "delta", Text: "Please choose"})
				res, err := run(ctx, opts)
				if res != nil {
					res.Text = "Please choose" + res.Text
				}
				return res, err
			}
			req := nativeRequest()
			req.Tools = json.RawMessage(`[{"name":"AskUserQuestion","description":"Ask the user for a color","input_schema":{"type":"object","properties":{"questions":{"type":"array"}},"required":["questions"],"additionalProperties":false}}]`)
			first := nativeReply(t, nativeHTTP(t, h, req, "narrated-question"))
			if len(first.Content) != 2 || first.Content[1].Name != "AskUserQuestion" {
				t.Fatalf("missing narrated question: %+v", first)
			}
			raw, _ := json.Marshal(first.Content)
			answer, _ := json.Marshal([]ContentBlock{{Type: "tool_result", ToolUseID: first.Content[1].ID, Content: json.RawMessage(`"Blue"`)}, {Type: "text", Text: "New explicit message"}})
			req.Messages = append(req.Messages, Message{Role: "assistant", Content: raw}, Message{Role: "user", Content: answer})
			if restart {
				h.Native.Close()
				h.Native = NewNativeBridge(context.Background(), time.Minute, h.Native.run)
				defer h.Native.Close()
			}
			last := nativeReply(t, nativeHTTP(t, h, req, "narrated-question"))
			if last.StopReason != "end_turn" {
				t.Fatal("narration prevented merged question continuation")
			}
		})
	}
}

func TestNativeFailedFinalDeliveryDiscardsCompletedSocket(t *testing.T) {
	var discarded atomic.Bool
	b := NewNativeBridge(context.Background(), time.Minute, func(_ context.Context, opts chat.TurnOpts) (*chat.TurnResult, error) {
		opts.OnNativeSession(func() { discarded.Store(true) })
		return &chat.TurnResult{Text: "completed", FinishReason: "stop"}, nil
	})
	defer b.Close()
	h := &Handler{Cfg: config.Defaults(), Native: b}
	body, _ := json.Marshal(nativeRequest())
	r := httptest.NewRequest("POST", "/v1/messages", bytes.NewReader(body))
	h.Messages(nativeFailedWriter{header: make(http.Header)}, r)
	if !discarded.Load() {
		t.Fatal("undelivered final retained a completed socket")
	}
}

func TestNativeAnonymousFinalClosesUnusableSocket(t *testing.T) {
	var discarded atomic.Bool
	b := NewNativeBridge(context.Background(), time.Minute, func(_ context.Context, opts chat.TurnOpts) (*chat.TurnResult, error) {
		opts.OnNativeSession(func() { discarded.Store(true) })
		return &chat.TurnResult{Text: "completed", FinishReason: "stop"}, nil
	})
	defer b.Close()
	h := &Handler{Cfg: config.Defaults(), Native: b}
	_ = nativeReply(t, nativeHTTP(t, h, nativeRequest(), ""))
	if !discarded.Load() {
		t.Fatal("anonymous completed socket retained without reusable checkpoint")
	}
}

func TestNativeCompletedCheckpointExactContract(t *testing.T) {
	var runs atomic.Int32
	var keys []string
	var deltas []*chat.NativeContinuation
	b := NewNativeBridge(context.Background(), time.Minute, func(_ context.Context, opts chat.TurnOpts) (*chat.TurnResult, error) {
		runs.Add(1)
		keys = append(keys, opts.ConversationID)
		deltas = append(deltas, opts.NativeContinuation)
		return &chat.TurnResult{Text: "completed", FinishReason: "stop"}, nil
	})
	defer b.Close()
	h := &Handler{Cfg: config.Defaults(), Native: b}
	req := nativeRequest()
	first := nativeReply(t, nativeHTTP(t, h, req, "checkpoint"))
	raw, _ := json.Marshal(first.Content)
	req.Messages = append(req.Messages, Message{Role: "assistant", Content: raw}, Message{Role: "user", Content: json.RawMessage(`"new task"`)})
	// Same content represented as a text block with a moved cache hint.
	req.Messages[0].Content = json.RawMessage(`[{"type":"text","text":"Read the approved test fixture","cache_control":{"type":"ephemeral"}}]`)
	second := nativeReply(t, nativeHTTP(t, h, req, "checkpoint"))
	if deltas[0] != nil || deltas[1] == nil || keys[0] != keys[1] || deltas[1].Text != "User: new task" || !strings.Contains(reqText(t, req), "completed") {
		t.Fatal("exact append did not select only the new task")
	}
	raw, _ = json.Marshal(second.Content)
	req.Messages = append(req.Messages, Message{Role: "assistant", Content: raw}, Message{Role: "user", Content: json.RawMessage(`"third task"`)})
	req.System = json.RawMessage(`"changed system"`)
	_ = nativeReply(t, nativeHTTP(t, h, req, "checkpoint"))
	if deltas[2] != nil || keys[2] == keys[1] || runs.Load() != 3 {
		t.Fatal("changed preamble reused a checkpoint")
	}
}

func reqText(t *testing.T, req MessagesRequest) string {
	t.Helper()
	text, _, _, _, err := renderNativePrompt(&req)
	if err != nil {
		t.Fatal(err)
	}
	return text
}

func TestNativeCheckpointRejectsHistoryForkExpiryAndNewAssistant(t *testing.T) {
	req := nativeRequest()
	digest, err := nativeHistoryDigest(req.Messages)
	if err != nil {
		t.Fatal(err)
	}
	c := nativeCheckpoint{identity: nativeIdentity(&req), history: digest, messages: 1, until: time.Now().Add(time.Minute)}
	for _, kind := range []string{"fork", "expiry", "assistant", "tool_catalog", "sessionless"} {
		t.Run(kind, func(t *testing.T) {
			changed := req
			changed.Messages = append(cloneMessages(req.Messages), Message{Role: "user", Content: json.RawMessage(`"new"`)})
			copy := c
			switch kind {
			case "fork":
				changed.Messages[0].Content = json.RawMessage(`"changed"`)
			case "expiry":
				copy.until = time.Now().Add(-time.Second)
			case "assistant":
				changed.Messages[1].Role = "assistant"
			case "tool_catalog":
				changed.Tools = json.RawMessage(`[]`)
			case "sessionless":
				changed.Messages = changed.Messages[:1]
			}
			if copy.continuation(&changed, nativeIdentity(&changed)) != nil {
				t.Fatal("unsafe checkpoint was accepted")
			}
		})
	}
}

func TestNativeStreamErrorDoesNotPublishSuccessfulCheckpoint(t *testing.T) {
	b := NewNativeBridge(context.Background(), time.Minute, func(_ context.Context, opts chat.TurnOpts) (*chat.TurnResult, error) {
		opts.OnEvent(chat.TurnEvent{Kind: "delta", Text: "partial"})
		return nil, errors.New("upstream failed")
	})
	defer b.Close()
	h := &Handler{Cfg: config.Defaults(), Native: b}
	req := nativeRequest()
	req.Stream = true
	w := nativeHTTP(t, h, req, "failed-stream")
	if !strings.Contains(w.Body.String(), "partial") || !strings.Contains(w.Body.String(), "event: error") || strings.Contains(w.Body.String(), "event: message_stop") {
		t.Fatal("partial failure was advertised as success")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.checkpoints) != 0 {
		t.Fatal("failed stream created checkpoint")
	}
}
