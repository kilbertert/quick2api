package anthropic

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kilbertert/quick2api/internal/chat"
	"github.com/kilbertert/quick2api/internal/config"
)

// Cache breakpoints are client transport hints, including when they appear on
// system text or a tool declaration rather than an echoed message block.
func TestNativeContinuationAcceptsPreambleCacheBreakpoints(t *testing.T) {
	h, runs := nativeFixture(t, 1, time.Minute)
	initial := nativeRequest()
	first := nativeReply(t, nativeHTTP(t, h, initial, "preamble-cache"))
	next := continueNative(t, initial, first, false)
	next.System = json.RawMessage(`[{"type":"text","text":"Fixture only","cache_control":{"type":"ephemeral","ttl":"1h"}}]`)
	var catalog []map[string]json.RawMessage
	if err := json.Unmarshal(next.Tools, &catalog); err != nil {
		t.Fatal(err)
	}
	catalog[0]["cache_control"] = json.RawMessage(`{"type":"ephemeral"}`)
	next.Tools, _ = json.Marshal(catalog)
	final := nativeReply(t, nativeHTTP(t, h, next, "preamble-cache"))
	if final.StopReason != "end_turn" || runs.Load() != 1 {
		t.Fatal("cache breakpoint restarted a live tool job")
	}
}

func TestNativeDuplicateCannotReplayThroughCacheRepresentation(t *testing.T) {
	var runs atomic.Int32
	b := NewNativeBridge(context.Background(), time.Minute, func(context.Context, chat.TurnOpts) (*chat.TurnResult, error) {
		runs.Add(1)
		return &chat.TurnResult{Text: "Completed fixture", FinishReason: "stop"}, nil
	})
	defer b.Close()
	h := &Handler{Cfg: config.Defaults(), Native: b}
	req := nativeRequest()
	_ = nativeReply(t, nativeHTTP(t, h, req, "duplicate-cache"))
	b.wg.Wait() // ensure the test exercises seen, not the active-job check
	req.Messages[0].Content = json.RawMessage(`[{"type":"text","text":"Read the approved test fixture","cache_control":{"type":"ephemeral"}}]`)
	if w := nativeHTTP(t, h, req, "duplicate-cache"); w.Code != 400 || runs.Load() != 1 {
		t.Fatal("transport-only representation change replayed a completed job", w.Code, runs.Load())
	}
}

func TestNativeIdentityPreservesCatalogDataInsideCacheNamedFields(t *testing.T) {
	a, b := nativeRequest(), nativeRequest()
	a.Tools = json.RawMessage(`[{"name":"Fixture","input_schema":{"type":"object","properties":{"cache_control":{"const":"first"},"n":{"const":9007199254740993}}},"cache_control":{"type":"ephemeral"}}]`)
	b.Tools = json.RawMessage(`[{"name":"Fixture","input_schema":{"type":"object","properties":{"cache_control":{"const":"second"},"n":{"const":9007199254740993}}}}]`)
	if nativeIdentity(&a) == nativeIdentity(&b) {
		t.Fatal("schema data mistaken for transport metadata")
	}
	b.Tools = json.RawMessage(`[{"name":"Fixture","input_schema":{"type":"object","properties":{"cache_control":{"const":"first"},"n":{"const":9007199254740992}}}}]`)
	if nativeIdentity(&a) == nativeIdentity(&b) {
		t.Fatal("large integer data changed without invalidating contract")
	}
}

func TestNativeCompletedCheckpointAcceptsPreambleCacheHints(t *testing.T) {
	var keys []string
	var continuation []*chat.NativeContinuation
	b := NewNativeBridge(context.Background(), time.Minute, func(_ context.Context, opts chat.TurnOpts) (*chat.TurnResult, error) {
		keys = append(keys, opts.ConversationID)
		continuation = append(continuation, opts.NativeContinuation)
		return &chat.TurnResult{Text: "Completed fixture", FinishReason: "stop"}, nil
	})
	defer b.Close()
	h := &Handler{Cfg: config.Defaults(), Native: b}
	req := nativeRequest()
	first := nativeReply(t, nativeHTTP(t, h, req, "checkpoint-cache"))
	raw, _ := json.Marshal(first.Content)
	req.Messages = append(req.Messages, Message{Role: "assistant", Content: raw}, Message{Role: "user", Content: json.RawMessage(`"A distinct new fixture request"`)})
	req.System = json.RawMessage(`[{"type":"text","text":"Fixture only","cache_control":{"type":"ephemeral"}}]`)
	var catalog []map[string]json.RawMessage
	_ = json.Unmarshal(req.Tools, &catalog)
	catalog[0]["cache_control"] = json.RawMessage(`{"type":"ephemeral","ttl":"1h"}`)
	req.Tools, _ = json.Marshal(catalog)
	_ = nativeReply(t, nativeHTTP(t, h, req, "checkpoint-cache"))
	if len(keys) != 2 || keys[0] != keys[1] || continuation[1] == nil || continuation[1].Text != "User: A distinct new fixture request" {
		t.Fatal("transport-only preamble change discarded a valid completed checkpoint")
	}
}

func TestNativeParkedQuestionAcceptsPreambleCacheHints(t *testing.T) {
	h, runs, req, first := nativeQuestionFixture(t, time.Minute, time.Minute)
	next := continueNative(t, req, first, false)
	next.System = json.RawMessage(`[{"type":"text","text":"Fixture only","cache_control":{"type":"ephemeral"}}]`)
	var catalog []map[string]json.RawMessage
	if err := json.Unmarshal(next.Tools, &catalog); err != nil {
		t.Fatal(err)
	}
	catalog[0]["cache_control"] = json.RawMessage(`{"type":"ephemeral","ttl":"1h"}`)
	next.Tools, _ = json.Marshal(catalog)
	last := nativeReply(t, nativeHTTP(t, h, next, "question-session"))
	if last.StopReason != "end_turn" || runs.Load() != 2 {
		t.Fatal("cache hints prevented exactly one actual question answer continuation")
	}
	if w := nativeHTTP(t, h, next, "question-session"); w.Code != 400 || runs.Load() != 2 {
		t.Fatal("question answer with cache hints was replayed")
	}
}
