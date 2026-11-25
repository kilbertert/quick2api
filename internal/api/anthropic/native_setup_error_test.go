package anthropic

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/kilbertert/quick2api/internal/chat"
	"github.com/kilbertert/quick2api/internal/config"
)

// Adapter contract only: no simulated evidence of Quick acceptance.
func TestNativeSetupFailureDoesNotReplayAndDisablesSDKRetry(t *testing.T) {
	for _, stream := range []bool{false, true} {
		var runs atomic.Int32
		b := NewNativeBridge(context.Background(), time.Minute, func(context.Context, chat.TurnOpts) (*chat.TurnResult, error) {
			runs.Add(1)
			return nil, &chat.ConnectionSetupError{Stage: "session_connect", Err: &websocket.CloseError{Code: 1011, Text: "PRIVATE_CLOSE_REASON"}}
		})
		h := &Handler{Cfg: config.Defaults(), Native: b}
		req := nativeRequest()
		req.Stream = stream
		w := nativeHTTP(t, h, req, "setup-failure")
		b.Close()
		want := http.StatusBadGateway
		if stream {
			want = http.StatusOK
		}
		body := w.Body.String()
		if w.Code != want || w.Header().Get("x-should-retry") != "false" || runs.Load() != 1 {
			t.Fatal("native setup failure replayed or lacks no-retry header", stream, w.Code, runs.Load())
		}
		if !strings.Contains(body, "1011") || !strings.Contains(body, "no user message was sent") || strings.Contains(body, "PRIVATE") {
			t.Fatal("native setup cause was lost or leaked", stream)
		}
		if stream && (!strings.Contains(body, "event: error") || strings.Contains(body, "event: message_stop")) {
			t.Fatal("failed stream was presented as success")
		}
	}
}

// A setup failure sent no user message, so the identical client retry must be
// admitted. Any failure after a message may have been sent keeps its guard.
func TestNativeSetupFailureAllowsImmediateIdenticalRetry(t *testing.T) {
	for _, stream := range []bool{false, true} {
		var runs atomic.Int32
		b := NewNativeBridge(context.Background(), time.Minute, func(context.Context, chat.TurnOpts) (*chat.TurnResult, error) {
			if runs.Add(1) == 1 {
				return nil, &chat.ConnectionSetupError{Stage: "session_connect", Err: &websocket.CloseError{Code: 1011}}
			}
			return &chat.TurnResult{Text: "RETRY_OK", FinishReason: "stop"}, nil
		})
		h := &Handler{Cfg: config.Defaults(), Native: b}
		req := nativeRequest()
		req.Stream = stream
		first := nativeHTTP(t, h, req, "setup-retry")
		if !strings.Contains(first.Body.String(), "no user message was sent") {
			t.Fatal("first attempt did not fail at setup", stream, first.Body.String())
		}
		second := nativeHTTP(t, h, req, "setup-retry")
		b.Close()
		if second.Code != http.StatusOK || !strings.Contains(second.Body.String(), "RETRY_OK") || runs.Load() != 2 {
			t.Fatal("identical retry after setup failure was rejected", stream, second.Code, runs.Load(), second.Body.String())
		}
	}
}

func TestNativeFailureAfterMessageStillBlocksIdenticalRetry(t *testing.T) {
	var runs atomic.Int32
	b := NewNativeBridge(context.Background(), time.Minute, func(context.Context, chat.TurnOpts) (*chat.TurnResult, error) {
		runs.Add(1)
		return nil, &chat.UpstreamError{Code: "content_filter", Message: "filtered after user message"}
	})
	h := &Handler{Cfg: config.Defaults(), Native: b}
	req := nativeRequest()
	nativeHTTP(t, h, req, "post-message-failure")
	second := nativeHTTP(t, h, req, "post-message-failure")
	b.Close()
	if second.Code != http.StatusBadRequest || runs.Load() != 1 {
		t.Fatal("failure after a possible send was replayed", second.Code, runs.Load())
	}
}
