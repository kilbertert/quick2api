package anthropic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kilbertert/quick2api/internal/bridge"
	"github.com/kilbertert/quick2api/internal/chat"
	"github.com/kilbertert/quick2api/internal/config"
	"github.com/kilbertert/quick2api/internal/debuglog"
)

func TestStreamFailureHasNoSuccessfulMessageEnd(t *testing.T) {
	// An actually empty local pool deterministically fails before any cloud
	// connection. This checks the handler's failure wire format, not upstream
	// availability or a simulated successful model response.
	h := &Handler{Cfg: config.Defaults(), Bridge: &bridge.Pool{}, Debug: &debuglog.Logger{}}
	w := httptest.NewRecorder()
	h.stream(w, context.Background(), "failure", "quick", "smart", "", "hello", "", "", nil, "", false, "", "", nil)
	if w.Code != http.StatusOK || w.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("stream never started: %d %s", w.Code, w.Body.String())
	}
	packets := strings.Split(strings.TrimSpace(w.Body.String()), "\n\n")
	want := []string{"message_start", "content_block_start", "content_block_stop", "error"}
	if len(packets) != len(want) {
		t.Fatalf("failed stream advertised a normal completion: %s", w.Body.String())
	}
	for i, packet := range packets {
		lines := strings.Split(packet, "\n")
		var event struct {
			Type  string `json:"type"`
			Error struct {
				Type, Message string
			} `json:"error"`
		}
		if len(lines) != 2 || lines[0] != "event: "+want[i] || json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "data: ")), &event) != nil || event.Type != want[i] {
			t.Fatalf("bad failure event: %s", packet)
		}
		if event.Type == "error" && (event.Error.Type != "api_error" || !strings.Contains(event.Error.Message, "no live lanes")) {
			t.Fatalf("underlying failure was lost: %+v", event)
		}
	}
}

func TestUpstreamHTTPAndStreamErrorTypesMatch(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		typ    string
	}{
		{&chat.UpstreamError{Code: "token_expired", Message: "expired"}, 401, "authentication_error"},
		{&chat.UpstreamError{Code: "throttled", Message: "slow down"}, 429, "rate_limit_error"},
		{&chat.AttachmentPolicyError{Blocked: true, Message: "blocked"}, 403, "permission_error"},
		{&chat.AttachmentPolicyError{Message: "unchecked"}, 502, "api_error"},
		{context.DeadlineExceeded, 504, "timeout_error"},
	} {
		err := fmt.Errorf("turn: %w", tc.err)
		status, typ, message := upstreamErrorDetails(err)
		w := httptest.NewRecorder()
		writeUpstream(w, err)
		var body struct {
			Error struct{ Type, Message string }
		}
		if json.Unmarshal(w.Body.Bytes(), &body) != nil || status != tc.status || typ != tc.typ || message == "" || w.Code != status || body.Error.Type != typ || body.Error.Message != message {
			t.Fatalf("inconsistent HTTP/stream error: %d %s %s", w.Code, typ, w.Body.String())
		}
	}
}

type failingStreamWriter struct {
	header             http.Header
	writes, flushes    int
	writeErr, flushErr error
}

func (w *failingStreamWriter) Header() http.Header { return w.header }
func (w *failingStreamWriter) WriteHeader(int)     {}
func (w *failingStreamWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	return len(p), nil
}
func (w *failingStreamWriter) Flush() { _ = w.FlushError() }
func (w *failingStreamWriter) FlushError() error {
	w.flushes++
	return w.flushErr
}

func TestFailedStreamWriteCancelsUpstreamAndStopsWriting(t *testing.T) {
	for _, flushFailure := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		w := &failingStreamWriter{header: make(http.Header)}
		if flushFailure {
			w.flushErr = errors.New("disconnected during flush")
		} else {
			w.writeErr = errors.New("disconnected during write")
		}
		s := &messageStream{w: w, cancel: cancel}
		if s.emit("ping", map[string]any{}) || ctx.Err() != context.Canceled {
			t.Fatal("failed downstream write left upstream active")
		}
		writes, flushes := w.writes, w.flushes
		if s.emit("message_stop", map[string]any{}) || w.writes != writes || w.flushes != flushes {
			t.Fatal("writer retried a failed stream")
		}
		cancel()
	}
}

func TestInitialStreamFailureNeverStartsUpstream(t *testing.T) {
	// The nil bridge would panic if the handler continued after losing its
	// very first downstream packet. It must return before spending a turn.
	h := &Handler{}
	w := &failingStreamWriter{header: make(http.Header), writeErr: errors.New("gone")}
	h.stream(w, context.Background(), "gone", "quick", "smart", "", "hello", "", "", nil, "", false, "", "", nil)
	if w.writes != 1 {
		t.Fatal("continued after losing the initial stream packet")
	}
}

type notifiedStreamWriter struct {
	*httptest.ResponseRecorder
	notified chan struct{}
	once     sync.Once
}

func (w *notifiedStreamWriter) Flush() {
	w.ResponseRecorder.Flush()
	w.once.Do(func() { close(w.notified) })
}

func TestBufferedReplyKeepaliveStopsBeforeHandlerReturns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := &notifiedStreamWriter{ResponseRecorder: httptest.NewRecorder(), notified: make(chan struct{})}
	s := &messageStream{w: w, cancel: cancel}
	stop := s.startKeepalive(ctx, time.Millisecond)
	select {
	case <-w.notified:
	case <-time.After(time.Second):
		stop()
		t.Fatal("buffered reply had no heartbeat")
	}
	stop()
	stop() // handler uses both explicit stop and deferred cleanup
	before := w.Body.Len()
	if before == 0 || strings.Contains(w.Body.String(), "message_stop") {
		t.Fatal("heartbeat advertised a completed turn")
	}
	for _, packet := range strings.Split(strings.TrimSpace(w.Body.String()), "\n\n") {
		if packet != "event: ping\ndata: {\"type\":\"ping\"}" {
			t.Fatalf("malformed heartbeat: %s", packet)
		}
	}
	if !s.emit("message_stop", map[string]any{}) || w.Body.Len() <= before {
		t.Fatal("stopping keepalive also stopped the normal final events")
	}
}

func TestHeartbeatsCannotSplitConcurrentMessagePackets(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	w := httptest.NewRecorder()
	s := &messageStream{w: w, cancel: cancel}
	stop := s.startKeepalive(ctx, time.Millisecond)
	var writers sync.WaitGroup
	for i := 0; i < 100; i++ {
		writers.Add(1)
		go func(i int) {
			defer writers.Done()
			s.emit("content_block_delta", map[string]any{"index": 0, "delta": map[string]any{"type": "text_delta", "text": fmt.Sprint(i)}})
		}(i)
	}
	writers.Wait()
	cancel()
	stop()
	texts := make(map[string]bool)
	for _, packet := range strings.Split(strings.TrimSpace(w.Body.String()), "\n\n") {
		lines := strings.Split(packet, "\n")
		var data struct {
			Type  string
			Delta struct{ Type, Text string }
		}
		if len(lines) != 2 || json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "data: ")), &data) != nil || lines[0] != "event: "+data.Type {
			t.Fatalf("concurrent packets interleaved: %s", packet)
		}
		if data.Type == "content_block_delta" {
			if texts[data.Delta.Text] || data.Delta.Type != "text_delta" {
				t.Fatalf("delta duplicated or corrupted: %s", packet)
			}
			texts[data.Delta.Text] = true
		}
	}
	if len(texts) != 100 {
		t.Fatalf("lost message deltas: %d", len(texts))
	}
}
