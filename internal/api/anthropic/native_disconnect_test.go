package anthropic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kilbertert/quick2api/internal/chat"
	"github.com/kilbertert/quick2api/internal/config"
)

// Real local HTTP disconnect with an instrumented job, not a simulated proof
// of remote Quick cancellation. Verify lifecycle effects the live API cannot see.
func TestNativeHTTPDisconnectAfterOutputReleasesJobAndSession(t *testing.T) {
	var runs atomic.Int32
	var discarded atomic.Bool
	var firstKey string
	b := NewNativeBridge(context.Background(), time.Minute, func(ctx context.Context, opts chat.TurnOpts) (*chat.TurnResult, error) {
		if runs.Add(1) == 1 {
			firstKey = opts.ConversationID
			opts.OnNativeSession(func() { discarded.Store(true) })
			opts.OnEvent(chat.TurnEvent{Kind: "delta", Text: "LOCAL_CANCEL_FIXTURE"})
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if opts.ConversationID == firstKey || opts.NativeContinuation != nil {
			t.Error("new task reused disconnected job")
		}
		return &chat.TurnResult{Text: "LOCAL_RECOVERY_FIXTURE", FinishReason: "stop"}, nil
	})
	defer b.Close()
	h := &Handler{Cfg: config.Defaults(), Native: b}
	handlerDone := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		h.Messages(w, r)
	}))
	defer srv.Close()
	req := nativeRequest()
	req.Stream = true
	body, _ := json.Marshal(req)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r, _ := http.NewRequestWithContext(ctx, "POST", srv.URL, bytes.NewReader(body))
	r.Header.Set("X-Session-Id", "disconnect-fixture")
	res, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	reader := bufio.NewReader(res.Body)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal("no upstream fixture output before disconnect", err)
		}
		if strings.Contains(line, "LOCAL_CANCEL_FIXTURE") {
			break
		}
	}
	cancel()
	res.Body.Close()
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("HTTP handler did not finish")
	}
	joined := make(chan struct{})
	go func() { b.wg.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("disconnected native job did not stop")
	}
	b.mu.Lock()
	leaks := len(b.active) + len(b.pending) + len(b.routes) + len(b.checkpoints)
	b.mu.Unlock()
	if leaks != 0 || !discarded.Load() {
		t.Fatal("disconnect retained state or socket", leaks, discarded.Load())
	}
	req = nativeRequest()
	req.Messages[0].Content = json.RawMessage(`"A distinct task after cancellation"`)
	final := nativeReply(t, nativeHTTP(t, h, req, "disconnect-fixture"))
	if final.StopReason != "end_turn" || final.Content[0].Text != "LOCAL_RECOVERY_FIXTURE" || runs.Load() != 2 {
		t.Fatal("new task did not recover independently")
	}
}
