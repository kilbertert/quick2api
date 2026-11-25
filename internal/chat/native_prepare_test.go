package chat

import (
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

	"github.com/gorilla/websocket"
	"github.com/kilbertert/quick2api/internal/session"
)

func TestNativePreparationOverlapsAndBindsOnlyAfterReady(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	text := strings.Repeat("inert context 汉字\n", 5000)
	opts := TurnOpts{NativeRelay: relayTestName, Text: text + "User: reply READY", Final: "User: reply READY", Suffix: "presentation reminder"}
	uploadStarted := make(chan struct{})
	var uploads, binds, polls, messages atomic.Int32
	var connectionReady atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/integration/quick-work/upload-attachment":
			uploads.Add(1)
			body, _ := io.ReadAll(r.Body)
			if string(body) != strings.TrimSpace(text) || r.URL.Query().Get("name") != "context.txt" {
				t.Error("uploaded context changed or includes the operative message")
			}
			close(uploadStarted)
			_, _ = w.Write([]byte(`{"attachmentId":"fixture-id"}`))
		case "/integration/quick-work/attachment-dlp-status":
			polls.Add(1)
			_, _ = w.Write([]byte(`{"policyAction":"ALLOW"}`))
		case "/integration/quick-work/qbs-event":
			binds.Add(1)
			body, _ := io.ReadAll(r.Body)
			if !connectionReady.Load() || !strings.Contains(string(body), "files/upload/fresh-fixture") {
				t.Error("attachment bound before conversation ready or to wrong conversation")
			}
			_, _ = w.Write([]byte(`{"eventResponse":{"path":"/fixture/context.txt"}}`))
		case "/local/mcp/list":
			_, _ = w.Write([]byte(relayTestCatalog))
		default:
			t.Error("unexpected HTTP path", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	wsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer ws.Close()
		_ = ws.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, data, err := ws.ReadMessage()
		if err != nil {
			t.Error(err)
			return
		}
		frame := parseEnvelopePayload(data)
		var payload struct {
			Text        string `json:"text"`
			Attachments []struct {
				Path string `json:"path"`
				Size int    `json:"size"`
			} `json:"attachments"`
		}
		if frame == nil || frame.Type != "user_message" || json.Unmarshal(frame.Payload, &payload) != nil {
			t.Error("wrong outgoing frame")
			return
		}
		messages.Add(1)
		if !strings.Contains(payload.Text, opts.Final+"\n\n"+opts.Suffix) || len(payload.Attachments) != 1 || payload.Attachments[0].Path != "/fixture/context.txt" || payload.Attachments[0].Size != len(strings.TrimSpace(text)) {
			t.Error("prepared context or inline message was lost")
		}
		_ = ws.WriteJSON(map[string]any{"type": "text_delta", "payload": map[string]string{"delta": "READY"}})
		_ = ws.WriteJSON(map[string]any{"type": "message_complete", "payload": map[string]any{}})
	}))
	defer wsSrv.Close()
	sess := &session.Session{IDToken: "fixture", TenantURL: srv.URL, OutpostURL: srv.URL, OutpostID: "fixture", OutpostToken: "fixture"}
	client := &Client{}
	cn, err := prepareNativeConversation(ctx, sess, opts, func(ctx context.Context, emit func(TurnEvent)) (*Conn, error) {
		// A sequential implementation cannot reach uploadStarted until this
		// returns. The shared deadline makes that regression fail boundedly.
		select {
		case <-uploadStarted:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if binds.Load() != 0 || messages.Load() != 0 {
			t.Error("preparation bound input or started inference early")
		}
		ws, _, err := websocket.DefaultDialer.DialContext(ctx, "ws"+strings.TrimPrefix(wsSrv.URL, "http"), nil)
		if err != nil {
			return nil, err
		}
		tr := &turn{ws: ws, in: make(chan inbound, 32), emit: emit, done: make(chan struct{})}
		conn := &Conn{c: client, t: tr, convID: "fresh-fixture", sess: sess}
		go conn.pump(tr)
		connectionReady.Store(true)
		return conn, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cn.Close()
	res, err := cn.Send(ctx, sess, opts)
	if err != nil || res == nil || res.Text != "READY" || uploads.Load() != 1 || polls.Load() != 1 || binds.Load() != 1 || messages.Load() != 1 {
		t.Fatalf("preparation duplicated or dropped work: result=%+v error=%v uploads=%d polls=%d binds=%d messages=%d", res, err, uploads.Load(), polls.Load(), binds.Load(), messages.Load())
	}
}

func TestNativePreparationFailureCancelsAndJoinsSibling(t *testing.T) {
	for _, mode := range []string{"policy_denied", "connect_failed", "caller_cancelled"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			started := make(chan struct{})
			finished := make(chan struct{})
			var binds atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/integration/quick-work/upload-attachment":
					_, _ = io.Copy(io.Discard, r.Body)
					close(started)
					if mode != "policy_denied" {
						defer close(finished)
						<-r.Context().Done()
						return
					}
					_, _ = w.Write([]byte(`{"attachmentId":"denied"}`))
				case "/integration/quick-work/attachment-dlp-status":
					_, _ = w.Write([]byte(`{"policyAction":"BLOCK","dlpJobStatus":"SKIPPED"}`))
				default:
					binds.Add(1)
				}
			}))
			defer srv.Close()
			sess := &session.Session{IDToken: "fixture", TenantURL: srv.URL}
			openFinished := false
			connectErr := errors.New("fixture dial failure")
			cn, err := prepareNativeConversation(ctx, sess, TurnOpts{NativeRelay: relayTestName, Text: strings.Repeat("inert", 20000)}, func(ctx context.Context, _ func(TurnEvent)) (*Conn, error) {
				defer func() { openFinished = true }()
				select {
				case <-started:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				if mode == "connect_failed" {
					return nil, connectErr
				}
				if mode == "caller_cancelled" {
					cancel()
				}
				<-ctx.Done()
				return nil, ctx.Err()
			})
			if cn != nil || err == nil || !openFinished || binds.Load() != 0 {
				t.Fatal("failed setup left a connection, binding, or unjoined operation")
			}
			if mode == "policy_denied" {
				var policy *AttachmentPolicyError
				if !errors.As(err, &policy) || !policy.Blocked {
					t.Fatal("lost actual policy denial", err)
				}
			} else {
				select {
				case <-finished:
				case <-time.After(time.Second):
					t.Fatal("cancelled upload HTTP request remained alive")
				}
				if mode == "connect_failed" && !errors.Is(err, connectErr) {
					t.Fatal("lost actual connect error", err)
				}
			}
		})
	}
}
