package chat

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/kilbertert/quick2api/internal/session"
)

// Local transport contract tests, not evidence of live Quick acceptance.
func TestHandshakeFatalFailuresDoNotAdvance(t *testing.T) {
	for _, stage := range []string{"session_connect", "session_create"} {
		for _, failure := range []string{"close", "error", "cancel"} {
			t.Run(stage+"/"+failure, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				var mu sync.Mutex
				var received []string
				serverDone := make(chan struct{})
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer close(serverDone)
					ws, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer ws.Close()
					_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
					for {
						_, data, err := ws.ReadMessage()
						if err != nil {
							return
						}
						frame := parseEnvelopePayload(data)
						if frame == nil {
							t.Error("invalid local fixture frame")
							return
						}
						mu.Lock()
						received = append(received, frame.Type)
						mu.Unlock()
						if frame.Type == stage {
							switch failure {
							case "close":
								_ = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(1011, "fixture close"))
							case "error":
								_ = ws.WriteJSON(map[string]any{"type": "error", "payload": map[string]string{"code": "fixture_error"}})
							case "cancel":
								cancel()
							}
							// Keep observing until the client closes. No later frame may
							// advance the failed stage to another session or inference.
						} else if frame.Type == "session_connect" {
							_ = ws.WriteJSON(map[string]any{"type": "session_established", "payload": map[string]any{}})
						}
					}
				}))
				defer srv.Close()
				ws, _, err := websocket.DefaultDialer.DialContext(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
				if err != nil {
					t.Fatal(err)
				}
				var stages []string
				tr := &turn{ws: ws, in: make(chan inbound, 32), done: make(chan struct{}), emit: func(ev TurnEvent) {
					if ev.Kind == "stage" {
						stages = append(stages, ev.Text)
					}
				}}
				go tr.pump()
				client := &Client{HandshakeTimeout: time.Second} // compatibility mode
				err = client.handshake(ctx, tr, &session.Session{ActorARN: "fixture"}, "fixture", "fixture", "")
				tr.close()
				<-serverDone
				if err == nil {
					t.Fatal("fatal handshake failure was ignored")
				}
				if failure == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation cause lost: %v", err)
				}
				if failure == "close" {
					var closeErr *websocket.CloseError
					if !errors.As(err, &closeErr) || closeErr.Code != 1011 {
						t.Fatalf("close cause lost: %v", err)
					}
				}
				mu.Lock()
				defer mu.Unlock()
				if slices.Contains(received, "user_message") || stage == "session_connect" && slices.Contains(received, "session_create") {
					t.Fatalf("advanced after failure: %v", received)
				}
				if stage == "session_connect" && len(stages) != 0 || slices.Contains(stages, "session_created") {
					t.Fatalf("reported failed stage as successful: %v", stages)
				}
			})
		}
	}
}

func TestHandshakeOnlyMissingAckIsOptional(t *testing.T) {
	for _, strict := range []bool{false, true} {
		var stages []string
		tr := &turn{in: make(chan inbound), emit: func(ev TurnEvent) { stages = append(stages, ev.Text) }}
		c := &Client{StrictHandshake: strict, HandshakeTimeout: time.Millisecond}
		err := c.waitForAck(context.Background(), tr, "session_connected", func(*Frame) bool { return true })
		if strict {
			if !errors.Is(err, errHandshakeTimeout) || len(stages) != 0 {
				t.Fatal("strict missing ack was accepted", err, stages)
			}
		} else if err != nil || len(stages) != 1 || stages[0] != "session_connected_unconfirmed" {
			t.Fatal("compatibility timeout misreported", err, stages)
		}
	}
	tr := &turn{in: make(chan inbound), emit: func(TurnEvent) { t.Error("closed channel reported connected") }}
	close(tr.in)
	if err := (&Client{HandshakeTimeout: time.Second}).waitForAck(context.Background(), tr, "session_connected", func(*Frame) bool { return true }); err == nil {
		t.Fatal("closed channel accepted")
	}
}

func TestHandshakeDeadlineDoesNotHideQueuedFailure(t *testing.T) {
	for _, kind := range []string{"closed", "read_error", "upstream_error"} {
		for i := 0; i < 100; i++ {
			tr := &turn{in: make(chan inbound, 3), emit: func(ev TurnEvent) {
				if ev.Kind == "stage" {
					t.Error("fatal failure advanced handshake", kind)
				}
			}}
			// A nonmatching frame may precede the failure at the deadline.
			tr.in <- inbound{f: &Frame{Type: "fixture_notice"}}
			switch kind {
			case "closed":
				close(tr.in)
			case "read_error":
				tr.in <- inbound{err: errors.New("fixture read failure")}
			case "upstream_error":
				tr.in <- inbound{f: &Frame{Type: "error"}}
			}
			err := (&Client{HandshakeTimeout: 0}).waitForAck(context.Background(), tr, "session_connected", func(f *Frame) bool { return f.Type == "session_established" })
			if err == nil || errors.Is(err, errHandshakeTimeout) {
				t.Fatal("deadline hid queued failure", kind, err)
			}
		}
	}
}

func TestHandshakeErrorFrameRetainsClassificationWithoutPayload(t *testing.T) {
	for _, key := range []string{"code", "error_code"} {
		for _, code := range []string{"unauthenticated", "token_expired", "rate_limited", "throttled", "server"} {
			tr := &turn{in: make(chan inbound, 1), emit: func(TurnEvent) {}}
			tr.in <- inbound{f: &Frame{Type: "error", Payload: []byte(`{"` + key + `":"` + code + `","message":"PRIVATE_PAYLOAD"}`)}}
			err := tr.waitFor(context.Background(), time.Second, func(*Frame) bool { return false })
			var upstream *UpstreamError
			if !errors.As(err, &upstream) || upstream.Code != code || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatal("error frame classification lost or payload exposed", key, code)
			}
		}
	}
	for _, payload := range []string{`{"code":"PRIVATE_CODE","message":"PRIVATE_MESSAGE"}`, `null`, `{"error_code":"unknown_code"}`, `{"code":{}}`} {
		tr := &turn{in: make(chan inbound, 1), emit: func(TurnEvent) {}}
		tr.in <- inbound{f: &Frame{Type: "error", Payload: []byte(payload)}}
		err := tr.waitFor(context.Background(), time.Second, func(*Frame) bool { return false })
		var upstream *UpstreamError
		if !errors.As(err, &upstream) || upstream.Code != "server" || strings.Contains(err.Error(), "PRIVATE") {
			t.Fatal("unknown error frame exposed arbitrary code or payload")
		}
	}
}
