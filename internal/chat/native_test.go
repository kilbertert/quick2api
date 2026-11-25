package chat

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/kilbertert/quick2api/internal/session"
)

const relayTestName = "quick2api_client_relay__quick2api_client_call"
const relayTestCatalog = `{"servers":[{"enabled":true,"status":"connected","tools":[{"name":"quick2api_client_relay__quick2api_client_call","input_schema":{"type":"object","required":["route_id","name","arguments"],"properties":{"route_id":{"type":"string"},"name":{"type":"string"},"arguments":{"type":"object"}}}}]}]}`

func TestNativeRuntimeRequiresRealCatalogAndOwnLoopback(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodGet || r.URL.Path != "/local/mcp/list" {
			t.Error("native registration must use the MCP catalog, not built-in list_tools")
		}
		if r.Header.Get("Authorization") != "Bearer local-fixture" {
			t.Error("missing outpost authorization")
		}
		_, _ = w.Write([]byte(relayTestCatalog))
	}))
	defer srv.Close()
	sess := &session.Session{OutpostURL: srv.URL, OutpostToken: "local-fixture", OutpostID: "fixture-device"}
	if _, err := prepareNativeRuntime(context.Background(), sess, relayTestName); err != nil {
		t.Fatal(err)
	}
	for _, url := range []string{"http://127.0.0.1:8780", "https://127.0.0.1:1234", "http://example.invalid:1234", "http://127.0.0.1:1234/?other=1"} {
		copy := *sess
		copy.OutpostURL = url
		if _, err := prepareNativeRuntime(context.Background(), &copy, relayTestName); err == nil {
			t.Fatalf("accepted unsafe outpost %q", url)
		}
	}
	if _, err := prepareNativeRuntime(context.Background(), sess, "missing_quick2api_client_call"); err == nil {
		t.Fatal("missing registration accepted")
	}
	if calls.Load() != 1 {
		t.Fatal("unsafe outpost was contacted")
	}
}

func TestNativeCatalogRejectsDisconnectedDisabledAndWrongSchemas(t *testing.T) {
	for _, kind := range []string{"builtin-only", "disabled", "disconnected", "wrong-name", "wrong-type", "not-required", "wrong-property"} {
		t.Run(kind, func(t *testing.T) {
			catalog := relayTestCatalog
			switch kind {
			case "builtin-only":
				catalog = `{"tools":[{"name":"quick2api_client_relay__quick2api_client_call"}]}`
			case "disabled":
				catalog = strings.Replace(catalog, `"enabled":true`, `"enabled":false`, 1)
			case "disconnected":
				catalog = strings.Replace(catalog, `"connected"`, `"disconnected"`, 1)
			case "wrong-name":
				catalog = strings.Replace(catalog, relayTestName, "other__quick2api_client_call", 1)
			case "wrong-type":
				catalog = strings.Replace(catalog, `"type":"object"`, `"type":"array"`, 1)
			case "not-required":
				catalog = strings.Replace(catalog, `["route_id","name","arguments"]`, `["name","arguments"]`, 1)
			case "wrong-property":
				catalog = strings.Replace(catalog, `"route_id":{"type":"string"}`, `"route_id":{"type":"object"}`, 1)
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(catalog)) }))
			defer srv.Close()
			_, err := prepareNativeRuntime(context.Background(), &session.Session{OutpostURL: srv.URL, OutpostToken: "fixture", OutpostID: "fixture"}, relayTestName)
			if err == nil {
				t.Fatal("unavailable or incompatible relay accepted")
			}
		})
	}
}

func TestNativeCatalogConnectionOwnership(t *testing.T) {
	for _, kind := range []string{"disabled", "wrong-schema", "ready"} {
		t.Run(kind, func(t *testing.T) {
			var connections atomic.Int32
			closed := make(chan struct{}, 2)
			catalog := relayTestCatalog
			switch kind {
			case "disabled":
				catalog = strings.Replace(catalog, `"enabled":true`, `"enabled":false`, 1)
			case "wrong-schema":
				catalog = strings.Replace(catalog, `"route_id":{"type":"string"}`, `"route_id":{"type":"object"}`, 1)
			}
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/local/mcp/list":
					_, _ = w.Write([]byte(catalog))
				case "/local/invoke_tool":
					_, _ = w.Write([]byte(`{"content":[],"isError":false}`))
				default:
					t.Errorf("unexpected request path: %s", r.URL.Path)
				}
			}))
			srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					connections.Add(1)
				}
				if state == http.StateClosed {
					select {
					case closed <- struct{}{}:
					default:
					}
				}
			}
			srv.Start()
			defer srv.Close()
			runtime, err := prepareNativeRuntime(context.Background(), &session.Session{OutpostURL: srv.URL, OutpostToken: "fixture", OutpostID: "fixture"}, relayTestName)
			if kind == "ready" {
				if err != nil {
					t.Fatal(err)
				}
				defer runtime.client.CloseIdleConnections()
				out := runtime.invoke(context.Background(), NativeCall{ID: "fixture-call", Name: relayTestName, Arguments: json.RawMessage(`{}`)})
				if out.err != nil || len(out.result) == 0 {
					t.Fatalf("prepared runtime could not invoke its relay: %+v", out)
				}
				if connections.Load() != 1 {
					t.Fatal("successful preparation closed the caller-owned connection before invocation")
				}
				runtime.client.CloseIdleConnections()
			} else if err == nil || runtime != nil {
				t.Fatal("unavailable relay accepted")
			}
			select {
			case <-closed:
			case <-time.After(2 * time.Second):
				t.Fatal("native catalog connection remained open after its owner finished")
			}
		})
	}
}

func TestNativeCatalogWaitsForMCPStartupWithoutInvokingTools(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/local/mcp/list" {
			t.Error("startup wait invoked a tool")
		}
		if calls.Add(1) == 1 {
			_, _ = w.Write([]byte(`{"servers":[{"enabled":true,"status":"connecting","tools":[]}]}`))
			return
		}
		_, _ = w.Write([]byte(relayTestCatalog))
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := prepareNativeRuntime(ctx, &session.Session{OutpostURL: srv.URL, OutpostToken: "fixture", OutpostID: "fixture"}, relayTestName); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatal("MCP startup discovery did not settle")
	}
}

func TestNativeInvokePreservesApplicationErrorsAndNeverRetries(t *testing.T) {
	for _, kind := range []string{"application-error", "oversized", "redirect", "invalid-json", "http-error"} {
		t.Run(kind, func(t *testing.T) {
			var calls atomic.Int32
			const actual = `{"content":[{"type":"text","text":"permission denied"}],"isError":true}`
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != "/local/invoke_tool" {
					t.Error("wrong courier endpoint")
				}
				var body struct {
					Arguments json.RawMessage `json:"arguments"`
				}
				if json.NewDecoder(r.Body).Decode(&body) != nil || string(body.Arguments) != `{"params":{"route_id":"fixture:once","name":"Read","arguments":{"file_path":"fixed"}}}` {
					t.Error("the upstream MCP params envelope was changed")
				}
				switch kind {
				case "application-error":
					_, _ = w.Write([]byte(actual))
				case "oversized":
					_, _ = w.Write([]byte(strings.Repeat("x", 48001)))
				case "redirect":
					http.Redirect(w, r, "/unexpected", http.StatusTemporaryRedirect)
				case "invalid-json":
					_, _ = w.Write([]byte("not-json"))
				case "http-error":
					w.WriteHeader(503)
				}
			}))
			defer srv.Close()
			runtime := &nativeRuntime{sess: &session.Session{OutpostURL: srv.URL, OutpostToken: "local-fixture"}, name: relayTestName, client: nativeHTTPClient()}
			call := NativeCall{ID: "fixture-call", Name: relayTestName, Arguments: json.RawMessage(`{"params":{"route_id":"fixture:once","name":"Read","arguments":{"file_path":"fixed"}}}`)}
			out := runtime.invoke(context.Background(), call)
			if kind == "application-error" {
				if out.err != nil || string(out.result) != actual {
					t.Fatal("application failure became fabricated success")
				}
			} else if out.err == nil || len(out.result) != 0 {
				t.Fatal("invalid courier response accepted")
			}
			for _, unapproved := range []string{"shell", "DEVICE_A_" + relayTestName, "DEVICE_B_" + relayTestName, "other_server__quick2api_client_call", relayTestName + "_extra"} {
				call.Name = unapproved
				if out := runtime.invoke(context.Background(), call); out.err == nil {
					t.Fatal("unapproved device/callable name entered the local courier", unapproved)
				}
			}
			if calls.Load() != 1 {
				t.Fatal("native courier retried or executed an unapproved device tool")
			}
		})
	}
}

// Local wire test only: actual WebSocket and HTTP IO, with no claim that these
// fixture frames establish Quick registration or remote protocol acceptance.
func TestNativeCourierKeepsReadingWhileAwaitingRealResult(t *testing.T) {
	invoked := make(chan struct{})
	release := make(chan struct{})
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(invoked)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"ACTUAL_LOCAL_FIXTURE"}],"isError":false}`))
	}))
	defer httpServer.Close()
	ack := make(chan Frame, 1)
	serverErrors := make(chan error, 1)
	wsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			serverErrors <- err
			return
		}
		defer ws.Close()
		defer func() { serverErrors <- nil }()
		_ = ws.SetReadDeadline(time.Now().Add(3 * time.Second))
		_ = ws.WriteJSON(map[string]any{"type": "tool_call", "payload": map[string]any{"tool_call_id": "native-fixture", "tool_name": relayTestName, "arguments": map[string]any{}, "timeout_ms": 2000}})
		_ = ws.WriteJSON(map[string]any{"type": "keepalive", "payload": map[string]any{}})
		mt, data, err := ws.ReadMessage()
		if err != nil {
			t.Error(err)
			return
		}
		if mt != websocket.BinaryMessage {
			t.Error("native result was not a binary rawInputEvent")
		}
		var env struct {
			EventType string `json:"eventType"`
			Payload   struct {
				Payload string `json:"payload"`
			} `json:"payload"`
		}
		if json.Unmarshal(data, &env) != nil || env.EventType != "rawInputEvent" {
			t.Error("wrong result envelope")
			return
		}
		inner, err := base64.StdEncoding.DecodeString(env.Payload.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		var frame Frame
		if json.Unmarshal(inner, &frame) != nil {
			t.Error("invalid result frame")
			return
		}
		ack <- frame
		_ = ws.WriteJSON(map[string]any{"type": "text_delta", "payload": map[string]string{"delta": "Completed"}})
		_ = ws.WriteJSON(map[string]any{"type": "message_complete", "payload": map[string]any{}})
	}))
	defer wsServer.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(wsServer.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	tr := &turn{ws: ws, in: make(chan inbound, 32), done: make(chan struct{}), native: &nativeRuntime{sess: &session.Session{OutpostURL: httpServer.URL}, name: relayTestName, client: nativeHTTPClient()}}
	keepalive := make(chan struct{})
	tr.emit = func(ev TurnEvent) {
		if ev.Frame != nil && ev.Frame.Type == "keepalive" {
			close(keepalive)
		}
	}
	defer tr.close()
	go tr.pump()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() {
		select {
		case <-ctx.Done():
		case <-keepalive:
		}
		select {
		case <-ctx.Done():
		case <-invoked:
		}
		close(release)
	}()
	res, err := tr.collect(ctx, &TurnResult{FinishReason: "stop"})
	if err != nil || res.Text != "Completed" {
		t.Fatalf("native courier stalled: %+v %v", res, err)
	}
	frame := <-ack
	if frame.Type != "tool_result" || !strings.Contains(string(frame.Payload), "ACTUAL_LOCAL_FIXTURE") || !strings.Contains(string(frame.Payload), `"error":null`) {
		t.Fatalf("actual courier result was changed: %+v", frame)
	}
	if err := <-serverErrors; err != nil {
		t.Fatal(err)
	}
}

func TestNativeCollectCancellationJoinsCourier(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		close(started)
		// Read the complete request before waiting for cancellation, so HTTP/1
		// can observe the peer disconnect rather than an unread body.
		var body any
		_ = json.NewDecoder(r.Body).Decode(&body)
		<-r.Context().Done()
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr := &turn{in: make(chan inbound, 1), emit: func(TurnEvent) {}, native: &nativeRuntime{sess: &session.Session{OutpostURL: srv.URL}, name: relayTestName, client: nativeHTTPClient()}}
	data, _ := json.Marshal(NativeCall{ID: "cancel", Name: relayTestName, Arguments: json.RawMessage(`{}`)})
	tr.in <- inbound{f: &Frame{Type: "tool_call", Payload: data}}
	go func() { <-started; cancel() }()
	_, err := tr.collect(ctx, &TurnResult{})
	if !errors.Is(err, context.Canceled) {
		t.Fatal("native collector did not cancel")
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("cancelled courier HTTP request remained running")
	}
}
