package mcprelay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

type protocolWriter struct{ lines chan []byte }

func (w *protocolWriter) Write(data []byte) (int, error) {
	w.lines <- append([]byte(nil), data...)
	return len(data), nil
}

type harness struct {
	t      *testing.T
	input  *io.PipeWriter
	output *protocolWriter
	done   chan error
	cancel context.CancelFunc
	once   sync.Once
}

func newHarness(t *testing.T, callback Callback) *harness {
	t.Helper()
	reader, writer := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	h := &harness{t: t, input: writer, output: &protocolWriter{lines: make(chan []byte, 100)}, done: make(chan error, 1), cancel: cancel}
	go func() { h.done <- Serve(ctx, reader, h.output, callback) }()
	t.Cleanup(func() { h.close() })
	return h
}
func (h *harness) close() {
	h.once.Do(func() {
		h.cancel()
		_ = h.input.Close()
		select {
		case err := <-h.done:
			if err != nil {
				h.t.Errorf("Serve cleanup: %v", err)
			}
		case <-time.After(2 * time.Second):
			h.t.Error("Serve leaked after cancellation")
		}
	})
}
func (h *harness) send(line string) {
	h.t.Helper()
	if _, err := io.WriteString(h.input, line+"\n"); err != nil {
		h.t.Fatal(err)
	}
}
func (h *harness) read() map[string]json.RawMessage {
	h.t.Helper()
	select {
	case line := <-h.output.lines:
		if bytes.Count(line, []byte("\n")) != 1 {
			h.t.Fatalf("not one NDJSON message: %q", line)
		}
		var message map[string]json.RawMessage
		if err := json.Unmarshal(line, &message); err != nil {
			h.t.Fatal(err)
		}
		if string(message["jsonrpc"]) != `"2.0"` {
			h.t.Fatal("missing JSON-RPC version")
		}
		return message
	case <-time.After(2 * time.Second):
		h.t.Fatal("protocol response timeout")
		return nil
	}
}
func (h *harness) init() {
	h.t.Helper()
	h.send(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}}`)
	r := h.read()
	if len(r["error"]) > 0 {
		h.t.Fatalf("initialize failed: %s", r["error"])
	}
	h.send(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)
}
func callLine(id string) string {
	return `{"jsonrpc":"2.0","id":` + id + `,"method":"tools/call","params":{"name":"quick2api_client_call","arguments":{"route_id":"fixture-route:invocation","name":"Read","arguments":{"file_path":"fixed-fixture"}}}}`
}
func noCall(context.Context, string, string, json.RawMessage) (json.RawMessage, error) {
	return nil, errors.New("unexpected callback")
}
func assertCode(t *testing.T, m map[string]json.RawMessage, code int) {
	t.Helper()
	var e struct {
		Code int `json:"code"`
	}
	if err := json.Unmarshal(m["error"], &e); err != nil || e.Code != code {
		t.Fatalf("error=%s, want %d", m["error"], code)
	}
}

func TestLifecycleAndToolDefinition(t *testing.T) {
	h := newHarness(t, noCall)
	h.send(callLine(`"before-init"`))
	assertCode(t, h.read(), -32600)
	h.init()
	h.send(`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`)
	var result struct {
		Tools []struct {
			Name   string `json:"name"`
			Schema struct {
				Required   []string `json:"required"`
				Additional bool     `json:"additionalProperties"`
			} `json:"inputSchema"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(h.read()["result"], &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Tools) != 1 || result.Tools[0].Name != ToolName || len(result.Tools[0].Schema.Required) != 3 || result.Tools[0].Schema.Additional {
		t.Fatalf("bad tool definition: %+v", result)
	}
	h.send(`{"jsonrpc":"2.0","id":3,"method":"ping"}`)
	if string(h.read()["result"]) != "{}" {
		t.Fatal("ping result")
	}
}

func TestCallWaitsForRealResultAndPreservesIt(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	h := newHarness(t, func(ctx context.Context, route, name string, args json.RawMessage) (json.RawMessage, error) {
		if route != "fixture-route:invocation" || name != "Read" || string(args) != `{"file_path":"fixed-fixture"}` {
			return nil, errors.New("wrong forwarded parameters")
		}
		entered <- struct{}{}
		select {
		case <-release:
			return json.RawMessage(`{"content":[{"type":"text","text":"ACTUAL_FIXTURE"}],"isError":false,"_meta":{"source":"client"}}`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	h.init()
	h.send(callLine(`"request-a"`))
	<-entered
	select {
	case line := <-h.output.lines:
		t.Fatalf("returned before client result: %s", line)
	default:
	}
	close(release)
	response := h.read()
	if string(response["id"]) != `"request-a"` || !bytes.Contains(response["result"], []byte(`"ACTUAL_FIXTURE"`)) || !bytes.Contains(response["result"], []byte(`"source":"client"`)) {
		t.Fatalf("lost real result: %s", response["result"])
	}
}

func TestConcurrentRequestsDuplicateIDAndCancellation(t *testing.T) {
	entered := make(chan struct{}, 32)
	h := newHarness(t, func(ctx context.Context, _, _ string, _ json.RawMessage) (json.RawMessage, error) {
		entered <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	h.init()
	for i := 0; i < maxActive; i++ {
		h.send(callLine(strconvI(i + 10)))
		<-entered
	}
	h.send(callLine(`10`))
	assertCode(t, h.read(), -32600)
	h.send(callLine(`1000`))
	assertCode(t, h.read(), -32000)
	h.send(`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":10,"reason":"fixture"}}`)
	r := h.read()
	if string(r["id"]) != "10" || !bytes.Contains(r["result"], []byte(`"isError":true`)) {
		t.Fatalf("cancellation not returned: %+v", r)
	}
	// Other active calls remain independent and EOF/Close cancels them all.
	h.send(`{"jsonrpc":"2.0","id":1001,"method":"ping"}`)
	if string(h.read()["result"]) != "{}" {
		t.Fatal("cancelled sibling request")
	}
}

func TestInvalidInputCannotReachCallback(t *testing.T) {
	cases := []struct {
		line string
		code int
	}{
		{`{`, -32700},
		{`[]`, -32600},
		{`{"jsonrpc":"2.0","id":null,"method":"ping"}`, -32600},
		{`{"jsonrpc":"2.0","id":1.5,"method":"ping"}`, -32600},
		{`{"jsonrpc":"2.0","id":2,"method":"ping","unknown":true}`, -32600},
		{`{"jsonrpc":"2.0","id":2,"id":3,"method":"ping"}`, -32600},
		{`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"quick2api_client_call","arguments":{"route_id":"a","name":"Read","arguments":null}}}`, -32602},
		{`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"quick2api_client_call","arguments":{"route_id":"a","name":"Read","arguments":{},"extra":1}}}`, -32602},
		{`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"quick2api_client_call","arguments":{"route_id":"a","name":"Read","arguments":{"x":1,"x":2}}}}`, -32600},
		{`{"jsonrpc":"2.0","id":5,"method":"does/not/exist"}`, -32601},
	}
	for _, tc := range cases {
		t.Run(tc.line, func(t *testing.T) {
			h := newHarness(t, func(context.Context, string, string, json.RawMessage) (json.RawMessage, error) {
				t.Error("invalid input reached callback")
				return nil, nil
			})
			h.init()
			h.send(tc.line)
			assertCode(t, h.read(), tc.code)
		})
	}
}

func TestToolFailureAndInvalidResultAreVisible(t *testing.T) {
	for _, test := range []struct {
		name string
		body json.RawMessage
		err  error
	}{
		{"real_error", nil, errors.New("actual client denied permission")},
		{"invalid_result", json.RawMessage(`{"content":null}`), nil},
		{"null_error_flag", json.RawMessage(`{"content":[],"isError":null}`), nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newHarness(t, func(context.Context, string, string, json.RawMessage) (json.RawMessage, error) {
				return test.body, test.err
			})
			h.init()
			h.send(callLine(`"failure"`))
			r := h.read()
			if !bytes.Contains(r["result"], []byte(`"isError":true`)) {
				t.Fatalf("false success: %s", r["result"])
			}
			if test.err != nil && !bytes.Contains(r["result"], []byte(test.err.Error())) {
				t.Fatal("discarded actual failure")
			}
		})
	}
}

func TestEOFJoinsActiveCallback(t *testing.T) {
	entered := make(chan struct{})
	stopped := make(chan struct{})
	h := newHarness(t, func(ctx context.Context, _, _ string, _ json.RawMessage) (json.RawMessage, error) {
		close(entered)
		<-ctx.Done()
		close(stopped)
		return nil, ctx.Err()
	})
	h.init()
	h.send(callLine(`99`))
	<-entered
	_ = h.input.Close()
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatal(err)
		}
		h.once.Do(func() { h.cancel() })
	case <-time.After(2 * time.Second):
		t.Fatal("EOF did not stop server")
	}
	select {
	case <-stopped:
	default:
		t.Fatal("callback not joined")
	}
}

func TestOversizedLineFailsBoundedly(t *testing.T) {
	input := io.NopCloser(strings.NewReader(strings.Repeat("x", maxLine+10) + "\n"))
	if err := Serve(context.Background(), input, io.Discard, noCall); err == nil {
		t.Fatal("oversized input accepted")
	}
}

type shortWriter struct{}

func (shortWriter) Write([]byte) (int, error) { return 0, nil }

func TestOutputFailureStopsReader(t *testing.T) {
	input := io.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}` + "\n"))
	if err := Serve(context.Background(), input, shortWriter{}, noCall); err == nil {
		t.Fatal("short protocol write was ignored")
	}
}

func strconvI(i int) string {
	// Keep test request IDs numeric without involving JSON string escapes.
	data, _ := json.Marshal(i)
	return string(data)
}
