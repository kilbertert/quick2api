// Package mcprelay exposes a single MCP tool that relays a declared client
// tool to quick2api. It never executes the requested tool in this process.
package mcprelay

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sync"
)

const (
	ToolName  = "quick2api_client_call"
	maxLine   = 4 << 20
	maxActive = 32
)

// Callback waits for the external client to execute its tool and returns a
// genuine MCP CallToolResult. The route is an opaque, request-bound handle.
type Callback func(ctx context.Context, route, name string, arguments json.RawMessage) (json.RawMessage, error)

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type server struct {
	ctx      context.Context
	cancel   context.CancelFunc
	callback Callback
	out      io.Writer
	writeMu  sync.Mutex
	mu       sync.Mutex
	active   map[string]context.CancelFunc
	wg       sync.WaitGroup
	writeErr error
}

// Serve implements newline-delimited JSON-RPC on stdio, using the MCP
// 2024-11-05 protocol. It owns in: cancellation and normal return close the
// reader; a closable output is closed too. Output contains only protocol
// messages. EOF cancels in-flight calls
// and joins their callbacks before returning; callbacks must respect ctx.
func Serve(ctx context.Context, in io.ReadCloser, out io.Writer, callback Callback) error {
	if in == nil || out == nil || callback == nil {
		return errors.New("MCP relay requires input, output and callback")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s := &server{ctx: ctx, cancel: cancel, callback: callback, out: out, active: make(map[string]context.CancelFunc)}
	readClosed := make(chan struct{})
	go func() {
		defer close(readClosed)
		<-ctx.Done()
		_ = in.Close()
		if closer, ok := out.(io.Closer); ok {
			_ = closer.Close()
		}
	}()
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 4096), maxLine+1)
	initialized, ready := false, false
	for scanner.Scan() {
		if ctx.Err() != nil {
			break
		}
		line := scanner.Bytes()
		if len(line) > maxLine {
			break
		}
		if !json.Valid(line) {
			s.failure(nil, -32700, "Parse error")
			continue
		}
		var req request
		if err := decodeStrict(line, &req); err != nil || req.JSONRPC != "2.0" || req.Method == "" {
			s.failure(nil, -32600, "Invalid JSON-RPC request")
			continue
		}
		var idKey string
		if len(req.ID) > 0 {
			var ok bool
			idKey, ok = canonicalID(req.ID)
			if !ok {
				s.failure(nil, -32600, "Request id must be a string or integer")
				continue
			}
		}
		if len(req.ID) == 0 {
			switch req.Method {
			case "notifications/initialized":
				var params struct {
					Meta json.RawMessage `json:"_meta,omitempty"`
				}
				if initialized && optionalParams(req.Params, &params) == nil {
					ready = true
				}
			case "notifications/cancelled":
				var params struct {
					RequestID json.RawMessage `json:"requestId"`
					Reason    string          `json:"reason,omitempty"`
					Meta      json.RawMessage `json:"_meta,omitempty"`
				}
				if decodeStrict(req.Params, &params) == nil {
					key, ok := canonicalID(params.RequestID)
					if ok {
						s.mu.Lock()
						stop := s.active[key]
						s.mu.Unlock()
						if stop != nil {
							stop()
						}
					}
				}
			}
			continue // Notifications never receive a response.
		}
		s.mu.Lock()
		_, duplicate := s.active[idKey]
		s.mu.Unlock()
		if duplicate {
			s.failure(req.ID, -32600, "Request id is already active")
			continue
		}
		switch req.Method {
		case "initialize":
			var params struct {
				ProtocolVersion string                     `json:"protocolVersion"`
				Capabilities    map[string]json.RawMessage `json:"capabilities"`
				ClientInfo      struct {
					Name    string `json:"name"`
					Version string `json:"version"`
				} `json:"clientInfo"`
				Meta json.RawMessage `json:"_meta,omitempty"`
			}
			if initialized || decodeStrict(req.Params, &params) != nil || params.ProtocolVersion == "" || params.Capabilities == nil || params.ClientInfo.Name == "" || params.ClientInfo.Version == "" {
				s.failure(req.ID, -32602, "Invalid initialize parameters or already initialized")
				continue
			}
			initialized = true
			s.result(req.ID, map[string]any{"protocolVersion": "2024-11-05", "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]string{"name": "quick2api-client-relay", "version": "0.1.0"}})
		case "ping":
			var params struct {
				Meta json.RawMessage `json:"_meta,omitempty"`
			}
			if optionalParams(req.Params, &params) != nil {
				s.failure(req.ID, -32602, "Invalid ping parameters")
				continue
			}
			s.result(req.ID, map[string]any{})
		case "tools/list":
			var params struct {
				Cursor string          `json:"cursor,omitempty"`
				Meta   json.RawMessage `json:"_meta,omitempty"`
			}
			if !ready || optionalParams(req.Params, &params) != nil || params.Cursor != "" {
				s.failure(req.ID, -32602, "Initialize first; no pagination cursor is supported")
				continue
			}
			s.result(req.ID, map[string]any{"tools": []any{toolDefinition()}})
		case "tools/call":
			if !ready {
				s.failure(req.ID, -32600, "MCP initialization is not complete")
				continue
			}
			var params struct {
				Name      string          `json:"name"`
				Arguments json.RawMessage `json:"arguments"`
				Meta      json.RawMessage `json:"_meta,omitempty"`
			}
			var args struct {
				Route     string          `json:"route_id"`
				Name      string          `json:"name"`
				Arguments json.RawMessage `json:"arguments"`
			}
			if decodeStrict(req.Params, &params) != nil {
				s.failure(req.ID, -32602, "Invalid tools/call parameter fields")
				continue
			}
			if params.Name != ToolName {
				s.failure(req.ID, -32602, "Unknown relay tool name")
				continue
			}
			if decodeStrict(params.Arguments, &args) != nil {
				s.failure(req.ID, -32602, "Invalid relay argument fields")
				continue
			}
			if args.Route == "" || args.Name == "" || !isObject(args.Arguments) {
				s.failure(req.ID, -32602, "Relay requires route_id, client name and object arguments")
				continue
			}
			callCtx, callCancel := context.WithCancel(ctx)
			s.mu.Lock()
			if len(s.active) >= maxActive {
				s.mu.Unlock()
				callCancel()
				s.failure(req.ID, -32000, "Too many active relay requests")
				continue
			}
			s.active[idKey] = callCancel
			s.mu.Unlock()
			s.wg.Add(1)
			go s.call(callCtx, callCancel, idKey, append(json.RawMessage(nil), req.ID...), args.Route, args.Name, append(json.RawMessage(nil), args.Arguments...))
		default:
			s.failure(req.ID, -32601, "Method not found")
		}
	}
	scanErr := scanner.Err()
	wasCancelled := ctx.Err() != nil
	cancel()
	_ = in.Close()
	<-readClosed
	s.wg.Wait()
	if s.writeErr != nil {
		return errors.New("MCP output write failed")
	}
	if scanErr != nil && !wasCancelled {
		return errors.New("MCP input read failed or message exceeds 4 MiB")
	}
	return nil
}

func (s *server) call(ctx context.Context, cancel context.CancelFunc, key string, id json.RawMessage, route, name string, arguments json.RawMessage) {
	defer s.wg.Done()
	defer cancel()
	defer func() { s.mu.Lock(); delete(s.active, key); s.mu.Unlock() }()
	result, err := s.callback(ctx, route, name, arguments)
	if s.ctx.Err() != nil {
		return
	}
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		message := err.Error()
		if ctx.Err() != nil {
			message = "Tool relay request was cancelled"
		}
		if len(message) > 1024 {
			message = message[:1024]
		}
		s.result(id, map[string]any{"content": []any{map[string]string{"type": "text", "text": message}}, "isError": true})
		return
	}
	if !validResult(result) {
		s.result(id, map[string]any{"content": []any{map[string]string{"type": "text", "text": "Tool relay returned an invalid MCP result"}}, "isError": true})
		return
	}
	s.result(id, result)
}

func (s *server) send(value any) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.writeErr != nil {
		return
	}
	data, err := json.Marshal(value)
	if err == nil {
		data = append(data, '\n')
		var written int
		written, err = s.out.Write(data)
		if err == nil && written != len(data) {
			err = io.ErrShortWrite
		}
	}
	if err != nil {
		s.writeErr = err
		s.cancel()
	}
}

func (s *server) result(id json.RawMessage, result any) {
	s.send(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}
func (s *server) failure(id json.RawMessage, code int, message string) {
	s.send(map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": code, "message": message}})
}

func toolDefinition() map[string]any {
	return map[string]any{
		"name":        ToolName,
		"description": "Request a tool declared by the current external client. The client executes it under its own permissions and returns the real result. This relay does not itself access files, terminals or SSH. Use the invocation route_id supplied for the current active request; duplicate delivery with the same route_id returns the same result without re-execution. Wait for that result before reporting completion.",
		"inputSchema": map[string]any{"type": "object", "properties": map[string]any{"route_id": map[string]string{"type": "string"}, "name": map[string]string{"type": "string"}, "arguments": map[string]string{"type": "object"}}, "required": []string{"route_id", "name", "arguments"}, "additionalProperties": false},
	}
}

var integerID = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)

func canonicalID(raw json.RawMessage) (string, bool) {
	raw = bytes.TrimSpace(raw)
	var str string
	if len(raw) > 0 && raw[0] == '"' && json.Unmarshal(raw, &str) == nil {
		data, _ := json.Marshal(str)
		return string(data), true
	}
	if integerID.Match(raw) {
		if string(raw) == "-0" {
			return "0", true
		}
		return string(raw), true
	}
	return "", false
}

func optionalParams(raw json.RawMessage, target any) error {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	return decodeStrict(raw, target)
}

func isObject(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 0 && trimmed[0] == '{' && json.Valid(trimmed)
}

func validResult(raw json.RawMessage) bool {
	if len(raw) > maxLine || !isObject(raw) || checkUnique(raw) != nil {
		return false
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return false
	}
	content := bytes.TrimSpace(fields["content"])
	if len(content) == 0 || content[0] != '[' {
		return false
	}
	if flag, ok := fields["isError"]; ok {
		flag = bytes.TrimSpace(flag)
		if !bytes.Equal(flag, []byte("true")) && !bytes.Equal(flag, []byte("false")) {
			return false
		}
	}
	return true
}

func decodeStrict(raw []byte, target any) error {
	if !isObject(raw) {
		return errors.New("expected object")
	}
	if err := checkUnique(raw); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(target)
}

// encoding/json otherwise accepts duplicate keys, which would leave routing
// and cancellation identity dependent on the parser used at either end.
func checkUnique(raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var value func(int) error
	value = func(depth int) error {
		if depth > 64 {
			return errors.New("JSON nesting limit exceeded")
		}
		token, err := dec.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := make(map[string]bool)
			for dec.More() {
				key, err := dec.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok || seen[name] {
					return errors.New("duplicate object key")
				}
				seen[name] = true
				if err = value(depth + 1); err != nil {
					return err
				}
			}
		case '[':
			for dec.More() {
				if err := value(depth + 1); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unexpected JSON delimiter")
		}
		_, err = dec.Token()
		return err
	}
	if err := value(0); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("trailing JSON value")
	}
	return nil
}
