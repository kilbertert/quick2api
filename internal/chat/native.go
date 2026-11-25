package chat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/kilbertert/quick2api/internal/session"
)

// NativeCall is the device-courier frame emitted by Quick, not a tool_start
// progress notification and not assistant-generated JSON.
type NativeCall struct {
	ID        string          `json:"tool_call_id"`
	Name      string          `json:"tool_name"`
	Arguments json.RawMessage `json:"arguments"`
	TimeoutMS int64           `json:"timeout_ms"`
}

type nativeRuntime struct {
	sess         *session.Session
	name         string
	client       *http.Client
	deviceFields map[string]any
}

type nativeReply struct {
	id     string
	result json.RawMessage
	err    any
}

func nativeHTTPClient() *http.Client {
	return &http.Client{Timeout: 5 * time.Minute, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func prepareNativeRuntime(ctx context.Context, sess *session.Session, name string) (*nativeRuntime, error) {
	bad := func(message string) (*nativeRuntime, error) {
		return nil, &UpstreamError{Code: "native_relay_unavailable", Message: message}
	}
	if sess == nil || sess.OutpostID == "" || sess.OutpostToken == "" {
		return bad("native relay requires a registered proxy-owned outpost")
	}
	u, err := url.Parse(sess.OutpostURL)
	if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return bad("native relay requires an explicit loopback outpost URL")
	}
	ip := net.ParseIP(u.Hostname())
	if ip == nil || !ip.IsLoopback() || u.Port() == "8780" {
		return bad("native relay cannot use a remote or Desktop-owned outpost")
	}
	if name != "quick2api_client_call" && !strings.HasSuffix(name, "__quick2api_client_call") {
		return bad("native relay name must identify the registered quick2api_client_call tool")
	}
	runtime := &nativeRuntime{sess: sess, name: name, client: nativeHTTPClient(), deviceFields: map[string]any{
		"surfaceType": "QUICK_WORK_DESKTOP_WINDOWS", "outpostId": sess.OutpostID,
		"appVersion": "1.2.4660", "acp_first_text": 1, "user_notification": 1,
		"subtask_progress": 1, "external_agent_sign_in": 1, "recents_promotion": 1,
	}}
	// The caller owns a ready runtime. Failed preparation has no caller to
	// close its private transport, whose idle connections otherwise persist.
	prepared := false
	defer func() {
		if !prepared {
			runtime.client.CloseIdleConnections()
		}
	}()
	probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for {
		req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, strings.TrimRight(sess.OutpostURL, "/")+"/local/mcp/list", nil)
		if err != nil {
			return bad("could not construct native relay catalog request")
		}
		req.Header.Set("Authorization", "Bearer "+sess.OutpostToken)
		resp, err := runtime.client.Do(req)
		if err != nil {
			return bad("native relay catalog request failed")
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return bad("native relay catalog request was rejected")
		}
		type registeredTool struct {
			Name   string `json:"name"`
			Schema struct {
				Type       string   `json:"type"`
				Required   []string `json:"required"`
				Properties map[string]struct {
					Type string `json:"type"`
				} `json:"properties"`
			} `json:"input_schema"`
		}
		var catalog struct {
			Servers []struct {
				Enabled bool             `json:"enabled"`
				Status  string           `json:"status"`
				Tools   []registeredTool `json:"tools"`
			} `json:"servers"`
		}
		decodeErr := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&catalog)
		resp.Body.Close()
		if decodeErr != nil {
			return bad("native relay catalog was invalid")
		}
		waiting := catalog.Servers != nil && len(catalog.Servers) == 0
		for _, server := range catalog.Servers {
			if server.Enabled && (server.Status == "connecting" || server.Status == "starting" || server.Status == "reconnecting") {
				waiting = true
			}
			if !server.Enabled || server.Status != "connected" {
				continue
			}
			for _, tool := range server.Tools {
				if tool.Name != name {
					continue
				}
				p := tool.Schema.Properties
				required := make(map[string]bool)
				for _, key := range tool.Schema.Required {
					required[key] = true
				}
				if tool.Schema.Type != "object" || !required["route_id"] || !required["name"] || !required["arguments"] || p["route_id"].Type != "string" || p["name"].Type != "string" || p["arguments"].Type != "object" {
					return bad("registered native relay does not expose the expected routing schema")
				}
				prepared = true
				return runtime, nil
			}
		}
		if waiting {
			select {
			case <-probeCtx.Done():
				return bad("native relay did not become ready within its catalog startup budget")
			case <-time.After(250 * time.Millisecond):
				continue
			}
		}
		return bad("native relay is not registered on this proxy outpost; approve its MCP registration before enabling mcp transport")
	}
}

func (r *nativeRuntime) invoke(ctx context.Context, call NativeCall) nativeReply {
	reply := nativeReply{id: call.ID}
	if call.Name != r.name {
		reply.err = "Native device tool is not enabled by this client relay; no tool was executed"
		return reply
	}
	budget := 5 * time.Minute
	if call.TimeoutMS > 0 && call.TimeoutMS < int64(budget/time.Millisecond) {
		budget = time.Duration(call.TimeoutMS) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	body, err := json.Marshal(map[string]any{"tool_name": call.Name, "arguments": call.Arguments, "wire_paths": true, "tool_call_id": call.ID, "request_timeout_ms": int64(budget / time.Millisecond)})
	if err != nil {
		reply.err = "Native relay request encoding failed"
		return reply
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(r.sess.OutpostURL, "/")+"/local/invoke_tool", bytes.NewReader(body))
	if err != nil {
		reply.err = "Native relay request construction failed"
		return reply
	}
	req.Header.Set("Authorization", "Bearer "+r.sess.OutpostToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		reply.err = "Native relay did not return a result; execution state may be uncertain"
		return reply
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 48_001))
	if err != nil || len(data) > 48_000 {
		reply.err = "Native relay result could not be delivered within the 48000-byte device result limit"
		return reply
	}
	if resp.StatusCode != http.StatusOK || !json.Valid(data) {
		reply.err = fmt.Sprintf("Native relay failed with HTTP %d; no success result was fabricated", resp.StatusCode)
		return reply
	}
	// Preserve the outpost's real application-level result, including isError,
	// success:false and policy refusal. HTTP 200 alone is not tool success.
	reply.result = append(json.RawMessage(nil), data...)
	return reply
}
