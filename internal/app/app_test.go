package app

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kilbertert/quick2api/internal/bridge"
	"github.com/kilbertert/quick2api/internal/config"
)

func TestModelEndpointsShareConfiguredRequestBodyBudget(t *testing.T) {
	cfg := config.Defaults()
	cfg.Server.MaxRequestBodyBytes = 1024
	a := New(cfg, nil, nil)
	defer a.Close()
	body := `{"messages":[{"role":"user","content":"` + strings.Repeat("x", 2000) + `"}]}`
	for _, path := range []string{"/v1/messages", "/v1/messages/count_tokens", "/v1/chat/completions"} {
		w := httptest.NewRecorder()
		a.Mux.ServeHTTP(w, httptest.NewRequest("POST", path, strings.NewReader(body)))
		if w.Code != 413 || !strings.Contains(w.Body.String(), "server.max_request_body_bytes") {
			t.Fatalf("%s: got %d, %s", path, w.Code, w.Body.String())
		}
	}
}

func TestCountTokensAcceptsHistoryAboveLegacyFourMiBBudget(t *testing.T) {
	cfg := config.Defaults()
	a := New(cfg, nil, nil)
	defer a.Close()
	body := `{"messages":[{"role":"user","content":"` + strings.Repeat("x", 5<<20) + `"}]}`
	w := httptest.NewRecorder()
	a.Mux.ServeHTTP(w, httptest.NewRequest("POST", "/v1/messages/count_tokens", strings.NewReader(body)))
	var result struct {
		InputTokens int `json:"input_tokens"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil || result.InputTokens < (5<<20)/4 {
		t.Fatalf("large history rejected or truncated: %d %s", w.Code, w.Body.String())
	}
}

func TestRelayEndpointIsOptInAuthenticatedAndLocal(t *testing.T) {
	for _, mode := range []string{"prompt", "mcp"} {
		t.Run(mode, func(t *testing.T) {
			cfg := config.Defaults()
			cfg.Tools.ClientTransport = mode
			cfg.Tools.NativeRelayName = "quick2api_client_call"
			cfg.API.APIKey = "public-test-fixture-key"
			a := New(cfg, &bridge.Pool{}, nil)
			defer a.Close()
			for _, kind := range []string{"no-key", "remote", "unknown-route", "missing-invocation"} {
				body := `{"route_id":"unknown:invocation","name":"Read","arguments":{}}`
				if kind == "missing-invocation" {
					body = `{"route_id":"unknown","name":"Read","arguments":{}}`
				}
				r := httptest.NewRequest("POST", "/internal/client-tool-relay", strings.NewReader(body))
				r.RemoteAddr = "127.0.0.1:1234"
				if kind != "no-key" {
					r.Header.Set("Authorization", "Bearer public-test-fixture-key")
				}
				if kind == "remote" {
					r.RemoteAddr = "198.51.100.1:1234"
				}
				w := httptest.NewRecorder()
				a.Mux.ServeHTTP(w, r)
				want := 409
				if mode == "prompt" {
					want = 404
				} else if kind == "no-key" {
					want = 401
				} else if kind == "remote" {
					want = 403
				} else if kind == "missing-invocation" {
					want = 400
				}
				if w.Code != want {
					t.Fatalf("mode=%s kind=%s got=%d want=%d", mode, kind, w.Code, want)
				}
			}
		})
	}
}
