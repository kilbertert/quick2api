// Package app wires the HTTP server: routes, auth, and the shared handler
// surface. Standard library mux only — the API is intentionally small.
package app

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/kilbertert/quick2api/internal/api/anthropic"
	"github.com/kilbertert/quick2api/internal/api/openai"
	"github.com/kilbertert/quick2api/internal/bridge"
	"github.com/kilbertert/quick2api/internal/config"
	"github.com/kilbertert/quick2api/internal/debuglog"
	"github.com/kilbertert/quick2api/internal/tools"
)

// App is the running server.
type App struct {
	Cfg       *config.Config
	Bridge    *bridge.Pool
	Debug     *debuglog.Logger
	Handler   *openai.Handler
	Anthropic *anthropic.Handler
	Mux       *http.ServeMux
}

// New builds the app over an already-initialized bridge.
func New(cfg *config.Config, b *bridge.Pool, dlog *debuglog.Logger) *App {
	max := cfg.Server.MaxConcurrency
	if max <= 0 {
		max = 16
	}
	sem := make(chan struct{}, max)
	h := &openai.Handler{Bridge: b, Cfg: cfg, Debug: dlog, Sem: sem}
	ah := &anthropic.Handler{Bridge: b, Cfg: cfg, Debug: dlog, Sem: sem}
	if cfg.Tools.ClientTransport == "mcp" {
		ttl := time.Duration(cfg.Tools.BridgeTimeoutS) * time.Second
		ah.Native = anthropic.NewNativeBridge(context.Background(), ttl, b.RunTurn)
	}
	if cfg.Tools.Bash.Enabled || cfg.Tools.Files.Enabled {
		// Server-side tool executor enabled: convert the advisory
		// "answer with tool_use JSON" contract into a real loop.
		var tcfg tools.Config
		tcfg.Bash.Enabled = cfg.Tools.Bash.Enabled
		tcfg.Bash.Allow = cfg.Tools.Bash.Allow
		tcfg.Bash.Cwd = cfg.Tools.Bash.Cwd
		tcfg.Bash.TimeoutS = cfg.Tools.Bash.TimeoutS
		tcfg.Files.Enabled = cfg.Tools.Files.Enabled
		tcfg.Files.Root = cfg.Tools.Files.Root
		exec := tools.New(tcfg)
		ah.Tools = exec
		h.Tools = exec
	}
	a := &App{Cfg: cfg, Bridge: b, Debug: dlog, Handler: h, Anthropic: ah, Mux: http.NewServeMux()}
	a.routes()
	return a
}

func (a *App) routes() {
	if a.Anthropic.Native != nil {
		a.Mux.HandleFunc("POST /internal/client-tool-relay", a.auth(a.Anthropic.Native.Relay))
	}
	a.Mux.HandleFunc("POST /v1/chat/completions", a.auth(a.Handler.ChatCompletions))
	a.Mux.HandleFunc("POST /v1/messages", a.auth(a.Anthropic.Messages))
	a.Mux.HandleFunc("POST /v1/messages/count_tokens", a.auth(a.Anthropic.CountTokens))
	a.Mux.HandleFunc("GET /v1/models", a.auth(a.Handler.Models))
	a.Mux.HandleFunc("GET /healthz", a.healthz)
	a.Mux.HandleFunc("GET /{$}", a.index)
}

// Close ends detached native jobs, including those waiting for client results.
func (a *App) Close() {
	if a.Anthropic.Native != nil {
		a.Anthropic.Native.Close()
	}
}

// auth enforces the configured bearer on /v1/*; empty api_key is pass-through.
func (a *App) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.Cfg.API.APIKey != "" {
			got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if got == "" {
				got = r.Header.Get("X-Api-Key")
			}
			if got != a.Cfg.API.APIKey {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error": map[string]any{"message": "invalid api key", "type": "authentication_error"},
				})
				return
			}
		}
		next(w, r)
	}
}

func (a *App) healthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":      true,
		"version": version,
		"bridge":  a.Bridge.Info(),
		"time":    time.Now().UTC(),
	})
}

func (a *App) index(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"service": "quick2api",
		"version": version,
		"endpoints": []string{
			"POST /v1/chat/completions",
			"POST /v1/messages",
			"GET  /v1/models",
			"GET  /healthz",
		},
	})
}

// version is set by main at startup (avoids a config<->main import cycle).
var version = "dev"

// SetVersion lets main inject the resolved build version.
func SetVersion(v string) {
	version = v
}
