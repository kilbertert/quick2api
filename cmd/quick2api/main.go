// quick2api exposes the model behind a logged-in Amazon Quick desktop
// account as an OpenAI-compatible /v1 API. See README.md for the chain.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/kilbertert/quick2api/internal/app"
	"github.com/kilbertert/quick2api/internal/bridge"
	"github.com/kilbertert/quick2api/internal/chat"
	"github.com/kilbertert/quick2api/internal/config"
	"github.com/kilbertert/quick2api/internal/debuglog"
)

// version is injected via -ldflags "-X main.version=vX.Y.Z"; the fallbacks
// below cover `go install @version` and plain `go build` inside the checkout.
var version = "dev"

func resolvedVersion() string {
	if version != "dev" {
		return version
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		if info.Main.Version != "" && info.Main.Version != "(devel)" {
			return info.Main.Version
		}
		var rev string
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" {
				rev = s.Value
			}
		}
		if rev != "" {
			if len(rev) > 12 {
				rev = rev[:12]
			}
			return "dev-" + rev
		}
	}
	return version
}

func main() {
	configPath := flag.String("config", "", "path to config.yaml; default resolves $QUICK2API_CONFIG → ./config.yaml → platform user-config dir")
	stateDir := flag.String("state-dir", "", "debug/state dir; default resolves $QUICK2API_STATE_DIR → platform state dir")
	showVersion := flag.Bool("version", false, "print the resolved build version and exit")
	probe := flag.Bool("probe", false, "verify the upstream chain end-to-end (outpost → session → WSS → one turn) and exit without serving")
	probeText := flag.String("probe-text", "Reply with exactly: PONG", "probe message sent with -probe")
	flag.Parse()

	resolved := resolvedVersion()
	if *showVersion {
		fmt.Println(resolved)
		return
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	app.SetVersion(resolved)

	cfgPath, err := config.ResolveConfigPath(*configPath)
	if err != nil {
		slog.Error("resolve config path", "error", err)
		os.Exit(1)
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		// Missing file → defaults only; parse/validation errors → fatal.
		if errors.Is(err, os.ErrNotExist) {
			cfg = config.Defaults()
			slog.Warn("config not found, running on defaults", "path", cfgPath)
		} else {
			slog.Error("load config", "path", cfgPath, "error", err)
			os.Exit(1)
		}
	}
	state, err := config.ResolveStateDir(*stateDir)
	if err != nil {
		slog.Error("resolve state dir", "error", err)
		os.Exit(1)
	}
	dlog, err := debuglog.New(state, cfg.Debug.Enabled, cfg.Debug.RetentionDays, cfg.Debug.LogFrames)
	if err != nil {
		slog.Error("init debug log", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	b, err := bridge.BuildPool(ctx, cfg)
	if err != nil {
		slog.Error("init upstream bridge", "error", err)
		os.Exit(1)
	}
	defer b.Close()
	b.Start(ctx) // lane health probe

	if *probe {
		if err := runProbe(ctx, cfg, b, *probeText); err != nil {
			slog.Error("probe failed", "error", err)
			os.Exit(1)
		}
		return
	}

	a := app.New(cfg, b, dlog)
	defer a.Close()
	srv := &http.Server{
		Addr:              cfg.Server.Listen,
		Handler:           a.Mux,
		ReadHeaderTimeout: 15 * time.Second,
		// BaseContext ties every request to the signal context: on SIGTERM
		// in-flight turns see ctx.Done() instead of just waiting for the
		// shutdown timeout to elapse.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	go func() {
		<-ctx.Done()
		a.Close()
		shCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shCtx)
	}()

	slog.Info("quick2api listening",
		"addr", cfg.Server.Listen, "version", resolved,
		"config", cfgPath, "state", state)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("serve", "error", err)
		os.Exit(1)
	}
}

// runProbe sends one canned turn and prints the result — the quickest way to
// prove the outpost → session → WSS chain works on this machine.
func runProbe(ctx context.Context, cfg *config.Config, b *bridge.Pool, text string) error {
	res, err := b.RunTurn(ctx, chat.TurnOpts{
		Text:           text,
		AgentMode:      cfg.Quick.DefaultAgentMode,
		ThinkingEffort: cfg.Quick.ThinkingEffort,
		UserTimezone:   cfg.Quick.UserTimezone,
		UserLocale:     cfg.Quick.UserLocale,
		Title:          "quick2api probe",
	})
	if err != nil {
		return err
	}
	fmt.Println("=== probe reply ===")
	fmt.Println(res.Text)
	fmt.Printf("\nresolved_model=%s finish=%s\n", res.ResolvedModel, res.FinishReason)
	if res.Usage != nil {
		fmt.Printf("usage: in=%d out=%d cache_read=%d cache_write=%d cost_usd=%.6f model=%s\n",
			res.Usage.InputTokens, res.Usage.OutputTokens,
			res.Usage.CacheReadTokens, res.Usage.CacheWriteTokens,
			res.Usage.CostUSD, strings.TrimSpace(res.Usage.Model))
	}
	return nil
}
