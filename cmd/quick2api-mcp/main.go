// quick2api-mcp exposes quick2api's client-tool relay as an MCP stdio server.
// This process performs no client file or terminal operations.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/kilbertert/quick2api/internal/config"
	"github.com/kilbertert/quick2api/internal/mcprelay"
)

func main() {
	configPath := flag.String("config", "", "path to the quick2api configuration (read only)")
	flag.Parse()
	if err := run(*configPath); err != nil {
		fmt.Fprintln(os.Stderr, "quick2api MCP relay:", err)
		os.Exit(1)
	}
}

func run(path string) error {
	resolved, err := config.ResolveConfigPath(path)
	if err != nil {
		return errors.New("cannot resolve configuration")
	}
	cfg, err := config.Load(resolved)
	if err != nil {
		return errors.New("cannot read a valid quick2api configuration")
	}
	host, port, err := net.SplitHostPort(cfg.Server.Listen)
	if err != nil || (host != "127.0.0.1" && host != "::1") {
		return errors.New("server.listen must use explicit 127.0.0.1 or [::1]")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return errors.New("server.listen has an invalid port")
	}
	if cfg.API.APIKey == "" {
		return errors.New("api.api_key is required for the MCP relay")
	}
	endpoint := "http://" + net.JoinHostPort(host, port) + "/internal/client-tool-relay"
	client := &http.Client{
		Timeout:       300 * time.Second,
		Transport:     &http.Transport{Proxy: nil, DisableKeepAlives: true},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	defer client.CloseIdleConnections()
	return mcprelay.Serve(ctx, os.Stdin, os.Stdout, func(callCtx context.Context, route, name string, args json.RawMessage) (json.RawMessage, error) {
		body, err := json.Marshal(map[string]any{"route_id": route, "name": name, "arguments": args})
		if err != nil {
			return nil, errors.New("cannot encode relay request")
		}
		req, err := http.NewRequestWithContext(callCtx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, errors.New("cannot create relay request")
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+cfg.API.APIKey)
		res, err := client.Do(req)
		if err != nil {
			return nil, errors.New("local client-tool relay request failed")
		}
		defer res.Body.Close()
		data, err := io.ReadAll(io.LimitReader(res.Body, (4<<20)+1))
		if err != nil {
			return nil, errors.New("cannot read local relay response")
		}
		if len(data) > 4<<20 {
			return nil, errors.New("local relay response exceeds 4 MiB")
		}
		if res.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("local client-tool relay returned HTTP %d", res.StatusCode)
		}
		return json.RawMessage(data), nil
	})
}
