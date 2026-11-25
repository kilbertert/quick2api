// outpost.go manages the local quickwork-agent outpost that answers
// GET /auth/sncs-session. quick2api always spawns its own instance on a
// private port — the Quick desktop app's outpost on :8780 is unusable because
// its bearer token exists only inside that app's main process memory.
package session

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	"github.com/kilbertert/quick2api/internal/util"
)

// SpawnOpts parameterizes Spawn.
type SpawnOpts struct {
	AgentPath   string
	Manifest    string // -manifest value, "desktop"
	BuildStage  string // QUICKWORK_BUILD_STAGE
	ProfileRoot string // existing user-approved Quick profile; QUICKWORK_ROOT
	Port        int
	WorkDir     string // agent working dir; empty = os.TempDir
}

// Outpost is a running quickwork-agent outpost instance.
type Outpost struct {
	URL   string // http://127.0.0.1:<port>
	Token string // bearer for /auth/sncs-session (self-chosen)
	cmd   *exec.Cmd
	owns  bool // we spawned it (vs. attached to an external URL)
}

// Close stops a spawned outpost. Attached outposts are left alone.
func (o *Outpost) Close() {
	if o == nil || o.cmd == nil || o.cmd.Process == nil {
		return
	}
	_ = o.cmd.Process.Kill()
	_, _ = o.cmd.Process.Wait()
}

// Attach wraps an externally-running outpost we did not spawn.
func Attach(url, token string) *Outpost {
	return &Outpost{URL: url, Token: token}
}

// DefaultAgentPath returns the standard per-user install location of the
// extracted agent binary. The Electron app unpacks resources/agent.zip into
// the data dir on first run.
func DefaultAgentPath() string {
	if runtime.GOOS == "windows" {
		local := os.Getenv("LOCALAPPDATA")
		if local == "" {
			local = filepath.Join(os.Getenv("USERPROFILE"), "AppData", "Local")
		}
		return filepath.Join(local, "quickwork", "agent", "quickwork-agent.exe")
	}
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".local", "share", "quickwork", "agent", "quickwork-agent")
	}
	return "quickwork-agent"
}

// randomToken returns 64 lowercase hex chars — the same shape the desktop
// main process generates. The outpost treats it as an opaque bearer.
func randomToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// Spawn starts `quickwork-agent --outpost-only --manifest <m> --port <p>`,
// feeds the bearer token on stdin (the agent reads exactly one line), waits
// for /health to answer, and returns the handle.
func Spawn(ctxDone <-chan struct{}, opts SpawnOpts) (*Outpost, error) {
	port := opts.Port
	if port <= 0 {
		p, err := freePort()
		if err != nil {
			return nil, fmt.Errorf("pick outpost port: %w", err)
		}
		port = p
	}
	token, err := randomToken()
	if err != nil {
		return nil, fmt.Errorf("generate outpost token: %w", err)
	}
	agentPath := opts.AgentPath
	if agentPath == "" {
		agentPath = DefaultAgentPath()
	}
	if _, err := os.Stat(agentPath); err != nil {
		return nil, fmt.Errorf("agent not found at %q — install Amazon Quick or set quick.agent_path", agentPath)
	}
	manifest := opts.Manifest
	if manifest == "" {
		manifest = "desktop"
	}
	cmd := exec.Command(agentPath,
		"--outpost-only",
		"--manifest", manifest,
		"--port", fmt.Sprint(port),
	)
	cmd.Dir = opts.WorkDir
	cmd.Env = append(os.Environ(), "QUICKWORK_BUILD_STAGE="+util.Or(opts.BuildStage, "prod"))
	if opts.ProfileRoot != "" {
		cmd.Env = append(cmd.Env, "QUICKWORK_ROOT="+opts.ProfileRoot)
	}
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	setSpawnAttrs(cmd)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("agent stdin: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("spawn agent: %w", err)
	}
	o := &Outpost{URL: fmt.Sprintf("http://127.0.0.1:%d", port), Token: token, cmd: cmd, owns: true}
	// The agent reads one line on stdin as its instance token.
	if _, err := io.WriteString(stdin, token+"\n"); err == nil {
		// Keep stdin open — the agent may keep reading for liveness.
	} else {
		slog.Warn("outpost stdin token write failed", "error", err)
	}
	// Reap the agent if our context dies.
	go func() {
		<-ctxDone
		o.Close()
	}()
	// Wait for /health.
	deadline := time.Now().Add(30 * time.Second)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get(o.URL + "/health")
		if err == nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				slog.Info("outpost ready", "url", o.URL)
				return o, nil
			}
			_ = body
		}
		select {
		case <-ctxDone:
			o.Close()
			return nil, fmt.Errorf("outpost startup cancelled")
		case <-time.After(300 * time.Millisecond):
		}
	}
	o.Close()
	return nil, fmt.Errorf("outpost at %s did not become healthy within 30s", o.URL)
}

// FetchSession pulls /auth/sncs-session once.
func (o *Outpost) FetchSession() (*Session, error) {
	req, err := http.NewRequest(http.MethodGet, o.URL+"/auth/sncs-session", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+o.Token)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return nil, fmt.Errorf("sncs-session: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("sncs-session: status %d: %s", resp.StatusCode, util.Trunc(string(body), 200))
	}
	var s Session
	if err := json.Unmarshal(body, &s); err != nil {
		return nil, fmt.Errorf("sncs-session: decode: %w", err)
	}
	if s.IDToken == "" || s.WSHost == "" {
		return nil, fmt.Errorf("sncs-session: response missing id_token/ws_host")
	}
	// Best effort: learn this outpost's registered device id. The chat WSS
	// closes the stream when ?outpostId= isn't the registered id, so this
	// is required for spawn/attach sessions; ignore failures (attach mode
	// tolerates partial answers too).
	if id, err := o.fetchOutpostID(); err == nil && id != "" {
		s.OutpostID = id
	} else {
		slog.Warn("outpost id lookup failed; chat will use a synthetic id", "error", err)
	}
	// Keep the outpost identity for future local-API needs (the cloud
	// attachment path uses TenantURL, not this loopback — see
	// chat.uploadAttachment).
	s.OutpostURL, s.OutpostToken = o.URL, o.Token
	return &s, nil
}

// fetchOutpostID reads only outpost_id from GET /local/list_tools. The
// payload is large (full tool catalog); we stream-decode just the key.
func (o *Outpost) fetchOutpostID() (string, error) {
	req, err := http.NewRequest(http.MethodGet, o.URL+"/local/list_tools", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+o.Token)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return "", fmt.Errorf("list_tools: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("list_tools: status %d", resp.StatusCode)
	}
	var probe struct {
		OutpostID string `json:"outpost_id"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&probe); err != nil {
		return "", fmt.Errorf("list_tools: decode: %w", err)
	}
	return probe.OutpostID, nil
}

// LoadFile reads a captured sncs-session JSON (quick.session_file).
func LoadFile(path string) (*Session, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s Session
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("session file %s: %w", path, err)
	}
	if s.IDToken == "" || s.WSHost == "" {
		return nil, fmt.Errorf("session file %s: missing id_token/ws_host", path)
	}
	return &s, nil
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
