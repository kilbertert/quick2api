// Package common holds the pieces both API adapters (openai + anthropic)
// share: request-id generation, the model fallback, the concurrency-slot
// acquire, the upstream-error mapping, and the 01-frames debug collector.
// Schema and wire details stay in the adapters themselves.
package common

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/kilbertert/quick2api/internal/chat"
	"github.com/kilbertert/quick2api/internal/config"
	"github.com/kilbertert/quick2api/internal/debuglog"
)

var idCounter uint64

// NewID mints a unique id with a prefix (tool_use ids, request fallbacks).
func NewID(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), atomic.AddUint64(&idCounter, 1))
}

// ReqID is the X-Request-Id when present, else a fresh unique id.
func ReqID(r *http.Request) string {
	if v := r.Header.Get("X-Request-Id"); v != "" {
		return v
	}
	return NewID("q2a")
}

// FirstModel returns a deterministic default model name: the alphabetically
// first configured agent_modes key, else "quick". (The previous
// pick-first-map-iteration was nondeterministic across restarts.)
func FirstModel(c *config.Config) string {
	best := ""
	for k := range c.Quick.AgentModes {
		if best == "" || k < best {
			best = k
		}
	}
	if best == "" {
		return "quick"
	}
	return best
}

// Acquire takes one slot out of the semaphore (non-blocking). ok=false
// means either the request was cancelled while waiting (ctx.Err()!=nil)
// or the pool is full — callers map the second case to a local 429.
func Acquire(ctx context.Context, sem chan struct{}) (release func(), ok bool) {
	if sem == nil {
		return func() {}, true
	}
	select {
	case sem <- struct{}{}:
		return func() { <-sem }, true
	case <-ctx.Done():
		return func() {}, false
	default:
		return func() {}, false
	}
}

// UpstreamKind classifies a run failure for HTTP mapping. The adapters
// translate the kind into their own error-type labels.
type UpstreamKind int

const (
	KindUpstream         UpstreamKind = iota // generic upstream failure → 502
	KindUnauthenticated                      // upstream says creds bad/expired → 401
	KindRateLimited                          // upstream throttled → 429
	KindTimeout                              // ctx deadline → 504
	KindPermissionDenied                     // attachment policy blocked input → 403
)

// UpstreamError maps err to (httpStatus, kind, message). Message is the
// user-facing text: UpstreamError.Message for typed frames, err.Error()
// otherwise (it carries the "upstream: " prefix).
func UpstreamError(err error) (status int, kind UpstreamKind, message string) {
	// Setup retains its typed cause for classification, but its safe message
	// must take precedence over arbitrary upstream bodies or close reasons.
	var setup *chat.ConnectionSetupError
	if errors.As(err, &setup) {
		message = setup.Error()
		var cause *chat.UpstreamError
		if errors.As(setup, &cause) {
			switch cause.Code {
			case "unauthenticated", "token_expired":
				return http.StatusUnauthorized, KindUnauthenticated, message
			case "rate_limited", "throttled":
				return http.StatusTooManyRequests, KindRateLimited, message
			}
		}
		if errors.Is(setup, context.DeadlineExceeded) {
			return http.StatusGatewayTimeout, KindTimeout, message
		}
		return http.StatusBadGateway, KindUpstream, message
	}
	var policy *chat.AttachmentPolicyError
	if errors.As(err, &policy) && policy.Blocked {
		return http.StatusForbidden, KindPermissionDenied, policy.Message
	}
	var ue *chat.UpstreamError
	if errors.As(err, &ue) {
		switch ue.Code {
		case "unauthenticated", "token_expired":
			return http.StatusUnauthorized, KindUnauthenticated, ue.Message
		case "rate_limited", "throttled":
			return http.StatusTooManyRequests, KindRateLimited, ue.Message
		}
		return http.StatusBadGateway, KindUpstream, ue.Message
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout, KindTimeout, err.Error()
	}
	return http.StatusBadGateway, KindUpstream, err.Error()
}

// Frames collects event names for 01-events and, when debug.log_frames is
// on, payloads for 01-frames. Adapters call OnEvent() inside their own
// OnEvent chain, then Write() on every exit path.
type Frames struct {
	enabled  bool
	raw      []json.RawMessage
	sequence []string
	counts   map[string]int
}

// NewFrames always collects event names; full payloads require explicit opt-in.
func NewFrames(enabled bool) *Frames {
	return &Frames{enabled: enabled, counts: make(map[string]int)}
}

// OnEvent records ev when it is an upstream frame.
func (f *Frames) OnEvent(ev chat.TurnEvent) {
	if ev.Kind != "frame" || ev.Frame == nil {
		return
	}
	f.counts[ev.Frame.Type]++
	if len(f.sequence) < 4096 {
		f.sequence = append(f.sequence, ev.Frame.Type)
	}
	if !f.enabled {
		return
	}
	if raw, err := json.Marshal(ev.Frame); err == nil {
		f.raw = append(f.raw, raw)
	}
}

// Write appends event names to 01-events and opt-in payloads to 01-frames.
// The debug logger decides whether request logging is enabled.
func (f *Frames) Write(log *debuglog.Logger, dir string) {
	if log == nil {
		return
	}
	// Event names establish turn boundaries without recording payloads,
	// credentials, tool arguments, or the user's conversation content.
	if len(f.sequence) > 0 {
		log.Write(dir, "01-events", map[string]any{"sequence": f.sequence, "counts": f.counts})
	}
	if f.enabled && len(f.raw) > 0 {
		log.Write(dir, "01-frames", f.raw)
	}
}
