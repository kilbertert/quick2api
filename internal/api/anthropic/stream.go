package anthropic

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"
)

const (
	streamPingInterval = 15 * time.Second
	streamWriteTimeout = 10 * time.Second
)

// messageStream serializes heartbeats and message events. A failed downstream
// write cancels the upstream turn instead of leaving it running for a client
// that can no longer receive its reply.
type messageStream struct {
	mu     sync.Mutex
	w      http.ResponseWriter
	cancel context.CancelFunc
	failed bool
}

func (s *messageStream) emit(event string, data map[string]any) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed {
		return false
	}
	controller := http.NewResponseController(s.w)
	// Bound a slow/disconnected client's write even while the provider is
	// buffering tool JSON. Test and third-party writers may lack deadlines.
	err := controller.SetWriteDeadline(time.Now().Add(streamWriteTimeout))
	if err == nil {
		defer controller.SetWriteDeadline(time.Time{})
	} else if errors.Is(err, http.ErrNotSupported) {
		err = nil
	}
	if err == nil {
		err = writeSSE(s.w, event, data)
	}
	if err == nil {
		err = controller.Flush()
	}
	if err != nil {
		s.failed = true
		s.cancel()
		return false
	}
	return true
}

// stop waits for the last heartbeat to finish, so the handler never returns
// while a goroutine still holds its ResponseWriter. It is safe to call twice.
func (s *messageStream) startKeepalive(ctx context.Context, interval time.Duration) (stop func()) {
	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				if !s.emit("ping", map[string]any{}) {
					return
				}
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() { close(done) })
		<-finished
	}
}
