package bridge

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/kilbertert/quick2api/internal/chat"
)

func TestConversationRequestsDoNotOverlap(t *testing.T) {
	var locks conversationLocks
	var active atomic.Int32
	var overlapped atomic.Bool
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			release, err := locks.acquire(context.Background(), "desktop-session")
			if err != nil {
				t.Error(err)
				return
			}
			if active.Add(1) != 1 {
				overlapped.Store(true)
			}
			runtime.Gosched()
			active.Add(-1)
			release()
		}()
	}
	close(start)
	wg.Wait()
	if overlapped.Load() {
		t.Fatal("two requests could consume the same conversation at once")
	}
	if len(locks.entries) != 0 {
		t.Fatal("completed conversation locks leaked")
	}
}

func TestPolicyAndCancelledTurnsNeverUseConnectionRetry(t *testing.T) {
	for _, err := range []error{
		&chat.AttachmentPolicyError{Blocked: true, Message: "blocked"},
		&chat.AttachmentPolicyError{Message: "policy check unavailable"},
		context.Canceled,
		context.DeadlineExceeded,
		&chat.UpstreamError{Code: "content_filtered", Message: "filtered"},
	} {
		if retryable(fmt.Errorf("turn failed: %w", err)) {
			t.Fatalf("non-transport failure would be sent again: %v", err)
		}
	}
	if !retryable(errors.New("upstream ws closed before message_complete")) {
		t.Fatal("ordinary connection recovery was disabled")
	}
}

func TestConversationWaitCancellationAndIndependentSession(t *testing.T) {
	var locks conversationLocks
	release, err := locks.acquire(context.Background(), "busy")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() {
		r, err := locks.acquire(ctx, "busy")
		if r != nil {
			r()
		}
		finished <- err
	}()
	// Another conversation can run even while this one is busy.
	other, err := locks.acquire(context.Background(), "other")
	if err != nil {
		t.Fatal(err)
	}
	other()
	cancel()
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter entered the conversation: %v", err)
	}
	release()
	if len(locks.entries) != 0 {
		t.Fatal("cancelled waiter leaked a lock")
	}
}
