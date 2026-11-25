package toolbridge

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/kilbertert/quick2api/internal/chat"
)

func TestTextPrecedesTerminalAndCapacityIsBounded(t *testing.T) {
	for _, failure := range []bool{false, true} {
		e := testExchange(t, nil)
		if err := e.PublishText("first"); err != nil {
			t.Fatal(err)
		}
		if err := e.PublishText("last"); err != nil {
			t.Fatal(err)
		}
		if failure {
			e.Complete(nil, context.Canceled)
		} else {
			e.Complete(&chat.TurnResult{Text: "firstlast"}, nil)
		}
		for _, want := range []string{"first", "last"} {
			ev, err := e.Next(testContext(t))
			if err != nil || ev.Text != want || ev.Final != nil || ev.Err != nil {
				t.Fatalf("text lost at completion: %+v, %v", ev, err)
			}
		}
		ev, err := e.Next(testContext(t))
		if err != nil || (failure && !errors.Is(ev.Err, context.Canceled)) || (!failure && ev.Final == nil) {
			t.Fatalf("terminal lost: %+v, %v", ev, err)
		}
		if e.PublishText("after end") == nil {
			t.Fatal("post-terminal text accepted")
		}
	}
	e := testExchange(t, nil)
	if err := e.PublishText(strings.Repeat("a", 1<<20)); err != nil {
		t.Fatal(err)
	}
	if err := e.PublishText("b"); !errors.Is(err, ErrTextCapacity) {
		t.Fatal("unbounded unread text")
	}
	if _, err := e.Next(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if err := e.PublishText("b"); err != nil {
		t.Fatal("consumed budget was not released")
	}
}

func TestTextCallOrderingAndCancellation(t *testing.T) {
	e := testExchange(t, nil)
	if err := e.PublishText("before"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(testContext(t))
	answers := startCall(e, ctx, "Read", []byte(`{}`))
	// Wait for admission without consuming the subscriber's ordered queue.
	for {
		e.mu.Lock()
		admitted := len(e.pending) == 1
		e.mu.Unlock()
		if admitted {
			break
		}
		select {
		case <-e.wake:
		case <-ctx.Done():
			t.Fatal("call not admitted")
		}
	}
	if err := e.PublishText("after"); err != nil {
		t.Fatal(err)
	}
	ev, err := e.Next(testContext(t))
	if err != nil || ev.Text != "before" {
		t.Fatal("call overtook prior text")
	}
	call := nextCall(t, e)
	if call.Name != "Read" {
		t.Fatal("wrong call")
	}
	cancel()
	if out := receiveOutcome(t, answers); !errors.Is(out.err, context.Canceled) {
		t.Fatal("cancelled call succeeded")
	}
	e.Complete(&chat.TurnResult{Text: "beforeafter"}, nil)
	ev, err = e.Next(testContext(t))
	if err != nil || ev.Text != "after" {
		t.Fatal("text lost when pending call was cancelled")
	}
}

func TestQueuedTextRespectsSubscriberCancellationWithoutConsuming(t *testing.T) {
	e := testExchange(t, nil)
	if err := e.PublishText("queued"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if ev, err := e.Next(ctx); !errors.Is(err, context.Canceled) || ev.Text != "" {
		t.Fatal("cancelled subscriber consumed queued text")
	}
	if ev, err := e.Next(testContext(t)); err != nil || ev.Text != "queued" {
		t.Fatal("cancellation lost queued text")
	}
	for i := 0; i < 4096; i++ {
		if err := e.PublishText("x"); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.PublishText("x"); !errors.Is(err, ErrTextCapacity) {
		t.Fatal("tiny events bypassed queue bound")
	}
}
