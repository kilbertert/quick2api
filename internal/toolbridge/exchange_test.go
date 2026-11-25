package toolbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/kilbertert/quick2api/internal/chat"
)

func testExchange(t *testing.T, validate func(string, json.RawMessage) error) *Exchange {
	t.Helper()
	e := NewExchange(context.Background(), time.Minute, validate)
	t.Cleanup(e.Close)
	return e
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func nextCall(t *testing.T, e *Exchange) Call {
	t.Helper()
	ev, err := e.Next(testContext(t))
	if err != nil || ev.Call == nil || ev.Final != nil || ev.Err != nil {
		t.Fatalf("expected call, got %+v, %v", ev, err)
	}
	return *ev.Call
}

func receiveOutcome(t *testing.T, answers <-chan outcome) outcome {
	t.Helper()
	select {
	case answer := <-answers:
		return answer
	case <-testContext(t).Done():
		t.Fatal("caller remained blocked")
		return outcome{}
	}
}

func startCall(e *Exchange, ctx context.Context, name string, input json.RawMessage) <-chan outcome {
	answers := make(chan outcome, 1)
	go func() {
		result, err := e.Call(ctx, name, input)
		answers <- outcome{result: result, err: err}
	}()
	return answers
}

func TestExchangeRoundTripClonesAndValidates(t *testing.T) {
	validated := make(chan struct{})
	e := testExchange(t, func(name string, input json.RawMessage) error {
		if name != "Read" || string(input) != `{"path":"fixture"}` {
			return errors.New("wrong tool or parameters")
		}
		input[0] = '[' // Validator cannot mutate the admitted call.
		close(validated)
		return nil
	})
	input := json.RawMessage(`{"path":"fixture"}`)
	answers := startCall(e, testContext(t), "Read", input)
	<-validated
	input[0] = '[' // Call copied its input before admission.
	call := nextCall(t, e)
	if call.Name != "Read" || string(call.Input) != `{"path":"fixture"}` || len(call.ID) < 32 {
		t.Fatalf("invalid admitted call: %+v", call)
	}
	call.Input[0] = '[' // Delivery does not expose internal parameter storage.
	e.mu.Lock()
	if string(e.pending[call.ID].call.Input) != `{"path":"fixture"}` {
		t.Error("delivered call aliases exchange state")
	}
	e.mu.Unlock()
	content := json.RawMessage(`"actual contents"`)
	if err := e.Resolve(Result{ID: call.ID, Content: content, IsError: true}); err != nil {
		t.Fatal(err)
	}
	content[0] = '['
	answer := receiveOutcome(t, answers)
	if answer.err != nil || answer.result.ID != call.ID || string(answer.result.Content) != `"actual contents"` || !answer.result.IsError {
		t.Fatalf("incorrect result: %+v", answer)
	}
	if err := e.Resolve(Result{ID: call.ID, Content: json.RawMessage(`"duplicate"`)}); !errors.Is(err, ErrAlreadyResolved) {
		t.Fatalf("duplicate result was accepted: %v", err)
	}
}

func TestExchangeResolveRequiresKnownDeliveredCall(t *testing.T) {
	e := testExchange(t, nil)
	answers := startCall(e, testContext(t), "Read", json.RawMessage(`{}`))
	ctx := testContext(t)
	var id string
	for id == "" {
		e.mu.Lock()
		for key := range e.pending {
			id = key
		}
		e.mu.Unlock()
		if ctx.Err() != nil {
			t.Fatal("call was not admitted")
		}
		runtime.Gosched()
	}
	if err := e.Resolve(Result{ID: id, Content: json.RawMessage(`"early"`)}); !errors.Is(err, ErrUndelivered) {
		t.Fatalf("undelivered call accepted result: %v", err)
	}
	if err := e.Resolve(Result{ID: "not-a-call", Content: json.RawMessage(`"wrong"`)}); !errors.Is(err, ErrUnknownCall) {
		t.Fatalf("unknown call accepted result: %v", err)
	}
	call := nextCall(t, e)
	if err := e.Resolve(Result{ID: call.ID, Content: json.RawMessage(`invalid`)}); !errors.Is(err, ErrInvalidResult) {
		t.Fatalf("invalid JSON result accepted: %v", err)
	}
	if err := e.Resolve(Result{ID: call.ID, Content: json.RawMessage(`"correct"`)}); err != nil {
		t.Fatal(err)
	}
	if answer := receiveOutcome(t, answers); answer.err != nil || string(answer.result.Content) != `"correct"` {
		t.Fatalf("rejected results consumed call: %+v", answer)
	}
}

func TestExchangeConcurrentCallsResolveInReverseOrder(t *testing.T) {
	e := testExchange(t, nil)
	ctx := testContext(t)
	answers := make(chan outcome, MaxPending)
	for i := 0; i < MaxPending; i++ {
		go func(index int) {
			result, err := e.Call(ctx, fmt.Sprintf("tool-%d", index), json.RawMessage(`{}`))
			if err == nil && string(result.Content) != fmt.Sprintf("%q", fmt.Sprintf("tool-%d", index)) {
				err = fmt.Errorf("crossed tool result for %d: %s", index, result.Content)
			}
			answers <- outcome{result: result, err: err}
		}(i)
	}
	calls := make([]Call, 0, MaxPending)
	ids := make(map[string]bool)
	for i := 0; i < MaxPending; i++ {
		call := nextCall(t, e)
		if ids[call.ID] {
			t.Fatal("duplicate generated ID")
		}
		ids[call.ID] = true
		calls = append(calls, call)
	}
	for i := len(calls) - 1; i >= 0; i-- {
		call := calls[i]
		if err := e.Resolve(Result{ID: call.ID, Content: json.RawMessage(fmt.Sprintf("%q", call.Name))}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < MaxPending; i++ {
		if answer := receiveOutcome(t, answers); answer.err != nil {
			t.Fatal(answer.err)
		}
	}
}

func TestExchangeRejectsValidationAndCapacity(t *testing.T) {
	validationErr := errors.New("not in admitted catalog")
	e := testExchange(t, func(string, json.RawMessage) error { return validationErr })
	if _, err := e.Call(testContext(t), "Write", json.RawMessage(`{}`)); !errors.Is(err, validationErr) {
		t.Fatal(err)
	}
	if _, err := e.Call(testContext(t), "", json.RawMessage(`{}`)); !errors.Is(err, ErrInvalidCall) {
		t.Fatal(err)
	}
	if _, err := e.Call(testContext(t), "Read", json.RawMessage(`oops`)); !errors.Is(err, ErrInvalidCall) {
		t.Fatal(err)
	}
	e.mu.Lock()
	if len(e.pending) != 0 || len(e.queue) != 0 {
		t.Error("invalid call entered queue")
	}
	e.mu.Unlock()

	e = testExchange(t, nil)
	ctx := testContext(t)
	answers := make([]<-chan outcome, 0, MaxPending)
	for i := 0; i < MaxPending; i++ {
		answers = append(answers, startCall(e, ctx, "Read", json.RawMessage(`{}`)))
		nextCall(t, e) // Delivered calls still count toward pending capacity.
	}
	if _, err := e.Call(ctx, "Read", json.RawMessage(`{}`)); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	e.Close()
	for _, answer := range answers {
		if got := receiveOutcome(t, answer); !errors.Is(got.err, ErrClosed) {
			t.Fatal(got.err)
		}
	}
}

func TestExchangeCallAndSubscriberCancellationDoNotCloseSession(t *testing.T) {
	e := testExchange(t, nil)
	ctx, cancel := context.WithCancel(testContext(t))
	answers := startCall(e, ctx, "Read", json.RawMessage(`{}`))
	call := nextCall(t, e)
	cancel()
	if answer := receiveOutcome(t, answers); !errors.Is(answer.err, context.Canceled) {
		t.Fatal(answer.err)
	}
	if err := e.Resolve(Result{ID: call.ID, Content: json.RawMessage(`"late"`)}); !errors.Is(err, ErrCallCancelled) {
		t.Fatal(err)
	}
	select {
	case <-e.Done():
		t.Fatal("one call cancellation closed the exchange")
	default:
	}
	request, disconnect := context.WithCancel(testContext(t))
	disconnect()
	if _, err := e.Next(request); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	answers = startCall(e, testContext(t), "Read", json.RawMessage(`{}`))
	call = nextCall(t, e)
	if err := e.Resolve(Result{ID: call.ID, Content: json.RawMessage(`"after disconnect"`)}); err != nil {
		t.Fatal(err)
	}
	if answer := receiveOutcome(t, answers); answer.err != nil {
		t.Fatal(answer.err)
	}
}

func TestExchangeParentAndTTLReleaseAllWaitersAndExposeFinal(t *testing.T) {
	for _, mode := range []string{"parent", "ttl", "close"} {
		t.Run(mode, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ttl := time.Minute
			if mode == "ttl" {
				ttl = 30 * time.Millisecond
			}
			e := NewExchange(parent, ttl, nil)
			defer e.Close()
			answers := startCall(e, testContext(t), "Read", json.RawMessage(`{}`))
			call := nextCall(t, e)
			want := error(context.Canceled)
			switch mode {
			case "parent":
				cancel()
			case "ttl":
				want = context.DeadlineExceeded
			case "close":
				want = ErrClosed
				e.Close()
			}
			if answer := receiveOutcome(t, answers); !errors.Is(answer.err, want) {
				t.Fatalf("got %v, want %v", answer.err, want)
			}
			request, requestCancel := context.WithCancel(context.Background())
			requestCancel()
			ev, err := e.Next(request) // Already cancelled request can read terminal error.
			if err != nil || !errors.Is(ev.Err, want) || ev.Call != nil {
				t.Fatalf("lost final error: %+v, %v", ev, err)
			}
			if _, err := e.Next(testContext(t)); !errors.Is(err, ErrClosed) {
				t.Fatal(err)
			}
			if err := e.Resolve(Result{ID: call.ID, Content: json.RawMessage(`"late"`)}); !errors.Is(err, ErrCallCancelled) {
				t.Fatal(err)
			}
			if _, err := e.Call(testContext(t), "Read", json.RawMessage(`{}`)); !errors.Is(err, want) {
				t.Fatal(err)
			}
		})
	}
}

func TestExchangeCompleteClonesFinalAndRejectsFurtherCalls(t *testing.T) {
	e := testExchange(t, nil)
	answers := startCall(e, testContext(t), "Read", json.RawMessage(`{}`))
	nextCall(t, e)
	final := &chat.TurnResult{Text: "finished", FinishReason: "stop", Usage: &chat.TurnUsage{InputTokens: 12}}
	e.Complete(final, nil)
	final.Text = "changed"
	final.Usage.InputTokens = 99
	e.Close() // First terminal boundary wins.
	if answer := receiveOutcome(t, answers); !errors.Is(answer.err, ErrCompleted) {
		t.Fatal(answer.err)
	}
	ev, err := e.Next(testContext(t))
	if err != nil || ev.Err != nil || ev.Final == nil || ev.Final.Text != "finished" || ev.Final.Usage.InputTokens != 12 {
		t.Fatalf("invalid completion: %+v %v", ev, err)
	}
	if _, err := e.Call(testContext(t), "Read", json.RawMessage(`{}`)); !errors.Is(err, ErrCompleted) {
		t.Fatal(err)
	}
	if _, err := e.Next(testContext(t)); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	invalid := testExchange(t, nil)
	invalid.Complete(nil, nil)
	ev, err = invalid.Next(testContext(t))
	if err != nil || !errors.Is(ev.Err, ErrInvalidCompletion) {
		t.Fatalf("invalid terminal event: %+v %v", ev, err)
	}
}

func TestExchangeAllowsOnlyOneConcurrentNext(t *testing.T) {
	e := testExchange(t, nil)
	ctx := testContext(t)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ev, err := e.Next(ctx)
		if err != nil || !errors.Is(ev.Err, ErrClosed) {
			t.Errorf("first subscriber: %+v %v", ev, err)
		}
	}()
	for !e.next.Load() {
		if ctx.Err() != nil {
			t.Fatal("subscriber did not start")
		}
		runtime.Gosched()
	}
	if _, err := e.Next(ctx); !errors.Is(err, ErrConcurrentNext) {
		t.Fatal(err)
	}
	e.Close()
	wg.Wait()
}

func TestExchangeCancelledUndeliveredCallsDoNotLeakQueue(t *testing.T) {
	e := testExchange(t, nil)
	deadline := testContext(t)
	for i := 0; i < MaxPending*2; i++ {
		ctx, cancel := context.WithCancel(deadline)
		answers := startCall(e, ctx, "Read", json.RawMessage(`{}`))
		for {
			e.mu.Lock()
			queued := len(e.queue)
			e.mu.Unlock()
			if queued != 0 {
				break
			}
			if deadline.Err() != nil {
				t.Fatal("call was not admitted")
			}
			runtime.Gosched()
		}
		cancel()
		if got := receiveOutcome(t, answers); !errors.Is(got.err, context.Canceled) {
			t.Fatal(got.err)
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.queue) != 0 || len(e.pending) != 0 {
		t.Fatalf("cancelled calls leaked: queue=%d pending=%d", len(e.queue), len(e.pending))
	}
}

func TestExchangeConcurrentDuplicateResultsDeliverOnce(t *testing.T) {
	e := testExchange(t, nil)
	answers := startCall(e, testContext(t), "Read", json.RawMessage(`{}`))
	call := nextCall(t, e)
	errorsReceived := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { errorsReceived <- e.Resolve(Result{ID: call.ID, Content: json.RawMessage(`"once"`)}) }()
	}
	first, second := <-errorsReceived, <-errorsReceived
	if (first == nil) == (second == nil) || (first != nil && !errors.Is(first, ErrAlreadyResolved)) || (second != nil && !errors.Is(second, ErrAlreadyResolved)) {
		t.Fatalf("expected exactly one result delivery: %v, %v", first, second)
	}
	if got := receiveOutcome(t, answers); got.err != nil || string(got.result.Content) != `"once"` {
		t.Fatalf("unexpected delivered result: %+v", got)
	}
}
