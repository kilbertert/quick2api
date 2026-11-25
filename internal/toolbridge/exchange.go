// Package toolbridge coordinates client tool requests across HTTP lifetimes.
// It does not register Quick tools, run client tools, or implement wire framing.
package toolbridge

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kilbertert/quick2api/internal/chat"
)

const (
	MaxPending   = 32
	defaultTTL   = 5 * time.Minute
	retiredLimit = 128
)

var (
	ErrClosed            = errors.New("tool exchange is closed")
	ErrCompleted         = errors.New("tool exchange has completed")
	ErrCapacity          = errors.New("tool exchange pending-call limit reached")
	ErrUnknownCall       = errors.New("unknown or expired tool call")
	ErrUndelivered       = errors.New("tool call has not been delivered to the client")
	ErrAlreadyResolved   = errors.New("tool result has already been consumed")
	ErrCallCancelled     = errors.New("tool call was cancelled")
	ErrConcurrentNext    = errors.New("tool exchange already has an active Next subscriber")
	ErrInvalidCall       = errors.New("tool call requires a name and valid JSON input")
	ErrInvalidResult     = errors.New("tool result requires valid JSON content")
	ErrInvalidCompletion = errors.New("tool exchange completed without a result or error")
	ErrTextCapacity      = errors.New("tool exchange unread text exceeds 1 MiB or 4096 events")
)

type Call struct {
	ID, Name string
	Input    json.RawMessage
}

type Result struct {
	ID      string
	Content json.RawMessage
	IsError bool
}

// Event has one call or a terminal result/error. Terminal events are readable
// once, including when the exchange context has already ended.
type Event struct {
	Text  string
	Call  *Call
	Final *chat.TurnResult
	Err   error
}

type outcome struct {
	result Result
	err    error
}

type pendingCall struct {
	call      Call
	ctx       context.Context
	delivered bool
	answer    chan outcome
}

// Exchange owns a bounded tool exchange independently of adapter request
// contexts. The parent must be a session/service context, not the first HTTP
// request context. The adapter decides whether a disconnected client warrants
// Close; cancelling Next or one Call does not cancel other calls.
type Exchange struct {
	ctx      context.Context
	cancel   context.CancelFunc
	validate func(string, json.RawMessage) error
	done     chan struct{}
	wake     chan struct{}
	next     atomic.Bool

	mu             sync.Mutex
	pending        map[string]*pendingCall
	queue          []Event
	textBytes      int
	textEvents     int
	retired        map[string]error
	retiredOrder   []string
	terminal       bool
	final          Event
	finalDelivered bool
}

// NewExchange starts a bounded session lifetime. Nonpositive ttl selects five
// minutes. Validation runs before admission and must be safe for concurrent use.
func NewExchange(parent context.Context, ttl time.Duration, validate func(string, json.RawMessage) error) *Exchange {
	if parent == nil {
		parent = context.Background()
	}
	if ttl <= 0 {
		ttl = defaultTTL
	}
	ctx, cancel := context.WithTimeout(parent, ttl)
	e := &Exchange{
		ctx: ctx, cancel: cancel, validate: validate,
		done: make(chan struct{}), wake: make(chan struct{}, 1),
		pending: make(map[string]*pendingCall), retired: make(map[string]error),
	}
	go func() {
		<-ctx.Done()
		e.finish(nil, ctx.Err())
	}()
	return e
}

func (e *Exchange) Done() <-chan struct{} { return e.done }

// Call admits a validated request and waits for the actual client result.
// Cancellation retires this call without cancelling the exchange. Input and
// returned result bytes do not alias caller-owned storage.
func (e *Exchange) Call(ctx context.Context, name string, input json.RawMessage) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if name == "" || !json.Valid(input) {
		return Result{}, ErrInvalidCall
	}
	input = cloneJSON(input)
	if e.validate != nil {
		if err := e.validate(name, cloneJSON(input)); err != nil {
			return Result{}, fmt.Errorf("validate client tool %q: %w", name, err)
		}
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return Result{}, fmt.Errorf("generate client tool call id: %w", err)
	}
	id := "toolu_q2a_" + hex.EncodeToString(random[:])
	p := &pendingCall{call: Call{ID: id, Name: name, Input: input}, ctx: ctx, answer: make(chan outcome, 1)}
	e.syncContext()
	e.mu.Lock()
	if e.terminal {
		err := e.terminalError()
		e.mu.Unlock()
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		e.mu.Unlock()
		return Result{}, err
	}
	if len(e.pending) >= MaxPending {
		e.mu.Unlock()
		return Result{}, ErrCapacity
	}
	e.pending[id] = p
	e.queue = append(e.queue, Event{Call: &p.call})
	e.signal()
	e.mu.Unlock()
	select {
	case answer := <-p.answer:
		return answer.result, answer.err
	case <-ctx.Done():
		e.mu.Lock()
		if _, exists := e.pending[id]; exists {
			e.remove(id, ErrCallCancelled)
			e.mu.Unlock()
			return Result{}, ctx.Err()
		}
		// Resolve or terminal completion already committed an answer while
		// holding mu. Prefer it to a simultaneous request cancellation.
		e.mu.Unlock()
		answer := <-p.answer
		return answer.result, answer.err
	case <-e.done:
		answer := <-p.answer
		return answer.result, answer.err
	}
}

// Next has a single concurrent subscriber. Cancelling its context leaves the
// exchange alive and does not mark any undelivered call as delivered.
func (e *Exchange) Next(ctx context.Context) (Event, error) {
	if !e.next.CompareAndSwap(false, true) {
		return Event{}, ErrConcurrentNext
	}
	defer e.next.Store(false)
	for {
		e.syncContext()
		e.mu.Lock()
		// Preserve text/call ordering, including text produced just before a
		// terminal completion. finish removes only unresolved tool calls.
		if len(e.queue) > 0 && e.queue[0].Call == nil {
			if err := ctx.Err(); err != nil {
				e.mu.Unlock()
				return Event{}, err
			}
			ev := e.queue[0]
			e.queue[0] = Event{}
			e.queue = e.queue[1:]
			e.textBytes -= len(ev.Text)
			e.textEvents--
			e.mu.Unlock()
			return ev, nil
		}
		if e.terminal {
			if e.finalDelivered {
				e.mu.Unlock()
				return Event{}, ErrClosed
			}
			e.finalDelivered = true
			ev := Event{Final: cloneFinal(e.final.Final), Err: e.final.Err}
			e.mu.Unlock()
			return ev, nil
		}
		if err := ctx.Err(); err != nil {
			e.mu.Unlock()
			return Event{}, err
		}
		for len(e.queue) > 0 {
			if e.queue[0].Call == nil {
				break
			}
			id := e.queue[0].Call.ID
			p := e.pending[id]
			if err := p.ctx.Err(); err != nil {
				e.remove(id, ErrCallCancelled)
				p.answer <- outcome{err: err}
				continue
			}
			e.queue[0] = Event{}
			e.queue = e.queue[1:]
			p.delivered = true
			call := p.call
			call.Input = cloneJSON(call.Input)
			e.mu.Unlock()
			return Event{Call: &call}, nil
		}
		if len(e.queue) > 0 {
			e.mu.Unlock()
			continue
		}
		e.mu.Unlock()
		select {
		case <-e.wake:
		case <-e.done:
		case <-ctx.Done():
			// Recheck terminal state first so its error remains observable.
		}
	}
}

// PublishText queues genuine upstream text without interpreting it as a tool.
// Slow readers have a fixed budget; exhaustion fails instead of dropping text
// or blocking the upstream reader indefinitely.
func (e *Exchange) PublishText(text string) error {
	if text == "" {
		return nil
	}
	e.syncContext()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.terminal {
		return e.terminalError()
	}
	if len(text) > (1<<20)-e.textBytes || e.textEvents >= 4096 {
		return ErrTextCapacity
	}
	e.queue = append(e.queue, Event{Text: text})
	e.textBytes += len(text)
	e.textEvents++
	e.signal()
	return nil
}

// Resolve consumes one delivered call exactly once. It never runs a tool.
// Recent consumed/cancelled IDs have explicit errors; older IDs are unknown.
func (e *Exchange) Resolve(result Result) error {
	if !json.Valid(result.Content) {
		return ErrInvalidResult
	}
	result.Content = cloneJSON(result.Content)
	e.syncContext()
	e.mu.Lock()
	defer e.mu.Unlock()
	if retired, exists := e.retired[result.ID]; exists {
		return retired
	}
	if e.terminal {
		return e.terminalError()
	}
	p, exists := e.pending[result.ID]
	if !exists {
		return ErrUnknownCall
	}
	if err := p.ctx.Err(); err != nil {
		e.remove(result.ID, ErrCallCancelled)
		p.answer <- outcome{err: err}
		return ErrCallCancelled
	}
	if !p.delivered {
		return ErrUndelivered
	}
	e.remove(result.ID, ErrAlreadyResolved)
	p.answer <- outcome{result: result}
	return nil
}

// Complete publishes a terminal event once and releases unresolved callers.
// Final completion must follow the upstream terminal boundary; outstanding
// client calls are cancelled rather than being advertised as successful.
func (e *Exchange) Complete(final *chat.TurnResult, err error) {
	if final == nil && err == nil {
		err = ErrInvalidCompletion
	}
	e.finish(final, err)
}

func (e *Exchange) Close() { e.finish(nil, ErrClosed) }

func (e *Exchange) syncContext() {
	if err := e.ctx.Err(); err != nil {
		e.finish(nil, err)
	}
}

func (e *Exchange) finish(final *chat.TurnResult, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.terminal {
		return
	}
	e.terminal = true
	e.final = Event{Final: cloneFinal(final), Err: err}
	waitErr := e.terminalError()
	for id, p := range e.pending {
		e.retire(id, ErrCallCancelled)
		p.answer <- outcome{err: waitErr}
	}
	clear(e.pending)
	kept := e.queue[:0]
	for _, ev := range e.queue {
		if ev.Call == nil {
			kept = append(kept, ev)
		}
	}
	clear(e.queue[len(kept):])
	e.queue = kept
	close(e.done)
	e.cancel()
}

// terminalError is called with mu held; a successful completion still rejects
// new calls and releases any unresolved callers with a non-success outcome.
func (e *Exchange) terminalError() error {
	if e.final.Err != nil {
		return e.final.Err
	}
	return ErrCompleted
}

func (e *Exchange) signal() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

func (e *Exchange) remove(id string, reason error) {
	delete(e.pending, id)
	for i, queued := range e.queue {
		if queued.Call != nil && queued.Call.ID == id {
			copy(e.queue[i:], e.queue[i+1:])
			e.queue[len(e.queue)-1] = Event{}
			e.queue = e.queue[:len(e.queue)-1]
			break
		}
	}
	e.retire(id, reason)
}

func (e *Exchange) retire(id string, reason error) {
	if len(e.retiredOrder) >= retiredLimit {
		delete(e.retired, e.retiredOrder[0])
		e.retiredOrder = e.retiredOrder[1:]
	}
	e.retired[id] = reason
	e.retiredOrder = append(e.retiredOrder, id)
}

func cloneJSON(raw json.RawMessage) json.RawMessage { return append(json.RawMessage(nil), raw...) }

func cloneFinal(final *chat.TurnResult) *chat.TurnResult {
	if final == nil {
		return nil
	}
	copy := *final
	if final.Usage != nil {
		usage := *final.Usage
		copy.Usage = &usage
	}
	return &copy
}
