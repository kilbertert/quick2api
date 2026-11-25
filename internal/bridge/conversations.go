package bridge

import (
	"context"
	"sync"
)

// conversationLocks serializes a logical conversation, including its first
// dial and lane selection. References include waiters so an entry cannot be
// replaced while another request is waiting. Different conversations proceed
// independently; cancelled waiters do not acquire the upstream connection.
type conversationLocks struct {
	mu      sync.Mutex
	entries map[string]*conversationLock
}

type conversationLock struct {
	gate chan struct{}
	refs int
}

func (l *conversationLocks) acquire(ctx context.Context, id string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id == "" {
		return func() {}, nil
	}
	l.mu.Lock()
	if l.entries == nil {
		l.entries = make(map[string]*conversationLock)
	}
	e := l.entries[id]
	if e == nil {
		e = &conversationLock{gate: make(chan struct{}, 1)}
		l.entries[id] = e
	}
	e.refs++
	l.mu.Unlock()
	drop := func() {
		l.mu.Lock()
		e.refs--
		if e.refs == 0 {
			delete(l.entries, id)
		}
		l.mu.Unlock()
	}
	select {
	case e.gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-e.gate
			drop()
			return nil, err
		}
		return func() { <-e.gate; drop() }, nil
	case <-ctx.Done():
		drop()
		return nil, ctx.Err()
	}
}
