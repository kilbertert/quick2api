package chat

import (
	"context"
	"sync"

	"github.com/kilbertert/quick2api/internal/session"
)

// A live handshake-only probe reproduced 1011 during concurrent cold connects
// for one registered device, without sending any prompt. Serialize setup, not
// inference. Bridge creates a Client per turn, so ownership must cross Clients.
type handshakeIdentity struct {
	host, tenant, actor, account, device string
}

type handshakeGate struct {
	ready chan struct{}
	refs  int
}

var handshakeSetup = struct {
	sync.Mutex
	entries map[handshakeIdentity]*handshakeGate
}{entries: make(map[handshakeIdentity]*handshakeGate)}

func acquireHandshake(ctx context.Context, sess *session.Session) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key := handshakeIdentity{sess.WSHost, sess.TenantURL, sess.ActorARN, sess.AWSAccountID, sess.OutpostID}
	handshakeSetup.Lock()
	g := handshakeSetup.entries[key]
	if g == nil {
		g = &handshakeGate{ready: make(chan struct{}, 1)}
		handshakeSetup.entries[key] = g
	}
	g.refs++
	handshakeSetup.Unlock()
	drop := func() {
		handshakeSetup.Lock()
		g.refs--
		if g.refs == 0 {
			delete(handshakeSetup.entries, key)
		}
		handshakeSetup.Unlock()
	}
	select {
	case g.ready <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-g.ready
			drop()
			return nil, err
		}
		var once sync.Once
		return func() { once.Do(func() { <-g.ready; drop() }) }, nil
	case <-ctx.Done():
		drop()
		return nil, ctx.Err()
	}
}
