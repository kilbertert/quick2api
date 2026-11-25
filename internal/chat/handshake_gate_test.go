package chat

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kilbertert/quick2api/internal/session"
)

func gateSession(t *testing.T) *session.Session {
	t.Helper()
	return &session.Session{WSHost: t.Name(), TenantURL: "tenant", ActorARN: "actor", AWSAccountID: "account", OutpostID: "device", IDToken: "old-token"}
}

func assertGateRemoved(t *testing.T, s *session.Session) {
	t.Helper()
	handshakeSetup.Lock()
	defer handshakeSetup.Unlock()
	key := handshakeIdentity{s.WSHost, s.TenantURL, s.ActorARN, s.AWSAccountID, s.OutpostID}
	if handshakeSetup.entries[key] != nil {
		t.Fatal("setup gate retained after all owners and waiters exited")
	}
}

func waitGateReferences(t *testing.T, s *session.Session, want int) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	key := handshakeIdentity{s.WSHost, s.TenantURL, s.ActorARN, s.AWSAccountID, s.OutpostID}
	for {
		handshakeSetup.Lock()
		g := handshakeSetup.entries[key]
		ready := g != nil && g.refs == want
		handshakeSetup.Unlock()
		if ready {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("waiter did not enter setup queue")
		case <-tick.C:
		}
	}
}

func TestHandshakeGateAcrossClientsAndTokenRefresh(t *testing.T) {
	s := gateSession(t)
	release, err := acquireHandshake(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	refreshed := *s
	refreshed.IDToken = "new-token"
	refreshed.OutpostToken = "new-outpost-token"
	for range 2 {
		// A new Client cannot bypass another Client's setup, even after refresh.
		c := &Client{Proxy: ":invalid-proxy"}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		var stages []string
		_, err := c.newConversation(ctx, &refreshed, "", func(ev TurnEvent) { stages = append(stages, ev.Text) })
		cancel()
		var setup *ConnectionSetupError
		if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &setup) || setup.Stage != "connection_queue" {
			t.Fatal("waiter bypassed gate or lost cancellation", err)
		}
		if len(stages) != 1 || stages[0] != "connection_queued" {
			t.Fatal("cancelled waiter attempted a dial", stages)
		}
	}
	release()
	release() // ownership release is idempotent
	assertGateRemoved(t, s)
}

func TestHandshakeGateSeparatesIdentities(t *testing.T) {
	s := gateSession(t)
	release, err := acquireHandshake(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	for _, field := range []string{"host", "tenant", "actor", "account", "device"} {
		t.Run(field, func(t *testing.T) {
			other := *s
			switch field {
			case "host":
				other.WSHost += "-other"
			case "tenant":
				other.TenantURL += "-other"
			case "actor":
				other.ActorARN += "-other"
			case "account":
				other.AWSAccountID += "-other"
			case "device":
				other.OutpostID += "-other"
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			unlock, err := acquireHandshake(ctx, &other)
			if err != nil {
				t.Fatal("independent identity blocked", err)
			}
			unlock()
			assertGateRemoved(t, &other)
		})
	}
	release()
	assertGateRemoved(t, s)
}

func TestHandshakeGateCancellationAndReleaseRace(t *testing.T) {
	s := gateSession(t)
	for range 100 {
		release, err := acquireHandshake(context.Background(), s)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock, err := acquireHandshake(ctx, s)
			if err == nil {
				unlock()
			} else if !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		}()
		waitGateReferences(t, s, 2)
		cancel()
		release()
		wg.Wait()
		assertGateRemoved(t, s)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := acquireHandshake(ctx, s); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	assertGateRemoved(t, s)
}

func TestHandshakeDialFailureReleasesGate(t *testing.T) {
	s := gateSession(t)
	for range 2 {
		// Parsing fails before networking. A failed first setup must not retain
		// the gate and prevent the next independent setup from starting.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := (&Client{Proxy: ":invalid-proxy"}).newConversation(ctx, s, "", nil)
		cancel()
		var setup *ConnectionSetupError
		if !errors.As(err, &setup) || setup.Stage != "websocket_dial" {
			t.Fatal("unexpected setup failure", err)
		}
		assertGateRemoved(t, s)
	}
}
