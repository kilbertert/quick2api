package bridge

import (
	"testing"
	"time"

	"github.com/kilbertert/quick2api/internal/chat"
	"github.com/kilbertert/quick2api/internal/config"
)

func TestSweepNeverClosesActiveNativeSocketButShutdownDoes(t *testing.T) {
	busy, idle := &chat.Conn{}, &chat.Conn{}
	b := &Bridge{cfg: config.Defaults(), stopSweep: make(chan struct{}), conns: map[string]*connEntry{
		"active": {conn: busy, lastAt: time.Now().Add(-24 * time.Hour), native: true, busy: true},
		"idle":   {conn: idle, lastAt: time.Now().Add(-24 * time.Hour), native: true},
	}}
	b.sweepOnce()
	if !busy.Alive() || idle.Alive() || len(b.conns) != 1 {
		t.Fatal("sweeper interrupted active native execution")
	}
	b.Close()
	if busy.Alive() || len(b.conns) != 0 {
		t.Fatal("shutdown leaked active socket")
	}
}
