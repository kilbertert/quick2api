package chat

import (
	"context"
	"errors"
	"fmt"

	"github.com/gorilla/websocket"
)

// ConnectionSetupError means no user_message has been sent on this connection.
// Retain the cause for classification without exposing URLs/tokens or arbitrary
// close reasons in user-visible errors and metadata logs.
type ConnectionSetupError struct {
	Stage string
	Err   error
}

func (e *ConnectionSetupError) Unwrap() error { return e.Err }

func (e *ConnectionSetupError) CloseCode() int {
	var closed *websocket.CloseError
	if errors.As(e.Err, &closed) {
		return closed.Code
	}
	return 0
}

func (e *ConnectionSetupError) Error() string {
	status := "failed"
	if errors.Is(e.Err, context.Canceled) {
		status = "was cancelled"
	}
	if errors.Is(e.Err, context.DeadlineExceeded) {
		status = "timed out"
	}
	if code := e.CloseCode(); code != 0 {
		return fmt.Sprintf("Quick connection setup %s at %s (WebSocket close %d); no user message was sent", status, e.Stage, code)
	}
	return fmt.Sprintf("Quick connection setup %s at %s; no user message was sent", status, e.Stage)
}
