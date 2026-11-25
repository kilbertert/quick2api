package chat

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

func TestConnectionSetupErrorSafeMessageAndCause(t *testing.T) {
	for _, cause := range []error{
		&websocket.CloseError{Code: 1011, Text: "PRIVATE_CLOSE_REASON"},
		errors.New("wss://private-host/?token=PRIVATE_TOKEN"),
		&UpstreamError{Code: "unauthenticated", Message: "PRIVATE_BODY"},
		context.Canceled, context.DeadlineExceeded,
	} {
		err := &ConnectionSetupError{Stage: "session_connect", Err: cause}
		if strings.Contains(err.Error(), "PRIVATE") || strings.Contains(err.Error(), "private-host") || !strings.Contains(err.Error(), "no user message was sent") {
			t.Fatal("unsafe or misleading setup message")
		}
		if !errors.Is(err, cause) {
			t.Fatal("setup lost underlying error")
		}
		var closed *websocket.CloseError
		if errors.As(cause, &closed) && (err.CloseCode() != 1011 || !strings.Contains(err.Error(), "1011")) {
			t.Fatal("close code lost")
		}
		if errors.Is(cause, context.DeadlineExceeded) && !strings.Contains(err.Error(), "timed out") {
			t.Fatal("deadline not distinguished")
		}
		if errors.Is(cause, context.Canceled) && !strings.Contains(err.Error(), "cancelled") {
			t.Fatal("cancellation not distinguished")
		}
	}
}
