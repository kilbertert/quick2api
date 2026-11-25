package common

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/kilbertert/quick2api/internal/chat"
)

func TestSetupErrorPreservesClassificationAndSafeMessage(t *testing.T) {
	for _, tc := range []struct {
		cause  error
		status int
		kind   UpstreamKind
	}{
		{&chat.UpstreamError{Code: "unauthenticated", Message: "PRIVATE_BODY"}, 401, KindUnauthenticated},
		{&chat.UpstreamError{Code: "token_expired", Message: "PRIVATE_BODY"}, 401, KindUnauthenticated},
		{&chat.UpstreamError{Code: "rate_limited", Message: "PRIVATE_BODY"}, 429, KindRateLimited},
		{&chat.UpstreamError{Code: "server", Message: "PRIVATE_BODY"}, 502, KindUpstream},
		{&websocket.CloseError{Code: 1011, Text: "PRIVATE_REASON"}, 502, KindUpstream},
		{context.DeadlineExceeded, http.StatusGatewayTimeout, KindTimeout},
		{errors.New("PRIVATE_DIAL_URL"), 502, KindUpstream},
	} {
		err := &chat.ConnectionSetupError{Stage: "session_connect", Err: tc.cause}
		status, kind, message := UpstreamError(err)
		if status != tc.status || kind != tc.kind || message != err.Error() || strings.Contains(message, "PRIVATE") {
			t.Fatal("setup classification or safe message lost", status, kind)
		}
	}
}
