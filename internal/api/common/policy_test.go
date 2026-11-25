package common

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/kilbertert/quick2api/internal/chat"
)

func TestAttachmentPolicyErrorMapping(t *testing.T) {
	err := fmt.Errorf("input attachment was not delivered: %w", &chat.AttachmentPolicyError{Blocked: true, Message: "attachment blocked by upstream content policy"})
	status, kind, message := UpstreamError(err)
	if status != http.StatusForbidden || kind != KindPermissionDenied || message != "attachment blocked by upstream content policy" {
		t.Fatalf("policy block became model failure: %d %d %s", status, kind, message)
	}
	status, kind, _ = UpstreamError(&chat.AttachmentPolicyError{Message: "policy check unavailable"})
	if status != http.StatusBadGateway || kind == KindPermissionDenied {
		t.Fatal("unavailable policy check was claimed to be a denial")
	}
}
