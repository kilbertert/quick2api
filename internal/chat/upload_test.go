package chat

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestAttachmentPolicyDoesNotPermitTextFallback(t *testing.T) {
	for _, action := range []string{"BLOCK", "DENY", "REJECT", "block"} {
		for _, status := range []string{"", "SKIPPED", "DISABLED", "NOT_APPLICABLE"} {
			cleared, err := attachmentPolicyDecision(action, status)
			var policy *AttachmentPolicyError
			if cleared || !errors.As(err, &policy) || !policy.Blocked || uploadFallbackAllowed(fmt.Errorf("upload failed: %w", err)) {
				t.Fatalf("blocked input permitted: action=%s status=%s", action, status)
			}
		}
	}
	for _, status := range []string{"SKIPPED", "DISABLED", "NOT_APPLICABLE"} {
		if cleared, err := attachmentPolicyDecision("", status); !cleared || err != nil {
			t.Fatalf("documented skip rejected: %s %v", status, err)
		}
		if cleared, _ := attachmentPolicyDecision("UNKNOWN", status); cleared {
			t.Fatal("unknown policy action accepted as a skip")
		}
	}
	if cleared, err := attachmentPolicyDecision("ALLOW", "COMPLETED"); !cleared || err != nil {
		t.Fatal("ALLOW rejected")
	}
	if cleared, err := attachmentPolicyDecision("", "PENDING"); cleared || err != nil {
		t.Fatal("pending policy was accepted")
	}
	for _, err := range []error{&AttachmentPolicyError{Message: "unsettled"}, context.Canceled, context.DeadlineExceeded} {
		if uploadFallbackAllowed(err) {
			t.Fatalf("unsafe fallback allowed: %v", err)
		}
	}
	if !uploadFallbackAllowed(errors.New("upload transport unavailable")) {
		t.Fatal("ordinary context upload failure lost its fallback")
	}
}

func TestMissingUploadSessionNeverDropsInputAttachments(t *testing.T) {
	// No socket or upstream substitute is provided. This must return before
	// attempting a user_message rather than silently discarding input bytes.
	res, err := (&Client{}).sendPrompt(context.Background(), &turn{}, "unused", TurnOpts{Text: "analyze the attachment", Attachments: []Attachment{{Name: "result.png", MimeType: "image/png", Data: []byte("fixture")}}}, nil)
	if err == nil || res != nil {
		t.Fatal("attachment was silently dropped")
	}
}
