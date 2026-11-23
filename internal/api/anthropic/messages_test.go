package anthropic

import (
	"encoding/json"
	"testing"
)

func req(t *testing.T, body string) *MessagesRequest {
	t.Helper()
	var r MessagesRequest
	if err := json.Unmarshal([]byte(body), &r); err != nil {
		t.Fatal(err)
	}
	return &r
}

func TestRenderPrompt_Plain(t *testing.T) {
	r := req(t, `{"messages":[{"role":"user","content":"hi"}]}`)
	text, _, _, atts, err := renderPrompt(r)
	if err != nil || text != "User: hi" || len(atts) != 0 {
		t.Fatalf("text=%q atts=%v err=%v", text, atts, err)
	}
}

func TestRenderPrompt_System(t *testing.T) {
	r := req(t, `{"system":"be terse","messages":[{"role":"user","content":"hi"}]}`)
	text, _, _, _, _ := renderPrompt(r)
	if text != "System: be terse\n\nUser: hi" {
		t.Fatalf("text=%q", text)
	}
}

func TestRenderPrompt_Assistant(t *testing.T) {
	r := req(t, `{"messages":[{"role":"assistant","content":"yes"}]}`)
	text, _, _, _, _ := renderPrompt(r)
	if text != "Assistant: yes" {
		t.Fatalf("text=%q", text)
	}
}

func TestRenderPrompt_ImageBase64(t *testing.T) {
	png := "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5+hHgAHggJ/PchI7wAAAABJRU5ErkJggg=="
	body := `{"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"` + png + `"}}]}]}`
	r := req(t, body)
	_, _, _, atts, err := renderPrompt(r)
	if err != nil || len(atts) != 1 {
		t.Fatalf("atts=%v err=%v", atts, err)
	}
	if atts[0].MimeType != "image/png" || len(atts[0].Data) == 0 {
		t.Fatalf("att %+v", atts[0])
	}
}

func TestRenderPrompt_ToolUse(t *testing.T) {
	r := req(t, `{"messages":[{"role":"assistant","content":[{"type":"tool_use","id":"x","name":"Bash","input":{"c":"ls"}}]}]}`)
	text, _, _, _, _ := renderPrompt(r)
	if text != `ToolUse Bash({"c":"ls"})` {
		t.Fatalf("text=%q", text)
	}
}

func TestConversationIDOf_Header(t *testing.T) {
	// header lookup is easiest via httptest — skip, exercised by e2e.
}
