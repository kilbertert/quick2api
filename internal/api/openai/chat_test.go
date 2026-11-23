package openai

import (
	"encoding/json"
	"testing"
)

func TestRenderTranscript_Simple(t *testing.T) {
	var req ChatRequest
	if err := json.Unmarshal([]byte(`{"messages":[{"role":"user","content":"hi"},{"role":"assistant","content":"yes"}]}`), &req); err != nil {
		t.Fatal(err)
	}
	text, _, atts, err := renderTranscript(req.Messages)
	if err != nil || text != "User: hi\n\nAssistant: yes" || len(atts) != 0 {
		t.Fatalf("text=%q atts=%v err=%v", text, atts, err)
	}
}

func TestRenderTranscript_SystemAndDeveloper(t *testing.T) {
	var req ChatRequest
	if err := json.Unmarshal([]byte(`{"messages":[{"role":"system","content":"sys"},{"role":"developer","content":"dev"},{"role":"user","content":"u"}]}`), &req); err != nil {
		t.Fatal(err)
	}
	text, _, _, _ := renderTranscript(req.Messages)
	if text != "System: sys\n\nSystem: dev\n\nUser: u" {
		t.Fatalf("text=%q", text)
	}
}

func TestRenderTranscript_ImageURL(t *testing.T) {
	var req ChatRequest
	body := `{"messages":[{"role":"user","content":[
		{"type":"image_url","image_url":{"url":"data:image/png;base64,iVBORw0KGgoAAAANSUhEUg=="}}
	]}]}`
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatal(err)
	}
	_, _, atts, err := renderTranscript(req.Messages)
	if err != nil || len(atts) != 1 {
		t.Fatalf("atts=%v err=%v", atts, err)
	}
	if atts[0].MimeType != "image/png" {
		t.Fatalf("att %+v", atts[0])
	}
}

func TestRenderTranscript_File(t *testing.T) {
	var req ChatRequest
	body := `{"messages":[{"role":"user","content":[
		{"type":"file","file":{"filename":"a.txt","file_data":"data:text/plain;base64,aGVsbG8="}}
	]}]}`
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatal(err)
	}
	_, _, atts, err := renderTranscript(req.Messages)
	if err != nil || len(atts) != 1 || string(atts[0].Data) != "hello" {
		t.Fatalf("atts=%v err=%v", atts, err)
	}
}
