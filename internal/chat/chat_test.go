package chat

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/kilbertert/quick2api/internal/session"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestNativeSessionBindingAllowsOnlySameIdentityRefresh(t *testing.T) {
	original := &session.Session{ActorARN: "actor-a", AWSAccountID: "account-a", WSHost: "host-a", OutpostID: "device-a", TenantURL: "tenant-a", IDToken: "old-local-fixture-token"}
	cn := &Conn{sess: original}
	refreshed := *original
	refreshed.IDToken = "refreshed-local-fixture-token"
	if !cn.MatchesSession(&refreshed) {
		t.Fatal("same identity token refresh rejected")
	}
	for _, field := range []string{"actor", "account", "host", "device", "tenant"} {
		changed := refreshed
		switch field {
		case "actor":
			changed.ActorARN = "other"
		case "account":
			changed.AWSAccountID = "other"
		case "host":
			changed.WSHost = "other"
		case "device":
			changed.OutpostID = "other"
		case "tenant":
			changed.TenantURL = "other"
		}
		if cn.MatchesSession(&changed) {
			t.Fatalf("native thread accepted changed %s", field)
		}
	}
}

func TestCollectNativeToolProgressBeforeClientToolReply(t *testing.T) {
	// Recorded event ordering: narration, two native attachment reads, then
	// client tool JSON. Payloads below use only a harmless local echo fixture.
	const narration = "I'll read the attached context first."
	const toolJSON = `{"type":"tool_use","id":"toolu_test","name":"Bash","input":{"command":"echo TOOL_BOUNDARY_OK"}}`
	frames := []Frame{
		{Type: "message_start", Payload: json.RawMessage(`{}`)},
		{Type: "text_delta", Payload: json.RawMessage(`{"delta":"I'll read the attached context first."}`)},
		{Type: "tool_start", Payload: json.RawMessage(`{}`)},
		{Type: "tool_result", Payload: json.RawMessage(`{}`)},
		{Type: "tool_start", Payload: json.RawMessage(`{}`)},
		{Type: "tool_result", Payload: json.RawMessage(`{}`)},
		{Type: "text_delta"},
		{Type: "message_complete", Payload: json.RawMessage(`{}`)},
	}
	frames[6].Payload, _ = json.Marshal(map[string]string{"delta": toolJSON})
	tr := &turn{in: make(chan inbound, len(frames)), emit: func(TurnEvent) {}}
	for i := range frames {
		tr.in <- inbound{f: &frames[i]}
	}
	res, err := tr.collect(context.Background(), &TurnResult{FinishReason: "stop"})
	if err != nil || res.Text != narration+toolJSON || res.ToolText() != toolJSON {
		t.Fatalf("native narration contaminated client tool span: res=%+v err=%v", res, err)
	}
}

func TestCollectRecordedContentFilterError(t *testing.T) {
	// Payload shape recorded from the real 2026-11-23 Desktop fork.
	tr := &turn{in: make(chan inbound, 1), emit: func(TurnEvent) {}}
	tr.in <- inbound{f: &Frame{Type: "error", Payload: json.RawMessage(`{"error":"The model's content filter stopped this reply. Rephrase, or start a new chat.","error_code":"content_filtered","initiator":"user"}`)}}
	res, err := tr.collect(context.Background(), &TurnResult{FinishReason: "stop"})
	if err != nil || res.FinishReason != "content_filter" || !strings.Contains(res.Text, "content filter stopped") {
		t.Fatalf("upstream refusal became an unavailable-model error: res=%+v err=%v", res, err)
	}
}

func TestConversationContinuation(t *testing.T) {
	const head = "System: coding assistant\n\n"
	const tail = "User: check progress"
	cn := &Conn{sentTail: tail, sentHead: head}
	for _, tc := range []struct {
		name string
		opts TurnOpts
		want bool
	}{
		{"tool result", TurnOpts{Text: head + tail + "\n\nToolResult: running", Tail: tail + "\n\nToolResult: running"}, true},
		{"model probe", TurnOpts{Text: "User: hi", Tail: "User: hi"}, false},
		{"fork or compaction", TurnOpts{Text: head + "User: summary", Tail: "User: summary"}, false},
		{"changed system", TurnOpts{Text: "System: new\n\n" + tail + "\n\nUser: next", Tail: tail + "\n\nUser: next"}, false},
		{"retry identical history", TurnOpts{Text: head + tail, Tail: tail}, false},
		{"server tool round", TurnOpts{Text: "ToolResult: ok"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := cn.CanContinue(tc.opts); got != tc.want {
				t.Fatalf("CanContinue=%v, want %v", got, tc.want)
			}
		})
	}
	cn.Close()
	if cn.CanContinue(TurnOpts{}) {
		t.Fatal("closed connection was reusable")
	}
}

func TestAbandonedTurnReleasesFullInboundQueue(t *testing.T) {
	tr := &turn{in: make(chan inbound, 1), done: make(chan struct{})}
	tr.in <- inbound{f: &Frame{Type: "text_delta"}}
	finished := make(chan bool, 1)
	go func() { finished <- tr.deliver(inbound{f: &Frame{Type: "message_complete"}}) }()
	tr.close()
	if <-finished {
		t.Fatal("abandoned turn accepted another frame")
	}
}

func innerB64(t *testing.T, typ, payload string) string {
	t.Helper()
	b, err := json.Marshal(map[string]any{"type": typ, "payload": json.RawMessage(payload)})
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func TestParseEnvelopePayload_FormA(t *testing.T) {
	env := `{"payload":"` + innerB64(t, "text_delta", `{"text":"hi"}`) + `"}`
	f := parseEnvelopePayload([]byte(env))
	if f == nil || f.Type != "text_delta" {
		t.Fatalf("form A: %+v", f)
	}
}

func TestParseEnvelopePayload_FormB(t *testing.T) {
	env := `{"eventType":"x","payload":{"payload":"` + innerB64(t, "message_complete", `{}`) + `"}}`
	f := parseEnvelopePayload([]byte(env))
	if f == nil || f.Type != "message_complete" {
		t.Fatalf("form B: %+v", f)
	}
}

func TestParseEnvelopePayload_DirectInner(t *testing.T) {
	env := `{"type":"session_established","payload":{"x":1}}`
	f := parseEnvelopePayload([]byte(env))
	if f == nil || f.Type != "session_established" {
		t.Fatalf("direct: %+v", f)
	}
}

func TestParseEnvelopePayload_Garbage(t *testing.T) {
	if parseEnvelopePayload([]byte("not json")) != nil {
		t.Fatal("garbage parsed")
	}
	if parseEnvelopePayload([]byte(`{"payload":"not b64"}`)) != nil {
		t.Fatal("bad b64 parsed")
	}
}

func TestSplitText_ASCII(t *testing.T) {
	got := splitText("abcdefghij", 3)
	if len(got) != 4 || strings.Join(got, "") != "abcdefghij" {
		t.Fatalf("got %v", got)
	}
}

func TestSplitText_Runes(t *testing.T) {
	// Each "😀" is 4 bytes. n=5 can only hold one rune per chunk, so the
	// four-rune input becomes four single-rune pieces.
	got := splitText("😀😀😀😀", 5)
	if len(got) != 4 {
		t.Fatalf("got %d chunks %v", len(got), got)
	}
	for _, p := range got {
		if p != "😀" {
			t.Fatalf("chunk %q", p)
		}
		if !utf8.ValidString(p) {
			t.Fatalf("invalid utf8 %q", p)
		}
	}
	if strings.Join(got, "") != "😀😀😀😀" {
		t.Fatalf("roundtrip lost data")
	}
}

func TestDeltaTail(t *testing.T) {
	// No Tail: caller gave no split info — always send Text, keep shadow.
	if send, commit := deltaTail("old", "full", ""); send != "full" || commit != "old" {
		t.Fatalf("no-tail: %q %q", send, commit)
	}
	// First sighting: send the full Text, commit the tail as watermark.
	if send, commit := deltaTail("", "full", "User: a"); send != "full" || commit != "User: a" {
		t.Fatalf("first: %q %q", send, commit)
	}
	// Append-only continuation: send just the unsent suffix.
	send, commit := deltaTail("User: a\n\nAssistant: b\n\n",
		"HEAD User: a\n\nAssistant: b\n\nUser: c",
		"User: a\n\nAssistant: b\n\nUser: c")
	if send != "User: c" || commit != "User: a\n\nAssistant: b\n\nUser: c" {
		t.Fatalf("delta: %q %q", send, commit)
	}
	// Divergence (edited/compacted history): full resend, commit new tail.
	if send, commit := deltaTail("User: a", "full2", "User: x"); send != "full2" || commit != "User: x" {
		t.Fatalf("diverge: %q %q", send, commit)
	}
	// Identical tail (nothing new): full resend, no delta empty send.
	if send, commit := deltaTail("User: a", "full3", "User: a"); send != "full3" || commit != "User: a" {
		t.Fatalf("same: %q %q", send, commit)
	}
}

func TestAppendPromptSuffix(t *testing.T) {
	const suffix = "[LOCAL TOOL RUNTIME]\ncall a tool"
	if got := appendPromptSuffix("User: run ssh\n\n", suffix); got != "User: run ssh\n\n"+suffix {
		t.Fatalf("got %q", got)
	}
	if got := appendPromptSuffix("User: hi", "  "); got != "User: hi" {
		t.Fatalf("blank suffix changed text: %q", got)
	}

	// The suffix is applied only after delta selection, so it never enters
	// the committed tail watermark or breaks the next append-only request.
	first, commit := deltaTail("", "HEAD User: a", "User: a")
	if first = appendPromptSuffix(first, suffix); !strings.HasSuffix(first, suffix) || commit != "User: a" {
		t.Fatalf("first=%q commit=%q", first, commit)
	}
	second, commit := deltaTail(commit, "HEAD User: a\n\nUser: b", "User: a\n\nUser: b")
	if second != "User: b" || commit != "User: a\n\nUser: b" {
		t.Fatalf("second=%q commit=%q", second, commit)
	}
}

func TestDeltaUploadsOnlyNewlyReferencedAttachments(t *testing.T) {
	const head = "System: UTF-8 环境\n\n"
	const previous = "User: old image reference"
	const next = "User: new document reference"
	full := head + previous + "\n\n" + next
	send, _ := deltaTail(previous, full, previous+"\n\n"+next)
	atts := []Attachment{
		{Name: "old.png", TranscriptOffset: len(head) + 1},
		{Name: "new.txt", TranscriptOffset: len(full) - len(next) + 1},
		{Name: "unbound.bin"}, // other adapters/callers retain their behavior
	}
	newAtts := deltaAttachments(full, send, atts)
	if send != next || len(newAtts) != 2 || newAtts[0].Name != "new.txt" || newAtts[1].Name != "unbound.bin" {
		t.Fatalf("delta repeated old attachment or lost a new one: %s %+v", send, newAtts)
	}
	if len(deltaAttachments(full, full, atts)) != 3 || len(deltaAttachments(full, "unrelated", atts)) != 3 {
		t.Fatal("a reseeded or unbound prompt lost its historical attachments")
	}
	if len(atts) != 3 || atts[0].Name != "old.png" {
		t.Fatal("filter mutated the caller's full attachment list")
	}
}
