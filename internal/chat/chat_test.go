package chat

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"
)

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
