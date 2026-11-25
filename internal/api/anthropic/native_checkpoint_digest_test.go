package anthropic

import (
	"crypto/sha256"
	"encoding/json"
	"testing"
)

func TestNativeHistoryDigestPreservesCanonicalFingerprint(t *testing.T) {
	// Compare to the existing full-array format, including exact JSON numbers,
	// field ordering, and transport-only content representation differences.
	for _, messages := range [][]Message{
		nil, {},
		{{Role: "user", Content: json.RawMessage(`"text <>& 汉字"`)}},
		{
			{Role: "assistant", Content: json.RawMessage(`[{"type":"tool_use","id":"call_1","name":"Read","input":{"n":9007199254740993,"cache_control":"actual tool data"},"cache_control":{"type":"ephemeral"}}]`)},
			{Role: "user", Content: json.RawMessage(`[{"type":"tool_result","tool_use_id":"call_1","content":[{"text":"result","type":"text","cache_control":{"type":"ephemeral"}}]}]`)},
		},
	} {
		copy := append([]Message(nil), messages...)
		for i := range copy {
			var err error
			copy[i].Content, err = nativeContent(copy[i].Content)
			if err != nil {
				t.Fatal(err)
			}
		}
		data, err := json.Marshal(copy)
		if err != nil {
			t.Fatal(err)
		}
		data, err = canonicalJSON(data)
		if err != nil {
			t.Fatal(err)
		}
		got, err := nativeHistoryDigest(messages)
		if err != nil || got != sha256.Sum256(data) {
			t.Fatal("streamed hash changed the completed-history fingerprint", err)
		}
	}
}

func TestNativeHistoryDigestNormalizationRetainsExecutionData(t *testing.T) {
	digest := func(raw string) [32]byte {
		t.Helper()
		value, err := nativeHistoryDigest([]Message{{Role: "user", Content: json.RawMessage(raw)}})
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	if digest(`"same text"`) != digest(`[{"text":"same text","cache_control":{"type":"ephemeral"},"type":"text"}]`) {
		t.Fatal("transport-only representation changed fingerprint")
	}
	base := `[{"type":"tool_use","id":"one","name":"Example","input":{"n":9007199254740993,"cache_control":"keep"}}]`
	for _, changed := range []string{
		`[{"type":"tool_use","id":"one","name":"Example","input":{"n":9007199254740992,"cache_control":"keep"}}]`,
		`[{"type":"tool_use","id":"two","name":"Example","input":{"n":9007199254740993,"cache_control":"keep"}}]`,
		`[{"type":"tool_use","id":"one","name":"Example","input":{"n":9007199254740993,"cache_control":"changed"}}]`,
	} {
		if digest(base) == digest(changed) {
			t.Fatal("execution data was lost during incremental hashing")
		}
	}
}
