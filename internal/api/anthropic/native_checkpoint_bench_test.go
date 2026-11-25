package anthropic

import (
	"encoding/json"
	"strings"
	"testing"
)

func BenchmarkNativeHistoryDigest(b *testing.B) {
	content, _ := json.Marshal(strings.Repeat("synthetic context data ", 320))
	messages := make([]Message, 780)
	for i := range messages {
		messages[i] = Message{Role: "user", Content: content}
	}
	b.SetBytes(int64(len(content) * len(messages)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := nativeHistoryDigest(messages); err != nil {
			b.Fatal(err)
		}
	}
}
