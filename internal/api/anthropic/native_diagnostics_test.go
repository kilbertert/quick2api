package anthropic

import (
	"encoding/json"
	"testing"
)

func TestNativeToolDiagnosticsExcludeArgumentsAndFreeFormData(t *testing.T) {
	got := nativeToolMetadata(json.RawMessage(`{"tool_name":"Read","arguments":{"command":"Unknown tool"},"result":{"content":[{"text":"Unknown tool: private detail"}]},"isError":true}`))
	if got.Name != "Read" || !got.UnknownToolText || !got.UnknownToolError || !got.IsError {
		t.Fatal("missed the upstream failure classification", got)
	}
	got = nativeToolMetadata(json.RawMessage(`{"name":"tool name with private text","arguments":{"command":"Unknown tool"},"result":{"text":"ok"}}`))
	if got.Name != "" || got.UnknownToolText || got.UnknownToolError || got.IsError {
		t.Fatal("arguments or free-form tool name entered diagnostic metadata", got)
	}
	got = nativeToolMetadata(json.RawMessage(`{"tool_name":"quick2api_client_relay__quick2api_client_call","error":"unknown_tool: omitted"}`))
	if got.Name != "quick2api_client_relay__quick2api_client_call" || !got.UnknownToolText || !got.UnknownToolError || !got.IsError {
		t.Fatal("missed a protocol-level tool failure", got)
	}
}

func TestNativeToolDiagnosticsSeparateHistoricalTextFromStructuredFailure(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		raw                    string
		text, failure, isError bool
	}{
		{"successful history read", `{"tool_name":"file_read","result":{"content":[{"text":"Earlier attempt: Unknown tool"}],"isError":false}}`, true, false, false},
		{"quoted error JSON", `{"result":{"content":[{"text":"{\"error\":\"unknown_tool\"}"}]}}`, true, false, false},
		{"nested MCP failure", `{"result":{"result":{"isError":true,"content":[{"text":"Unknown tool: omitted"}]}}}`, true, true, true},
		{"output failure", `{"output":{"is_error":true,"message":"Unknown tool"}}`, true, true, true},
		{"unrelated sibling failure", `{"result":{"content":[{"text":"Unknown tool in history"}]},"output":{"isError":true,"message":"permission denied"}}`, true, false, true},
		{"unrelated sibling failure reverse order", `{"result":{"isError":true,"message":"permission denied"},"output":{"content":[{"text":"Unknown tool in history"}]}}`, true, false, true},
		{"parent failure with successful child", `{"isError":true,"error":"permission denied","result":{"isError":false,"content":[{"text":"Earlier attempt: Unknown tool"}]}}`, true, false, true},
		{"parent detail with unmarked historical child", `{"isError":true,"error":"permission denied","result":{"content":[{"text":"Earlier attempt: Unknown tool"}]}}`, true, false, true},
		{"parent failure with successful output", `{"isError":true,"output":{"isError":false,"content":[{"text":"Earlier attempt: Unknown tool"}]}}`, true, false, true},
		{"parent marker with unmarked error text", `{"isError":true,"result":{"content":[{"text":"Unknown tool"}]}}`, true, true, true},
		{"false camel case cannot hide true snake case", `{"isError":false,"is_error":true,"message":"unknown_tool"}`, true, true, true},
		{"arguments excluded", `{"arguments":{"error":"Unknown tool","isError":true},"result":"ok"}`, false, false, false},
		{"malformed", `{`, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := nativeToolMetadata(json.RawMessage(tc.raw))
			if got.UnknownToolText != tc.text || got.UnknownToolError != tc.failure || got.IsError != tc.isError {
				t.Fatalf("unexpected classification: %+v", got)
			}
		})
	}
}
