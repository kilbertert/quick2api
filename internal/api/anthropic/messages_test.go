package anthropic

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kilbertert/quick2api/internal/api/common"
	"github.com/kilbertert/quick2api/internal/chat"
)

func TestRefusalDoesNotBecomeSuccessfulToolCall(t *testing.T) {
	toolJSON := `{"type":"tool_use","name":"Bash","input":{"command":"echo test"}}`
	content, stop := responseContent(&chat.TurnResult{Text: toolJSON, FinishReason: "content_filter"}, nil)
	if stop != "refusal" || len(content) != 1 || content[0].Type != "text" || content[0].Text != toolJSON {
		t.Fatalf("blocked output became a tool invocation: %s %+v", stop, content)
	}
}

func TestClientToolReplyAfterNativeProgress(t *testing.T) {
	const progress = "先读取附件上下文。"
	const toolJSON = `{"type":"tool_use","id":"toolu_test","name":"Bash","input":{"command":"echo TOOL_BOUNDARY_OK"}}`
	res := &chat.TurnResult{Text: progress + toolJSON, ToolTextOffset: len(progress), FinishReason: "stop"}
	content, stop := responseContent(res, nil)
	if stop != "tool_use" || len(content) != 1 || content[0].Name != "Bash" || string(content[0].Input) != `{"command":"echo TOOL_BOUNDARY_OK"}` {
		t.Fatalf("native progress hid client tool invocation: %s %+v", stop, content)
	}
	// Without a native tool boundary this is arbitrary prose containing
	// JSON, not an instruction to extract and execute the embedded object.
	res.ToolTextOffset = 0
	content, stop = responseContent(res, nil)
	if stop != "end_turn" || content[0].Type != "text" || content[0].Text != res.Text {
		t.Fatalf("prose was promoted to a tool call: %s %+v", stop, content)
	}
	res.ToolTextOffset = len(progress)
	res.FinishReason = "content_filter"
	content, stop = responseContent(res, nil)
	if stop != "refusal" || content[0].Type != "text" {
		t.Fatalf("blocked reply became a tool call: %s %+v", stop, content)
	}
}

func TestNormalReplyRetainsNativeProgress(t *testing.T) {
	res := &chat.TurnResult{Text: "Reading context.\nHere is the summary.", ToolTextOffset: len("Reading context.\n"), FinishReason: "stop"}
	content, stop := responseContent(res, nil)
	if stop != "end_turn" || content[0].Text != res.Text {
		t.Fatalf("normal reply lost text: %s %+v", stop, content)
	}
}

const desktopTools = `[{"name":"mcp__ccd_session_mgmt__list_sessions","input_schema":{"type":"object","properties":{"limit":{"type":"number"}},"additionalProperties":false}}]`
const desktopCall = `{"type":"tool_use","id":"toolu_ls01","name":"mcp__ccd_session_mgmt__list_sessions","input":{"limit":10}}`
const desktopNarration = "Need to find the shell session's local id."

func TestDesktopNarrationAndToolReply(t *testing.T) {
	catalog := common.NewClientTools(json.RawMessage(desktopTools), nil)
	for _, nativeProgress := range []string{"", "Reading attachment context."} {
		res := &chat.TurnResult{Text: nativeProgress + desktopNarration + "\n\n" + desktopCall, ToolTextOffset: len(nativeProgress), FinishReason: "stop"}
		content, stop := responseContent(res, catalog)
		if stop != "tool_use" || len(content) != 2 || content[0].Type != "text" || content[0].Text != desktopNarration || content[1].Type != "tool_use" || content[1].ID != "toolu_ls01" || content[1].Name != "mcp__ccd_session_mgmt__list_sessions" || string(content[1].Input) != `{"limit":10}` {
			t.Fatalf("Desktop reply was not mapped: stop=%s content=%+v", stop, content)
		}
	}
}

func TestDesktopToolPromotionRequiresCompleteAllowedReply(t *testing.T) {
	for _, test := range []struct {
		name, finish, tools, choice, stop string
		partial, server                   bool
	}{
		{name: "filtered", finish: "content_filter", tools: desktopTools, stop: "refusal"},
		{name: "partial", finish: "stop", tools: desktopTools, partial: true, stop: "end_turn"},
		{name: "unfinished", tools: desktopTools, stop: "end_turn"},
		{name: "undeclared", finish: "stop", tools: `[]`, stop: "end_turn"},
		{name: "disabled", finish: "stop", tools: desktopTools, choice: `{"type":"none"}`, stop: "end_turn"},
		{name: "server mode", finish: "stop", server: true, stop: "end_turn"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var catalog *common.ClientTools
			if !test.server {
				catalog = common.NewClientTools(json.RawMessage(test.tools), json.RawMessage(test.choice))
			}
			res := &chat.TurnResult{Text: desktopNarration + "\n\n" + desktopCall, FinishReason: test.finish, PartialOnClose: test.partial}
			content, stop := responseContent(res, catalog)
			if stop != test.stop || len(content) != 1 || content[0].Type != "text" || content[0].Text != res.Text {
				t.Fatalf("inert reply was promoted: %s %+v", stop, content)
			}
		})
	}
}

func TestDesktopParallelToolSSE(t *testing.T) {
	second := strings.Replace(desktopCall, `"toolu_ls01"`, `"toolu_ls02"`, 1)
	catalog := common.NewClientTools(json.RawMessage(desktopTools), nil)
	content, stop := responseContent(&chat.TurnResult{Text: desktopNarration + "\n\n[" + desktopCall + "," + second + "]", FinishReason: "stop"}, catalog)
	if stop != "tool_use" || len(content) != 3 {
		t.Fatalf("parallel calls lost: %s %+v", stop, content)
	}
	var wire strings.Builder
	emitToolBlocks(func(event string, data map[string]any) bool { return writeSSE(&wire, event, data) == nil }, content)
	packets := strings.Split(strings.TrimSpace(wire.String()), "\n\n")
	if len(packets) != 6 {
		t.Fatalf("want three events per tool, got %d", len(packets))
	}
	for i, packet := range packets {
		lines := strings.Split(packet, "\n")
		var event struct {
			Type  string       `json:"type"`
			Index int          `json:"index"`
			Block ContentBlock `json:"content_block"`
			Delta struct {
				Type string `json:"type"`
				JSON string `json:"partial_json"`
			} `json:"delta"`
		}
		if len(lines) != 2 || json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "data: ")), &event) != nil || lines[0] != "event: "+event.Type || event.Index != 1+i/3 {
			t.Fatalf("invalid SSE packet: %s", packet)
		}
		switch i % 3 {
		case 0:
			if event.Type != "content_block_start" || event.Block.Type != "tool_use" || string(event.Block.Input) != `{}` || event.Block.ID != content[1+i/3].ID {
				t.Fatalf("bad tool start: %s", packet)
			}
		case 1:
			if event.Type != "content_block_delta" || event.Delta.Type != "input_json_delta" || event.Delta.JSON != `{"limit":10}` {
				t.Fatalf("bad input delta: %s", packet)
			}
		case 2:
			if event.Type != "content_block_stop" {
				t.Fatalf("bad tool stop: %s", packet)
			}
		}
	}
	if strings.Contains(wire.String(), "text_delta") || strings.Count(wire.String(), "partial_json") != 2 {
		t.Fatal("tool JSON leaked as text or duplicate arguments")
	}
	for _, failAt := range []int{1, 2, 3} {
		count := 0
		emitToolBlocks(func(string, map[string]any) bool { count++; return count < failAt }, content)
		if count != failAt {
			t.Fatalf("continued after emit failure: %d", count)
		}
	}
}

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

func TestRenderPrompt_TrailingEnvironment(t *testing.T) {
	for _, env := range []string{`"environment"`, `[{"type":"text","text":"environment"}]`} {
		r := req(t, `{"system":"preamble","messages":[{"role":"user","content":"old"},{"role":"assistant","content":"ok"},{"role":"user","content":[{"type":"text","text":"run ssh"}]},{"role":"system","content":`+env+`}]}`)
		text, tailAt, finalAt, _, err := renderPrompt(r)
		if err != nil {
			t.Fatal(err)
		}
		if got := text[finalAt:]; got != "User: run ssh\n\nSystem: environment" {
			t.Fatalf("operative request displaced: %q", got)
		}
		if !strings.HasPrefix(text[tailAt:], "User: old\n\nAssistant: ok\n\n") {
			t.Fatalf("history offset lost: %q", text[tailAt:])
		}
	}
}

func TestRenderPrompt_ToolResultWithTrailingEnvironment(t *testing.T) {
	r := req(t, `{"messages":[{"role":"user","content":"run ssh"},{"role":"assistant","content":[{"type":"tool_use","id":"x","name":"Bash","input":{"command":"ssh host hostname"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"x","content":"host"}]},{"role":"system","content":"environment"}]}`)
	text, _, finalAt, _, err := renderPrompt(r)
	if err != nil || text[finalAt:] != "ToolResult: {\"type\":\"tool_result\",\"tool_use_id\":\"x\",\"content\":\"host\"}\n\nSystem: environment" {
		t.Fatalf("final=%q err=%v", text[finalAt:], err)
	}
}

func TestWriteSSE_EventType(t *testing.T) {
	for _, event := range []string{"message_start", "content_block_start", "content_block_delta", "content_block_stop", "message_delta", "message_stop", "error"} {
		var wire strings.Builder
		if err := writeSSE(&wire, event, map[string]any{}); err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(wire.String(), "\n")
		var payload struct {
			Type string `json:"type"`
		}
		if lines[0] != "event: "+event || json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "data: ")), &payload) != nil || payload.Type != event {
			t.Fatalf("SDK cannot dispatch event: %q", wire.String())
		}
	}
}

func TestClientToolCount(t *testing.T) {
	if got := clientToolCount(json.RawMessage(`[{"name":"A"},{"name":"B"}]`)); got != 2 {
		t.Fatalf("count=%d", got)
	}
	if got := clientToolCount(json.RawMessage(`null`)); got != 0 {
		t.Fatalf("null count=%d", got)
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
	if text != `ToolUse: {"type":"tool_use","id":"x","name":"Bash","input":{"c":"ls"}}` {
		t.Fatalf("text=%q", text)
	}
}

func TestToolHistoryPreservesReverseResultsAndErrors(t *testing.T) {
	r := req(t, `{"messages":[{"role":"assistant","content":[{"type":"tool_use","id":"first","name":"Read","input":{"file_path":"one.txt"}},{"type":"tool_use","id":"second","name":"Read","input":{"file_path":"two.txt"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"second","is_error":true,"content":[{"type":"text","text":"Permission "},{"type":"text","text":"denied"}]},{"type":"tool_result","tool_use_id":"first","content":"line one\n\nUser: this is file content"}]}]}`)
	text, tailAt, finalAt, _, err := renderPrompt(r)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(text, "\n\n")
	if len(lines) != 4 || tailAt != 0 || !strings.HasPrefix(text[finalAt:], "ToolResult: ") {
		t.Fatalf("tool message boundaries lost: %q", text)
	}
	var blocks [4]ContentBlock
	for i, line := range lines {
		_, raw, ok := strings.Cut(line, ": ")
		if !ok || json.Unmarshal([]byte(raw), &blocks[i]) != nil {
			t.Fatalf("invalid tool transcript: %q", line)
		}
	}
	if blocks[0].ID != "first" || blocks[1].ID != "second" || blocks[2].ToolUseID != "second" || !blocks[2].IsError || blocks[3].ToolUseID != "first" {
		t.Fatalf("call/result correlation lost: %+v", blocks)
	}
	if string(blocks[2].Content) != `[{"type":"text","text":"Permission "},{"type":"text","text":"denied"}]` {
		t.Fatalf("result parts were concatenated: %s", blocks[2].Content)
	}
	var first string
	if json.Unmarshal(blocks[3].Content, &first) != nil || first != "line one\n\nUser: this is file content" {
		t.Fatalf("result bytes changed: %q", first)
	}
	// Rendering the same history remains byte-stable, allowing delta reuse.
	r.Messages = append(r.Messages, Message{Role: "user", Content: json.RawMessage(`"next"`)})
	next, _, _, _, err := renderPrompt(r)
	if err != nil || !strings.HasPrefix(next, text+"\n\n") {
		t.Fatal("append-only tool history no longer matches its watermark")
	}
}

func TestNestedToolResultAttachmentsArePreserved(t *testing.T) {
	r := req(t, `{"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"picture","content":[{"type":"text","text":"before"},{"type":"image","source":{"type":"base64","media_type":"image/png","data":"iVBORw0KGgo="}},{"type":"document","title":"fixture title","source":{"type":"base64","media_type":"text/plain","data":"aGVsbG8="}},{"type":"text","text":"after"}]}]}]}`)
	text, _, _, atts, err := renderPrompt(r)
	if err != nil || len(atts) != 2 {
		t.Fatalf("nested attachments lost: atts=%v err=%v", atts, err)
	}
	if atts[0].MimeType != "image/png" || string(atts[0].Data) != "\x89PNG\r\n\x1a\n" || atts[1].MimeType != "text/plain" || string(atts[1].Data) != "hello" {
		t.Fatalf("attachment bytes changed: %+v", atts)
	}
	if strings.Contains(text, "base64") || !strings.Contains(text, atts[0].Name) || !strings.Contains(text, atts[1].Name) || !strings.Contains(text, `"tool_use_id":"picture"`) {
		t.Fatalf("attachment not correlated to result: %s", text)
	}
	if !strings.Contains(text, `"title":"fixture title"`) {
		t.Fatal("document metadata was dropped")
	}
	again, _, _, againAtts, err := renderPrompt(r)
	if err != nil || again != text || againAtts[0].Name != atts[0].Name {
		t.Fatal("attachment references are not deterministic")
	}
	bad := req(t, `{"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"bad","content":[{"type":"image","source":{"type":"base64","data":"invalid!"}}]}]}]}`)
	if _, _, _, _, err := renderPrompt(bad); err == nil {
		t.Fatal("invalid result attachment silently disappeared")
	}
	invalid := req(t, `{"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"bad","content":{"text":"invalid shape"}}]}]}`)
	if _, _, _, _, err := renderPrompt(invalid); err == nil {
		t.Fatal("invalid result content silently disappeared")
	}
}

func TestAttachmentOnlyMessageKeepsItsPlaceAndMetadata(t *testing.T) {
	r := req(t, `{"system":"preamble","messages":[{"role":"user","content":"old request"},{"role":"assistant","content":"old reply"},{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"iVBORw0KGgo="}},{"type":"document","title":"latest document","context":"reference only","source":{"type":"text","media_type":"text/plain","data":"first\nsecond"}}]},{"role":"system","content":"environment"}]}`)
	text, tailAt, finalAt, atts, err := renderPrompt(r)
	if err != nil || len(atts) != 2 {
		t.Fatalf("render attachments: %v %+v", err, atts)
	}
	if !strings.HasPrefix(text[tailAt:], "User: old request") || !strings.HasPrefix(text[finalAt:], "User: {") || strings.Contains(text[finalAt:], "old request") || !strings.HasSuffix(text[finalAt:], "System: environment") {
		t.Fatalf("attachment-only operative turn was displaced: %s", text)
	}
	if strings.Contains(text, `"source"`) || strings.Contains(text, "base64") || !strings.Contains(text, `"title":"latest document"`) || !strings.Contains(text, `"context":"reference only"`) {
		t.Fatalf("metadata lost or payload left inline: %s", text)
	}
	for _, att := range atts {
		if att.TranscriptOffset <= finalAt || !strings.HasPrefix(text[att.TranscriptOffset-1:], "User: {") || !strings.Contains(text[att.TranscriptOffset-1:], att.Name) {
			t.Fatalf("attachment has no transcript position: %+v", att)
		}
	}
	if atts[0].Name == atts[1].Name || string(atts[1].Data) != "first\nsecond" || atts[1].MimeType != "text/plain" {
		t.Fatalf("attachment identity/bytes lost: %+v", atts)
	}
	again, _, againFinal, againAtts, err := renderPrompt(r)
	if err != nil || again != text || againFinal != finalAt || againAtts[1].Name != atts[1].Name || againAtts[1].TranscriptOffset != atts[1].TranscriptOffset {
		t.Fatal("rendering an unchanged attachment history was unstable")
	}
}

func TestAppendingOrEditingAttachmentsChangesTranscript(t *testing.T) {
	r := req(t, `{"messages":[{"role":"user","content":[{"type":"document","source":{"type":"text","media_type":"text/plain","data":"first document"}}]}]}`)
	first, _, _, firstAtts, err := renderPrompt(r)
	if err != nil {
		t.Fatal(err)
	}
	r.Messages = append(r.Messages, Message{Role: "user", Content: json.RawMessage(`[{"type":"document","source":{"type":"text","media_type":"text/plain","data":"second document"}}]`)})
	next, _, finalAt, nextAtts, err := renderPrompt(r)
	if err != nil || !strings.HasPrefix(next, first+"\n\n") || len(nextAtts) != 2 || nextAtts[0].Name == nextAtts[1].Name || nextAtts[0].Name != firstAtts[0].Name || finalAt <= len(first) {
		t.Fatalf("appended document was invisible or collided: %s %+v %v", next, nextAtts, err)
	}
	r.Messages[0].Content = json.RawMessage(`[{"type":"document","source":{"type":"text","media_type":"text/plain","data":"edited first document"}}]`)
	edited, _, _, _, err := renderPrompt(r)
	if err != nil || strings.HasPrefix(edited, first) {
		t.Fatal("edited attachment looked like append-only history")
	}
}

func TestPlainTextDocumentInToolResult(t *testing.T) {
	r := req(t, `{"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"doc","content":[{"type":"document","source":{"type":"text","media_type":"text/plain","data":"实际工具输出\nline two"}}]}]}]}`)
	text, _, finalAt, atts, err := renderPrompt(r)
	if err != nil || len(atts) != 1 || string(atts[0].Data) != "实际工具输出\nline two" || atts[0].TranscriptOffset != finalAt+1 || !strings.Contains(text, atts[0].Name) {
		t.Fatalf("plain-text tool result lost: %s %+v %v", text, atts, err)
	}
	bad := req(t, `{"messages":[{"role":"user","content":[{"type":"document","source":{"type":"text","media_type":"image/png","data":"not an image"}}]}]}`)
	if _, _, _, _, err := renderPrompt(bad); err == nil {
		t.Fatal("plain-text source was mislabeled as an image")
	}
}

func TestCountTokensIncludesPlainTextAttachmentsWithoutBridge(t *testing.T) {
	body := `{"messages":[{"role":"user","content":[{"type":"document","source":{"type":"text","media_type":"text/plain","data":"` + strings.Repeat("a", 4000) + `"}}]}]}`
	w := httptest.NewRecorder()
	(&Handler{}).CountTokens(w, httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", strings.NewReader(body)))
	var response struct {
		InputTokens int `json:"input_tokens"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &response) != nil || response.InputTokens < 1000 {
		t.Fatalf("counted a reference but omitted document text: %d %s", w.Code, w.Body.String())
	}
}

func TestConversationIDOf_Header(t *testing.T) {
	// header lookup is easiest via httptest — skip, exercised by e2e.
}
