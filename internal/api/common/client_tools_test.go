package common

import (
	"encoding/json"
	"strings"
	"testing"
)

const sessionTool = `[{"name":"mcp__ccd_session_mgmt__list_sessions","input_schema":{"type":"object","properties":{"limit":{"type":"number","minimum":1}},"additionalProperties":false}}]`
const sessionCall = `{"type":"tool_use","id":"toolu_ls01","name":"mcp__ccd_session_mgmt__list_sessions","input":{"limit":10}}`
const sessionNarration = "Need to find the shell session's local id."

func TestClientToolTypedCallsEnforceWireShape(t *testing.T) {
	// A permissive schema must not loosen the transport's object-only contract.
	catalog := NewClientTools(json.RawMessage(`[{"name":"Read","input_schema":{}}]`), nil)
	for _, test := range []struct{ name, kind, input string }{
		{"missing type", "", `{}`},
		{"text block", "text", `{}`},
		{"null", "tool_use", `null`},
		{"array", "tool_use", `[]`},
		{"scalar", "tool_use", `"path"`},
		{"duplicate key", "tool_use", `{"path":"a","path":"b"}`},
		{"nested duplicate", "tool_use", `{"options":{"path":"a","path":"b"}}`},
		{"trailing data", "tool_use", `{} {}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := catalog.ValidateCalls([]ToolCallJSON{{Type: test.kind, Name: "Read", Input: json.RawMessage(test.input)}}); err == nil {
				t.Fatal("invalid typed tool call accepted")
			}
		})
	}
	if err := catalog.ValidateCalls([]ToolCallJSON{{Type: "tool_use", Name: "Read", Input: json.RawMessage(`{"offset":9007199254740993}`)}}); err != nil {
		t.Fatalf("valid typed object rejected: %v", err)
	}
}

func TestClientToolReplyObservedDesktopFormat(t *testing.T) {
	catalog := NewClientTools(json.RawMessage(sessionTool), nil)
	for _, text := range []string{sessionCall, "```json\n" + sessionCall + "\n```", sessionNarration + "\n\n" + sessionCall, sessionNarration + "\r\n\r\n" + sessionCall} {
		prefix, calls, ok := catalog.ParseReply(text)
		if !ok || len(calls) != 1 || calls[0].Name != "mcp__ccd_session_mgmt__list_sessions" || string(calls[0].Input) != `{"limit":10}` {
			t.Fatalf("observed reply lost: %q %+v %v", text, calls, ok)
		}
		if strings.HasPrefix(text, sessionNarration) && prefix != sessionNarration {
			t.Fatalf("narration lost: %q", prefix)
		}
	}
}

func TestClientToolReplyRejectsAmbiguousProse(t *testing.T) {
	catalog := NewClientTools(json.RawMessage(sessionTool), nil)
	for _, text := range []string{
		"Here is an example:\n\n" + sessionCall,
		"I will show an example.\n\n" + sessionCall,
		"I will not execute anything.\n\n" + sessionCall,
		"我不会执行这个操作。\n\n" + sessionCall,
		"我先说明示例。\n\n" + sessionCall,
		"Need to find it:\n\n" + sessionCall,
		sessionNarration + " " + sessionCall,
		sessionNarration + "\n\n```json\n" + sessionCall + "\n```",
		sessionNarration + "\n\n" + sessionCall + "\nThis is just text.",
		"> " + sessionNarration + "\n\n" + sessionCall,
		sessionNarration + "\nAn additional paragraph.\n\n" + sessionCall,
		sessionNarration + "\n\n" + sessionCall + "\n\n" + sessionCall,
		sessionNarration + "\n\n" + strings.TrimSuffix(sessionCall, "}"),
	} {
		if _, _, ok := catalog.ParseReply(text); ok {
			t.Fatalf("ambiguous prose became executable: %q", text)
		}
	}
}

func TestClientToolReplyValidatesCatalogAndSchema(t *testing.T) {
	for _, test := range []struct{ name, tools, call, choice string }{
		{"unknown tool", `[]`, sessionCall, ""},
		{"different tool", sessionTool, strings.Replace(sessionCall, "list_sessions", "delete_session", 1), ""},
		{"wrong type", sessionTool, strings.Replace(sessionCall, `10`, `"10"`, 1), ""},
		{"out of range", sessionTool, strings.Replace(sessionCall, `10`, `0`, 1), ""},
		{"extra argument", sessionTool, strings.Replace(sessionCall, `"limit":10`, `"limit":10,"extra":true`, 1), ""},
		{"missing input", sessionTool, strings.Replace(sessionCall, `,"input":{"limit":10}`, "", 1), ""},
		{"input null", sessionTool, strings.Replace(sessionCall, `{"limit":10}`, `null`, 1), ""},
		{"unexpected wrapper field", sessionTool, strings.TrimSuffix(sessionCall, "}") + `,"example":true}`, ""},
		{"duplicate wrapper key", sessionTool, strings.Replace(sessionCall, `"type":"tool_use"`, `"type":"text","type":"tool_use"`, 1), ""},
		{"duplicate input key", sessionTool, strings.Replace(sessionCall, `"limit":10`, `"limit":5,"limit":10`, 1), ""},
		{"none choice", sessionTool, sessionCall, `{"type":"none"}`},
		{"named choice", sessionTool, sessionCall, `{"type":"tool","name":"Read"}`},
		{"remote schema", `[{"name":"mcp__ccd_session_mgmt__list_sessions","input_schema":{"$ref":"http://127.0.0.1:1/schema"}}]`, sessionCall, ""},
		{"invalid schema", `[{"name":"mcp__ccd_session_mgmt__list_sessions","input_schema":{"type":123}}]`, sessionCall, ""},
		{"duplicate declaration", strings.TrimSuffix(sessionTool, "]") + "," + strings.TrimPrefix(sessionTool, "["), sessionCall, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, _, ok := NewClientTools(json.RawMessage(test.tools), json.RawMessage(test.choice)).ParseReply(sessionNarration + "\n\n" + test.call); ok {
				t.Fatal("invalid call accepted")
			}
		})
	}
}

func TestClientToolReplyNestedLocalSchemaAndParallelCalls(t *testing.T) {
	tools := json.RawMessage(`[{"name":"Read","input_schema":{"type":"object","$defs":{"path":{"type":"string","minLength":1}},"properties":{"file_path":{"$ref":"#/$defs/path"}},"required":["file_path"]}}]`)
	call := `{"type":"tool_use","id":"a","name":"Read","input":{"file_path":"fixture.txt"}}`
	for _, text := range []string{call, "I will read the files.\n\n[" + call + "," + strings.Replace(call, `"a"`, `"b"`, 1) + "]"} {
		if _, _, ok := NewClientTools(tools, nil).ParseReply(text); !ok {
			t.Fatalf("valid nested/parallel call rejected: %s", text)
		}
	}
	for _, choice := range []json.RawMessage{nil, json.RawMessage(`{"disable_parallel_tool_use":true}`)} {
		if _, _, ok := NewClientTools(tools, choice).ParseReply("[" + call + "," + call + "]"); ok {
			t.Fatal("duplicate call ids accepted")
		}
	}
	if _, _, ok := NewClientTools(tools, json.RawMessage(`{"disable_parallel_tool_use":true}`)).ParseReply("[" + call + "," + strings.Replace(call, `"a"`, `"b"`, 1) + "]"); ok {
		t.Fatal("parallel restriction ignored")
	}
	invalid := strings.Replace(call, `{"file_path":"fixture.txt"}`, `{}`, 1)
	if _, _, ok := NewClientTools(tools, nil).ParseReply(invalid); ok {
		t.Fatal("missing required field accepted")
	}
}
