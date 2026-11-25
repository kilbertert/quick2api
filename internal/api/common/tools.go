package common

import (
	"encoding/json"
	"strings"
)

// ToolRuntimeReminder stays close to the operative user message. The full
// schemas live in ToolsInstruction, but oversized requests move that preamble
// into an attachment; this short suffix keeps the client/runtime boundary
// unambiguous at the point where the model decides whether to call a tool.
const ToolRuntimeReminder = `[LOCAL TOOL RUNTIME]
This request comes through quick2api from a local Claude client agent. The client tools in the attached context are separate from Amazon Quick's native tools. quick2api parses a whole-response tool_use JSON object and returns it to the client as an Anthropic tool_use content block with stop_reason=tool_use. The local client runs the requested tool under its own permissions and sends a tool_result on the next request. You do not need direct access to its terminal or SSH connection. Requesting a tool is not claiming that it already ran; wait for its tool_result before reporting any output. When the task requires an available client tool, respond with ONLY one JSON object using its exact declared name and input schema:
{"type":"tool_use","id":"<id>","name":"<exact tool name>","input":{...}}
Do not add explanation or Markdown. When no tool is needed, answer normally.`

// CompactToolsInstruction keeps the callable schemas inline even when the full
// descriptions live in an attachment. Schema annotations are omitted here only;
// the original ToolsInstruction remains the source for tool behavior and limits.
func CompactToolsInstruction(raw json.RawMessage) string {
	var tools []struct {
		Name   string          `json:"name"`
		Schema json.RawMessage `json:"input_schema"`
	}
	if json.Unmarshal(raw, &tools) != nil || len(tools) == 0 {
		return ""
	}
	catalog := make(map[string]json.RawMessage, len(tools))
	for _, tool := range tools {
		if tool.Name != "" && len(tool.Schema) > 0 {
			catalog[tool.Name] = compactSchema(tool.Schema)
		}
	}
	data, err := json.Marshal(catalog)
	if err != nil || len(catalog) == 0 {
		return ToolRuntimeReminder
	}
	return "[CLIENT TOOL SCHEMAS]\nExact tool names mapped to input schemas. Full descriptions, permissions and usage constraints remain in the earlier tools context.\n" + string(data) + "\n\n" + ToolRuntimeReminder
}

func compactSchema(raw json.RawMessage) json.RawMessage {
	var schema map[string]json.RawMessage
	if json.Unmarshal(raw, &schema) != nil || schema == nil {
		return raw // boolean schemas are valid too
	}
	for _, key := range []string{"description", "title", "$schema", "examples"} {
		delete(schema, key)
	}
	// Only recurse through schema-valued keywords. Property names and literal
	// enum/const/default data may themselves be named "description" or "title".
	for _, key := range []string{"properties", "patternProperties", "$defs", "definitions", "dependentSchemas"} {
		var entries map[string]json.RawMessage
		if json.Unmarshal(schema[key], &entries) == nil && entries != nil {
			for name, value := range entries {
				entries[name] = compactSchema(value)
			}
			schema[key], _ = json.Marshal(entries)
		}
	}
	for _, key := range []string{"items", "additionalProperties", "unevaluatedProperties", "propertyNames", "contains", "not", "if", "then", "else"} {
		if value, ok := schema[key]; ok {
			schema[key] = compactSchema(value)
		}
	}
	for _, key := range []string{"allOf", "anyOf", "oneOf", "prefixItems"} {
		var entries []json.RawMessage
		if json.Unmarshal(schema[key], &entries) == nil && entries != nil {
			for i, value := range entries {
				entries[i] = compactSchema(value)
			}
			schema[key], _ = json.Marshal(entries)
		}
	}
	out, _ := json.Marshal(schema)
	return out
}

// ToolsInstruction writes the prompt-side contract that tells the model
// how to request a tool call. Both adapters emit this same text so the
// upstream model sees one protocol regardless of which API surface the
// client speaks.
//
// The contract is deliberately tool-agnostic: the proxy parses the
// reply for {type:"tool_use",id,name,input} — the same shape Anthropic
// clients already use — so the executor code is shared.
func ToolsInstruction(toolsRaw []byte) string {
	if len(toolsRaw) == 0 || string(toolsRaw) == "null" || string(toolsRaw) == "[]" {
		return ""
	}
	var b strings.Builder
	b.WriteString("The user has declared the following tools available for use. " +
		"When you want to use one, respond with ONLY the JSON object " +
		"{\"type\":\"tool_use\",\"id\":\"<id>\",\"name\":\"<tool name>\"," +
		"\"input\":{...}}. Do not wrap it in markdown.\nTools:\n")
	b.Write(toolsRaw)
	b.WriteString("\n\n")
	return b.String()
}

// ToolCallJSON is the single tool-use shape the prompt contract asks
// for: {"type":"tool_use","id":"…","name":"…","input":{…}}.
type ToolCallJSON struct {
	Type  string          `json:"type"`
	ID    string          `json:"id"`
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input"`
}

// ParseToolCallJSON detects a whole-response tool_use call. Accepts the
// bare JSON object, a one-element array containing it, or a markdown
// fenced block containing it. Returns ok=false when the text is a
// normal reply (the caller then returns it verbatim).
func ParseToolCallJSON(text string) (ToolCallJSON, bool) {
	t := strings.TrimSpace(text)
	// Unwrap a single markdown fence (```json … ``` or ``` … ```).
	if strings.HasPrefix(t, "```") {
		t = strings.TrimPrefix(t, "```")
		if i := strings.IndexByte(t, '\n'); i >= 0 {
			t = t[i+1:]
		}
		t = strings.TrimSuffix(strings.TrimSpace(t), "```")
		t = strings.TrimSpace(t)
	}
	var one ToolCallJSON
	if err := json.Unmarshal([]byte(t), &one); err == nil && one.Type == "tool_use" && one.Name != "" {
		return one, true
	}
	var arr []json.RawMessage
	if err := json.Unmarshal([]byte(t), &arr); err == nil && len(arr) == 1 {
		if err := json.Unmarshal(arr[0], &one); err == nil && one.Type == "tool_use" && one.Name != "" {
			return one, true
		}
	}
	return ToolCallJSON{}, false
}

// ParseToolCallsJSON detects zero-or-more tool_use calls packed in one
// reply: either a single object or an array of objects. Used by the
// multi-call variant of the tool loop.
func ParseToolCallsJSON(text string) ([]ToolCallJSON, bool) {
	if one, ok := ParseToolCallJSON(text); ok {
		return []ToolCallJSON{one}, true
	}
	t := strings.TrimSpace(text)
	if strings.HasPrefix(t, "```") {
		t = strings.TrimPrefix(t, "```")
		if i := strings.IndexByte(t, '\n'); i >= 0 {
			t = t[i+1:]
		}
		t = strings.TrimSuffix(strings.TrimSpace(t), "```")
		t = strings.TrimSpace(t)
	}
	var arr []json.RawMessage
	if err := json.Unmarshal([]byte(t), &arr); err != nil || len(arr) == 0 {
		return nil, false
	}
	var out []ToolCallJSON
	for _, raw := range arr {
		var one ToolCallJSON
		if err := json.Unmarshal(raw, &one); err != nil || one.Type != "tool_use" || one.Name == "" {
			return nil, false
		}
		out = append(out, one)
	}
	return out, true
}
