package anthropic

import (
	"bytes"
	"encoding/json"
	"strings"
)

// Classify tool events without retaining/logging their arguments, output,
// arbitrary error text, invocation IDs or private route tokens.
type nativeToolDiagnostic struct {
	Name             string
	UnknownToolText  bool
	UnknownToolError bool
	IsError          bool
	HasName          bool
}

func nativeToolMetadata(raw json.RawMessage) nativeToolDiagnostic {
	var event map[string]json.RawMessage
	if json.Unmarshal(raw, &event) != nil {
		return nativeToolDiagnostic{}
	}
	var d nativeToolDiagnostic
	for _, key := range []string{"tool_name", "name", "tool"} {
		if json.Unmarshal(event[key], &d.Name) == nil && d.Name != "" {
			d.HasName = true
			break
		}
	}
	// Tool names are protocol identifiers, not free-form model output. Reject
	// malformed names rather than admitting their content into diagnostic logs.
	if len(d.Name) > 200 || strings.IndexFunc(d.Name, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("_.:/-", r))
	}) >= 0 {
		d.Name = ""
	}
	d.UnknownToolText = nativeUnknownToolText(event)
	d.IsError, d.UnknownToolError = nativeToolErrorMetadata(event, 0, false)
	return d
}

func nativeUnknownToolText(event map[string]json.RawMessage) bool {
	for _, key := range []string{"result", "output", "error", "message", "content", "code", "error_code"} {
		value := bytes.ToLower(event[key])
		if bytes.Contains(value, []byte("unknown tool")) || bytes.Contains(value, []byte("unknown_tool")) {
			return true
		}
	}
	return false
}

// Only follow structured result envelopes. Text returned by a successful file
// read can contain old errors (including JSON); it is not a protocol failure.
func nativeToolErrorMetadata(event map[string]json.RawMessage, depth int, inheritedError bool) (isError, unknownToolError bool) {
	hasMarker := false
	for _, key := range []string{"isError", "is_error"} {
		var flag bool
		if value := bytes.TrimSpace(event[key]); bytes.Equal(value, []byte("true")) || bytes.Equal(value, []byte("false")) {
			_ = json.Unmarshal(value, &flag)
			hasMarker = true
			isError = isError || flag
		}
	}
	if !hasMarker {
		isError = inheritedError
	}
	if value := bytes.TrimSpace(event["error"]); len(value) > 0 && !bytes.Equal(value, []byte("null")) && !bytes.Equal(value, []byte(`""`)) && !bytes.Equal(value, []byte("false")) {
		isError = true
	}
	// A detailed error at this level owns its text. Do not associate it with
	// unrelated successful result objects that happen to quote old failures.
	hasDetail := false
	for _, key := range []string{"error", "message", "code", "error_code", "content"} {
		value := bytes.TrimSpace(event[key])
		if len(value) > 0 && !bytes.Equal(value, []byte("null")) && !bytes.Equal(value, []byte(`""`)) && !bytes.Equal(value, []byte("false")) {
			hasDetail = true
			unknownToolError = unknownToolError || isError && nativeUnknownToolText(map[string]json.RawMessage{key: value})
		}
	}
	if depth < 4 {
		inheritResultError := isError && !hasDetail
		for _, key := range []string{"result", "output"} {
			var child map[string]json.RawMessage
			if json.Unmarshal(event[key], &child) != nil || child == nil {
				if inheritResultError && nativeUnknownToolText(map[string]json.RawMessage{key: event[key]}) {
					unknownToolError = true
				}
				continue
			}
			childError, childUnknown := nativeToolErrorMetadata(child, depth+1, inheritResultError)
			isError = isError || childError
			unknownToolError = unknownToolError || childUnknown
		}
	}
	return isError, unknownToolError
}
