package common

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// ClientTools is a request-scoped catalog. It grants no execution permission:
// accepted calls are returned to the client, which applies its own permissions.
type ClientTools struct {
	schemas          map[string]json.RawMessage
	only             string
	disabled, single bool
}

// NewClientTools accepts Anthropic tool declarations and tool_choice. Invalid or
// duplicate declarations fail closed; schema compilation never loads URLs/files.
func NewClientTools(raw, choice json.RawMessage) *ClientTools {
	c := &ClientTools{schemas: make(map[string]json.RawMessage)}
	var specs []struct {
		Name   string          `json:"name"`
		Schema json.RawMessage `json:"input_schema"`
	}
	if json.Unmarshal(raw, &specs) != nil {
		return c
	}
	for _, spec := range specs {
		if spec.Name == "" || len(spec.Schema) == 0 {
			c.disabled = true
			break
		}
		if _, duplicate := c.schemas[spec.Name]; duplicate {
			c.disabled = true
			break
		}
		c.schemas[spec.Name] = spec.Schema
	}
	var policy struct {
		Type   string `json:"type"`
		Name   string `json:"name"`
		Single bool   `json:"disable_parallel_tool_use"`
	}
	if len(choice) > 0 && string(choice) != "null" {
		if json.Unmarshal(choice, &policy) != nil {
			c.disabled = true
		}
		if policy.Type == "none" {
			c.disabled = true
		}
		if policy.Type == "tool" {
			c.only = policy.Name
			if c.only == "" {
				c.disabled = true
			}
		}
	}
	c.single = policy.Single
	return c
}

// ParseReply accepts a complete call object/array, or the observed Desktop form
// of one short action-intent paragraph, a blank line, and complete terminal JSON.
// It does not search arbitrary prose, Markdown examples, or trailing fragments.
// The narration check is a deliberately narrow compatibility heuristic, not an
// intent classifier. Unsupported/ambiguous formats remain ordinary text.
func (c *ClientTools) ParseReply(text string) (narration string, calls []ToolCallJSON, ok bool) {
	if c == nil || c.disabled || len(c.schemas) == 0 {
		return "", nil, false
	}
	t := strings.TrimSpace(strings.ReplaceAll(text, "\r\n", "\n"))
	calls, ok = strictToolCalls(t)
	if !ok {
		at := strings.Index(t, "\n\n")
		if at < 0 || !actionNarration(t[:at]) {
			return "", nil, false
		}
		narration, t = strings.TrimSpace(t[:at]), strings.TrimSpace(t[at+2:])
		// Mixed prose + a fenced example is not the wire contract.
		if strings.HasPrefix(t, "```") {
			return "", nil, false
		}
		calls, ok = strictToolCalls(t)
	}
	if !ok || c.ValidateCalls(calls) != nil {
		return "", nil, false
	}
	return narration, calls, true
}

// ValidateCalls applies the client catalog and tool choice directly to typed
// calls. Native transports must not serialize a call back into assistant prose
// merely to reuse the text compatibility parser.
func (c *ClientTools) ValidateCalls(calls []ToolCallJSON) error {
	if c == nil || c.disabled || len(c.schemas) == 0 || len(calls) == 0 || (c.single && len(calls) > 1) {
		return errors.New("client tool catalog or tool choice disallows this call")
	}
	ids := make(map[string]bool, len(calls))
	for _, call := range calls {
		if call.Type != "tool_use" {
			return errors.New("client tool call must have type tool_use")
		}
		input, err := uniqueJSON(strings.NewReader(string(call.Input)))
		if _, object := input.(map[string]any); err != nil || !object {
			return errors.New("client tool arguments must be an unambiguous JSON object")
		}
		if c.only != "" && call.Name != c.only {
			return errors.New("client tool choice disallows this tool")
		}
		schema, declared := c.schemas[call.Name]
		if !declared || !validToolInput(schema, call.Input) {
			return errors.New("client tool is undeclared or its arguments do not match its schema")
		}
		if call.ID != "" {
			if ids[call.ID] {
				return errors.New("duplicate client tool call id")
			}
			ids[call.ID] = true
		}
	}
	return nil
}

func actionNarration(prefix string) bool {
	p := strings.TrimSpace(prefix)
	if p == "" || utf8.RuneCountInString(p) > 256 || strings.ContainsAny(p, "\n\r{}[]<>`:\"：") {
		return false
	}
	lower := strings.ToLower(p)
	for _, forbidden := range []string{"example", "sample", "schema", "json", "format", "hypothetical", "would ", "could ", "not ", "cannot", "can't", "don't", "won't", "示例", "例如", "假设", "假如", "演示", "格式", "不要", "不能", "无法", "不会", "不执行"} {
		if strings.Contains(lower, forbidden) {
			return false
		}
	}
	for _, start := range []string{"need to ", "i need to ", "i'll ", "i will ", "let me ", "first, i'll ", "next, i'll ", "我先", "我需要", "我将", "我会", "接下来我", "现在我", "先查询", "先读取"} {
		if strings.HasPrefix(lower, start) {
			return true
		}
	}
	return false
}

func strictToolCalls(text string) ([]ToolCallJSON, bool) {
	t := strings.TrimSpace(text)
	if strings.HasPrefix(t, "```") {
		line := strings.IndexByte(t, '\n')
		if line < 0 || !strings.HasSuffix(t, "```") {
			return nil, false
		}
		header := strings.TrimSpace(t[:line])
		if header != "```" && header != "```json" {
			return nil, false
		}
		t = strings.TrimSpace(t[line+1 : len(t)-3])
	}
	value, err := uniqueJSON(strings.NewReader(t))
	if err != nil {
		return nil, false
	}
	if _, isObject := value.(map[string]any); isObject {
		value = []any{value}
	}
	list, isArray := value.([]any)
	if !isArray || len(list) == 0 {
		return nil, false
	}
	calls := make([]ToolCallJSON, 0, len(list))
	for _, item := range list {
		obj, isObject := item.(map[string]any)
		if !isObject {
			return nil, false
		}
		for key := range obj {
			if key != "type" && key != "id" && key != "name" && key != "input" {
				return nil, false
			}
		}
		if obj["type"] != "tool_use" {
			return nil, false
		}
		name, _ := obj["name"].(string)
		if name == "" {
			return nil, false
		}
		if _, isObject := obj["input"].(map[string]any); !isObject {
			return nil, false
		}
		id := ""
		if raw, exists := obj["id"]; exists {
			var isString bool
			id, isString = raw.(string)
			if !isString {
				return nil, false
			}
		}
		input, err := json.Marshal(obj["input"])
		if err != nil {
			return nil, false
		}
		calls = append(calls, ToolCallJSON{Type: "tool_use", ID: id, Name: name, Input: input})
	}
	return calls, true
}

type noSchemaLoader struct{}

func (noSchemaLoader) Load(string) (any, error) {
	return nil, errors.New("external tool-schema references are disabled")
}

func validToolInput(raw, input json.RawMessage) bool {
	doc, err := jsonschema.UnmarshalJSON(strings.NewReader(string(raw)))
	if err != nil {
		return false
	}
	compiler := jsonschema.NewCompiler()
	compiler.UseLoader(noSchemaLoader{})
	const location = "https://quick2api.invalid/client-tool.json"
	if compiler.AddResource(location, doc) != nil {
		return false
	}
	schema, err := compiler.Compile(location)
	if err != nil {
		return false
	}
	value, err := jsonschema.UnmarshalJSON(strings.NewReader(string(input)))
	return err == nil && schema.Validate(value) == nil
}

// Reject duplicate keys instead of validating one interpretation and forwarding
// an ambiguous JSON document to a different client decoder.
func uniqueJSON(r io.Reader) (any, error) {
	d := json.NewDecoder(r)
	d.UseNumber()
	var read func(int) (any, error)
	read = func(depth int) (any, error) {
		if depth > 128 {
			return nil, errors.New("tool JSON is too deeply nested")
		}
		token, err := d.Token()
		if err != nil {
			return nil, err
		}
		switch token {
		case json.Delim('{'):
			obj := map[string]any{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return nil, err
				}
				name, ok := key.(string)
				if !ok {
					return nil, errors.New("invalid JSON key")
				}
				if _, exists := obj[name]; exists {
					return nil, errors.New("duplicate JSON key")
				}
				value, err := read(depth + 1)
				if err != nil {
					return nil, err
				}
				obj[name] = value
			}
			_, err := d.Token()
			return obj, err
		case json.Delim('['):
			array := []any{}
			for d.More() {
				value, err := read(depth + 1)
				if err != nil {
					return nil, err
				}
				array = append(array, value)
			}
			_, err := d.Token()
			return array, err
		default:
			if _, isDelim := token.(json.Delim); isDelim {
				return nil, errors.New("unexpected JSON delimiter")
			}
			return token, nil
		}
	}
	value, err := read(0)
	if err != nil {
		return nil, err
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, errors.New("trailing data after tool JSON")
	}
	return value, nil
}
