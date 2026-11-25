// prompt.go decides how the prompt text reaches the upstream: a single
// user_message when it fits, an out-of-band attachment reference when it
// doesn't, or sequential primed chunks as the last-resort fallback.
package chat

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/kilbertert/quick2api/internal/session"
)

// attachmentKind picks the wire `type` the renderer puts on an attachment.
func attachmentKind(mime string) string {
	if strings.HasPrefix(mime, "image/") {
		return "image"
	}
	return "file"
}

// maxSingleText is the largest user_message text the AIDP upstream accepts
// before it closes the turn with an empty {"type":"error"} frame. Verified
// 2026-11-22: 64_000 bytes OK, 80_000 rejected. Keep margin for framing text.
const maxSingleText = 55_000

// sendPrompt sends the rendered prompt. For oversized text it first
// tries the out-of-band attachment path: upload the prompt as a file and
// reference it from a short user_message — one model turn, no per-chunk
// re-inference. If the attachment path fails it falls back to sequential
// user_message turns with context-priming prefixes.
func (c *Client) sendPrompt(ctx context.Context, t *turn, conversationID string, opts TurnOpts, sess *session.Session) (*TurnResult, error) {
	if opts.NativeRelay != "" {
		runtime, err := prepareNativeRuntime(ctx, sess, opts.NativeRelay)
		if err != nil {
			return nil, err
		}
		t.native = runtime
		t.emit(TurnEvent{Kind: "stage", Text: "relay_ready"})
		defer runtime.client.CloseIdleConnections()
		runtime.deviceFields["surfaceType"] = c.SurfaceType
		runtime.deviceFields["appVersion"] = orElse(c.AppVersion, "1.2.4660")
		defer func() { t.native = nil }()
	}
	opts.Text = appendPromptSuffix(opts.Text, opts.Suffix)
	if opts.Final != "" {
		opts.Final = appendPromptSuffix(opts.Final, opts.Suffix)
	}

	// 1. Upload any caller-provided attachments (multimodal input). Image
	// and file parts ride the out-of-band upload-attachment channel — the
	// wire `type` is "image" or "file" and the referenced `path` is the
	// server path returned by the deliver call.
	var wireAtts []map[string]any
	canUpload := sess != nil && sess.TenantURL != "" && sess.IDToken != ""
	if len(opts.Attachments) > 0 && !canUpload {
		return nil, fmt.Errorf("input attachments cannot be uploaded: no bound upload session")
	}
	if canUpload {
		if len(opts.Attachments) > 0 {
			t.emit(TurnEvent{Kind: "stage", Text: "input_attachments_started"})
		}
		for _, att := range opts.Attachments {
			mime := att.MimeType
			if mime == "" {
				mime = "application/octet-stream"
			}
			path, attID, err := uploadAttachment(ctx, sess, conversationID, att.Name, mime, att.Data)
			slog.Debug("attachment upload", "name", att.Name, "path", path, "attID", attID, "err", err)
			if err != nil || path == "" {
				if err == nil {
					err = fmt.Errorf("upload returned an empty attachment path")
				}
				return nil, fmt.Errorf("input attachment %q was not delivered: %w", att.Name, err)
			}
			wireAtts = append(wireAtts, map[string]any{
				"type":         attachmentKind(mime),
				"name":         att.Name,
				"size":         len(att.Data),
				"mimeType":     mime,
				"path":         path,
				"attachmentId": attID,
			})
		}
		if len(opts.Attachments) > 0 {
			t.emit(TurnEvent{Kind: "stage", Text: "input_attachments_finished"})
		}
	}

	// 2. Small text (+ any uploaded attachments) → single user_message.
	if len(opts.Text) <= maxSingleText {
		if err := sendUserMessage(t, conversationID, opts.Text, opts, wireAtts); err != nil {
			return nil, err
		}
		return t.collect(ctx, &TurnResult{FinishReason: "stop"})
	}

	// 3. Oversized text — try the out-of-band path first (one model turn
	// instead of N); fall back to sequential primed chunks when the upload
	// channel can't deliver.
	//
	// The attachment must carry CONTEXT ONLY: when the whole transcript —
	// including the operative last message — goes into the file, the model
	// answers as a document analyst ("this file contains a Claude Code
	// session…") instead of executing the request. Keep the final message
	// inline so the model sees a real user turn; earlier context rides the
	// file. Final=="" (or Final alone exceeding the inline limit) degrades
	// to the legacy everything-in-one-file shape.
	if sess != nil && sess.TenantURL != "" && sess.IDToken != "" {
		ctxData, ctxName, inline := oversizedPrompt(opts.Text, opts.Final)
		t.emit(TurnEvent{Kind: "stage", Text: "context_upload_started"})
		var path, attID string
		var err error
		if prepared := opts.preparedContext; prepared != nil {
			if prepared.text != opts.Text || prepared.final != opts.Final {
				return nil, fmt.Errorf("prepared native context does not match the outgoing prompt")
			}
			path, attID, err = deliverAttachment(ctx, sess, conversationID, prepared.attachment)
		} else {
			path, attID, err = uploadAttachment(ctx, sess, conversationID, ctxName, "text/plain", ctxData)
		}
		t.emit(TurnEvent{Kind: "stage", Text: "context_upload_finished"})
		slog.Debug("sendPrompt upload attempt", "path", path, "attID", attID, "err", err)
		if err == nil && path != "" {
			wireAtts = append(wireAtts, map[string]any{
				"type":         "file",
				"name":         ctxName,
				"size":         len(ctxData),
				"mimeType":     "text/plain",
				"path":         path,
				"attachmentId": attID,
			})
			if err := sendUserMessage(t, conversationID, inline, opts, wireAtts); err != nil {
				return nil, err
			}
			res, err := t.collect(ctx, &TurnResult{FinishReason: "stop"})
			if err != nil || res == nil || res.FinishReason == "content_filter" || res.PartialOnClose {
				return res, err
			}
			if err == nil && res != nil && res.Text != "" {
				slog.Debug("attachment path succeeded", "path", path)
				return res, nil
			}
			if opts.NativeRelay != "" {
				return nil, &UpstreamError{Code: "native_context_delivery_failed", Message: "native turn returned an empty answer; it will not be replayed through chunk fallback"}
			}
			slog.Warn("attachment path returned empty/failed; falling back to chunks", "err", err)
		} else if err != nil {
			if opts.NativeRelay != "" {
				return nil, err
			}
			if !uploadFallbackAllowed(err) {
				return nil, err
			}
			slog.Warn("attachment upload failed; falling back to chunks", "error", err)
		}
	}
	if opts.NativeRelay != "" {
		return nil, &UpstreamError{Code: "native_context_delivery_failed", Message: "native transport requires intact context delivery; chunk fallback is disabled"}
	}

	// Fallback: sequential user_message turns with context-priming prefixes.
	chunks := splitText(opts.Text, maxSingleText)
	var last *TurnResult
	for i, ch := range chunks {
		text := ch
		var chunkAtts []map[string]any
		if i < len(chunks)-1 {
			text = fmt.Sprintf("[CONTEXT PART %d/%d — do not answer yet, just store this context]\n%s", i+1, len(chunks), ch)
		} else {
			text = fmt.Sprintf("[CONTEXT PART %d/%d — final part. Now answer based on ALL parts above.]\n%s", i+1, len(chunks), ch)
			// Caller attachments were already delivered above. Keep them on
			// the answering turn if only the context upload required fallback.
			chunkAtts = wireAtts
		}
		if err := sendUserMessage(t, conversationID, text, opts, chunkAtts); err != nil {
			return nil, err
		}
		got, err := t.collect(ctx, &TurnResult{FinishReason: "stop"})
		if err != nil {
			return nil, err
		}
		if got.FinishReason == "content_filter" || got.PartialOnClose {
			return got, nil
		}
		if i == len(chunks)-1 {
			last = got
		}
	}
	return last, nil
}

func oversizedPrompt(text, final string) (data []byte, name, inline string) {
	data, name, inline = []byte(text), "prompt.txt",
		"The full prompt is in the attached file prompt.txt. Respond to the last user message in it — earlier content is context, not a file to summarize."
	if final != "" && strings.HasSuffix(text, final) {
		if earlier := strings.TrimSpace(text[:len(text)-len(final)]); earlier != "" {
			candidate := "Earlier conversation context is in the attached file context.txt — treat it as history you already read. Respond directly to this new message:\n\n" + final
			if len(candidate) <= maxSingleText {
				return []byte(earlier), "context.txt", candidate
			}
		}
	}
	return
}

func appendPromptSuffix(text, suffix string) string {
	suffix = strings.TrimSpace(suffix)
	if suffix == "" {
		return text
	}
	text = strings.TrimRight(text, "\r\n")
	if text == "" {
		return suffix
	}
	return text + "\n\n" + suffix
}

// sendUserMessage emits one user_message frame; attachments, when given,
// go on payload.attachments as [{type,name,size,mimeType,path,attachmentId}].
func sendUserMessage(t *turn, conversationID, text string, opts TurnOpts, attachments []map[string]any) error {
	payload := map[string]any{"thread_id": conversationID, "text": text, "message_id": newUUID()}
	fillMsgOpts(payload, opts)
	if t.native != nil {
		for key, value := range t.native.deviceFields {
			payload[key] = value
		}
	}
	if len(attachments) > 0 {
		payload["attachments"] = attachments
	}
	if err := t.send(map[string]any{"type": "user_message", "payload": payload}); err != nil {
		return fmt.Errorf("send user_message: %w", err)
	}
	t.emit(TurnEvent{Kind: "stage", Text: "user_message_sent"})
	return nil
}

// fillMsgOpts copies the optional per-message knobs onto a payload map.
func fillMsgOpts(p map[string]any, opts TurnOpts) {
	if opts.AgentMode != "" {
		p["agent_mode"] = opts.AgentMode
	}
	if opts.ThinkingEffort != "" {
		p["thinking_effort"] = opts.ThinkingEffort
	}
	if opts.UserTimezone != "" {
		p["user_timezone"] = opts.UserTimezone
	}
	if opts.UserLocale != "" {
		p["user_locale"] = opts.UserLocale
	}
}
