// upload.go implements the out-of-band attachment path: oversized prompts
// are uploaded to the tenant's attachment APIs and the user_message only
// carries the returned server path — one model turn instead of N chunks.
package chat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/kilbertert/quick2api/internal/session"
)

// uploadAttachment delivers data as a Quick cloud attachment and
// returns (serverPath, attachmentId). Three steps, all on the tenant_url the
// outpost's sncs-session hands out — same IPCs the renderer's nZ()
// uses (nook-upload-attachment / nook-describe-attachment-dlp / deliver via
// invokeAgent):
//
//	POST {tenant}/integration/quick-work/upload-attachment?name=...
//	      &contentType=...&contentLength=...  → {attachmentId}
//	GET  {tenant}/integration/quick-work/attachment-dlp-status
//	      ?attachmentId=...                     → {dlpJobStatus,policyAction}
//	POST {tenant}/integration/quick-work/qbs-event
//	      invokeAgent{path:"files/upload/<sid>",_method:POST,
//	                  body:{attachmentId}}     → {path|files[0].path}
//
// The returned serverPath goes onto user_message payload.attachments[].path;
// the model reads the file server-side in one turn.
func uploadAttachment(ctx context.Context, sess *session.Session, convID, name string, data []byte) (string, string, error) {
	if sess == nil || sess.IDToken == "" || sess.TenantURL == "" {
		return "", "", fmt.Errorf("upload: no session bound")
	}
	auth := "Bearer " + sess.IDToken
	httpc := &http.Client{Timeout: 30 * time.Second}

	// 1. upload-attachment: raw bytes, octet-stream, id_token bearer.
	up, err := http.NewRequestWithContext(ctx, "POST",
		sess.TenantURL+"/integration/quick-work/upload-attachment"+
			"?name="+url.QueryEscape(name)+
			"&contentType="+url.QueryEscape("text/plain")+
			"&contentLength="+fmt.Sprint(len(data)),
		bytes.NewReader(data))
	if err != nil {
		return "", "", err
	}
	up.Header.Set("Authorization", auth)
	up.Header.Set("Content-Type", "application/octet-stream")
	up.Header.Set("Origin", "https://quick.aws.com")
	upResp, err := httpc.Do(up)
	if err != nil {
		return "", "", fmt.Errorf("upload: %w", err)
	}
	upBody, _ := io.ReadAll(io.LimitReader(upResp.Body, 1<<20))
	upResp.Body.Close()
	slog.Debug("upload-attachment", "status", upResp.StatusCode, "body", trunc(string(upBody), 400))
	if upResp.StatusCode >= 400 {
		return "", "", fmt.Errorf("upload-attachment %d: %s", upResp.StatusCode, trunc(string(upBody), 200))
	}
	var upOut struct {
		AttachmentID string `json:"attachmentId"`
		Path         string `json:"path"`
	}
	if err := json.Unmarshal(upBody, &upOut); err != nil {
		return "", "", fmt.Errorf("upload-attachment decode: %w", err)
	}
	if upOut.AttachmentID == "" {
		if upOut.Path != "" {
			return upOut.Path, "", nil
		}
		return "", "", fmt.Errorf("upload-attachment returned no attachmentId: %s", trunc(string(upBody), 200))
	}
	attID := upOut.AttachmentID

	// 2. DLP poll — wait for policyAction=ALLOW (or a skip status).
	for i := 0; i < 20; i++ {
		select {
		case <-ctx.Done():
			return "", "", ctx.Err()
		case <-time.After(400 * time.Millisecond):
		}
		d, err := http.NewRequestWithContext(ctx, "GET",
			sess.TenantURL+"/integration/quick-work/attachment-dlp-status?attachmentId="+url.QueryEscape(attID), nil)
		if err != nil {
			return "", "", err
		}
		d.Header.Set("Authorization", auth)
		d.Header.Set("Origin", "https://quick.aws.com")
		dResp, err := httpc.Do(d)
		if err != nil {
			slog.Warn("dlp-status poll failed", "error", err)
			continue
		}
		dBody, _ := io.ReadAll(io.LimitReader(dResp.Body, 1<<20))
		dResp.Body.Close()
		slog.Debug("dlp-status", "attempt", i+1, "status", dResp.StatusCode, "body", trunc(string(dBody), 400))
		var dOut struct {
			DLPJobStatus string `json:"dlpJobStatus"`
			PolicyAction string `json:"policyAction"`
		}
		if err := json.Unmarshal(dBody, &dOut); err != nil {
			return "", "", fmt.Errorf("dlp-status decode: %w", err)
		}
		switch strings.ToUpper(dOut.PolicyAction) {
		case "ALLOW":
			goto deliver
		case "BLOCK", "DENY", "REJECT":
			return "", "", fmt.Errorf("dlp blocked: %s", trunc(string(dBody), 200))
		}
		switch strings.ToUpper(dOut.DLPJobStatus) {
		case "SKIPPED", "NOT_APPLICABLE", "DISABLED":
			goto deliver
		}
	}
	return "", "", fmt.Errorf("dlp-status did not settle in time")

deliver:
	// 3. Deliver: bind attachmentId to the conversation, returns serverPath.
	// Renderer lZn() → ll("/files/upload/<sid>", {attachmentId}) → flat
	// qbs-event invokeAgent.
	delBody := map[string]any{
		"eventType": "invokeAgent",
		"eventBody": map[string]any{"invokeAgent": map[string]any{
			"path":        "files/upload/" + convID,
			"queryParams": map[string]any{"_method": "POST"},
			"body":        `{"attachmentId":"` + attID + `"}`,
		}},
	}
	raw, _ := json.Marshal(delBody)
	delReq, err := http.NewRequestWithContext(ctx, "POST",
		sess.TenantURL+"/integration/quick-work/qbs-event", bytes.NewReader(raw))
	if err != nil {
		return "", "", err
	}
	delReq.Header.Set("Authorization", auth)
	delReq.Header.Set("Content-Type", "application/json")
	delReq.Header.Set("Origin", "https://quick.aws.com")
	delResp, err := httpc.Do(delReq)
	if err != nil {
		return "", "", fmt.Errorf("deliver: %w", err)
	}
	delBodyRaw, _ := io.ReadAll(io.LimitReader(delResp.Body, 2<<20))
	delResp.Body.Close()
	slog.Debug("deliver", "status", delResp.StatusCode, "body", trunc(string(delBodyRaw), 500))
	if delResp.StatusCode >= 400 {
		return "", "", fmt.Errorf("deliver %d: %s", delResp.StatusCode, trunc(string(delBodyRaw), 200))
	}
	// lZn response: sZn(o)=o.path||o.files[0].path, wrapped inside
	// eventResponse.invokeAgentResponse → {type:api_response, body:"<json>"}.
	var ev struct {
		EventResponse json.RawMessage `json:"eventResponse"`
	}
	if err := json.Unmarshal(delBodyRaw, &ev); err != nil || len(ev.EventResponse) == 0 {
		return "", "", fmt.Errorf("deliver decode: %w body=%s", err, trunc(string(delBodyRaw), 300))
	}
	var inner struct {
		InvokeAgentResponse string `json:"invokeAgentResponse"`
	}
	if err := json.Unmarshal(ev.EventResponse, &inner); err != nil || inner.InvokeAgentResponse == "" {
		var direct struct {
			Path  string `json:"path"`
			Files []struct {
				Path string `json:"path"`
			} `json:"files"`
		}
		if err2 := json.Unmarshal(ev.EventResponse, &direct); err2 == nil {
			if direct.Path != "" {
				return direct.Path, attID, nil
			}
			if len(direct.Files) > 0 && direct.Files[0].Path != "" {
				return direct.Files[0].Path, attID, nil
			}
		}
		return "", "", fmt.Errorf("deliver inner decode: %s", trunc(string(ev.EventResponse), 300))
	}
	var resp struct {
		Type string `json:"type"`
		Body string `json:"body"`
	}
	if err := json.Unmarshal([]byte(inner.InvokeAgentResponse), &resp); err != nil {
		return "", "", fmt.Errorf("invokeAgentResponse decode: %w", err)
	}
	var fin struct {
		Path  string `json:"path"`
		Files []struct {
			Path string `json:"path"`
		} `json:"files"`
	}
	if err := json.Unmarshal([]byte(resp.Body), &fin); err != nil {
		return "", "", fmt.Errorf("deliver body decode: %w body=%s", err, trunc(resp.Body, 300))
	}
	if fin.Path != "" {
		return fin.Path, attID, nil
	}
	if len(fin.Files) > 0 && fin.Files[0].Path != "" {
		return fin.Files[0].Path, attID, nil
	}
	return "", "", fmt.Errorf("deliver returned no path: %s", trunc(resp.Body, 300))
}
