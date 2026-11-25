// conn.go owns the wire: the dial URL, the rawInputEvent envelope, and the
// inbound read pump that turns binary event-stream WS messages into inner
// frames on the turn's channel.
package chat

import (
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/url"
	"time"

	"github.com/gorilla/websocket"

	"github.com/kilbertert/quick2api/internal/eventstream"
	"github.com/kilbertert/quick2api/internal/session"
)

// wsURL builds the dial URL. token is query-escaped (its `:` and `:`-joined
// segments must survive).
func (c *Client) wsURL(sess *session.Session, outpostID, workingSessionID string) string {
	q := url.Values{}
	q.Set("surfaceType", c.SurfaceType)
	q.Set("token", sess.IDToken)
	q.Set("outpostId", outpostID)
	q.Set("workingSessionId", workingSessionID)
	q.Set("channel", c.Channel)
	return "wss://" + sess.WSHost + "/streaming/quickwork-chat-bidirectional-stream?" + q.Encode()
}

// wrap encodes an inner frame into the rawInputEvent envelope the renderer
// sends.
func wrap(inner map[string]any) ([]byte, error) {
	innerJSON, err := json.Marshal(inner)
	if err != nil {
		return nil, err
	}
	env := map[string]any{
		"eventType": "rawInputEvent",
		"payload":   map[string]string{"payload": base64.StdEncoding.EncodeToString(innerJSON)},
	}
	return json.Marshal(env)
}

// send wraps and writes one inner frame as a binary WS message — CUe() in
// the renderer produces a Uint8Array and the server answers TEXT frames
// with "Unexpected WebSocket frame received".
func (t *turn) send(inner map[string]any) error {
	b, err := wrap(inner)
	if err != nil {
		return err
	}
	slog.Debug("ws send", "type", inner["type"], "len", len(b))
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	if err := t.ws.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	return t.ws.WriteMessage(websocket.BinaryMessage, b)
}

// pump frames from the socket into t.in until the read fails, then
// delivers the error as the last inbound item before closing the channel —
// one channel for consumers, never two.
func (t *turn) pump() {
	defer close(t.in)
	var rawbuf []byte
	for {
		mt, data, err := t.ws.ReadMessage()
		if err != nil {
			t.deliver(inbound{err: err})
			return
		}
		slog.Debug("ws inbound", "mt", mt, "len", len(data))
		if mt == websocket.TextMessage {
			slog.Debug("ws text payload", "data", trunc(string(data), 400))
			if fr := parseEnvelopePayload(data); fr != nil {
				if !t.deliver(inbound{f: fr}) {
					return
				}
			}
			continue
		}
		// eventstream messages can straddle WS frames — keep a byte buffer
		// and drain complete messages.
		rawbuf = append(rawbuf, data...)
		msgs, used, err := eventstream.Drain(rawbuf)
		if err != nil {
			slog.Warn("eventstream drain error", "error", err)
		}
		rawbuf = rawbuf[used:]
		for i := range msgs {
			slog.Debug("es msg", "headers", len(msgs[i].Headers), "payload", trunc(string(msgs[i].Payload), 400))
			fr := parseEnvelopePayload(msgs[i].Payload)
			if fr != nil {
				if !t.deliver(inbound{f: fr}) {
					return
				}
			}
		}
	}
}

// Closing an abandoned turn must also release a pump blocked on a full
// inbound channel; closing the WebSocket alone cannot unblock that send.
func (t *turn) deliver(in inbound) bool {
	select {
	case <-t.done:
		return false
	default:
	}
	select {
	case t.in <- in:
		return true
	case <-t.done:
		return false
	}
}

// parseEnvelopePayload turns one inbound event-stream payload (UTF-8 JSON)
// into the inner frame. Observed wire shapes:
//
//	{"payload":"<b64(inner)>"}                            (most frames)
//	{"eventType":..., "payload":{"payload":"<b64(inner)>"} (some builds)
//	{"type":<frame_type>, "payload":{...}}                (direct inner)
//
// Returns nil for unparseable payloads.
func parseEnvelopePayload(data []byte) *Frame {
	var outer struct {
		EventType string          `json:"eventType"`
		Type      string          `json:"type"`
		Payload   json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(data, &outer); err != nil {
		return nil
	}
	// Direct inner-frame form: {"type": ..., "payload": {...}}
	if outer.Type != "" && outer.EventType == "" {
		var f Frame
		f.Type = outer.Type
		f.Payload = outer.Payload
		return &f
	}
	// Envelope form A: payload is the b64 string itself: {"payload":"<b64>"}
	var b64s string
	if err := json.Unmarshal(outer.Payload, &b64s); err == nil && b64s != "" {
		if f := decodeInner(b64s); f != nil {
			return f
		}
	}
	// Envelope form B: payload is an object holding "payload": "<b64>"
	var pl struct {
		Payload string `json:"payload"`
	}
	if err := json.Unmarshal(outer.Payload, &pl); err == nil && pl.Payload != "" {
		return decodeInner(pl.Payload)
	}
	return nil
}

// decodeInner base64-decodes an inner frame JSON into a Frame.
func decodeInner(b64 string) *Frame {
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil
	}
	var f Frame
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil
	}
	return &f
}
