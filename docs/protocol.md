# Quick upstream protocol — what quick2api actually speaks

This document is the load-bearing knowledge. Everything below was recovered
from the Amazon Quick desktop app (1.2.x) main/renderer bundles and verified
against a live account in November 2026.

## 1. Session acquisition (local, HTTP)

The Quick desktop app ships a Python "outpost" agent
(`quickwork-agent`, extracted from `resources/agent.zip` into
`%LOCALAPPDATA%\quickwork\agent\`). The desktop spawns it with:

```
quickwork-agent --outpost-only --manifest desktop --port 8780
```

and hands it a random 64-hex token on **stdin line 1**. The outpost exposes
loopback HTTP; the only endpoint we need:

```
GET  http://127.0.0.1:<port>/health                      → 200 {"status":"ok","service":"outpost"}
GET  http://127.0.0.1:<port>/auth/sncs-session           → 200 session JSON   (Bearer: the stdin token)
```

Environment: `QUICKWORK_BUILD_STAGE=prod` is mandatory — without it the
agent exits with RuntimeError.

Session JSON (key fields):

| field | meaning |
|---|---|
| `id_token` | ~230-char JWT-ish token; goes in the WSS `token=` query param |
| `ws_host` | `qbs-cell<uuid>.dp.appintegrations.<region>.<stage>.plato.ai.aws.dev` |
| `tenant_url` | `https://<same host>` — base for the REST/qbs-event surface |
| `provider` | `social` (consumer Quick) — selects the AIDP transport, NOT the gateway transport |
| `actor_arn` | user arn for `session_connect` |
| `aws_account_id` | the Quick account |
| `usable_until` | unix seconds; the underlying login is ~7 days, refreshed by the desktop app |

Credentials chain: the agent reads the desktop app's login from **Windows
Credential Manager** (`quickwork-social-social-*` entries). A second
outpost spawned by us reads the same credential store — that is why no
extra login is needed.

## 2. Two transports — use AIDP

| provider | transport | chat path |
|---|---|---|
| `gateway`, `social_gateway` | Gateway WSS | CBOR+event-stream, different URL |
| `enterprise`, `social` | AIDP WSS | `/streaming/quickwork-chat-bidirectional-stream`, JSON-in-b64 |

Consumer Quick (what we target) is `social` → AIDP.

There is also a REST surface
`POST {tenant_url}/integration/quick-work/qbs-event` for
`invokeAgent:{path,queryParams:{_method},body}` — it answers `api/*` RPCs
(e.g. `api/chat-agents`, `api/ws-rpc` session management) but **does not
run chat turns**: `user_message` replies `unknown_message_type`. Chat is
WS-only.

## 3. The chat WebSocket

```
wss://<ws_host>/streaming/quickwork-chat-bidirectional-stream
    ?surfaceType=QUICK_WORK_DESKTOP_WINDOWS
    &token=<id_token>            # percent-encoded
    &outpostId=<registered outpost device id>   # NOT arbitrary — see below
    &workingSessionId=<uuid>
    &channel=prod
Header: Origin: https://quick.aws.com     # required — no Origin, no upgrade
```

Outbound frames are the same JSON envelope, sent as a **binary** WS
frame whose payload is UTF-8 text (the renderer passes `Uint8Array` to
`WebSocket.send`). Text frames are rejected with
`{"Message":"Unexpected WebSocket frame received."}` and a close 1000.

```json
{"eventType":"rawInputEvent","payload":{"payload":"<base64(inner JSON)>"}}
```

`inner JSON = {"type": <frame_type>, "payload": {...}}`.

Inbound frames are **binary AWS event-stream** messages (prelude CRC +
headers + payload + trailing CRC — CRCs are advisory, several endpoints
differ). Each message payload is UTF-8 JSON in *either* shape:

```json
{"payload": "<base64(inner JSON)>"}                              // most frames
{"eventType": <outer>, "payload": {"payload": "<base64(inner JSON)>"}}
```

and the inner JSON is the same `{type, payload}` shape.

Two things that bite if skipped:

- `?outpostId=` must be the outpost's **registered** device id, learned
  from `GET /local/list_tools` (`outpost_id`). An arbitrary string gets
  a polite close-1000 right after `session_established`.
- `session_connect` needs the capability flags the renderer sends
  (`acp_first_text`, `user_notification`, `subtask_progress`,
  `external_agent_sign_in`, `recents_promotion`, `appVersion`,
  `clientInstanceId`) — a bare `{actor_id,surfaceType,outpostId}`
  connect gets `{"type":"status","payload":{"status":"unknown_
  message_type","received_type":"session_init"}}` and an early close.

## 4. Turn sequence

```
→ {"type":"session_connect","payload":{"actor_id":<arn>,
     "surfaceType":"QUICK_WORK_DESKTOP_WINDOWS","outpostId":<registered>,
     "appVersion":"1.2.4660","clientInstanceId":<uuid>,
     "acp_first_text":1,"user_notification":1,"subtask_progress":1,
     "external_agent_sign_in":1,"recents_promotion":1}}
→ {"type":"federate_status","payload":{}}          // optional, harmless
← session_established + connected (user info) + federate_auth_status

→ {"type":"session_create","payload":{"session_id":<uuid>,"title":<any>}}
← session_created

→ {"type":"user_message","payload":{
     "thread_id":<same uuid>, "text":<prompt>, "message_id":<uuid>,
     "agent_mode":"smart",                  # optional
     "thinking_effort":"high",              # optional
     "user_timezone":"Asia/Shanghai",
     "user_locale":"zh-CN"}}
← message_start
← text_delta        {"delta": "…"}         (N times — the answer body)
← thinking_delta    {"delta": "…"}         (optional — reasoning, not shown)
← turn_usage        {"input_tokens":…, "output_tokens":…,
                     "cache_read_input_tokens":…, "cache_write_input_tokens":…,
                     "model":"smart", "resolved_model_id":"us.anthropic.…",
                     "cost_usd":…, "context_window":…}
← message_complete
← keepalive                                 (idle heartbeats, ignore)
```

Local handshake failure handling (corrected after live 2026-11-24 regressions):
the compatibility setting may tolerate only a missing acknowledgement deadline.
Socket closure (including observed 1011 during `session_connect`), explicit
error frames and caller cancellation make the handshake return failure instead
of advancing. The handshake itself does not retry, and native jobs do not
retry; the existing compatibility bridge's reconnect policy is separate.
Queued fatal input at the deadline must not be hidden by
timer selection. Tolerated timeouts emit `session_connected_unconfirmed` or
`session_created_unconfirmed`, not a confirmed acknowledgement. This is a local
error-handling rule; it does not establish the server-side cause of a 1011.

A separate handshake-only live diagnostic on 2026-11-24 sent zero
`user_message` frames, uploaded no history and invoked no tools. Six serial
connections succeeded; of six connections opened in pairs, two closed with
1011 before the first acknowledgement. This reproduces a transport failure
without any model prompt. With setup serialized, the same twelve-connection
matrix passed. It supports concurrent setup as a contributing condition, not
the sole cause of every 1011 or an explanation for content filtering.

Cold connections now acquire a process-wide gate keyed by host, tenant,
actor, account and registered device. Tokens are excluded so refresh cannot
bypass the gate. Ownership covers dial, connect/create and settle, and ends
before sending a user message. Existing conversations can infer concurrently;
oversized-context preparation can still overlap setup. Queue waits are
cancellable; all setup failures release ownership. Metadata stages
`connection_queued` / `connection_acquired` expose waiting separately from
network setup. This coordinates only this proxy process, not other proxies or
the official Quick application. A queued cold connection takes longer by design.

`ConnectionSetupError` retains its cause for classification while reporting
only the failed stage, close code if available, and that no user message was
sent on that connection. It does not expose dial URLs, response bodies or
arbitrary close reasons. Native HTTP and SSE responses advertise
`x-should-retry: false`; clients that honor it must not replay a failed job.
This header is advisory and does not control every SDK or intermediary.

Also possible mid-turn: `tool_start` / `tool_arguments` / `tool_progress` /
`tool_result` / `tool_error` (the agent runs its own tools upstream; use their
boundaries to separate text spans, not as client tool calls), `error` `{code,message}`,
`guardrail_blocked`, `session_end`, `session_title_updated`, various
`*_sync` housekeeping frames.

### Registered MCP relay and duplicate device delivery (2026-11-24)

An independently spawned outpost loaded the user's UI-approved MCP registration
only with the selected account profile passed as `QUICKWORK_ROOT`. Its
`GET /local/mcp/list` returned an enabled, connected server and tool
`quick2api_client_relay__quick2api_client_call` with the relay routing schema.
`/local/list_tools` still listed only the built-in tools and the outpost ID.
Do not infer MCP availability or synthesize an `mcp__` prefix from that catalog.

Observed native WebSocket device frame:

```json
{"type":"tool_call","payload":{"tool_call_id":"<id>",
 "tool_name":"quick2api_client_relay__quick2api_client_call",
 "arguments":{"params":{"route_id":"<turn-token>:read_1",
 "name":"Read","arguments":{"file_path":"<approved-fixture>"}}}}}
```

Courier to the proxy-owned outpost uses `POST /local/invoke_tool`, bearer
authentication, and `{tool_name, arguments, wire_paths:true, tool_call_id,
request_timeout_ms}`. Preserve `arguments` including its `params` envelope.
A diagnostic that passed the relay's raw arguments instead caused the outpost
to unwrap the client's nested `arguments` and the MCP server rejected it.
The response is returned unchanged in
`{type:"tool_result",payload:{tool_call_id,result:<outpost JSON>,error:null}}`.
An HTTP 200 can still contain an actual `isError:true` tool failure.

The same invocation was also observed arriving directly through MCP before
its WebSocket courier delivery. Without invocation receipts this produced two
external client tool requests from one upstream job. The relay now binds a
per-action suffix in `route_id` to one name, canonical input and actual result.
The real isolated acceptance run had two MCP callbacks, one actual fixture Read,
one upstream RunTurn/socket and the correct final first line. This establishes
that tested path only; it is not proof that all old Desktop sessions recover.

An actual Claude Code client then exposed two continuation representation
differences: an earlier message's content changed from a string into a text
block array, and its echoed assistant tool block carried cache metadata. Raw
JSON equality rejected the valid continuation with HTTP 409, causing repeated
client retries after a successful Read. Native history/call matching now
normalizes text strings into text blocks and ignores only content-block
`cache_control` hints. Text, roles, message identity, tool IDs, names, numeric
inputs (including large integers) and attachment sources remain exact. A
`cache_control` property inside a tool's input is ordinary data and is retained.

### Conversation reuse and content-filter errors (2026-11-23)

The upstream thread is stateful. A downstream conversation id is a local
transcript key, not permission to append concurrent prompts to one upstream
thread. Live regression: two overlapping requests with the same client id
asked for an ALPHA sequence and BETA_COMPLETE respectively; both returned the
ALPHA sequence before serialization was added. Serialize lane selection, first
dial and Send per client conversation. Reuse a connection only for an
append-only transcript with an unchanged preamble. Seed divergent histories
and reopened connections into fresh upstream UUIDs; replaying a full transcript
against its old server-side UUID duplicates context.

Content filtering can also arrive as an `error` frame with this observed
payload (a real Desktop fork), rather than `guardrail_blocked`:

```json
{"error":"The model's content filter stopped this reply. Rephrase, or start a new chat.","error_code":"content_filtered","initiator":"user"}
```

Map this to `content_filter` (Anthropic `stop_reason: refusal`), not a generic
unavailable-model error. Do not retry it through the chunk fallback. After an
error, cancellation, partial close, or guardrail termination, close the socket
and discard its pending frames rather than pooling an ambiguous turn boundary.
This preserves upstream checks; it does not remove Quick's agent constraints.
Attachment-policy failures and request cancellation/deadlines must also be
excluded from the bridge's fresh-connection retry path. Disabling chunk
fallback alone does not prevent the bridge from repeating a failed upload.

### Native-tool narration before client tool JSON (2026-11-23)

Observed in two actual Desktop requests: three `text_delta` events announce
an attachment read, followed by two native `tool_start` / `tool_result` pairs,
then the final text span contains a complete client `tool_use` JSON object.
Concatenating all text makes the answer look like
`I'll read the attached context first.{"type":"tool_use",...}` and defeats
whole-response JSON parsing. Track the byte offset of the latest native tool
boundary. Only that final text span is eligible for client-tool JSON parsing;
ordinary text responses retain all text. Do not extract arbitrary embedded JSON
from prose, and never convert blocked or incomplete output into a tool call.

### Per-message text limit (~64–80 KB)

A single `user_message.payload.text` over roughly **64–80 KB** makes the
upstream close the turn with an empty `{"type":"error","payload":null}`
frame — no code, no message. Verified 2026-11-22: 64 000 bytes goes
through, 80 000 does not; the exact cutoff sits somewhere in between and
probably depends on framing overhead.

quick2api has two workarounds, tried in this order:

**Primary — out-of-band attachment.** The renderer uploads files through
the tenant's attachment service and references them from a short
`user_message`; the model reads the file server-side in **one** turn.
quick2api mirrors that path:

1. `POST {tenant}/integration/quick-work/upload-attachment?name=…&contentType=<MIME>&contentLength=…`
   with the prompt as the octet-stream body → `{attachmentId}`.
2. `GET {tenant}/integration/quick-work/attachment-dlp-status?attachmentId=…`
   polled until `policyAction=ALLOW` (or `dlpJobStatus` is
   SKIPPED/NOT_APPLICABLE/DISABLED).
3. Deliver: `POST {tenant}/integration/quick-work/qbs-event` with
   `invokeAgent{path:"files/upload/<conversationId>",_method:POST,body:{attachmentId}}`
   → `{path}` (nested inside `eventResponse.invokeAgentResponse` →
   `api_response.body`).
4. `user_message` is sent with
   `payload.attachments=[{type:"file",name,size,mimeType,path,attachmentId}]`
   and a short text telling the model to read the file.

Prompt/context uploads use `text/plain`; client-provided image and document
uploads retain their MIME type. Tool-result attachments use this same path.
Anthropic attachment references carry their transcript position, so clean
append-only continuation uploads only newly referenced attachments; reseeding a
fresh upstream thread still uploads every attachment in that history.
If a caller-provided attachment cannot be delivered, return an error rather
than dropping it and asking the model to answer without the input. An explicit
DLP denial, a malformed/non-successful policy check, or an unsettled check
terminates the request; none may fall back to sending the content as chunks.

Note the classic MTU pattern: the ~64–80 KB limit lives in the frame
codec, not the model — attachments bypass the frame entirely.

**Fallback — sequential chunks.** If the upload or deliver step fails
(e.g. attachments unavailable on a provider/edition, or `path` comes back
empty), the prompt is split into ≤ 55 000-byte rune-boundary chunks sent
as sequential `user_message` turns in the same session. Non-final chunks
are prefixed `[CONTEXT PART i/N — do not answer yet, just store this
context]`; the last gets `[CONTEXT PART N/N — final part. Now answer
based on ALL parts above.]` and its `text_delta` / `turn_usage` /
`message_complete` frames are what the caller sees.

Side effects of the chunk fallback:

- **One API call = N upstream turns.** A 200 KB prompt becomes ~4 model
  turns, so upstream token cost and latency scale with chunk count —
  this is exactly why the attachment path exists.
- Interim chunks' replies are discarded; only the last turn's reply is
  surfaced.
- Empty `error` frames from the upstream lose their detail — see
  `debug.log_frames` if you need to watch the raw inbound stream.

## 5. What the "model" is

`agent_mode` selects a server-side preset. Observed Nov 2026 on a LITE
account: `agent_mode:"smart"` →
`turn_usage.resolved_model_id = "us.anthropic.claude-opus-5-5"`,
`context_window` 1,000,000. The account is billed `cost_usd` per turn —
**this proxy spends your Quick subscription quota**, not AWS credits.

## 6. Hard constraints

- **The outpost must run on the same Windows machine** as the logged-in
  Quick desktop install (credential store + agent binary live there).
  quick2api therefore runs on Windows; the API can be consumed from
  anywhere that can reach the listen address.
- `id_token` in the WSS query is unavoidable (server design); keep the URL
  out of logs — we never print it.
- No streaming cancellation beyond closing the socket (the protocol has
  `steering_cancelled` for the app's own UI, not wired here).
