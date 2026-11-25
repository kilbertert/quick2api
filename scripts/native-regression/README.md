# Native relay regression

These scripts test a separately deployed, owned candidate at loopback port
19377. They do not operate Desktop UI, change providers or edit source chats.
Use only with an already registered native relay and an authorized local
account. Captures, recovered histories, credentials and raw replies must stay
in a private directory outside the repository.

## Recover real historical requests

`trace.cjs` exports `recover(snapshotPath, requestId)`. Recovery follows request
base IDs, not file order, and accepts reconstructed message groups or flattened
blobs only when their canonical SHA-256 matches. Missing data, a mismatched
shape or parse errors prevent a payload from being returned. Cache hints are
restored. `requestDigestVerified` remains false: this is logical transcript
recovery, not byte-identical HTTP replay.

Prepare a private root containing `cases.private.json` with entries such as
`{"label":"old-chat-a"}`. Each label directory needs `integrity.json` (the
recovery report) and `request.private.json` (the recovered payload). The runner
requires `verified`, zero missing messages and a verified shape. For example:

```powershell
node scripts/native-regression/run.cjs <private-root> <candidate-config> old-chat-a
```

Each run preserves the full recovered history and tool catalog, then adds an
explicit isolation restriction. The original-message observation is separate
from five gates: SSE Read/result, JSON AskUserQuestion/result with cache hints
and trailing environment context, an actual ENOENT, a different Read after the
error, and `tool_choice:none`. Only run-owned fixture files may be read.
Fixture color answers come from the test client; no human business decision
is made. Every other client action is rejected without execution. Tool calls
are delivered only after the complete response and tool JSON arrive.

For the fixed A–D checklist, add `--checklist`. This skips the original-message
observation and starts D1, D2, D3 and D5 from separate complete history copies
and fresh IDs. D4 continues D3's actual error exchange when valid; a failed or
refused scenario resets the next distinct scenario. Reports retain request
IDs, timestamps, latency, stop reasons and tool delivery for each HTTP request.
Provider refusals remain separate from local failures and are never retried.

A transport/protocol exception saves private evidence and resets the next
distinct planned scenario to the complete base history and a fresh ID. It does
not resend the failed action. Refusals are recorded as failures; a filtered
request must not be automatically retried or rewritten. Cases are serial;
at most two independent case processes should run concurrently. Failed gates
produce a nonzero exit status. Observations do not count as gates.

## State boundaries and cancellation

```powershell
node scripts/native-regression/extended.cjs <private-root> <candidate-config>
node scripts/native-regression/extended.cjs <private-root> <candidate-config> cancel-active
node scripts/native-regression/verify-stages.cjs <extended-report> <candidate-log>
node scripts/native-regression/summarize.cjs <private-root>
node --test scripts/native-regression/client.test.cjs scripts/native-regression/trace.test.cjs scripts/native-regression/private.test.cjs
```

Extensions use new synthetic histories, separate from the historical baseline:
two concurrent sessions with different file nonces, warm append, replacement
history under the same client ID, tool-catalog changes, tools disabled, setup
cancellation and a distinct subsequent task. `cancel-active` additionally
cancels after actual streamed text and checks a new Read with the same ID.
No side effect is executed during cancellation tests.

`verify-stages` correlates request IDs with handshake stages: append should
have no new handshake; forks/catalog changes should have one. It also verifies
that active cancellation followed upstream text. These are observed setup and
functional checks, not remote cancellation acknowledgements or proof of all
internal isolation. Local Go lifecycle tests separately instrument job stop,
state cleanup, socket discard and fresh session selection.

Responses and failure evidence are saved privately. Console/report errors use
fixed parsing errors instead of private JSON excerpts. An absolute deadline
bounds each request even if SSE heartbeats continue. Partial responses cannot
execute client tools. The summary includes every saved run, excludes original
observations (including older reports with a missing flag), and keeps baseline
and synthetic results separate. Do not average them into a stability claim.

## Cache movement and duplicate contracts

```powershell
node scripts/native-regression/contract.cjs <private-root> <candidate-config>
node scripts/native-regression/verify-contract.cjs <contract-report> <candidate-log>
```

This independent synthetic scenario stops on its first failure. It reads one
run-owned fixture, moves system/tool/message cache hints and text representation
while submitting the actual result, then verifies an equivalent completed
request receives the specific duplicate rejection and `x-should-retry:false`.
It next asks a CSV/JSON fixture question on the warm conversation and submits
the test client's fixed CSV answer with another cache move and trailing context.
Unexpected actions are rejected without execution. No old filtered history is
replayed, and no Desktop UI or human business choice is automated. The summary
lists every `contract-*` run separately from old-history and boundary results.

## Recorded baseline, November 24, 2026

Four authorized old-chat snapshots retained 116 tools each, with 178, 302,
1,524 and 627 messages. All message hashes and shapes matched; the HTTP request
digest was not verified. The five-gate baseline passed 4/5, 5/5, 2/5 and 1/5:
12/20 total, plus four original-message observations. Five failed gates ended
in upstream content filtering; three had HTTP/SSE transport errors. The latter
coincided with upstream WebSocket 1011 handshake closures, whose server-side
cause is unknown. Multiple steps share history, so failures are not independent
samples. The original baseline harness did not reset after transport exceptions;
later failures can include continuation effects. Preserve the raw reports.

The expanded synthetic run passed 9/9 gates; the additional active-cancel run
passed 2/2, with correlated stage checks passing for both runs. These results
do not override the old-history failures or establish Desktop UI acceptance.
No filtered baseline was automatically replayed after the local fix.

The baseline also exposed a local handshake bug: compatibility mode swallowed
fatal close/error/cancellation as if an acknowledgement were merely absent.
It now tolerates only the explicit acknowledgement-timeout sentinel, reports
unconfirmed stages accurately, and checks already queued fatal input at the
deadline. Loopback regressions cover both handshake stages, close/error/cancel,
strict/compatible missing acknowledgements, deadline races and active HTTP
disconnect cleanup. This fixes local failure handling; it does not fix an
upstream 1011 or override upstream filtering.

## Handshake-only transport isolation

```powershell
go run ./scripts/native-regression/handshake -config <candidate-config> -out <new-private-report>
go run ./scripts/native-regression/handshake -parallel 12 -config <candidate-config> -out <new-private-report>
```

This diagnostic owns a temporary outpost on an ephemeral port, reads its own
registered device and opens twelve fresh cloud sessions (six serial, then
three pairs). It sends no user messages, uploads no history, invokes no tools
and closes every connection and its own outpost. Reports contain only stages,
frame types, elapsed times and error categories/codes. Failed probes save the
report and exit nonzero. Use a new report path; existing files are never
overwritten. Do not infer model or Desktop acceptance from this probe.
The `-parallel 12` mode starts all twelve probes together for checklist B1;
the setup gate still serializes their handshakes. Each probe records a local
request ID, start timestamp and cumulative stage times. Other local processes
are outside this gate.

Before process-wide setup serialization, the live matrix passed serial 6/6
and paired 4/6, with two 1011 closures before the first acknowledgement. The
same-sized matrix with the gate passed 12/12. No request content was involved.
This supports a concurrent-setup defect and a mitigation in this process,
not a unique server-side diagnosis, a stable failure-rate estimate or a remedy
for upstream content filtering. A second paired connection includes queue
wait; existing inference is not gated. Other processes remain uncoordinated.

After deployment, a new synthetic boundary run passed 9/9 with correlated
stage checks, and a separate active-output cancellation run passed 2/2. A
previously passing 302-message old-history copy passed 4/5 on its next run:
Read, ENOENT, subsequent Read and no-tool passed, while the JSON question
request received an upstream content filter. There was no observed transport
failure in these new model tests. This is further evidence that transport
repair does not establish stable old-history acceptance. The filtered
question was not retried; the original-message observation delivered no client
tool and was not counted as a gate.

## Upstream new-session limit and setup cooldown

Quick limits new sessions per device: about four succeed, then setup is closed
with WebSocket 1011 for roughly a minute. The proxy reports this before any user
message is sent. `client.cjs` therefore waits 90 seconds and resends only that
exact condition (new request ID suffix `-setupN`, at most four attempts); each
wait is recorded as `setupCooldowns`. Content filtering and every failure after a
possible send are never resent. The handshake diagnostic takes `-cooldown 90`
for the same behavior; reports then include `cooldownWaits`.
