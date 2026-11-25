# Native relay progress — November 24, 2026

This checkpoint improves local transport, continuation contracts and diagnostic
coverage. It does not establish that every historical Desktop conversation has
recovered.

## Changes

- Discover the currently advertised, device-qualified native relay instead of
  reusing an unqualified name from conversation history. Require current
  invocation evidence before claiming a current tool-channel failure.
- Distinguish structured unknown-tool errors from successful tool output that
  merely quotes an earlier error. Diagnostics retain metadata, not tool data.
- Hash canonical history incrementally; overlap large-context upload/policy
  approval with cold setup, and check already-approved attachments immediately.
  Conversation binding still waits for setup; policy failures remain failures.
- Stop swallowing handshake closure/error/cancellation in compatibility mode.
  Serialize cold setup for the same account/device across Client instances in
  this process. Established inference remains concurrent and queue waits cancel.
- Preserve typed setup failures while withholding arbitrary upstream bodies,
  close reasons and URLs. Native HTTP/SSE responses advertise no automatic retry.
- Accept equivalent text representations and moved system/tool/message cache
  hints during ordinary results, parked question answers and warm reuse.
  Schema data and precise numbers remain significant. The same normalization
  protects completed-request deduplication against representation-only replays.
- Add reproducible private-history recovery, synthetic boundary/contract and
  handshake-only tooling. Keep failures and test categories separate.

## Evidence

Local `go test ./...`, `go vet ./...`, `go build ./...`, ten Node tests and diff
checks pass. Two independent reviewers assessed the implementation.

The handshake-only diagnostic sent no prompts or tools: the original matrix
passed serial 6/6 and paired 4/6, with two pre-ACK 1011 closures. With the gate,
the same matrix passed 12/12. This supports a local mitigation, not a unique
server-side root cause or a stable failure-rate estimate.

The latest isolated candidate passed the independent synthetic cache/duplicate/
question contract run 3/3 and an additional synthetic boundary run 9/9. Actual
fixture reads and test-client answers were used. Correlated metadata verified
ordinary-result continuation, duplicate rejection without setup, warm reuse,
fresh setup after the question boundary, forks and setup cancellation.

## Still unresolved

The four-history baseline passed 12/20, with five upstream filters and three
transport failures. A later 302-message history run passed 4/5: its question was
filtered upstream before client tool delivery, while the other four gates
passed. No filtered request was automatically replayed or rewritten.

The specific upstream filtering rule remains unknown. Synthetic successes and
transport repair do not prove those old histories have recovered. The latest
acceptance used an isolated API client, not Desktop UI or real project tasks;
it does not cover every declared tool. Raw histories, credentials and live
reports stay outside the repository. See [native-client-relay.md](native-client-relay.md)
and [the regression guide](../scripts/native-regression/README.md) for details.
