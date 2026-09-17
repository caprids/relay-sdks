# Issue #21: restart replay and retry amplification

On 2026-09-17, a bounded trace against `https://sjc.relay.mux.global` reproduced
[issue #21](https://github.com/muxinc/relay-sdks/issues/21) using the Python SDK at
`c218cec76d37fdbb47ec9cd45eb4bdad25082bd6`. The deployment was reported as moq-relay
0.14.17; the endpoint did not independently expose its version. This investigation
supports an application retry mitigation and a relay-side follow-up, not an SDK
exactly-once-delivery fix.

## Method and results

Each case used a fresh random namespace under the authorized prefix, one persistent
subscriber connection, and two publisher connections sequentially announcing the same
namespace and track. Each publisher wrote 15 JSON objects at 5 objects/s, carrying
`generation` and `seq`, then closed cleanly. After the first publisher fully closed,
the harness waited 4 seconds before opening the second, then observed 6 seconds after
the second closed. A new publisher's group IDs started at zero.

The trace recorded outgoing subscription IDs, decoded incoming control messages in
`MoqtSession._handle_subscription_message` before calling the original handler,
callback counts, and each object alongside its subscription ID. Both SDK packages
were explicitly imported from the issue worktree, not the original editable install.
This was a decoded request-stream trace, not a packet capture or relay-internal trace.

| Retry policy after done | Requests | Wire done / callbacks | Published / delivered | Repeated payloads across requests |
| --- | ---: | ---: | ---: | ---: |
| Cancel old subscription, retry after 10 ms | 15 | 2 / 2 | 30 / 30 | 0 |
| Keep old subscription until test cleanup, retry after 10 ms | 59 | 46 / 46 | 30 / 74 | 44 |
| Cancel old subscription, back off 0.5, 1, 2, 4 s (cap) | 11 | 5 / 5 | 30 / 33 | 3 |

All cases also retried request errors after 500 ms. The backoff case applied the
increasing schedule to done notifications without resetting on `SUBSCRIBE_OK` or
cached objects; it was a diagnostic control, not a complete production retry policy.
Each case received data from both publisher generations. In every case, request IDs
were unique, each request received at most one done callback, and there were **zero
repeated `(generation, seq)` payloads within a single subscription**.

For example, without cancellation:

```text
time (s)  request  received control or object
22.270    2        PUBLISH_DONE: 2, "track ended"
22.332    4        SUBSCRIBE_OK: track alias 4
22.332    4        group 14, object 0; generation 0, seq 14
22.383    4        PUBLISH_DONE: 2, "track ended"
22.444    6        SUBSCRIBE_OK: track alias 6
22.444    6        group 14, object 0; generation 0, seq 14
22.495    6        PUBLISH_DONE: 2, "track ended"
22.556    8        SUBSCRIBE_OK: track alias 8
22.557    8        group 14, object 0; generation 0, seq 14
22.607    8        PUBLISH_DONE: 2, "track ended"
```

This is fresh cached delivery and a fresh terminal response for each request, not
repeated invocation of a callback for one received control message. The reproduction
without cancellation shows that a cancellation/reset race is not required. The clean
immediate-cancellation case also shows timing variability: failure to reproduce in one
short run is insufficient evidence that the relay behavior is fixed.

## Ownership and limits

The SDK allocates increasing even request IDs and dispatches decoded `PUBLISH_DONE`
to that request's callback. It deliberately keeps the alias route alive until
cancellation or connection closure so trailing data streams can still arrive. The
TypeScript implementation follows the same policy. Existing tests cover fresh request
IDs and trailing delivery after done in both languages.

The 2026-09-16 20-generation run reproduced replay in both SDKs: Python made 428
resubscription attempts with 299 repeated payload callbacks; TypeScript made 448 with
269. Its done callbacks immediately cancelled and re-subscribed. The configured retry
delay affected request errors only, despite the original issue describing a delay
following done. That distinction explains why the loop could run at round-trip cadence.
The new control trace above covers Python; it does not claim a new TypeScript wire trace.

Relay source at `moq-relay-v0.14.17` sends `SUBSCRIBE_OK` with a per-request alias,
serves the track's selected range, then sends `PUBLISH_DONE` when that serving operation
ends (`rs/moq-net/src/ietf/publisher.rs`). This is consistent with the observed cached
replay, but identifying exactly which relay retained-track transition causes it
requires a relay-side trace or regression test. The evidence does not prove every
restart-related defect is relay-owned or that status-0 errors are harmless.

The scoped repository change is the documented
[retry and continuity policy](../relay-interop.md#publisher-restarts-and-resubscription).
No automatic retry, object suppression, or terminal-callback suppression is added to
the low-level SDK. The relay follow-up should cover fresh subscriptions to an ended
cached track, clean namespace withdrawal, replacement publisher generations, and
both cancellation and bounded draining of the old subscription. Backoff reduces
request load but does not remove cached replay; issue #21 should remain open for that
behavior until the desired relay semantics are agreed and verified.
