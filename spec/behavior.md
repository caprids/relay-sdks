# SDK behavior contract

Every Relay SDK, in any language, implements the same protocol pin and the same observable
semantics. This document states those semantics without reference to any one language. The
language READMEs describe how each API spells them; this file says what must be true; and
`vocabulary.md` fixes the public names every SDK uses for the concepts named here.

`protocol.json` holds the machine-readable pins. Each SDK has a test that checks its constants
against that file.

## Connection

- The MOQT version is the draft pinned in `protocol.json`, currently draft-ietf-moq-transport-20
  with wire identifier `moqt-20`. An SDK offers that protocol identifier and no other.
- Authentication is a JWT supplied by the application. The SDK sends it as the `jwt` query
  parameter on the endpoint URL. An explicit `jwt` parameter already present on the URL takes
  precedence over a token callback, and the callback is not called. An empty `jwt` parameter
  (`?jwt=` or a bare `?jwt`), including one among several `jwt` parameters, is an argument error
  (see "Arguments") rather than a way to connect with no credential; the SDK does not treat it as
  absent. A whitespace-only value is not empty and is sent as given, as a token callback's would be.
- SDK-generated logs and error text do not include the token, and SDK tooling does not persist it
  to disk. Tooling redacts the supplied token from browser diagnostics it forwards. The browser
  itself is outside that guarantee: Chromium can log the full authenticated WebTransport URL on
  handshake failure. Applications must sanitize captured diagnostics before storing or forwarding
  them; see `ts/README.md`, "Browser token exposure".
- Connecting has one deadline that covers token resolution and both handshakes, transport and
  MOQT setup.
- The application can give the SDK a cancellation primitive, such as an abort signal or a
  cancellation token, that governs the connection's entire lifetime rather than only setup.
- Every connection exposes a way to observe its closure and the error that caused it, if any.
- A GOAWAY from the relay ends the connection with an error. The application opens a replacement.
  Subscriptions still awaiting their first response end as terminated by the closure (see
  "Subscription lifecycle"); accepted or cancelled requests do not receive a second initial
  response. The core's own track source still fails pending requests through their error callback.
  Automatic reconnection and token refresh on a live session are not yet part of this contract.

## Error identities and operation phases

Branch on `code` and `phase`, not message wording. SDK lifecycle errors use these stable codes:

| Code | Application response |
| --- | --- |
| `CONNECTION_TIMEOUT` | Connecting exceeded its deadline; consider a new connection with backoff. |
| `WRITE_TIMEOUT` | Inspect the phase below, then choose the next object or group explicitly. |
| `CLOSED` | The handle is closed; use an open connection, track, or group. |
| `WRITE_PENDING` | Await the outstanding operation or finish/abort the open group before starting another. |
| `NAMESPACE_NOT_SOLICITED` | Received namespace solicitations do not cover this publisher; check the namespace and token configuration. A later matching solicitation can restore demand. |
| `NAMESPACE_ENDED` | The relay ended the namespace announcement; open a replacement publisher if appropriate. |
| `SUBSCRIPTION_REQUEST_FAILED` | An async subscription request failed; inspect its preserved MOQT request error and the connection state. |
| `SUBSCRIPTION_OVERFLOW` | The event buffer filled; the subscription was cancelled. Choose a suitable capacity or faster consumption before subscribing again. |
| `UPDATE_QUEUE_FULL` | More than eight updates were issued before the subscription was accepted; await acceptance (`onSubscribed` / `on_subscribed`) before updating again. Nothing was sent for the refused update. |
| `SUBSCRIPTION_CHANGED` | Upstream demand ended or changed during an operation; choose fresh data or a new random-access point. |
| `GOAWAY` | The relay requested migration; open a replacement connection. |
| `TRANSPORT_ERROR` | Inspect the original cause and connection state before deciding whether to reconnect or publish again. |
| `TRANSPORT_UNAVAILABLE` | Supply a transport implementation in a runtime without browser WebTransport (TypeScript). |

`connecting` covers token resolution and connection setup. `connection` marks a connection-wide
failure; it makes no claim about individual writes that were in flight. `subscription` marks an
async subscription request failure or local event-buffer overflow. Publishing records
`waiting-for-demand`, `opening-stream`, `writing`, or `closing` before entering each step.
A `WRITE_TIMEOUT` in `waiting-for-demand` means this attempt sent no object bytes. For timeouts or transport/subscription failures in a transport
phase, conservatively allow for partial delivery; `closing` can fail after all object bytes were
sent. Opening a group can start sending its header before the first object write. A phase records
the failure's origin, and a later call on the failed handle can report that same error.

For `WRITE_PENDING` and calls rejected because a handle is already closed, phase describes the
refused operation; that call sends nothing. `CLOSED` can also interrupt an operation already in
flight, so the code alone does not rule out partial delivery.

No code or phase promises safe replay. Account for previous attempts, data freshness, and your
application's deduplication policy. Successful writes mean transport acceptance, not viewer
acknowledgment. The SDK does not automatically retry or reconnect. Neither a connection failure nor
an uncovered namespace proves authentication was denied.

Clean connection shutdown still resolves `closed` without an error; subsequent operations fail
with `CLOSED`. After a failed connection, `closed` and subsequent connection operations expose the
same failure. Caller cancellation reasons and token-provider exceptions retain their identity.
Argument and serialization errors keep their idiomatic language types. Subscription `onError` /
`on_error` callbacks retain the separate MOQT `RequestError` contract and numeric protocol codes.

Both SDKs export one Relay error type, `RelayError`, with a stable `code` discriminant and a `phase`
field, plus the three subscription errors that carry payload (`SubscriptionRequestError`,
`SubscriptionOverflowError`, `SubscriptionConnectionClosedError`). There are no other subclasses;
callers branch on `code`. Human-readable messages are not a compatibility contract. Wrapped
transport and session failures retain the original cause: TypeScript in `RelayError.cause`, Python
chained as `__cause__`. Transport-layer exceptions are not Relay errors; a Relay operation that fails
on the transport raises `TRANSPORT_ERROR` with the transport exception as its cause.

Local argument errors remain `TypeError`/`RangeError` in TypeScript and `TypeError`/`ValueError` in
Python: a value of the wrong type or shape is a `TypeError` in both languages; a value of the right
type outside its bound is a `RangeError` in TypeScript and a `ValueError` in Python. The
[Arguments](#arguments) table lists the rules. A refused concurrent operation uses `WRITE_PENDING`
and does not cancel the active operation. Caller-provided cancellation reasons bypass wrapping,
including non-Error JavaScript values and Python task cancellation. Connection failures are shared
with pending publishing operations; their connection phase does not establish whether those writes
sent bytes.

## Arguments

The values an application hands the SDK that end up on the wire are checked in their original
container, copied once, and only then used. Both roles apply the same rules, so a namespace the
publisher accepts is one the subscriber accepts. Arguments are checked before connection or slot
state at every entry point: a bad argument is reported the same way on a closed connection or a busy
track, and it never reaches the token provider, the transport factory, a request id or a stream. The
portable cases live in `vectors/arguments.json` and run in every SDK's suite.

| Argument | Rule | `TypeError` in both languages | `RangeError` (TypeScript) / `ValueError` (Python) | Checked at |
| --- | --- | --- | --- | --- |
| Namespace | 1 to 32 non-empty string fields in an array (TypeScript) or a list or tuple (Python); sent verbatim, never split or joined on `/`. The 32-field limit is `limits.maxNamespaceFields` in `protocol.json`. | any other container, including a single string; no fields; a field that is not a non-empty string | more than 32 fields | `connectRelayPublisher` / `connect_relay_publisher`; `subscribe`, `subscribeEvents` / `subscribe_events`, `trackSource.subscribe` / `track_source.subscribe` |
| Track name | A non-empty string, in both roles. | anything else | | `registerTrack` / `register_track`; the three subscriber entry points |
| Properties | A list of `{type, value}` pairs. `type` is a non-negative integer; an even type carries a non-negative integer, an odd type carries bytes, which are copied. Integers are bounded by what the language's core varint carries: 2^53-1 in TypeScript, 2^64-1 in Python. Each byte value is at most `limits.maxPropertyValueBytes` (its 16-bit wire length) and the encoded list at most `limits.maxObjectPropertiesBytes`; the SDK measures the encoding with the core's own encoder before any slot or stream is taken. | not a list; a pair without the shape; a type or integer value that is not a non-negative integer the varint carries; bytes where an integer belongs or an integer where bytes belong | a byte value longer than its wire length; a list whose encoding exceeds the object bound | `registerTrack` / `register_track`; `write` on a track or group writer |
| Endpoint | An `https` URL with a host and no fragment or userinfo. A `jwt` query parameter, when present, is not empty; this holds for every `jwt` parameter when there are several. | another scheme; a fragment or userinfo; no host; an empty `jwt` parameter | | `connectRelaySubscriber` / `connect_relay_subscriber`, `connectRelayPublisher` / `connect_relay_publisher` |
| Payload | A byte sequence: any `ArrayBufferView` in TypeScript; any object supporting the buffer protocol in Python (`bytes`, `bytearray`, `memoryview`, ...). Copied once. At most `limits.maxObjectPayloadBytes`. | a string, an `ArrayBuffer`, a list of integers, anything else | longer than the object payload limit | `write`, `writeText`, `writeJSON` on a track or group writer |

Messages are not a compatibility contract; the class and the point of rejection are.

## Subscribing

- Namespaces are ordered tuples of fields sent verbatim. The SDK adds no prefix and performs no
  normalization. Fields are absolute: the token's root prefix is part of the namespace the
  application supplies. Namespaces and track names follow the [Arguments](#arguments) table in both
  roles: an invalid one is rejected locally, before connecting a publisher or sending a subscription.
- A known track can be subscribed directly. The SDK does not interpret payloads and does not
  assume a track carries audio, video, or timestamps.
- Object callbacks run as the stream layer delivers objects. The callback path adds no application queue, no
  ordering across streams, no automatic payload decoding, no clock synchronization, and no media
  scheduling.
- Each new subscription is independent; re-subscribing does not resume the preceding request.
  Relay caching and location filters can deliver the same object again across subscriptions,
  including after a publisher restart. Applications own retry backoff and any cross-subscription
  continuity policy; see [publisher restarts and resubscription](relay-interop.md#publisher-restarts-and-resubscription).
- The request timeout bounds only the subscription's initial response. An accepted subscription
  has no application inactivity deadline. Keeping an idle transport connected depends on peer
  traffic or transport-provided keepalive; the SDK does not schedule its own keepalive. The Python
  transport uses aioquic's default 60-second local idle timeout, subject to peer negotiation.
- `update()` sends REQUEST_UPDATE, which the draft allows only on an accepted subscription
  ([draft-20 Section 5.1](https://www.ietf.org/archive/id/draft-ietf-moq-transport-20.html#section-5.1)).
  An update issued before acceptance is queued in order and sent when SUBSCRIBE_OK arrives; at most
  eight wait, and a further one raises `UPDATE_QUEUE_FULL` without sending anything. A subscription
  that ends before acceptance discards its queue, and its ending is what the application observes.
  An update on an ended subscription raises `CLOSED`. Each update consumes its own Request ID, and
  the relay's acknowledgement arrives as `onUpdateOk` / `on_update_ok`; a refused update arrives as
  REQUEST_ERROR and ends the subscription ([issue #94](https://github.com/muxinc/relay-sdks/issues/94)).
  Implemented in both SDKs.
- A subscribe for a namespace the relay has not seen announced is rejected with request error 404
  (moq-relay's literal), not parked. The relay routes subscribes to a namespace only once it has
  processed the publisher's announce, about one round trip after the publisher's `announced` settles
  ([announce propagation](relay-interop.md#announce-propagation)), so a subscribe that races the
  announce is rejected the same way. The SDK reports that rejection like any other, through
  `subscribed` and `on_error`, and does not retry a subscribe; the application retries it within its
  own budget rather than treating it as fatal. The examples share one helper per language for this.
- Subgroup resets and connection closure are surfaced explicitly. There is no exactly-once or
  gap-free delivery promise.
- The publisher's done notification is a control message and may arrive before trailing objects
  still in transit. It does not end the subscription: the request stays open until the application
  cancels it or the connection ends, and the SDK does not resubscribe on its own.
  It is not a prompt publisher-death detector; the SDK reports it when the relay sends it.
- Replacing a publisher on the same namespace does not guarantee handoff of existing subscriptions.
  New subscriptions may also see transient 404s or initial-response timeouts after retract/re-announce.
  Applications own freshness checks and bounded recovery; see [publisher recovery](publisher-recovery.md).
- Subscriptions cancel independently of each other. The application owns connection shutdown.
- `subscribe()` returns a `RelaySubscription`: the request handle plus two awaitables. `subscribed`
  settles when the relay answers, and `ended` settles once when the subscription can no longer
  deliver. Neither implies exactly-once or gap-free delivery, and publisher completion settles
  neither. The table below is the shared contract for both.

### Subscription lifecycle

The application's subscription (see `CONTEXT.md`) is pending until the relay answers, accepted after
SUBSCRIBE_OK, and ended once it can no longer deliver. Every ending is recorded as a
`SubscriptionEnd` with a `kind` (`rejected`, `terminated`, `cancelled`), the protocol `cause`
(`request-error`, `connection-closed`, `cancelled`), whether the subscription had been `accepted`,
and the `error`. Implemented in both SDKs.

| State | Event | `subscribed` | `ended` | Callbacks |
| --- | --- | --- | --- | --- |
| pending | SUBSCRIBE_OK | resolves with the acceptance info | stays pending | `onSubscribed` |
| pending | REQUEST_ERROR: rejection, request timeout, or the request stream failing before an answer | rejects `SubscriptionRequestError`, preserving the request error | resolves `kind: rejected`, `cause: request-error` | `onError` |
| pending | connection ends | rejects `SubscriptionConnectionClosedError`, preserving the connection error as its cause when known | resolves `kind: terminated`, `cause: connection-closed`, `accepted: false` | nothing on the subscription; the connection's `closed` carries the reason |
| pending | `cancel(reason)` | rejects with `reason`, or a `CLOSED` Relay error when none was given | resolves `kind: cancelled`, with the reason as `error` | nothing |
| accepted | objects, subgroup end or reset, state notify, update acknowledged, GOAWAY on the request stream | settled | stays pending | the matching callback |
| accepted | PUBLISH_DONE | settled | stays pending: publisher completion is not an ending, trailing objects can still arrive | `onPublisherDone` |
| accepted | REQUEST_ERROR, which is how a refused `update()` is answered ([issue #94](https://github.com/muxinc/relay-sdks/issues/94)) | settled | resolves `kind: terminated`, `cause: request-error`, `accepted: true` | `onError` |
| accepted | connection ends | settled | resolves `kind: terminated`, `cause: connection-closed`, `accepted: true` | nothing on the subscription; the connection's `closed` carries the reason |
| accepted | `cancel(reason)` | settled | resolves `kind: cancelled` | nothing; trailing objects stop |
| ended | anything, including a second `cancel()` | settled | settled, unchanged | nothing; `update()` raises `CLOSED`; the events iterator has already raised or reached EOF |

The core fails a pending request before it reports the closure that killed it: in the same turn
when the session notices the closure, or turns earlier when the transport fails its streams before
the session learns of it. A pre-acceptance request failure is therefore never recorded before the
turn ends, and one that arrives after the transport has already ended is left to the closure, so
the closure is the ending the subscription describes, with the connection error preserved. A
genuine rejection reaches `subscribed`, `ended` and `onError` one turn after the core delivers it.
A REQUEST_ERROR after acceptance is recorded at once.

Python asks the transport whether it has already ended. A browser's WebTransport reports its end
only through the `closed` promise, and Chromium settles it a task or more after it aborts the
request stream of a connection lost while that stream opens
([issue #102](https://github.com/muxinc/relay-sdks/issues/102)). So TypeScript holds a
pre-acceptance failure the core synthesized from a stream event (FIN before a response, a stream
reset, a failed stream open; not the response timeout) until the transport's `closed` settles, and
records it as a rejection only if the connection is still up 250 ms later. A REQUEST_ERROR message
and the response timeout keep the one-turn hold.

A readiness awaitable nobody awaits cannot surface as an unhandled rejection: the SDK pre-handles
it. Awaiting it later still observes the same result.

## Async subscription consumption

The Relay product API provides `subscriber.subscribeEvents(options, { capacity, signal })` in TS and
`subscriber.subscribe_events(namespace, track, capacity=..., cancellation=...)` in Python. These create
and own a new subscription immediately. They yield typed data and lifecycle events, using the same
payload objects as callbacks. The generic `MoqTrackSource` and existing `subscribe` callbacks remain
available and retain their synchronous delivery model. The handle's `subscribed` and `ended`
awaitables (see the subscription lifecycle table above) are the readiness and termination signals;
the iterator's raise or EOF is its own. It follows the product vocabulary from #63; its explicit
`publisherDone` / `publisher_done` event distinguishes publisher completion from final delivery.

- `capacity` defaults to **64 events** and must be a positive integer (a safe integer in TS).
  Every queued object **and lifecycle event** consumes one slot. This is an event-count limit,
  not a byte budget: an object may contain up to 16 MiB of payload and 1 MiB of encoded properties,
  plus decoded metadata overhead. Choose a smaller capacity for large objects. Protocol decoding
  limits also bound individual control events. The queue owns at most `capacity` event references;
  objects retained by application code and transport buffers are outside this limit. An event
  handed directly to a waiting reader is delivered and no longer counts against queue capacity,
  even before that reader resumes. Later teardown cannot retract that handoff.
- Overflow fails immediately with `SubscriptionOverflowError`, discards the whole queue, cancels
  only the owned request and its active subgroup readers, and detaches observers. It does not
  silently drop objects and continue. Buffered data is also discarded on any other termination.
  These limits provide local buffering, **not transport-wide backpressure**.
- Events preserve callback arrival order. They do not sort groups or invent an order across streams.
  No exactly-once, gap-free, or fully drained delivery guarantee is implied.

| Event / condition | Iterator result |
| --- | --- |
| Subscription accepted | `subscribed`, with `SubscribeOk` |
| Object (including protocol object status) | `object`, with `MoqtObject` |
| Publisher control completion | `publisherDone` / `publisher_done`, with `PublishDone`, including status, stream count and reason; **iteration stays open for trailing objects** |
| Subgroup FIN | `subgroupEnd` / `subgroup_end`; only that subgroup ended |
| Subgroup reset | `subgroupReset` / `subgroup_reset`; a gap may have occurred; iteration continues |
| Publisher state change | `stateNotify` / `state_notify`, with changed parameters |
| Update acknowledged | `updateOk` / `update_ok` |
| Request GOAWAY | `goaway`, with relocation details; informational, no automatic retry |
| Request rejection, initial-response timeout, or initial request failure | Raises `SubscriptionRequestError`, preserving `requestError` / `request_error` |
| Connection ends at any point, including a clean local close | Raises `SubscriptionConnectionClosedError`, preserving the connection error as `cause` / `__cause__` if available; never a claim of drained EOF |
| Iterator `close()` / TS `return()` / Python `aclose()` | Discards buffered events, cancels the owned subscription, pending reads settle as clean EOF |
| Per-iterator abort signal / cancellation token | Discards buffered events and raises the supplied reason (or the platform's default cancellation reason) |

Before acceptance, a connection failure reaches the core's request-error callback before the
connection observer. The subscription holds that failure until the turn ends (see "Subscription
lifecycle"), so the iterator raises `SubscriptionConnectionClosedError` with the connection
exception preserved, the same as after acceptance, rather than a `SubscriptionRequestError`
carrying the core's reason string.

The iterator errors are `RelayError` subclasses with stable codes: `SubscriptionRequestError`
uses `SUBSCRIPTION_REQUEST_FAILED`, and `SubscriptionOverflowError` uses `SUBSCRIPTION_OVERFLOW`,
both in the `subscription` phase. `SubscriptionConnectionClosedError` uses `CLOSED` in the
`connection` phase and retains the underlying connection failure as its cause when present.
The callback API still receives unwrapped MOQT `RequestError` values.

A failure remains observable on subsequent reads. The first termination wins. Do not close the
shared connection merely to stop this consumer. Publisher completion never auto-cancels or
resubscribes; choose a timeout or explicit application policy if no more data is expected.

Only one pending iterator read is supported; a concurrent read fails without disrupting the first.
There is no background consumer task. TS `for await` calls `return()` on `break` or a
consumer-body exception. Use a `finally { events.close(); }` if cancelling while the body awaits
other work. A per-iterator `signal` can also interrupt a pending read. Python **requires `async
with events:`** (or `try/finally: events.close()`) around `async for` for deterministic cleanup on
early `break`, consumer exception, or task cancellation in the loop body: Python's loop alone does
not call `aclose()`. Cancelling a Python task blocked in `anext()` also closes the iterator.
Consequently, `asyncio.timeout()` or `asyncio.wait_for(anext(events), ...)` expiring cancels the
subscription; the caller receives `TimeoutError` and subsequent reads return EOF. Use those
deadlines to **end** consumption, not to skip an idle interval and resume the same subscription.
In TS, `Promise.race` does not cancel the losing read: use the iterator's abort signal to end
consumption, or retain and await that same pending read before requesting another. Consumer
exceptions propagate to the caller; the SDK never retries or swallows them. Stop using abandoned
iterators explicitly; garbage collection is not the ownership policy. Connection observation and
cancellation listeners are removed on teardown, and pending reads settle.

### Synchronous callback policy

Subscription callbacks must be synchronous in both APIs, including the generic core. A declared
async callback is rejected with `TypeError` during `subscribe`, before allocating a request ID.
For a synchronous function that returns an awaitable dynamically, Python emits `RuntimeWarning`,
closes a returned coroutine, or cancels a returned Future/Task and observes its eventual
exception. Other custom awaitables are diagnosed and discarded without running them. TS reports a
`TypeError` to `console.error` once per handler per subscription and observes every returned
promise's rejection; JS promises cannot be cancelled, so already-started work may continue.
Delivery never awaits these results, and such callbacks are unsupported. Ordinary synchronous
return values remain ignored, and ordinary synchronous callback exceptions retain the existing
callback error behavior. Use the iterator for awaited work. If starting independent work
deliberately, use a synchronous callback that returns nothing and arrange its task lifetime and
error handling explicitly.

Handlers are validated and snapshotted when `subscribe` is called; set them all before subscribing.
Later assignments to the original handler object do not change an existing subscription. Request
rejection and local/session teardown now cancel the request stream and active subgroup readers.
Teardown suppresses subsequent end/reset callbacks and protocol errors from those cancelled readers;
it does not report cancellation as a network gap. An actual reset while the subscription is active
still invokes the gap callback. Publisher completion leaves readers active for trailing objects.

## Publishing

- Publishing uses a dedicated connection with a token authorized to publish the namespace. One
  publisher connection serves one namespace and any number of tracks.
- Publishing follows the relay's announce-and-serve flow. Connecting a publisher announces its
  namespace. Registering a track makes it servable and sends nothing; the relay learns of the track
  when a subscriber asks for it. A write waits for the relay to subscribe upstream before sending
  bytes; a subscriber creates that demand.
  If namespace solicitations have arrived but none currently covers the publisher namespace when
  the demand deadline expires, the write reports a namespace coverage error instead of an ordinary
  demand timeout. Check the token and namespace before retrying. This does not prove authorization
  was denied: solicitations can arrive independently, and a later matching prefix permits writes.
- `publisher.announced` settles once the namespace has been handed to the transport on a
  solicitation that covers it, no earlier than the first registered track. It is the strongest fact
  the publisher has: the relay acknowledges an announce with nothing and routes subscribes about a
  round trip later. It stays pending while no solicitation covers the namespace (the first write
  reports `NAMESPACE_NOT_SOLICITED`), and the connection's closure terminates it if that comes
  first, with the closure's error or `CLOSED` after a clean shutdown.
- The default write deadline is 10 seconds and covers both waiting for demand and the transport
  write.
- A timeout, cancellation, transport write failure, or relay subscription change resets the
  affected write's stream and leaves the track open while the publisher connection remains open.
  A stream that opens after cancellation is reset without sending payload bytes. The next explicit
  write uses a new group ID; failed groups are never reused. After demand disappears, later writes
  wait for a new relay subscription. Applications choose fresh data or the next random-access point
  after a failure; already delivered bytes cannot be withdrawn. There is no automatic retry.
- A write's payload must be a byte sequence in the language's own terms, and its properties must
  follow the [Arguments](#arguments) table. Anything else is rejected before the write starts, which
  is a local argument error and leaves the track usable; an oversized property is such an error too,
  not a transport failure once demand arrives.
- Each write copies its bytes and sends one independent group containing one object. Only one
  write may be pending per track; a concurrent write is rejected rather than queued.
- Write completion means the transport accepted the complete group, not that any viewer received
  it.
- There is no publisher-side retention or replay. Late-subscriber behavior depends on relay caching
  and subscription filters.
- A failed write may have delivered partial data. There is no automatic retry.
- The publisher encodes supplied track and object properties as protocol metadata. The raw transport
  API adds no default timescale, timestamp, or LOC media packaging. Subscribers expose the properties received
  from the relay, which may differ from those sent by the publisher; end-to-end preservation of
  arbitrary properties is not guaranteed. See [Relay property handling](#relay-property-handling).
- Object payloads, encoded object properties and individual property values are limited to the
  sizes in `protocol.json`.
- Closing a track writer cancels an outstanding write and ends that track; other tracks continue.
  Closing the publisher ends all tracks, retracts the namespace, and waits for transport shutdown.
- Track names cannot be reused within a publisher connection, including after their writer closes.
- A track writer may open a multi-object group: objects are appended with object IDs 0, 1, 2, ...
  on one stream, and closing the group FINs the stream. Object 0 is the group's random-access point,
  which is how LOC/MSF media (one GOP per group) is published. A group that fails part-way is reset
  rather than completed, and the track stays open for the next group; only one write or group may
  be pending per track. The write deadline applies separately to opening a group (including demand),
  each object write, and closing the group. Idle time between operations is unrestricted. Aborting a
  group after it closed cleanly has no effect: the FIN already ended the stream, and a reset after
  complete delivery is ignorable
  ([draft-20 Section 11.4.3](https://www.ietf.org/archive/id/draft-ietf-moq-transport-20.html#section-11.4.3)).
  Implemented in both SDKs.
- A track writer exposes whether the relay currently demands the track and lets the application
  observe every change, so state-shaped tracks such as a catalog can be re-sent on each subscribe.
  Demand observers emit the current state first, include replacement subscriptions (a repeated
  "demanded" value), and finish when the track ends. A closed track reports no demand. Demand is
  the relay's single upstream subscription for the track: the relay shares it across every
  downstream viewer, so additional viewers produce no further notification, and it may keep the
  subscription for a period after the last viewer leaves. Demand is therefore neither a viewer
  count nor a liveness signal. Implemented in both SDKs.

### Write phases and interruptions

Every write and every open group is one operation occupying the track's write slot (see
`CONTEXT.md`). The operation records its phase before each step, and this table is the shared test
list both SDKs implement: what the caller sees, what reaches the wire, and what state the track is
left in. The Source column separates what the protocol rules from SDK policy. Section numbers refer
to [draft-ietf-moq-transport-20](https://www.ietf.org/archive/id/draft-ietf-moq-transport-20.html)
unless stated otherwise.

| Phase | Interruption | Reported to the caller | Wire effect | Track afterwards | Source |
| --- | --- | --- | --- | --- | --- |
| waiting-for-demand | write deadline expires | `WRITE_TIMEOUT`, or `NAMESPACE_NOT_SOLICITED` when solicitations arrived but none covers the namespace; phase `waiting-for-demand` | nothing sent | open; the next write waits for demand again | SDK policy: the protocol has no timer for awaiting a subscription (§13.6 only warns of idle timeout) |
| waiting-for-demand | relay subscription ends while waiting | the wait continues until the deadline | nothing sent | open | SDK policy |
| waiting-for-demand | caller cancels | the caller's reason, unwrapped; phase `waiting-for-demand` | nothing sent | open | SDK policy |
| waiting-for-demand | track closed | `CLOSED`, phase `waiting-for-demand` | nothing sent | ended | SDK policy |
| waiting-for-demand | connection ends | the connection's own error (phase `connection`), or `CLOSED` after a clean shutdown | nothing sent | ended | §3.5: streams and subscriptions end with the session |
| opening-stream | write deadline expires or caller cancels | `WRITE_TIMEOUT` or the caller's reason; phase `opening-stream` | a stream that opens after the failure is reset before any object is written, code `DELIVERY_TIMEOUT` (0x2) or `CANCELLED` (0x1); its header may precede the reset (RESET_STREAM_AT) | open; the next write uses a new group ID | §11.4.3: a stream closed before all objects are delivered MUST be reset, and the reset SHOULD include the header; the deadline itself is SDK policy |
| writing, closing | write deadline expires | `WRITE_TIMEOUT`, phase `writing` or `closing`; object bytes may have been delivered, and `closing` can fail after all of them were | stream reset, code `DELIVERY_TIMEOUT` (0x2) | open; the next write uses a new group ID | §11.4.3 MUST reset; §8 and §15.11.4 for the code; the deadline is SDK policy |
| opening-stream, writing, closing | caller cancels (AbortSignal, task cancellation, or `group.abort()`) | the caller's reason unwrapped, or `CLOSED` when `abort()` gave none; the current phase | stream reset, code `CANCELLED` (0x1) | open | §11.4.3 MUST reset; code choice is SDK policy (§3.3.4 SHOULD be relevant) |
| opening-stream, writing, closing | the transport refuses the stream or fails a write | `TRANSPORT_ERROR` with the cause preserved; the current phase | stream reset, code `INTERNAL_ERROR` (0x0), when a stream exists | open | SDK policy |
| opening-stream, writing, closing | relay subscription replaced or ended | `SUBSCRIPTION_CHANGED`; the current phase | stream reset, code `CANCELLED` (0x1) | open; the next write waits for the new demand | §5.1.1: the publisher MUST reset open streams of a cancelled subscription; treating a new SUBSCRIBE as a replacement rather than a concurrent subscription is SDK policy (§5.1, ADR-0001) |
| opening-stream, writing, closing | track closed | `CLOSED`; the current phase | stream reset, code `CANCELLED` (0x1) | ended | §10.12: every stream is closed or reset before the track ends |
| opening-stream, writing, closing | connection ends | the connection's own error (phase `connection`), or `CLOSED` | streams end with the session; a reset that is still sent carries `SESSION_CLOSED` (0x3), or `GOING_AWAY` (0x4) after GOAWAY | ended | §3.5; §15.11.4 for the codes |
| any | a second write or group is started | `WRITE_PENDING`, phase `writing`; the active operation is unaffected | nothing sent | unchanged | SDK policy: one pending operation per track |
| after a clean group close | `abort()` | nothing; `closed` stays true and a repeated `close()` returns | nothing sent | open | §11.4.3: a reset after all data and the FIN is ignorable |

A failed group's ID is never reused, which is stricter than §2.2 (a subgroup may resume on a new
stream after a premature reset); group IDs increase monotonically and object IDs start at 0 within a
group (draft-ietf-moq-msf-01 §6). A group may end without an `END_OF_GROUP` marker (§2.4.2). There
is no automatic retry, and the application chooses the next random-access point after a failure;
MSF and LOC say nothing about an abandoned group. Implemented in both SDKs.

### Relay property handling

These are deployment observations, separate from the SDK guarantees above. Tests against
`sjc.relay.mux.global` running relay 0.14.17 on 2026-09-16 reproduced the following:

- A track without declared Timescale (`0x08`) arrived with Timescale = 1,000,000 and
  Default Publisher Group Order (`0x22`, decimal 34) = 2 (descending). The relay stamped its own
  timestamps on objects, including objects with a publisher-supplied Timestamp (`0x10`).
- With track Timescale = 1,000,000 declared, supplied Timestamp values were preserved. Objects
  without a Timestamp still received a relay-generated one. The relay still included Default
  Publisher Group Order in the track properties.
- Of the object properties tested, Timestamp was the only type received. Object-level Timescale
  (`0x08`), Video Config (`0x0D`), and the tested values of types `0x02` and `0x05` were dropped.
  This is not an exhaustive registry compatibility test or a promise about future relay versions.
  Object payloads and IDs in these tests were preserved.

For publisher-defined media timing, declare a track timescale and put timestamps in that unit on
objects. Interpret received timestamps using the received track timescale; a timestamp's presence
alone does not establish that the publisher supplied it. Do not treat a relay-generated timestamp
as the application's event time or a synchronized wall clock. Put application metadata that must
arrive intact in the payload (or a separate metadata track's payload). For the Video.js media
example, codec configuration is also carried in the catalog's `initDataList`, so decoding does not
depend on Video Config object-property forwarding.

Evidence and protocol naming: [issue #23](https://github.com/muxinc/relay-sdks/issues/23) and the
relay release's
[track property implementation](https://github.com/moq-dev/moq/blob/b2f26b920f617c033ef7a393bcc894f0bb9b350b/rs/moq-net/src/ietf/properties.rs).

## Text and JSON helpers

- Both product SDKs provide opt-in text and JSON writes on track and group writers and standalone
  readers for received objects. The protocol core remains payload-agnostic.
- Each helper encodes or decodes one complete UTF-8 object. Text readers preserve a leading U+FEFF
  and reject malformed UTF-8; JSON readers additionally reject invalid JSON, including non-standard
  `NaN` and `Infinity` tokens. Empty text is valid, but an empty payload is not JSON.
- Writes delegate to the byte writer, retaining its options, metadata, grouping, demand,
  concurrency, cancellation, deadlines, and size limit (measured in encoded bytes). Serialization
  failures occur before starting a write and leave the writer usable.
- JSON uses the language's standard serializer and parser, with no schema validation or automatic
  content-type properties. Unsupported values fail locally; JavaScript also rejects top-level
  values for which `JSON.stringify` returns undefined. Runtime-specific number, Unicode, and
  serialization conversion rules are documented in each SDK's README; this is not canonical JSON.
- Readers do not mutate payloads or metadata. Decode failures propagate to the caller; applications
  catch them inside object callbacks when they want to skip malformed objects.

## Media helpers

The optional `@mux/moq-media` / `moq-media` packages implement pure format helpers. Raw Relay
writers keep their existing byte/group behavior; no automatic media packaging is added to them.
The media pins in `protocol.json` and exact bytes in `vectors/media.json` apply to both languages.

- LOC track properties declare a timescale (default 1,000,000). Object properties carry Timestamp
  (0x10), Timescale (0x08), and explicitly supplied Video Config (0x0D) / Audio Config (0x0F).
  Callers supply configs only on keyframes. Timestamps arrive in microseconds; other timescales
  round to the nearest integer tick, ties upward. Inputs and the resulting tick count must be
  nonnegative safe integers (at most 2^53−1); the timescale must be positive.
- The independent MSF catalog is compact UTF-8 JSON with `version: "draft-01"`, `isComplete: true`,
  and explicit per-track namespace, LOC packaging, and live status. Absent optional fields are
  omitted, as is empty init data. Catalog strings must be valid Unicode, with no lone surrogates.
  Entries are ordered video, audio, then data in caller order. Init-data entries have
  `<track-name>-init` IDs and inline base64 data. Numbers must be finite; integral values must be safe integers and have no decimal suffix.
  Fractional values use ECMAScript JSON number spelling, including its exponent threshold.
- `avc1` and `avc3` video entries are withheld without non-empty init data. With avcC, the codec
  suffix comes from the record's profile/constraints/level; the `avc1`/`avc3` prefix is retained.
  Header inspection rejects truncated/wrong-version headers; it does not decode SPS semantics.
- Opus always declares 48,000 Hz and defaults to channel configuration `"2"`. Data entries have
  only name and optional role beyond the common fields, and reject the media roles `video`,
  `signlanguage`, `audio`, `audiodescription`, `caption`, and `subtitle`. Track names must be
  non-empty and unique, including tracks temporarily withheld pending init data.
- Namespace inputs are sequences of fields, never bare strings. Catalog namespace fields must be
  non-empty and cannot contain `/`: the supported Video.js
  catalog dialect joins/splits namespace fields using that separator. Source URL components
  support arbitrary valid UTF-8 and escape every byte outside `[A-Za-z0-9_]` as lowercase `.xx`.
  Source composition accepts explicit `https://` or `moqt://` URLs without a fragment or credentials,
  preserves authority/path/query, supplies `/` for an absent path, defaults to the `catalog` track,
  and percent-escapes an optional `c4m` token using RFC 3986 unreserved characters.
- Python H.264 helpers accept three- or four-byte Annex B start codes, omit Annex B padding zeros,
  extract SPS/PPS, construct a four-byte-length avcC, and convert access units to four-byte NAL
  lengths. SPS/PPS/AUD are dropped by default; retaining them is an explicit option. avcC creation
  rejects missing/wrong-kind parameter sets, more than 31 SPS or 255 PPS, and NALs over 65,535 bytes.
  Construction supports Baseline, Main, and Extended profiles (66, 77, 88); other profiles are
  rejected until their SPS-derived chroma/bit-depth extension fields are implemented. Encoder-generated
  avcC records of other profiles can still be passed to the catalog builder. The extension requirement
  is reflected in [FFmpeg's avcC writer](https://patches.ffmpeg.org/doxygen/4.4/avc_8c_source.html).
- TypeScript consumes encoder/demuxer-provided avcC and length-prefixed packets, and exposes only
  `avcCodecString` for header inspection. It does not expose Annex B parsing or construction APIs.
  `@mux/moq-media` maps standard WebCodecs decoder configs for four-byte-length AVC and Opus,
  copying the exact description buffer/view range. `chunkToLocFrame` accepts native encoded video
  or audio chunks, copies their payload bytes unchanged, preserves microsecond presentation
  timestamps, and attaches supplied configs only to key chunks. Callers preserve encoder output
  order. These helpers require no browser globals at import time.
  The optional `@mux/moq-mediabunny` file-input adapter borrows public packet payloads and rejects
  metadata-only packets. Both chunk and packet helpers accept a shared integer timestamp offset
  to normalize negative timestamps; input timestamps must be safe integers and the final timestamp
  follows the LOC nonnegative safe-integer contract. The adapter retains compatibility re-exports
  of the config mappers and `LocFrame` type from the media package.
- Encoding, pacing, and time-based frame dropping remain application responsibilities. Catalog
  serving, GOP grouping and recovery are the [Media publishing](#media-publishing) policy below,
  implemented in Python; a `register_media` publishing handle and public readers are not
  implemented. The TypeScript package keeps an internal independent Video.js catalog parser
  extraction for builder compatibility tests.

## Media publishing

The media publishing policy lives in the product package (`mux_relay.media`) over registered
tracks: one tier above the write slot, one below the proposed `register_media` handle. It knows no
format. A frame carries its own object properties and a catalog is bytes. Implemented in Python;
not yet implemented in TypeScript.

- A catalog is a state-shaped track. The catalog service sends its document on the current relay
  demand and again on every later relay subscription, including a replacement subscription (the
  repeated "demanded" value), so a viewer joining after a relay reconnect receives it. It does not
  publish on change alone, and a replaceable document is not implemented. Not adopted: MSF's
  publish-on-change with a Joining FETCH from the relay cache. A plain SUBSCRIBE against the tested
  relay is live-edge only ([#26](https://github.com/muxinc/relay-sdks/issues/26)), re-sending is
  what the Video.js player relies on, and MSF-01 §6's one-object-per-stream sentence is treated as
  a draft defect because it contradicts LOC-04 §4.2 and MSF-01 §5.
- A catalog write that fails while the track stays open is retried after a configurable delay
  (default one second) for as long as demand remains, keeping only the current document; losing
  demand ends the retries until the next relay subscription. The service runs until cancelled or
  until the track ends, and raises the failure that ended the track when a write exposed it.
- A frame writer maps encoded frames onto one track's groups. Under keyframe grouping, the LOC
  mapping for video, a keyframe FINs the open group and opens a new one, and deltas append to it.
  Under frame grouping, for audio and any other independently decodable frames, every frame is its
  own single-object group. Object properties are sent as the frame supplies them.
- A frame the relay has no subscription for is skipped at once; the writer never waits on the write
  deadline, because an encoder loop cannot block. When demand returns mid-GOP, deltas are skipped
  until the next keyframe; the open group is not replayed.
- Lateness is the application's. An optional `stale` predicate is consulted before a group is
  opened and again before each write, since time passes inside those steps. A stale frame is
  dropped, an open group that depended on it is reset, and deltas are dropped until the next
  keyframe. A frame flagged as following a discontinuity resets the open group the same way.
- A failure is one of two kinds. It ends the writer (a retirement, see `CONTEXT.md`) exactly when
  the error code is `CLOSED` or the track reports `closed` afterwards, and likewise when the track
  is already closed as a frame arrives; that frame and every later one are reported as ended, with
  the error, and nothing further touches the track. Any other failure (an interruption) resets the
  group, leaves the track open, and reports the frame as interrupted with its error; the next
  keyframe, or the next independent frame, opens a new group. The FIN of the previous group that a
  keyframe triggers is part of that keyframe's write: a retirement during it ends the writer, while
  an interruption during it is absorbed, because the previous group's delivered objects stand and
  the new group opens regardless. Demand alone never decides: no demand is also what a track nobody
  subscribes to looks like.
- Every frame gets exactly one outcome: sent, skipped, stale, interrupted or ended. The writer
  returns it rather than raising, so a caller keeps counters without catching per frame, and the
  group the frame was written to or that was reset on its behalf travels with it. Closing the writer
  FINs the open group and refuses further frames with `CLOSED`; the registered track stays the
  application's to close.

## Package boundaries

- The optional TypeScript Mediabunny adapter depends on the media format package and declares
  Mediabunny as a peer. Neither the core, format package, nor product SDK depends on the adapter.
- The generic media format package depends only on the core, imports no product or transport
  library, and owns no encoder. The core imports nothing from the media package.

- Each language has a product package that owns the Relay API and the authentication convention,
  and a generic MOQT core package that is protocol-only.
- The core has no runtime dependencies, no knowledge of Relay, and no player, UI, or media engine
  imports. It works over an abstract transport that the product package supplies.
- Each language enforces this mechanically in its check task.

## Tooling conventions

Live tests and examples in every language read the same environment:

| Variable | Meaning |
| --- | --- |
| `RELAY_TOKEN` | Relay JWT. When absent, tooling prompts with input hidden. Never written to disk or printed. |
| `RELAY_URL` | Overrides the default endpoint in `protocol.json`. Must be https and must not carry the token. |
| `RELAY_NAMESPACE_PREFIX` | Namespace prefix the token authorizes. Live tests append a random field so runs never share relay state. |

## Live smoke test

Each SDK ships an opt-in test against a real relay, excluded from the default check. It publishes
and subscribes on the same random namespace, then asserts:

1. A publisher connection and a subscriber connection both open with the supplied token.
2. Once the publisher's `announced` settles, subscriptions to two tracks on the namespace are
   accepted, retrying a 404 rejection while the announce propagates; the report records the retries.
3. Binary payloads, an empty payload, and a text payload all arrive intact.
4. Closing one track writer does not affect delivery on another track.
5. A late subscriber decodes `LARGEST_OBJECT` from a populated track's `SUBSCRIBE_OK` and receives the next live object.
6. All connections close cleanly once the test closes them.

Adding an assertion here means adding it to every SDK's smoke test.
See [relay interoperability](relay-interop.md) for a local moq-relay 0.14.17 test.

## Not yet specified

Automatic reconnection, token refresh on a live session, datagram reception, product resource
discovery, and player adapters are not implemented in any SDK and have no contract yet. Add them
here when the first SDK gains them.

## Draft-20 protocol boundaries

- `MAX_REQUEST_ID` and `REQUESTS_BLOCKED` were removed after draft-16
  ([draft-20 Appendix A.4](https://www.ietf.org/archive/id/draft-ietf-moq-transport-20.html#appendix-A.4)).
  They are not draft-20 request-budget messages. Unknown control messages terminate the session,
  as required by [Section 10](https://www.ietf.org/archive/id/draft-ietf-moq-transport-20.html#section-10).
- An unrecognized wire Object Status terminates the session with `PROTOCOL_VIOLATION`, following
  [Section 11.2.1.1](https://www.ietf.org/archive/id/draft-ietf-moq-transport-20.html#section-11.2.1.1).
  The `UNKNOWN_OBJECT_STATUS` stream-reset code has a different meaning: a FETCH publisher cannot
  determine whether an object exists ([Section 3.3.4](https://www.ietf.org/archive/id/draft-ietf-moq-transport-20.html#section-3.3.4)).
  Ordinary subgroup transport resets remain per-stream delivery events.
