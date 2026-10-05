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
  them; see `ts/GUIDE.md`, "Browser token exposure".
- Connecting has one deadline that covers token resolution and both handshakes, transport and
  MOQT setup.
- The browser transport may end a silent connection after about 5 s while data is outstanding.
  The SDK reports it as `TRANSPORT_ERROR` in phase `connection` and cannot tell it apart from other
  losses. See [browser transport limits](../ts/GUIDE.md#browser-transport-limits).
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
| `DELIVERY_TIMEOUT` | An object outlived its [delivery timeout](#delivery-timeouts) before it reached the transport. The data is stale: do not retry it as it is; choose fresh data or the next random-access point. |
| `CLOSED` | The handle is closed; use an open connection, track, or group. |
| `WRITE_PENDING` | Await the outstanding operation or finish/abort the open group before starting another. |
| `NAMESPACE_NOT_SOLICITED` | Received namespace solicitations do not cover this publisher; check the namespace and token configuration. A later matching solicitation can restore demand. |
| `NAMESPACE_ENDED` | The relay ended the namespace announcement; open a replacement publisher if appropriate. |
| `SUBSCRIPTION_REQUEST_FAILED` | An async subscription request failed; inspect its preserved MOQT request error and the connection state. |
| `SUBSCRIPTION_OVERFLOW` | The event buffer filled; the subscription was cancelled. Choose a suitable capacity or faster consumption before subscribing again. |
| `UPDATE_QUEUE_FULL` | More than eight updates were issued before the subscription was accepted; await acceptance (`onSubscribed` / `on_subscribed`) before updating again. Nothing was sent for the refused update. |
| `SUBSCRIPTION_CHANGED` | Upstream demand ended or changed during an operation; choose fresh data or a new random-access point. It can be group-level: the relay may stop one group's stream while its subscription continues, so check `hasDemand` / `has_demand` rather than assume demand ended. |
| `GOAWAY` | The relay requested migration; open a replacement connection. |
| `TRANSPORT_ERROR` | Inspect the original cause and connection state before deciding whether to reconnect or publish again. In TypeScript, a `connecting`-phase `TRANSPORT_ERROR` usually means the browser abandoned the handshake, so a retry policy should treat it like `CONNECTION_TIMEOUT` (see [browser transport limits](../ts/GUIDE.md#browser-transport-limits)). |
| `TRANSPORT_UNAVAILABLE` | Supply a transport implementation in a runtime without browser WebTransport; in Node, import from `@mux/relay/node` with `@mux/relay-quic` installed (TypeScript). |
| `UNAUTHORIZED` | The relay refused the connection's credentials: no token, a bad, malformed or expired one, or an endpoint path the token does not grant. Supply a valid token before connecting again. TypeScript reports it in browsers and over native QUIC (`@mux/relay/node`); against relays before moq-relay 0.17.0 a browser does not see the refusal, so there it arrives as `TRANSPORT_ERROR` (#14). |

`connecting` covers token resolution and connection setup. `connection` marks a connection-wide
failure; it makes no claim about individual writes that were in flight. `subscription` marks an
async subscription request failure or local event-buffer overflow. Publishing records
`waiting-for-demand`, `opening-stream`, `writing`, or `closing` before entering each step.
A `WRITE_TIMEOUT` or `DELIVERY_TIMEOUT` in `waiting-for-demand` means this attempt sent no object bytes. For timeouts or transport/subscription failures in a transport
phase, conservatively allow for partial delivery; `closing` can fail after all object bytes were
sent. Opening a group can start sending its header before the first object write. A phase records
the failure's origin, and a later call on the failed handle can report that same error.

For `WRITE_PENDING` and calls rejected because a handle is already closed, phase describes the
refused operation; that call sends nothing. A write or group refused with `CLOSED` because its
track or group is already closed reports phase `waiting-for-demand`, the phase that means nothing
was sent. `CLOSED` can also interrupt an operation already in flight; it then carries that
operation's phase, so the code alone does not rule out partial delivery.

No code or phase promises safe replay. Account for previous attempts, data freshness, and your
application's deduplication policy. Successful writes mean transport acceptance, not viewer
acknowledgment. The SDK does not automatically retry or reconnect. `UNAUTHORIZED` is the one code that
says the relay refused the credentials: the relay closed the session with MOQT session termination
code `UNAUTHORIZED` (0x2; `MALFORMED_AUTH_TOKEN` 0x16 and `EXPIRED_AUTH_TOKEN` 0x18 map to it too).
TypeScript maps it only in phase `connecting`, before the relay's SETUP, since draft-20 §3.5 defines
it as refusing to establish a session; Python maps it in any phase (#227). From moq-relay 0.17.0 a
browser sees the relay's close (CLOSE_WEBTRANSPORT_SESSION with code 2 and reason `unauthorized`), so
TypeScript reports it in browsers as well as over native QUIC; against older relays the browser lost
the close, and a refusal arrived as `TRANSPORT_ERROR` (#14). Neither another connection failure nor
an uncovered namespace proves authentication was denied. A subscription outside the token's grant
is refused per request, through `onError` / `on_error` (MOQT `UNAUTHORIZED`, 0x1), and leaves the connection open.

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
portable cases live in `vectors/arguments.json` and run in every SDK's suite. The table also lists
the local checks that stay where they are consumed (deadlines, track registration, object status,
event capacity); they follow the same class rule, and only track registration is checked after
connection state, because it depends on the connection's registry.

| Argument | Rule | `TypeError` in both languages | `RangeError` (TypeScript) / `ValueError` (Python) | Checked at |
| --- | --- | --- | --- | --- |
| Namespace | 1 to 32 non-empty string fields in an array (TypeScript) or a list or tuple (Python); sent verbatim, never split or joined on `/`. The 32-field limit is `limits.maxNamespaceFields` in `protocol.json`. | any other container, including a single string; no fields; a field that is not a non-empty string | more than 32 fields | `connectRelayPublisher` / `connect_relay_publisher`; `subscribe`, `subscribeEvents` / `subscribe_events`, `trackSource.subscribe` / `track_source.subscribe` |
| Track name | A non-empty string, in both roles. | anything else | | `registerTrack` / `register_track`; the three subscriber entry points |
| Properties | A list of `{type, value}` pairs. `type` is a non-negative integer; an even type carries a non-negative integer, an odd type carries bytes, which are copied. Integers are bounded by what the language's core varint carries: 2^53-1 in TypeScript, 2^64-1 in Python. Each byte value is at most `limits.maxPropertyValueBytes` (its 16-bit wire length). A track's properties encode to at most `limits.maxTrackPropertiesBytes` (65,525 bytes, the room SUBSCRIBE_OK leaves them), hold at most `relay.limits.maxTrackProperties` (64) pairs, and keep Default Publisher Priority (`0x0E`) at most 255 and Default Publisher Group Order (`0x22`) at 1 or 2 (`trackProperties`, draft-20 §12.4 and §12.5), because otherwise the track registers but every subscribe to it fails while the publisher's writes time out. An object's properties on `write` encode to at most `relay.limits.maxObjectPropertiesBytes` (64 KiB), because the relay drops a larger object without an error to either side. The SDK measures the encoding with the core's own encoder before any slot or stream is taken. | not a list; a pair without the shape; a type or integer value that is not a non-negative integer the varint carries; bytes where an integer belongs or an integer where bytes belong | a byte value longer than its wire length; a list whose encoding exceeds its bound; track properties with more than 64 pairs, or a `0x0E` or `0x22` value draft-20 rules out | `registerTrack` / `register_track`; `write` on a track or group writer |
| Endpoint | A string holding an absolute `https` URL with a host and no fragment or userinfo. A `jwt` query parameter, when present, is not empty; this holds for every `jwt` parameter when there are several. | not a string; a string that does not parse as an absolute URL, including an `https` URL with no host | another scheme; a fragment or userinfo; an empty `jwt` parameter | `connectRelaySubscriber` / `connect_relay_subscriber`, `connectRelayPublisher` / `connect_relay_publisher` |
| Deadline | A number in (0, 2147483647] milliseconds in TypeScript, (0, 2147483.647] seconds in Python: the largest delay a JavaScript timer holds, and the same bound in both SDKs. The connection, request and write deadlines share one check. | not a number, including a boolean | zero, negative, NaN, infinite, or above the bound | `connectTimeoutMs` / `connect_timeout` on both connect calls; `requestTimeoutMs` / `request_timeout`; `writeTimeoutMs` / `write_timeout` on `registerTrack` / `register_track` |
| Track registration | A track name not yet registered on this publisher connection, including after its writer closed. Checked after the track name, properties and deadline, and after connection state. | | a name already registered | `registerTrack` / `register_track` |
| Object status | `normal`, `end-of-group` or `end-of-track`; defaults to `normal`. | not a string | any other string | `write`, `writeText`, `writeJSON` on a group writer |
| Event capacity | A positive integer (a safe integer in TypeScript); defaults to 64. | not a number, including a boolean | zero, negative, fractional, NaN, or infinite | `subscribeEvents` / `subscribe_events` |
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
- A subscription without a location filter, whether it sends no LOCATION_FILTER or an explicit
  zero-field one, carries the draft's meaning: it passes all Objects published or received via
  upstream subscriptions
  ([draft-20 Section 5.1.2](https://www.ietf.org/archive/id/draft-ietf-moq-transport-20.html#section-5.1.2)).
  Whether that reaches back into what a relay cached before the subscription is the relay's
  choice. The Mux relay (moq-relay 0.17.0) starts it at the beginning of its latest group, and
  starts a Next Object filter after its largest object, skipping the rest of that group
  ([late subscribers](relay-interop.md#late-subscribers-and-location-filters)). A subscriber that
  needs earlier groups asks for them with a relative-group or absolute filter; an absolute start is
  served from the requested group while the relay still caches it. The SDK sends the
  filter it is given and adds none.
- The request timeout bounds only the subscription's initial response. An accepted subscription
  has no application inactivity deadline. The SDK schedules no application-level keepalive: keeping
  the transport connected is left to peer traffic and the transport's own keepalive, which
  [draft-20 Section 13.6.1](https://www.ietf.org/archive/id/draft-ietf-moq-transport-20.html#section-13.6.1)
  names as one way to keep an idle session open. In TypeScript that is Chromium's own QUIC PING. In
  Python the aioquic adapter sends a QUIC PING once it has sent nothing for 5 seconds, which also
  lets a receive-only connection survive a NAT rebinding: the relay learns the new address from
  that packet. The Python transport uses aioquic's default 60-second local idle timeout, subject to
  peer negotiation.
- `update()` sends REQUEST_UPDATE, which the draft allows only on an accepted subscription
  ([draft-20 Section 5.1](https://www.ietf.org/archive/id/draft-ietf-moq-transport-20.html#section-5.1)).
  An update issued before acceptance is queued in order and sent when SUBSCRIBE_OK arrives; at most
  eight wait, and a further one raises `UPDATE_QUEUE_FULL` without sending anything. A subscription
  that ends before acceptance discards its queue, and its ending is what the application observes.
  An update on an ended subscription raises `CLOSED`. Each update consumes its own Request ID. A
  relay that acknowledges an update with REQUEST_OK reaches `onUpdateOk` / `on_update_ok`; one that
  refuses it with REQUEST_ERROR ends the subscription
  ([issue #94](https://github.com/muxinc/relay-sdks/issues/94)).
  Implemented in both SDKs. moq-relay, however, never answers a REQUEST_UPDATE: any byte on the
  request stream after SUBSCRIBE_OK ends the subscription with PUBLISH_DONE `TRACK_ENDED`, so
  against it an update ends delivery ([relay interop deviation 4](#relay-interop-deviations)).
- A subscribe for a namespace the relay has not seen announced is rejected, not parked: moq-relay
  0.15.8 and 0.17.0 answer request error 16 (0x10, `DOES_NOT_EXIST`) with reason `unroutable`, and
  moq-relay through 0.15.1 answered its literal 404. The same code 16 with reason `not found` means the
  namespace is announced but the track is not, which is not the race. The relay routes subscribes to
  a namespace only once it has processed the publisher's announce, about one round trip after the
  publisher's `announced` settles ([announce propagation](relay-interop.md#announce-propagation)),
  so a subscribe that races the announce is rejected the same way. The SDK reports that rejection
  like any other, through `subscribed` and `on_error`, and does not retry a subscribe; the
  application retries it within its own budget rather than treating it as fatal. The examples share
  one helper per language for this; it retries 16 `unroutable` and the older 404 only, never
  16 `not found` ([issue #136](https://github.com/muxinc/relay-sdks/issues/136)).
- The SDK surfaces every subgroup reset it receives, and connection closure, explicitly. It surfaces
  a reset when the relay forwards it, which may be well after the publisher's abort: moq-relay 0.15.8
  forwarded each one only when the publisher's next group began, about 0.5 s later, or 3 s after a
  3 s publisher pause. It cannot surface a reset the relay does not forward: a relay may drop a
  subgroup that was reset upstream before it began forwarding it (seen before 0.15.8, unverified on
  it), and then the subscriber sees neither objects nor a reset for that group, only a gap in group
  IDs that cannot be told apart from a group never published
  ([subgroup resets](relay-interop.md#subgroup-resets)). There is no exactly-once or gap-free
  delivery promise.
- The publisher's done notification is a control message and may arrive before trailing objects
  still in transit. It does not end the subscription: the request stays open until the application
  cancels it or the connection ends, and the SDK does not resubscribe on its own.
  It is not a prompt publisher-death detector; the SDK reports it when the relay sends it. On
  moq-relay 0.17.0 a publisher's clean close, or its connection closing, reaches existing
  subscriptions within 0.1 s, as status 2 after `close()` and as 0 when the connection drops without
  MoQ cleanup; a crashed publisher's arrives as status 0 only after the relay's QUIC idle timeout,
  about 11 s (it was about 30-35 s on 0.15.8, whose idle timeout was 30 s).
- Replacing a publisher on the same namespace does not hand off existing subscriptions: moq-relay
  0.15.8 and 0.17.0 never move them to the replacement. After a clean end, new subscriptions fail
  with request error 16 `unroutable` until the replacement announces, and are served by it at once
  after that. After a crash, the relay still holds the dead publisher's announcement until its QUIC
  idle timeout (about 11 s on 0.17.0), so new subscriptions are accepted and served only the old
  publisher's cached last object; on 0.17.0 that can continue after the replacement announces, until
  the timeout, because the relay does not prefer the newer announcement
  ([duplicate announce](relay-interop.md#duplicate-announce)).
  Re-subscribing to a track that finished while its namespace stays announced replays the cached
  last group and another done, one round trip later each time, because the relay keeps a finished
  track cached by design; once nobody has read it for about 30 s, moq-relay 0.17.0 forgets it and a
  subscribe gets request error 16 `not found`. Do not re-subscribe to a track the publisher ended.
  A done that followed the subscription's own `update()` is different: the track is live, and
  re-subscribing is how to apply the change against moq-relay
  ([publisher restarts](relay-interop.md#publisher-restarts-and-resubscription)).
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
| accepted | objects, subgroup end or reset, state notify, update acknowledged (moq-relay never acknowledges one, see [deviation 4](#relay-interop-deviations)), GOAWAY on the request stream | settled | stays pending | the matching callback |
| accepted | PUBLISH_DONE | settled | stays pending: publisher completion is not an ending, trailing objects can still arrive | `onPublisherDone` |
| accepted | REQUEST_ERROR, which is how a relay refuses an `update()` ([issue #94](https://github.com/muxinc/relay-sdks/issues/94)); moq-relay sends none and ends the subscription with PUBLISH_DONE instead ([deviation 4](#relay-interop-deviations)) | settled | resolves `kind: terminated`, `cause: request-error`, `accepted: true` | `onError` |
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
It is a state error, not an argument error: a plain `Error` in TypeScript and a `RuntimeError` in
Python, and not a `RelayError`.
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
- Two publishers may announce the same namespace, and nothing warns either one or any subscriber.
  On moq-relay 0.17.0 the relay picks one of the two announcing connections for new subscribers,
  effectively at random (a hash of the namespace and each connection's random hop id ranks the
  routes before their age does); the choice is stable while both stay connected, and the other
  publisher gets no demand at all. Existing viewers stay on the publisher that served them, and
  when it retracts they get done status 2 (or, if it crashed, done status 0 about 11 s later) and
  do not move; new subscribers then go to the remaining one. After a crash, new subscribers may be
  routed to the dead publisher and served its cached last object until the relay's idle timeout.
  moq-relay 0.15.8 instead gave every new subscriber to the newest announcer. Neither SDK can detect
  which publisher serves a viewer
  ([issue #37](https://github.com/muxinc/relay-sdks/issues/37)); keep one publisher per namespace.
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
  it. The transport accepted the group once its FIN was handed to the transport, not once the relay
  acknowledged it, so a track is not limited to one group per round trip. Until that
  acknowledgment the group is a settling group (see `CONTEXT.md`); closing a track does not wait
  for its settling groups. A settling group is never reset when the relay subscription changes or
  ends; only its own [delivery timeout](#delivery-timeouts) resets it. One that fails later
  (STOP_SENDING, a reset, its delivery timeout, the session ending) is dropped without an
  error on the track, and frees its place under the limit below: the cause surfaces as demand, a new relay subscription, or the connection's
  closure. While a publisher connection has 64 settling groups, the next write or group waits in
  `opening-stream` under its write deadline before opening its stream, so a stall is a
  `WRITE_TIMEOUT`, not a transport failure; open groups do not count toward that limit. This also
  bounds how much a Python publisher accepts into a dead uplink, since its writes complete once
  the transport has sent the FIN. `publisher.close()` waits for the connection's settling groups for
  up to the largest write deadline among its tracks before it ends the session; in Python the same
  wait also covers each ended track's PUBLISH_DONE (see deviation 2). Delivery timeouts end some
  settling groups sooner and leave this bound unchanged. Groups still unacknowledged
  by then may be lost, and the close still succeeds. A write that
  completed just before the relay ended its subscription may reach no viewer, and no error follows.
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
  A track's subscription ends with PUBLISH_DONE (`TRACK_ENDED`) after its data streams close, then
  a FIN, which needs moq-relay 0.15.4 or later; neither connection sends GOAWAY
  ([relay interop deviations](#relay-interop-deviations)).
- Track names cannot be reused within a publisher connection, including after their writer closes.
- A track writer may open a multi-object group: objects are appended with object IDs 0, 1, 2, ...
  on one stream, and closing the group FINs the stream. Object 0 is the group's random-access point,
  which is how LOC/MSF media (one GOP per group) is published. A group that fails part-way is reset
  rather than completed, and the track stays open for the next group; whether a subscriber learns
  of the reset depends on the relay (see [Subscribing](#subscribing)); only one write or group may
  be pending per track. The write deadline applies separately to opening a group (including demand),
  each object write, and closing the group. Idle time between operations is unrestricted. Aborting a
  group after it closed cleanly has no effect: the FIN already ended the stream, and a reset after
  complete delivery is ignorable
  ([draft-20 Section 11.4.3](https://www.ietf.org/archive/id/draft-ietf-moq-transport-20.html#section-11.4.3)).
  Implemented in both SDKs.
- A track writer exposes whether the relay currently demands the track and lets the application
  observe every change, so a state-shaped track such as a catalog can re-send its document on each
  relay subscription.
  Demand observers emit the current state first, include replacement subscriptions (a repeated
  "demanded" value), and finish when the track ends. A closed track reports no demand. Demand is
  the relay's single upstream subscription for the track: the relay shares it across every
  downstream viewer, so additional viewers produce no further notification, and when it ends is
  the relay's choice (moq-relay 0.15 and later cancel it about one round trip after the last viewer
  leaves; earlier relays held it for about 30 seconds). Demand is therefore neither a viewer
  count nor a liveness signal. A document re-sent on each demanded value reaches the viewers who
  join during one relay subscription from the relay's cache, not as a re-send, so a state-shaped
  track writes only state that is safe to serve from cache and to receive more than once
  ([relay demand](relay-interop.md#relay-demand)). When the first viewer is on another relay, the
  publisher's relay first subscribes briefly to answer a track-info query and then subscribes again,
  so objects written during the first subscription reach no viewer, with or without a
  `SUBSCRIPTION_CHANGED` error; a live publisher cannot rely on its first objects after demand
  begins being delivered ([a viewer on another relay](relay-interop.md#a-viewer-on-another-relay)).
  Implemented in both SDKs.

### Delivery timeouts

[Draft-20 Section 8](https://www.ietf.org/archive/id/draft-ietf-moq-transport-20.html#section-8) gives each group two delivery timeouts, in milliseconds:
SUBGROUP_DELIVERY_TIMEOUT (`0x06`) bounds how long a group's FIN may wait for acknowledgment, and
OBJECT_DELIVERY_TIMEOUT (`0x02`) bounds how old an object may be when it reaches the transport.
There are no options for them: an application sets the publisher's values with the track and object
properties it already passes.

- **The value.** Each group's value is fixed just before its stream opens, after any wait for
  settling room. The publisher's is the property on the group's first object when present, and the
  track property otherwise; on any later object the property is ignored. The subscriber's is the
  parameter on the relay subscription's SUBSCRIBE, as updated by any REQUEST_UPDATE so far. When
  both are non-zero the smaller applies, and otherwise the non-zero one; 0 means no timeout. A value
  above 2147483647 ms is clamped to that bound, and registration refuses neither 0 nor a clamped
  value. Both types are even, so their values are integers: the [Arguments](#arguments) table
  already refuses bytes there. A REQUEST_UPDATE carrying either parameter is acknowledged with
  REQUEST_OK, and the new value applies to groups whose stream opens after it; groups already open
  or settling keep theirs.
- **The subgroup timeout** starts when the group's FIN is handed to the transport, the moment its
  operation completes and it becomes a settling group. If the relay has not acknowledged the FIN
  when it expires, the stream is reset with `DELIVERY_TIMEOUT` (0x2), as Section 8 requires before
  the stream reaches "all data committed". Nothing is reported to the track: the group leaves the
  settling set like any other late failure and frees its place under the 64-group limit. The timer
  belongs to the group, so it keeps running through a relay subscription change or end and through
  the track's PUBLISH_DONE; only the connection's end stops it.
- **The object timeout** counts from the application's write call (`write()`, or the group's
  `write()`) to the moment the transport takes the object, so waiting for demand, for settling
  room, for the stream and behind the group's earlier bytes all count. The age is checked again
  just before the object is handed over, as Section 8 requires. Until the group's value is fixed
  only the publisher's value times the wait; an object already older than the fixed value fails
  then, before any stream opens. When it expires the operation fails with `DELIVERY_TIMEOUT`
  in its current phase, and a stream already open is reset with code 0x2. In a group the whole
  group is reset and its later writes fail with the same error; no stream is opened again for it
  (Section 8: SHOULD NOT). The track stays open and the next write uses a new group ID, as after
  any interruption. The object timeout runs alongside the write deadline: whichever expires first
  fails the write, with `DELIVERY_TIMEOUT` or `WRITE_TIMEOUT`. Once the transport has taken an
  object, its bytes cannot be timed out. In TypeScript the transport has taken an object when its
  stream write resolves, so an object queued behind stream flow control is still timed; in Python,
  when the stream write returns, once aioquic has transmitted the bytes.
- **On moq-relay 0.15.8 and 0.17.0** neither value has any effect beyond the publisher. The relay
  never sends either parameter upstream: its SUBSCRIBE carries only `0x10`, `0x20`, `0x21`, `0x22`
  and `0x23`, plus `0x35` only to opt out of properties, which the relay never does
  ([subscribe.rs](https://github.com/moq-dev/moq/blob/cd59d15b07c2c2c8a5bfa8a2e82b3bbc24e82a76/rs/moq-net/src/ietf/subscribe.rs#L187-L199)), its REQUEST_UPDATE only `0x10`, `0x20` and `0x21`,
  and it states that it never negotiates the Section 8 parameter ([error.rs](https://github.com/moq-dev/moq/blob/cd59d15b07c2c2c8a5bfa8a2e82b3bbc24e82a76/rs/moq-net/src/ietf/error.rs#L85)).
  It drops both track properties and the `0x02` object property, and reads an object's `0x06` as a
  Timestamp ([Relay property handling](#relay-property-handling)), so a first-object `0x06` reaches
  viewers as a Timestamp. In production the publisher's own values are therefore the only ones that
  apply, and a track property is the safer way to set the subgroup timeout. The reverse also holds:
  a publisher that still sends timestamps as `0x06` (the draft-ietf-moq-loc-03 id) now gives each
  group a subgroup timeout equal to its first object's timestamp. Applications must not send a
  timestamp as `0x06`; LOC Timestamp is `0x10`. Evidence:
  [issue #167](https://github.com/muxinc/relay-sdks/issues/167).

Implemented in both SDKs.

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
| waiting-for-demand | object timeout expires: the publisher's value, the only one that applies before the group's value is fixed | `DELIVERY_TIMEOUT`, phase `waiting-for-demand` | nothing sent | open; the next write waits for demand again | §8: an object's age counts from when the application provided it, and is checked before it reaches the transport |
| waiting-for-demand | relay subscription ends while waiting | the wait continues until the deadline | nothing sent | open | SDK policy |
| waiting-for-demand | caller cancels | the caller's reason, unwrapped; phase `waiting-for-demand` | nothing sent | open | SDK policy |
| waiting-for-demand | track closed | `CLOSED`, phase `waiting-for-demand` | nothing sent | ended | SDK policy |
| waiting-for-demand | connection ends | the connection's own error (phase `connection`), or `CLOSED` after a clean shutdown | nothing sent | ended | §3.5: streams and subscriptions end with the session |
| opening-stream | write deadline expires or caller cancels | `WRITE_TIMEOUT` or the caller's reason; phase `opening-stream` | a stream that opens after the failure is reset before any object is written, code `DELIVERY_TIMEOUT` (0x2) or `CANCELLED` (0x1); its header may precede the reset (RESET_STREAM_AT) | open; the next write uses a new group ID | §11.4.3: a stream closed before all objects are delivered MUST be reset, and the reset SHOULD include the header; the deadline itself is SDK policy |
| opening-stream | object timeout expires, while waiting for settling room or for the stream, or the object is already older than the value fixed just before the stream opens | `DELIVERY_TIMEOUT`, phase `opening-stream` | no stream opens, or a stream that opens after the failure is reset before any object is written, code `DELIVERY_TIMEOUT` (0x2) | open; the next write uses a new group ID | §8 MUST reset with `DELIVERY_TIMEOUT`; §11.4.3 |
| writing | object timeout expires before the transport takes the object | `DELIVERY_TIMEOUT`, phase `writing`; a group's earlier objects may have been delivered. Never `closing`: the object is with the transport by then | stream reset, code `DELIVERY_TIMEOUT` (0x2); the group's later writes and `close()` fail with the same error, and no stream is opened again for it | open; the next write uses a new group ID | §8: MUST reset with `DELIVERY_TIMEOUT`, SHOULD NOT open a new stream for the subgroup |
| writing, closing | write deadline expires | `WRITE_TIMEOUT`, phase `writing` or `closing`; object bytes may have been delivered, and `closing` can fail after all of them were. In TypeScript `closing` can only fail before the FIN is handed to the transport; Python's `closing` can still wait on transport backpressure | stream reset, code `DELIVERY_TIMEOUT` (0x2) | open; the next write uses a new group ID | §11.4.3 MUST reset; §8 and §15.11.4 for the code; the deadline is SDK policy |
| opening-stream, writing, closing | caller cancels (AbortSignal, task cancellation, or `group.abort()`) | the caller's reason unwrapped, or `CLOSED` when `abort()` gave none; the current phase | stream reset, code `CANCELLED` (0x1) | open | §11.4.3 MUST reset; code choice is SDK policy (§3.3.4 SHOULD be relevant) |
| opening-stream, writing, closing | the transport refuses the stream or fails a write, other than the relay stopping the operation's own stream | `TRANSPORT_ERROR` with the cause preserved; the current phase | stream reset, code `INTERNAL_ERROR` (0x0), when a stream exists | open | SDK policy |
| opening-stream, writing, closing | relay subscription replaced or ended, or the relay stopped the operation's stream (STOP_SENDING, §11.4) | `SUBSCRIPTION_CHANGED`; the current phase; after a stop the transport error is kept as the cause. The same code whichever of the stop and the subscription change arrives first, and nothing further is reported for the operation | stream reset, code `CANCELLED` (0x1); after a stop the QUIC stack has already answered with its own RESET_STREAM (RFC 9000 §3.5) | open; the next write waits for new demand only if the relay subscription ended, and proceeds at once while it is still live | §5.1.1: the publisher MUST reset open streams of a cancelled subscription; §11.4: a subscriber MAY stop a subgroup stream whose group is no longer of interest, which does not by itself end the subscription; treating a new SUBSCRIBE as a replacement rather than a concurrent subscription is SDK policy (§5.1, ADR-0001) |
| opening-stream, writing, closing | track closed | `CLOSED`; the current phase | stream reset, code `CANCELLED` (0x1) | ended | §10.12: every stream is closed or reset before the track ends |
| opening-stream, writing, closing | connection ends | the connection's own error (phase `connection`), or `CLOSED` | streams end with the session; a reset that is still sent carries `SESSION_CLOSED` (0x3), or `GOING_AWAY` (0x4) after GOAWAY | ended | §3.5; §15.11.4 for the codes |
| none: refused before an operation starts | a write or group on a track that already ended, or a write on a group already closed with its FIN | `CLOSED`, phase `waiting-for-demand` | nothing sent | unchanged | SDK policy: the phase that means nothing was sent |
| any | a second write or group is started | `WRITE_PENDING`, phase `writing`; the active operation is unaffected | nothing sent | unchanged | SDK policy: one pending operation per track |
| after the FIN is handed over (settling group) | subgroup timeout expires before the relay acknowledges the FIN | nothing; the operation had completed | stream reset, code `DELIVERY_TIMEOUT` (0x2); the group leaves the settling set | open, whether or not the subscription or the track has ended since | §8: MUST reset the stream when the timer expires before all data is acknowledged |
| after a clean group close | `abort()` | nothing; `closed` stays true and a repeated `close()` returns | nothing sent | open | §11.4.3: a reset after all data and the FIN is ignorable |

A failed group's ID is never reused, which is stricter than §2.2 (a subgroup may resume on a new
stream after a premature reset); group IDs increase monotonically and object IDs start at 0 within a
group (draft-ietf-moq-msf-01 §6). A group may end without an `END_OF_GROUP` marker (§2.4.2). There
is no automatic retry, and the application chooses the next random-access point after a failure;
MSF and LOC say nothing about an abandoned group. Implemented in both SDKs.

### Relay property handling

These are deployment observations, separate from the SDK guarantees above. Tests against
production moq-relay 0.15.8 on 2026-09-28 (3 of 3 runs identical) found the following, and the
same matrix was identical on production 0.17.0 on 2026-10-05. The relay stores each object as its
size and a timestamp only, so no other object property can survive it.

- Of the object properties, only Timestamp (`0x10`) reaches a subscriber. The relay also reads
  `0x06`, the Timestamp id of draft-ietf-moq-loc-03, as a Timestamp and forwards it as `0x10`.
  Every other object property tested was dropped, even- and odd-typed alike: Timescale (`0x08`),
  Video Config (`0x0D`), `0x02`, `0x04`, `0x05`, `0x0B`, and `0x40` to `0x43`. Object payloads and
  IDs were preserved.
- Timestamps are rescaled to microseconds using the declared Timescale: with track Timescale 90000,
  a Timestamp of 123456 arrives as 1371733. An object-level Timescale takes precedence over the
  track's. With Timescale 1,000,000 a supplied Timestamp passes through unchanged.
- A track without a declared Timescale gets relay-clock timestamps on every object, replacing any
  Timestamp the publisher supplied. So does an object without a Timestamp on a declared track.
- `SUBSCRIBE_OK` carries the relay's own track properties for every track: Timescale (`0x08`) =
  1,000,000, Default Publisher Priority (`0x0E`) = 128, and Default Publisher Group Order (`0x22`,
  decimal 34) = 2 (descending). A publisher's own `0x0E` passes through, its `0x22` is replaced
  with 2, and its other track properties (for example `0x0D`, `0x40`, `0x41`) are dropped.
- A track registered with a Default Publisher Priority (`0x0E`) above 255 or a Default Publisher
  Group Order (`0x22`) other than 1 or 2 makes the relay refuse **every** subscribe to it with
  REQUEST_ERROR 18 `invalid value`. The publisher gets no error; its writes time out waiting for
  demand. Draft-20 rules both values out (§12.4, §12.5), so the SDKs refuse them at registration
  ([issue #252](https://github.com/muxinc/relay-sdks/issues/252)).
- **On 0.17.0**, a track registered with more than 64 track properties makes the relay refuse
  **every** subscribe to it with REQUEST_ERROR 18 `too many` (`MAX_PROPERTIES`, an implementation
  limit), again with no error to the publisher; 64 pairs, or one byte value filling the rest of a
  maximal `SUBSCRIBE_OK`, are accepted. The SDKs refuse such a track at registration
  (`relay.limits.maxTrackProperties`, [issue #252](https://github.com/muxinc/relay-sdks/issues/252)).
- **From 0.17.0**, an object whose property block is larger than 64 KiB (65,536 bytes) is dropped
  by the relay: it stops that stream, and the subscriber sees a gap with no reset, while the
  publisher's write succeeds. The SDKs refuse such a write (`relay.limits.maxObjectPropertiesBytes`,
  [issue #247](https://github.com/muxinc/relay-sdks/issues/247); upstream `434ab81f5`, an
  implementation limit rather than a draft one).
- **On 0.17.0**, a subscription with `INCLUDE_PROPERTIES` = 0 gets an empty track property list in
  `SUBSCRIBE_OK` and objects with no properties at all, Timestamp included
  ([publisher.rs](https://github.com/moq-dev/moq/blob/cd59d15b07c2c2c8a5bfa8a2e82b3bbc24e82a76/rs/moq-net/src/ietf/publisher.rs#L683-L695)).

This is not an exhaustive registry compatibility test or a promise about future relay versions.
Compared with the 0.14.17 and 0.15.1 observations, the `0x0E` in `SUBSCRIBE_OK` and the
invalid-value refusal are new.

For publisher-defined media timing, declare a track timescale and put timestamps in that unit on
objects. Interpret received timestamps using the received track timescale; a timestamp's presence
alone does not establish that the publisher supplied it. Do not treat a relay-generated timestamp
as the application's event time or a synchronized wall clock. Put application metadata that must
arrive intact in the payload (or a separate metadata track's payload). For the Video.js media
example, codec configuration is also carried in the catalog's `initDataList`, so decoding does not
depend on Video Config object-property forwarding.

Evidence and protocol naming: [issue #23](https://github.com/muxinc/relay-sdks/issues/23) and, at
moq-relay 0.15.8, the relay's
[track property implementation](https://github.com/moq-dev/moq/blob/b00be0ffc3734addcdd5e6ad4b348ae055e70957/rs/moq-net/src/ietf/properties.rs),
[SUBSCRIBE_OK track properties](https://github.com/moq-dev/moq/blob/b00be0ffc3734addcdd5e6ad4b348ae055e70957/rs/moq-net/src/ietf/publisher.rs#L575-L587)
and [frame model](https://github.com/moq-dev/moq/blob/b00be0ffc3734addcdd5e6ad4b348ae055e70957/rs/moq-net/src/model/frame.rs).

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
The media pins in `protocol.json` and exact bytes and parsed values in `vectors/media.json` apply
to both languages.

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
- The catalog reader (`parseMsfCatalog` / `parse_msf_catalog`) takes a catalog object's payload
  and the catalog track's namespace, and returns the version, generation time, completeness, and
  track descriptions in document order. It reads the independent form only. It rejects a payload
  that is not a UTF-8 JSON object (no byte-order mark, no `NaN`/`Infinity`), a missing or
  non-string `version`, a version other than `draft-01` or the deployed alias `1` (msf-01 §5.1.1:
  a subscriber MUST NOT parse a version it does not understand), a missing `tracks` array, a
  `deltaUpdate`, and init data that is not base64 (decoded as WHATWG `atob` does). Everything else
  is ignored when not understood, as msf-01 requires: unknown fields, fields of the wrong JSON type,
  non-integral widths, heights, sample rates, bitrates and generation times, track entries that are
  not objects or have no string name, and init entries whose type is not `inline`. A track's
  slash-joined `namespace` is split back into fields, dropping empty ones (the builder's dialect);
  a track without one inherits the catalog track's namespace (§5.2.2). Packaging is returned as
  declared, `loc` or otherwise, and payloads are not interpreted. Absent packaging reads as the
  empty string and absent `isLive` as false. `initRef` resolves through `initDataList` to bytes;
  an unresolved reference leaves the track without init data. Every builder document must read
  back as its vector's `parsed` value.
- Encoding, pacing, and time-based frame dropping remain application responsibilities. Catalog
  serving, GOP grouping and recovery are the [Media publishing](#media-publishing) policy below,
  implemented in Python; a `register_media` publishing handle and the object-to-frame reader are
  not implemented.

## Media publishing

The media publishing policy lives in the product package (`mux_relay.media`) over registered
tracks: one tier above the write slot, one below the proposed `register_media` handle. It knows no
format. A frame carries its own object properties and a catalog is bytes. Implemented in Python;
not yet implemented in TypeScript.

- A catalog is a state-shaped track. The catalog service sends its document on the current relay
  demand and again on every later relay subscription, including a replacement subscription (the
  repeated "demanded" value), so a viewer joining after a relay reconnect receives it. A viewer
  joining while one relay subscription lasts triggers no re-send and is served the relay's cached
  copy. It does not
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
   accepted, retrying the fresh-namespace rejection while the announce propagates; the report
   records the retries.
3. Binary payloads, an empty payload, and a text payload all arrive intact.
4. Closing one track writer does not affect delivery on another track.
5. A late subscriber decodes `LARGEST_OBJECT` from a populated track's `SUBSCRIBE_OK` and receives the next live object.
6. All connections close cleanly once the test closes them.

Adding an assertion here means adding it to every SDK's smoke test.
See [relay interoperability](relay-interop.md#reproduce-against-a-local-relay) for a local moq-relay 0.17.0 test.

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

### Relay interop deviations

Four choices in the generic cores depend on how moq-relay treats the draft's teardown messages,
track aliases and request updates. All four conform to draft-20; each is recorded because the
relay's behavior, and for the first one its version, decides whether it works. The relay behavior,
the evidence and how to re-test are in [relay
interoperability](relay-interop.md#draft-20-deviations). Re-test against the relay before changing
one: on a relay older than 0.15.4 the first one aborts every viewer's track. Both SDKs agree on all
four, and the core module headers (`ts/packages/moq-core/src/protocol/publish-session.ts`,
`python/packages/moq-core/src/moq_core/publish_session.py`) state the same constraints.

1. **A subscription ends with PUBLISH_DONE, then FIN, which needs moq-relay 0.15.4 or later.**
   Draft-20 [Section 3.3.2](https://www.ietf.org/archive/id/draft-ietf-moq-transport-20.html#section-3.3.2):
   "the publisher of an Established subscription MUST send PUBLISH_DONE, before sending a FIN"; and
   [Section 10.12](https://www.ietf.org/archive/id/draft-ietf-moq-transport-20.html#section-10.12):
   "A sender MUST NOT send PUBLISH_DONE until it has closed all streams it will ever open". This
   conforms. Every end the publisher chooses resets the subscription's data streams that are still
   open, writes PUBLISH_DONE with Stream Count set to the data streams opened for that
   subscription, and FINs the request stream:

   | end | PUBLISH_DONE status | reason |
   | --- | --- | --- |
   | closing a track writer, or the publisher | `TRACK_ENDED` (0x2) | `track ended` |
   | a replacement subscription supersedes an older one | `TRACK_ENDED` (0x2) | `superseded by a newer subscription` |
   | an unsupported REQUEST_UPDATE, after its REQUEST_ERROR ([Section 10.9.1](https://www.ietf.org/archive/id/draft-ietf-moq-transport-20.html#section-10.9.1)) | `UPDATE_FAILED` (0x8) | `unsupported subscription update` |
   | the subscriber FINs its direction while the subscription is live | `INTERNAL_ERROR` (0x0) | `subscriber closed its request stream` |

   No status names a replacement, and the relay finishes a track only on `TRACK_ENDED`. A
   subscription the subscriber cancels (a reset) gets no PUBLISH_DONE, and its open data streams
   are reset ([Section 5.1.1](https://www.ietf.org/archive/id/draft-ietf-moq-transport-20.html#section-5.1.1)).
   Once a subscription is ending no data stream opens on its alias, and one that finishes opening
   afterwards is reset and not counted. moq-relay 0.15.4 and later read the PUBLISH_DONE, wait for
   Stream Count data streams or a grace period (1 s), finish the track, and only then FIN their side
   of the request stream: a viewer receives an end-of-track object and PUBLISH_DONE status 2
   `track ended`. A session that closes before the relay has done so aborts the track, which a
   viewer sees as status 0 `internal error`, so closing the publisher waits for that FIN (the one
   Section 3.3.2 says a requester SHOULD send once the responder's FIN completes the request) on
   every subscription it ended before closing the transport (bounded, see deviation 2). The SDK
   subscriber sends that FIN too, once the publisher's FIN follows PUBLISH_DONE, so an SDK publisher
   facing an SDK subscriber directly closes without waiting out the bound. moq-relay 0.15.4 and later
   treat a FIN without PUBLISH_DONE as a failed request and abort the track. moq-relay 0.15.1 to 0.15.3 do the reverse:
   any byte after SUBSCRIBE_OK aborts the track, so every end these SDKs send reaches viewers as
   status 0 `internal error`, where a bare FIN finished the track. No behavior ends cleanly on both,
   and the SDKs cannot tell the versions apart (the relay's SETUP names only `moq-lite-rs`), so they
   require 0.15.4 or later.
2. **Neither connection sends GOAWAY.** Draft-20
   [Section 10.4](https://www.ietf.org/archive/id/draft-ietf-moq-transport-20.html#section-10.4):
   "An endpoint sends a GOAWAY message on its control stream to inform the peer it intends to close
   the session soon", and "A client MUST send a zero-length New Session URI in any GOAWAY". GOAWAY
   is optional, so omitting it conforms. The SDK sends none because it has no drain behavior: a
   GOAWAY written as the transport closes gives the peer nothing to act on. How the relay treats a
   client GOAWAY depends on its version: moq-relay 0.14.17 closes the whole session, and 0.15.1 and
   later record it and keep the session. Closing the publisher ends every subscription with
   PUBLISH_DONE and a FIN (deviation 1). It then waits, for up to the largest write deadline among
   its tracks, for the relay to acknowledge the connection's settling groups; Python also waits for
   the PUBLISH_DONEs and FINs to be acknowledged, so a short uplink outage at close does not send the
   session close while they are still in flight. After that it retracts the namespace with
   NAMESPACE_DONE, gives those writes a bounded window (250 ms), then waits up to 2 s for the relay
   to FIN its side of each ended subscription's request stream, and ends the session. None of these
   bounds reports an error when it expires. The 2 s covers the relay's 1 s grace plus a round trip; against moq-relay the wait
   ends as soon as the relay has finished every track, typically within a round trip. Closing a
   subscriber connection closes the transport and writes nothing more on the control stream. The application observes nothing: the relay learns of the shutdown from the
   transport close, and from a publisher's PUBLISH_DONEs and retraction before it. A GOAWAY the relay sends
   is still received and handled on both connections. Sending GOAWAY becomes useful once the SDK
   can drain on it, which means giving it time to arrive before the transport closes; on relays
   older than 0.15.1 it would still close the session.
3. **A track alias is the peer's own request ID.** Draft-20
   [Section 5.1](https://www.ietf.org/archive/id/draft-ietf-moq-transport-20.html#section-5.1): "A
   publisher MAY assign the same or different Track Aliases to these subscriptions"; and
   [Section 11.1](https://www.ietf.org/archive/id/draft-ietf-moq-transport-20.html#section-11.1):
   "The same Track Alias MUST NOT be used by a publisher to refer to two different Tracks
   simultaneously in the same session." This conforms, and narrows a MAY: every SUBSCRIBE_OK
   carries the SUBSCRIBE's request ID as the alias, which is unique for the session by
   construction, so an alias is never shared between subscriptions, of one track or of two. A
   request ID the peer reuses, or one with a client's (even) parity, closes the session with
   `INVALID_REQUEST_ID` before it can become an alias, as
   [Section 10.1](https://www.ietf.org/archive/id/draft-ietf-moq-transport-20.html#section-10.1)
   requires. The subscriber connection applies the same check to every request the relay opens,
   including the ones it rejects, before answering. moq-relay allocates server request IDs odd,
   from 1, so the parity check never fires against it. moq-relay 0.15.1 and 0.15.8 close the session with PROTOCOL_VIOLATION when one
   alias names two different tracks, and cancel the newer subscription when two live subscriptions
   to one track share an alias; unique aliases avoid both. The application never sees an alias on
   the Relay surface. Nothing needs to change unless the SDK serves concurrent subscriptions to one
   track, which
   [ADR-0001](../docs/adr/0001-one-relay-subscription-per-track.md) rules out today.
4. **`update()` sends REQUEST_UPDATE, which moq-relay does not implement.** Draft-20
   [Section 10.9](https://www.ietf.org/archive/id/draft-ietf-moq-transport-20.html#section-10.9):
   "The receiver of a REQUEST_UPDATE MUST respond with exactly one REQUEST_OK or REQUEST_ERROR
   message indicating if the update was successful". This conforms: each `update()` on an accepted
   subscription writes one REQUEST_UPDATE, with its own Request ID, on that subscription's request
   stream; a REQUEST_OK reaches `onUpdateOk` / `on_update_ok`, and a REQUEST_ERROR reaches
   `onError` / `on_error` and ends the subscription. moq-relay answers neither, from 0.14.17 through
   0.17.0: after SUBSCRIBE_OK it only polls the request stream for closure, any inbound byte counts
   as a close there, and it then takes its normal end path, dropping its upstream demand and
   writing PUBLISH_DONE status `TRACK_ENDED` (0x2) `track ended`. A viewer that calls `update()`
   therefore gets `onPublisherDone` / `on_publisher_done` within one round trip and no further
   objects, whatever the update carried (a subscriber priority, `forward` 0, or nothing), and
   `onUpdateOk` / `on_update_ok` never fires. As after any done, the subscription's `ended` stays
   pending until it is cancelled or the connection closes. The session stays open, and a new
   subscribe on the same connection works. So against moq-relay a pause is a cancel followed by a
   new subscribe later, and a changed priority or filter is a new subscription. Subscribing paused
   is not an alternative: moq-relay 0.17.0 refuses a SUBSCRIBE with `FORWARD` = 0 with request
   error 3 (`NOT_SUPPORTED`) `FORWARD=0 not supported` and keeps the session open, where 0.15.8
   closed the session. The SDK sends the
   update anyway and does not warn; whether it should is open in
   [issue #94](https://github.com/muxinc/relay-sdks/issues/94), and REQUEST_UPDATE support would be
   an upstream relay feature.

Both cores pin the second choice for each role: after an orderly close, the control stream of a
publish session and of a subscribe session carries SETUP and nothing else.
