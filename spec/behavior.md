# SDK behavior contract

Every Relay SDK, in any language, implements the same protocol pin and the same observable
semantics. This document states those semantics without reference to any one language. The
language READMEs describe how each API spells them; this file says what must be true; and
`vocabulary.md` fixes the public names every SDK uses for the concepts named here.

`protocol.json` holds the machine-readable pins. Each SDK has a test that checks its constants
against that file.

## Connection

- The MOQT version is the draft pinned in `protocol.json`, currently draft-ietf-moq-transport-22
  with wire identifier `moqt-22`, and draft-ietf-moq-transport-20 (`moqt-20`) as a fallback. An SDK
  offers `moqt-22`, then `moqt-20`, and no other protocol identifier, and speaks whichever the relay
  selects. The two drafts differ on the wire only in the LOCATION_FILTER parameter, which the core
  encodes for the negotiated one. moq-relay selects `moqt-22` from 0.15.0, so every relay version
  the SDKs support speaks it, but it frames LOCATION_FILTER the draft-20 way on `moqt-22` too
  ([draft-22 LOCATION_FILTER framing](relay-interop.md#draft-22-location_filter-framing)).
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

### Transport fallback

- The connection's transport is the application's choice, through the transport factory. The
  default stays the browser's WebTransport (TypeScript) or native QUIC (Python, and `@mux/relay/node`):
  the SDK never falls back to another transport by itself.
- The TypeScript SDK's opt-in `@mux/relay-websocket` package carries MOQT on QMux over a WebSocket,
  TCP to the same host and port as the endpoint (`https:` becomes `wss:`), for networks that block
  UDP and browsers without a usable WebTransport. `createWebSocketTransport()` always dials it.
- `withWebSocketFallback()` dials WebTransport first, and QMux over WebSocket when that handshake
  fails or has not completed within `handshakeTimeoutMs` (default 4 s, about when Chromium itself gives
  up on a silent handshake). The two dials run one after the other, never at once, and the abandoned
  WebTransport dial is closed. Where the runtime has no WebTransport that can negotiate protocols, it
  dials WebSocket at once. Each connection, a supervisor's reconnects and rotations included, decides
  afresh; nothing is remembered between connections. A WebTransport session that comes up and is
  then refused (for example over its credentials) is not retried over WebSocket.
- Both transports run the same session; the differences are the relay's and TCP's
  ([QMux over WebSocket](relay-interop.md#qmux-over-websocket)): streams share one TCP connection, so
  a lost packet holds up every stream (head-of-line blocking); datagrams are unavailable; a
  credentials refusal arrives as a failed WebSocket handshake, reported as `TRANSPORT_ERROR` in phase
  `connecting`, not `UNAUTHORIZED`; and a send stream's close resolves once its FIN reaches the socket,
  not the relay ([transport adapter](transport-adapter.md#known-deviations)).
- The application can give the SDK a cancellation primitive, such as an abort signal or a
  cancellation token, that governs the connection's entire lifetime rather than only setup.
- Every connection exposes a way to observe its closure and the error that caused it, if any.
- A GOAWAY from the relay ends the connection with an error. The application opens a replacement.
  Subscriptions still awaiting their first response end as terminated by the closure (see
  "Subscription lifecycle"); accepted or cancelled requests do not receive a second initial
  response. The core's own track source still fails pending requests through their error callback.
  Opt-in [supervised connections](#supervised-connections) reconnect; the primitives never do.

### Credential lifetime

- A session lasts until its credential expires, and no longer. moq-relay ends a session at its
  grant's deadline: the JWT's `exp` with the reference auth server (`moq auth serve`), and no
  later with Mux's. A live session cannot take a new token: the relay ignores
  AUTHORIZATION_TOKEN on later requests, and revalidation re-checks the credential the session
  connected with ([token expiry](relay-interop.md#token-expiry)). Plan to replace a connection
  before its credential expires.
- The token provider runs once per connection, while connecting. It returns a token, or a
  `RelayCredential`: the token together with `expiresAt` / `expires_at`, the time the relay stops
  honoring it. An empty token sends none, as before. A credential of the wrong shape is a
  `TypeError`; an invalid `Date` (TypeScript) or a naive `datetime` (Python) is a `RangeError` /
  `ValueError`.
- Every connection exposes `expiresAt` / `expires_at`: the credential's expiry when the provider
  gave one, otherwise the `exp` claim of the JWT it connected with (a bare token from the provider,
  or a `jwt` already on the URL), read without verifying it. A token that is not a JWT with a
  numeric `exp` leaves it unknown (`undefined` / `None`). The primitives only report it; they never
  replace a connection.
- At that time the relay closes the session with MOQT session termination code `UNAUTHORIZED`
  (0x2), reason `unauthorized`. Both SDKs report it as `UNAUTHORIZED` in phase `connection` on the
  connection's closure, and operations still pending fail with it. On moq-relay 0.17.0 the close
  arrives about 80 ms after `exp`. One exception: a TypeScript publisher in Chromium sees the relay
  abort its control stream before WebTransport reports the close code, and reports
  `TRANSPORT_ERROR` in phase `connection` instead
  ([issue #279](https://github.com/muxinc/relay-sdks/issues/279)); a Chromium subscriber reports
  `UNAUTHORIZED`.
- [Supervised connections](#supervised-connections) replace their connection make-before-break
  before the credential expires; see [rotation](#rotation).

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
| `PREFIX_OVERLAP` | `namespaceEvents` / `namespace_events` was given a prefix that equals, contains or falls under one this connection is already discovering (draft-22 §9.15). Nothing was sent. Widen one iterator's prefix and filter, end the other iterator first, or use another connection. |
| `UPDATE_QUEUE_FULL` | More than eight updates were issued before the subscription was accepted; await acceptance (`onSubscribed` / `on_subscribed`) before updating again. Nothing was sent for the refused update. |
| `SUBSCRIPTION_CHANGED` | Upstream demand ended or changed during an operation; choose fresh data or a new random-access point. It can be group-level: the relay may stop one group's stream while its subscription continues, so check `hasDemand` / `has_demand` rather than assume demand ended. |
| `GOAWAY` | The relay requested migration; open a replacement connection. |
| `TRANSPORT_ERROR` | Inspect the original cause and connection state before deciding whether to reconnect or publish again. In TypeScript, a `connecting`-phase `TRANSPORT_ERROR` usually means the browser abandoned the handshake, so a retry policy should treat it like `CONNECTION_TIMEOUT` (see [browser transport limits](../ts/GUIDE.md#browser-transport-limits)). |
| `TRANSPORT_UNAVAILABLE` | Supply a transport implementation in a runtime without browser WebTransport; in Node, import from `@mux/relay/node` with `@mux/relay-quic` installed (TypeScript). |
| `UNAUTHORIZED` | The relay refused the connection's credentials: no token, a bad, malformed or expired one, or an endpoint path the token does not grant. Supply a valid token before connecting again. In phase `connection`, the relay ended a live session because its credential expired or its grant was revoked ([credential lifetime](#credential-lifetime)); connect again with a fresh token. TypeScript reports it in browsers and over native QUIC (`@mux/relay/node`); against relays before moq-relay 0.17.0 a browser does not see the close, so there it arrives as `TRANSPORT_ERROR` (#14). |

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
acknowledgment. The SDK does not automatically retry or reconnect, outside the opt-in
[supervised connections](#supervised-connections). `UNAUTHORIZED` is the one code that
says the relay refused the credentials: the relay closed the session with MOQT session termination
code `UNAUTHORIZED` (0x2; `MALFORMED_AUTH_TOKEN` 0x16 and `EXPIRED_AUTH_TOKEN` 0x18 map to it too).
Draft-22 §12.2 defines it as refusing to establish a session, but moq-relay also closes a live
session with it when the session's grant expires or is revoked, so both SDKs map it in any phase:
`connecting` before the relay's SETUP, `connection` after it (#227, #261). A close in the turn SETUP
arrives stays `TRANSPORT_ERROR`. From moq-relay 0.17.0 a
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
| Namespace | 1 to 32 non-empty string fields in an array (TypeScript) or a list or tuple (Python); sent verbatim, never split or joined on `/`. The 32-field limit is `limits.maxNamespaceFields` in `protocol.json`. A discovery prefix follows the same rule. | any other container, including a single string; no fields; a field that is not a non-empty string | more than 32 fields | `connectRelayPublisher` / `connect_relay_publisher`; `subscribe`, `subscribeEvents` / `subscribe_events`, `trackSource.subscribe` / `track_source.subscribe`; the prefix of `namespaceEvents` / `namespace_events` |
| Track name | A non-empty string, in both roles. | anything else | | `registerTrack` / `register_track`; the three subscriber entry points |
| Properties | A list of `{type, value}` pairs. `type` is a non-negative integer; an even type carries a non-negative integer, an odd type carries bytes, which are copied. Integers are bounded by what the language's core varint carries: 2^53-1 in TypeScript, 2^64-1 in Python. Each byte value is at most `limits.maxPropertyValueBytes` (its 16-bit wire length). A track's properties encode to at most `limits.maxTrackPropertiesBytes` (65,525 bytes, the room SUBSCRIBE_OK leaves them), hold at most `relay.limits.maxTrackProperties` (64) pairs, and keep Default Publisher Priority (`0x0E`) at most 255 and Default Publisher Group Order (`0x22`) at 1 or 2 (`trackProperties`, draft-22 §10.4 and §10.5), because otherwise the track registers but every subscribe to it fails while the publisher's writes time out. An object's properties on `write` encode to at most `relay.limits.maxObjectPropertiesBytes` (64 KiB), because the relay drops a larger object without an error to either side. The SDK measures the encoding with the core's own encoder before any slot or stream is taken. | not a list; a pair without the shape; a type or integer value that is not a non-negative integer the varint carries; bytes where an integer belongs or an integer where bytes belong | a byte value longer than its wire length; a list whose encoding exceeds its bound; track properties with more than 64 pairs, or a `0x0E` or `0x22` value draft-22 rules out | `registerTrack` / `register_track`; `write` on a track or group writer |
| Endpoint | A string holding an absolute `https` URL with a host and no fragment or userinfo. A `jwt` query parameter, when present, is not empty; this holds for every `jwt` parameter when there are several. | not a string; a string that does not parse as an absolute URL, including an `https` URL with no host | another scheme; a fragment or userinfo; an empty `jwt` parameter | `connectRelaySubscriber` / `connect_relay_subscriber`, `connectRelayPublisher` / `connect_relay_publisher` |
| Deadline | A number in (0, 2147483647] milliseconds in TypeScript, (0, 2147483.647] seconds in Python: the largest delay a JavaScript timer holds, and the same bound in both SDKs. The connection, request and write deadlines share one check. | not a number, including a boolean | zero, negative, NaN, infinite, or above the bound | `connectTimeoutMs` / `connect_timeout` on both connect calls; `requestTimeoutMs` / `request_timeout`; `writeTimeoutMs` / `write_timeout` on `registerTrack` / `register_track` |
| Track registration | A track name not yet registered on this publisher connection, including after its writer closed. Checked after the track name, properties and deadline, and after connection state. | | a name already registered | `registerTrack` / `register_track` |
| Media track timing | A media kind, `'audio'` or `'video'`; every duration a positive integer number of microseconds (a safe integer in TypeScript, an `int` other than a boolean in Python) of at most 2147483647000, the deadline bound in whole milliseconds. Video needs a keyframe interval, and a target latency is at least the keyframe interval. | a kind that is not a string; video without a keyframe interval; a duration that is not a number (TypeScript) or not an `int`, including a boolean and any float (Python) | another kind; a target latency below the keyframe interval; a duration that is zero, negative or above the bound, and in TypeScript also fractional, NaN or infinite | `mediaTrackProperties` / `media_track_properties` |
| Object status | `normal`, `end-of-group` or `end-of-track`; defaults to `normal`. | not a string | any other string | `write`, `writeText`, `writeJSON` on a group writer |
| Event capacity | A positive integer (a safe integer in TypeScript); defaults to 64. | not a number, including a boolean | zero, negative, fractional, NaN, or infinite | `subscribeEvents` / `subscribe_events`, `namespaceEvents` / `namespace_events` |
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
  ([draft-22 Section 3.3.1](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#section-3.3.1)).
  Whether that reaches back into what a relay cached before the subscription is the relay's
  choice. The Mux relay (moq-relay 0.17.0) starts it at the beginning of its latest group, and
  starts a Next Object filter after its largest object, skipping the rest of that group
  ([late subscribers](relay-interop.md#late-subscribers-and-location-filters)). A subscriber that
  needs earlier groups asks for them with a relative-group or absolute filter; an absolute start is
  served from the requested group while the relay still caches it. The SDK sends the
  filter it is given and adds none.
- The SDK sends every subscription parameter the application sets. Which ones take effect is the
  relay's choice: moq-relay 0.17.0 ignores GROUP_ORDER, NEW_GROUP_REQUEST and the delivery
  timeouts on a SUBSCRIBE, and refuses FORWARD = 0 and range filters
  ([subscription parameters](relay-interop.md#subscription-parameters)). The SDK does not warn
  about an ignored parameter.
- The request timeout bounds only the subscription's initial response. An accepted subscription
  has no application inactivity deadline. The SDK schedules no application-level keepalive: keeping
  the transport connected is left to peer traffic and the transport's own keepalive, which
  [draft-22 Section 15.6.1](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#section-15.6.1)
  names as one way to keep an idle session open. In TypeScript that is Chromium's own QUIC PING. In
  Python the aioquic adapter sends a QUIC PING once it has sent nothing for 5 seconds, which also
  lets a receive-only connection survive a NAT rebinding: the relay learns the new address from
  that packet. The Python transport uses aioquic's default 60-second local idle timeout, subject to
  peer negotiation.
- `update()` sends REQUEST_UPDATE, which the draft allows only on an accepted subscription
  ([draft-22 Section 3.1](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#section-3.1)).
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
  REQUEST_UPDATE carries no track either, so it cannot change tracks against any relay. A
  [switchable subscription](#switching-tracks) changes the track, the filter or the priority with
  a new subscription, make-before-break.
- A subscribe for a namespace the relay has not seen announced is rejected, not parked: moq-relay
  0.15.8 and 0.17.0 answer request error 16 (0x10, `DOES_NOT_EXIST`) with reason `unroutable`, and
  moq-relay through 0.15.1 answered its literal 404. The same code 16 with reason `not found` means the
  namespace is announced but the track is not, which is not the race. The relay routes subscribes to
  a namespace only once it has processed the publisher's announce, about one round trip after the
  publisher's `announced` settles, and later on a subscriber's relay that learns the route from a
  cluster peer ([announce propagation](relay-interop.md#announce-propagation)), so a subscribe that
  races the announce is rejected the same way. The SDK reports that rejection like any other,
  through `subscribed` and `on_error`, and does not retry a subscribe. An application avoids the
  race by subscribing once its [discovery](#discovery) reports the namespace `announced`, or else
  retries the rejection within its own budget rather than treating it as fatal. A
  [supervised subscriber](#supervised-subscriber)'s want retries it: 16 `unroutable` and the older
  404 without reporting them, never treating 16 `not found` as the race
  ([issue #136](https://github.com/muxinc/relay-sdks/issues/136)).
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

## Switching tracks

Changing what a viewer receives (a simulcast or ABR layer, a location filter, a priority) takes a
new SUBSCRIBE: draft-22 REQUEST_UPDATE names no track, and moq-relay ends a subscription on any
update. The draft recommends subscribing to the alternate track before cancelling the old one
([draft-22 Section 3.7.2](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#section-3.7.2)).
`subscriber.subscribeSwitchable(options, handlers)` / `subscriber.subscribe_switchable(namespace,
track, handlers, parameters=...)` does that for the application
([issue #264](https://github.com/muxinc/relay-sdks/issues/264)). Implemented in both SDKs, on the
primitive subscriber. A supervised want moves the same way when it follows a newer generation or
is wanted again with another track or parameters ([moves](#supervised-subscriber)).

- One subscription at a time is **active**, and only its events reach the handlers. Each handler
  gets the active track's name as its last argument. Every subscription underneath is an ordinary
  subscription with the lifecycle above. The handle has no `update()`.
- `switchTo(track, { parameters, until, drainMs, signal })` / `switch_to(track, parameters=...,
  until=..., drain=..., cancellation=...)` subscribes to `track` in the same namespace. The track
  may be the active one, which is how to change parameters. `parameters` are not inherited: omitted
  means none. The new subscription is **pending**, and the active one keeps delivering. The
  arguments, the connection and the switchable's own state are checked at once. A failed check
  leaves any pending switch alone.
- **Cutover.** Each object of the pending subscription is offered to `until`. `'first-object'`
  (the default) accepts the first object. A synchronous predicate accepts each object it returns
  true for. Objects it refuses, and the pending subscription's other events, are discarded, with
  one exception. Its objects, subgroup ends and resets are held (the latest 256), and those that come
  after the accepted object in its own subscription follow it. Those are its later groups, and
  its group's later objects, subgroup ends and resets. Across relays a later group can arrive before the object that cuts
  over, and the predicate refuses it as too far ahead ([issue #282](https://github.com/muxinc/relay-sdks/issues/282)).
  On the first accepted object:
  1. the old subscription is cancelled;
  2. `onSwitched` / `on_switched` runs with the old track, the new track and the new
     subscription's SUBSCRIBE_OK;
  3. the accepted object is delivered, then the held events that come after it, in arrival order.

  `switchTo` resolves with the same record.
- **Avoiding a gap needs a timeline.** The SDK knows no media time, and group IDs are not one.
  Each track numbers its own groups, and the SDK's publisher counts them from the track's first
  sent group, so layers that gained demand at different times disagree. An application that must
  not skip media supplies a predicate. It compares the object's own timeline (a timestamp property,
  a frame number) with what the active subscription delivered, and accepts an object within a few
  frames of it. The same check refuses a stale first object. The relay starts a subscription at its
  cached latest group, and that group is old if the publisher skipped the track while nobody watched
  it. A subscriber on another relay can also lose the first object altogether
  ([issue #243](https://github.com/muxinc/relay-sdks/issues/243)).
- **Drain.** Groups arrive out of order across streams, especially through another relay, so an
  older group of the old track can still be in flight at the cutover. `drainMs` / `drain` (default
  0) keeps the old subscription open for that long. It takes 0 up to the bound every SDK deadline
  has (2147483647 ms, 2147483.647 s), the longest a JavaScript timer holds; beyond it, or negative,
  is a `RangeError` / `ValueError`, and a non-number a `TypeError`. While it drains, its objects, subgroup ends
  and resets still reach the handlers, with the old track's name, if they belong to a group no
  later than the largest it had delivered. Its later groups and other events are discarded. When
  the time is up, it is cancelled. An old subscription that had delivered nothing is cancelled at
  once.
- **A failed switch leaves the active subscription delivering.** A switch fails, reaching no
  handler, when:
  - the pending subscription ends before its cutover: it fails as `subscribed` would;
  - a newer `switchTo` supersedes it: `CLOSED`;
  - the signal or token cancels it: the signal's or token's reason;
  - the predicate throws: that error.
- **Endings.**
  - `subscribed` settles with the SUBSCRIBE_OK of the first subscription to become active. If the
    switchable ends before that, it fails as the first subscription's `subscribed` does.
  - `ended` settles with that subscription's `SubscriptionEnd` when the active subscription ends
    other than by a cutover, or on `cancel()`. A REQUEST_ERROR ending also reaches `onError` with
    the track.
  - Any pending switch then fails, with `CLOSED` or with the connection error if the connection
    ended. Any drain stops.
  - Publisher completion is not an ending, as for any subscription.
  - `switchTo` on an ended switchable raises `CLOSED`.
- **Measured** on moq-relay 0.17.0 at `*.relay.mux.global` (2026-10-06). The test switched between
  two layers written frame for frame every 50 ms, with a frame-window predicate and a 1 s drain. The
  cutover came about 140 ms after `switchTo` with publisher and subscriber on one relay (sjc), and
  about 1 to 1.6 s with the subscriber on another (fra). The live smoke test asserts that no frame
  is missing across the cutover, in both runtimes of each SDK. Without the predicate and drain, a
  `'first-object'` switch skipped a frame in most cross-relay runs.

## Discovery

A subscriber can learn which namespaces under a prefix are announced, without an application
channel of its own. `subscriber.namespaceEvents({ prefix, capacity, signal })` in TS and
`subscriber.namespace_events(prefix, capacity=..., cancellation=...)` in Python send one
SUBSCRIBE_NAMESPACE for the prefix. They return a bounded iterator with the same ownership,
capacity, overflow and cancellation rules as [async subscription consumption](#async-subscription-consumption).
The iterator owns the request, never the connection.

- Each event is `announced` or `unannounced`, with the full namespace: the prefix's fields followed
  by the rest, sent verbatim. The relay names each namespace relative to the prefix, and the SDK
  joins the two. A namespace equal to the prefix is reported as the prefix itself.
- The events are edge-triggered per namespace. Draft-22 lets a relay name a namespace again, and
  moq-relay does when its route changes. Those repeats are dropped, so an application that tracks
  the `announced` set holds the relay's current view. A NAMESPACE_DONE for a namespace the relay
  never named closes the connection with `PROTOCOL_VIOLATION`, as draft-22 §9.15 requires.
- The prefix follows the namespace argument rules (1 to 32 non-empty fields), checked before
  connection state. Draft-22 also allows an empty prefix, meaning every namespace; the SDK does
  not. The prefix matches whole fields: `['room', 'a']` does not cover `['room', 'ab']`.
- One connection discovers each prefix tree once. Draft-22 §9.15 has a relay answer a
  SUBSCRIBE_NAMESPACE whose prefix overlaps one already open on the session (equal, shorter or
  longer, field by field) with PREFIX_OVERLAP. moq-relay 0.17.0 does not enforce this, so the SDK
  refuses such a prefix itself, with `PREFIX_OVERLAP` in phase `subscription`, before anything is
  sent, and behaves the same against every relay. The prefix is free again once its iterator has
  ended. To watch both `['room']` and `['room', 'a']`, discover `['room']` and filter.
- The subscriber connection's SETUP carries the SOLICIT option (`0x40B5A` = 1,
  draft-lcurley-moq-solicit-00), so the relay advertises namespaces only on these requests. Without
  it, moq-relay 0.17.0 pushes an unsolicited PUBLISH_NAMESPACE for everything in scope right after
  SETUP, and a SUBSCRIBE_NAMESPACE stream then carries nothing. The publisher connection does not
  send SOLICIT: it never asks for namespaces.
- The relay sends no marker between the initial snapshot and later changes. Measured on moq-relay
  0.17.0 at `*.relay.mux.global` (2026-10-06):

  | What | Same relay (sjc) | Subscriber on another relay (fra) |
  | --- | --- | --- |
  | Already-announced namespaces arrive after the request | ~50 ms | ~110 ms |
  | A new announce arrives after the publisher's `announced` settles | ~50 ms | ~140 ms |
  | `unannounced` after the publisher's clean `close()` | ~50 to 300 ms | ~150 to 400 ms |
  | `unannounced` after the publisher process is killed | ~12 s (QUIC idle timeout) | ~12 s |

  An announce learned from a cluster peer looks the same as a local one. Nothing in the event says
  which relay holds the publisher.
- A prefix the token does not cover is accepted and stays silent; the relay does not reject it.
  An event is about the relay's routing table, not a publisher's health: `announced` does not mean
  any track has content, and a namespace two publishers announce
  ([duplicate announce](relay-interop.md#duplicate-announce)) is reported once.
- Discovery is how a subscriber avoids the [announce race](#subscribing). A subscribe sent on the
  same connection once `announced` arrives was never rejected `unroutable` on moq-relay 0.17.0
  (0 of 160 trials on 2026-10-06, both SDKs, same relay and another relay), while one sent as soon
  as the publisher's `announced` settled was rejected 6 to 14 of 40 times on the same relay and
  40 of 40 on another ([announce propagation](relay-interop.md#announce-propagation)). This is an
  observation of that relay, not a protocol guarantee. A subscriber that waits for `announced` needs
  no timed retry for the race. A later `unroutable` means the namespace has gone since; `unannounced`
  follows, and the next `announced` is the cue to subscribe again. `not found` still means the track
  is not there.
- Endings: a REQUEST_ERROR, the request timeout, or the request's stream failing raises
  `SubscriptionRequestError`. A stream that fails without a REQUEST_ERROR first waits up to the
  same closure grace as a subscription. The connection ending raises
  `SubscriptionConnectionClosedError`, a full queue `SubscriptionOverflowError`, and the signal or
  cancellation its reason. A relay that finishes the request cleanly ends the iterator without an
  error, after `unannounced` for every namespace still announced (draft-22 §9.15 treats the end
  of the response stream as a withdrawal of each), and after any events still queued, which a
  clean end does not discard. After an error ending, treat every namespace as withdrawn. A GOAWAY
  on the request stream is not surfaced; the connection's GOAWAY ends the connection.

## Supervised connections

The primitives above never reconnect or retry. Applications can opt into a supervision layer
built only on them: `connectSupervisedPublisher` / `connect_supervised_publisher` and
`connectSupervisedSubscriber` / `connect_supervised_subscriber`. It owns the recovery policy in
[publisher recovery](publisher-recovery.md), so live applications don't each write it again
([issue #260](https://github.com/muxinc/relay-sdks/issues/260)). Neither supervisor changes what
the primitives do. Python gives the backoff bounds and `stale_after` in seconds, and its want
handlers take the namespace as a second argument.

Both supervisors:

- Resolve once their first connection is up. A first attempt that fails on its arguments, or
  because the relay refused the credentials (`UNAUTHORIZED`), rejects with that failure. Any other
  failure is retried with backoff, bounded only by the cancellation primitive, whose abort rejects
  with its reason. After the first connection, every ending is retried, `UNAUTHORIZED` included,
  since the token provider runs again on each attempt.
- Reconnect whenever the connection ends without the application closing it: error, GOAWAY, or a
  clean close by the relay. The delay starts at 500 ms and doubles up to 10 s. Each delay is drawn
  from the upper half of its ceiling (jitter), and the backoff starts over after a connection that
  stayed up longer than the longest delay. Both bounds are options.
- Report `connecting`, `live`, `reconnecting` and `closed` through a status callback, with the
  error that ended the previous connection or failed the last attempt. `closed` settles once the
  supervisor stops: `close()` (no error), the cancellation primitive (its reason), or a first
  attempt that is not retried (that failure). It never rejects. A [rotation](#rotation) reports
  nothing: the status stays `live`.
- Replace their connection before its credential expires, make-before-break ([rotation](#rotation)).

### Rotation

A rotation replaces a supervisor's live connection make-before-break, so that no connection reaches
its credential's expiry ([credential lifetime](#credential-lifetime)) and viewers see a switch
instead of a drop. It is not a reconnect: the old connection keeps serving until the new one has
taken over.

- **When.** `rotateBeforeMs` / `rotate_before` (default 60 s) before the live connection's
  `expiresAt` / `expires_at`, but never earlier than halfway through the time left when it
  connected, so a credential shorter than the margin still serves half its life. A connection whose
  expiry is unknown never rotates on its own. `rotate()` rotates now, for an application that
  revokes credentials or wants a fresh token. It is a no-op while no connection is up.
- **The replacement** connects with the same options, so the token provider runs again. A failed
  attempt is retried with the reconnection backoff while the old connection lives; if the old one
  ends first, the supervisor reconnects as after any other ending. When a rotation the supervisor
  started on its own gets a credential that expires no later than the live one, it discards the
  replacement and lets the live connection run to its end: two connections the relay ends together
  replace nothing. `rotate()` always takes the replacement.
- **Overlap.** The old connection closes once everything has moved to the new one, after 10 s at
  most, or sooner if it ends on its own.
- **Publisher.** The replacement is the next generation. Every registered track is registered on it,
  and `onGeneration` / `on_generation` runs for it once it is up; `generation` names it from then on.
  Each track's writer keeps writing to the old generation until the track has demand on the new one
  or has none on the old one, and then moves at the start of its next write or group, never under
  one: a group opened on the old generation finishes there. Subscribers following generations move
  to the new one when it is announced, which creates its demand. `hasDemand` and `demandChanges()`
  follow the generation the writer is on, and report no `false` when demand carries over. They report
  `true` again on the move, as they do when the relay replaces its upstream subscription, so state an
  application writes on demand (a catalog) reaches the new generation. When the
  overlap runs out, every track moves regardless of demand, and the old generation closes as
  `close()` closes it.
- **Subscriber.** The replacement is a new connection, and every want **moves** its subscription to
  it make-before-break, as a [move](#supervised-subscriber) does: the replacement opens pending on
  the new connection while the old subscription keeps delivering. A want following generations
  discovers the root on the new connection first. Then:
  - **Same target** (namespace, track and parameters): the same publisher's stream continues. The
    replacement starts at the latest group, so its objects at or before the last location the old
    subscription delivered are dropped, and the first one after it cuts over without `until`. No
    callback marks it, no `onSwitched`, `onRestarted` or `onSubscribed`, and nothing is lost or
    repeated, so decoders keep their state. The old subscription is cancelled, not drained. Once
    any of the want's subscriptions has heard `PUBLISH_DONE`, nothing is dropped: a restarted
    publisher numbers its groups from 0 again.
  - **Another target**, a newer generation announced on the new connection or a re-want: an
    ordinary move, with `until`, the drain and `onSwitched`.
  - The old connection closing under the old subscription leaves the replacement to carry on, and
    a replacement refused or ended before its cutover is retried after the retry backoff, as a
    move's is. The new connection ending restarts the want with `connection`. A want with no
    accepted subscription starts over on the new connection at once. A rotation to a connection
    that is itself rotated away before the cutover moves on to the newest one.

### Supervised publisher

- Each connection is a **generation**. It serves the application's root namespace followed by one
  generation field the SDK chooses: the start time in fixed-width base 36, then four random
  characters. Generations therefore sort by age as strings. The token must grant the root. The
  root plus one field must satisfy the namespace rules, checked when the supervisor starts.
  `generation` exposes the live generation's field and namespace, and a callback runs once each
  generation is up and every track is bound to it.
- A fresh namespace per connection is what `publisher-recovery.md` recommends for predictable
  handoff on moq-relay 0.17.0: a reconnect on the same namespace is a new publisher to the relay,
  and duplicate announces are ranked arbitrarily ([duplicate announce](relay-interop.md#duplicate-announce)).
  If the relay later supports taking over a route, the same API can keep one namespace across
  reconnects without applications changing.
- `registerTrack` checks its options at once, under the same rules as the primitive's, and returns
  a writer that outlives the generations. The supervisor registers the track again on every
  generation. Track names are unique per supervised publisher, including after their writer
  closes. A track closed by the application is never registered again.
- **Writes are never replayed.** While no generation is up, `write`, `writeText`, `writeJSON` and
  `openGroup` fail at once with `RECONNECTING` in phase `connection`, and `hasDemand` is false. An
  operation that the connection's end interrupts also fails with `RECONNECTING`, never `CLOSED`.
  A frame writer therefore treats it as an [interruption](../CONTEXT.md), not a retirement, and
  resumes at the next keyframe on the next generation. The writer's `closed` is true only once the
  application closed the track or the supervisor stopped.
- `demandChanges()` spans generations. It reports the current state first, `false` when a
  generation that had demand is lost, and the next generation's changes after it. It ends when
  the track closes.
- `close()` stops reconnecting and closes the live generation, as the primitive's `close()` does.

### Supervised subscriber

- `want(key, options, handlers)` keeps a subscription to one track open until `unwant(key)`.
  Arguments are checked at once. Wanting a key again with the same namespace and
  generation-following updates the want in place. The handlers, the retry backoff, the stale
  threshold, `until` and the drain are swapped. Another track or other parameters **move** the
  subscription (below). Another namespace or generation-following replaces the want.
- Each want runs one subscription at a time on the live connection, two while a move is pending:
  - **Retried at once**, without backoff: the connection ended (the want moves to the replacement
    connection), the followed generation was withdrawn with none newer, or a move came before the
    subscription was accepted.
  - **Retried after backoff** (from 250 ms, doubling up to 8 s, with jitter): the announce race
    (16 `unroutable`, or the older 404), every other rejection not listed below, `PUBLISH_DONE`,
    a REQUEST_ERROR after acceptance, and a subscription that went stale. Only an object from a
    later group than the subscription's first resets the backoff. Cached objects followed by a
    done are not progress, so a finished track still announced is replayed no faster than the
    backoff allows ([issue #21](https://github.com/muxinc/relay-sdks/issues/21)).
  - **Surfaced, after which the want stops and is forgotten:** a rejection that no wait fixes.
    These are REQUEST_ERROR 0x1 `UNAUTHORIZED`, 0x3 `NOT_SUPPORTED`, 0x4 `MALFORMED_AUTH_TOKEN`,
    0x5 `EXPIRED_AUTH_TOKEN`, 0x11 `INVALID_RANGE` and 0x12 `MALFORMED_TRACK`, reported as
    `SubscriptionRequestError`. Discovery refusing the root is surfaced the same way, and so is
    `PREFIX_OVERLAP` between two followed roots on one connection.
- `staleAfterMs` / `stale_after` resubscribes when no object arrived for that long, measured from
  acceptance or the last object. It is for tracks that are never silent while live: after a
  publisher crash, moq-relay 0.17.0 can hold an accepted subscription on the dead publisher's
  cached object for about 11 s. Without it, silence is not a failure.
- **Following generations.** With `generations: true`, the want's namespace is a supervised
  publisher's root. The supervisor discovers the root ([discovery](#discovery)) and subscribes on
  the newest generation announced one field below it. Deeper namespaces are ignored. One discovery
  per root per connection is shared by every want on it. The want moves as soon as a newer
  generation is announced, even while the old one is still announced, as a crashed publisher's is
  until the relay's idle timeout. It also moves when the current generation is withdrawn and an
  older one is still announced. With none announced, it waits. No application channel is needed.
- **Moves are made before the break** ([issue #282](https://github.com/muxinc/relay-sdks/issues/282)),
  as on a [switchable subscription](#switching-tracks). A move is a newer generation, or a re-want
  with another track or parameters. Once the current subscription has been accepted, the want opens
  the replacement as **pending** while the current one keeps delivering:
  - Each object of the pending subscription is offered to `until` (`WantOptions.until` /
    `until=`). `'first-object'`, the default, accepts the first. A synchronous predicate accepts
    each object it returns true for. The objects it refuses are discarded, except those that come
    after the accepted object, which follow it as on a switchable. A new generation's
    namespace is fresh, so its first group is its publisher's first. The first-object default is
    never worse than a break, and an application with a timeline passes a predicate that compares
    it. Group IDs are not a shared timeline: each generation and track numbers its own groups. A
    predicate that throws is logged and accepts the object, so application code never stalls a
    move.
  - **Cutover.** On the first accepted object, the old subscription is cancelled, or drains for
    `drainMs` / `drain` (default 0) as a switchable's does. Then `onSwitched` / `on_switched` runs
    with the old and new namespace and track and the new SUBSCRIBE_OK, and then the accepted
    object is delivered. A move runs neither `onRestarted` nor `onSubscribed`.
  - **The old subscription ends first** (`PUBLISH_DONE`, as when a supervised publisher replaces
    its connection, a REQUEST_ERROR, or the stale threshold). The pending subscription carries on
    and cuts over on its next object, whatever `until` says, since there is nothing left to
    compare. `onPublisherDone` still runs for the old one. The stale threshold counts objects from
    either subscription. When it passes during a move, only the old subscription ends, and the
    replacement gets a full threshold of its own before the want restarts with `stale`.
  - **The pending subscription fails before its cutover.** A rejection no wait fixes is surfaced,
    and the want stops, unless the key was wanted again with another target first. Then the want
    reports `onRestarted` for the subscription it cancelled (its own ending's reason, else
    `terminated`), and tries the new target at once. Any other failure leaves the current subscription delivering, and the move
    is retried after the retry backoff, sooner if the want asks for something else. If the old
    subscription had already ended, `onRestarted` reports its reason instead, and the want
    resubscribes as after any restart.
  - A newer move supersedes a pending one. A re-want back to the current track and parameters
    cancels the pending one. A move before the current subscription is accepted replaces it at
    once, since there is nothing to overlap. The connection's end takes both subscriptions, and
    the want restarts with `connection`.
- **Catch-up** (TypeScript only). `catchUp(key)` replaces the want's accepted subscription at once,
  break-before-make: it cancels it, which makes the relay drop what it still holds queued for it,
  and subscribes again with the want's options, so a want with no location filter starts at the
  newest group. `onRestarted` reports `catch-up`. The replacement opens without delay when the
  cancelled subscription had progressed (delivered an object from a later group than its first),
  and otherwise after the retry backoff, so a link too slow for the track cannot make it spin. It
  is ignored for an unknown key and while no subscription is accepted; during a move it ends both
  subscriptions, and the replacement is made for whatever the want asks for then. It exists
  because moq-relay 0.17.5 (measured) and 0.17.7 (read in source) drop nothing for a late
  subscriber, whatever delivery timeout the subscriber sends
  ([relay-interop.md](relay-interop.md#voice-delivery-on-0175-deadlines-fetch-and-group-order)). It
  is meant to work with a relay that honours that timeout too
  ([moq-dev/moq#5881](https://github.com/moq-dev/moq/issues/5881)): such a relay drops the late
  groups itself, and a catch-up then clears only what still arrives late.
  A frame reader's `onCatchUp` is its usual caller ([media subscribing](#media-subscribing)).
  Measured on 2026-10-17 against a local moq-relay 0.17.5, with a 3 s outage on a 600 kbit/s link
  carrying a 400 kbit/s audio track (`ts/scripts/probe-catch-up.mjs`, two runs each): without
  catch-up, the listener got 401 and 407 of 1,200 frames: once delivery resumed (about 5 s after
  the link came back, as QUIC recovered), it got a few frames a second, about 800 ms late, to the end
  of the run 8 s later. With `catchUpAfterUs` 300 ms and a late-drop limit of 100 ms, one catch-up
  followed the resumption, the listener got 796 and 801 of 1,200 frames, and latency was back at
  21 ms within a second.
- Handlers receive the current subscription's objects, subgroup endings and resets, and its
  `PUBLISH_DONE`. `onSubscribed` receives the namespace the subscription was made on: the
  generation's, when following one. `onRestarted({ reason, namespace })` runs when an accepted
  subscription ends and will be replaced, before any object of the replacement. Its reason is
  `publisher-done`, `stale`, `generation`, `connection` or `terminated`, or in TypeScript `catch-up`. `generation` remains for
  a withdrawal with none newer announced. Decoders drop state that assumed continuity there, and at
  `onSwitched`. Rejections before acceptance, the announce race included, never reach
  a handler unless they are surfaced.
- **Nothing is fetched to fill a gap.** A replacement subscription after a restart starts live with
  the want's parameters, so objects published between two subscriptions are not delivered. A move
  leaves no gap only as far as its `until` and drain allow. Trailing objects
  after a `PUBLISH_DONE` are discarded when the subscription is cancelled for its replacement.
- `close()` cancels every want and closes the connection.

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
  ([draft-22 Section 11.3.2](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#section-11.3.2)).
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

[Draft-22 Section 5.2](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#section-5.2) gives each group two delivery timeouts, in milliseconds:
SUBGROUP_DELIVERY_TIMEOUT (`0x06`) bounds how long a group's FIN may wait for acknowledgment, and
OBJECT_DELIVERY_TIMEOUT (`0x02`) bounds how old an object may be when it reaches the transport.
There are no options for them: an application sets the publisher's values with the track and object
properties it already passes.

- **The value.** Each group's value is fixed just before its stream opens, after any wait for
  settling room; for a group opened without its first object, the publisher's side is fixed when
  that object is written. The publisher's is the property on the group's first object when present,
  and the track property otherwise; on any later object the property is ignored. The subscriber's is the
  parameter on the relay subscription's SUBSCRIBE, as updated by any REQUEST_UPDATE so far. When
  both are non-zero the smaller applies, and otherwise the non-zero one; 0 means no timeout. A value
  above 2147483647 ms is clamped to that bound, and registration refuses neither 0 nor a clamped
  value. Both types are even, so their values are integers: the [Arguments](#arguments) table
  already refuses bytes there. A REQUEST_UPDATE carrying either parameter is acknowledged with
  REQUEST_OK, and the new value applies to groups whose stream opens after it; groups already open
  or settling keep theirs.
- **The subgroup timeout** starts when the group's FIN is handed to the transport, the moment its
  operation completes and it becomes a settling group. If the relay has not acknowledged the FIN
  when it expires, the stream is reset with `DELIVERY_TIMEOUT` (0x2), as Section 5.2 requires before
  the stream reaches "all data committed". Nothing is reported to the track: the group leaves the
  settling set like any other late failure and frees its place under the 64-group limit. The timer
  belongs to the group, so it keeps running through a relay subscription change or end and through
  the track's PUBLISH_DONE; only the connection's end stops it.
- **The object timeout** counts from the call that hands the object over (`write()`, `openGroup()`
  / `open_group()` given the group's first object, or the group's `write()`) to the moment the
  transport takes the object, so waiting for demand, for settling room, for the stream and behind
  the group's earlier bytes all count. Section 5.2 starts the clock when the application provides
  the object, so a bare `openGroup()` has none to time: only the write deadline bounds its wait for
  demand, settling room and the stream. Opening a group with its first object (`payload`, with
  optional `properties`) puts that wait on the object's clock, and its `0x02` property is known
  before the stream opens. The call returns once the transport takes the object, with
  `objectCount` / `object_count` at 1, or fails like a `write()` and returns no group
  ([#306](https://github.com/muxinc/relay-sdks/issues/306)). Properties without a payload are a
  TypeError. The age is checked again
  just before the object is handed over, as Section 5.2 requires. Until the group's value is fixed
  only the publisher's value times the wait; an object already older than the fixed value fails
  then, before any stream opens. When it expires the operation fails with `DELIVERY_TIMEOUT`
  in its current phase, and a stream already open is reset with code 0x2. In a group the whole
  group is reset and its later writes fail with the same error; no stream is opened again for it
  (Section 5.2: SHOULD NOT). The track stays open and the next write uses a new group ID, as after
  any interruption. The object timeout runs alongside the write deadline: whichever expires first
  fails the write, with `DELIVERY_TIMEOUT` or `WRITE_TIMEOUT`. Once the transport has taken an
  object, its bytes cannot be timed out. In TypeScript the transport has taken an object when its
  stream write resolves, so an object queued behind stream flow control is still timed; in Python,
  when the stream write returns, once aioquic has transmitted the bytes.
- **On moq-relay 0.15.8, 0.17.0 and 0.17.5** (and 0.17.7, read in source) neither value has any
  effect beyond the publisher.
  A subscriber's values are sent on SUBSCRIBE as `parameters.objectDeliveryTimeout` /
  `subgroupDeliveryTimeout` (`object_delivery_timeout` / `subgroup_delivery_timeout`) and the relay
  drops nothing for them: measured on 0.17.5, a listener with a 100 ms timeout behind a link too
  slow for the track received every group, up to 11 s late
  ([relay-interop.md](relay-interop.md#voice-delivery-on-0175-deadlines-fetch-and-group-order)). A
  late listener's remedies are the sequencer's late drop and catch-up
  ([media subscribing](#media-subscribing)). The relay
  never sends either parameter upstream: its SUBSCRIBE carries only `0x10`, `0x20`, `0x21`, `0x22`
  and `0x23`, plus `0x35` only to opt out of properties, which the relay never does
  ([subscribe.rs](https://github.com/moq-dev/moq/blob/cd59d15b07c2c2c8a5bfa8a2e82b3bbc24e82a76/rs/moq-net/src/ietf/subscribe.rs#L187-L199)), its REQUEST_UPDATE only `0x10`, `0x20` and `0x21`,
  and it states that it never negotiates the Section 5.2 parameter ([error.rs](https://github.com/moq-dev/moq/blob/cd59d15b07c2c2c8a5bfa8a2e82b3bbc24e82a76/rs/moq-net/src/ietf/error.rs#L85)).
  It drops both track properties and the `0x02` object property, and reads an object's `0x06` as a
  Timestamp ([Relay property handling](#relay-property-handling)), so a first-object `0x06` reaches
  viewers as a Timestamp. In production the publisher's own values are therefore the only ones that
  apply, and a track property is the safer way to set the subgroup timeout. The reverse also holds:
  a publisher that still sends timestamps as `0x06` (the draft-ietf-moq-loc-03 id) now gives each
  group a subgroup timeout equal to its first object's timestamp. Applications must not send a
  timestamp as `0x06`; LOC Timestamp is `0x10`. Evidence:
  [issue #167](https://github.com/muxinc/relay-sdks/issues/167).
- **Defaults** belong to the media tier, not the write slot: `registerTrack` sets none, and
  [media publishing](#media-publishing) gives media tracks a subgroup timeout.

Implemented in both SDKs.

### Write phases and interruptions

Every write and every open group is one operation occupying the track's write slot (see
`CONTEXT.md`). The operation records its phase before each step, and this table is the shared test
list both SDKs implement: what the caller sees, what reaches the wire, and what state the track is
left in. The Source column separates what the protocol rules from SDK policy. Section numbers refer
to [draft-ietf-moq-transport-22](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html)
unless stated otherwise.

| Phase | Interruption | Reported to the caller | Wire effect | Track afterwards | Source |
| --- | --- | --- | --- | --- | --- |
| waiting-for-demand | write deadline expires | `WRITE_TIMEOUT`, or `NAMESPACE_NOT_SOLICITED` when solicitations arrived but none covers the namespace; phase `waiting-for-demand` | nothing sent | open; the next write waits for demand again | SDK policy: the protocol has no timer for awaiting a subscription (§15.6 only warns of idle timeout) |
| waiting-for-demand | object timeout expires: the publisher's value, the only one that applies before the group's value is fixed | `DELIVERY_TIMEOUT`, phase `waiting-for-demand` | nothing sent | open; the next write waits for demand again | §5.2: an object's age counts from when the application provided it, and is checked before it reaches the transport |
| waiting-for-demand | relay subscription ends while waiting | the wait continues until the deadline | nothing sent | open | SDK policy |
| waiting-for-demand | caller cancels | the caller's reason, unwrapped; phase `waiting-for-demand` | nothing sent | open | SDK policy |
| waiting-for-demand | track closed | `CLOSED`, phase `waiting-for-demand` | nothing sent | ended | SDK policy |
| waiting-for-demand | connection ends | the connection's own error (phase `connection`), or `CLOSED` after a clean shutdown | nothing sent | ended | §6.6: streams and subscriptions end with the session |
| opening-stream | write deadline expires or caller cancels | `WRITE_TIMEOUT` or the caller's reason; phase `opening-stream` | a stream that opens after the failure is reset before any object is written, code `DELIVERY_TIMEOUT` (0x2) or `CANCELLED` (0x1); its header may precede the reset (RESET_STREAM_AT) | open; the next write uses a new group ID | §11.3.2: a stream closed before all objects are delivered MUST be reset, and the reset SHOULD include the header; the deadline itself is SDK policy |
| opening-stream | object timeout expires, while waiting for settling room or for the stream, or the object is already older than the value fixed just before the stream opens | `DELIVERY_TIMEOUT`, phase `opening-stream` | no stream opens, or a stream that opens after the failure is reset before any object is written, code `DELIVERY_TIMEOUT` (0x2) | open; the next write uses a new group ID | §5.2 MUST reset with `DELIVERY_TIMEOUT`; §11.3.2 |
| writing | object timeout expires before the transport takes the object | `DELIVERY_TIMEOUT`, phase `writing`; a group's earlier objects may have been delivered. Never `closing`: the object is with the transport by then | stream reset, code `DELIVERY_TIMEOUT` (0x2); the group's later writes and `close()` fail with the same error, and no stream is opened again for it | open; the next write uses a new group ID | §5.2: MUST reset with `DELIVERY_TIMEOUT`, SHOULD NOT open a new stream for the subgroup |
| writing, closing | write deadline expires | `WRITE_TIMEOUT`, phase `writing` or `closing`; object bytes may have been delivered, and `closing` can fail after all of them were. In TypeScript `closing` can only fail before the FIN is handed to the transport; Python's `closing` can still wait on transport backpressure | stream reset, code `DELIVERY_TIMEOUT` (0x2) | open; the next write uses a new group ID | §11.3.2 MUST reset; §5.2 and §16.11.4 for the code; the deadline is SDK policy |
| opening-stream, writing, closing | caller cancels (AbortSignal, task cancellation, or `group.abort()`) | the caller's reason unwrapped, or `CLOSED` when `abort()` gave none; the current phase | stream reset, code `CANCELLED` (0x1) | open | §11.3.2 MUST reset; code choice is SDK policy (§12.5 SHOULD be relevant) |
| opening-stream, writing, closing | the transport refuses the stream or fails a write, other than the relay stopping the operation's own stream | `TRANSPORT_ERROR` with the cause preserved; the current phase | stream reset, code `INTERNAL_ERROR` (0x0), when a stream exists | open | SDK policy |
| opening-stream, writing, closing | relay subscription replaced or ended, or the relay stopped the operation's stream (STOP_SENDING, §11.3.2) | `SUBSCRIPTION_CHANGED`; the current phase; after a stop the transport error is kept as the cause. The same code whichever of the stop and the subscription change arrives first, and nothing further is reported for the operation | stream reset, code `CANCELLED` (0x1); after a stop the QUIC stack has already answered with its own RESET_STREAM (RFC 9000 §3.5) | open; the next write waits for new demand only if the relay subscription ended, and proceeds at once while it is still live | §3.1.2: the publisher MUST reset open streams of a cancelled subscription; §11.3.2: a subscriber MAY stop a subgroup stream whose group is no longer of interest, which does not by itself end the subscription; treating a new SUBSCRIBE as a replacement rather than a concurrent subscription is SDK policy (§3.1, ADR-0001) |
| opening-stream, writing, closing | track closed | `CLOSED`; the current phase | stream reset, code `CANCELLED` (0x1) | ended | §9.9: every stream is closed or reset before the track ends |
| opening-stream, writing, closing | connection ends | the connection's own error (phase `connection`), or `CLOSED` | streams end with the session; a reset that is still sent carries `SESSION_CLOSED` (0x3), or `GOING_AWAY` (0x4) after GOAWAY | ended | §12.2; §16.11.4 for the codes |
| none: refused before an operation starts | a write or group on a track that already ended, or a write on a group already closed with its FIN | `CLOSED`, phase `waiting-for-demand` | nothing sent | unchanged | SDK policy: the phase that means nothing was sent |
| any | a second write or group is started | `WRITE_PENDING`, phase `writing`; the active operation is unaffected | nothing sent | unchanged | SDK policy: one pending operation per track |
| after the FIN is handed over (settling group) | subgroup timeout expires before the relay acknowledges the FIN | nothing; the operation had completed | stream reset, code `DELIVERY_TIMEOUT` (0x2); the group leaves the settling set | open, whether or not the subscription or the track has ended since | §5.2: MUST reset the stream when the timer expires before all data is acknowledged |
| after a clean group close | `abort()` | nothing; `closed` stays true and a repeated `close()` returns | nothing sent | open | §11.3.2: a reset after all data and the FIN is ignorable |

A failed group's ID is never reused, which is stricter than §2.2 (a subgroup may resume on a new
stream after a premature reset); group IDs increase monotonically and object IDs start at 0 within a
group (draft-ietf-moq-msf-01 §6). A group may end without an `END_OF_GROUP` marker (§12.1). There
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
  This departs from LOC, which defines a Timestamp without a Timescale as wall-clock microseconds
  since the Unix epoch. Such a stamp from a publisher does not survive this relay.
- `SUBSCRIBE_OK` carries the relay's own track properties for every track: Timescale (`0x08`) =
  1,000,000, Default Publisher Priority (`0x0E`) = 128, and Default Publisher Group Order (`0x22`,
  decimal 34) = 2 (descending). A publisher's own `0x0E` passes through, its `0x22` is replaced
  with 2, and its other track properties (for example `0x0D`, `0x40`, `0x41`) are dropped.
- A track registered with a Default Publisher Priority (`0x0E`) above 255 or a Default Publisher
  Group Order (`0x22`) other than 1 or 2 makes the relay refuse **every** subscribe to it with
  REQUEST_ERROR 18 `invalid value`. The publisher gets no error; its writes time out waiting for
  demand. Draft-22 rules both values out (§10.4, §10.5), so the SDKs refuse them at registration
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
objects. To sync a broadcast's tracks, stamp them all from one clock; that convention, and the
player's playout clock, are in
[media-layer.md, Shared timeline and playout clock](media-layer.md#shared-timeline-and-playout-clock). Interpret received timestamps using the received track timescale; a timestamp's presence
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
- The LOC timestamp reader (`locTimestampUs` / `loc_timestamp_us`, issue #290) returns an object's
  Timestamp (0x10) in microseconds, or nothing when the object has no Timestamp that is a
  nonnegative safe integer. It scales by the first Timescale that is a positive safe integer,
  looking at the object's Timescale (0x08), then the track properties' (as `SUBSCRIBE_OK` carries
  them), and skipping any other value. It never reads the catalog's `timescale`: msf-01 §5.2.21
  does not tie that field to the LOC Timestamp. The result is
  `round(value × 1,000,000 / timescale)`, ties upward. With no Timescale it is the value itself,
  LOC's wall-clock microseconds. A result past 2^53−1 reads as nothing. Pinned by `vectors/media.json`
  `locTimestamps`.
- The independent MSF catalog is compact UTF-8 JSON with `version: "draft-01"` and explicit
  per-track namespace, LOC packaging, and live status. It omits `isComplete`: msf-01 §5.1.3 forbids
  the field while false and forbids removing it once added. Absent optional fields are
  omitted, as is empty init data. Catalog strings must be valid Unicode, with no lone surrogates.
  Entries are ordered video, audio, then data in caller order. Init-data entries have
  `<track-name>-init` IDs and inline base64 data. Numbers must be finite; integral values must be safe integers and have no decimal suffix.
  Fractional values use ECMAScript JSON number spelling, including its exponent threshold.
- The end-of-broadcast catalog (`buildMsfEndCatalog` / `build_msf_end_catalog`) is msf-01 §11.3's
  final document: `version`, the optional `generatedAt`, `isComplete: true`, and an empty `tracks`
  array, encoded by the same rules. A publisher sends it only when a broadcast ends on purpose,
  after every media track has ended (closing a track sends PUBLISH_DONE Track Ended), and never on
  a crash or a lost connection.
- `avc1` and `avc3` video entries are withheld without non-empty init data. With avcC, the codec
  suffix comes from the record's profile/constraints/level; the `avc1`/`avc3` prefix is retained.
  Header inspection rejects truncated/wrong-version headers; it does not decode SPS semantics.
- Opus always declares 48,000 Hz and defaults to channel configuration `"2"`. Video and audio
  entries carry `renderGroup: 1` (msf-01 §5.2.11): one broadcast timeline, played together. No
  target latency or buffer is set. Data entries have only name and optional role beyond the
  common fields, and reject the media roles `video`,
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
- One timeline, and the playout clock
  ([media-layer.md](media-layer.md#shared-timeline-and-playout-clock), issues #285 and #290): every
  LOC media track one catalog lists is stamped in microseconds since the Unix epoch, its capture
  time on the publisher's system clock, with Timescale 1,000,000 declared on the track. The
  broadcast clock (`createBroadcastClock` / `BroadcastClock`) pins an integer-microsecond origin
  on its monotonic source (`originUs`) to an integer Unix-epoch time (`originEpochUs`). It maps
  times on its own source as `originEpochUs + source − originUs`, rounded to whole microseconds
  with ties upward. It maps a capture clock with another origin and rate (an `AudioContext`,
  Chromium's `MediaStreamTrackProcessor`) through a source timeline:
  `anchor + round(s − anchor s) − captureLatencyUs`. A calibration pair `(s, at)` re-anchors when
  there is no anchor, or when the anchor (without the capture latency) is off by more than
  `toleranceUs` (5 ms). Uncalibrated, the first timestamp anchors at `now()`. The capture latency
  defaults to 0, must be a nonnegative safe integer, and can be changed between timestamps; no
  helper knows a device's latency. It maps sample index `n` as `first + floor(n × 1,000,000 / rate)`. The playout clock
  (`createPlayoutClock` / `PlayoutClock`) is master-audio: its position is the latest audio report
  plus elapsed local time. It paces by wall clock while idle, after `stopAudio` / `stop_audio`,
  and after `audioTimeoutUs` (500 ms) without a report arriving (measured from arrival, not
  from the report's `atUs`). `due` answers `wait` (early by at most
  `maxVideoDelayUs`, 1 s), `show` (late by at most `lateDropUs`, 40 ms), or `drop`. A frame further
  ahead is shown at once. Wall pacing re-anchors on a frame more than `maxVideoDelayUs` early or
  more than the target latency late. The target latency (`targetLatencyUs` / `target_latency_us`)
  is the option when given, a positive number; otherwise twice the longest of the last four
  keyframe intervals reported (`reportKeyframeInterval` / `report_keyframe_interval`, each positive),
  and 2 s before any is. `reset()` keeps the reported intervals and counts itself in `resets`. Both clocks read an injected
  monotonic microsecond source and need no DOM or transport. The decisions are pinned by
  `vectors/clock.json`. Implemented in both SDKs.
- The Media Timeline track (msf-01 §7; [media-layer.md](media-layer.md#media-timeline-track),
  issue #291). The broadcast clock's `originEpochUs` / `origin_epoch_us` is the origin as integer
  Unix-epoch microseconds: as given, or else read once at creation as the system clock
  (`Date.now()`, `time.time_ns()`) less the source time elapsed since the origin. An entry for a
  timeline timestamp `t`, location `(g, o)` (object defaults to 0) and wall-clock time `w`
  (defaults to `t`, the epoch-anchored timeline; 0 means unknown) is
  `[floor(t / 1000), [g, o], floor(w / 1000)]`. Every number must be a nonnegative safe integer.
  The document builder encodes entries in the order given as compact JSON. The reader rejects a
  payload that is not a UTF-8 JSON array (no byte-order mark, no `NaN`/`Infinity`). It reads a
  record whose first three items are a number, an array whose first two items are numbers, and a
  number, all nonnegative safe integers, and ignores extra items. It skips every other record.
  It does not decompress. The catalog builder's
  optional timeline description emits one entry after video and audio and before data:
  `packaging` and `role` `mediatimeline`, `mimetype` `application/json`, `isLive`, and `depends`.
  `depends` defaults to the video track's name followed by the audio track's, whichever exist. Each name must be a
  video or audio track of the same catalog, and the list must be non-empty with no duplicates.
  Names listed but withheld pending init data are left out, and the entry is withheld when none
  remains. The timeline's name defaults to `timeline` and joins the uniqueness check. The catalog
  reader returns `depends` (string items only) and `mimetype` as declared. The timeline accumulator
  (`createMediaTimeline` / `MediaTimeline`) holds one broadcast's entries, each built as the entry
  helper builds it (wall-clock time defaulting to the timestamp), and keeps at most `maxEntries` / `max_entries` (a positive
  safe integer, default 512), dropping the oldest. Each add returns the independent document of the
  retained entries; before the first it is `[]`. A rejected entry leaves the timeline unchanged.
  Retention is pinned by `vectors/media.json`. Cadence (one entry per video group that was sent)
  and the publishing rules are in media-layer.md. Implemented in both SDKs.
- The Opus packet duration (`opusPacketDurationUs` / `opus_packet_duration_us`) reads RFC 6716
  §3.1's TOC byte: the frame length from the configuration (SILK 10, 20, 40 or 60 ms, hybrid 10 or
  20 ms, CELT 2.5, 5, 10 or 20 ms), times one frame for code 0, two for codes 1 and 2, and for code 3
  the low six bits of the next byte. An empty packet, or a code 3 packet without that byte, reads as
  nothing. Pinned by `vectors/media.json` `opusPacketDurations`.
- The interrupt track (`INTERRUPT_TRACK`, `interrupt`; [media-layer.md](media-layer.md#interrupt-track),
  issue #320) carries one interrupt per group as compact UTF-8 JSON: `track` (default `audio`), the
  optional `fromUs`, then `untilUs`, with no `fromUs` key when it is absent. The builder
  (`buildInterrupt` / `build_interrupt`) and the reader (`parseInterrupt` / `parse_interrupt`)
  refuse an empty or non-string track, a track that is not valid Unicode, a timestamp that is not a
  nonnegative safe integer, and a `fromUs` that is not before `untilUs`. The reader refuses a
  payload that is not a UTF-8 JSON object, reads an integral number such as `2e3` as an integer,
  and ignores unknown fields. A listener applies an interrupt for an audio track it plays as a cut
  of that range. Pinned by `vectors/media.json` `interrupts`, `interruptErrors`, `interruptReads`
  and `interruptReadErrors`; the track name by `protocol.json`.
- The transcript track (`TRANSCRIPT_TRACK`, `transcript`; [media-layer.md](media-layer.md#transcript-track),
  issue #320) carries one version of one segment per group as compact UTF-8 JSON: `seq`, `track`
  (default `audio`), `fromUs`, the optional `untilUs`, `text` and `final`, in that order, with no
  `untilUs` key when it is absent. The builder and the reader (`buildTranscriptSegment` /
  `build_transcript_segment`, `parseTranscriptSegment` / `parse_transcript_segment`) refuse a `seq`
  or timestamp that is not a nonnegative safe integer, an empty or non-string track, a track or
  text that is not valid Unicode, a non-string text, a non-boolean `final`, an `untilUs` not after
  `fromUs`, and a final segment without `untilUs`. The reader refuses a payload that is not a UTF-8
  JSON object, reads an integral number as an integer, and ignores unknown fields. A `Transcript`
  keeps each segment's latest version, a final one against any later, and ignores a segment that
  starts inside a range cut earlier. A cut starts at the later of the range's `fromUs` (or 0) and
  the floored playout position, and changes nothing when that is not before `untilUs`; otherwise
  it drops the segments starting in the cut and ends any segment running past its start there,
  final and marked cut. Lines are ordered by `fromUs`, then `seq`; at a position, those with
  `fromUs` at or before it, and none while idle. Pinned by `vectors/media.json`
  `transcriptSegments`, `transcriptSegmentErrors`, `transcriptSegmentReads`,
  `transcriptSegmentReadErrors`, `transcripts` and `transcriptCutErrors`; the track name by
  `protocol.json`.
- Python's Opus encoder and decoder (`moq_media.opus`, the `opus` extra; issue #320, ADR 0006) are
  PyAV's libopus. The encoder takes signed 16-bit PCM, or a PyAV frame, in runs: the first call of a
  run gives the timeline timestamp of its first sample and later calls refuse one, packet *k* of a
  run is stamped at that timestamp plus *k* × 20 ms, and ending the run pads its partial frame with
  silence and drains the encoder. Defaults are voice: mono, 32 kb/s, libopus's `voip` application.
  The decoder returns s16 PCM at the caller's rate and channel count, and resets on request, at a
  discontinuity. TypeScript uses WebCodecs instead and has no counterpart.
- Encoding, pacing, and time-based frame dropping remain application responsibilities. Catalog
  serving, GOP grouping and recovery are the [Media publishing](#media-publishing) policy below, and
  reading frames in decode order within a latency budget is [Media subscribing](#media-subscribing),
  both implemented in both SDKs; a `register_media` publishing handle is not implemented.

## Media publishing

The media publishing policy lives in the product package (`@mux/relay/media`, `mux_relay.media`)
over registered tracks: one tier above the write slot, one below the proposed `register_media`
handle. It knows no format. A frame carries its own object properties and a catalog is bytes.
Implemented in both SDKs.

- A catalog is a state-shaped track. The catalog service sends its document on the current relay
  demand and again on every later relay subscription, including a replacement subscription (the
  repeated "demanded" value), so a viewer joining after a relay reconnect receives it. A viewer
  joining while one relay subscription lasts triggers no re-send and is served the relay's cached
  copy. Its document is fixed. Not adopted: MSF's
  publish-on-change with a Joining FETCH from the relay cache. A plain SUBSCRIBE against the tested
  relay is live-edge only ([#26](https://github.com/muxinc/relay-sdks/issues/26)), re-sending is
  what the Video.js player relies on, and MSF-01 §6's one-object-per-stream sentence is treated as
  a draft defect because it contradicts LOC-04 §4.2 and MSF-01 §5.
- A catalog write that fails while the track stays open is retried after a configurable delay
  (default one second) for as long as demand remains, keeping only the current document; losing
  demand ends the retries until the next relay subscription. The service runs until cancelled or
  until the track ends, and raises the failure that ended the track when a write exposed it.
- A document service serves a state-shaped track whose document the application replaces, such as
  the media timeline. It sends the latest document, each time as a new group, on every relay
  subscription as the catalog service does, and again on every update while the relay is
  subscribed. It sends nothing before the first document. An update without relay demand only
  replaces the document; an update made during a write is sent after it. Retries follow the
  catalog's rule but always send the latest document. A retirement during a write ends the service
  with that failure; the track ending otherwise ends it quietly.
- A frame writer maps encoded frames onto one track's groups. Under keyframe grouping, the LOC
  mapping for video, a keyframe FINs the open group and opens a new one, and deltas append to it.
  Under frame grouping, for audio and any other independently decodable frames, every frame is its
  own single-object group. Object properties are sent as the frame supplies them.
- Writes on one writer are served one at a time, in the order they were called, so an encoder whose
  output callback cannot await may call `write` without waiting for the previous one. Frames waiting
  behind the one being written are the writer's backlog, bounded by `maxBacklog` / `max_backlog` (a
  positive integer; default 8 under keyframe grouping, 25 under frame grouping). A caller that awaits
  every write never has a backlog. A waiting write that is cancelled (Python; a TypeScript promise
  cannot be) leaves the backlog, and under keyframe grouping leaves a gap: the frame after it is
  treated as following a discontinuity, and a keyframe is needed.
- A frame that arrives at a full backlog means the transport has fallen behind, and the writer drops
  rather than queues. Under keyframe grouping every waiting frame is dropped and the open group is
  reset at once; the frame being written into it, if any, is dropped too. The arriving frame then
  opens the next group if it is a keyframe, and is dropped otherwise, as is every delta after it
  until the next keyframe. Under frame grouping the oldest waiting frame is dropped and the arriving
  frame takes its place at the back.
- A frame the relay has no subscription for when its turn comes is skipped at once; the writer never
  waits on the write deadline, because an encoder loop cannot block. A relay demand change (a new or
  replacement subscription, or its end) ends the open group in the write slot at once; the writer
  finds it ended at the next frame, even one written before it has seen the change, and skips deltas
  until the next keyframe. The open group is not replayed.
- Under keyframe grouping a writer says when it needs a keyframe (`keyframeNeeded` /
  `keyframe_needed`): from its start until a keyframe arrives, and again from each demand change,
  interruption, stale or discontinuous frame, or drop until the next keyframe arrives. A keyframe
  arrives when `write` is called with it, whether or not it is later sent. The optional
  `onKeyframeNeeded` / `on_keyframe_needed` callback runs synchronously on every entry into that
  state after the start, so an encoder can force a keyframe instead of waiting out its interval; an
  exception from it is reported (`console.error`; the asyncio loop's exception handler) and does not
  touch the writer. The writer watches the track's demand from its creation until it closes or the
  track ends; in Python it is therefore created with an event loop running. Under frame grouping no
  keyframe is ever needed.
- A frame that starts a group (a keyframe, or under frame grouping every frame) opens it as the
  group's first object, so a [delivery timeout](#delivery-timeouts) counts the wait for settling
  room and for the stream. When that open fails, no group came back, so the outcome names none.
- Lateness is the application's. An optional `stale` predicate is consulted before a group is
  opened, again once a frame has opened its group (the frame went out with the open, and a stale
  one has its group reset), and before each later write, since time passes inside those steps. A stale frame is
  dropped, an open group that depended on it is reset, and deltas are dropped until the next
  keyframe. A frame flagged as following a discontinuity resets the open group the same way.
- A failure is one of two kinds. It ends the writer (a retirement, see `CONTEXT.md`) exactly when
  the error code is `CLOSED` or the track reports `closed` afterwards, and likewise when the track
  is already closed as a frame's turn comes; that frame and every later one are reported as ended,
  with the error, and nothing further touches the track. Any other failure (an interruption) resets
  the group, leaves the track open, and reports the frame as interrupted with its error; the next
  keyframe, or the next independent frame, opens a new group. The FIN of the previous group that a
  keyframe triggers is part of that keyframe's write: a retirement during it ends the writer, while
  an interruption during it is absorbed, because the previous group's delivered objects stand and
  the new group opens regardless. A group the writer resets for a drop is not a failure. Demand
  alone never decides: no demand is also what a track nobody subscribes to looks like.
- A media track gets a default [delivery timeout](#delivery-timeouts) from
  `mediaTrackProperties(kind, options)` / `media_track_properties(kind, ...)`, whose track
  properties go to `registerTrack` / `register_track` beside the format's own (for LOC,
  `locTrackProperties()`): a stall then costs stale frames, not lasting latency. They hold one
  SUBGROUP_DELIVERY_TIMEOUT, in whole milliseconds rounded up from microseconds. Audio's is 500 ms.
  Video's is the [target latency](../CONTEXT.md) less the keyframe interval, and never less than the
  interval; the target latency defaults to twice the interval, so video's default is one interval.
  An explicit subgroup timeout replaces either default. It is a track property because moq-relay
  reads an object's `0x06` as a Timestamp, and because the relay ignores both sides' values only the
  publisher's own enforcement applies.
  - No object timeout is set. The frame writer opens each group with its first frame, so an object
    timeout would count the wait for settling room
    ([#306](https://github.com/muxinc/relay-sdks/issues/306)), but once the group's stream is open
    Chromium's WebTransport takes each object at once even during a stall (every write resolved in
    under 1 ms through a 2 s blackout; [#309](https://github.com/muxinc/relay-sdks/issues/309)).
    Whether the defaults should add one is [#310](https://github.com/muxinc/relay-sdks/issues/310).
  - Audio's value is measured, not derived from the target latency, which would let a whole stall's
    audio through late. Through a 2 s uplink blackout to sjc, with the subscriber on a clean path,
    it brought the player's audio lag after the stall from 2.44 s to 0.43 s (0.27 s before it),
    and under a 4 s 80 kbit/s uplink cap from 4.6 s to 1.6–1.7 s. Without it, audio's 50 groups a
    second fill the 64-group settling limit during a stall, so both tracks' groups wait up to about
    1 s to open ([#302](https://github.com/muxinc/relay-sdks/issues/302)).
    The timer starts when a frame's FIN is handed over, so the audio from a stall's last 500 ms
    survives and arrives late: after an uplink stall the player's lag can rise by up to about the
    audio timeout. With the WebCodecs example's defaults, three 2 s uplink blackouts raised it by
    214, 168 and 511 ms (2170 ms without), so the live bar is a rise below the audio timeout plus
    100 ms, not a fixed jitter allowance.
  - A subgroup timeout resets a settling group the writer has moved past. It reports nothing to the
    writer and raises no keyframe-needed, since the open group decodes without it. Under keyframe
    grouping the open group has no timer until its FIN, at the next keyframe, so its objects can
    wait out a stall in the transport; dropping late video is then the reader's.
  - A downlink stall is outside the publisher's reach: the relay keeps a stalled viewer's backlog
    whatever these values are.
- Every frame gets exactly one outcome: sent, skipped, stale, dropped, interrupted or ended. The
  writer returns it rather than raising, so a caller keeps counters without catching per frame, and
  the group the frame was written to or that was reset on its behalf travels with it. Closing the
  writer FINs the open group, reports every waiting frame as dropped, and refuses further frames
  with `CLOSED`; the registered track stays the application's to close.

## Media subscribing

A track's objects arrive in any order across group streams: after a stall, an older group's stream
can finish after newer groups have started ([issue #303](https://github.com/muxinc/relay-sdks/issues/303)).
The raw subscription delivers them as they arrive. A frame sequencer (`createFrameSequencer` /
`FrameSequencer`, in the format packages) turns one track's objects into frames in decode order and
drops what is too late against the broadcast's playout clock. A frame reader (`createFrameReader` /
`FrameReader`, in `@mux/relay/media` and `mux_relay.media`) is a subscription's or a want's handlers
that drive a sequencer and poll it when a wait ends; it knows no format. Implemented in both SDKs.

- Lateness is measured against the playout clock's position, which advances in real time through a
  stall. While the clock is idle nothing is late. The budget is the clock's target latency, one
  setting for every track of the broadcast; the sequencer reports the interval between the
  keyframes of consecutive groups to the clock, so the default follows the stream. Only a keyframe
  that can still be delivered reports one: an old duplicate does not move the budget.
- A frame's timestamp is its LOC Timestamp, scaled by the object's Timescale, then the track
  properties from SUBSCRIBE_OK. An object without one is counted (`untimed`) and ignored. An
  end-of-group status object ends its group. A repeated object is ignored.
- Keyframe grouping (video): frames go out in order, group by group and object by object, as soon
  as they are next; in-order delivery adds no delay. A frame more than the target latency behind the
  position is past its deadline: its group is abandoned, and so is any group whose keyframe is.
- When the next frame is missing (a gap inside the group being delivered, a still-open group whose
  next frame has not arrived, or the next group's object 0), later frames wait only while a newer
  group's keyframe is waiting, and only until the position passes the last delivered frame's
  timestamp plus the target latency (at once if nothing was delivered yet), and while the clock is
  idle until the group ends or is reset. The group is then passed over. A group whose stream ended with
  a hole, or was reset, is passed over at once. A group that ended after its last object was
  delivered is followed by the next group with no discontinuity. Any other group that ends or is
  reset without its keyframe is discarded, since it can never be delivered, and the end or reset of
  a group that has not been seen is kept only for the next group.
- When the next group ID has not been seen, the lowest group holding its keyframe waits while that
  keyframe is not yet due (its timestamp ahead of the position), since the missing group may still
  arrive or may never have been sent (a failed group's ID is not reused). Groups and objects behind
  what was delivered or passed over, or below the group being delivered, are dropped as late.
- Frame grouping (audio): every frame stands alone and goes out at once in group and object order. A
  group may hold several frames, one per object, as a group opened with `openGroup()` /
  `open_group()` writes them; the position is then a group and an object, so a group's later objects
  are still ahead of its first. A frame whose group ID skips past the last delivered one, whose
  object ID skips within the last delivered group, or that opens a newer group past its object 0,
  carries `gap`. A frame more than the clock's `lateDropUs` behind the position is dropped, so a
  burst after a stall cannot build a lasting backlog in the player; one behind what was delivered
  (an earlier group, or an earlier object of the same group) is dropped too. Nothing waits
  unless a reorder window is set (below).
- Per-track delivery policy (TypeScript only; Python keeps the defaults). Three sequencer options
  tune one track without touching the clock its other tracks share; each default leaves the
  behaviour above unchanged:
  - `lateDropUs`, frame grouping only: the late-drop limit for this track instead of the clock's.
    `Infinity` drops nothing as late, for a reader that wants every frame (speech-to-text).
  - `reorderWindowUs`, frame grouping only (default 0): a frame whose group ID is more than one past
    the last group delivered, dropped or known to be over waits, with every newer one, for the
    missing group (a later object of the last group taken is not held), until
    the window has passed since that frame's group arrived (on the clock's local time). A missing
    group that arrives in time goes out in order with no `gap`; after the window the waiting frames
    go out with `gap`. A group that ended or was reset with nothing in it releases the wait at once,
    whenever its end arrived, before the first delivery or after a restart included.
    A frame that turns late while it waits is dropped as late. For a reader that wants every frame
    of a talker, the window should be at least the publisher's delivery timeout (audio's default is
    500 ms): a group the publisher's transport is still resending can arrive that long after the
    group that follows it, and moq-relay 0.17.5 delivers it however late. With a relay that
    honours a subscriber's delivery timeout, the agent's subscription sends none, or one at least
    as long as the window.
  - `catchUpAfterUs`: when frames keep arriving late with none delivered, from the first one
    dropped as late to the latest one at least this long apart (on the clock's local time), the
    next poll returns `catchUp: true`, once, until a frame is delivered again. A restart does not
    re-arm it, so a replacement that is late from its first frame resyncs rather than catches up
    again. The TypeScript reader calls `onCatchUp`, whose owner replaces the subscription at the
    live edge (a want's [catch-up](#supervised-subscriber)). Set it below the target latency, or a
    resync re-anchors on the late frames first.
- A delivered frame whose group ID is more than one past the previous delivered frame's carries
  `missingGroups` (TypeScript only): the inclusive range of group IDs of which nothing was
  delivered, whether never received, reset, or dropped as late or behind. It is the gap report an
  application acts on; nothing in the SDK fetches the missing groups. A trailing loss is reported
  only with the next delivered frame, and never across a restart, where group IDs start over.
- The first frame after anything was skipped carries the discontinuity `gap`; a decoder resets
  there, as at a keyframe it can start from. No gap is marked before the first delivery.
- Resync: once frames have arrived but none within the budget for one whole target latency of the
  position, with their lateness steady (within a quarter of the target latency of where it stood
  when the wait began, judged on every late object, held or arriving; a stall's growing lateness
  and a draining backlog's shrinking one restart the wait), the sequencer delivers the newest late group that still has its keyframe (or the newest
  late frame), marks it `resync`, and resets the clock, which re-anchors on the next frame shown.
  That group keeps collecting its later objects and its end while it waits. The wait is measured on
  the clock's local time (`localNow()` / `local_now_us()`). If another track resets or re-anchors
  the shared clock meanwhile (a reset, or a position that no longer moves with local time, by more
  than a quarter of the target latency), a candidate back within the budget (or facing an idle
  clock) is delivered normally instead, and otherwise the wait starts over on the new timeline.
- Resync forward: a keyframe that would open the next delivered group (or, under frame grouping, a
  frame) more than the target latency ahead of the position means the clock was anchored on a
  backlog, as after a resync during a long stall. The sequencer resets the clock and marks that
  frame `resync`, so the backlog does not become lasting latency. It does so in a poll of its own:
  frames before it in the same poll are returned first, with a wait of 0.
  A lasting increase in delay therefore costs one target latency, not a black or silent player.
- A want's Restart or Move restarts the sequencer: everything held is forgotten, group IDs start
  over, the clock is reset so the new timeline is not judged against the old anchor, and the next
  frame carries `restart`, even when nothing was delivered before and even when that frame comes
  by a resync. A Move also brings the new subscription's track
  properties.
- `poll()` returns the frames ready now, at most `maxFrames` / `max_frames` (a positive integer;
  default all), and how long until a wait can end: 0 when more is ready now, nothing when only a new
  object can change it.
  Frames not taken stay held and are judged again at the next poll. While the clock is idle and
  frames are held, it asks to be polled again after 50 ms, since the clock may anchor without any
  new object. At most about 1,024 frames are held: past that, as when nothing polls, everything
  held is dropped as late and the next frame carries `gap`. The TypeScript reader polls after every
  event, the Python reader while a read is pending; both poll again when that time comes, or after
  50 ms if sooner: another track may reset or re-anchor a shared clock meanwhile, which no event of
  this track's reports. The TypeScript reader hands each frame to `onFrame`; an event fed from within
  `onFrame` is applied after the frame in hand, and a Restart or Move fed that way drops the rest of
  the batch, which belongs to the replaced subscription. The Python reader is
  iterated and takes one frame per read, so a slow reader gets fresh frames, not a backlog. Both treat
  each group as one subgroup, as the frame writer sends it. A want read through a reader keeps
  `drainMs` / `drain` at 0, the default: a drained old subscription's groups would arrive after the
  Move, indistinguishable from the new subscription's. Counters: delivered, late,
  skipped groups, resyncs, untimed. The decisions are pinned by `vectors/frame-sequencer.json`.

## Package boundaries

- The optional TypeScript Mediabunny adapter depends on the media format package and declares
  Mediabunny as a peer. Neither the core, format package, nor product SDK depends on the adapter.
- The generic media format package depends only on the core, imports no product or transport
  library, and owns no encoder outside a module behind an optional extra that its root does not
  import: Python's `moq_media.opus`, on PyAV (ADR 0006). The core imports nothing from the media
  package.
- The product package does not depend on the media format package either. Its media policy pairs
  with it structurally: a frame carries its own properties, and the frame reader drives a sequencer
  it is given.

- Each language has a product package that owns the Relay API and the authentication convention,
  and a generic MOQT core package that is protocol-only.
- The core has no runtime dependencies, no knowledge of Relay, and no player, UI, or media engine
  imports. It works over an abstract transport that the product package supplies.
- Transport adapters are separate packages the application passes in, and the product package
  depends on none of them. The TypeScript WebSocket adapter is the only package with a third-party
  runtime dependency, `@moq/qmux` at an exact version (ADR 0005).
- Each language enforces this mechanically in its check task.

## Tooling conventions

Live tests and examples in every language read the same environment:

| Variable | Meaning |
| --- | --- |
| `RELAY_TOKEN` | Relay JWT. When absent, tooling prompts with input hidden. Never written to disk or printed. |
| `RELAY_URL` | Overrides the default endpoint in `protocol.json`. Must be https and must not carry the token. |
| `RELAY_SUB_URL` | Live tests only: the subscribers' relay, for a cross-relay run. Defaults to `RELAY_URL`; same rules. |
| `RELAY_NAMESPACE_PREFIX` | Namespace prefix the token authorizes. Live tests append a random field so runs never share relay state. |

## Live smoke test

Each SDK ships an opt-in test against a real relay, excluded from the default check. It publishes
and subscribes on the same random namespace, then asserts:

1. A publisher connection and a subscriber connection both open with the supplied token.
2. Once the publisher's `announced` settles, subscriptions to two tracks on the namespace are
   accepted, retrying the fresh-namespace rejection while the announce propagates. A
   [supervised subscriber](#supervised-subscriber)'s wants make them and never resubscribe
   during the run.
3. Binary payloads, an empty payload, and a text payload all arrive intact. Each is written once,
   after a warm-up marker the subscriber drops has arrived on that track: a `SUBSCRIBE_OK` does not
   mean the path to the publisher is ready ([a viewer on another relay](relay-interop.md#a-viewer-on-another-relay)).
   The report records the warm-up writes.
4. Closing one track writer does not affect delivery on another track.
5. A late subscriber decodes `LARGEST_OBJECT` from a populated track's `SUBSCRIBE_OK` and receives the next live object.
6. The subscriber's [discovery](#discovery) on the namespace's parent prefix reports the namespace
   `announced` once the publisher's `announced` settles, then `unannounced` after the publisher's
   clean `close()`, and nothing else for that namespace. The report records how long the withdrawal took.
7. A want following a [supervised publisher](#supervised-publisher)'s root receives the first
   generation's objects. When a second supervised publisher starts on the same root while the
   first is still up, the want moves to the newer generation make-before-break. Both generations
   beat one shared counter every 50 ms, and the want cuts over once a beat of the new one is within
   a few of the latest delivered, with a 1 s drain. It reports one `onSwitched` and no
   `onRestarted`, and no beat number is missing across the move, on one relay and across two. The
   report records how long the move took.
8. All connections close cleanly once the test closes them.

Adding an assertion here means adding it to every SDK's smoke test. The TypeScript test runs the
same scenario over each transport it ships: native QUIC in Node, WebTransport in Chromium, and QMux
over WebSocket in Node and Chromium, with WebKit, Firefox and the WebTransport-to-WebSocket fallback
on request (`RELAY_RUNTIME`).
See [relay interoperability](relay-interop.md#reproduce-against-a-local-relay) for a local moq-relay test.

## Not yet specified

Datagram reception, discovery of tracks within a namespace (namespace discovery is [specified](#discovery)),
and player adapters are not implemented in any SDK and have no contract yet. Add them here when the first SDK gains them.

## Draft-22 protocol boundaries

- `MAX_REQUEST_ID` and `REQUESTS_BLOCKED` were removed after draft-16
  ([draft-22 Appendix A.6](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#appendix-A.6)).
  They are not draft-22 request-budget messages. Unknown control messages terminate the session,
  as required by [Section 9](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#section-9).
- An unrecognized wire Object Status terminates the session with `PROTOCOL_VIOLATION`, following
  [Section 11.1.1](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#section-11.1.1).
  The `UNKNOWN_OBJECT_STATUS` stream-reset code has a different meaning: a FETCH publisher cannot
  determine whether an object exists ([Section 12.5](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#section-12.5)).
  Ordinary subgroup transport resets remain per-stream delivery events.

### Relay interop deviations

Four choices in the generic cores depend on how moq-relay treats the draft's teardown messages,
track aliases and request updates. All four conform to draft-22 (and draft-20); each is recorded because the
relay's behavior, and for the first one its version, decides whether it works. The relay behavior,
the evidence and how to re-test are in [relay
interoperability](relay-interop.md#draft-22-deviations). Re-test against the relay before changing
one: on a relay older than 0.15.4 the first one aborts every viewer's track. Both SDKs agree on all
four, and the core module headers (`ts/packages/moq-core/src/protocol/publish-session.ts`,
`python/packages/moq-core/src/moq_core/publish_session.py`) state the same constraints.

1. **A subscription ends with PUBLISH_DONE, then FIN, which needs moq-relay 0.15.4 or later.**
   Draft-22 [Section 6.4.2.2](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#section-6.4.2.2):
   "the publisher of an Established subscription MUST send PUBLISH_DONE, before sending a FIN"; and
   [Section 9.9](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#section-9.9):
   "A sender MUST NOT send PUBLISH_DONE until it has closed all streams it will ever open". This
   conforms. Every end the publisher chooses resets the subscription's data streams that are still
   open, writes PUBLISH_DONE with Stream Count set to the data streams opened for that
   subscription, and FINs the request stream:

   | end | PUBLISH_DONE status | reason |
   | --- | --- | --- |
   | closing a track writer, or the publisher | `TRACK_ENDED` (0x2) | `track ended` |
   | a replacement subscription supersedes an older one | `TRACK_ENDED` (0x2) | `superseded by a newer subscription` |
   | an unsupported REQUEST_UPDATE, after its REQUEST_ERROR ([Section 9.5.1](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#section-9.5.1)) | `UPDATE_FAILED` (0x8) | `unsupported subscription update` |
   | the subscriber FINs its direction while the subscription is live | `INTERNAL_ERROR` (0x0) | `subscriber closed its request stream` |

   No status names a replacement, and the relay finishes a track only on `TRACK_ENDED`. A
   subscription the subscriber cancels (a reset) gets no PUBLISH_DONE, and its open data streams
   are reset ([Section 3.1.2](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#section-3.1.2)).
   Once a subscription is ending no data stream opens on its alias, and one that finishes opening
   afterwards is reset and not counted. moq-relay 0.15.4 and later read the PUBLISH_DONE, wait for
   Stream Count data streams or a grace period (1 s), finish the track, and only then FIN their side
   of the request stream: a viewer receives an end-of-track object and PUBLISH_DONE status 2
   `track ended`. A session that closes before the relay has done so aborts the track, which a
   viewer sees as status 0 `internal error`, so closing the publisher waits for that FIN (the one
   Section 6.4.2.2 says a requester SHOULD send once the responder's FIN completes the request) on
   every subscription it ended before closing the transport (bounded, see deviation 2). The SDK
   subscriber sends that FIN too, once the publisher's FIN follows PUBLISH_DONE, so an SDK publisher
   facing an SDK subscriber directly closes without waiting out the bound. moq-relay 0.15.4 and later
   treat a FIN without PUBLISH_DONE as a failed request and abort the track. moq-relay 0.15.1 to 0.15.3 do the reverse:
   any byte after SUBSCRIBE_OK aborts the track, so every end these SDKs send reaches viewers as
   status 0 `internal error`, where a bare FIN finished the track. No behavior ends cleanly on both,
   and the SDKs cannot tell the versions apart (the relay's SETUP names only `moq-lite-rs`), so they
   require 0.15.4 or later.
2. **Neither connection sends GOAWAY.** Draft-22
   [Section 9.2](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#section-9.2):
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
3. **A track alias is the peer's own request ID.** Draft-22
   [Section 3.1](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#section-3.1): "A
   publisher MAY assign the same or different Track Aliases to these subscriptions"; and
   [Section 3.1.3](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#section-3.1.3):
   "The same Track Alias MUST NOT be used by a publisher to refer to two different Tracks
   simultaneously in the same session." This conforms, and narrows a MAY: every SUBSCRIBE_OK
   carries the SUBSCRIBE's request ID as the alias, which is unique for the session by
   construction, so an alias is never shared between subscriptions, of one track or of two. A
   request ID the peer reuses, or one with a client's (even) parity, closes the session with
   `INVALID_REQUEST_ID` before it can become an alias, as
   [Section 6.4.2.1](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#section-6.4.2.1)
   requires. The subscriber connection applies the same check to every request the relay opens,
   including the ones it rejects, before answering. moq-relay allocates server request IDs odd,
   from 1, so the parity check never fires against it. moq-relay 0.15.1 and 0.15.8 close the session with PROTOCOL_VIOLATION when one
   alias names two different tracks, and cancel the newer subscription when two live subscriptions
   to one track share an alias; unique aliases avoid both. The application never sees an alias on
   the Relay surface. Nothing needs to change unless the SDK serves concurrent subscriptions to one
   track, which
   [ADR-0001](../docs/adr/0001-one-relay-subscription-per-track.md) rules out today.
4. **`update()` sends REQUEST_UPDATE, which moq-relay does not implement.** Draft-22
   [Section 9.5](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#section-9.5):
   "The receiver of a REQUEST_UPDATE MUST respond with exactly one REQUEST_UPDATE_OK or
   REQUEST_UPDATE_ERROR message indicating if the update was successful", per-request names for
   REQUEST_OK and REQUEST_ERROR ([Section 1.5](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#section-1.5)). This conforms: each `update()` on an accepted
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
