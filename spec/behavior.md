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
  precedence over a token callback.
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
  Requests still awaiting their first response receive their error callback once; accepted or
  cancelled requests do not receive a second initial response.
  Automatic reconnection and token refresh on a live session are not yet part of this contract.

## Subscribing

- Namespaces are ordered tuples of fields sent verbatim. The SDK adds no prefix and performs no
  normalization. Fields are absolute: the token's root prefix is part of the namespace the
  application supplies. Subscription namespaces must contain non-empty string fields; invalid
  namespaces are rejected locally before sending a request. Publishing and subscription namespaces
  contain at most 32 fields (pinned in `protocol.json`); oversized namespaces are rejected locally,
  before connecting a publisher or sending a subscription. Track names must be non-empty strings.
- A known track can be subscribed directly. The SDK does not interpret payloads and does not
  assume a track carries audio, video, or timestamps.
- Object callbacks run as the stream layer delivers objects. The SDK adds no application queue, no
  ordering across streams, no payload decoding, no clock synchronization, and no media scheduling.
- Each new subscription is independent; re-subscribing does not resume the preceding request.
  Relay caching and location filters can deliver the same object again across subscriptions,
  including after a publisher restart. Applications own retry backoff and any cross-subscription
  continuity policy; see [publisher restarts and resubscription](relay-interop.md#publisher-restarts-and-resubscription).
- The request timeout bounds only the subscription's initial response. An accepted subscription
  has no application inactivity deadline. Keeping an idle transport connected depends on peer
  traffic or transport-provided keepalive; the SDK does not schedule its own keepalive. The Python
  transport uses aioquic's default 60-second local idle timeout, subject to peer negotiation.
- A subscribe for a namespace the relay has not seen announced is rejected with request error 404,
  not parked; a namespace freshly announced can still answer 404 for about a second, so applications
  retry that rejection rather than treat it as fatal.
- Subgroup resets and connection closure are surfaced explicitly. There is no exactly-once or
  gap-free delivery promise.
- The publisher's done notification is a control message and may arrive before trailing objects
  still in transit. It does not end the subscription: the request stays open until the application
  cancels it or the connection ends, and the SDK does not resubscribe on its own.
- Subscriptions cancel independently of each other. The application owns connection shutdown.

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
- The default write deadline is 10 seconds and covers both waiting for demand and the transport
  write.
- A timeout, cancellation, transport write failure, or relay subscription change resets the
  affected write's stream and leaves the track open while the publisher connection remains open.
  A stream that opens after cancellation is reset without sending payload bytes. The next explicit
  write uses a new group ID; failed groups are never reused. After demand disappears, later writes
  wait for a new relay subscription. Applications choose fresh data or the next random-access point
  after a failure; already delivered bytes cannot be withdrawn. There is no automatic retry.
- A write's payload must be a byte sequence in the language's own terms. Any other type is rejected
  before the write starts, which is a local argument error and leaves the track usable.
- Each write copies its bytes and sends one independent group containing one object. Only one
  write may be pending per track; a concurrent write is rejected rather than queued.
- Write completion means the transport accepted the complete group, not that any viewer received
  it.
- There is no publisher-side retention or replay. Late-subscriber behavior depends on relay caching
  and subscription filters.
- A failed write may have delivered partial data. There is no automatic retry.
- The publisher encodes supplied track and object properties as protocol metadata. The SDK adds no
  default timescale, timestamp, or LOC media packaging. Subscribers expose the properties received
  from the relay, which may differ from those sent by the publisher; end-to-end preservation of
  arbitrary properties is not guaranteed. See [Relay property handling](#relay-property-handling).
- Object payloads are limited to the size in `protocol.json`.
- Closing a track writer cancels an outstanding write and ends that track; other tracks continue.
  Closing the publisher ends all tracks, retracts the namespace, and waits for transport shutdown.
- Track names cannot be reused within a publisher connection, including after their writer closes.
- A track writer may open a multi-object group: objects are appended with object IDs 0, 1, 2, ...
  on one stream, and closing the group FINs the stream. Object 0 is the group's random-access point,
  which is how LOC/MSF media (one GOP per group) is published. A group that fails part-way is reset
  rather than completed, and the track stays open for the next group; only one write or group may
  be pending per track. The write deadline applies separately to opening a group (including demand),
  each object write, and closing the group. Idle time between operations is unrestricted.
  Implemented in both SDKs.
- A track writer exposes whether the relay currently demands the track and lets the application
  observe every change, so state-shaped tracks such as a catalog can be re-sent on each subscribe.
  Demand observers emit the current state first, include replacement subscriptions (a repeated
  "demanded" value), and finish when the track ends. A closed track reports no demand. Demand is
  the relay's single upstream subscription for the track: the relay shares it across every
  downstream viewer, so additional viewers produce no further notification, and it may keep the
  subscription for a period after the last viewer leaves. Demand is therefore neither a viewer
  count nor a liveness signal. Implemented in both SDKs.

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

## Package boundaries

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
2. Subscriptions to two tracks on the namespace are accepted.
3. Binary payloads, an empty payload, and a text payload all arrive intact.
4. Closing one track writer does not affect delivery on another track.
5. A late subscriber decodes `LARGEST_OBJECT` from a populated track's `SUBSCRIBE_OK` and receives the next live object.
6. All connections close cleanly once the test closes them.

Adding an assertion here means adding it to every SDK's smoke test.
See [relay interoperability](relay-interop.md) for a local moq-relay 0.14.17 test.

## Not yet specified

Automatic reconnection, token refresh on a live session, datagram reception, product resource
discovery, typed payload helpers, and player adapters are not implemented in any SDK and have no
contract yet. Add them here when the first SDK gains them.

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
