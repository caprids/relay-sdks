# Relay SDKs

The Relay SDKs (TypeScript `@mux/relay`, Python `mux_relay`) publish to and subscribe from the Mux
MoQ Relay under one shared behaviour contract. Public names live in
[spec/vocabulary.md](spec/vocabulary.md) and are not repeated here. This glossary holds the
internal, relay-side and test vocabulary that the code, its tests and its reviews use.

## Language

### Track naming

**Namespace**:
The ordered tuple of non-empty string fields that scopes a publisher connection and every track
under it. The application supplies it in full and it travels verbatim: a slash inside a field is part
of that field, and the SDK never splits, joins or prefixes. It is never a single string.
_Avoid_: path, prefix, channel, broadcast

**Track name**:
The string that names one track within a namespace; the two together are the full track name the
relay routes on. Non-empty, in both roles.
_Avoid_: track id, alias (that is the relay's numeric binding for a track), stream

### Publishing internals

**Write slot**:
A track's single place for a pending operation. Only one write or open group may occupy it at a
time; a second is refused, never queued.
_Avoid_: in-flight slot, lock, mutex, binding

**Operation**:
One pending write or open group occupying a track's write slot, from the moment it begins until it
completes or fails. Its steps are the phases `spec/behavior.md` records: waiting-for-demand,
opening-stream, writing, closing. It completes once the transport has taken the group's FIN, not
once the relay has acknowledged it.
_Avoid_: request, job, transaction, publish

**Settling group**:
A group whose operation has completed, so it no longer holds the write slot, but whose FIN the
relay has not yet acknowledged. It can still be lost if the connection closes before the
acknowledgment, or be reset when its delivery timeout expires.
_Avoid_: in-flight group, draining stream, unacked group

**Delivery timeout**:
The draft's bound on how long a group stays deliverable: the subgroup timeout runs from the FIN to
its acknowledgment, the object timeout caps an object's age when it reaches the transport. Its value
is the smaller non-zero of the publisher's (the track property, or the group's first object) and the
subscriber's; expiry resets the stream.
_Avoid_: write deadline (the SDK's own bound on one operation), write timeout, TTL, max age

### Announcing

**Announce**:
The publisher connection's namespace, written onto a relay solicitation that covers it, no earlier
than its first registered track. The relay routes subscribes for the namespace only once it has
processed the announce, about a round trip later, and acknowledges nothing: `announced` is the
publisher's last fact.
_Avoid_: announcement (the act or the relay's state, ambiguous), publish namespace (the draft's
message), registration (that is a track's)

**Announce race**:
A subscribe that reaches the relay before it has processed the announce of its namespace. The relay
rejects it (request error 16 with reason `unroutable` on moq-relay 0.15.8, 404 on older relays)
rather than parking it; the application retries within its own budget, the SDK never does.
_Avoid_: startup race, propagation delay, transient 404 (the same rejection after a retract is
publisher recovery, a different case)

### Media publishing internals

**Frame writer**:
The module that maps one track's encoded frames onto its groups, one frame at a time: under
keyframe grouping a keyframe opens a group and deltas append to it, under frame grouping every
frame is its own group. It skips frames the relay has no subscription for and resumes at the next
keyframe after any failure. It holds no queue and knows no format.
_Avoid_: frame publisher, media writer, GOP mapper, track publisher

**Catalog service**:
The module that serves a state-shaped track: it sends the track's document on every relay
subscription and retries a failed send while demand remains. The document is bytes to it.
_Avoid_: catalog loop, catalog publisher, catalog server

**Frame**:
One encoded frame as the frame writer sees it: payload, keyframe flag, the object properties to
send with it, and whether frames before it were dropped (a discontinuity). Timestamps and lateness
are the application's business, expressed through the writer's `stale` predicate.
_Avoid_: sample (the example's own type, a frame plus its place on the timeline), chunk, packet

**Interruption**:
A failure that resets the group in progress and leaves the track open: a demand timeout, a changed
relay subscription, a transport error. The frame writer reports it and resumes at the next keyframe.
_Avoid_: transient error, retryable failure (nothing is retried)

**Retirement**:
The end of a track under an operation, or before one: the failure's code is `CLOSED`, or the track
reports `closed` afterwards. It ends the frame writer and the catalog service, and nothing further
touches the track. Whether the relay closed the track or the connection ended is not distinguished.
_Avoid_: terminal error, track death, disconnect

### Subscribing internals

**Subscription**:
The application's own SUBSCRIBE request for one track, from the moment it is made until it ends,
whether the relay rejected it, the connection closed, or the application cancelled it. It is not
demand, which is the relay's upstream subscription to a publisher, and it does not end when the
publisher is done.
_Avoid_: relay subscription (that is demand), request (fetch and track status are requests too),
listener, viewer

### Transport internals

**Transport adapter**:
An implementation of the transport seam a connection runs over: create and accept streams, close,
observe closure (the core's `MoqtTransport`). The aioquic WebTransport client is the Python product's
adapter; in TypeScript a runtime's native `WebTransport` is the default adapter, and runtimes without
one take an adapter the caller supplies. The tests supply in-memory pairs and a loopback accepting end.
_Avoid_: polyfill (an adapter satisfies the SDK's seam, not the W3C `WebTransport` API), transport
layer, transport implementation, WebTransport (the protocol, not the adapter), connection (a Relay
connection runs over an adapter)

**Native QUIC**:
MOQT carried directly on a QUIC connection, with the MOQT version as the ALPN and the path and
authority sent in SETUP. The SDK uses it where a runtime has no usable `WebTransport`.
_Avoid_: raw QUIC, direct QUIC, plain QUIC

**WebTransport session**:
MOQT carried inside a WebTransport session over HTTP/3, with the version negotiated through
`WT-Available-Protocols` and the path in the URL. Browsers always use this.
_Avoid_: WebTransport connection, H3 transport

**Dialing end**:
The end of a transport that opens the connection. The SDK is always the dialing end, whether it runs
in a browser or server-side.
_Avoid_: client (pairs with "server", which collides with server-side runtimes), server-side
(that is where code runs, not which end it is)

**Accepting end**:
The end of a transport that listens for and accepts connections. The relay is the accepting end; the
SDKs ship none, and only test helpers play it.
_Avoid_: server, server role (collides with server-side runtimes)

### Releasing

**Release train**:
The set of packages that release together at one version under one tag prefix: the TypeScript
train (`ts/v*`, every npm package) or the Python train (`python/v*`, every PyPI package). The two
trains never share a version number. See `docs/releasing.md` and ADR 0003.
_Avoid_: release channel (that is a dist-tag), monorepo version, lockstep (across languages that
means the protocol pin and the behaviour contract, not versions)

### Test vocabulary

**Relay hub**:
The in-memory fake relay the product tests run against over a transport pair. It speaks MOQT to a
publisher connection and a subscriber connection and owns the relay behaviour the core does not.
_Avoid_: mock relay, relay stub, test server
