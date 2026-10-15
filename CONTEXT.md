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
of that field, and the SDK never splits, joins or prefixes. The one exception is a supervised
publisher, which appends one generation field to the application's root. It is never a single string.
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
rejects it (request error 16 with reason `unroutable` from moq-relay 0.15.8, 404 on older relays)
rather than parking it; the application retries within its own budget, the SDK never does. A
subscriber that waits for namespace discovery (`namespaceEvents` / `namespace_events`) to report
the namespace announced avoids it (measured on moq-relay 0.17.0).
_Avoid_: startup race, propagation delay, transient 404 (the same rejection after a retract is
publisher recovery, a different case)

### Supervising

**Supervisor**:
The opt-in policy layer over a connection primitive: it reconnects with backoff and keeps the
application's tracks registered or subscribed across connections. The primitives never do either.
_Avoid_: auto-reconnect (the primitives have no such mode), manager, hub, client

**Generation**:
One supervised publisher connection, and the namespace it serves: the root plus one field that sorts
by start time. Each reconnect is a new generation, which the relay treats as a new publisher.
_Avoid_: session, epoch, incarnation, reconnect count

**Rotation**:
Replacing a supervisor's connection before its credential expires, make-before-break: the new
connection takes over while the old one still serves, so viewers see a switch, not a drop. The relay
ends a session at its credential's expiry and cannot take a new token on a live one.
_Avoid_: token refresh (no token is refreshed in place), reconnect (nothing was lost)

**Want**:
A supervised subscriber's standing request for one track, keyed by the application, held across
subscriptions until it is withdrawn. Each subscription it makes is an ordinary subscription.
_Avoid_: subscription (that is one attempt), watch, follow

**Restart**:
The point where a want's accepted subscription ended and a replacement will deliver the next
objects, live, with the gap unfilled. A decoder resets there.
_Avoid_: resume (nothing resumes), reconnect (that is the connection's), retry (a rejected attempt
was never accepted)

**Move**:
A want's change of subscription made before the break: a newer generation, or the same key wanted
with another track or parameters. The replacement opens while the current subscription keeps
delivering, and takes over at a cutover (`onSwitched`), which is not a restart. A decoder resets
there too.
_Avoid_: restart (nothing was lost in between), switch (the switchable's verb, for the primitive),
migration

### Media publishing internals

**Frame writer**:
The module that maps one track's encoded frames onto its groups, one frame at a time: under
keyframe grouping a keyframe opens a group and deltas append to it, under frame grouping every
frame is its own group. It skips frames the relay has no subscription for, resumes at the next
keyframe after any failure, and says when it needs one. It knows no format.
_Avoid_: frame publisher, media writer, GOP mapper, track publisher, sink, media sink

**Backlog**:
The frames handed to a frame writer that the transport has not taken yet, in arrival order and
bounded. An encoder whose output it cannot await fills it; one that awaits each write never does.
_Avoid_: queue (unqualified), buffer, pending writes

**Drop**:
A frame the frame writer discarded because its backlog overflowed: under keyframe grouping the
whole backlog and the open group go, under frame grouping the oldest frame. Unlike a stale frame,
it says the transport fell behind, not that the frame was late against the application's clock.
_Avoid_: stale (that is the application's lateness), skip (that is no demand), overflow (the cause)

**Keyframe needed**:
The frame writer's state while the next frame it can send must be a keyframe: from the start, and
again after new or replacement demand, a generation move, an interruption, a stale or
discontinuous frame, or a drop. Entering it is the signal for an encoder to force one rather than
wait out its keyframe interval.
_Avoid_: keyframe request (the relay's NEW_GROUP_REQUEST is a different thing), PLI, refresh

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

### Media timing

**Broadcast timeline**:
The one microsecond timeline that every LOC media track listed in one catalog is stamped on, from an
origin at or before the first capture. It belongs to one catalog namespace, so a supervised
publisher's next generation starts a new one. Relay-clock stamps on a track that declares no
timescale are not on it.
_Avoid_: shared clock, media clock, wall clock (that is the local monotonic source), PTS base

**Media timeline**:
A broadcast's optional track that maps media time on the broadcast timeline to the location of each
video group's first object, and to its capture time as Unix-epoch time (msf-01's "wallclock"). It
is how a player finds which group to fetch for a time. It belongs to one catalog namespace, so a
supervised publisher's next generation starts an empty one.
_Avoid_: timeline (ambiguous with the broadcast timeline and the clock's source and sample
timelines), program date time, wall clock track (wall clock is the local monotonic source)

**Playout clock**:
A player's position on one broadcast timeline, which video frames are scheduled against. Audio
reports drive it while they arrive; otherwise it follows the local clock (wall pacing). It resets
wherever the decoders reset.
_Avoid_: playback clock, master clock, sync clock, jitter buffer (the audio path owns that)

**Target latency**:
The one latency budget a broadcast is played at: how far behind live a viewer plays. It defaults to
twice the keyframe interval on both sides, and nothing carries it on the wire. The publisher derives
video's default delivery timeout from it (the target latency less the keyframe interval, and at
least one interval). On the player it is the playout clock's: by default twice the longest recent
keyframe interval the frame reader sees, and how far behind the clock's position a frame may still
be delivered for decoding. It is not msf-01's catalog `targetLatency`, which is end-to-end
wall-clock time.
_Avoid_: latency budget, delay, buffer depth, jitter buffer, max delay (that is the clock's lead
before re-sync), deadline (that is one frame's)

**Render clock**:
When an `AudioContext` renders, on `performance.now()`'s clock: of the observations of a context time
and when it was seen over the last second, the one seen soonest. Capture calibrates its source
timeline from it, and a player subtracts the output delay from it to know what is audible. It is not
`getOutputTimestamp()`, whose pair means the speaker in Chromium and the render shifted back in Firefox.
_Avoid_: output timestamp, audio clock, context clock

### Media subscribing internals

**Frame reader**:
The module that reads one subscribed track as frames in decode order: it feeds a subscription's or
a want's events to a frame sequencer and polls it when a wait ends. The sequencer reorders groups,
waits for what may still come, and drops what is too late. Neither buffers for smoothing.
_Avoid_: jitter buffer (the audio path owns that), reorder buffer, frame source, player

**Discontinuity** (reading):
Why a frame the reader delivers is not the one decoded after the frame before it: a gap (frames
were skipped), a resync, or a restart. A decoder resets there. On the writing side it is the frame's
flag that frames before it were dropped.
_Avoid_: break, seek, reset (that is the decoder's response)

**Resync**:
The reader's escape when nothing has arrived within the target latency for one whole target
latency: it delivers the newest late keyframe group, or audio frame, and resets the playout clock to
re-anchor there, accepting the added delay rather than staying black or silent.
_Avoid_: re-anchor (the clock's own act), seek, catch-up

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
observe closure (the core's `MoqtTransport`). The aioquic native QUIC client (`mux_relay.quic`) is the
Python product's only adapter (ADR 0004); in TypeScript a runtime's native `WebTransport` is the default
adapter, and runtimes without one take an adapter the caller supplies. The tests supply in-memory pairs and
loopback accepting ends, a WebTransport one among them.
_Avoid_: polyfill (an adapter satisfies the SDK's seam, not the W3C `WebTransport` API), transport
layer, transport implementation, WebTransport (the protocol, not the adapter), connection (a Relay
connection runs over an adapter)

**Native QUIC**:
MOQT carried directly on a QUIC connection, with the MOQT version as the ALPN and the path and
authority sent in SETUP. The TypeScript SDK uses it where a runtime has no usable `WebTransport`, and
it is the Python SDK's default transport (ADR 0004).
_Avoid_: raw QUIC, direct QUIC, plain QUIC

**WebTransport session**:
MOQT carried inside a WebTransport session over HTTP/3, with the version negotiated through
`WT-Available-Protocols` and the path in the URL. Browsers always use this; the Python SDK dials none,
and keeps one only as the accepting end its tests run a browser against.
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
