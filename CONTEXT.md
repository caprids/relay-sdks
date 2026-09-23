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
opening-stream, writing, closing.
_Avoid_: request, job, transaction, publish

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

### Test vocabulary

**Relay hub**:
The in-memory fake relay the product tests run against over a transport pair. It speaks MOQT to a
publisher connection and a subscriber connection and owns the relay behaviour the core does not.
_Avoid_: mock relay, relay stub, test server
