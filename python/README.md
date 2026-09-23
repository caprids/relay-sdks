# Relay Python SDK

A standalone product SDK for Relay. Connect to a Relay endpoint and subscribe to arbitrary track
objects, or publish data, from any Python 3.11+ asyncio program. No browser is involved: the SDK
speaks WebTransport over HTTP/3 through aioquic.

This is the Python SDK in the Relay SDKs repository. The shared protocol pins, behavior contract, and
test vectors live in [spec](../spec/README.md); the [root README](../README.md) describes the suite. Package names are provisional
and publication is disabled.

## First round trip

The packages are **unpublished**. Use the local uv workspace below; there is no registry install
step. You need Git and [mise](https://mise.jdx.dev/getting-started.html) on PATH. The SDK requires
Python 3.11+; mise installs the repository's pinned Python and uv. Commands below run in Bash from
the repository root. The default transport is native aioquic WebTransport over HTTP/3, not a browser
or a WebSocket. The endpoint and network must allow QUIC/UDP traffic.

### Endpoint and credentials

Ask the team operating your Relay deployment (or your application's backend owner) for an HTTPS
WebTransport endpoint, the exact authorized namespace fields including any root prefix, and a
short-lived Relay JWT authorized to **publish and subscribe** there. The SDK consumes tokens; it
does not issue them or provide a public token signup flow. A Mux API access token is not a Relay JWT.
The hosted demo defaults to `https://sjc.relay.mux.global`, which still requires authorized credentials.
For another deployment, use the endpoint supplied by its operator.

The demo reads `RELAY_TOKEN` when set, otherwise prompts with hidden input. Prefer the prompt for
manual runs; automation can inject the environment variable through its secret manager. Do not put
tokens in commands, source files, URLs, or committed `.env` files. The demo passes the token to
`get_token` and redacts it from output. Authentication currently uses `?jwt=<token>` on the transport
URL: scrub query values and token strings before collecting network URLs, traces, or exception
reports. Browser consumers have an additional [console exposure risk](../ts/README.md#browser-token-exposure).

### Install and run

```sh
git clone https://github.com/muxinc/relay-sdks.git
cd relay-sdks
mise install
mise run python:install
```

A publisher's `write()` waits for relay demand: a matching subscriber must be running. Even when
publishing connects first, a fresh namespace can briefly reject subscriptions with `404 Broadcast
not found` while its announcement propagates. The combined demo starts both sides, retries only
that subscription rejection with capped backoff, and waits for demand before writing. Once a write
starts, a failure stops the run; it never automatically replays potentially delivered bytes.

Enter the endpoint and a **fresh namespace authorized by your token** when prompted below. The CLI
splits `team/quickstart/unique-run` into `["team", "quickstart", "unique-run"]`; the SDK itself sends
fields verbatim. Both connections use those same fields and track `ticker`. If your token authorizes
only one exact namespace, use that and avoid overlapping publishers or stale cached objects.

```sh
read -r -p 'Relay HTTPS endpoint: ' RELAY_URL
read -r -p 'Authorized namespace (slash-separated): ' RELAY_NAMESPACE
mise run python:example:mux-global -- --url "$RELAY_URL" --namespace "$RELAY_NAMESPACE" --track ticker --count 1 --timeout 30
# Enter the Relay token at the hidden prompt.
```

The complete example is [demo.py](examples/mux_global/demo.py), including imports, explicit token
configuration, and byte decoding. It uses `asyncio.timeout` and the SDK's existing async context
managers (through `AsyncExitStack` to support either or both modes). Each connection is closed when
the context exits, including if the second connection fails. Ordinary cleanup needs no custom
`CancellationToken`. In a fixed-mode application, the same lifecycle can be written as
`async with await connect_relay_subscriber(...) as relay:` and
`async with await connect_relay_publisher(...) as publisher:`.

Output includes the following (byte counts, timings, and ordering can vary):

```text
starting             mode=both namespace=... track=ticker count=1
publisher connected  ...
subscriber connected ...
subscribed           ...
published            seq=1 bytes=...
received             group=0 bytes=... seq=1 ageMs=...
finished             published=1 received=1
```

`finished published=1 received=1` is success. The 30-second run deadline bounds connection setup,
waiting for demand, retries, and reception. Errors print a redacted `failed` line and exit nonzero;
background tasks are cancelled and awaited, and connection contexts exit on success or failure.
A write completing only means the transport accepted it; the `received` line establishes this
demo's round trip.

### Use the local packages in your application

The workspace installs both packages as editable sources. Put a script in `python/` and run it
with `mise exec -- uv run --project python python python/your_script.py` from the repository root;
`from mux_relay import connect_relay_subscriber, connect_relay_publisher` resolves the local packages.
`mux_relay` re-exports the core types its own signatures use (`MoqtObject`, `MessageParameters`,
`SubscribeOk`, `RequestError`, `Closed`, and the rest of the "Protocol types borrowed from the core"
row in [the vocabulary](../spec/vocabulary.md)); other protocol types, such as the location filters
for `subscribe(parameters=...)`, come from the core package, which the workspace installs alongside:
`from moq_core import AbsoluteLocationFilter`.
For a separate virtual environment, install **both** local packages together (from the repository root):

```sh
mise exec -- uv venv /tmp/relay-quickstart-venv
mise exec -- uv pip install --python /tmp/relay-quickstart-venv/bin/python -e ./python/packages/moq-core -e ./python/packages/mux-relay
/tmp/relay-quickstart-venv/bin/python -c 'import mux_relay; print("Local Relay SDK ready")'
```

Choose your application's environment path in place of the temporary path when adopting the SDK.
Installing only `mux-relay` would leave its unpublished `moq-core` dependency unresolved.

## Subscribe and publish separately

Use two terminals in the same checkout. Set `RELAY_URL` and `RELAY_NAMESPACE` in **each** terminal
using the prompts above, with the exact same values. Each process prompts for its own token; the
subscriber needs subscribe permission and the publisher needs publish permission. Start them within
60 seconds of each other, in either order. A subscriber started first retries fresh-namespace 404s;
a publisher started alone waits for demand and then fails at the overall deadline.

Subscribe to one object:

```sh
mise run python:example:mux-global -- --mode subscribe --url "$RELAY_URL" --namespace "$RELAY_NAMESPACE" --track ticker --count 1 --timeout 60
```

Publish one object from the other terminal:

```sh
mise run python:example:mux-global -- --mode publish --url "$RELAY_URL" --namespace "$RELAY_NAMESPACE" --track ticker --count 1 --timeout 60
```

Success is `finished published=0 received=1` in the subscriber and `finished published=1 received=0`
in the publisher. Both modes use the same bounded error handling and cleanup as the combined run.
See [demo options](examples/mux_global/README.md) for multiple objects and intervals.

## If the run fails

- A connection or handshake failure: check the endpoint, Relay JWT expiry/permissions, TLS trust,
  and QUIC/UDP reachability. The CLI redacts tokens; keep them out of diagnostic reports.
- Repeated `404 Broadcast not found`: check that the publisher is still running and both sides
  use the exact same authorized namespace and track. The demo's backoff stops at the run deadline.
- `waiting ... no relay demand yet` until the deadline: start the matching subscriber, and verify
  the token's namespace prefix and publish/subscribe permissions.
- A write failure after demand: delivery may be partial. The demo stops without resending. Decide
  on fresh data or an explicit application replay policy before trying again.

## Subscription lifecycle

A subscriber connection can only subscribe; publishing uses a separate publisher connection. The
names both SDKs use for each concept, and why, are recorded in
[SDK vocabulary](../spec/vocabulary.md).

`subscribe` returns a `RelaySubscription` immediately. Await `subscription.subscribed` for the
relay's answer: it returns the acceptance info (`on_subscribed` receives the same) and raises
`SubscriptionRequestError` on rejection or request timeout, `SubscriptionConnectionClosedError`
if the connection ends first, or your own reason if you cancelled. Await `subscription.ended` to
learn that the subscription can no longer deliver and why: it settles once, never raises, with
`kind` (`rejected`, `terminated` or `cancelled`), the protocol `cause`, whether it had been
accepted, and the error. Both are `SubscriptionAwaitable` objects rather than bare futures: any
number of tasks can await them, a cancelled waiter or a timed-out `asyncio.wait_for` leaves the
result intact, and `done()`, `result()` and `add_done_callback()` read it without allocating.

```python
subscription = subscriber.subscribe(namespace, track, RelaySubscriptionHandlers(on_object=handle))
await subscription.subscribed  # raises on rejection, closure, or cancel
subscription.ended.add_done_callback(lambda done: print("subscription", done.result().kind))
```

Cancelling a subscription stops that consumer (including trailing objects after `on_publisher_done`),
while closing the connection stops all of its subscriptions. `on_publisher_done` settles neither
awaitable: it is the publisher's control notification, trailing objects can still arrive, and the
request stays open until you cancel it or the connection ends. The demo waits for its object count
or the deadline. The full state table is in the
[behavior contract](../spec/behavior.md#subscription-lifecycle).

Namespace fields are preserved verbatim. Known tracks can be subscribed to directly; the SDK neither
interprets the payload nor assumes that a track contains audio, video, or timestamps. An accepted
data subscription has no application inactivity deadline. The request timeout only bounds its
initial response. An idle connection still depends on peer traffic or transport-provided keepalive;
the SDK does not schedule its own keepalive. The Python transport uses aioquic's default 60-second
local idle timeout, subject to peer negotiation.
Subscriptions have independent cancellation; the application owns connection shutdown.

Object callbacks are plain synchronous callables invoked as streams deliver objects. The SDK adds no
application queue, ordering across streams, automatic JSON decoding, clock synchronization, or media
scheduling. Use `subscribe_events` below to await work without blocking delivery. Handle subgroup resets and connection closure explicitly; this API makes no exactly-once
or gap-free delivery promise. `on_publisher_done` is the publisher's control notification: it may
precede trailing objects in transit, and it does not end the subscription. Cancel the subscription
when you are finished with it.

`subscription.update(parameters)` changes what an accepted subscription delivers (REQUEST_UPDATE);
`subscription.update(MessageParameters(forward=0))` pauses forwarding, for example, and the relay
acknowledges through `on_update_ok`. The protocol allows updates only after acceptance, so an update
issued before `on_subscribed` is queued in order and sent as soon as the relay accepts; at most eight
wait, and a further one raises a `RelayError` with code `UPDATE_QUEUE_FULL`. Updating an ended
subscription raises `RelayError` with code `CLOSED`. A refused update arrives as `on_error` and ends the
subscription ([#94](https://github.com/muxinc/relay-sdks/issues/94)).

Re-subscribing after `on_publisher_done` creates a new request and can replay cached objects followed
by another done notification while the publisher is absent. Apply retry backoff to done callbacks
as well as retryable request errors, and define any continuity policy in the application. See
[publisher restarts and resubscription](../spec/relay-interop.md#publisher-restarts-and-resubscription)
for retry, trailing-object, and publisher-generation guidance.

`connect_relay_subscriber` has one deadline (`connect_timeout`, seconds) covering token resolution and both
handshakes. The optional `CancellationToken` controls the connection's entire lifetime: cancelling
it closes an established connection, and the `closed` future carries the reason. An explicit `jwt`
query parameter takes precedence over `get_token`, matching the current Relay connection
authentication convention. In-flight operations re-raise the application's exact cancellation reason;
with `cancel()` this is `Cancelled`, which is not a `RelayError`. Handle lifetime cancellation
separately from SDK errors (compare the exception to your application's `CancellationToken.reason` when stopping normally).
`RelaySubscriber` is also an async context manager that closes on exit.

See [Errors and recovery](#errors-and-recovery) for stable SDK error codes, phases, and preserved
transport causes.

Namespaces are lists or tuples of 1–32 non-empty string fields, sent verbatim without splitting on
`/`; a single `str` is a `TypeError`, not a one-field namespace. The 32-field protocol limit is pinned
in [protocol.json](../spec/protocol.json) and exceeding it is a `ValueError`. Track names must be
non-empty strings. Both rules apply to publishing and subscribing alike, and are checked before the
token provider, the transport factory, or any connection state is consulted; see the Arguments table
in [behavior.md](../spec/behavior.md#arguments).

## Consume asynchronously

`subscribe_events` creates a subscription owned by a single async iterator. Always use its async
context manager around the loop so a `break`, consumer exception, or task cancellation releases it:

```python
async with subscriber.subscribe_events(["sensors", "lab"], "readings", capacity=32) as events:
    async for event in events:
        if event.type == "object":
            await process_reading(event.value.payload)
        elif event.type == "subgroup_reset":
            print("Objects may be missing", event.value)
        elif event.type == "publisher_done":
            print("Publisher finished; trailing objects may arrive", event.value)
        # break is safe inside async with; consumer exceptions propagate after cleanup.
```

Python's `async for` alone does **not** close an iterator on `break`. Alternatively use
`try/finally: events.close()` or `contextlib.aclosing(events)`. Cancelling a task awaiting `anext()`
closes this subscription too. This includes expiry of `asyncio.timeout()` or
`asyncio.wait_for(anext(events), ...)`: the caller receives `TimeoutError`, the subscription is
cancelled, and subsequent reads return EOF. Use these deadlines to end consumption, not to skip an
idle interval and resume it. Optional `cancellation=consumer_token` accepts a separate
`CancellationToken` for this iterator without affecting the connection. The loop propagates the
cancellation reason and consumer exceptions to its caller.

The default capacity is **64 events**, including lifecycle events; it is a count limit, not a byte
budget. Objects may be up to 16 MiB plus properties. Overflow discards queued events, cancels the
subscription and raises `SubscriptionOverflowError`. Request failures raise
`SubscriptionRequestError`; connection closure raises `SubscriptionConnectionClosedError`, even
for a clean connection close, because closure does not prove all objects arrived, and before
acceptance as well as after it. Errors propagate out of the loop for the caller to handle. `publisher_done` is an event, never EOF. Arrival order is preserved without ordering
across streams or transport-wide backpressure. Early exit releases the owned subscription and
observers without closing `subscriber` or other consumers. Only one pending `anext()` is supported. See
the [complete event and ownership contract](../spec/behavior.md#async-subscription-consumption).

Passing an `async def` subscription callback raises `TypeError` before sending. A synchronous
callback returning an awaitable emits `RuntimeWarning`; returned coroutines are closed and returned
Futures/Tasks are cancelled with their exceptions observed. Callbacks are never awaited. Use
`subscribe_events` for async consumers. Handlers are snapshotted at subscription creation; later
assignments to the handler object do not change that subscription.

## Publishing lifecycle

A dedicated publisher connection serves one namespace with multiple tracks. It uses a separate
transport from subscriber connections. No camera, microphone, catalog, or timestamps are required.

Publishing follows Relay's announce-and-serve ingest flow. `register_track()` registers the track
and sends nothing; `write()` waits for the relay to subscribe upstream (demand) before sending
bytes. Start a subscriber to create that demand. The default 10-second write deadline (`write_timeout`) covers both demand and
transport writes. A `RelayError` with code `WRITE_TIMEOUT` and `phase == "waiting-for-demand"` means
this attempt sent no object bytes; choose the next write using application freshness and
deduplication policy. The code alone does not prove replay is safe. If the relay has solicited
namespace prefixes but none covers this publisher's namespace, the demand deadline instead raises
`NAMESPACE_NOT_SOLICITED`: check the token and namespace instead of blindly retrying. The track stays
open, and a later matching solicitation can let it proceed. An ordinary absence of subscriber
demand still raises `WRITE_TIMEOUT`. A failed or cancelled write resets its stream and leaves
`track.closed` false while the publisher connection remains open. A relay subscription change
raises `SUBSCRIPTION_CHANGED`; a transport deadline raises `WRITE_TIMEOUT`. The next explicit write uses
a new group ID. Choose fresh data or the next random-access point; already delivered bytes cannot
be withdrawn. If demand disappeared, the next write waits for a new relay subscription, and
`demand_changes()` continues reporting changes.

Each write copies its bytes and sends one independent group containing one object. Await each
write; a concurrent call raises `WRITE_PENDING` rather than growing a queue. Completion
means the transport accepted the complete group, not that a viewer acknowledged it. There is no
publisher-side replay or retention after an object is sent. Late-subscriber behavior also depends on
relay caching and subscription filters. A failed write may have delivered partial data; there is no
automatic retry.

Optional `track_properties` on `register_track()` and `properties` on `write()` carry explicit protocol
metadata as `KeyValuePair` values. Property types and even-type values must be non-negative integers
up to `2**64 - 1`, the largest value the core's varint carries (`bool` is not an integer here);
odd-type values must expose a bytes-like buffer and are copied. Invalid properties raise `TypeError`
naming the property index before a write starts or a track is registered. A byte value longer than
65535 bytes, or a list whose encoding exceeds 1 MiB, is a `ValueError` at the same point: the SDK
measures the encoding with the core's own encoder, so an oversized property never opens a stream.
The SDK adds no default timescale, timestamp, or LOC media packaging. Objects are limited to 16 MiB
of payload.

Properties are encoded as supplied, but subscribers see the relay's output. With relay 0.14.17,
tracks without a declared Timescale (`0x08`) received relay-generated timestamps and a microsecond
timescale. Declaring `track_properties=[KeyValuePair(0x08, 1_000_000)]` preserved supplied Timestamp
(`0x10`) values in the tests; other tested object properties, including Video Config, were dropped.
Missing timestamps were synthesized even on declared tracks. Keep application metadata that must
arrive intact in payloads, and read the received track timescale when interpreting timestamps. See
[Relay property handling](../spec/behavior.md#relay-property-handling) for the version-specific
observations and media guidance.

`events.close()` cancels an outstanding write and ends that track; other tracks
keep running. `await publisher.close()` ends all tracks, retracts the namespace, and waits for
transport shutdown. Track names cannot be reused within a publisher connection, including after
their writer closes.

### Advanced media groups

For media, a track needs more than one object per group: LOC/MSF maps one GOP to one MOQT group
whose object 0 is the keyframe. `open_group()` waits for relay demand like `write()`, then returns a
`RelayGroupWriter` that appends objects with increasing IDs and FINs the group on `close()`.
Opening the group, each `group.write()`, and `group.close()` get a separate `write_timeout`
deadline. Time between calls does not count toward a deadline. A timeout resets that group and
raises `WRITE_TIMEOUT`; the track stays open for a new group at the next keyframe:

The following is an application sketch, not the data quickstart. `loc_properties` belongs to
[the Video.js example](examples/videojs_stream/moq_media.py), not `mux_relay`; the encoded frames,
timestamps, and codec configuration come from your encoder. Making this media recipe standalone
is tracked separately in [#29](https://github.com/muxinc/relay-sdks/issues/29).

```python
video = publisher.register_track("video", track_properties=[KeyValuePair(0x08, 1_000_000)])

if video.has_demand:  # the relay currently has an upstream subscription for the track
    group = await video.open_group()
    await group.write(keyframe_bytes, properties=loc_properties(timestamp_us, video_config=avcc))
    await group.write(delta_bytes, properties=loc_properties(next_timestamp_us))
    await group.close()
```

A group that fails part-way (the relay's subscription changed or ended) is reset rather than
completed, and the track stays open; start the next group at the next keyframe. `demand_changes()`
iterates relay demand: the current state first, then `True` when the relay subscribes or replaces
its subscription (so `True` can repeat) and `False` when its subscription ends, which is how a
catalog track re-sends its document on each relay subscription:

```python
async for demanded in catalog.demand_changes():
    if demanded:
        await catalog.write(catalog_document)
```

Demand is the relay's own subscription, not a viewer count and not a liveness signal. The relay
fans one upstream subscription out to every downstream viewer, so a second viewer produces no new
`True` and is served from the relay's cache; and it keeps the subscription for a while after the
last viewer leaves (about 30 seconds on the tested relay), so `has_demand` lags the last viewer and
`True` does not prove anyone is watching. `examples/videojs_stream` puts all of this together for a
Video.js player.

Run the data-only publishing/subscribing integration tests without a live relay:

```sh
cd python && mise exec -- uv run pytest packages/mux-relay/tests/test_publish.py
```

### Text and JSON

Track and group writers provide `await writer.write_text(text, properties=...)` and
`await writer.write_json(value, properties=...)`. Each call writes one UTF-8 object using the same
demand, deadline, cancellation, and encoded byte-size limits as `write()`. Group helpers also accept
`status`. Serialization errors leave the writer usable.

Use the exported readers explicitly in subscription callbacks:

```python
from mux_relay import RelaySubscriptionHandlers, read_json, read_text

text_handlers = RelaySubscriptionHandlers(on_object=lambda obj: print(read_text(obj)))
json_handlers = RelaySubscriptionHandlers(on_object=lambda obj: print(read_json(obj)))
```

`read_text(obj)` preserves a leading U+FEFF and raises `UnicodeDecodeError` on malformed UTF-8.
`read_json(obj)` parses UTF-8 JSON without schema validation and raises `ValueError` on invalid
input (including `UnicodeDecodeError` and `JSONDecodeError`). Catch decoding errors inside the
callback if malformed objects should be skipped. Empty text is valid; empty JSON is not. Readers
leave payload bytes and metadata unchanged, and the SDK adds no content-type properties.

JSON writing uses compact `json.dumps` with `ensure_ascii=False` and `allow_nan=False`; unsupported
values, cycles, non-finite numbers, and unpaired surrogates fail locally. Reading also rejects
`NaN` and `Infinity`. Other conversion rules are Python's standard JSON behavior. Text writing uses
strict UTF-8 encoding. Raw byte `write()` and subscriptions remain available.

## Errors and recovery

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

Python raises one `RelayError`, with the `code` above and a `phase`, exactly as TypeScript does; the
`RelayErrorCode` and `RelayErrorPhase` aliases type them. The only subclasses are the three
subscription errors that carry payload (`SubscriptionRequestError`, `SubscriptionOverflowError`,
`SubscriptionConnectionClosedError`). Branch on `code`, never on the message.

Wrapped transport/session failures preserve the original exception in `__cause__`. The lower-level
WebTransport API's `WebTransportError` (in `mux_relay.webtransport`) is the transport's own
exception, not a `RelayError`; it keeps its numeric `code` and `reason`, and a failure surfaced
through the Relay connection/publishing API wraps it in a `RelayError` with `code == "TRANSPORT_ERROR"`
and the `WebTransportError` as `__cause__`. Explicit caller cancellation, including
`asyncio.CancelledError`, is passed through unchanged.

```python
from mux_relay import RelayError

try:
    await track.write_text("current state")
except RelayError as error:
    if error.code != "WRITE_TIMEOUT":
        raise
    print("Write deadline expired during", error.phase)
    # Choose what to publish next using phase and application freshness/deduplication policy.
```

## Transport

`connect_relay_subscriber` and `connect_relay_publisher` default to `mux_relay.connect_webtransport`, a
WebTransport-over-HTTP/3 client on aioquic: extended CONNECT with `WT-Available-Protocols: "moqt-20"`,
session-bound streams, CLOSE_WEBTRANSPORT_SESSION on shutdown. TLS verification uses the system CA
bundle that ships with `certifi`. Pass `create_transport` to supply a `QuicConfiguration` (custom
CAs, QUIC parameters) or any other implementation of the `moq_core.MoqtTransport` seam:

```python
from aioquic.quic.configuration import QuicConfiguration
from mux_relay import connect_relay_subscriber, connect_webtransport

configuration = QuicConfiguration(is_client=True, cafile="/path/to/ca.pem")
subscriber = await connect_relay_subscriber(
    url, create_transport=lambda url, protocols: connect_webtransport(url, protocols, configuration=configuration)
)
```

`packages/mux-relay/tests/test_webtransport.py` runs the full smoke-test assertion list over a real
QUIC loopback: a WebTransport server on `127.0.0.1` (aioquic, self-signed certificate) fronting the
in-memory relay hub, so the transport adapter is exercised end to end in `python:check`.

## Video.js media example

[videojs_stream](examples/videojs_stream/README.md) streams live H.264 (a generated test pattern or
transcoded file, with optional Opus audio) in the MSF/LOC shape consumed by Video.js. It needs the
`examples` dependency group (PyAV, numpy), which `uv sync` installs by default. Follow that example's
setup after completing the data quickstart; media helpers and player integration are separate topics.

## Package boundaries

- `packages/mux-relay`, `mux_relay`, owns the Relay product API, the authentication convention, and
  the aioquic transport.
- `packages/moq-core`, `moq_core`, is an independently buildable, Apache-2.0 generic protocol
  package. It declares no dependencies and imports no transport, player, UI, or media engine. It
  works over the abstract `moq_core.transport.MoqtTransport` seam that `mux_relay` supplies.
- `subscriber.track_source` exposes the generic `MoqTrackSource` contract for future integrations:
  track requests in protocol terms (with the core's `on_ok`/`on_done` handler names) and closure
  observation, with no track listing and no operation to close the shared connection.
- import-linter forbids `moq_core` from importing `mux_relay` or any transport library; the
  contracts are in `pyproject.toml`. `tests/test_boundaries.py` pins the declared dependency lists.
- `packages/moq-core/tests/test_spec_protocol.py` and `test_spec_vectors.py` check the core against
  the shared `../spec`, which is how this SDK stays in lockstep with the other languages.

The core is a port of the TypeScript core, which is itself an attributed snapshot of the Video.js
protocol modules and their regression tests. See `packages/moq-core/UPSTREAM.json`.

## Differences from the TypeScript SDK

The observable behavior is the same; the spelling follows Python conventions:

| TypeScript | Python |
| --- | --- |
| `connectRelaySubscriber({ url, getToken, signal, connectTimeoutMs })` | `connect_relay_subscriber(url, get_token=..., cancellation=CancellationToken(), connect_timeout=seconds)` |
| `subscriber.subscribeEvents(options, { capacity, signal })` | `subscriber.subscribe_events(namespace, track, capacity=..., cancellation=...)` |
| `subscriber.subscribe({ namespace, track, parameters }, handlers)` | `subscriber.subscribe(namespace, track, RelaySubscriptionHandlers(...), parameters=...)` |
| `onSubscribed`, `onPublisherDone` | `on_subscribed`, `on_publisher_done` |
| `publisher.registerTrack({ track, trackProperties, writeTimeoutMs })` | `publisher.register_track(track, track_properties=..., write_timeout=seconds)` |
| `writer.write(bytes, { properties })` | `await writer.write(payload, properties=...)` |
| `write()` takes any `ArrayBufferView`; a string, `ArrayBuffer`, or array raises `TypeError` | `write()` takes any object with the buffer protocol (`bytes`, `bytearray`, `memoryview`, ...); a `str` or `list[int]` raises `TypeError` |
| `subscriber.closed` resolves `{ error? }` | `await subscriber.closed` returns `Closed(error=...)` |
| `AbortSignal` | `CancellationToken` (`cancel(reason)` closes every connection given the token) |
| `RelayError` with `code` and `phase` | `RelayError` with `code` and `phase` |
| Varints capped at 2^53-1 | full 64-bit range (Python integers are exact) |
| `openGroup()`, `hasDemand`, `demandChanges()`, `writer.closed` | `open_group()`, `has_demand`, `demand_changes()`, `writer.closed` (see `spec/behavior.md`) |
| `subscriber.trackSource` | `subscriber.track_source` |
| `writeText()`, `writeJSON()` | `write_text()`, `write_json()` on track and group writers |
| `readText(object)`, `readJSON(object)` | `read_text(obj)`, `read_json(obj)` |
| Browser `WebTransport` | aioquic WebTransport client (`connect_webtransport`) |

## Initial scope

This slice supports one connection, arbitrary reliable-stream track subscriptions, subscription
updates and cancellation, plus data-only publication on a dedicated connection. The generic core
also exposes FETCH and TRACK_STATUS. The protocol is pinned to `moqt-20`.

Automatic reconnection, token refresh on a live session, datagram reception, and product resource
discovery are not implemented yet. A GOAWAY ends the connection with a `RelayError` of code `GOAWAY`;
subscriptions still awaiting their first response end with it (`subscribed` raises
`SubscriptionConnectionClosedError`, `ended` resolves `terminated`), never through `on_error`;
callers can open a replacement.

## Live Relay smoke test

This opt-in test publishes and subscribes against a real relay. It is excluded from `python:check`
and needs a token authorized for both operations. The token can be supplied through a hidden prompt
or the `RELAY_TOKEN` environment variable; it is never written to a file and is redacted from output.

Set `RELAY_URL` and `RELAY_NAMESPACE` using the quickstart prompts first. Your token must
authorize both operations under that namespace prefix, including children: the test appends a
random UUID. Running this command explicitly opts into contacting that endpoint.

```sh
RELAY_URL="$RELAY_URL" RELAY_NAMESPACE_PREFIX="$RELAY_NAMESPACE" mise run python:test:relay
```

The test checks binary,
empty, and text payloads plus independent track closure, then closes both connections. It defaults to
`https://sjc.relay.mux.global`; override that with `RELAY_URL` if needed. Namespace fields sent by
the SDK are absolute: include the token's root prefix when required by the relay.

The live test passed against `sjc.relay.mux.global` on 2026-09-11 over the aioquic WebTransport
transport: three objects on `events` and two on `status`, with the second status object delivered
after the events track closed, and both connections closed cleanly.

## Development

Tool versions come from the repository's root `mise.toml`. The Python toolchain is the Astral stack:
uv for the workspace and lockfile, ruff for linting and formatting, ty for type checking.

```sh
mise install              # once
mise run python:check     # uv sync, ruff check, ruff format --check, lint-imports, ty check, pytest
mise run python:test      # pytest only
```

From this directory under `mise exec`, the native commands are `uv sync`, `uv run pytest`,
`uv run ruff check .`, `uv run ruff format .`, `uv run ty check`, and `uv run lint-imports`. Run
`uv lock` after changing a dependency and commit `uv.lock`.
