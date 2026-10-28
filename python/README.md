# Relay Python SDK

A standalone product SDK for Relay. Connect to a Relay endpoint and subscribe to arbitrary track
objects, or publish data, from any Python 3.11+ asyncio program. No browser is involved: the SDK
speaks MOQT directly on QUIC (native QUIC) through aioquic.

This is the Python SDK in the Relay SDKs repository. The shared protocol pins, behavior contract, and
test vectors live in [spec](../spec/README.md); the [root README](../README.md) describes the suite. Package names are provisional
and publication is disabled.

## First round trip

The packages are **unpublished**. Use the local uv workspace below; there is no registry install
step. You need Git and [mise](https://mise.jdx.dev/getting-started.html) on PATH. The SDK requires
Python 3.11+; mise installs the repository's pinned Python and uv. Commands below run in Bash from
the repository root. The default transport is native QUIC through aioquic, not a browser, HTTP/3
or a WebSocket. The endpoint and network must allow QUIC/UDP traffic.

### Endpoint and credentials

Ask the team operating your Relay deployment (or your application's backend owner) for an HTTPS
Relay endpoint, your token's root (the namespace prefix it grants), and a
short-lived Relay JWT authorized to **publish and subscribe** there. The SDK consumes tokens; it
does not issue them or provide a public token signup flow. A Mux API access token is not a Relay JWT.
Use an endpoint whose path is your token's root (for example
`https://relay.mux.global/acme/rooms/42` for a token rooted at `acme/rooms/42`), and name
namespaces relative to it (`["p", "a"]` rather than `["acme", "rooms", "42", "p", "a"]`). The
connection then cannot reach outside that root, and a path the token does not grant fails at
connect with `UNAUTHORIZED`. A client that spans several rooms under one grant can use the bare
host, where namespace fields include the token's root
([namespaces](../spec/behavior.md#subscribing)). The Mux relays carry announcements between regions,
so a publisher and a subscriber can each use their nearest relay
([a viewer on another relay](../spec/relay-interop.md#a-viewer-on-another-relay)).
The hosted demo defaults to the bare host `https://relay.mux.global`, which still requires authorized credentials.
For another deployment, use the endpoint supplied by its operator.

`room_endpoint` builds that endpoint from the token's root, so the URL is not assembled by hand.
Given only the root, it roots `DEFAULT_ENDPOINT`, `https://relay.mux.global`; pass an endpoint first
for another relay:

```python
from caprid_relay import room_endpoint

url = room_endpoint(["acme", "rooms", "42"])
# 'https://relay.mux.global/acme/rooms/42'
url = room_endpoint("https://ord.relay.mux.global", ["acme", "rooms", "42"])
# 'https://ord.relay.mux.global/acme/rooms/42'
```

It appends each field to the endpoint's path and keeps the endpoint's query. It encodes nothing,
because the relay reads the path without decoding it. A field may hold only letters, digits and
`-._~!$&'()+,;=:@`, and may not be `.` or `..`; any other field raises `ValueError`. Reach a room
with such a field from the bare host
([room-rooted endpoints](../spec/behavior.md#room-rooted-endpoints)).

The demo reads `RELAY_TOKEN` when set, otherwise prompts with hidden input. Prefer the prompt for
manual runs; automation can inject the environment variable through its secret manager. Do not put
tokens in commands, source files, URLs, or committed `.env` files. The demo passes the token to
`get_token` and redacts it from output. Authentication currently uses `?jwt=<token>` on the transport
URL: scrub query values and token strings before collecting network URLs, traces, or exception
reports. Browser consumers have an additional [console exposure risk](../ts/GUIDE.md#browser-token-exposure).

### Install and run

```sh
git clone https://github.com/caprids/relay-sdks.git
cd relay-sdks
mise install
mise run python:install
```

A publisher's `write()` waits for relay demand: a matching subscriber must be running. Even when
publishing connects first, a fresh namespace can briefly reject subscriptions with request error 16
`unroutable` (404 `Broadcast not found` on relays before 0.15.8) while its announcement propagates,
about a round trip. The combined demo starts both sides, waits for the publisher's `announced`,
retries only that subscription rejection with capped backoff, and waits for demand before writing.
Once a write starts, a failure stops the run; it never automatically replays potentially delivered
bytes.

Enter the endpoint, ending in your token's root, and a **fresh namespace below it** when prompted
below. The CLI splits `quickstart/unique-run` into `["quickstart", "unique-run"]`; the SDK itself sends
fields verbatim. Both connections use those same fields and track `ticker`. If your token authorizes
only one exact namespace, enter the bare host as the endpoint and that whole namespace (a namespace
needs at least one field), and avoid overlapping publishers or stale cached objects.

```sh
read -r -p "Relay HTTPS endpoint with your token's root as its path: " RELAY_URL
read -r -p 'Namespace below that root (slash-separated): ' RELAY_NAMESPACE
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
`from caprid_relay import connect_relay_subscriber, connect_relay_publisher` resolves the local packages.
`caprid_relay` re-exports the core types its own signatures use (`MoqtObject`, `MessageParameters`,
`SubscribeOk`, `RequestError`, `Closed`, and the rest of the "Protocol types borrowed from the core"
row in [the vocabulary](../spec/vocabulary.md)); other protocol types, such as the location filters
for `subscribe(parameters=...)`, come from the core package, which the workspace installs alongside:
`from moq_core import AbsoluteLocationFilter`.
For a separate virtual environment, install **both** local packages together (from the repository root):

```sh
mise exec -- uv venv /tmp/relay-quickstart-venv
mise exec -- uv pip install --python /tmp/relay-quickstart-venv/bin/python -e ./python/packages/moq-core -e ./python/packages/caprid-relay
/tmp/relay-quickstart-venv/bin/python -c 'import caprid_relay; print("Local Relay SDK ready")'
```

Choose your application's environment path in place of the temporary path when adopting the SDK.
Installing only `caprid-relay` would leave its unpublished `caprid-moq-core` dependency unresolved.

## Subscribe and publish separately

Use two terminals in the same checkout. Set `RELAY_URL` and `RELAY_NAMESPACE` in **each** terminal
using the prompts above, with the exact same values. Each process prompts for its own token; the
subscriber needs subscribe permission and the publisher needs publish permission. Start them within
60 seconds of each other, in either order. A subscriber started first retries fresh-namespace
rejections; a publisher started alone waits for demand and then fails at the overall deadline.

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
- Repeated request error 16 `unroutable` (or `404 Broadcast not found` on older relays): check that
  the publisher is still running and both sides use the exact same authorized namespace. Error 16
  `not found` means the namespace is announced but the track is not: check the track name. The
  demo's backoff stops at the run deadline.
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
accepted, and the error. Both are `RelayAwaitable` objects rather than bare futures, as are the
publisher's `announced` and every connection's `closed`: any number of tasks can await them, a
cancelled waiter or a timed-out `asyncio.wait_for` leaves the result intact, and `done()`,
`result()` and `add_done_callback()` read it without allocating.

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
initial response. The SDK schedules no application-level keepalive, but the aioquic transport sends
a QUIC PING once it has sent nothing for 5 seconds, on publisher and subscriber connections alike.
That keeps an idle connection open, and lets a receive-only subscriber survive a NAT rebinding: the
relay keeps sending to the old address until a packet from the new one arrives. The Python transport
uses aioquic's default 60-second local idle timeout, subject to peer negotiation.
Subscriptions have independent cancellation; the application owns connection shutdown.

A subscription without a location filter (no `parameters`, or `NoLocationFilter()`) starts at the
live edge on Relay: a subscriber that joins an ongoing track receives only the objects published
after it joined, although the relay still holds earlier ones. To reach back, pass
`MessageParameters(location_filter=RelativeGroupFilter(groups_before_next=N))` (`1` starts at the
current group, `N` at N-1 groups before it) or an `AbsoluteLocationFilter` with an explicit start;
the relay serves those from its cache, as far back as it still holds. Both filters come from
`moq_core`. See [late subscribers](../spec/relay-interop.md#late-subscribers-and-location-filters).

Object callbacks are plain synchronous callables invoked as streams deliver objects. The SDK adds no
application queue, ordering across streams, automatic JSON decoding, clock synchronization, or media
scheduling. Use `subscribe_events` below to await work without blocking delivery. Handle subgroup resets and connection closure explicitly; this API makes no exactly-once
or gap-free delivery promise. The SDK reports every reset it receives, but the relay may drop a
reset for a subgroup it had not begun forwarding, so an aborted group can leave no trace except a
gap in group IDs ([subgroup resets](../spec/relay-interop.md#subgroup-resets)). `on_publisher_done` is the publisher's control notification: it may
precede trailing objects in transit, and it does not end the subscription. Cancel the subscription
when you are finished with it.

The SDK sends every `MessageParameters` field you set, but moq-relay acts on only some of them. It
honors `subscriber_priority` and `location_filter` on the subscribe, and ignores `group_order` (the
relay always serves Descending), `new_group_request`, both delivery timeouts and a per-request
`authorization_tokens`. With `new_group_request` ignored there is no protocol key-frame request through
the relay: an application that needs one sends it to the publisher on a track of its own. See
[subscription parameters](../spec/relay-interop.md#subscription-parameters).

For media, `caprid_relay.media.media_subscribe_parameters(kind)` gives a subscription the subscriber
priority that puts audio ahead of video when the viewer's downlink is congested: 16 for audio, 96
for video, the values `media_track_properties` gives the publisher. moq-relay orders what it sends a
viewer by subscriber priority alone and ignores the publisher's, so without it audio waits behind
video's backlog. Pass other parameters as `overrides`; a `subscriber_priority` there replaces the
default. See [media subscribing](../spec/behavior.md#media-subscribing).

```python
from caprid_relay.media import media_subscribe_parameters

audio = subscriber.subscribe(namespace, "audio", handlers, parameters=media_subscribe_parameters("audio"))
video = subscriber.subscribe(namespace, "video", handlers, parameters=media_subscribe_parameters("video"))
```

`subscription.update(parameters)` sends REQUEST_UPDATE, the protocol's way to change what an
accepted subscription delivers. **moq-relay does not implement it**: the relay never acknowledges an
update, and any update, including `MessageParameters(forward=0)` or an empty one, ends the
subscription within one round trip with `on_publisher_done` (status 2 `track ended`) and stops
delivery; `on_update_ok` never fires. To pause, cancel the subscription and subscribe again later.
To change the track, the priority or the filter, use a [switchable subscription](#switching-tracks),
which opens the new subscription before it cancels the old one
([relay interop deviation 4](../spec/behavior.md#relay-interop-deviations)). Subscribing with
`parameters=MessageParameters(forward=0)` does not work either: moq-relay refuses it with request
error 3 `FORWARD=0 not supported` through `on_error`, and the connection stays open. The protocol allows
updates only after acceptance, so an update issued before `on_subscribed` is queued in order and
sent as soon as the relay accepts; at most eight wait, and a further one raises a `RelayError` with
code `UPDATE_QUEUE_FULL`. Updating an ended subscription raises `RelayError` with code `CLOSED`. A relay
that refuses an update with REQUEST_ERROR reaches `on_error`, which ends the subscription.

Re-subscribing after `on_publisher_done` creates a new request, and it cannot bring back a finished
track. Once the publisher has gone the relay answers request error 16 `unroutable`; while its
namespace stays announced, the relay replays the finished track's cached last group followed by
another done, one round trip later each time. Don't re-subscribe to a track the publisher ended
until the application knows new content exists; a status-2 done alone does not show that, since
moq-relay also sends one after this subscription's own `update()`, and then re-subscribing is the
way to apply the change. Apply retry backoff to done callbacks as well as retryable
request errors, and define any continuity policy in the application. See
[publisher restarts and resubscription](../spec/relay-interop.md#publisher-restarts-and-resubscription)
for retry, trailing-object, and publisher-generation guidance.

`connect_relay_subscriber` has one deadline (`connect_timeout`, seconds) covering token resolution and both
handshakes. On Linux, a relay address that refuses the connection (nothing listens on its UDP port)
fails at once with `TRANSPORT_ERROR` / `connecting` and the OS's `ConnectionRefusedError` in the
`__cause__` chain, as in TypeScript. Elsewhere (macOS, for one), and for an address that never
answers, the attempt waits out the deadline and fails with `CONNECTION_TIMEOUT`. The optional `CancellationToken` controls the connection's entire lifetime: cancelling
it closes an established connection, and the `closed` future carries the reason. An explicit `jwt`
query parameter takes precedence over `get_token`, matching the current Relay connection
authentication convention. An empty one (`?jwt=`) is a `ValueError` before anything connects: put a
token in it, or drop it and use `get_token`. So is an endpoint with another scheme, a fragment, or
userinfo; an endpoint that is not a string holding an absolute URL with a host is a `TypeError`.
Every deadline (`connect_timeout`, `request_timeout`, and `write_timeout`) must be between 0
(exclusive) and 2147483.647 seconds, the same bound the TypeScript SDK's timers impose. A value
outside it is a `ValueError`; a value that is not a number (including a `bool`) is a `TypeError`. In-flight operations re-raise the application's exact cancellation reason;
with `cancel()` this is `Cancelled`, which is not a `RelayError`. Handle lifetime cancellation
separately from SDK errors (compare the exception to your application's `CancellationToken.reason` when stopping normally).
`RelaySubscriber` is also an async context manager that closes on exit.

See [Errors and recovery](#errors-and-recovery) for stable SDK error codes, phases, and preserved
transport causes.

### Token expiry

The relay ends a session when its token expires, and a live session cannot take a new token. Every
connection exposes `expires_at`, the time that will happen: from the JWT's `exp` claim, or from a
`RelayCredential` your token provider returns when the token is not a JWT or your server knows
better.

```python
from datetime import UTC, datetime

from caprid_relay import RelayCredential, connect_relay_subscriber


async def get_token() -> RelayCredential:
    token, expires = await fetch_relay_token()  # your server
    return RelayCredential(token, datetime.fromtimestamp(expires, UTC))


subscriber = await connect_relay_subscriber(url, get_token=get_token)
print("ends at", subscriber.expires_at)
```

At that time the connection closes with `UNAUTHORIZED` in phase `connection`. The primitives do not
prevent it: open a replacement before then. The [supervised connections](#stay-connected-supervised-connections)
do it for you, make-before-break, a minute before expiry by default (`rotate_before`).

Namespaces are lists or tuples of 1–32 non-empty string fields, sent verbatim without splitting on
`/`; a single `str` is a `TypeError`, not a one-field namespace. The 32-field protocol limit is pinned
in [protocol.json](../spec/protocol.json) and exceeding it is a `ValueError`. Track names must be
non-empty strings. Both rules apply to publishing and subscribing alike, and are checked before the
token provider, the transport factory, or any connection state is consulted; see the Arguments table
in [behavior.md](../spec/behavior.md#arguments).

### Switching tracks

REQUEST_UPDATE names no track, so moving a viewer between layers (`video-hi`, `video-mid`,
`video-lo`) takes a new subscription. `subscribe_switchable` keeps one set of handlers on whichever
subscription is active and moves them make-before-break: `switch_to` subscribes to the new track
while the old one keeps delivering, and cuts over once a new object passes `until`.

```python
from caprid_relay import CancellationToken, SwitchableHandlers

switchable = subscriber.subscribe_switchable(
    namespace,
    "video-mid",
    SwitchableHandlers(
        on_switched=lambda info: decoder.reset(),  # the next object starts the new layer
        on_object=lambda obj, track: decoder.decode(obj, track),
    ),
)
await switchable.subscribed


def near_playhead(obj) -> bool:
    # Your own timestamps, not group IDs: no later than the next frame, a few frames behind at most.
    t = timestamp_of(obj)
    return last_delivered - 5 * frame_duration <= t <= last_delivered + frame_duration


# Keep the old layer open for a second to deliver groups still in flight on it.
probe = CancellationToken()
asyncio.get_running_loop().call_later(5, probe.cancel)
await switchable.switch_to("video-hi", until=near_playhead, drain=1.0, cancellation=probe)
```

- Handlers see only the active subscription, and get its track name as their last argument.
  `on_switched` receives a `SwitchedInfo` (`from_track`, `to_track`, `subscribed`) before the first
  object of the new track. A switchable has no `update()`; switch to the same track with new
  `parameters` to change a filter or priority. Parameters are not inherited.
- `until` defaults to `"first-object"`. That is fine where delivery is in order, but it can skip
  media: the relay starts a subscription at its cached latest group, which can be stale, and a
  subscriber on another relay can lose the first object. Compare your own timeline, not group IDs:
  each track numbers its own groups.
- `switch_to` starts at once and returns a `RelayAwaitable`. A timed-out `wait_for` on it leaves the
  switch pending; pass `cancellation=` to abandon it. If the new subscription fails, a newer `switch_to`
  supersedes it, or the token is cancelled, the awaitable raises and the old track keeps playing.
  `ended` settles when the active subscription ends; `cancel()` cancels everything. See
  [switching tracks](../spec/behavior.md#switching-tracks) for the full contract and measured
  cutover times.

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
observers without closing `subscriber` or other consumers. Only one pending `anext()` is supported; a concurrent one raises `RuntimeError`. See
the [complete event and ownership contract](../spec/behavior.md#async-subscription-consumption).

Passing an `async def` subscription callback raises `TypeError` before sending. A synchronous
callback returning an awaitable emits `RuntimeWarning`; returned coroutines are closed and returned
Futures/Tasks are cancelled with their exceptions observed. Callbacks are never awaited. Use
`subscribe_events` for async consumers. Handlers are snapshotted at subscription creation; later
assignments to the handler object do not change that subscription.

## Stay connected: supervised connections

`connect_relay_publisher` and `connect_relay_subscriber` never reconnect or resubscribe. A live
application that has to survive network loss can use the supervisors instead. They take the same
connection options and return once their first connection is up.

```python
from caprid_relay import SupervisionBackoff, WantHandlers, connect_supervised_publisher, connect_supervised_subscriber

publisher = await connect_supervised_publisher(
    url,
    namespace=["room", participant_id],  # the root: each connection serves root + one generation field
    get_token=get_token,
    on_status=lambda status, error: print("publisher", status, error),
)
video = publisher.register_track("video")  # the same writer across reconnects

subscriber = await connect_supervised_subscriber(url, get_token=get_token)
subscriber.want(
    "alice/video",
    ["room", "alice"],
    "video",
    WantHandlers(
        on_object=decoder.push,
        on_restarted=lambda reason, namespace: decoder.reset(),  # the gap is not filled
        on_switched=lambda info: decoder.reset(),  # a newer generation took over with no gap; see below
        on_error=lambda error: print("gave up on alice/video", error),
    ),
    generations=True,
    stale_after=3.0,
)
```

The supervised publisher reconnects with backoff (from 0.5 s up to 10 s) whenever its connection
ends. Each connection is a new **generation** with its own namespace, the root plus a field that
sorts by start time. Each one gets a fresh namespace because moq-relay 0.17.0 never hands existing
subscriptions to a reconnected publisher, and can keep serving a dead one's cached objects
([publisher recovery](../spec/publisher-recovery.md)). Every registered track is registered again on
each generation. Writes are never replayed: while no generation is up, a write raises `RECONNECTING`
at once, and an operation the connection's end interrupted raises it too, so write the next object,
or the next keyframe, when it is live again. A `caprid_relay.media.FrameWriter` on a supervised track
treats it as an interruption and resumes at the next keyframe.

`want(key, namespace, track, handlers, ...)` keeps one track subscribed until `unwant(key)`. It
retries the announce race, resubscribes after `PUBLISH_DONE` or a rejection a wait may fix (from
0.25 s up to 8 s), and moves to the replacement connection. With `stale_after` (seconds), it also
resubscribes a track that went silent; set that only for tracks that are never silent while live.
With `generations=True`, it discovers the publisher's root and stays on the newest generation,
moving as soon as a newer one is announced, with no application channel. Rejections no wait fixes,
such as credentials or a malformed request, reach `on_error`, and the want stops. Backoff bounds are
`SupervisionBackoff(initial_delay=..., max_delay=...)`, given as `reconnect=` to either supervisor
or `retry=` to a want. See the [supervised contract](../spec/behavior.md#supervised-connections).

A move to a newer generation is made before the break, as `switch_to` makes one. The old
subscription keeps delivering until the new generation delivers, and `on_switched` runs at the
cutover (with a `WantSwitchedInfo`) instead of `on_restarted`. Wanting the key again with another
track or parameters moves the same way, so a supervised viewer changes layers without a gap:

```python
# Cut over once the new layer reaches what was already shown, and let the old one finish in-flight groups.
subscriber.want(
    "alice/video",
    namespace,
    "video-hi",
    handlers,
    generations=True,
    until=lambda obj: timestamp_of(obj) >= last_shown_timestamp,
    drain=1.0,
)
```

`until` and `drain` mean what they mean on [`switch_to`](#switching-tracks), and apply to every move
of the want. Compare your own timeline, never group IDs, since each generation and track numbers
its own groups. Wanting the key again with only `until`, `drain`, `stale_after`, `retry` or the
handlers changed swaps them without resubscribing. Another namespace, or turning `generations` on or
off, replaces the want.

Both supervisors **rotate** before the token expires ([token expiry](#token-expiry)), so a long
session never hits the relay's deadline. `rotate_before` seconds (default 60) before `expires_at`,
they call `get_token` again and open the replacement while the old connection still serves. The
publisher's next generation is announced, each track moves to it once viewers subscribe there, and
the old generation closes. Each want moves its subscription to the new connection as a move does;
when its target is unchanged, the cutover is invisible: no callback runs and nothing is lost or
repeated. Call `rotate()` to do it now, for example after revoking a token. See
[rotation](../spec/behavior.md#rotation).

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

Connecting announces the namespace once the first track is registered, and `publisher.announced`
settles when that announce has reached the wire: the relay acknowledges nothing more, and it routes
subscribes to the namespace about a round trip later. A subscribe that arrives before then is
rejected with request error 16 (`DOES_NOT_EXIST`), reason `unroutable` (moq-relay 0.15.8 and later;
relays through 0.15.1 answered `404 Broadcast not found`). A subscriber on another relay of the
cluster waits longer still, until its relay learns the route. The SDK does not retry a subscribe, so
a subscriber either waits for `subscriber.namespace_events(prefix)` to report the namespace
`announced`, which avoids the race, or retries that rejection within its own budget, as a supervised
subscriber's [want](#stay-connected-supervised-connections) does. A process that owns both
sides awaits `announced` before subscribing, which narrows the race but does not close it. On
moq-relay 0.17.0 a subscribe on the discovering connection once `announced` arrived was never
rejected `unroutable` (0 of 160 trials on 2026-10-06), while one sent as soon as the publisher's
`announced` settled was rejected 6 to 14 times in 40 on the publisher's relay and every time
on another relay ([measurements](../spec/relay-interop.md#announce-propagation)). With discovery, a
later `unroutable` means the namespace has gone since; wait for its next `announced` rather than
retrying on a timer ([discovery contract](../spec/behavior.md#discovery)). `announced` stays pending while no solicitation covers the namespace, and the
connection's closure terminates it if that comes first.

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
naming the property index before a write starts or a track is registered. A `ValueError` at the
same point covers: a byte value longer than 65535 bytes; `track_properties` that encode to more
than 65525 bytes (what `SUBSCRIBE_OK` has room for), hold more than 64 pairs (the relay's limit), or
carry a `0x0E` above 255 or a `0x22` other than 1 or 2 (values draft-22 rules out), any of which
would leave the track unsubscribable; and object `properties` on `write()` that encode to more than
64 KiB, which the relay drops silently. The SDK measures the encoding with the core's own encoder, so
an oversized property never opens a stream.
The SDK adds no default timescale, timestamp, or LOC media packaging. Objects are limited to 16 MiB
of payload.

Properties are encoded as supplied, but subscribers see the relay's output. moq-relay 0.17.5 and
earlier strip object properties (fixed upstream in moq #5494, not yet released): only the object Timestamp (`0x10`, or the
older `0x06`) survives, rescaled to microseconds by the declared Timescale (`0x08`); every other
object property, including Video Config, is dropped. Tracks
without a declared Timescale, and objects without a Timestamp, get relay-clock timestamps. Declaring
`track_properties=[KeyValuePair(0x08, 1_000_000)]` keeps supplied Timestamp values unchanged. Every
`SUBSCRIBE_OK` carries the relay's track properties (`0x08` = 1,000,000, `0x0E` = 128 unless the
publisher set it, `0x22` = 2), not the publisher's; the relay would refuse every subscriber to a
track whose `0x0E` or `0x22` is out of range or that has more than 64 track properties, which is why
`register_track()` refuses those. A
subscription with
`MessageParameters(include_properties=0)` gets no track properties and objects with no properties at
all, not even a Timestamp. Keep
application metadata that must arrive intact in payloads, and read the received track timescale when
interpreting timestamps. See [Relay property handling](../spec/behavior.md#relay-property-handling)
for the observations and media guidance.

Two properties also set delivery timeouts (draft-ietf-moq-transport-22 §5.2), in milliseconds:
`0x02` (`OBJECT_DELIVERY_TIMEOUT`) and `0x06` (`SUBGROUP_DELIVERY_TIMEOUT`). Set either as a track
property, or on a group's first object to override the track for that group; on any later object it
is only sent. The relay subscription's own values, if it sends any, combine with them: the smaller
non-zero value wins, 0 means no timeout, and values above 2147483647 are clamped. The object timeout
bounds an object's age from the `write()` or `group.write()` call until the transport has taken it,
which is when its stream write returns (aioquic has transmitted the bytes); waiting for demand and for
settling room count too, timed by the publisher's value alone until the group's value is fixed just
before its stream opens (an object already older than the fixed value fails then, before any stream
opens). An older object raises `DELIVERY_TIMEOUT` in its current phase and resets
its group with code `DELIVERY_TIMEOUT` (0x2), and the group's later writes fail too; the track stays
open. A write waiting for demand fails at whichever of `write_timeout` and the object timeout comes
first.
The subgroup timeout starts when a group's FIN is handed to the transport and resets that settling
group (code `DELIVERY_TIMEOUT`, 0x2) if the relay has not acknowledged the FIN by then; nothing is
reported to the track, and the group frees its place under the settling-group limit. moq-relay 0.17.0
neither sends these values upstream nor forwards the properties, so the publisher's own properties are
the only ones in effect against it. Because `0x06` is now a timeout, do not send the older LOC
Timestamp id `0x06`; use `0x10`.

`events.close()` cancels an outstanding write and ends that track; other tracks
keep running. `await publisher.close()` ends all tracks, retracts the namespace, and waits for
transport shutdown. That includes QUIC's closing period after CONNECTION_CLOSE (RFC 9000 §10.2),
about three probe timeouts, roughly 0.3 s against a relay 50 ms away: waiting for it means a process
that exits right after `close()` leaves no UDP socket open and sees no `ResourceWarning` (#8).
TypeScript returns sooner because the browser keeps its closing period to itself. Track names cannot be reused within a publisher connection, including after
their writer closes.

### Advanced media groups

For media, a track needs more than one object per group: LOC/MSF maps one GOP to one MOQT group
whose object 0 is the keyframe. `open_group()` waits for relay demand like `write()`, then returns a
`RelayGroupWriter` that appends objects with increasing IDs and FINs the group on `close()`.
Pass the keyframe to `open_group()` itself to send it as object 0: its delivery timeout then counts
the wait for the stream, which a bare `open_group()` has no object to charge.
Opening the group, each `group.write()`, and `group.close()` get a separate `write_timeout`
deadline. Time between calls does not count toward a deadline. A timeout resets that group and
raises `WRITE_TIMEOUT`; the track stays open for a new group at the next keyframe:

The following is an application sketch, not the data quickstart. The optional
[moq-media format package](packages/moq-media/README.md) supplies `loc_properties` and
`loc_track_properties`; the encoded frames, timestamps, and codec configuration come from your
encoder. Install `caprid-moq-media` alongside `caprid-relay` when publishing media (the development workspace
includes it). It also provides the MSF catalog builder, source URLs, and H.264 configuration helpers
used by the Video.js example.

```python
from moq_media import loc_properties, loc_track_properties

video = publisher.register_track("video", track_properties=loc_track_properties())

if video.has_demand:  # the relay currently has an upstream subscription for the track
    group = await video.open_group(keyframe_bytes, properties=loc_properties(timestamp_us, video_config=avcc))
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
`True` and is served from the relay's cache. moq-relay 0.15 and later cancel the subscription about one
round trip after the last viewer leaves, so the next viewer brings a new `True` and a re-send, and a
viewer that resubscribes on the same connection within about 30 seconds can receive the relay's
cached copy before the re-sent one. `True` does not prove anyone is watching. Write only state that
is safe to serve from cache and to receive more than once. Pausing an encoder while there is no
demand is safe, because a frame written then reaches no viewer; resume at a keyframe, which a
`FrameWriter` asks for through `keyframe_needed`. When the first viewer is on another relay, or is
a moq-lite viewer (such as `@moq/watch`) on any relay, the publisher's relay subscribes once
to look up the track's info, cancels about 50 ms later, and subscribes again: `True`, `False`, `True`. Objects
written during that first subscription reach no viewer, so do not rely on the first objects after
demand begins. See [relay demand](../spec/relay-interop.md#relay-demand) and
[a viewer on another relay](../spec/relay-interop.md#a-viewer-on-another-relay).

Both loops above exist ready-made in `caprid_relay.media`, the media publishing policy the Video.js
example runs on. `CatalogService(track, document)` re-sends a document on every relay subscription
and retries a failed send while demand remains; `DocumentService(track)` does the same for a
document you replace with `update(document)`, such as a media timeline, and also sends each update
while the relay is subscribed; `FrameWriter(track)` maps encoded frames onto groups
(a keyframe opens a group and deltas append, or `grouping="frame"` for one group per frame), skips
frames the relay has no subscription for instead of waiting on the deadline, and resumes at the
next keyframe after any failure. Each `await writer.write(frame)` returns what became of the frame
(`sent`, `skipped`, `stale`, `dropped`, `interrupted` or `ended`) rather than raising. Writes from
several tasks are served in call order, and when more than `max_backlog` frames wait behind the
one being written the writer drops rather than queues: the waiting frames and the group in
progress under keyframe grouping, the oldest waiting frame under `grouping="frame"`.
`writer.keyframe_needed` and `on_keyframe_needed=` say when the next frame must be a keyframe, after
a demand change, a failure, a stale frame or a drop, so an encoder can force one. A frame is anything
with `payload`, `key`, `properties` and `discontinuity` attributes, so your own encoded-sample type
qualifies once it carries its LOC properties. Pacing and time-based frame dropping stay yours; pass
a `stale` predicate to have the writer drop what went stale while a write blocked. The contract is
the "Media publishing" section of [behavior.md](../spec/behavior.md#media-publishing).
`examples/videojs_stream` puts all of this together for a Video.js player.

Run the data-only publishing/subscribing integration tests without a live relay:

```sh
cd python && mise exec -- uv run pytest packages/caprid-relay/tests/test_publish.py
```

### Text and JSON

Track and group writers provide `await writer.write_text(text, properties=...)` and
`await writer.write_json(value, properties=...)`. Each call writes one UTF-8 object using the same
demand, deadline, cancellation, and encoded byte-size limits as `write()`. Group helpers also accept
`status`. Serialization errors leave the writer usable.

Use the exported readers explicitly in subscription callbacks:

```python
from caprid_relay import RelaySubscriptionHandlers, read_json, read_text

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
| `DELIVERY_TIMEOUT` | The object outlived its delivery timeout (property `0x02`) before reaching the transport, and its group was reset. The data is stale: write fresh data, at the next random-access point for a group, rather than retrying it as-is. |
| `CLOSED` | The handle is closed; use an open connection, track, or group. |
| `WRITE_PENDING` | Await the outstanding operation or finish/abort the open group before starting another. |
| `NAMESPACE_NOT_SOLICITED` | Received namespace solicitations do not cover this publisher; check the namespace and token configuration. A later matching solicitation can restore demand. |
| `NAMESPACE_ENDED` | The relay ended the namespace announcement; open a replacement publisher if appropriate. |
| `SUBSCRIPTION_REQUEST_FAILED` | An async subscription request failed; inspect its preserved MOQT request error and the connection state. |
| `SUBSCRIPTION_OVERFLOW` | The event buffer filled; the subscription was cancelled. Choose a suitable capacity or faster consumption before subscribing again. |
| `SUBSCRIPTION_CHANGED` | Upstream demand ended or changed during an operation; choose fresh data or a new random-access point. |
| `GOAWAY` | The relay requested migration; open a replacement connection. |
| `RECONNECTING` | A supervised publisher had no generation up, or the connection ended under the operation; nothing was replayed. Write the next object, or the next keyframe, once it is live. |
| `TRANSPORT_ERROR` | Inspect the original cause and connection state before deciding whether to reconnect or publish again. |
| `TRANSPORT_UNAVAILABLE` | Supply a transport implementation in a runtime without browser WebTransport; in Node, import from `@caprid/relay/node` with `@caprid/relay-quic` installed (TypeScript). |
| `UNAUTHORIZED` | The relay refused the connection's credentials: no token, a bad, malformed or expired one, or an endpoint path the token does not grant. Supply a valid token before connecting again. TypeScript reports it too, in browsers only against moq-relay 0.17.0 or later (older relays' refusals reach a browser as `TRANSPORT_ERROR`; see #14). |

`connecting` covers token resolution and connection setup. `connection` marks a connection-wide
failure; it makes no claim about individual writes that were in flight. `subscription` marks an
async subscription request failure or local event-buffer overflow. Publishing records
`waiting-for-demand`, `opening-stream`, `writing`, or `closing` before entering each step.
A `WRITE_TIMEOUT` in `waiting-for-demand` means this attempt sent no object bytes. For timeouts or transport/subscription failures in a transport
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
acknowledgment. The primitives do not retry or reconnect; the
[supervisors](#stay-connected-supervised-connections) do. `UNAUTHORIZED` is the one code that
says the relay refused the credentials: the relay closed the session with MOQT session termination
code `UNAUTHORIZED` (0x2; `MALFORMED_AUTH_TOKEN` 0x16 and `EXPIRED_AUTH_TOKEN` 0x18 map to it too), in
any phase: in phase `connection` it means the credential of a live session expired or its grant was
revoked ([token expiry](#token-expiry)). TypeScript reports it too; in a browser only against moq-relay
0.17.0 or later, since older relays' close never reached the page and a refusal arrived as
`TRANSPORT_ERROR` (#14). Neither another connection failure nor an uncovered
namespace proves authentication was denied. A subscription outside the token's grant is refused
per request, through `onError` / `on_error` (MOQT `UNAUTHORIZED`, 0x1), and leaves the connection open.

Clean connection shutdown still resolves `closed` without an error; subsequent operations fail
with `CLOSED`. After a failed connection, `closed` and subsequent connection operations expose the
same failure. Caller cancellation reasons and token-provider exceptions retain their identity.
Argument and serialization errors keep their idiomatic language types. Subscription `onError` /
`on_error` callbacks retain the separate MOQT `RequestError` contract and numeric protocol codes.

Python raises one `RelayError`, with the `code` above and a `phase`, exactly as TypeScript does; the
`RelayErrorCode` and `RelayErrorPhase` aliases type them. The only subclasses are the three
subscription errors that carry payload (`SubscriptionRequestError`, `SubscriptionOverflowError`,
`SubscriptionConnectionClosedError`). Branch on `code`, never on the message.

Wrapped transport/session failures preserve the original exception in `__cause__`. The transport's
own exception is not a `RelayError`: a failure surfaced through the Relay connection/publishing API
wraps it in a `RelayError` with `code == "TRANSPORT_ERROR"` and the transport's exception as
`__cause__`. Read that exception by its shape, as in TypeScript, not by its class, which is private:
`source` is `"stream"` (the peer reset or stopped one stream; `stream_error_code` is the peer's code,
or `None` when unknown) or `"session"` (the connection is over; `close_code` and `reason` are set when
the peer closed it). `moq_core.transport.is_stream_error` and `is_session_error` test it. A
credential refusal is the relay's application CONNECTION_CLOSE with code 0x2 and reason
`"unauthorized"`, and is reported as `code == "UNAUTHORIZED"` instead. Explicit caller cancellation, including
`asyncio.CancelledError`, is passed through unchanged.

```python
from caprid_relay import RelayError

try:
    await track.write_text("current state")
except RelayError as error:
    if error.code != "WRITE_TIMEOUT":
        raise
    print("Write deadline expired during", error.phase)
    # Choose what to publish next using phase and application freshness/deduplication policy.
```

## Transport

`connect_relay_subscriber` and `connect_relay_publisher` default to `caprid_relay.quic.connect_quic`,
native QUIC on aioquic: the wire path `@caprid/relay-quic` uses on Node. MOQT runs straight on the QUIC
connection with ALPN `moqt-22` (or `moqt-20`, offered second as a fallback) and no HTTP/3, and SETUP carries the endpoint's path (with the token)
and authority ([ADR 0004](../docs/adr/0004-python-dialing-end-speaks-native-quic.md)). The connection
ends with an application CONNECTION_CLOSE. TLS verification uses the system CA bundle that ships with
`certifi`. Pass `create_transport` to supply a `QuicConfiguration` (custom CAs, QUIC parameters) or
any other implementation of the `moq_core.MoqtTransport` seam:

```python
from aioquic.quic.configuration import QuicConfiguration
from caprid_relay import connect_relay_subscriber
from caprid_relay.quic import connect_quic

configuration = QuicConfiguration(is_client=True, cafile="/path/to/ca.pem")
subscriber = await connect_relay_subscriber(
    url, create_transport=lambda url, protocols: connect_quic(url, protocols, configuration=configuration)
)
```

The package ships no WebTransport session and no HTTP/3: native QUIC is its only dialing end.

### Receive throughput

The QUIC stack is pure Python, so a subscriber's receive rate is bound by CPU on the event loop's one
core, not by the network. On an idle host, a connection receiving 1 MiB objects from
`sjc.relay.mux.global` spends about 25 ms of CPU per MB, a ceiling of roughly **40 MB/s** (measured
2026-09-30, aioquic 1.3). A host whose CPU is contended, or an application that does its own work on
the same loop, gets much less. The adapter takes in every datagram already waiting on the socket
before it transmits (#162). Most of the remaining cost is aioquic's per-packet receive work.

Media bitrates are far below this ceiling; bulk data transfers may not be. A subscriber that falls
behind gets no signal. The relay keeps queueing toward it, and the only symptoms are growing
end-to-end latency and growing subscriber memory. The SDK has no transport-wide backpressure: the
async consumption limits bound local buffering only (see
[async subscription consumption](../spec/behavior.md#async-subscription-consumption)).

A very long-lived connection also grows slowly in memory: aioquic keeps every finished stream's ID in
`QuicConnection._streams_finished` for the life of the connection, and no release prunes it, so a
publisher holds about 28 bytes per group it has ever sent until it reconnects: roughly 2.4 MB a day
for each group per second it sends, or 2.4 GB a day at 1000 groups/s.

`packages/caprid-relay/tests/test_quic.py` runs the full smoke-test assertion list over a real QUIC
loopback: native QUIC listeners on `localhost` (aioquic, self-signed certificates) fronting the
in-memory relay hub (`tests/relay_helpers/quic_hub.py`), so the default transport is exercised end to
end in `python:check`. It also holds the native QUIC dialing end to the transport adapter rules, one
test per rule, against the conformance peer's native QUIC mode. `test_quic_session.py` drives the
session against a loopback server on the same QUIC plumbing (`tests/relay_helpers/quic_server.py`), whose
inbound link it can impair: backpressure, the drain before close, pruning, the keepalive and the socket.
The stream halves and the dropped-FIN retry (`caprid_relay/quic/_fin_retry.py`) also have in-process
tests that need no socket
(`test_quic_streams.py`, which holds the halves to the same expectations as the core's in-memory
`Pipe`, and `test_quic_fin_retry.py`), and `test_quic_aioquic.py` pins the aioquic internals the adapter
reaches into, all named in `caprid_relay/quic/_aioquic.py`, along with the aioquic behaviour its workarounds
exist for. Where a test needs real packets it handshakes a client and
server `QuicConnection` in memory (`tests/relay_helpers/quic_pair.py`). The package pins
`aioquic>=1.3,<1.4` until that list is re-verified against a newer minor. The live smoke test dials
native QUIC.

## Video.js media example

[videojs_stream](examples/videojs_stream/README.md) streams live H.264 (a generated test pattern or
transcoded file, with optional Opus audio) in the MSF/LOC shape consumed by Video.js. It needs the
`examples` dependency group (PyAV, numpy), which `uv sync` installs by default. Follow that example's
setup after completing the data quickstart; media helpers and player integration are separate topics.

## Voice bot example

[voice_bot](examples/voice_bot/README.md) is a parrot voice bot for a browser user: it decodes the
user's microphone with `moq_media.opus.OpusDecoder`, waits for a pause, answers with `OpusEncoder`,
and sends an interrupt when the user talks over it. STT, a language model and TTS plug in through
one async function, and the text of each reply goes on the transcript track. Its browser side is
`ts/examples/voice-bot`.

## Package boundaries

- `packages/caprid-relay`, `caprid_relay`, owns the Relay product API, the authentication convention, and
  the aioquic transport.
- `packages/moq-core`, `moq_core`, is an independently buildable, Apache-2.0 generic protocol
  package. It declares no dependencies and imports no transport, player, UI, or media engine. It
  works over the abstract `moq_core.transport.MoqtTransport` seam that `caprid_relay` supplies.
- `subscriber.track_source` exposes the generic `MoqTrackSource` contract for future integrations:
  track requests in protocol terms (with the core's `on_ok`/`on_done` handler names) and closure
  observation, with no track listing and no operation to close the shared connection.
- import-linter forbids `moq_core` from importing `caprid_relay` or any transport library; the
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
| `subscriber.namespaceEvents({ prefix, capacity, signal })` | `subscriber.namespace_events(prefix, capacity=..., cancellation=...)`; `NamespaceEvent.namespace` is a tuple, and a prefix overlapping one the connection already discovers raises `RelayError` with code `PREFIX_OVERLAP` |
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
| `roomEndpoint([endpoint,] room)` throws `RangeError` | `room_endpoint([endpoint,] room)` raises `ValueError` |
| Browser `WebTransport`, or `@caprid/relay-quic` on Node | aioquic native QUIC client (`caprid_relay.quic.connect_quic`) |
| `subscriber.want(key, { namespace, track, generations, staleAfterMs, retry, until, drainMs }, handlers)` | `subscriber.want(key, namespace, track, WantHandlers(...), generations=..., stale_after=seconds, retry=..., until=..., drain=seconds)` |
| `onSubscribed(info, { namespace })`, `onRestarted({ reason, namespace })`, `onSwitched({ from, to, subscribed })` | `on_subscribed(info, namespace)`, `on_restarted(reason, namespace)`, `on_switched(WantSwitchedInfo)` with `from_namespace`, `from_track`, `to_namespace`, `to_track` |
| `SupervisionBackoff` in milliseconds (`initialDelayMs`, `maxDelayMs`) | `SupervisionBackoff` in seconds (`initial_delay`, `max_delay`) |

## Initial scope

This slice supports one connection, arbitrary reliable-stream track subscriptions, subscription
updates and cancellation, plus data-only publication on a dedicated connection. The generic core
also exposes FETCH and TRACK_STATUS. The SDK offers `moqt-22`, then `moqt-20` as a fallback, and
speaks whichever the relay selects.

Token refresh on a live session, datagram reception, and discovery of
tracks within a namespace are not implemented yet. Namespace discovery is `namespace_events`
([discovery contract](../spec/behavior.md#discovery)). A GOAWAY ends the connection with a `RelayError` of code `GOAWAY`;
subscriptions still awaiting their first response end with it (`subscribed` raises
`SubscriptionConnectionClosedError`, `ended` resolves `terminated`), never through `on_error`;
callers can open a replacement.

## Development

Contributor setup, checks, formatting, and the opt-in live Relay smoke test for this workspace are
in the root [README](../README.md#development).
