# Relay Python SDK

The Python SDK for Relay. It connects to a Relay endpoint from any Python 3.11+ asyncio program,
publishes data, and subscribes to track objects. No browser is involved. The SDK speaks MOQT
directly on QUIC (native QUIC) through aioquic.

The SDK is three packages on PyPI, released together at one version:

| Package | Import name | Contents |
| --- | --- | --- |
| `caprid-relay` | `caprid_relay` | Relay connections, publishing, subscribing, and supervised connections. `caprid_relay.media` adds media publishing and reading. |
| `caprid-moq-core` | `moq_core` | The generic MOQT protocol core. `caprid-relay` depends on it. |
| `caprid-moq-media` | `moq_media` | Optional LOC and MSF media format helpers. The `opus` extra adds Opus encoding and decoding. |

The shared protocol pins, behavior contract, and test vectors live in [spec](../spec/README.md). The
[root README](../README.md) describes the whole repository. Before 1.0, a minor version can break the
API, and [CHANGELOG.md](CHANGELOG.md) says what changed. This guide follows the `main` branch, which
can be ahead of the latest release.

## First round trip

In this section you publish one object and receive it back through Relay, from a new project that
installs the SDK from PyPI. You need Python 3.11 or later and [uv](https://docs.astral.sh/uv/). The
SDK dials native QUIC through aioquic, not a browser, HTTP/3, or a WebSocket, so the endpoint and
your network must allow QUIC traffic over UDP.

### Endpoint and credentials

Ask the team that operates your Relay deployment, or your application's backend owner, for these:

- an HTTPS Relay endpoint
- your token's root, which is the namespace prefix the token grants
- a short-lived Relay JWT that can **publish and subscribe** under that root

The SDK uses tokens but does not issue them, and there is no public token signup. A Mux API access
token is not a Relay JWT.

Use an endpoint whose path is your token's root, for example `https://relay.mux.global/acme/rooms/42`
for a token rooted at `acme/rooms/42`. Name namespaces relative to it: `["p", "a"]`, not
`["acme", "rooms", "42", "p", "a"]`. The connection then cannot reach outside that root, and a path
the token does not grant fails at connect with `UNAUTHORIZED`. A client that spans several rooms
under one grant can use the bare host instead, where namespace fields include the token's root
([namespaces](../spec/behavior.md#subscribing)). The Mux relays carry announcements between regions,
so a publisher and a subscriber can each use their nearest relay
([a viewer on another relay](../spec/relay-interop.md#a-viewer-on-another-relay)). The SDK's default
endpoint is the bare host `https://relay.mux.global`, which still needs authorized credentials. For another
deployment, use the endpoint its operator gives you.

`room_endpoint` builds that endpoint from the token's root, so you do not assemble the URL by hand.
Given only the root, it uses `DEFAULT_ENDPOINT`, `https://relay.mux.global`. Pass an endpoint
first for another relay:

```python
from caprid_relay import room_endpoint

url = room_endpoint(["acme", "rooms", "42"])
# 'https://relay.mux.global/acme/rooms/42'
url = room_endpoint("https://ord.relay.mux.global", ["acme", "rooms", "42"])
# 'https://ord.relay.mux.global/acme/rooms/42'
```

It appends each field to the endpoint's path and keeps the endpoint's query. It encodes nothing,
because the relay reads the path without decoding it. A field may hold only letters, digits, and
`-._~!$&'()+,;=:@`, and may not be `.` or `..`. Any other field raises `ValueError`. To reach a room
with such a field, use the bare host
([room-rooted endpoints](../spec/behavior.md#room-rooted-endpoints)).

The round trip below reads the token from `RELAY_TOKEN`. Set it with the hidden prompt shown there
for manual runs. Automation can inject it through its secret manager. Do not put tokens in commands,
source files, URLs, or committed `.env` files. The SDK sends the token as the `jwt` query parameter of the
endpoint, so scrub query values and token strings from URLs, traces, and exception reports before
you collect them. Browser consumers have an additional
[console exposure risk](../ts/GUIDE.md#browser-token-exposure).

### Install and run

Create a project and add the SDK:

```sh
mkdir relay-hello && cd relay-hello
uv init --bare
uv add caprid-relay
```

`pip install caprid-relay` in a virtual environment works too. `caprid-relay` brings
`caprid-moq-core` with it. Add `caprid-moq-media` as well to publish or read media.

`caprid_relay` re-exports the core types its own signatures use: `MoqtObject`, `MessageParameters`,
`SubscribeOk`, `RequestError`, `Closed`, and the rest of the "Protocol types borrowed from the core"
row in [the vocabulary](../spec/vocabulary.md). Import other protocol types from the core package,
such as the location filters for `subscribe(parameters=...)`:
`from moq_core import AbsoluteLocationFilter`.

Save this as `hello.py`, with your
token's root in place of `acme/rooms/42`:

```python
import asyncio
import os
import uuid

from caprid_relay import (
    WantHandlers,
    connect_relay_publisher,
    connect_supervised_subscriber,
    read_text,
    room_endpoint,
)

URL = room_endpoint(["acme", "rooms", "42"])  # your token's root, on https://relay.mux.global
# URL = os.environ["RELAY_URL"]  # or any endpoint whose path is your token's root
NAMESPACE = ["hello", uuid.uuid4().hex[:8]]  # fresh for each run, relative to the root


def get_token() -> str:
    return os.environ["RELAY_TOKEN"]


async def main() -> None:
    received: asyncio.Future[str] = asyncio.get_running_loop().create_future()

    # Subscribe first: the publisher's write() waits until someone wants the track.
    async with await connect_supervised_subscriber(URL, get_token=get_token) as subscriber:
        subscriber.want(
            "greeting",
            NAMESPACE,
            "greetings",
            WantHandlers(on_object=lambda obj: received.set_result(read_text(obj)), on_error=received.set_exception),
        )
        async with await connect_relay_publisher(URL, namespace=NAMESPACE, get_token=get_token) as publisher:
            track = publisher.register_track("greetings")
            await track.write_text("hello, relay")
            print("received:", await received)


asyncio.run(main())
```

Enter the token at the hidden prompt, then run the script:

```sh
read -rs -p 'Relay token: ' RELAY_TOKEN && echo && export RELAY_TOKEN
uv run hello.py
```

It prints `received: hello, relay`. That line is what proves the round trip. A completed
`write_text()` only means the transport accepted the object.

`room_endpoint` puts the token's root in the endpoint's path, so the namespace is relative to it.
The publisher announces that namespace, and its `write_text()` waits for relay demand before it
sends anything. The subscriber's `want` creates that demand. A fresh namespace can briefly reject a
subscription with request error 16 `unroutable` while its announcement propagates, for about a round
trip, and the want retries it until the subscription succeeds. A rejection that no wait fixes, such
as an unauthorized track, reaches `on_error`. Each `async with` closes its connection when the block
exits, even on an error.

This script has no run deadline, and a write that times out waiting for demand fails the run.
[The full demo](#run-the-full-demo-from-a-checkout) adds a deadline and a write retry, and runs the
publisher and subscriber in separate processes.

## Run the full demo from a checkout

The repository's mux_global demo is the same round trip, built to show what a longer-lived program
handles. It has a run deadline, retries only writes that provably sent nothing, prompts for the
token and redacts it from its output, and can run either side alone. You need Git and
[mise](https://mise.jdx.dev/getting-started.html) on PATH. mise installs the repository's pinned
Python and uv. Run the commands in Bash from the repository root.

```sh
git clone https://github.com/caprids/relay-sdks.git
cd relay-sdks
mise install
mise run python:install
```

The combined demo starts both sides in one process. Its subscriber is the same
[supervised subscriber](#stay-connected-supervised-connections) `want`, which retries request error
16 `unroutable` (`404 Broadcast not found` on relays before 0.15.8) with capped backoff. The
publisher retries a write only when the write deadline expired before any bytes were sent. Once
a write starts sending, a failure stops the run. The demo never replays bytes the relay may already
hold.

At the prompts, enter the endpoint, ending in your token's root, and a **fresh namespace below it**.
The demo splits `quickstart/unique-run` into `["quickstart", "unique-run"]`. The SDK itself sends
fields verbatim. Both connections use those fields and the track `ticker`. If your token authorizes
only one exact namespace, enter the bare host as the endpoint and that whole namespace (a namespace
needs at least one field), and avoid overlapping publishers and stale cached objects.

```sh
read -r -p "Relay HTTPS endpoint with your token's root as its path: " RELAY_URL
read -r -p 'Namespace below that root (slash-separated): ' RELAY_NAMESPACE
mise run python:example:mux-global -- --url "$RELAY_URL" --namespace "$RELAY_NAMESPACE" --track ticker --count 1 --timeout 30
# Enter the Relay token at the hidden prompt.
```

The output includes these lines. Byte counts, timings, and ordering vary:

```text
starting             mode=both namespace=... track=ticker count=1
publisher connected  ...
subscriber connected ...
announced            ...
subscribed           ...
published            seq=1 bytes=...
received             group=0 bytes=... seq=1 ageMs=...
finished             published=1 received=1
```

`finished published=1 received=1` means success. A completed write only means the transport accepted
the object. The `received` line is what proves the round trip. The 30-second deadline covers
connection setup, waiting for demand, retries, and reception. An error prints a redacted `failed`
line and exits nonzero. Either way, the demo cancels and awaits its background tasks and closes both
connections.

The complete example is [demo.py](examples/mux_global/demo.py), with imports, token configuration,
and byte decoding. It bounds the run with `asyncio.timeout` and enters the SDK's async context
managers through `AsyncExitStack`, so it can run either side or both. Each connection closes when the
context exits, even if the second connection fails. Ordinary cleanup needs no `CancellationToken`. An
application with a fixed mode can write the same lifecycle as
`async with await connect_relay_publisher(...) as publisher:`.

### Subscribe and publish separately

Use two terminals in the same checkout. In **each** terminal, set `RELAY_URL` and `RELAY_NAMESPACE`
with the prompts above, using exactly the same values. Each process prompts for its own token. The
subscriber needs subscribe permission and the publisher needs publish permission. Start them within
60 seconds of each other, in either order. A subscriber started first retries the fresh-namespace
rejection. A publisher started alone waits for demand and fails at the overall deadline.

Subscribe to one object:

```sh
mise run python:example:mux-global -- --mode subscribe --url "$RELAY_URL" --namespace "$RELAY_NAMESPACE" --track ticker --count 1 --timeout 60
```

Publish one object from the other terminal:

```sh
mise run python:example:mux-global -- --mode publish --url "$RELAY_URL" --namespace "$RELAY_NAMESPACE" --track ticker --count 1 --timeout 60
```

The subscriber succeeds with `finished published=0 received=1` and the publisher with
`finished published=1 received=0`. Both modes handle errors and clean up as the combined run does.
See [demo options](examples/mux_global/README.md) for more objects and intervals.

### Use the local packages in your application

Inside a checkout, the uv workspace already holds all three packages as editable sources. Put a
script in `python/` and run it from the repository root with
`mise exec -- uv run --project python python python/your_script.py`.

## If the run fails

- A connection or handshake failure: check the endpoint, the Relay JWT's expiry and permissions, TLS
  trust, and whether QUIC over UDP can reach the relay. The demo redacts tokens. Keep them out of
  diagnostic reports.
- Repeated request error 16 `unroutable` (or `404 Broadcast not found` on older relays): check that
  the publisher is still running and that both sides use exactly the same authorized namespace.
  Error 16 `not found` means the namespace is announced but the track is not, so check the track
  name. The subscriber's retries stop at the run deadline.
- `waiting ... no relay demand yet` until the deadline: start the matching subscriber, and check the
  token's namespace prefix and its publish and subscribe permissions.
- A write failure after demand: delivery may be partial. The demo stops without resending. Decide
  whether to send fresh data or to replay under an explicit application policy before you try again.

## Subscription lifecycle

A subscriber connection can only subscribe. Publishing uses a separate publisher connection. The
names both SDKs use for each concept, and why, are in [SDK vocabulary](../spec/vocabulary.md).

`subscribe` returns a `RelaySubscription` at once. Await `subscription.subscribed` for the relay's
answer. It returns the acceptance info, which `on_subscribed` also receives. It raises
`SubscriptionRequestError` on rejection or request timeout, `SubscriptionConnectionClosedError` if
the connection ends first, or your own reason if you cancelled. Await `subscription.ended` to learn
that the subscription can no longer deliver, and why. It settles once and never raises. Its value
has the `kind` (`rejected`, `terminated`, or `cancelled`), the protocol `cause`, whether the
subscription had been `accepted`, and the `error`.

Both are `RelayAwaitable` objects, not bare futures, as are the publisher's `announced` and every
connection's `closed`. Any number of tasks can await them. A cancelled waiter or a timed-out
`asyncio.wait_for` leaves the result intact. `done()`, `result()`, and `add_done_callback()` read it
without allocating.

```python
subscription = subscriber.subscribe(namespace, track, RelaySubscriptionHandlers(on_object=handle))
await subscription.subscribed  # raises on rejection, closure, or cancel
subscription.ended.add_done_callback(lambda done: print("subscription", done.result().kind))
```

Cancelling a subscription stops that consumer, including any trailing objects after
`on_publisher_done`. Closing the connection stops all of its subscriptions. `on_publisher_done`
settles neither awaitable. It is the publisher's control notification: trailing objects can still
arrive, and the request stays open until you cancel it or the connection ends. The full state table
is in the [behavior contract](../spec/behavior.md#subscription-lifecycle).

The SDK sends namespace fields verbatim. You can subscribe to a known track directly. The SDK does
not interpret the payload or assume that a track holds audio, video, or timestamps. An accepted data
subscription has no inactivity deadline. The request timeout bounds only the relay's first answer.
The SDK schedules no application-level keepalive, but the aioquic transport sends a QUIC PING after
5 seconds in which it has sent nothing, on publisher and subscriber connections alike. The PING keeps
an idle connection open and lets a receive-only subscriber survive a NAT rebinding: the relay keeps
sending to the old address until a packet arrives from the new one. The Python transport uses
aioquic's default local idle timeout of 60 seconds, subject to peer negotiation. Each subscription
cancels independently. Your application owns connection shutdown.

A subscription without a location filter (no `parameters`, or `NoLocationFilter()`) starts at the
live edge on Relay. A subscriber that joins an ongoing track receives only objects published after
it joined, although the relay still holds earlier ones. To reach back, pass
`MessageParameters(location_filter=RelativeGroupFilter(groups_before_next=N))`, where `1` starts at
the current group and `N` starts N-1 groups before it. You can also pass an `AbsoluteLocationFilter`
with an explicit start. The relay serves either from its cache, as far back as it still holds. Both
filters come from `moq_core`. See
[late subscribers](../spec/relay-interop.md#late-subscribers-and-location-filters).

Object callbacks are plain synchronous callables that run as streams deliver objects. The SDK adds no
application queue, no ordering across streams, no JSON decoding, no clock synchronization, and no
media scheduling. To await work without blocking delivery, use
[`subscribe_events`](#consume-asynchronously). Handle subgroup resets and connection closure in your
code. The API does not promise exactly-once or gap-free delivery. The SDK reports every reset it
receives, but the relay may drop a reset for a subgroup it had not begun forwarding. An aborted group
can then leave no trace except a gap in group IDs
([subgroup resets](../spec/relay-interop.md#subgroup-resets)). `on_publisher_done` may arrive before
trailing objects in transit, and it does not end the subscription. Cancel the subscription when you
are finished with it.

The SDK sends every `MessageParameters` field you set, but moq-relay acts on only some of them. It
honors `subscriber_priority` and `location_filter` on the subscribe. It ignores `group_order` (the
relay always serves Descending), `new_group_request`, both delivery timeouts, and a per-request
`authorization_tokens`. Because the relay ignores `new_group_request`, there is no protocol keyframe
request through the relay. An application that needs one sends it to the publisher on a track of its
own. See [subscription parameters](../spec/relay-interop.md#subscription-parameters).

For media, `caprid_relay.media.media_subscribe_parameters(kind)` gives a subscription the subscriber
priority that puts audio ahead of video when the viewer's downlink is congested. It uses 16 for audio
and 96 for video, the same values `media_track_properties` gives the publisher. moq-relay orders what
it sends a viewer by subscriber priority alone and ignores the publisher's, so without it audio waits
behind video's backlog. Pass other parameters as `overrides`. A `subscriber_priority` there replaces
the default. See [media subscribing](../spec/behavior.md#media-subscribing), which also covers
`caprid_relay.media.FrameReader`, the reader that turns a media track's objects into frames in decode
order.

```python
from caprid_relay.media import media_subscribe_parameters

audio = subscriber.subscribe(namespace, "audio", handlers, parameters=media_subscribe_parameters("audio"))
video = subscriber.subscribe(namespace, "video", handlers, parameters=media_subscribe_parameters("video"))
```

`subscription.update(parameters)` sends REQUEST_UPDATE, the protocol's way to change what an accepted
subscription delivers. **moq-relay does not implement it.** The relay never acknowledges an update.
Any update, including `MessageParameters(forward=0)` or an empty one, ends the subscription within one
round trip with `on_publisher_done` (status 2 `track ended`) and stops delivery. `on_update_ok` never
fires. To pause, cancel the subscription and subscribe again later. To change the track, the
priority, or the filter, use a [switchable subscription](#switching-tracks), which opens the new
subscription before it cancels the old one
([relay interop deviation 4](../spec/behavior.md#relay-interop-deviations)). Subscribing with
`parameters=MessageParameters(forward=0)` does not work either. moq-relay refuses it with request
error 3 `FORWARD=0 not supported` through `on_error`, and the connection stays open.

The protocol allows updates only after acceptance. An update you issue before `on_subscribed` waits
in order and goes out as soon as the relay accepts. At most eight can wait, and one more raises a
`RelayError` with code `UPDATE_QUEUE_FULL`. Updating an ended subscription raises `RelayError` with
code `CLOSED`. A relay that refuses an update with REQUEST_ERROR reaches `on_error`, which ends the
subscription.

Re-subscribing after `on_publisher_done` creates a new request, and it cannot bring back a finished
track. Once the publisher has gone, the relay answers with request error 16 `unroutable`. While the
publisher's namespace stays announced, the relay replays the finished track's cached last group
followed by another done, one round trip later each time. Do not re-subscribe to a track the
publisher ended until your application knows new content exists. A status-2 done alone does not show
that, because moq-relay also sends one after this subscription's own `update()`, and then
re-subscribing is how you apply the change. Apply retry backoff to done callbacks as well as to
retryable request errors, and define any continuity policy in your application. See
[publisher restarts and resubscription](../spec/relay-interop.md#publisher-restarts-and-resubscription)
for retry, trailing-object, and publisher-generation guidance.

### Connection options and argument errors

`connect_relay_subscriber` has one deadline, `connect_timeout` in seconds, that covers token
resolution and both handshakes. On Linux, a relay address that refuses the connection (nothing
listens on its UDP port) fails at once with `TRANSPORT_ERROR` in phase `connecting`, with the
operating system's `ConnectionRefusedError` in the `__cause__` chain, as in TypeScript. On other
systems such as macOS, and for an address that never answers, the attempt waits out the deadline and
fails with `CONNECTION_TIMEOUT`.

The optional `cancellation` token controls the connection's whole lifetime. Cancelling it closes an
established connection, and `closed` carries the reason. An explicit `jwt` query parameter on the
endpoint takes precedence over `get_token`, matching the Relay authentication convention. An empty
one (`?jwt=`) is a `ValueError` before anything connects: put a token in it, or drop it and use
`get_token`. An endpoint with another scheme than `https`, a fragment, or userinfo is also a
`ValueError`. An endpoint that is not a string holding an absolute URL with a host is a `TypeError`.

Every deadline (`connect_timeout`, `request_timeout`, and `write_timeout`) must be greater than 0 and
at most 2147483.647 seconds, the same bound the TypeScript SDK's timers impose. A value outside that
range is a `ValueError`. A value that is not a number, including a `bool`, is a `TypeError`.

In-flight operations re-raise your application's exact cancellation reason. With a bare `cancel()`,
that reason is `Cancelled`, which is not a `RelayError`. Handle lifetime cancellation separately from
SDK errors: when you stop normally, compare the exception to your `CancellationToken.reason`.
`RelaySubscriber` is also an async context manager that closes on exit.

Namespaces are lists or tuples of 1 to 32 non-empty string fields, sent verbatim without splitting on
`/`. A single `str` is a `TypeError`, not a one-field namespace. [protocol.json](../spec/protocol.json)
pins the 32-field protocol limit, and more fields is a `ValueError`. Track names must be non-empty
strings. Both rules apply to publishing and subscribing alike. The SDK checks them before it calls
the token provider or the transport factory, or reads any connection state. See the Arguments table
in [behavior.md](../spec/behavior.md#arguments).

See [Errors and recovery](#errors-and-recovery) for stable SDK error codes, phases, and preserved
transport causes.

### Token expiry

The relay ends a session when its token expires, and a live session cannot take a new token. Every
connection exposes `expires_at`, the time that will happen. It comes from the JWT's `exp` claim, or
from a `RelayCredential` your token provider returns when the token is not a JWT or your server knows
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

At that time the connection closes with `UNAUTHORIZED` in phase `connection`. The plain connections
do not prevent it, so open a replacement before then. The
[supervised connections](#stay-connected-supervised-connections) do this for you. They open the
replacement before closing the old connection, a minute before expiry by default (`rotate_before`).

### Switching tracks

REQUEST_UPDATE names no track, so moving a viewer between layers (`video-hi`, `video-mid`,
`video-lo`) takes a new subscription. `subscribe_switchable` keeps one set of handlers on whichever
subscription is active and moves them make-before-break. `switch_to` subscribes to the new track
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
  object of the new track. A switchable has no `update()`. To change a filter or priority, switch to
  the same track with new `parameters`. Parameters are not inherited.
- `until` defaults to `"first-object"`. That works where delivery is in order, but it can skip media.
  The relay starts a subscription at its cached latest group, which can be stale, and a subscriber on
  another relay can lose the first object. Compare your own timeline, not group IDs, because each
  track numbers its own groups.
- `switch_to` starts at once and returns a `RelayAwaitable`. A timed-out `wait_for` on it leaves the
  switch pending. Pass `cancellation=` to abandon it. The awaitable raises, and the old track keeps
  playing, if the new subscription fails, a newer `switch_to` replaces it, or the token is cancelled.
  `ended` settles when the active subscription ends. `cancel()` cancels everything. See
  [switching tracks](../spec/behavior.md#switching-tracks) for the full contract and measured cutover
  times.

## Consume asynchronously

`subscribe_events` creates a subscription owned by a single async iterator. Always wrap the loop in
the iterator's async context manager, so that a `break`, a consumer exception, or task cancellation
releases the subscription:

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

Python's `async for` alone does **not** close an iterator on `break`. Instead of `async with`, you can
use `try`/`finally: events.close()` or `contextlib.aclosing(events)`. Cancelling a task that awaits
`anext()` also closes the subscription. That includes the expiry of `asyncio.timeout()` or
`asyncio.wait_for(anext(events), ...)`. The caller receives `TimeoutError`, the subscription is
cancelled, and later reads return end of iteration. Use these deadlines to end consumption, not to
skip an idle interval and resume. To cancel this iterator without affecting the connection, pass a
separate `CancellationToken` as `cancellation=`. The loop raises the cancellation reason and consumer
exceptions to its caller.

The default capacity is **64 events**, lifecycle events included. It counts events, not bytes, and an
object may be up to 16 MiB plus properties. An overflow discards the queued events, cancels the
subscription, and raises `SubscriptionOverflowError`. A request failure raises
`SubscriptionRequestError`. Connection closure raises `SubscriptionConnectionClosedError`, before or
after acceptance, and even for a clean close, because closure does not prove that every object
arrived. These errors propagate out of the loop to the caller. `publisher_done` is an event, never
the end of iteration. Events keep their arrival order, with no ordering across streams and no
transport-wide backpressure. Leaving early releases the owned subscription and its observers without
closing `subscriber` or other consumers. Only one pending `anext()` is supported, and a concurrent one
raises `RuntimeError`. See the
[complete event and ownership contract](../spec/behavior.md#async-subscription-consumption).

An `async def` subscription callback raises `TypeError` before anything is sent. A synchronous
callback that returns an awaitable emits `RuntimeWarning`. The SDK closes a returned coroutine, and
cancels a returned future or task and observes its exception. The SDK never awaits callbacks, so use
`subscribe_events` for async consumers. The SDK copies the handlers when it creates the subscription,
so later assignments to the handler object do not change that subscription.

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

The supervised publisher reconnects with backoff, from 0.5 s up to 10 s, whenever its connection
ends. Each connection is a new **generation** with its own namespace: the root plus a field that sorts
by start time. Each generation gets a fresh namespace because moq-relay 0.17.0 never hands existing
subscriptions to a reconnected publisher, and can keep serving a dead one's cached objects
([publisher recovery](../spec/publisher-recovery.md)). The publisher registers every track again on
each generation. It never replays writes. While no generation is up, a write raises `RECONNECTING` at
once, and so does an operation that the connection's end interrupted. Write the next object, or the
next keyframe, when the publisher is live again. A `caprid_relay.media.FrameWriter` on a supervised
track treats `RECONNECTING` as an interruption and resumes at the next keyframe.

`want(key, namespace, track, handlers, ...)` keeps one track subscribed until `unwant(key)`. It
retries the announce race, and resubscribes after `PUBLISH_DONE` or after a rejection that waiting may
fix, with backoff from 0.25 s up to 8 s. It also moves to the replacement connection. With
`stale_after` in seconds, it resubscribes a track that went silent. Set that only for tracks that are
never silent while live. With `generations=True`, it discovers the publisher's root and stays on the
newest generation, moving as soon as a newer one is announced, with no application channel. A
rejection that waiting does not fix, such as bad credentials or a malformed request, reaches
`on_error`, and the want stops. To change the backoff bounds, pass
`SupervisionBackoff(initial_delay=..., max_delay=...)` as `reconnect=` to either supervisor or as
`retry=` to a want. See the [supervised contract](../spec/behavior.md#supervised-connections).

A move to a newer generation is make-before-break, as with `switch_to`. The old subscription keeps
delivering until the new generation delivers, and `on_switched` runs at the cutover with a
`WantSwitchedInfo` instead of `on_restarted`. Wanting the same key again with another track or other
parameters moves the same way, so a supervised viewer changes layers without a gap:

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
of the want. Compare your own timeline, never group IDs, because each generation and each track
numbers its own groups. Wanting the key again with only `until`, `drain`, `stale_after`, `retry`, or
the handlers changed swaps them without resubscribing. Another namespace, or turning `generations` on
or off, replaces the want.

Both supervisors **rotate** before the token expires ([token expiry](#token-expiry)), so a long
session never reaches the relay's deadline. `rotate_before` seconds (default 60) before `expires_at`,
they call `get_token` again and open the replacement while the old connection still serves. The
publisher's next generation is announced, each track moves to it once viewers subscribe there, and
the old generation closes. Each want moves its subscription to the new connection, as in any other
move. When the want's target is unchanged, the cutover is invisible: no callback runs, and nothing is
lost or repeated. Call `rotate()` to rotate now, for example after you revoke a token. See
[rotation](../spec/behavior.md#rotation).

## Publishing lifecycle

A publisher connection serves one namespace with any number of tracks. It uses a separate transport
from subscriber connections. Publishing needs no camera, microphone, catalog, or timestamps.

Publishing follows Relay's announce-and-serve flow. `register_track()` registers the track and sends
nothing. `write()` waits for the relay to subscribe upstream, which is called demand, before it sends
bytes. Start a subscriber to create that demand. The write deadline, `write_timeout`, defaults to 10
seconds and covers both the wait for demand and the transport write.

A `RelayError` with code `WRITE_TIMEOUT` and `phase == "waiting-for-demand"` means this attempt sent
no object bytes. Choose the next write by your application's freshness and deduplication policy: the
code alone does not prove that a replay is safe. If the relay has solicited namespace prefixes but
none covers this publisher's namespace, the demand deadline raises `NAMESPACE_NOT_SOLICITED` instead.
Check the token and namespace rather than retrying blindly. The track stays open, and a later
matching solicitation can let it continue. An ordinary absence of subscribers still raises
`WRITE_TIMEOUT`.

A failed or cancelled write resets its stream and leaves `track.closed` false while the publisher
connection stays open. A change in the relay's subscription raises `SUBSCRIPTION_CHANGED`, and a
transport deadline raises `WRITE_TIMEOUT`. The next write uses a new group ID. Choose fresh data or
the next random-access point, because bytes already delivered cannot be withdrawn. If demand went
away, the next write waits for a new relay subscription, and `demand_changes()` keeps reporting
changes.

The publisher announces its namespace when you register the first track. `publisher.announced`
settles when that announce has reached the wire. The relay acknowledges nothing more, and it routes
subscribes to the namespace about a round trip later. It rejects a subscribe that arrives before then
with request error 16 (`DOES_NOT_EXIST`), reason `unroutable`, on moq-relay 0.15.8 and later. Relays
through 0.15.1 answered `404 Broadcast not found`. A subscriber on another relay of the cluster waits
longer, until its relay learns the route.

The SDK does not retry a subscribe. A subscriber can wait for `subscriber.namespace_events(prefix)`
to report the namespace `announced`, which avoids the race. It can instead retry that rejection within
its own budget, as a supervised subscriber's [want](#stay-connected-supervised-connections) does. A
process that owns both sides awaits `announced` before subscribing, which narrows the race but does
not close it. On moq-relay 0.17.0, a subscribe sent on the discovering connection once `announced`
arrived was never rejected `unroutable` (0 of 160 trials on 2026-10-06). A subscribe sent as soon as
the publisher's `announced` settled was rejected 6 to 14 times in 40 on the publisher's relay, and
every time on another relay ([measurements](../spec/relay-interop.md#announce-propagation)). With
discovery, a later `unroutable` means the namespace has gone since. Wait for its next `announced`
instead of retrying on a timer ([discovery contract](../spec/behavior.md#discovery)). `announced`
stays pending while no solicitation covers the namespace, and the connection's closure ends it if
that comes first.

Each write copies its bytes and sends one independent group that holds one object. Await each write.
A concurrent call raises `WRITE_PENDING` instead of growing a queue. Completion means the transport
accepted the complete group, not that a viewer acknowledged it. The publisher keeps nothing to replay
after it sends an object. What a late subscriber gets also depends on relay caching and subscription
filters. A failed write may have delivered partial data, and the SDK does not retry it.

Optional `track_properties` on `register_track()` and `properties` on `write()` carry explicit
protocol metadata as `KeyValuePair` values. Property types, and values of even-numbered types, must
be non-negative integers up to `2**64 - 1`, the largest value the core's varint carries. A `bool` is
not an integer here. Values of odd-numbered types must expose a bytes-like buffer, and the SDK copies
them. An invalid property raises `TypeError` that names the property index, before a write starts or
a track is registered. At the same point, these raise `ValueError`:

- a byte value longer than 65535 bytes
- `track_properties` that encode to more than 65525 bytes (the room `SUBSCRIBE_OK` has), hold more
  than 64 pairs (the relay's limit), or carry a `0x0E` above 255 or a `0x22` other than 1 or 2 (values
  draft-22 rules out). Any of these would leave the track unsubscribable.
- object `properties` on `write()` that encode to more than 64 KiB, which the relay drops silently

The SDK measures the encoding with the core's own encoder, so an oversized property never opens a
stream. The SDK adds no default timescale, timestamp, or LOC media packaging. An object's payload is
limited to 16 MiB.

The SDK encodes properties as supplied, but subscribers see the relay's output. moq-relay 0.17.5 and
earlier strip object properties (fixed upstream in moq #5494, not yet released). Only the object
Timestamp (`0x10`, or the older `0x06`) survives, rescaled to microseconds by the declared Timescale
(`0x08`). Every other object property, including Video Config, is dropped. Tracks without a declared
Timescale, and objects without a Timestamp, get relay-clock timestamps. Declaring
`track_properties=[KeyValuePair(0x08, 1_000_000)]` keeps supplied Timestamp values unchanged.

Every `SUBSCRIBE_OK` carries the relay's track properties, not the publisher's: `0x08` = 1,000,000,
`0x0E` = 128 unless the publisher set it, and `0x22` = 2. The relay would refuse every subscriber to a
track whose `0x0E` or `0x22` is out of range, or that has more than 64 track properties, which is why
`register_track()` refuses those. A subscription with `MessageParameters(include_properties=0)` gets
no track properties, and objects with no properties at all, not even a Timestamp. Keep application
metadata that must arrive intact in payloads, and read the received track timescale when you
interpret timestamps. See [Relay property handling](../spec/behavior.md#relay-property-handling) for
the observations and media guidance.

Two properties also set delivery timeouts in milliseconds (draft-ietf-moq-transport-22 §5.2):
`0x02` (`OBJECT_DELIVERY_TIMEOUT`) and `0x06` (`SUBGROUP_DELIVERY_TIMEOUT`). Set either as a track
property, or on a group's first object to override the track's value for that group. On any later
object, the property is only sent. If the relay's subscription carries its own values, the smaller
non-zero value wins. 0 means no timeout, and values above 2147483647 are clamped.

The object timeout bounds an object's age, from the `write()` or `group.write()` call until the
transport takes the object. That happens when its stream write returns, after aioquic has transmitted
the bytes. Waiting for demand and for room under the settling-group limit counts too. Until the
group's value is fixed, just before its stream opens, the publisher's value alone times the object.
An object already older than the fixed value fails then, before any stream opens. An object that
outlives the timeout raises `DELIVERY_TIMEOUT` in its current phase and resets its group with code
`DELIVERY_TIMEOUT` (0x2). The group's later writes fail too, and the track stays open. A write that
waits for demand fails at whichever of `write_timeout` and the object timeout comes first.

The subgroup timeout starts when the publisher hands a group's FIN to the transport. If the relay has
not acknowledged the FIN by then, the publisher resets that settling group (code `DELIVERY_TIMEOUT`,
0x2). The track hears nothing of it, and the group frees its place under the settling-group limit.
moq-relay 0.17.0 neither sends these values upstream nor forwards the properties, so against it only
the publisher's own properties take effect. Because `0x06` is now a timeout, do not send the older
LOC Timestamp ID `0x06`. Use `0x10`.

`track.close()` cancels an outstanding write and ends that track. Other tracks keep running.
`await publisher.close()` ends all tracks, retracts the namespace, and waits for transport shutdown.
That includes QUIC's closing period after CONNECTION_CLOSE (RFC 9000 §10.2), about three probe
timeouts, roughly 0.3 s against a relay 50 ms away. Because `close()` waits for it, a process that
exits right after `close()` leaves no UDP socket open and sees no `ResourceWarning`. TypeScript
returns sooner because the browser keeps its closing period to itself. A track name cannot be reused
on the same publisher connection, even after its writer closes.

### Advanced media groups

A media track needs more than one object per group. LOC and MSF map one GOP to one MOQT group whose
object 0 is the keyframe. `open_group()` waits for relay demand as `write()` does, then returns a
`RelayGroupWriter`. The group writer appends objects with increasing IDs and sends the group's FIN on
`close()`. Pass the keyframe to `open_group()` itself to send it as object 0. Its delivery timeout
then counts the wait for the stream, which a bare `open_group()` has no object to charge.

Opening the group, each `group.write()`, and `group.close()` each get their own `write_timeout`
deadline. Time between calls does not count. A timeout resets that group and raises
`WRITE_TIMEOUT`. The track stays open for a new group at the next keyframe.

The following sketch is media publishing, not the data quickstart. The optional
[moq-media format package](packages/moq-media/README.md) supplies `loc_properties` and
`loc_track_properties`. Your encoder supplies the encoded frames, timestamps, and codec
configuration. Install `caprid-moq-media` alongside `caprid-relay` when you publish media (the
development workspace includes it). It also has the MSF catalog builder, source URLs, and H.264
configuration helpers that the Video.js example uses.

```python
from moq_media import loc_properties, loc_track_properties

video = publisher.register_track("video", track_properties=loc_track_properties())

if video.has_demand:  # the relay currently has an upstream subscription for the track
    group = await video.open_group(keyframe_bytes, properties=loc_properties(timestamp_us, video_config=avcc))
    await group.write(delta_bytes, properties=loc_properties(next_timestamp_us))
    await group.close()
```

A group that fails part-way, because the relay's subscription changed or ended, is reset instead of
completed, and the track stays open. Start the next group at the next keyframe.

`demand_changes()` iterates relay demand. It yields the current state first. After that it yields
`True` when the relay subscribes or replaces its subscription, so `True` can repeat, and `False` when
the subscription ends. A catalog track uses it to re-send its document on each relay subscription:

```python
async for demanded in catalog.demand_changes():
    if demanded:
        await catalog.write(catalog_document)
```

Demand is the relay's own subscription, not a viewer count and not a liveness signal. The relay fans
one upstream subscription out to every downstream viewer, so a second viewer produces no new `True`
and gets the relay's cached copy. moq-relay 0.15 and later cancel the subscription about one round
trip after the last viewer leaves. The next viewer then brings a new `True` and a re-send. A viewer
that resubscribes on the same connection within about 30 seconds can receive the relay's cached copy
before the re-sent one. `True` does not prove that anyone is watching, so write only state that is
safe to serve from cache and to receive more than once. Pausing an encoder while there is no demand
is safe, because a frame written then reaches no viewer. Resume at a keyframe, which a `FrameWriter`
asks for through `keyframe_needed`.

When the first viewer is on another relay, or is a moq-lite viewer such as `@moq/watch` on any relay,
the publisher's relay subscribes once to look up the track's info, cancels about 50 ms later, and
subscribes again. The track sees `True`, `False`, `True`. Objects written during that first
subscription reach no viewer, so do not rely on the first objects after demand begins. See
[relay demand](../spec/relay-interop.md#relay-demand) and
[a viewer on another relay](../spec/relay-interop.md#a-viewer-on-another-relay).

`caprid_relay.media` has both loops above ready-made. It is the media publishing policy the Video.js
example runs on:

- `CatalogService(track, document)` re-sends a document on every relay subscription, and retries a
  failed send while demand remains.
- `DocumentService(track)` does the same for a document you replace with `update(document)`, such as
  a media timeline. It also sends each update while the relay is subscribed.
- `FrameWriter(track)` maps encoded frames onto groups. By default a keyframe opens a group and deltas
  append to it. `grouping="frame"` sends one group per frame. The writer skips frames the relay has
  no subscription for instead of waiting on the deadline, and resumes at the next keyframe after any
  failure.

Each `await writer.write(frame)` returns what became of the frame (`sent`, `skipped`, `stale`,
`dropped`, `interrupted`, or `ended`) instead of raising. The writer serves writes from several tasks
in call order. When more than `max_backlog` frames wait behind the one being written, the writer
drops frames instead of queueing them. Under keyframe grouping it drops the waiting frames and the
group in progress. Under `grouping="frame"` it drops the oldest waiting frame.
`writer.keyframe_needed` and `on_keyframe_needed=` say when the next frame must be a keyframe: after a
demand change, a failure, a stale frame, or a drop. An encoder can then force one. A frame is any
object with `payload`, `key`, `properties`, and `discontinuity` attributes, so your own encoded-sample
type works once it carries its LOC properties. Pacing and time-based frame dropping stay in your
code. Pass a `stale` predicate to have the writer drop frames that went stale while a write blocked.
The contract is the "Media publishing" section of
[behavior.md](../spec/behavior.md#media-publishing). `examples/videojs_stream` puts all of this
together for a Video.js player.

### Text and JSON

Track and group writers have `await writer.write_text(text, properties=...)` and
`await writer.write_json(value, properties=...)`. Each call writes one UTF-8 object, with the same
demand, deadline, cancellation, and encoded byte-size limits as `write()`. The group writer's versions
also accept `status`. A serialization error leaves the writer usable.

Use the exported readers in subscription callbacks:

```python
from caprid_relay import RelaySubscriptionHandlers, read_json, read_text

text_handlers = RelaySubscriptionHandlers(on_object=lambda obj: print(read_text(obj)))
json_handlers = RelaySubscriptionHandlers(on_object=lambda obj: print(read_json(obj)))
```

`read_text(obj)` keeps a leading U+FEFF and raises `UnicodeDecodeError` on malformed UTF-8.
`read_json(obj)` parses UTF-8 JSON without schema validation and raises `ValueError` on invalid input,
which includes `UnicodeDecodeError` and `JSONDecodeError`. To skip malformed objects, catch decoding
errors inside the callback. Empty text is valid, but empty JSON is not. The readers leave payload
bytes and metadata unchanged, and the SDK adds no content-type properties.

JSON writing uses compact `json.dumps` with `ensure_ascii=False` and `allow_nan=False`. Unsupported
values, cycles, non-finite numbers, and unpaired surrogates fail locally. Reading also rejects `NaN`
and `Infinity`. Other conversions follow Python's standard JSON behavior. Text writing uses strict
UTF-8 encoding. Raw byte `write()` and plain subscriptions remain available.

## Errors and recovery

Branch on `code` and `phase`, not on message wording. SDK errors use these stable codes:

| Code | Application response |
| --- | --- |
| `CONNECTION_TIMEOUT` | Connecting exceeded its deadline. Consider a new connection with backoff. |
| `WRITE_TIMEOUT` | Inspect the phase, then choose the next object or group explicitly. |
| `DELIVERY_TIMEOUT` | The object outlived its delivery timeout (property `0x02`) before reaching the transport, and its group was reset. The data is stale. Write fresh data, at the next random-access point for a group, instead of retrying it as-is. |
| `CLOSED` | The handle is closed. Use an open connection, track, or group. |
| `WRITE_PENDING` | Await the outstanding operation, or finish or abort the open group, before starting another. |
| `NAMESPACE_NOT_SOLICITED` | The relay's namespace solicitations do not cover this publisher. Check the namespace and token configuration. A later matching solicitation can restore demand. |
| `NAMESPACE_ENDED` | The relay ended the namespace announcement. Open a replacement publisher if appropriate. |
| `SUBSCRIPTION_REQUEST_FAILED` | An async subscription request failed. Inspect its preserved MOQT request error and the connection state. |
| `SUBSCRIPTION_OVERFLOW` | The event buffer filled and the subscription was cancelled. Choose a larger capacity or consume faster before subscribing again. |
| `SUBSCRIPTION_CHANGED` | Upstream demand ended or changed during an operation. Choose fresh data or a new random-access point. |
| `UPDATE_QUEUE_FULL` | Eight updates already wait for the relay to accept the subscription. Wait for `subscribed` before updating again. |
| `PREFIX_OVERLAP` | `namespace_events` refused a prefix that equals, contains, or falls under one this connection already discovers. Nothing was sent. Widen one iterator's prefix and filter, or use a second connection. |
| `GOAWAY` | The relay asked the client to migrate. Open a replacement connection. |
| `RECONNECTING` | A supervised publisher had no generation up, or the connection ended under the operation. Nothing was replayed. Write the next object, or the next keyframe, once the publisher is live. |
| `TRANSPORT_ERROR` | Inspect the original cause and the connection state before deciding whether to reconnect or publish again. |
| `TRANSPORT_UNAVAILABLE` | No transport is available in this runtime. In TypeScript, supply a transport where browser WebTransport is missing. In Node, import from `@caprid/relay/node` with `@caprid/relay-quic` installed. |
| `UNAUTHORIZED` | The relay refused the connection's credentials: no token, a bad, malformed, or expired one, or an endpoint path the token does not grant. Supply a valid token before connecting again. TypeScript reports it too, in browsers only against moq-relay 0.17.0 or later. Older relays' refusals reach a browser as `TRANSPORT_ERROR` (#14). |

Each error also has a `phase`:

- `connecting` covers token resolution and connection setup.
- `connection` marks a failure of the whole connection. It makes no claim about individual writes
  that were in flight.
- `subscription` marks an async subscription request failure or a local event-buffer overflow.
- Publishing records `waiting-for-demand`, `opening-stream`, `writing`, or `closing` before it enters
  each step.

A `WRITE_TIMEOUT` in `waiting-for-demand` means this attempt sent no object bytes. For a timeout, a
transport failure, or a subscription failure in a transport phase, allow for partial delivery. A
failure in `closing` can come after all object bytes were sent. Opening a group can start sending its
header before the first object write. A phase records where the failure came from, and a later call
on the failed handle can report that same error.

For `WRITE_PENDING`, and for calls refused because a handle is already closed, the phase describes
the refused operation, and that call sends nothing. A write or group refused with `CLOSED` because
its track or group is already closed reports phase `waiting-for-demand`, the phase that means nothing
was sent. `CLOSED` can also interrupt an operation already in flight. It then carries that
operation's phase, so the code alone does not rule out partial delivery.

No code or phase promises a safe replay. Account for previous attempts, data freshness, and your
application's deduplication policy. A successful write means the transport accepted it, not that a
viewer acknowledged it. The plain connections do not retry or reconnect, and the
[supervisors](#stay-connected-supervised-connections) do.

`UNAUTHORIZED` is the one code that says the relay refused the credentials. The relay closed the
session with MOQT session termination code `UNAUTHORIZED` (0x2). `MALFORMED_AUTH_TOKEN` (0x16) and
`EXPIRED_AUTH_TOKEN` (0x18) map to it too. It can come in any phase. In phase `connection` it means
the credential of a live session expired or its grant was revoked ([token expiry](#token-expiry)).
TypeScript reports it too, though in a browser only against moq-relay 0.17.0 or later. Older relays'
close never reached the page, so a refusal arrived as `TRANSPORT_ERROR` (#14). No other connection
failure, and no uncovered namespace, proves that authentication was denied. A subscription outside
the token's grant is refused per request through `onError` or `on_error` (MOQT `UNAUTHORIZED`, 0x1),
and the connection stays open.

A clean connection shutdown still resolves `closed` without an error, and later operations fail with
`CLOSED`. After a failed connection, `closed` and later connection operations expose the same
failure. Caller cancellation reasons and token-provider exceptions keep their identity. Argument and
serialization errors keep their usual Python types. The subscription `on_error` callback keeps the
separate MOQT `RequestError` contract and its numeric protocol codes.

Python raises one `RelayError`, with the `code` above and a `phase`, as TypeScript does. The
`RelayErrorCode` and `RelayErrorPhase` aliases type them. The only subclasses are the three
subscription errors that carry extra data: `SubscriptionRequestError`, `SubscriptionOverflowError`,
and `SubscriptionConnectionClosedError`. Branch on `code`, never on the message.

A wrapped transport or session failure keeps the original exception in `__cause__`. The transport's
own exception is not a `RelayError`. A failure that surfaces through the Relay connection or
publishing API wraps it in a `RelayError` with `code == "TRANSPORT_ERROR"` and the transport's
exception as `__cause__`. Read that exception by its shape, as in TypeScript, not by its class, which
is private:

- `source` is `"stream"` when the peer reset or stopped one stream. `stream_error_code` is the peer's
  code, or `None` when unknown.
- `source` is `"session"` when the connection is over. `close_code` and `reason` are set when the
  peer closed it.

`moq_core.transport.is_stream_error` and `is_session_error` test for each shape. A credential refusal
is the relay's application CONNECTION_CLOSE with code 0x2 and reason `"unauthorized"`, and the SDK
reports it as `code == "UNAUTHORIZED"` instead. Explicit caller cancellation, including
`asyncio.CancelledError`, passes through unchanged.

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
native QUIC on aioquic. It is the same wire path `@caprid/relay-quic` uses on Node. MOQT runs
straight on the QUIC connection with no HTTP/3. The ALPN offers `moqt-22`, then `moqt-20` as a
fallback. SETUP carries the endpoint's path (with the token) and authority
([ADR 0004](../docs/adr/0004-python-dialing-end-speaks-native-quic.md)). The connection ends with an
application CONNECTION_CLOSE. TLS verification uses aioquic's default, the CA bundle from `certifi`,
not the operating system's certificate store. To supply a `QuicConfiguration` (custom CAs, QUIC
parameters), or any other implementation of the `moq_core.MoqtTransport` interface, pass
`create_transport`:

```python
from aioquic.quic.configuration import QuicConfiguration
from caprid_relay import connect_relay_subscriber
from caprid_relay.quic import connect_quic

configuration = QuicConfiguration(is_client=True, cafile="/path/to/ca.pem")
subscriber = await connect_relay_subscriber(
    url, create_transport=lambda url, protocols: connect_quic(url, protocols, configuration=configuration)
)
```

The package has no WebTransport session and no HTTP/3. Native QUIC is its only way to dial a relay.

### Receive throughput

The QUIC stack is pure Python, so the CPU of the event loop's one core limits a subscriber's receive
rate, not the network. On an idle host, a connection receiving 1 MiB objects from
`sjc.relay.mux.global` spends about 25 ms of CPU per MB, a ceiling of roughly **40 MB/s** (measured
2026-09-30, aioquic 1.3). A host whose CPU is contended, or an application that does its own work on
the same loop, gets much less. The adapter reads every datagram already waiting on the socket before
it transmits (#162). Most of the remaining cost is aioquic's per-packet receive work.

Media bitrates are far below this ceiling, but bulk data transfers may not be. A subscriber that
falls behind gets no signal. The relay keeps queueing toward it, and the only symptoms are growing
end-to-end latency and growing subscriber memory. The SDK has no transport-wide backpressure. The
async consumption limits bound local buffering only (see
[async subscription consumption](../spec/behavior.md#async-subscription-consumption)).

A very long-lived connection also grows slowly in memory. aioquic keeps every finished stream's ID in
`QuicConnection._streams_finished` for the life of the connection, and nothing prunes it. A publisher
therefore holds about 28 bytes for every group it has sent until it reconnects. That is roughly
2.4 MB a day for each group per second it sends, or 2.4 GB a day at 1000 groups per second.

## Video.js media example

[videojs_stream](examples/videojs_stream/README.md) streams live H.264, either a generated test
pattern or a transcoded file, with optional Opus audio, in the MSF and LOC shape that Video.js
consumes. It needs the `examples` dependency group (PyAV, numpy), which `uv sync` installs by default.
Follow that example's setup after you finish the data quickstart.

## Voice bot example

[voice_bot](examples/voice_bot/README.md) is a parrot voice bot for a browser user. It decodes the
user's microphone with `moq_media.opus.OpusDecoder`, waits for a pause, answers with `OpusEncoder`,
and sends an interrupt when the user talks over it. Speech-to-text, a language model, and
text-to-speech plug in through one async function, and the text of each reply goes on the transcript
track. Its browser side is `ts/examples/voice-bot`.

## Package boundaries

- `packages/caprid-relay`, imported as `caprid_relay`, owns the Relay product API, the authentication
  convention, the aioquic transport, and the media publishing and reading policy in
  `caprid_relay.media`.
- `packages/moq-core`, imported as `moq_core`, is the generic protocol package. It builds on its own,
  declares no dependencies, and imports no transport, player, UI, or media engine. It works over the
  abstract `moq_core.transport.MoqtTransport` interface that `caprid_relay` supplies.
- `packages/moq-media`, imported as `moq_media`, holds the LOC and MSF format helpers. It depends only
  on `moq_core` and imports neither Relay nor a transport library. Only `moq_media.opus` imports PyAV.
- `subscriber.track_source` exposes the generic `MoqTrackSource` contract for other integrations. It
  makes track requests in protocol terms, with the core's `on_ok` and `on_done` handler names, and
  observes closure. It has no track listing and no way to close the shared connection.

All three packages are Apache-2.0. The core is a port of the TypeScript core, which is itself an
attributed snapshot of the Video.js protocol modules and their regression tests. See
`packages/moq-core/UPSTREAM.json`.

## Differences from the TypeScript SDK

The observable behavior is the same. The spelling follows Python conventions:

| TypeScript | Python |
| --- | --- |
| `connectRelaySubscriber({ url, getToken, signal, connectTimeoutMs })` | `connect_relay_subscriber(url, get_token=..., cancellation=CancellationToken(), connect_timeout=seconds)` |
| `subscriber.subscribeEvents(options, { capacity, signal })` | `subscriber.subscribe_events(namespace, track, capacity=..., cancellation=...)` |
| `subscriber.namespaceEvents({ prefix, capacity, signal })` | `subscriber.namespace_events(prefix, capacity=..., cancellation=...)`. `NamespaceEvent.namespace` is a tuple, and a prefix that overlaps one the connection already discovers raises `RelayError` with code `PREFIX_OVERLAP`. |
| `subscriber.subscribe({ namespace, track, parameters }, handlers)` | `subscriber.subscribe(namespace, track, RelaySubscriptionHandlers(...), parameters=...)` |
| `onSubscribed`, `onPublisherDone` | `on_subscribed`, `on_publisher_done` |
| `publisher.registerTrack({ track, trackProperties, writeTimeoutMs })` | `publisher.register_track(track, track_properties=..., write_timeout=seconds)` |
| `writer.write(bytes, { properties })` | `await writer.write(payload, properties=...)` |
| `write()` takes any `ArrayBufferView`. A string, `ArrayBuffer`, or array raises `TypeError`. | `write()` takes any object with the buffer protocol (`bytes`, `bytearray`, `memoryview`, and others). A `str` or `list[int]` raises `TypeError`. |
| `subscriber.closed` resolves `{ error? }` | `await subscriber.closed` returns `Closed(error=...)` |
| `AbortSignal` | `CancellationToken` (`cancel(reason)` closes every connection given the token) |
| `RelayError` with `code` and `phase` | `RelayError` with `code` and `phase` |
| Varints capped at 2^53-1 | Full 64-bit range (Python integers are exact) |
| `openGroup()`, `hasDemand`, `demandChanges()`, `writer.closed` | `open_group()`, `has_demand`, `demand_changes()`, `writer.closed` (see `spec/behavior.md`) |
| `subscriber.trackSource` | `subscriber.track_source` |
| `writeText()`, `writeJSON()` | `write_text()`, `write_json()` on track and group writers |
| `readText(object)`, `readJSON(object)` | `read_text(obj)`, `read_json(obj)` |
| `roomEndpoint([endpoint,] room)` throws `RangeError` | `room_endpoint([endpoint,] room)` raises `ValueError` |
| Browser `WebTransport`, or `@caprid/relay-quic` on Node | aioquic native QUIC client (`caprid_relay.quic.connect_quic`) |
| `subscriber.want(key, { namespace, track, generations, staleAfterMs, retry, until, drainMs }, handlers)` | `subscriber.want(key, namespace, track, WantHandlers(...), generations=..., stale_after=seconds, retry=..., until=..., drain=seconds)` |
| `onSubscribed(info, { namespace })`, `onRestarted({ reason, namespace })`, `onSwitched({ from, to, subscribed })` | `on_subscribed(info, namespace)`, `on_restarted(reason, namespace)`, `on_switched(WantSwitchedInfo)` with `from_namespace`, `from_track`, `to_namespace`, `to_track` |
| `SupervisionBackoff` in milliseconds (`initialDelayMs`, `maxDelayMs`) | `SupervisionBackoff` in seconds (`initial_delay`, `max_delay`) |

## Scope and limits

The SDK subscribes to tracks over reliable streams, updates and cancels subscriptions, discovers
namespaces with `namespace_events` ([discovery contract](../spec/behavior.md#discovery)), and publishes
on a dedicated connection. The supervised connections add reconnection, resubscription, and token
rotation. The generic core also exposes FETCH and TRACK_STATUS. The SDK offers `moqt-22`, then
`moqt-20` as a fallback, and speaks whichever the relay selects.

The SDK does not refresh the token of a live session, receive datagrams, or discover tracks within a
namespace. A GOAWAY ends the connection with a `RelayError` of code `GOAWAY`. Subscriptions still
waiting for their first response end with it, never through `on_error`: `subscribed` raises
`SubscriptionConnectionClosedError` and `ended` resolves with `terminated`. Callers can open a
replacement connection.

## Development

Contributor setup, checks, formatting, and the opt-in live Relay smoke test for this workspace are in
the root [README](../README.md#development).

`packages/caprid-relay/tests/test_publish.py` runs the data publishing and subscribing integration
tests without a live relay:

```sh
cd python && mise exec -- uv run pytest packages/caprid-relay/tests/test_publish.py
```

`test_quic.py` runs the full smoke-test assertion list over a real QUIC loopback: native QUIC
listeners on `localhost` (aioquic, self-signed certificates) in front of the in-memory relay hub
(`tests/relay_helpers/quic_hub.py`). That exercises the default transport end to end in
`python:check`. The same file holds the native QUIC dialing end to the transport adapter rules, one
test per rule, against the conformance peer's native QUIC mode. `test_quic_session.py` drives the
session against a loopback server on the same QUIC plumbing (`tests/relay_helpers/quic_server.py`).
That server can impair its inbound link to test backpressure, the drain before close, pruning, the
keepalive, and the socket. The stream halves and the dropped-FIN retry
(`caprid_relay/quic/_fin_retry.py`) have in-process tests that need no socket: `test_quic_streams.py`
holds the halves to the same expectations as the core's in-memory `Pipe`, and
`test_quic_fin_retry.py` covers the retry. `test_quic_aioquic.py` pins the aioquic internals the
adapter reaches into, all named in `caprid_relay/quic/_aioquic.py`, and the aioquic behavior its
workarounds exist for. A test that needs real packets handshakes a client and server
`QuicConnection` in memory (`tests/relay_helpers/quic_pair.py`). The package pins
`aioquic>=1.3,<1.4` until that list is checked against a newer minor version. The live smoke test
dials native QUIC.

import-linter keeps `moq_core` from importing `caprid_relay`, `moq_media`, or any transport library,
and keeps `moq_media` from importing Relay or a transport library. The contracts are in
`pyproject.toml`. `packages/caprid-relay/tests/test_boundaries.py` pins the declared dependency lists.
`packages/moq-core/tests/test_spec_protocol.py` and `test_spec_vectors.py` check the core against the
shared `../spec`, which keeps this SDK in lockstep with the other languages.
