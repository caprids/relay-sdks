# Relay Python SDK

A standalone product SDK for Relay. Connect to a Relay endpoint and subscribe to arbitrary track
objects, or publish data, from any Python 3.11+ asyncio program. No browser is involved: the SDK
speaks WebTransport over HTTP/3 through aioquic.

This is the Python SDK in the Relay SDKs repository. The shared protocol pins, behavior contract, and
test vectors live in `../spec`; the root README describes the suite. Package names are provisional
and publication is disabled.

## Layout

- `packages/moq-core` (module `moq_core`): transport-independent MOQT primitives. Zero runtime
  dependencies and no knowledge of Relay, a port of `ts/packages/moq-core`.
- `packages/mux-relay` (module `mux_relay`): the Relay product API, authentication convention, and
  the aioquic WebTransport transport. Depends on `moq_core` and `aioquic`.

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

## Subscribe to a track

```python
import asyncio

from mux_relay import CancellationToken, SubscriptionHandlers, connect_relay


async def main() -> None:
    lifetime = CancellationToken()
    relay = await connect_relay(
        "https://relay.example.com/moq",
        # Obtain this token from your application backend.
        get_token=lambda: playback_token,
        cancellation=lifetime,
    )

    readings = relay.subscribe(
        ["sensors", "lab"],
        "readings",
        SubscriptionHandlers(
            on_ok=lambda info: print("Subscribed", info.track_properties),
            on_object=lambda obj: consume_bytes(obj.payload, obj.properties),
            on_error=lambda error: print("Subscription rejected", error),
            on_subgroup_reset=lambda gap: print("Objects may be missing", gap),
            on_done=lambda info: print("Publisher finished; trailing objects may still arrive", info),
        ),
    )

    closed = await relay.closed
    print("Connection ended", closed.error)

    # When this consumer finishes: readings.cancel()
    # When the application finishes: relay.close(), or lifetime.cancel()


asyncio.run(main())
```

Namespace fields are preserved verbatim. Known tracks can be subscribed to directly; the SDK neither
interprets the payload nor assumes that a track contains audio, video, or timestamps. An accepted
data subscription has no application inactivity deadline. The request timeout only bounds its
initial response. An idle connection still depends on peer traffic or transport-provided keepalive;
the SDK does not schedule its own keepalive. The Python transport uses aioquic's default 60-second
local idle timeout, subject to peer negotiation.
Subscriptions have independent cancellation; the application owns connection shutdown.

Object callbacks are plain synchronous callables invoked as streams deliver objects. The SDK adds no
application queue, ordering across streams, automatic JSON decoding, clock synchronization, or media
scheduling. Hand work to your own `asyncio.Queue` or task from the callback if it must not block
delivery. Handle subgroup resets and connection closure explicitly; this API makes no exactly-once
or gap-free delivery promise. `on_done` is the publisher's control notification, and may precede
trailing objects in transit.

Re-subscribing after `on_done` creates a new request and can replay cached objects followed
by another done notification while the publisher is absent. Apply retry backoff to done callbacks
as well as retryable request errors, and define any continuity policy in the application. See
[publisher restarts and resubscription](../spec/relay-interop.md#publisher-restarts-and-resubscription)
for retry, trailing-object, and publisher-generation guidance.

`connect_relay` has one deadline (`connect_timeout`, seconds) covering token resolution and both
handshakes. The optional `CancellationToken` controls the connection's entire lifetime: cancelling
it closes an established connection, and the `closed` future carries the reason. An explicit `jwt`
query parameter takes precedence over `get_token`, matching the current Relay connection
authentication convention. In-flight operations re-raise the application's exact cancellation reason;
with `cancel()` this is `Cancelled`, which is not a `RelayError`. Handle lifetime cancellation
separately from SDK errors (compare the exception to `lifetime.reason` when stopping normally).
`RelayConnection` is also an async context manager that closes on exit.

Namespaces contain 1–32 non-empty string fields, sent verbatim without splitting on `/`.
The 32-field protocol limit is pinned in `spec/protocol.json`. Oversized namespaces are
rejected before opening a publisher connection or sending a subscription; track names must
be non-empty strings.

## Publish data for testing

Use a dedicated publisher connection with a token authorized to publish the namespace:

```python
from mux_relay import connect_relay_publisher


async def publish() -> None:
    publisher = await connect_relay_publisher(
        "https://relay.example.com/moq",
        namespace=["testing", "data"],
        get_token=lambda: publish_token,
    )

    events = publisher.publish("events")

    try:
        await events.write_json({"message": "hello"})
    finally:
        events.close()
        await publisher.close()
```

In another client, use `connect_relay` to subscribe to namespace `["testing", "data"]`, track
`"events"`. The publisher connection currently serves one namespace with multiple tracks. It uses a
separate transport from subscriber connections; both directions share the authentication and
connection-lifetime implementation. No camera, microphone, catalog, or timestamps are required.

Publishing follows Relay's announce-and-serve ingest flow. `publish()` registers the track;
`write()` waits for the relay to subscribe upstream before sending bytes. Start a subscriber to
create that demand. The default 10-second write deadline (`write_timeout`) covers both demand and
transport writes. A `RelayTimeoutError` waiting for demand can be retried. If the relay has solicited
namespace prefixes but none covers this publisher's namespace, the demand deadline instead raises
`RelayNamespaceError`: check the token and namespace instead of blindly retrying. The track stays
open, and a later matching solicitation can let it proceed. An ordinary absence of subscriber
demand still raises `RelayTimeoutError`. A failed or cancelled write resets its stream and leaves
`track.closed` false while the publisher connection remains open. A relay subscription change
raises `RelayError`; a transport deadline raises `RelayTimeoutError`. The next explicit write uses
a new group ID. Choose fresh data or the next random-access point; already delivered bytes cannot
be withdrawn. If demand disappeared, the next write waits for a new relay subscription, and
`bindings()` continues reporting changes.

Each write copies its bytes and sends one independent group containing one object. Await each
write; a concurrent call raises `RelayWritePendingError` rather than growing a queue. Completion
means the transport accepted the complete group, not that a viewer acknowledged it. There is no
publisher-side replay or retention after an object is sent. Late-subscriber behavior also depends on
relay caching and subscription filters. A failed write may have delivered partial data; there is no
automatic retry.

Optional `track_properties` on `publish()` and `properties` on `write()` carry explicit protocol
metadata as `KeyValuePair` values. Property types and even-type values must be non-negative 64-bit
integers (excluding `bool`); odd-type values must expose a bytes-like buffer. Invalid properties
raise `TypeError` before a write starts or a track is registered. The SDK adds no default timescale,
timestamp, or LOC media packaging. Objects are limited to 16 MiB.

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

For media, a track needs more than one object per group: LOC/MSF maps one GOP to one MOQT group
whose object 0 is the keyframe. `open_group()` waits for relay demand like `write()`, then returns a
`RelayGroupWriter` that appends objects with increasing IDs and FINs the group on `close()`.
Opening the group, each `group.write()`, and `group.close()` get a separate `write_timeout`
deadline. Time between calls does not count toward a deadline. A timeout resets that group and
raises `RelayTimeoutError`; the track stays open for a new group at the next keyframe:


```python
video = publisher.publish("video", track_properties=[KeyValuePair(0x08, 1_000_000)])

if video.bound:  # the relay currently has a subscription for the track
    group = await video.open_group()
    await group.write(keyframe_bytes, properties=loc_properties(timestamp_us, video_config=avcc))
    await group.write(delta_bytes, properties=loc_properties(next_timestamp_us))
    await group.close()
```

A group that fails part-way (the relay's subscription changed or ended) is reset rather than
completed, and the track stays open; start the next group at the next keyframe. `bindings()` iterates
the track's demand (`True` on every subscribe, `False` on unsubscribe), which is how a catalog track
re-sends its document to each new relay subscription. `examples/videojs_stream` puts all of this
together for a Video.js player.

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
from mux_relay import SubscriptionHandlers, read_json, read_text

text_handlers = SubscriptionHandlers(on_object=lambda obj: print(read_text(obj)))
json_handlers = SubscriptionHandlers(on_object=lambda obj: print(read_json(obj)))
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

## Transport

`connect_relay` and `connect_relay_publisher` default to `mux_relay.connect_webtransport`, a
WebTransport-over-HTTP/3 client on aioquic: extended CONNECT with `WT-Available-Protocols: "moqt-20"`,
session-bound streams, CLOSE_WEBTRANSPORT_SESSION on shutdown. TLS verification uses the system CA
bundle that ships with `certifi`. Pass `create_transport` to supply a `QuicConfiguration` (custom
CAs, QUIC parameters) or any other implementation of the `moq_core.MoqtTransport` seam:

```python
from aioquic.quic.configuration import QuicConfiguration
from mux_relay import connect_relay, connect_webtransport

configuration = QuicConfiguration(is_client=True, cafile="/path/to/ca.pem")
relay = await connect_relay(
    url, create_transport=lambda url, protocols: connect_webtransport(url, protocols, configuration=configuration)
)
```

`packages/mux-relay/tests/test_webtransport.py` runs the full smoke-test assertion list over a real
QUIC loopback: a WebTransport server on `127.0.0.1` (aioquic, self-signed certificate) fronting the
in-memory relay hub, so the transport adapter is exercised end to end in `python:check`.

## Examples against a live Relay

`examples/mux_global` publishes JSON ticks to `sjc.relay.mux.global` and subscribes to them again,
either in one process or split across a publisher process and a subscriber process. See
`examples/mux_global/README.md`.

```sh
RELAY_TOKEN=... mise run python:example:mux-global -- --namespace mattypoo/relay-sdk-example
```

`examples/videojs_stream` streams live H.264 (a generated test pattern or a transcoded file, with
optional Opus audio) in the MSF/LOC shape the Video.js v10 MoQ player consumes and prints the
`moqt://` source URL to give the player. It needs the `examples` dependency group (PyAV, numpy), which
`uv sync` installs by default. See `examples/videojs_stream/README.md`.

```sh
RELAY_TOKEN=... mise run python:example:videojs -- --namespace mattypoo/relay-sdk-example/tv --show-token
```

## Package boundaries

- `packages/mux-relay`, `mux_relay`, owns the Relay product API, the authentication convention, and
  the aioquic transport.
- `packages/moq-core`, `moq_core`, is an independently buildable, Apache-2.0 generic protocol
  package. It declares no dependencies and imports no transport, player, UI, or media engine. It
  works over the abstract `moq_core.transport.MoqtTransport` seam that `mux_relay` supplies.
- `relay.tracks` exposes the generic `MoqTrackSource` contract for future integrations. It offers
  track requests and closure observation, with no operation to close the shared connection.
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
| `connectRelay({ url, getToken, signal, connectTimeoutMs })` | `connect_relay(url, get_token=..., cancellation=CancellationToken(), connect_timeout=seconds)` |
| `relay.subscribe({ namespace, track, parameters }, handlers)` | `relay.subscribe(namespace, track, SubscriptionHandlers(...), parameters=...)` |
| `publisher.publish({ track, trackProperties, writeTimeoutMs })` | `publisher.publish(track, track_properties=..., write_timeout=seconds)` |
| `writer.write(bytes, { properties })` | `await writer.write(payload, properties=...)` |
| `write()` takes any `ArrayBufferView`; a string, `ArrayBuffer`, or array raises `TypeError` | `write()` takes any bytes-like object, including `list[int]`; a `str` raises `TypeError` |
| `relay.closed` resolves `{ error? }` | `await relay.closed` returns `Closed(error=...)` |
| `AbortSignal` | `CancellationToken` (`cancel(reason)` closes every connection given the token) |
| `Error` messages | typed exceptions: `RelayClosedError`, `RelayTimeoutError`, `RelayGoawayError`, `RelayWritePendingError`, `RelayNamespaceError`, `WebTransportError` |
| Varints capped at 2^53-1 | full 64-bit range (Python integers are exact) |
| `openGroup()`, `bindings()`, `writer.closed` | `open_group()`, `bindings()`, `writer.closed` (see `spec/behavior.md`) |
| `writeText()`, `writeJSON()` | `write_text()`, `write_json()` on track and group writers |
| `readText(object)`, `readJSON(object)` | `read_text(obj)`, `read_json(obj)` |
| Browser `WebTransport` | aioquic WebTransport client (`connect_webtransport`) |

## Initial scope

This slice supports one connection, arbitrary reliable-stream track subscriptions, subscription
updates and cancellation, plus data-only publication on a dedicated connection. The generic core
also exposes FETCH and TRACK_STATUS. The protocol is pinned to `moqt-20`.

Automatic reconnection, token refresh on a live session, datagram reception, and product resource
discovery are not implemented yet. A GOAWAY ends the connection with a `RelayGoawayError` and fails
requests still awaiting their first response via `on_error`; callers can open a replacement.

## Live Relay smoke test

This opt-in test publishes and subscribes against a real relay. It is excluded from `python:check`
and needs a token authorized for both operations. The token can be supplied through a hidden prompt
or the `RELAY_TOKEN` environment variable; it is never written to a file and is redacted from output.

```sh
RELAY_NAMESPACE_PREFIX=mattypoo/relay-sdk-smoke mise run python:test:relay
```

Use the namespace prefix authorized by your token. The test appends a random UUID, checks binary,
empty, and text payloads plus independent track closure, then closes both connections. It defaults to
`https://sjc.relay.mux.global`; override that with `RELAY_URL` if needed. Namespace fields sent by
the SDK are absolute: include the token's root prefix when required by the relay.

The live test passed against `sjc.relay.mux.global` on 2026-09-11 over the aioquic WebTransport
transport: three objects on `events` and two on `status`, with the second status object delivered
after the events track closed, and both connections closed cleanly.

`RelayError` is the base class for Relay connection and publishing failures, including
`WebTransportError` for QUIC, WebTransport session, and stream failures. Transport errors preserve
their message and optional `code` and `reason` details; catch `RelayError` to handle these together
with the SDK's deadline and closed errors.
