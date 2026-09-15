# Relay JavaScript SDK

A standalone product SDK for Relay. Connect to a Relay endpoint and subscribe to arbitrary
track objects without installing Video.js, loading a media catalog, or creating a player.

This is the TypeScript SDK in the Relay SDKs repository. The shared protocol pins, behavior
contract, and test vectors live in `../spec`; the root README describes the suite. Package names
are provisional and publication is disabled.

## Development

Tool versions are pinned in the repository's root `mise.toml`. Run the root tasks from anywhere in
the repository, or run pnpm from this directory under `mise exec`:

```sh
mise install          # once
mise run ts:install
mise run ts:check     # equivalent to: cd ts && mise exec -- pnpm check
mise run ts:lint      # Biome format and lint check only
mise run ts:fix       # apply Biome's safe formatting and lint fixes
```

Formatting and linting are configured in `biome.jsonc`, and every package extends the strict compiler
options in `tsconfig.base.json`. `pnpm lint` fails on warnings, so a clean check prints nothing.

## Browser token exposure

Relay currently authenticates with `?jwt=<token>` on the WebTransport URL. On a failed handshake,
Chromium can log that full URL, including the token, to its own console. The SDK cannot suppress
those browser-generated messages. This can expose a live token to console collectors and error
reporting services even when the error returned by `connectRelay` contains no token.

Before forwarding browser diagnostics, remove `jwt` query values and token strings from console
breadcrumbs, network URLs, traces, and error reports, or disable collection of those fields. Use
short-lived tokens scoped to the namespaces and operations the application needs. The repository's
CLI tools redact the supplied token from their own output; that does not sanitize the browser's
console or third-party collectors. Moving authentication off the URL requires a relay protocol
change and is not supported by this SDK today.

## Subscribe to a track

```ts
import { connectRelay } from '@mux/relay';

const lifetime = new AbortController();
const relay = await connectRelay({
  url: 'https://relay.example.com/moq',
  // Obtain this token from your application backend.
  getToken: () => playbackToken,
  signal: lifetime.signal,
});

const readings = relay.subscribe(
  { namespace: ['sensors', 'lab'], track: 'readings' },
  {
    onOk: (info) => console.log('Subscribed', info.trackProperties),
    onObject: (object) => consumeBytes(object.payload, object.properties),
    onError: (error) => console.error('Subscription rejected', error),
    onSubgroupReset: (gap) => console.warn('Objects may be missing', gap),
    onDone: (info) => console.log('Publisher finished; trailing objects may still arrive', info),
  },
);

void relay.closed.then(({ error }) => console.log('Connection ended', error));

// When this consumer finishes: readings.cancel();
// When the application finishes: relay.close(); or lifetime.abort();
```

Namespace fields are preserved verbatim. Known tracks can be subscribed to directly; the SDK
neither interprets the payload nor assumes that a track contains audio, video, or timestamps.
An accepted data subscription can stay quiet indefinitely. The request timeout only bounds its
initial response. Subscriptions have independent cancellation; the application owns connection shutdown.

Object callbacks run synchronously as streams deliver objects. The SDK adds no application queue,
ordering across streams, JSON decoding, clock synchronization, or media scheduling. Handle subgroup
resets and connection closure explicitly; this API makes no exactly-once or gap-free delivery promise.
`onDone` is the publisher's control notification, and may precede trailing objects in transit.

`connectRelay` has a deadline covering token resolution and both handshakes. Its optional abort
signal controls the connection's entire lifetime. An explicit `jwt` query parameter takes precedence
over `getToken`, matching the current Relay connection authentication convention. Every deadline
(`connectTimeoutMs`, `requestTimeoutMs`, and `writeTimeoutMs`) must be between 0 (exclusive) and
2147483647 milliseconds, the largest delay a JavaScript timer holds. A larger value is a `RangeError`
rather than a deadline that fires immediately.

## Publish data for testing

Use a dedicated publisher connection with a token authorized to publish the namespace:

```ts
import { connectRelayPublisher } from '@mux/relay';

const publisher = await connectRelayPublisher({
  url: 'https://relay.example.com/moq',
  namespace: ['testing', 'data'],
  getToken: () => publishToken,
});

const events = publisher.publish({ track: 'events' });

try {
  await events.write(new TextEncoder().encode(JSON.stringify({ message: 'hello' })));
} finally {
  events.close();
  await publisher.close();
}
```

In another client, use `connectRelay` to subscribe to namespace `['testing', 'data']`, track
`'events'`. The publisher connection currently serves one namespace with multiple tracks.
It uses a separate transport from subscriber connections; both directions share the authentication
and connection-lifetime implementation. No camera, microphone, catalog, or timestamps are required.

Publishing follows Relay's announce-and-serve ingest flow. `publish()` registers the track;
`write()` waits for the relay to subscribe upstream before sending bytes. Start a subscriber to
create that demand. The default 10-second write deadline covers both demand and transport writes.
A timeout waiting for demand can be retried. A failure once a write is bound to a relay subscription
ends that track, since an incomplete group may have reached the relay.

Each write copies its bytes and sends one independent group containing one object. The payload is a
`Uint8Array`; any other `ArrayBufferView` is accepted and copied as the raw bytes it spans, while a
string, an `ArrayBuffer`, or an array is a `TypeError` raised before the write starts, so the track
stays usable. Await each write; a concurrent call rejects rather than growing a queue. Completion
means the transport accepted the complete group, not that a viewer acknowledged it. There is no publisher-side replay
or retention after an object is sent. Late-subscriber behavior also depends on relay caching and
subscription filters. A failed write may have delivered partial data; there is no automatic retry.

Optional `trackProperties` on `publish()` and `properties` on `write()` carry explicit protocol
metadata. Property types and even-type integer values must be non-negative safe integers; odd-type
values must be `ArrayBufferView`s and are copied as their raw bytes. Invalid properties raise a
`TypeError` naming the property index before registering a track or starting a write. Raw tracks have no default timescale or LOC framing. Objects are limited to 16 MiB.
`events.close()` cancels an outstanding write and ends that track; other tracks keep running.
`publisher.close()` ends all tracks, retracts the namespace, and waits for transport shutdown.
Track names cannot be reused within a publisher connection, including after their writer closes.

### Multi-object groups and relay demand

For media, use `openGroup()` to put multiple objects on one stream. Each object gets the next ID,
starting at 0; `group.close()` sends the group's FIN. This amortizes stream creation and completion
across a group, such as a video GOP, instead of waiting for a FIN for every frame.

```ts
const video = publisher.publish({ track: 'video', writeTimeoutMs: 10_000 });
const lifetime = new AbortController();
const group = await video.openGroup({ signal: lifetime.signal });
try {
  for (const frame of encodedGop) {
    await group.write(frame.bytes, { properties: frame.properties });
  }
  await group.close();
} catch (error) {
  group.abort(error);
  throw error;
}
```

Only one write or open group may occupy a track, and group writes/close must be awaited serially.
The deadline applies separately to opening the group (including demand), each object write, and
closing it. Idle time between group operations is unrestricted. `group.abort(reason)`, its optional
`AbortSignal`, a relay re-subscription, or an operation failure resets the group and releases the
track for another group. Start the next group at a random-access point. Local payload/property
validation errors leave the group usable. `group.closed`, `group.groupId`, and `group.objectCount`
report its state; the count includes only successfully accepted objects. Empty payloads may carry
`status: 'end-of-group'` or `'end-of-track'`. Application queues and frame dropping remain the
application's responsibility.

`track.bound` reports current relay demand. `track.closed` distinguishes a retired track from a
track waiting for demand. `track.bindings()` yields the current demand state followed by changes,
including `true` again for replacement subscriptions, and ends when the track ends. For example,
a catalog can be re-sent on each relay subscription:

```ts
for await (const bound of catalog.bindings()) {
  if (bound) await catalog.write(catalogBytes);
}
```

Breaking the loop releases its observer. Explicit iterator consumers can call `return()` to release
an observer, including a pending `next()`. Demand is the relay's upstream subscription; it does not
represent each individual downstream viewer.

Custom transports with asynchronous sink writes or closes must honor the writable controller's
abort signal to interrupt them and reset their underlying stream, as browser WebTransport does.
A generic `WritableStream` sink that ignores the signal can finish its in-flight close after an abort.

Run the data-only publishing/subscribing integration test without a live relay:

```sh
cd ts && mise exec -- pnpm test tests/publish.test.ts
```

## Example against a live Relay

`examples/mux-global` publishes JSON ticks to `sjc.relay.mux.global` and subscribes to them again,
either in one page or split across a publisher process and a subscriber process. Node has no
WebTransport, so the runner drives the SDK in headless Chromium. See `examples/mux-global/README.md`.

```sh
RELAY_TOKEN=... mise run ts:example:mux-global -- --namespace mattypoo/relay-sdk-example
```

## Package boundaries

- The `ts/` root package, `@mux/relay`, owns the Relay product API and authentication convention.
- `packages/moq-core` is an independently buildable, Apache-2.0 generic protocol package. It uses
  standard streams, has no runtime dependencies, and imports no player, UI, or media engine.
- `relay.tracks` exposes the generic `MoqTrackSource` contract for future player integrations. It
  offers track requests and closure observation, with no operation to close the shared connection.
- `tests/spec-protocol.test.ts` and `tests/spec-vectors.test.ts` check the core against the shared
  `../spec`, which is how this SDK stays in lockstep with the other languages.

The core starts from an attributed snapshot of the existing Video.js protocol modules and their
regression tests. Video.js is unchanged and still has the original implementation. Migrating it to
this separately versioned dependency is the next integration step; the snapshot is not intended to
become a separately maintained protocol fork. See `packages/moq-core/UPSTREAM.json`.

## Initial scope

This slice supports one connection, arbitrary reliable-stream track subscriptions, subscription
updates and cancellation, plus data-only publication on a dedicated connection. The borrowed generic interface also exposes FETCH and TRACK_STATUS.
The protocol is pinned to `moqt-20`, matching the extracted implementation.

Automatic reconnection, token refresh on a live session, datagram reception, product
resource discovery, typed payload helpers, and a Video.js adapter are not implemented yet. A GOAWAY
ends the connection with an error; callers can open a replacement. A custom `createTransport`
factory supports tests or runtimes with a compatible transport; the default uses browser WebTransport.

## Live Relay smoke test

This opt-in Chromium test publishes and subscribes against a real relay. It is excluded from
`pnpm check` and needs a token authorized for both operations. The token can be supplied through
hidden stdin input or the `RELAY_TOKEN` environment variable; it is never written to a file.

```sh
cd ts && mise exec -- pnpm exec playwright install chromium   # once
RELAY_NAMESPACE_PREFIX=mattypoo/relay-sdk-smoke mise run ts:test:relay
```

Use the namespace prefix authorized by your token. The test appends a random UUID, checks binary
and empty payloads plus independent track closure, then closes both connections. It defaults to
`https://sjc.relay.mux.global`; override that with `RELAY_URL` if needed. Namespace fields sent by
the SDK are absolute: include the token's root prefix when required by the relay.

The live test passed against `sjc.relay.mux.global` on 2026-09-09: three objects on `events` and two
on `status`, with the second status object delivered after the events track closed.
