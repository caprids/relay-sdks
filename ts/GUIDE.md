# Relay JavaScript SDK

Publish and subscribe to arbitrary track objects through Relay without Video.js, a media catalog,
or a player. `@mux/relay` runs in a browser page: its default transport is **browser WebTransport
in a secure context** (HTTPS or localhost). In Node 24+, import from `@mux/relay/node`, which
connects over native QUIC through the optional `@mux/relay-quic` package (see the
[README's Node section](README.md#node)). Other runtimes must supply a compatible
`createTransport` factory.

This is the TypeScript SDK in the Relay SDKs repository. The shared protocol pins, behavior
contract, and test vectors live in [spec](../spec/README.md); the [root README](../README.md) describes the suite. Package names
are provisional and publication is disabled.

## First round trip

The packages are **unpublished**, so the first round trip runs the repository's example from a
checkout; there is no registry install step. The example launcher runs the demo in Node, over
[`@mux/relay-quic`](packages/relay-quic/README.md)'s native QUIC transport, and prints its output in
the terminal. You need Git and
[mise](https://mise.jdx.dev/getting-started.html) available on your PATH. Commands below run in Bash
from the repository root.

### Endpoint and credentials

Ask the team operating your Relay deployment (or your application's backend owner) for an HTTPS
WebTransport endpoint, your token's root (the namespace prefix it grants), and a
short-lived Relay JWT authorized to **publish and subscribe** there. The SDK consumes tokens; it
does not issue them or provide a public token signup flow. Do not use a Mux API access token in
place of a Relay JWT. Use an endpoint whose path is your token's root (for example
`https://relay.mux.global/acme/rooms/42` for a token rooted at `acme/rooms/42`), and name
namespaces relative to it (`['p', 'a']` rather than `['acme', 'rooms', '42', 'p', 'a']`). The
connection then cannot reach outside that root, and a path the token does not grant fails at
connect with `UNAUTHORIZED`. A client that spans several rooms under one grant can use the bare
host, where namespace fields include the token's root
([namespaces](../spec/behavior.md#subscribing)). The Mux relays carry announcements between regions,
so a publisher and a subscriber can each use their nearest relay
([a viewer on another relay](../spec/relay-interop.md#a-viewer-on-another-relay)). The hosted demo defaults to the bare host
`https://relay.mux.global`, which still requires authorized credentials. For another deployment,
use the endpoint supplied by its operator.

`roomEndpoint` builds that endpoint from a relay and the token's root, so the URL is not assembled by
hand:

```ts
import { roomEndpoint } from '@mux/relay';

const url = roomEndpoint('https://relay.mux.global', ['acme', 'rooms', '42']);
// 'https://relay.mux.global/acme/rooms/42'
```

It appends each field to the endpoint's path and keeps the endpoint's query. It encodes nothing,
because the relay reads the path without decoding it. A field may hold only letters, digits and
`-._~!$&'()+,;=:@`, and may not be `.` or `..`; any other field throws a `RangeError`. Reach a room
with such a field from the bare host
([room-rooted endpoints](../spec/behavior.md#room-rooted-endpoints)).

The runner reads `RELAY_TOKEN` when set, otherwise prompts with hidden input. Prefer the prompt for
manual runs; automation can inject the environment variable through its secret manager. Do not put
tokens in commands, source files, URLs, or committed `.env` files. Tokens are passed to `getToken`
inside the demo and are redacted from its output.

### Browser token exposure

Relay currently authenticates with `?jwt=<token>` on the WebTransport URL. On a failed handshake,
the browser can log that full URL, including the token, to its own console. The SDK cannot suppress
those browser-generated messages. This can expose a live token to console collectors and error
reporting services even when the error returned by `connectRelaySubscriber` contains no token.

Before forwarding browser diagnostics, remove `jwt` query values and token strings from console
breadcrumbs, network URLs, traces, and error reports, or disable collection of those fields. Use
short-lived tokens scoped to the namespaces and operations the application needs. The repository's
CLI tools redact the supplied token from their own output; that does not sanitize the browser's
console or third-party collectors. Moving authentication off the URL requires a relay protocol
change and is not supported by this SDK today.

### Install and run

```sh
git clone https://github.com/muxinc/relay-sdks.git
cd relay-sdks
mise install
mise run ts:install
```

The example task also builds `@mux/relay-quic`'s native addon, which needs Rust until prebuilt binaries
ship ([#195](https://github.com/muxinc/relay-sdks/issues/195)). The mise task brings the pinned
toolchain; see [the example's setup notes](examples/mux-global/README.md#run-it).

A publisher's `write()` waits for relay demand: a matching subscriber must be running. Even when
publishing connects first, a fresh namespace can briefly reject subscriptions with request error 16
`unroutable` (404 `Broadcast not found` on relays before 0.15.8) while its announcement propagates,
about a round trip. The combined demo starts both sides, waits for the publisher's `announced`,
retries only that subscription rejection with capped backoff, and waits for demand before writing.
Once a write starts, a failure stops the run; it never automatically replays potentially delivered
bytes.

Enter the endpoint, ending in your token's root, and a **fresh namespace below it** when prompted
below. The CLI splits `quickstart/unique-run` into `['quickstart', 'unique-run']`; the SDK itself sends
fields verbatim. Both connections use those same fields and track `ticker`. If your token authorizes
only one exact namespace, enter the bare host as the endpoint and that whole namespace (a namespace
needs at least one field), and avoid overlapping publishers or stale cached objects.

```sh
read -r -p "Relay HTTPS endpoint with your token's root as its path: " RELAY_URL
read -r -p 'Namespace below that root (slash-separated): ' RELAY_NAMESPACE
mise run ts:example:mux-global -- --url "$RELAY_URL" --namespace "$RELAY_NAMESPACE" --track ticker --count 1 --timeout 30
# Enter the Relay token at the hidden prompt.
```

The task builds the local packages and the native addon, then runs the demo in Node. The complete SDK example, including imports and cleanup, is
[demo.js](examples/mux-global/demo.js); [run.mjs](examples/mux-global/run.mjs) supplies the explicit
configuration and token. Edit the demo to adapt the payload for your application.

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
`finally` closes the connections on success or failure. A write completing
only means the transport accepted it; the `received` line establishes this demo's round trip.

### Use the local packages in your application

The supported first-use path is the workspace above. To use the build in an existing browser app,
serve `ts/dist/` and `ts/packages/moq-core/dist/` as JavaScript modules and map **both** bare imports:

```html
<script type="importmap">
{"imports":{"@mux/relay":"/sdk/index.js","@mux/moq-core":"/core/index.js"}}
</script>
```

Here `/sdk/` maps to `ts/dist/` and `/core/` to `ts/packages/moq-core/dist/`; preserve each directory's
module layout. Import `@mux/relay` from a module script in your secure browser page. Rebuild with
`mise run ts:build` after editing sources. Copying only the Relay build or installing its directory
in isolation does not resolve its `workspace:*` core dependency.

## Subscribe and publish separately

Use two terminals in the same checkout. Set `RELAY_URL` and `RELAY_NAMESPACE` in **each** terminal
using the prompts above, with the exact same values. Each process prompts for its own token; the
subscriber needs subscribe permission and the publisher needs publish permission. Start them within
60 seconds of each other, in either order. A subscriber started first retries fresh-namespace
rejections; a publisher started alone waits for demand and then fails at the overall deadline.

Subscribe to one object:

```sh
mise run ts:example:mux-global -- --mode subscribe --url "$RELAY_URL" --namespace "$RELAY_NAMESPACE" --track ticker --count 1 --timeout 60
```

Publish one object from the other terminal:

```sh
mise run ts:example:mux-global -- --mode publish --url "$RELAY_URL" --namespace "$RELAY_NAMESPACE" --track ticker --count 1 --timeout 60
```

Success is `finished published=0 received=1` in the subscriber and `finished published=1 received=0`
in the publisher. Both modes use the same bounded error handling and cleanup as the combined run.
See [demo options](examples/mux-global/README.md) for multiple objects and intervals.

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
relay's answer: it resolves with the acceptance info (`onSubscribed` receives the same) and rejects
with `SubscriptionRequestError` on rejection or request timeout, `SubscriptionConnectionClosedError`
if the connection ends first, or your own reason if you cancelled. Await `subscription.ended` to
learn that the subscription can no longer deliver and why: it resolves once, never rejects, with
`kind` (`rejected`, `terminated` or `cancelled`), the protocol `cause`, whether it had been
accepted, and the error.

```ts
const subscription = subscriber.subscribe({ namespace, track }, { onObject: handle });
await subscription.subscribed; // throws on rejection, closure, or cancel
subscription.ended.then((end) => console.log(`subscription ${end.kind}`, end.error));
```

Cancelling a subscription stops that consumer (including trailing objects after `onPublisherDone`),
while closing the connection stops all of its subscriptions. `onPublisherDone` settles neither
awaitable: it is the publisher's control notification, trailing objects can still arrive, and the
request stays open until you cancel it or the connection ends. The demo waits for its object count
or the deadline. The full state table is in the
[behavior contract](../spec/behavior.md#subscription-lifecycle).

Namespace fields are preserved verbatim. Known tracks can be subscribed to directly; the SDK
neither interprets the payload nor assumes that a track contains audio, video, or timestamps.
An accepted data subscription has no application inactivity deadline. The request timeout only
bounds its initial response. An idle connection still depends on peer traffic or transport-provided
keepalive; the SDK does not schedule its own keepalive. Subscriptions have independent cancellation; the application owns connection shutdown.

A subscription without a location filter (no `parameters.locationFilter`, or `{ type: 'none' }`)
starts at the live edge on Relay: a subscriber that joins an ongoing track receives only the objects
published after it joined, although the relay still holds earlier ones. To reach back, pass
`parameters: { locationFilter: { type: 'relative-group', groupsBeforeNext: N } }` (`1` starts at the
current group, `N` at N-1 groups before it) or `{ type: 'absolute', start }` with an explicit start;
the relay serves those from its cache, as far back as it still holds. See
[late subscribers](../spec/relay-interop.md#late-subscribers-and-location-filters).

Object callbacks run synchronously as streams deliver objects. The SDK adds no application queue,
ordering across streams, automatic JSON decoding, clock synchronization, or media scheduling. Handle subgroup
resets and connection closure explicitly; this API makes no exactly-once or gap-free delivery promise.
The SDK reports every reset it receives, but the relay may drop a reset for a subgroup it had not
begun forwarding, so an aborted group can leave no trace except a gap in group IDs
([subgroup resets](../spec/relay-interop.md#subgroup-resets)).
`onPublisherDone` is the publisher's control notification: it may precede trailing objects in
transit, and it does not end the subscription. Cancel the subscription when you are finished with it.

The SDK sends every subscription parameter you set, but moq-relay acts on only some of them. It
honors `subscriberPriority` (for media, see [`mediaSubscribeParameters`](#subscribing-to-media)) and `locationFilter` on the subscribe, and ignores `groupOrder` (the
relay always serves Descending), `newGroupRequest`, both delivery timeouts and a per-request
`authorizationTokens`. With `newGroupRequest` ignored there is no protocol key-frame request through
the relay: an application that needs one sends it to the publisher on a track of its own. See
[subscription parameters](../spec/relay-interop.md#subscription-parameters).

`subscription.update(parameters)` sends REQUEST_UPDATE, the protocol's way to change what an
accepted subscription delivers. **moq-relay does not implement it**: the relay never acknowledges an
update, and any update, including `{ forward: 0 }` or an empty one, ends the subscription within one
round trip with `onPublisherDone` (status 2 `track ended`) and stops delivery; `onUpdateOk` never
fires. To pause, cancel the subscription and subscribe again later. To change the track, the priority
or the filter, use a [switchable subscription](#switching-tracks), which opens the new subscription
before it cancels the old one ([relay interop deviation 4](../spec/behavior.md#relay-interop-deviations)).
Subscribing with `parameters: { forward: 0 }` does not work either: moq-relay refuses it with request error 3
`FORWARD=0 not supported` through `onError`, and the connection stays open.
The protocol allows updates only after acceptance, so an update issued before `onSubscribed` is
queued in order and sent as soon as the relay accepts; at most eight wait, and a further one throws
a `RelayError` with code `UPDATE_QUEUE_FULL`. Updating an ended subscription throws `CLOSED`. A
relay that refuses an update with REQUEST_ERROR reaches `onError`, which ends the subscription
([#94](https://github.com/muxinc/relay-sdks/issues/94)).

Re-subscribing after `onPublisherDone` creates a new request, and it cannot bring back a finished
track. Once the publisher has gone the relay answers request error 16 `unroutable`; while its
namespace stays announced, the relay replays the finished track's cached last group followed by
another done, one round trip later each time. Don't re-subscribe to a track the publisher ended
until the application knows new content exists; a status-2 done alone does not show that, since
moq-relay also sends one after this subscription's own `update()`, and then re-subscribing is the
way to apply the change. Apply retry backoff to done callbacks as well as retryable
request errors, and define any continuity policy in the application. See
[publisher restarts and resubscription](../spec/relay-interop.md#publisher-restarts-and-resubscription)
for retry, trailing-object, and publisher-generation guidance.

`connectRelaySubscriber` has a deadline covering token resolution and both handshakes. Its optional abort
signal controls the connection's entire lifetime. An explicit `jwt` query parameter takes precedence
over `getToken`, matching the current Relay connection authentication convention. An empty one
(`?jwt=`) is a `RangeError` before anything connects: put a token in it, or drop it and use
`getToken`. So is an endpoint with another scheme, a fragment, or userinfo; an endpoint that is not
a string holding an absolute URL is a `TypeError`. Every deadline (`connectTimeoutMs`,
`requestTimeoutMs`, and `writeTimeoutMs`) must be between 0 (exclusive) and 2147483647
milliseconds, the largest delay a JavaScript timer holds. A larger value is a `RangeError` rather
than a deadline that fires immediately; a value that is not a number is a `TypeError`.

### Token expiry

The relay ends a session when its token expires, and a live session cannot take a new token. Every
connection exposes `expiresAt`, the time that will happen: from the JWT's `exp` claim, or from a
`RelayCredential` your token provider returns when the token is not a JWT or your server knows
better.

```js
const subscriber = await connectRelaySubscriber({
  url,
  getToken: async () => {
    const { token, expiresAt } = await fetchRelayToken(); // your server
    return { token, expiresAt: new Date(expiresAt) };
  },
});
console.log('ends at', subscriber.expiresAt);
```

At that time the connection closes with `UNAUTHORIZED` in phase `connection`. The primitives do not
prevent it: open a replacement before then. The [supervised connections](#stay-connected-supervised-connections)
do it for you, make-before-break, a minute before expiry by default (`rotateBeforeMs`).

Both roles take the same connection options, `ConnectRelayOptions`: `url`, `getToken`, `signal`,
`connectTimeoutMs`, and `createTransport`. `ConnectRelaySubscriberOptions` adds `requestTimeoutMs`,
the deadline for a subscription's initial response. `ConnectRelayPublisherOptions` adds `namespace`
and has no request deadline: each track's `writeTimeoutMs` bounds its wait for demand and its
writes. Passing `requestTimeoutMs` to `connectRelayPublisher` is a type error; untyped JavaScript
that passes it anyway has it ignored, like any other option the call does not take.

### Switching tracks

REQUEST_UPDATE names no track, so moving a viewer between layers (`video-hi`, `video-mid`,
`video-lo`) takes a new subscription. `subscribeSwitchable` keeps one set of handlers on whichever
subscription is active and moves them make-before-break: `switchTo` subscribes to the new track while
the old one keeps delivering, and cuts over once a new object passes `until`.

```ts
const switchable = subscriber.subscribeSwitchable(
  { namespace, track: 'video-mid' },
  {
    onSwitched: ({ from, to }) => decoder.reset(), // the next object starts the new layer
    onObject: (object, track) => decoder.decode(object, track),
  },
);
await switchable.subscribed;

// Cut over on a new-layer object no later than the next frame and no more than a few frames behind
// the media already delivered (your own timestamps, not group IDs). Keep the old layer open for a
// second to deliver groups still in flight on it.
await switchable.switchTo('video-hi', {
  until: (object) => {
    const t = timestampOf(object);
    return t <= lastDelivered + frameDuration && t >= lastDelivered - 5 * frameDuration;
  },
  drainMs: 1_000,
  signal: AbortSignal.timeout(5_000),
});
```

- Handlers see only the active subscription, and get its track name as their last argument.
  `onSwitched` runs before the first object of the new track. A switchable has no `update()`; switch to
  the same track with new `parameters` to change a filter or priority. Parameters are not inherited.
- `until` defaults to `'first-object'`. That is fine where delivery is in order, but it can skip media:
  the relay starts a subscription at its cached latest group, which can be stale, and a subscriber on
  another relay can lose the first object ([#243](https://github.com/muxinc/relay-sdks/issues/243)).
  Compare your own timeline, not group IDs: each track numbers its own groups.
- If the new subscription fails, a newer `switchTo` supersedes it, or the signal aborts, `switchTo`
  rejects and the old track keeps playing. `ended` settles when the active subscription ends;
  `cancel()` cancels everything. See [switching tracks](../spec/behavior.md#switching-tracks) for the
  full contract and measured cutover times.

### Browser transport limits

The browser's QUIC stack, not the SDK, owns the connection, and a page cannot configure it. It can
end a connection before any SDK deadline acts. The SDK reports what the browser reports and cannot
tell these cases from other losses. Plan for:

- **A connect that fails early.** A handshake that hears nothing for about 4 s fails with
  `TRANSPORT_ERROR` "Opening handshake failed." in phase `connecting`, even when `connectTimeoutMs`
  is longer. A connect to a port nothing listens on fails within milliseconds. In a retry policy,
  treat `TRANSPORT_ERROR` in phase `connecting` like `CONNECTION_TIMEOUT`.
- **A connection lost after about 5 s of silence.** A connection with data outstanding that hears
  nothing for about 5 s is closed, even if the network has since come back. The app sees
  `TRANSPORT_ERROR` "Connection lost." in phase `connection`, with a `WebTransportError` cause.
- **Networks that carry only small packets.** Some tunnels and VPNs with a small MTU never let the
  handshake through; the connect fails as above.

These figures were measured in Chromium 147 (2026-09); other browsers are untested and may differ.
The QUIC-level causes, and how the Python SDK differs, are recorded in
[browser QUIC limits](../spec/relay-interop.md#browser-quic-limits).

On a network that blocks UDP, none of this applies because nothing connects; see
[networks without UDP](#networks-without-udp-qmux-over-websocket) for the WebSocket transport.

A browser publisher that must survive outages longer than about 5 s, or heavy burst loss, has to
open a new connection and register its tracks again; see
[publisher recovery](../spec/publisher-recovery.md). Subscriptions on the old connection are not
handed to the new one ([#34](https://github.com/muxinc/relay-sdks/issues/34)). The SDK does not
reconnect for you.

Namespaces are arrays of 1–32 non-empty string fields, sent verbatim without splitting on `/`; a
single string is a `TypeError`, not a one-field namespace. The 32-field protocol limit is pinned in
[protocol.json](../spec/protocol.json) and exceeding it is a `RangeError`. Track names must be
non-empty strings. Both rules apply to publishing and subscribing alike, and are checked before the
token provider, the transport factory, or any connection state is consulted; see the Arguments table
in [behavior.md](../spec/behavior.md#arguments).

## Networks without UDP: QMux over WebSocket

WebTransport and native QUIC both need UDP. On a network that blocks it, and in a browser without a
WebTransport that can negotiate protocols (Safari before WebTransport, Firefox before 155), the relay
is still reachable over TCP 443: both fleets serve MOQT on QMux over a WebSocket there. The optional
[`@mux/relay-websocket`](packages/relay-websocket/README.md) package is that transport. Nothing
changes unless you pass it, as `createTransport`:

```ts
import { connectRelayPublisher, connectRelaySubscriber } from '@mux/relay';
import { createWebSocketTransport, withWebSocketFallback } from '@mux/relay-websocket';

// WebTransport first; WebSocket when its handshake fails or takes longer than 4 s.
const subscriber = await connectRelaySubscriber({ url, getToken, createTransport: withWebSocketFallback() });

// Or always WebSocket, in a browser or in Node.
const publisher = await connectRelayPublisher({ url, getToken, namespace, createTransport: createWebSocketTransport() });
```

The endpoint URL stays the `https://` one; the adapter dials `wss://` to the same host and port, with
the path and token in the upgrade request. `withWebSocketFallback({ handshakeTimeoutMs, onFallback })`
sets how long the WebTransport handshake may take (default 4000 ms, about when Chromium gives up on a
silent one) and tells you when a connection fell back. Where the runtime has no usable WebTransport it
dials WebSocket at once. It runs the two dials one after the other and decides afresh for every
connection, a supervisor's reconnects included, so on a network that silently drops UDP every connect
waits out the timeout first; where UDP is refused, the fallback costs a few milliseconds. Keep
`connectTimeoutMs` well above the handshake timeout.

What changes over WebSocket:

- **Head-of-line blocking.** Every stream shares one TCP connection, so a lost packet holds up every
  track behind it until TCP retransmits it. Groups on separate streams no longer escape each other's
  loss.
- **No datagrams.** Datagram reception is not implemented on any transport yet; over WebSocket there
  is none to add.
- **A refused token is a `TRANSPORT_ERROR`.** The relay checks the token before it accepts the
  WebSocket and answers a refusal with HTTP 401, which no WebSocket API exposes. A bad token and an
  unreachable relay both fail with `TRANSPORT_ERROR` in phase `connecting`. A token that expires on a
  live connection is still `UNAUTHORIZED`.
- **Crash detection.** A publisher whose process dies is noticed at once (the TCP connection closes);
  one that hangs without closing is noticed only by the relay's keep-alive, after about 30 s, where
  QUIC notices either after its idle timeout.
- **Settling groups.** A group's close resolves once its FIN is handed to the socket, not once the
  relay has it, so `publisher.close()` cannot prove the last groups arrived.

Measurements and the relay's side are in
[QMux over WebSocket](../spec/relay-interop.md#qmux-over-websocket); the adapter's own deviations from
the transport contract are in [transport adapter](../spec/transport-adapter.md#known-deviations).

## Consume asynchronously

`subscribeEvents` creates a subscription owned by a single async iterator. Await work directly in
the loop; the existing `subscribe` API remains available for synchronous callbacks.

```ts
const events = subscriber.subscribeEvents(
  { namespace: ['sensors', 'lab'], track: 'readings' },
  { capacity: 32, signal: consumerLifetime.signal }, // a separate AbortController for this consumer
);
try {
  for await (const event of events) {
    if (event.type === 'object') await processReading(event.value.payload);
    if (event.type === 'subgroupReset') console.warn('Objects may be missing', event.value);
    if (event.type === 'publisherDone') console.log('Publisher finished; trailing objects may arrive', event.value);
    // break is safe: for-await cancels this subscription. Consumer exceptions propagate too.
  }
} finally {
  events.close(); // also safe before iteration or while the loop body is awaiting other work
}
```

The default capacity is **64 events**, including lifecycle events; it is a count limit, not a byte
budget. Objects may be up to 16 MiB plus properties. Overflow discards queued events, cancels the
subscription and throws `SubscriptionOverflowError`. Request failures throw
`SubscriptionRequestError`; connection closure throws `SubscriptionConnectionClosedError`, even
for a clean connection close, because closure does not prove all objects arrived, and before
acceptance as well as after it. Errors propagate out of the loop for the caller to handle. `publisherDone` is an event, never EOF. Arrival order is preserved without ordering across
streams or transport-wide backpressure. Early exit releases the owned subscription and observers
without closing `subscriber` or other consumers. Only one pending `next()` is supported; a concurrent one rejects with a plain `Error`. `Promise.race`
with a timeout leaves the losing `next()` pending: use the iterator's abort signal to end
consumption, or retain and await that same read before starting another. Python's
`asyncio.timeout`/`wait_for` instead cancels the read and its subscription when the deadline
expires. See the [complete event and ownership
contract](../spec/behavior.md#async-subscription-consumption).

Passing an `async` subscription callback throws `TypeError` before sending. A synchronous callback
returning a promise is diagnosed once per handler per subscription via `console.error`; every
rejection is observed but its work cannot be cancelled. Callbacks are never awaited. Use
`subscribeEvents` for async consumers. Handlers are snapshotted at subscription creation; later
assignments do not change that subscription.

## Discover namespaces

`namespaceEvents` tells a subscriber which namespaces under a prefix are announced, so a call room,
a set of cameras or a channel list can keep its roster without a server of its own. It is the same
bounded iterator as `subscribeEvents`, with the same capacity, signal, overflow and error classes,
and it owns one request, never the connection.

```ts
const roster = new Map<string, readonly string[]>();
for await (const event of subscriber.namespaceEvents({ prefix: ['rooms', 'lobby'], signal })) {
  const key = JSON.stringify(event.namespace); // e.g. ['rooms', 'lobby', 'alice']
  if (event.type === 'announced') roster.set(key, event.namespace);
  else roster.delete(key);
}
```

Namespaces already announced arrive first, about one round trip after the call, with no marker
between them and later changes. A publisher's clean `close()` is reported `unannounced` within a few
hundred milliseconds. A crashed publisher is reported only at the relay's QUIC idle timeout, about
12 s later. An announce on another relay of the cluster looks the same as a local one. A prefix
matches whole fields, and a prefix the token does not cover is silent rather than rejected. One
connection discovers each prefix tree once: a prefix that equals, contains or falls under one
already being discovered throws `PREFIX_OVERLAP` until that iterator ends. If the relay ends the
request, the iterator first reports every remaining namespace `unannounced`, then finishes.
`announced` means the relay routes the namespace, not that its tracks have content.

Subscribing on the same connection once `announced` arrives avoids the announce race, so it needs
no timed retry: on moq-relay 0.17.0 such a subscribe was never rejected `unroutable` (0 of 160 trials
on 2026-10-06), while one sent as soon as the publisher's `announced` settled was rejected 6 to 14
times in 40 on the publisher's relay and every time on another relay
([measurements](../spec/relay-interop.md#announce-propagation)). A later `unroutable` means the
namespace has gone since; wait for its next `announced` rather than retrying on a timer. See the
[discovery contract](../spec/behavior.md#discovery).

## Stay connected: supervised connections

`connectRelayPublisher` and `connectRelaySubscriber` never reconnect or resubscribe. A live
application that has to survive network loss can use the supervisors instead. A browser's QUIC
connection ends after about 5 s without connectivity ([browser transport limits](#browser-transport-limits)).
The supervisors take the same connection options and resolve once their first connection is up.

```js
import { connectSupervisedPublisher, connectSupervisedSubscriber } from '@mux/relay';

const publisher = await connectSupervisedPublisher({
  url,
  namespace: ['room', participantId], // the root: each connection serves root + one generation field
  getToken,
  onStatus: (status, error) => console.log('publisher', status, error),
});
const video = publisher.registerTrack({ track: 'video' }); // the same writer across reconnects

const subscriber = await connectSupervisedSubscriber({ url, getToken });
subscriber.want(
  'alice/video',
  { namespace: ['room', 'alice'], track: 'video', generations: true, staleAfterMs: 3000 },
  {
    onObject: (object) => decoder.push(object),
    onRestarted: () => decoder.reset(), // the next object starts a new subscription; the gap is not filled
    onSwitched: () => decoder.reset(), // a newer generation took over with no gap; see below
    onError: (error) => console.error('gave up on alice/video', error),
  },
);
```

The supervised publisher reconnects with backoff (from 500 ms up to 10 s) whenever its connection
ends. Each connection is a new **generation** with its own namespace, the root plus a field that
sorts by start time. Each one gets a fresh namespace because moq-relay 0.17.0 never hands existing
subscriptions to a reconnected publisher, and can keep serving a dead one's cached objects
([publisher recovery](../spec/publisher-recovery.md)). Every registered track is registered again on
each generation. Writes are never replayed: while no generation is up, a write fails at once with
`RECONNECTING`, and an operation the connection's end interrupted fails the same way, so write the
next object, or the next keyframe, when it is live again.

`want(key, ...)` keeps one track subscribed until `unwant(key)`. It retries the announce race,
resubscribes after `PUBLISH_DONE` or a rejection a wait may fix (from 250 ms up to 8 s), and moves
to the replacement connection. With `staleAfterMs`, it also resubscribes a track that went silent.
Set that only for tracks that are never silent while live. With `generations: true`, it discovers
the publisher's root and stays on the newest generation, moving as soon as a newer one is
announced, with no application channel. Rejections no wait fixes, such as credentials or a
malformed request, reach `onError`, and the want stops. See the
[supervised contract](../spec/behavior.md#supervised-connections).

A move to a newer generation is made before the break, as `switchTo` makes one. The old
subscription keeps delivering until the new generation delivers, and `onSwitched` runs at the
cutover instead of `onRestarted`. Wanting the key again with another track or parameters moves the
same way, so a supervised viewer changes layers without a gap:

```js
// Cut over once the new layer reaches what was already shown, and let the old one finish in-flight groups.
const caughtUp = (object) => timestampOf(object) >= lastShownTimestamp;
subscriber.want('alice/video', { namespace, track: 'video-hi', generations: true, until: caughtUp, drainMs: 1000 }, handlers);
```

`until` and `drainMs` mean what they mean on [`switchTo`](#switching-tracks), and apply to every move
of the want. Compare your own timeline, never group IDs, since each generation and track numbers
its own groups. Wanting the key again with only `until`, `drainMs`, `staleAfterMs`, `retry` or the
handlers changed swaps them without resubscribing. Another namespace, or turning `generations` on or
off, replaces the want.

Both supervisors **rotate** before the token expires ([token expiry](#token-expiry)), so a long
session never hits the relay's deadline. `rotateBeforeMs` (default 60 s) before `expiresAt`, they
call `getToken` again and open the replacement while the old connection still serves. The
publisher's next generation is announced, each track moves to it once viewers subscribe there, and
the old generation closes. Each want moves its subscription to the new connection as a move does;
when its target is unchanged, the cutover is invisible: no callback runs and nothing is lost or
repeated. Call `rotate()` to do it now, for example after revoking a token. In a local test with
12 s tokens, 989 objects written every 40 ms over 9 rotations all arrived, with no gap longer than
42 ms. See [rotation](../spec/behavior.md#rotation).

## Publishing lifecycle

A dedicated publisher connection serves one namespace with multiple tracks. It uses a separate
transport from subscriber connections. No camera, microphone, catalog, or timestamps are required.

Publishing follows Relay's announce-and-serve ingest flow. `registerTrack()` registers the track
and sends nothing; `write()` waits for the relay to subscribe upstream (demand) before sending
bytes. Start a subscriber to create that demand. The default 10-second write deadline covers both demand and transport writes.
A failed or cancelled write resets its stream and leaves the track open while the publisher
connection remains open. The next explicit write uses a new group ID. After a timeout or relay
subscription change, choose fresh data or the next random-access point; already delivered bytes
cannot be withdrawn. If demand disappeared, the next write waits for a new relay subscription.

Connecting announces the namespace once the first track is registered, and `publisher.announced`
settles when that announce has reached the wire: the relay acknowledges nothing more, and it routes
subscribes to the namespace about a round trip later. A subscribe that arrives before then is
rejected with request error 16 (`DOES_NOT_EXIST`), reason `unroutable` (moq-relay 0.15.8 and later;
relays through 0.15.1 answered `404 Broadcast not found`). A subscriber on another relay of the
cluster waits longer still, until its relay learns the route. `subscribe()` does not retry, so a
subscriber either waits for [discovery](#discover-namespaces) to report the namespace `announced`,
which avoids the race, or retries that rejection within its own budget, or uses a supervised
subscriber's `want`, which retries it for you (the demo and the live smoke test do). A page that
owns both sides awaits `announced` before subscribing, which narrows the race but does not close
it. `announced` stays pending while no solicitation covers the namespace, and the
connection's closure rejects it if that comes first; it is pre-handled, so nobody has to await it.

Each write copies its bytes and sends one independent group containing one object. The payload is a
`Uint8Array`; any other `ArrayBufferView` is accepted and copied as the raw bytes it spans, while a
string, an `ArrayBuffer`, or an array is a `TypeError` raised before the write starts, so the track
stays usable. Await each write; a concurrent call rejects rather than growing a queue. Completion
means the transport accepted the complete group, not that a viewer acknowledged it. There is no publisher-side replay
or retention after an object is sent. Late-subscriber behavior also depends on relay caching and
subscription filters. A failed write may have delivered partial data; there is no automatic retry.

Optional `trackProperties` on `registerTrack()` and `properties` on `write()` carry explicit protocol
metadata. Property types and even-type integer values must be non-negative integers up to
`Number.MAX_SAFE_INTEGER`, the largest value the core's varint carries; odd-type values must be
`ArrayBufferView`s and are copied as their raw bytes. Invalid properties raise a `TypeError` naming
the property index before registering a track or starting a write. A `RangeError` at the same
point covers: a byte value longer than 65535 bytes; `trackProperties` that encode to more than 65525
bytes (what `SUBSCRIBE_OK` has room for), hold more than 64 pairs (the relay's limit), or carry a
`0x0E` above 255 or a `0x22` other than 1 or 2 (values draft-22 rules out), any of which would leave
the track unsubscribable; and object `properties` on `write()` that encode to more than 64 KiB,
which the relay drops silently. The SDK measures the encoding with the core's own encoder, so an
oversized property never opens a stream. The SDK adds
no default timescale, timestamp, or LOC media packaging. Objects are limited to 16 MiB of payload.

Properties are encoded as supplied, but subscribers see the relay's output. moq-relay 0.17.5 and
earlier strip object properties (fixed upstream in moq #5494, not yet released;
[#262](https://github.com/muxinc/relay-sdks/issues/262)): only the object Timestamp (`0x10`, or the
older `0x06`) survives, rescaled to microseconds by the declared Timescale (`0x08`); every other
object property, including Video Config, is dropped. Tracks
without a declared Timescale, and objects without a Timestamp, get relay-clock timestamps. Declaring
`trackProperties: [{ type: 0x08, value: 1_000_000 }]` keeps supplied Timestamp values unchanged.
Every `SUBSCRIBE_OK` carries the relay's track properties (`0x08` = 1,000,000, `0x0E` = 128 unless
the publisher set it, `0x22` = 2), not the publisher's; the relay would refuse every subscriber to
a track whose `0x0E` or `0x22` is out of range or that has more than 64 track properties, which is
why `registerTrack()` refuses those ([#252](https://github.com/muxinc/relay-sdks/issues/252)). A
subscription with `parameters: { includeProperties: 0 }`
gets no track properties and objects with no properties at all, not even a Timestamp.
Keep application metadata that must arrive intact in payloads, and read the received track timescale
when interpreting timestamps. See [Relay property
handling](../spec/behavior.md#relay-property-handling) for the observations and media guidance.

`events.close()` cancels an outstanding write and ends that track; other tracks keep running.
`publisher.close()` ends all tracks, retracts the namespace, and waits for transport shutdown.
Track names cannot be reused within a publisher connection, including after their writer closes.

### Text and JSON

Track and group writers provide `writeText(text, options?)` and `writeJSON(value, options?)`.
They encode one complete object as UTF-8 and accept the same options as `write()`, including
properties and the track write's abort signal. Group helpers also accept object status.
Await each call. The existing demand, deadline, cancellation, and encoded byte-size limits apply;
encoding or serialization errors leave the writer usable.

Use the exported readers explicitly in an object callback:

```ts
import { type MoqtObject, readJSON, readText } from '@mux/relay';

// For a text track:
const onTextObject = (object: MoqtObject) => console.log(readText(object));
// For a JSON track:
const onJSONObject = (object: MoqtObject) => console.log(readJSON(object));
```

`readText(object)` returns a string, preserving a leading U+FEFF, and throws on malformed UTF-8.
`readJSON(object)` returns `unknown` and throws on invalid UTF-8 or JSON; validate its shape in
application code. Catch decoding errors in the callback if malformed objects should be skipped.
Empty text is valid; an empty JSON payload is not. Neither reader changes the object or its metadata.

JSON writing uses `JSON.stringify`, including its usual omission and numeric conversion rules.
A top-level value that produces no JSON (`undefined`, a function, or a symbol) is a `TypeError`,
as are unsupported values such as cyclic objects or bigint. Reading uses `JSON.parse`; no schema
validation or content-type properties are added. Text writing uses `TextEncoder` (unpaired UTF-16
surrogates become replacement characters). Raw byte `write()` and subscriptions remain available.

### Multi-object groups and relay demand

For media, use `openGroup()` to put multiple objects on one stream. Each object gets the next ID,
starting at 0; `group.close()` sends the group's FIN. This amortizes stream creation and completion
across a group, such as a video GOP, instead of waiting for a FIN for every frame. Pass the first
object to `openGroup({ payload, properties })` to send it as object 0: its delivery timeout then
counts the wait for the stream, which a bare `openGroup()` has no object to charge.

```ts
const video = publisher.registerTrack({ track: 'video', writeTimeoutMs: 10_000 });
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
application's responsibility at this level; the frame writer below handles both for encoded media.

`track.hasDemand` reports whether the relay currently has an upstream subscription for the track,
so a write would proceed at once. `track.closed` distinguishes a retired track from a track waiting
for demand. `track.demandChanges()` yields the current demand state first, then one value per relay
subscription change: `true` when the relay subscribes or replaces its subscription (so `true` can
repeat), `false` when its subscription ends; it ends when the track ends. For example, a catalog
can be re-sent on each relay subscription:

```ts
for await (const demanded of catalog.demandChanges()) {
  if (demanded) await catalog.write(catalogBytes);
}
```

Breaking the loop releases its observer. Explicit iterator consumers can call `return()` to release
an observer, including a pending `next()`.

Demand is the relay's own subscription, not a viewer count and not a liveness signal. The relay
fans one upstream subscription out to every downstream viewer, so a second viewer produces no new
`true` and is served from the relay's cache. moq-relay 0.15 and later cancel the subscription about one
round trip after the last viewer leaves, so the next viewer brings a new `true` and a re-send, and a
viewer that resubscribes on the same connection within about 30 seconds can receive the relay's
cached copy before the re-sent one. `true` does not prove anyone is watching. Write only state that
is safe to serve from cache and to receive more than once. Pausing an encoder while there is no
demand is safe, because a frame written then reaches no viewer; resume at a keyframe, which a frame
writer asks for through `keyframeNeeded`. When the first viewer is on another relay, or is a
moq-lite viewer (such as `@moq/watch`) on any relay, the publisher's relay subscribes once
to look up the track's info, cancels about 50 ms later, and subscribes again: `true`, `false`, `true`. Objects
written during that first subscription reach no viewer, so do not rely on the first objects after
demand begins. See [relay demand](../spec/relay-interop.md#relay-demand) and
[a viewer on another relay](../spec/relay-interop.md#a-viewer-on-another-relay).

Both loops exist ready-made in `@mux/relay/media`, the media publishing policy the
[WebCodecs example](examples/webcodecs/README.md) runs on, and Python has as `mux_relay.media`.
`createCatalogService(track, document)` re-sends a document on every relay subscription and retries
a failed send while demand remains; `createDocumentService(track)` does the same for a document you
replace with `update(document)`, such as a media timeline, and also sends each update while the
relay is subscribed. `createFrameWriter(track)` maps encoded frames onto groups (a keyframe opens a
group and deltas append, or `grouping: 'frame'` for one group per frame), skips frames the relay has
no subscription for instead of waiting on the deadline, and resumes at the next keyframe after any
failure. Its `write(frame)` resolves to what became of the frame (`'sent'`, `'skipped'`, `'stale'`,
`'dropped'`, `'interrupted'` or `'ended'`) rather than rejecting, and may be called from a WebCodecs
output callback without awaiting: frames are served in call order, and when more than `maxBacklog`
wait behind the one being written the writer drops rather than queues. `writer.keyframeNeeded` and
`onKeyframeNeeded` say when the next frame must be a keyframe, after a demand change, a failure, a
stale frame or a drop, so an encoder can force one. `@mux/moq-media`'s `LocFrame` is a frame as it
stands.

```ts
import { createFrameWriter } from '@mux/relay/media';

let forceKeyframe = false;
const writer = createFrameWriter(video, { onKeyframeNeeded: () => (forceKeyframe = true) });
// VideoEncoder output callback:
void writer.write(chunkToLocFrame(chunk, { videoConfig })).then((outcome) => {
  if (outcome.kind === 'ended') stop(outcome.error);
});
```

Custom transports with asynchronous sink writes or closes must honor the writable controller's
abort signal to interrupt them and reset their underlying stream, as browser WebTransport does.
A generic `WritableStream` sink that ignores the signal can finish its in-flight close after an abort.

Run the data-only publishing/subscribing integration test without a live relay:

```sh
cd ts && mise exec -- pnpm test tests/publish.test.ts
```

## Publishing media

The optional [@mux/moq-media format package](packages/moq-media/README.md) provides LOC properties,
independent MSF catalogs, source URLs, and native WebCodecs chunk/config mapping. It depends only
on `@mux/moq-core`; install it alongside `@mux/relay` when publishing media. Start with the
[direct WebCodecs example](examples/webcodecs/README.md) for live browser video: encoded chunks
become LOC payloads without a muxer or media engine. For existing media files, the optional
[Mediabunny adapter](packages/moq-mediabunny/README.md) maps demuxed packets; see the
[file publishing example](examples/mediabunny/README.md).

## Subscribing to media

A player reads the same tracks back. Read the catalog first: `parseMsfCatalog` from
[@mux/moq-media](packages/moq-media/README.md) lists the tracks and their decoder configurations.
After a stall, a track's objects can arrive out of order across group streams, and a raw
subscription delivers them as they arrive. `createFrameSequencer({ clock })` from `@mux/moq-media`
turns one track's objects into frames in decode order and drops what is too late against the
broadcast's playout clock. Pass `grouping: 'frame'` for audio, where every group is one
independently decodable frame. `createFrameReader(sequencer, { onFrame })` from `@mux/relay/media`
returns a subscription's handlers that drive the sequencer. It feeds the sequencer each event,
calls `onFrame` synchronously with each ready frame in decode order, and polls again when a wait
the sequencer reports ends. Spread it into `subscribe`, or into a supervised `want`, beside your own
`onError`. It handles a want's `onRestarted` and `onSwitched` too; keep the want's `drainMs` at 0,
the default. `reader.close()` stops delivery; cancel the subscription as well.

```ts
import { createFrameSequencer, createPlayoutClock } from '@mux/moq-media';
import { createFrameReader, mediaSubscribeParameters } from '@mux/relay/media';

const clock = createPlayoutClock(); // one per broadcast, shared by its tracks
const video = createFrameReader(createFrameSequencer({ clock }), {
  onFrame(frame) {
    if (frame.discontinuity) resetVideoDecoder(); // 'gap', 'resync' or 'restart'
    videoDecoder.decode(
      new EncodedVideoChunk({ type: frame.key ? 'key' : 'delta', timestamp: frame.timestampUs, data: frame.payload }),
    );
  },
});
const audio = createFrameReader(createFrameSequencer({ clock, grouping: 'frame' }), { onFrame: decodeAudio });
subscriber.subscribe({ namespace, track: 'video', parameters: mediaSubscribeParameters('video') }, { ...video, onError });
subscriber.subscribe({ namespace, track: 'audio', parameters: mediaSubscribeParameters('audio') }, { ...audio, onError });
```

`mediaSubscribeParameters(kind)` gives each subscription a subscriber priority that puts audio ahead
of video when your downlink is congested: 16 for audio, 96 for video, the values
`mediaTrackProperties` gives the publisher. moq-relay orders what it sends you by your subscriber
priority alone and ignores the publisher's, so without it audio waits behind video's backlog. Choose
it when you subscribe: an `update()` ends the subscription. Pass other parameters as the second
argument, such as `{ locationFilter }`; a `subscriberPriority` there replaces the default.

A frame's `timestampUs` is its LOC timestamp on the broadcast timeline, scaled by whichever
Timescale the object or the track declares. The clock's target latency is the lateness budget for
every track: by default twice the longest of the last four keyframe intervals the sequencer
reports, or 2 s before any. Under keyframe grouping, a frame more than that behind the clock's
position is dropped with its group, and delivery resumes at a later keyframe. Under frame grouping,
a frame later than the clock's `lateDropUs` (40 ms by default) is dropped. The first frame after a
skip, a resync or a restart carries `discontinuity`: reset the decoder there. `sequencer.stats`
counts delivered, late and untimed frames, skipped groups, and resyncs.

The reader paces decoding, not display. Audio is the master: report what is audible with
`clock.reportAudio(timestampUs, atUs)`, and before showing each decoded video frame ask
`clock.due(frame.timestamp)` whether to `wait` (for `waitUs`), `show`, or `drop` it. Until audio is
reported or `due` is first asked, the clock is idle and nothing is late; with no audio it paces by
wall clock. The [@mux/moq-media README](packages/moq-media/README.md) shows how to compute the
audible timestamp. See [media subscribing](../spec/behavior.md#media-subscribing) for the
sequencer's contract.

A broadcast with no video never reports a keyframe interval, so its clock would keep the 2 s
default: a lasting delay increase would then cost 2 s of silence before the reader resyncs. Create
an audio-only clock with `createPlayoutClock({ targetLatencyUs: AUDIO_ONLY_TARGET_LATENCY_US })`
(500 ms).

### Playing audio

`createAudioPlayer({ context })` from `@mux/moq-media` plays decoded audio, with no UI. Each
`player.addInput({ clock, decoderConfig })`, usually one per speaker, decodes the frames it is
pushed with WebCodecs, holds them in an adaptive jitter buffer in an `AudioWorklet`, and reports
what is audible to `clock` about every 20 ms, so the clock follows the device and stays the master
for any video. Pass `input.push` as a frame reader's `onFrame`. Every input goes through its own
`gain` into `player.output`, one node for the whole mix, which you connect:

```ts
import {
  AUDIO_ONLY_TARGET_LATENCY_US,
  audioDecoderConfigFromTrack,
  createAudioPlayer,
  createFrameSequencer,
  createPlayoutClock,
} from '@mux/moq-media';
import { createFrameReader } from '@mux/relay/media';

const context = new AudioContext({ sampleRate: 48_000, latencyHint: 'interactive' }); // in a user gesture
const player = await createAudioPlayer({ context }); // one per context, shared by every speaker
player.output.connect(context.destination);

const clock = createPlayoutClock({ targetLatencyUs: AUDIO_ONLY_TARGET_LATENCY_US }); // one per broadcast
const input = player.addInput({ clock, decoderConfig: audioDecoderConfigFromTrack(audioTrack) });
const reader = createFrameReader(createFrameSequencer({ clock, grouping: 'frame' }), { onFrame: input.push });
subscriber.subscribe({ namespace, track: 'audio' }, { ...reader, onError });
// Later: reader.close(), cancel the subscription, input.remove(); player.close() when the call ends.
```

The buffer starts at 60 ms, grows by 40 ms after each underrun, shrinks by 10 ms after 2 s of
smooth playback (never below 20 ms), and trims audio it held but never needed. Decoded audio reaches the
worklet from the main thread, so a page whose main thread stalls for tens of milliseconds can underrun at that
floor; such a page can pass `minJitterTargetUs: 40_000`, the floor before. It stays under 400 ms
and under the clock's target latency less the output delay. `input.stats` has the underruns, the
buffer depth and target, the frames dropped as late, and what was trimmed, concealed with silence or
cut. The decoder resets at every discontinuity and timestamp jump, and a `resync` or `restart`
flushes the buffer. `input.cut({ fromUs, untilUs })` drops received audio in a timeline range, held
or arriving later, as when a talker is interrupted. A talker that can stop mid-utterance, such as a
voice bot, lists an `interrupt` data track in its catalog and writes an interrupt there when it does;
apply each one to the input that plays its track:

```ts
import { INTERRUPT_TRACK, parseInterrupt } from '@mux/moq-media';

subscriber.subscribe(
  { namespace, track: INTERRUPT_TRACK },
  {
    onObject(object) {
      const interrupt = parseInterrupt(object.payload); // throws on a malformed payload
      if (interrupt.track === 'audio') input.cut(interrupt);
    },
  },
);
```

A talker can also list a `transcript` track with the text of what it says. Keep it in a
`Transcript`, cut it with the same interrupt, and show what playback has reached:

```ts
import { createTranscript, parseTranscriptSegment, TRANSCRIPT_TRACK } from '@mux/moq-media';

const transcript = createTranscript();
subscriber.subscribe(
  { namespace, track: TRANSCRIPT_TRACK },
  { onObject: (object) => transcript.apply(parseTranscriptSegment(object.payload)) },
);
// With each interrupt: transcript.cut(interrupt, clock.position()).
const said = transcript.linesAt(clock.position()).map((line) => line.text);
```

See [spec/media-layer.md](../spec/media-layer.md#audio-player) for the rules and why, and its
[Interrupt track](../spec/media-layer.md#interrupt-track) and
[Transcript track](../spec/media-layer.md#transcript-track) sections for the conventions.

**Echo cancellation.** In a call the page also captures a microphone, and Chromium's echo canceller
only removes audio that WebRTC played: audio played straight from an `AudioContext` reaches the
other side again through the speakers. In Chromium, route the mix through a local WebRTC loopback
instead of connecting it to the destination:

```ts
import { createEchoCancellationLoopback } from '@mux/moq-media';

const loopback = await createEchoCancellationLoopback(context, player.output); // rejects if it cannot connect
// loopback.setSinkId(deviceId) picks the output device; loopback.close() stops it.
```

Firefox and Safari cancel echo from all page output, so play directly there. The loopback adds the
WebRTC receiver's buffering, which no API reports; set `player.outputDelayUs` if video must stay in
sync with the audio. The [WebCodecs example](examples/webcodecs/README.md)'s `listen-audio.ts`
plays its microphone track this way, and the [voice bot page](examples/voice-bot/README.md) plays a
Python bot through it while publishing the microphone.

### Voice: delivery per direction

Voice wants different things in each direction, and each track's sequencer takes its own policy
without touching the clock its other tracks share. moq-relay 0.17.5 (measured) and 0.17.7 (read in
source) deliver every group however late, whatever delivery timeout a subscriber sends, and answer
FETCH without timestamps
([relay-interop.md](../spec/relay-interop.md#voice-delivery-on-0175-deadlines-fetch-and-group-order)),
so on those releases both policies live in the subscriber. The options are meant to work with and
without a relay that honours the subscriber's delivery timeout
([moq-dev/moq#5881](https://github.com/moq-dev/moq/issues/5881)): there the relay drops what is
late, and the subscriber's options handle whatever still arrives late or out of order.

**To a listener** (an agent's or a caller's voice to a person): low, steady latency, where a gap
beats a stall. Drop audio late against the playout target, and catch up when it keeps arriving
late: `catchUpAfterUs` makes the sequencer ask for it, and a supervised subscriber's `catchUp(key)`
replaces the subscription at the live edge, which drops the relay's backlog. The want's
`onRestarted` (reason `'catch-up'`) restarts the reader. Keep `catchUpAfterUs` below the clock's
target latency, or a resync re-anchors on the late audio first. The subscriber's own delivery
timeout goes on the SUBSCRIBE as well, for a relay that honours it; moq-relay 0.17.5 and 0.17.7
ignore it.

```ts
const voice = createFrameReader(
  createFrameSequencer({ clock, grouping: 'frame', lateDropUs: 60_000, catchUpAfterUs: 300_000 }),
  { onFrame: input.push, onCatchUp: () => subscriber.catchUp('voice') },
);
subscriber.want(
  'voice',
  { namespace, track: 'audio', parameters: { objectDeliveryTimeout: 150, subgroupDeliveryTimeout: 150 } },
  { ...voice, onError },
);
```

**To an agent** (a person's voice to speech-to-text): every word, soon after the person stops.
Completeness comes from the person's publisher and the reorder window, not from FETCH. Give the
person's audio track a longer delivery timeout
(`mediaTrackProperties('audio', { subgroupTimeoutUs: 2_000_000 })`, 1 to 2 s instead of audio's
500 ms), so the publisher's transport keeps resending through loss. On the agent, drop nothing as late, and wait for a missing group at least
as long as that timeout, since a resent group can arrive that long after the one that follows it.
A frame after a group that never came carries `missingGroups`: the group IDs to act on.

```ts
const words = createFrameReader(
  createFrameSequencer({ clock, grouping: 'frame', lateDropUs: Number.POSITIVE_INFINITY, reorderWindowUs: 2_000_000 }),
  {
    onFrame(frame) {
      if (frame.missingGroups) noteGap(frame.missingGroups.firstGroupId, frame.missingGroups.lastGroupId);
      transcriber.push(frame);
    },
  },
);
```

## Errors and recovery

Branch on `code` and `phase`, not message wording. SDK lifecycle errors use these stable codes:

| Code | Application response |
| --- | --- |
| `CONNECTION_TIMEOUT` | Connecting exceeded its deadline; consider a new connection with backoff. |
| `WRITE_TIMEOUT` | Inspect the phase below, then choose the next object or group explicitly. |
| `DELIVERY_TIMEOUT` | An object outlived its delivery timeout before it reached the transport. The data is stale: do not retry it as it is; choose fresh data or the next random-access point. See [delivery timeouts](../spec/behavior.md#delivery-timeouts). |
| `CLOSED` | The handle is closed; use an open connection, track, or group. |
| `WRITE_PENDING` | Await the outstanding operation or finish/abort the open group before starting another. |
| `NAMESPACE_NOT_SOLICITED` | Received namespace solicitations do not cover this publisher; check the namespace and token configuration. A later matching solicitation can restore demand. |
| `NAMESPACE_ENDED` | The relay ended the namespace announcement; open a replacement publisher if appropriate. |
| `SUBSCRIPTION_REQUEST_FAILED` | An async subscription request failed; inspect its preserved MOQT request error and the connection state. |
| `SUBSCRIPTION_OVERFLOW` | The event buffer filled; the subscription was cancelled. Choose a suitable capacity or faster consumption before subscribing again. |
| `SUBSCRIPTION_CHANGED` | Upstream demand ended or changed during an operation; choose fresh data or a new random-access point. |
| `GOAWAY` | The relay requested migration; open a replacement connection. |
| `TRANSPORT_ERROR` | Inspect the original cause and connection state before deciding whether to reconnect or publish again. |
| `TRANSPORT_UNAVAILABLE` | Supply a transport implementation in a runtime without browser WebTransport; in Node, import from `@mux/relay/node` with `@mux/relay-quic` installed (TypeScript). |
| `UNAUTHORIZED` | The relay refused the connection's credentials: no token, a bad, malformed or expired one, or an endpoint path the token does not grant. Supply a valid token before connecting again. In phase `connection`, the credential of a live session expired or its grant was revoked ([token expiry](#token-expiry)). Reported in browsers and over native QUIC (`@mux/relay/node`); against relays before moq-relay 0.17.0 a browser does not see the refusal, so there it arrives as `TRANSPORT_ERROR` (#14). |

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
acknowledgment. A plain connection does not automatically retry or reconnect; the
[supervisors](#stay-connected-supervised-connections) reconnect, but never replay a write. `UNAUTHORIZED` is the one code that
says the relay refused the credentials: the relay closed the session with MOQT session termination
code `UNAUTHORIZED` (0x2; `MALFORMED_AUTH_TOKEN` 0x16 and `EXPIRED_AUTH_TOKEN` 0x18 map to it too).
In phase `connecting` the relay refused to connect; in phase `connection` it ended a live session
whose credential expired or whose grant was revoked ([token expiry](#token-expiry)). TypeScript reports it in
browsers and over native QUIC; against relays older than moq-relay 0.17.0 a browser does not see the
relay's close, so a refusal there arrives as `TRANSPORT_ERROR` (#14). Neither another connection failure nor an uncovered
namespace proves authentication was denied. A subscription outside the token's grant is refused
per request, through `onError` / `on_error` (MOQT `UNAUTHORIZED`, 0x1), and leaves the connection open.

Clean connection shutdown still resolves `closed` without an error; subsequent operations fail
with `CLOSED`. After a failed connection, `closed` and subsequent connection operations expose the
same failure. Caller cancellation reasons and token-provider exceptions retain their identity.
Argument and serialization errors keep their idiomatic language types. Subscription `onError` /
`on_error` callbacks retain the separate MOQT `RequestError` contract and numeric protocol codes.

TypeScript exports `RelayError`, `RelayErrorCode`, and `RelayErrorPhase` from `@mux/relay`.
Wrapped transport/session failures preserve the original error in `cause`.

```ts
import { RelayError } from '@mux/relay';

try {
  await track.writeText('current state');
} catch (error) {
  if (error instanceof RelayError && error.code === 'WRITE_TIMEOUT') {
    console.log('Write deadline expired during', error.phase);
    // Choose what to publish next using phase and application freshness/deduplication policy.
  } else {
    throw error; // Includes the caller's original AbortSignal reason.
  }
}
```

## Package boundaries

- The `ts/` root package, `@mux/relay`, owns the Relay product API and authentication convention.
- `packages/moq-core` is an independently buildable, Apache-2.0 generic protocol package. It uses
  standard streams, has no runtime dependencies, and imports no player, UI, or media engine.
- `packages/relay-quic` and `packages/relay-websocket` are transport adapters the application passes
  as `createTransport`; `@mux/relay` depends on neither. The WebSocket adapter is the one package with
  a third-party runtime dependency, `@moq/qmux` at an exact version
  ([ADR 0005](../docs/adr/0005-websocket-adapter-wraps-moq-qmux.md)).
- `subscriber.trackSource` exposes the generic `MoqTrackSource` contract for future player
  integrations: track requests in protocol terms (with the core's `onOk`/`onDone` handler names) and
  closure observation, with no track listing and no operation to close the shared connection.
- `tests/spec-protocol.test.ts` and `tests/spec-vectors.test.ts` check the core against the shared
  `../spec`, which is how this SDK stays in lockstep with the other languages.

The core starts from an attributed snapshot of the existing Video.js protocol modules and their
regression tests. Video.js is unchanged and still has the original implementation. Migrating it to
this separately versioned dependency is the next integration step; the snapshot is not intended to
become a separately maintained protocol fork. See `packages/moq-core/UPSTREAM.json`.

## Initial scope

This slice supports reliable-stream track subscriptions, subscription updates and cancellation,
[namespace discovery](#discover-namespaces), and publication on a dedicated connection: data, or
encoded media through `@mux/relay/media`. The [supervisors](#stay-connected-supervised-connections)
add reconnection, resubscription and token rotation on a live session. The borrowed generic
interface also exposes FETCH and TRACK_STATUS.
The SDK offers `moqt-22`, then `moqt-20` as a fallback, and speaks whichever the relay selects.

Datagram reception, discovery of tracks within a namespace, and a Video.js adapter are
not implemented yet. On a plain connection, a GOAWAY ends the connection with an error; subscriptions still awaiting their first response end with it (`subscribed` rejects with `SubscriptionConnectionClosedError`, `ended` resolves `terminated`), never through `onError`;
callers can open a replacement, which a supervisor does for them. A custom `createTransport`
factory supports tests or runtimes with a compatible transport; the default uses browser WebTransport.
A factory whose transport carries MOQT directly on QUIC, not a WebTransport session, returns
`framing: 'native-quic'`, and the core then sends the endpoint's path, token and authority in SETUP.
`@mux/relay-websocket`'s factories return `framing: 'websocket'`, which sends neither, as on
WebTransport; see [networks without UDP](#networks-without-udp-qmux-over-websocket).

## Development

Contributor setup, checks, formatting, and the opt-in live Relay smoke test for this workspace are
in the root [README](../README.md#development).
