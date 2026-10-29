# Relay JavaScript SDK

Publish and subscribe to arbitrary track objects through Relay without Video.js, a media catalog,
or a player. `@caprid/relay` runs in a browser page, and its default transport is **browser
WebTransport in a secure context** (HTTPS or localhost). In Node 24 or later, import from
`@caprid/relay/node`, which connects over native QUIC through the optional `@caprid/relay-quic`
package (see the [README's Node section](README.md#node)). Other runtimes must supply a compatible
`createTransport` factory.

This is the TypeScript SDK in the Relay SDKs repository. The shared protocol pins, behavior
contract, and test vectors live in [spec](../spec/README.md), and the [root README](../README.md)
describes the suite. The packages are on npm as `@caprid/*` ([install](README.md#install)).

## First round trip

The first round trip publishes one object and receives it back through Relay, from a new Node
project that installs the packages from npm. You need Node 24 or later. The connections run over
[`@caprid/relay-quic`](packages/relay-quic/README.md)'s native QUIC transport, so your network must
allow QUIC traffic over UDP.

### Endpoint and credentials

Ask the team that operates your Relay deployment, or your application's backend owner, for three
things:

- an HTTPS WebTransport endpoint.
- your token's root, the namespace prefix the token grants.
- a short-lived Relay JWT authorized to **publish and subscribe** there.

The SDK consumes tokens. It does not issue them, and there is no public token signup flow. Do not
use a Mux API access token in place of a Relay JWT.

Use an endpoint whose path is your token's root, for example `https://relay.mux.global/acme/rooms/42`
for a token rooted at `acme/rooms/42`. Then name namespaces relative to it: `['p', 'a']`, not
`['acme', 'rooms', '42', 'p', 'a']`. The connection cannot reach outside that root, and a path the
token does not grant fails at connect with `UNAUTHORIZED`. A client that spans several rooms under
one grant can use the bare host, where namespace fields include the token's root
([namespaces](../spec/behavior.md#subscribing)).

The Mux relays carry announcements between regions, so a publisher and a subscriber can each use
their nearest relay ([a viewer on another relay](../spec/relay-interop.md#a-viewer-on-another-relay)).
The SDK's default endpoint is the bare host `https://relay.mux.global`, which still requires
authorized credentials. For another deployment, use the endpoint its operator supplies.

`roomEndpoint` builds that endpoint from the token's root, so you do not assemble the URL by hand.
Given only the root, it uses `DEFAULT_ENDPOINT`, `https://relay.mux.global`. Pass an endpoint
first for another relay:

```ts
import { roomEndpoint } from '@caprid/relay';

const url = roomEndpoint(['acme', 'rooms', '42']);
// 'https://relay.mux.global/acme/rooms/42'
const nearby = roomEndpoint('https://ord.relay.mux.global', ['acme', 'rooms', '42']);
// 'https://ord.relay.mux.global/acme/rooms/42'
```

It appends each field to the endpoint's path and keeps the endpoint's query. It encodes nothing,
because the relay reads the path without decoding it. A field may hold only letters, digits, and
`-._~!$&'()+,;=:@`, and may not be `.` or `..`. Any other field throws a `RangeError`. Reach a room
with such a field from the bare host
([room-rooted endpoints](../spec/behavior.md#room-rooted-endpoints)).

The round trip below reads the token from `RELAY_TOKEN`. Set it with the hidden prompt shown there
for manual runs. Automation can inject it through its secret manager. Do not put tokens in commands,
source files, URLs, or committed `.env` files.

### Browser token exposure

Relay authenticates with `?jwt=<token>` on the WebTransport URL. On a failed handshake, the browser
can log that full URL, token included, to its own console. The SDK cannot suppress those
browser-generated messages. They can expose a live token to console collectors and error reporting
services, even when the error that `connectRelaySubscriber` returns contains no token.

Before you forward browser diagnostics, remove `jwt` query values and token strings from console
breadcrumbs, network URLs, traces, and error reports, or turn off collection of those fields. Use
short-lived tokens scoped to the namespaces and operations the application needs. The repository's
CLI tools redact the supplied token from their own output, but that does not clean the browser's
console or third-party collectors. Moving authentication off the URL needs a relay protocol change,
and this SDK does not support it.

### Install and run

Create a project and install the SDK with its Node transport:

```sh
mkdir relay-hello && cd relay-hello
npm init -y
npm install @caprid/relay @caprid/relay-quic
```

`@caprid/relay-quic` brings a prebuilt native binary for your platform, so the install needs no
compiler ([Node](README.md#node)). Save this as `hello.mjs`, with your token's root in place of
`acme/rooms/42`:

```js
import { connectRelayPublisher, connectSupervisedSubscriber, readText, roomEndpoint } from '@caprid/relay/node';

const url = roomEndpoint(['acme', 'rooms', '42']); // your token's root, on https://relay.mux.global
// const url = process.env.RELAY_URL; // or any endpoint whose path is your token's root
const getToken = () => process.env.RELAY_TOKEN;
const namespace = ['hello', crypto.randomUUID().slice(0, 8)]; // fresh for each run, relative to the root

// Subscribe first: the publisher's write() waits until someone wants the track.
const subscriber = await connectSupervisedSubscriber({ url, getToken });
const received = new Promise((resolve, reject) => {
  subscriber.want('greeting', { namespace, track: 'greetings' }, {
    onObject: (object) => resolve(readText(object)),
    onError: reject,
  });
});

const publisher = await connectRelayPublisher({ url, namespace, getToken });
const track = publisher.registerTrack({ track: 'greetings' });
await track.writeText('hello, relay');

console.log('received:', await received);
subscriber.close();
await publisher.close();
```

Enter the token at the hidden prompt, then run the script:

```sh
printf 'Relay token: ' && read -rs RELAY_TOKEN && echo && export RELAY_TOKEN
node hello.mjs
```

It prints `received: hello, relay`. That line is what proves the round trip. A completed
`writeText()` only means the transport accepted the object.

`roomEndpoint` puts the token's root in the endpoint's path, so the namespace is relative to it. The
publisher announces that namespace, and its `writeText()` waits for relay demand before it sends
anything. The subscriber's `want` creates that demand. A fresh namespace can briefly reject a
subscription with request error 16 `unroutable` while its announcement propagates, for about a round
trip, and the want retries it until the subscription succeeds. A rejection that no wait fixes, such
as an unauthorized track, reaches `onError`. Both connections keep Node running until they close.

This script has no run deadline and no cleanup on failure, and a write that times out waiting for
demand fails the run. [The full demo](#run-the-full-demo-from-a-checkout) adds those, and can run
the publisher and subscriber in separate processes.

## Run the full demo from a checkout

The repository's mux-global demo is the same round trip, built to show what a longer-lived program
handles. It has a run deadline, retries only writes that provably sent nothing, prompts for the
token and redacts it from its output, and can run either side alone. You need Git and
[mise](https://mise.jdx.dev/getting-started.html) on your PATH. Run the commands below in Bash from
the repository root.

```sh
git clone https://github.com/caprids/relay-sdks.git
cd relay-sdks
mise install
mise run ts:install
```

The example task builds `@caprid/relay-quic`'s native addon from source, which needs Rust. The mise
task brings the pinned toolchain. See [the example's setup notes](examples/mux-global/README.md#run-it).

The combined demo starts both sides and logs the publisher's `announced`. Its subscriber is the same
supervised `want`, which retries request error 16 `unroutable` (`404 Broadcast not found` on relays
before 0.15.8) with capped backoff, resubscribes after a publisher restart, and reconnects. Each
write is retried only while it times out waiting for demand (`WRITE_TIMEOUT` in phase
`waiting-for-demand`), which proves it sent nothing. A failure after sending starts stops the run,
so the demo never replays bytes that may already have been delivered.

When prompted below, enter the endpoint, ending in your token's root, and a **fresh namespace below
it**. The CLI splits `quickstart/unique-run` into `['quickstart', 'unique-run']`. The SDK itself sends
fields verbatim. Both connections use those same fields and the track `ticker`. If your token
authorizes only one exact namespace, enter the bare host as the endpoint and that whole namespace (a
namespace needs at least one field). In that case, avoid overlapping publishers and stale cached
objects.

```sh
read -r -p "Relay HTTPS endpoint with your token's root as its path: " RELAY_URL
read -r -p 'Namespace below that root (slash-separated): ' RELAY_NAMESPACE
mise run ts:example:mux-global -- --url "$RELAY_URL" --namespace "$RELAY_NAMESPACE" --track ticker --count 1 --timeout 30
# Enter the Relay token at the hidden prompt.
```

The task builds the local packages and the native addon, then runs the demo in Node. The complete SDK
example, including imports and cleanup, is [demo.js](examples/mux-global/demo.js).
[run.mjs](examples/mux-global/run.mjs) supplies the explicit configuration and token. Edit the demo
to adapt the payload for your application.

The output includes these lines. Byte counts, timings, and ordering can vary.

```text
starting             mode=both namespace=... track=ticker count=1
publisher connected  ...
subscriber connected ...
subscribed           ...
published            seq=1 bytes=...
received             group=0 bytes=... seq=1 ageMs=...
finished             published=1 received=1
```

`finished published=1 received=1` means success. The 30-second run deadline bounds connection
setup, the wait for demand, retries, and reception. An error prints a redacted `failed` line and
exits nonzero. A `finally` block closes the connections on success or failure. A completed write
only means the transport accepted it. The `received` line is what proves this demo's round trip.

### Subscribe and publish separately

Use two terminals in the same checkout. In **each** terminal, set `RELAY_URL` and `RELAY_NAMESPACE`
with the prompts above, to exactly the same values. Each process prompts for its own token. The
subscriber needs subscribe permission and the publisher needs publish permission. Start them within
60 seconds of each other, in either order. A subscriber started first retries fresh-namespace
rejections. A publisher started alone waits for demand and then fails at the overall deadline.

Subscribe to one object:

```sh
mise run ts:example:mux-global -- --mode subscribe --url "$RELAY_URL" --namespace "$RELAY_NAMESPACE" --track ticker --count 1 --timeout 60
```

Publish one object from the other terminal:

```sh
mise run ts:example:mux-global -- --mode publish --url "$RELAY_URL" --namespace "$RELAY_NAMESPACE" --track ticker --count 1 --timeout 60
```

Success is `finished published=0 received=1` in the subscriber and `finished published=1 received=0`
in the publisher. Both modes handle errors and clean up as the combined run does. See
[demo options](examples/mux-global/README.md) for multiple objects and intervals.

### Use the local packages in your application

To try unreleased changes from a checkout in an existing browser app, serve `ts/dist/` and
`ts/packages/moq-core/dist/` as JavaScript modules and map **both** bare imports:

```html
<script type="importmap">
{"imports":{"@caprid/relay":"/sdk/index.js","@caprid/moq-core":"/core/index.js"}}
</script>
```

Here `/sdk/` maps to `ts/dist/` and `/core/` to `ts/packages/moq-core/dist/`. Keep each directory's
module layout. Import `@caprid/relay` from a module script in your secure browser page. Rebuild with
`mise run ts:build` after you edit sources. If you copy only the Relay build, or install its
directory on its own, its `workspace:*` dependency on the core does not resolve.

## If the run fails

- For a connection or handshake failure, check the endpoint, the Relay JWT's expiry and permissions,
  TLS trust, and whether QUIC over UDP can reach the relay. The CLI redacts tokens. Keep them out of
  diagnostic reports.
- For repeated request error 16 `unroutable` (or `404 Broadcast not found` on older relays), check
  that the publisher is still running and that both sides use exactly the same authorized namespace.
  Error 16 `not found` means the namespace is announced but the track is not, so check the track
  name. The demo's backoff stops at the run deadline.
- For `waiting ... no relay demand yet` until the deadline, start the matching subscriber. Then check
  the token's namespace prefix and its publish and subscribe permissions.
- After a write failure that follows demand, delivery may be partial. The demo stops without
  resending. Before you try again, decide whether to send fresh data or apply an explicit
  application replay policy.

## Subscription lifecycle

A subscriber connection can only subscribe. Publishing uses a separate publisher connection.
[SDK vocabulary](../spec/vocabulary.md) records the names both SDKs use for each concept, and why.

`subscribe` returns a `RelaySubscription` at once. Await `subscription.subscribed` for the relay's
answer. It resolves with the acceptance info, which `onSubscribed` also receives. It rejects with:

- `SubscriptionRequestError` on a rejection or a request timeout.
- `SubscriptionConnectionClosedError` if the connection ends first.
- your own reason if you cancelled.

Await `subscription.ended` to learn that the subscription can no longer deliver, and why. It resolves
once and never rejects. Its value holds `kind` (`rejected`, `terminated`, or `cancelled`), the
protocol `cause`, whether the relay had accepted the subscription, and the error.

```ts
const subscription = subscriber.subscribe({ namespace, track }, { onObject: handle });
await subscription.subscribed; // throws on rejection, closure, or cancel
subscription.ended.then((end) => console.log(`subscription ${end.kind}`, end.error));
```

Cancelling a subscription stops that consumer, including trailing objects after `onPublisherDone`.
Closing the connection stops all of its subscriptions. `onPublisherDone` settles neither awaitable.
It is the publisher's control notification, trailing objects can still arrive, and the request stays
open until you cancel it or the connection ends. The demo waits for its object count or the
deadline. The [behavior contract](../spec/behavior.md#subscription-lifecycle) has the full state
table.

The SDK sends namespace fields verbatim. You can subscribe to a known track directly. The SDK does
not interpret the payload, and does not assume that a track holds audio, video, or timestamps. An
accepted data subscription has no application inactivity deadline. The request timeout bounds only
its initial response. An idle connection still depends on peer traffic or on keepalive from the
transport, because the SDK schedules no keepalive of its own. You cancel each subscription on its
own, and the application owns connection shutdown.

A subscription without a location filter (no `parameters.locationFilter`, or `{ type: 'none' }`)
starts at the live edge on Relay. A subscriber that joins an ongoing track receives only the objects
published after it joined, although the relay still holds earlier ones. To reach back, pass
`parameters: { locationFilter: { type: 'relative-group', groupsBeforeNext: N } }`. A value of `1`
starts at the current group, and `N` starts N-1 groups before it. Or pass `{ type: 'absolute', start }`
with an explicit start. The relay serves either from its cache, as far back as it still holds. See
[late subscribers](../spec/relay-interop.md#late-subscribers-and-location-filters).

Object callbacks run synchronously as streams deliver objects. The SDK adds no application queue,
ordering across streams, automatic JSON decoding, clock synchronization, or media scheduling. Handle
subgroup resets and connection closure explicitly. This API promises neither exactly-once nor
gap-free delivery. The SDK reports every reset it receives, but the relay may drop a reset for a
subgroup it had not begun forwarding. An aborted group can then leave no trace except a gap in group
IDs ([subgroup resets](../spec/relay-interop.md#subgroup-resets)). `onPublisherDone` may arrive before
trailing objects still in transit, and it does not end the subscription. Cancel the subscription when
you are finished with it.

The SDK sends every subscription parameter you set, but moq-relay acts on only some of them. It
honors `subscriberPriority` (for media, see [`mediaSubscribeParameters`](#subscribing-to-media)) and
`locationFilter` on the subscribe. It ignores `groupOrder` (the relay always serves Descending),
`newGroupRequest`, both delivery timeouts, and a per-request `authorizationTokens`. Because it ignores
`newGroupRequest`, there is no protocol key-frame request through the relay. An application that
needs one sends it to the publisher on a track of its own. See
[subscription parameters](../spec/relay-interop.md#subscription-parameters).

`subscription.update(parameters)` sends REQUEST_UPDATE, the protocol's way to change what an
accepted subscription delivers. **moq-relay does not implement it.** The relay never acknowledges an
update. Any update, including `{ forward: 0 }` or an empty one, ends the subscription within one
round trip with `onPublisherDone` (status 2 `track ended`) and stops delivery, and `onUpdateOk`
never fires. To pause, cancel the subscription and subscribe again later. To change the track, the
priority, or the filter, use a [switchable subscription](#switching-tracks), which opens the new
subscription before it cancels the old one
([relay interop deviation 4](../spec/behavior.md#relay-interop-deviations)). Subscribing with
`parameters: { forward: 0 }` does not work either. moq-relay refuses it with request error 3
`FORWARD=0 not supported` through `onError`, and the connection stays open.

The protocol allows updates only after acceptance. The SDK queues an update issued before
`onSubscribed`, in order, and sends it as soon as the relay accepts. At most eight can wait, and a
further one throws a `RelayError` with code `UPDATE_QUEUE_FULL`. Updating an ended subscription
throws `CLOSED`. A relay that refuses an update with REQUEST_ERROR reaches `onError`, which ends the
subscription ([#94](https://github.com/muxinc/relay-sdks/issues/94)).

Re-subscribing after `onPublisherDone` creates a new request, and it cannot bring back a finished
track. Once the publisher has gone, the relay answers request error 16 `unroutable`. While its
namespace stays announced, the relay replays the finished track's cached last group followed by
another done, one round trip later each time. Don't re-subscribe to a track the publisher ended
until the application knows new content exists. A status-2 done alone does not show that, because
moq-relay also sends one after this subscription's own `update()`, and then re-subscribing is the
way to apply the change. Apply retry backoff to done callbacks as well as to retryable request
errors, and define any continuity policy in the application. See
[publisher restarts and resubscription](../spec/relay-interop.md#publisher-restarts-and-resubscription)
for guidance on retries, trailing objects, and publisher generations.

`connectRelaySubscriber` has a deadline that covers token resolution and both handshakes. Its
optional abort signal controls the connection's entire lifetime. An explicit `jwt` query parameter
takes precedence over `getToken`, which matches the current Relay connection authentication
convention. An empty one (`?jwt=`) throws a `RangeError` before anything connects. Put a token in
it, or drop it and use `getToken`. An endpoint with another scheme, a fragment, or userinfo also
throws a `RangeError`. An endpoint that is not a string holding an absolute URL throws a
`TypeError`. Every deadline (`connectTimeoutMs`, `requestTimeoutMs`, and `writeTimeoutMs`) must be
greater than 0 and at most 2147483647 milliseconds, the largest delay a JavaScript timer holds. A
larger value throws a `RangeError` rather than becoming a deadline that fires at once. A value that
is not a number throws a `TypeError`.

### Token expiry

The relay ends a session when its token expires, and a live session cannot take a new token. Every
connection exposes `expiresAt`, the time that will happen. It comes from the JWT's `exp` claim, or
from a `RelayCredential` that your token provider returns when the token is not a JWT or your server
knows better.

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

At that time the connection closes with `UNAUTHORIZED` in phase `connection`. The plain connections
do not prevent it, so open a replacement before then. The
[supervised connections](#stay-connected-supervised-connections) do it for you, make-before-break,
a minute before expiry by default (`rotateBeforeMs`).

Both roles take the same connection options, `ConnectRelayOptions`: `url`, `getToken`, `signal`,
`connectTimeoutMs`, and `createTransport`. `ConnectRelaySubscriberOptions` adds `requestTimeoutMs`,
the deadline for a subscription's initial response. `ConnectRelayPublisherOptions` adds `namespace`
and has no request deadline. Instead, each track's `writeTimeoutMs` bounds its wait for demand and
its writes. Passing `requestTimeoutMs` to `connectRelayPublisher` is a type error. Untyped JavaScript
that passes it anyway has it ignored, like any other option the call does not take.

### Switching tracks

REQUEST_UPDATE names no track, so moving a viewer between layers (`video-hi`, `video-mid`,
`video-lo`) takes a new subscription. `subscribeSwitchable` keeps one set of handlers on whichever
subscription is active, and moves them make-before-break. `switchTo` subscribes to the new track
while the old one keeps delivering, and cuts over once a new object passes `until`.

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
  `onSwitched` runs before the first object of the new track. A switchable has no `update()`. To
  change a filter or priority, switch to the same track with new `parameters`. The new subscription
  does not inherit parameters.
- `until` defaults to `'first-object'`. That works where delivery is in order, but it can skip media.
  The relay starts a subscription at its cached latest group, which can be stale, and a subscriber on
  another relay can lose the first object ([#243](https://github.com/muxinc/relay-sdks/issues/243)).
  Compare your own timeline, not group IDs, because each track numbers its own groups.
- If the new subscription fails, a newer `switchTo` supersedes it, or the signal aborts, `switchTo`
  rejects and the old track keeps playing. `ended` settles when the active subscription ends.
  `cancel()` cancels everything. See [switching tracks](../spec/behavior.md#switching-tracks) for the
  full contract and measured cutover times.

### Browser transport limits

The browser's QUIC stack owns the connection, not the SDK, and a page cannot configure it. The
browser can end a connection before any SDK deadline acts. The SDK reports what the browser reports,
and cannot tell these cases apart from other losses. Plan for:

- **A connect that fails early.** A handshake that hears nothing for about 4 s fails with
  `TRANSPORT_ERROR` "Opening handshake failed." in phase `connecting`, even when `connectTimeoutMs`
  is longer. A connect to a port where nothing listens fails within milliseconds. In a retry policy,
  treat `TRANSPORT_ERROR` in phase `connecting` like `CONNECTION_TIMEOUT`.
- **A connection lost after about 5 s of silence.** The browser closes a connection with data
  outstanding that hears nothing for about 5 s, even if the network has since come back. The app sees
  `TRANSPORT_ERROR` "Connection lost." in phase `connection`, with a `WebTransportError` cause.
- **Networks that carry only small packets.** Some tunnels and VPNs with a small MTU never let the
  handshake through, and the connect fails as above.

These figures come from Chromium 147 (2026-09). Other browsers are untested and may differ.
[Browser QUIC limits](../spec/relay-interop.md#browser-quic-limits) records the QUIC-level causes.

On a network that blocks UDP, none of this applies because nothing connects. See
[networks without UDP](#networks-without-udp-qmux-over-websocket) for the WebSocket transport.

A browser publisher that must survive outages longer than about 5 s, or heavy burst loss, has to
open a new connection and register its tracks again
([publisher recovery](../spec/publisher-recovery.md)). The new connection does not take over
subscriptions from the old one ([#34](https://github.com/muxinc/relay-sdks/issues/34)). A plain
connection does not reconnect for you, but the
[supervised connections](#stay-connected-supervised-connections) do.

A namespace is an array of 1 to 32 non-empty string fields, sent verbatim without splitting on `/`.
A single string throws a `TypeError`. It is not a one-field namespace. [protocol.json](../spec/protocol.json)
pins the 32-field protocol limit, and exceeding it throws a `RangeError`. Track names must be
non-empty strings. Both rules apply to publishing and subscribing alike. The SDK checks them before
it consults the token provider, the transport factory, or any connection state. See the Arguments
table in [behavior.md](../spec/behavior.md#arguments).

## Networks without UDP: QMux over WebSocket

WebTransport and native QUIC both need UDP. The relay is still reachable over TCP 443 on a network
that blocks UDP, and from a browser without a WebTransport that can negotiate protocols (Safari, or
Firefox before 155). Mux's relays serve MOQT on QMux over a WebSocket there. The optional
[`@caprid/relay-websocket`](packages/relay-websocket/README.md) package is that transport. Nothing
changes unless you pass it as `createTransport`:

```ts
import { connectRelayPublisher, connectRelaySubscriber } from '@caprid/relay';
import { createWebSocketTransport, withWebSocketFallback } from '@caprid/relay-websocket';

// WebTransport first; WebSocket when its handshake fails or takes longer than 4 s.
const subscriber = await connectRelaySubscriber({ url, getToken, createTransport: withWebSocketFallback() });

// Or always WebSocket, in a browser or in Node.
const publisher = await connectRelayPublisher({ url, getToken, namespace, createTransport: createWebSocketTransport() });
```

The endpoint URL stays the `https://` one. The adapter dials `wss://` to the same host and port, with
the path and token in the upgrade request. `withWebSocketFallback({ handshakeTimeoutMs, onFallback })`
sets how long the WebTransport handshake may take, and tells you when a connection fell back. The
timeout defaults to 4000 ms, about when Chromium gives up on a silent handshake. Where the runtime has
no usable WebTransport, the fallback dials WebSocket at once.

The fallback runs the two dials one after the other, and decides again for every connection,
including a supervisor's reconnects. On a network that silently drops UDP, every connect therefore
waits out the timeout first. Where the network refuses UDP, the fallback costs a few milliseconds.
Keep `connectTimeoutMs` well above the handshake timeout.

These things change over WebSocket:

- **Head-of-line blocking.** Every stream shares one TCP connection, so a lost packet holds up every
  track behind it until TCP retransmits it. Groups on separate streams no longer escape each other's
  loss.
- **No datagrams.** No transport implements datagram reception yet, and WebSocket has none to add.
- **A refused token is a `TRANSPORT_ERROR`.** The relay checks the token before it accepts the
  WebSocket, and answers a refusal with HTTP 401, which no WebSocket API exposes. A bad token and an
  unreachable relay both fail with `TRANSPORT_ERROR` in phase `connecting`. A token that expires on a
  live connection is still `UNAUTHORIZED`.
- **Crash detection.** The relay notices a publisher whose process dies at once, because the TCP
  connection closes. It notices one that hangs without closing only through its keep-alive, after
  about 30 s. Over QUIC the relay notices either after its idle timeout.
- **Settling groups.** A group's close resolves once its FIN is handed to the socket, not once the
  relay has it, so `publisher.close()` cannot prove the last groups arrived.

[QMux over WebSocket](../spec/relay-interop.md#qmux-over-websocket) has the measurements and the
relay's side. [Transport adapter](../spec/transport-adapter.md#known-deviations) lists the adapter's
own deviations from the transport contract.

## Consume asynchronously

`subscribeEvents` creates a subscription that a single async iterator owns. Await work directly in
the loop. The `subscribe` API stays available for synchronous callbacks.

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

The default capacity is **64 events**, lifecycle events included. It is a count limit, not a byte
budget, and an object may be up to 16 MiB plus properties. Overflow discards the queued events,
cancels the subscription, and throws `SubscriptionOverflowError`. A request failure throws
`SubscriptionRequestError`. Connection closure throws `SubscriptionConnectionClosedError`, before or
after acceptance, even for a clean connection close, because closure does not prove all objects
arrived. Errors propagate out of the loop for the caller to handle. `publisherDone` is an event,
never the end of iteration.

The iterator preserves arrival order. It does not order objects across streams, and it applies no
transport-wide backpressure. An early exit releases the owned subscription and its observers without
closing `subscriber` or other consumers. Only one pending `next()` is supported, and a concurrent one
rejects with a plain `Error`. `Promise.race` with a timeout leaves the losing `next()` pending. To end
consumption, use the iterator's abort signal, or keep that same read and await it before you start
another. See the [complete event and ownership contract](../spec/behavior.md#async-subscription-consumption).

Passing an `async` subscription callback throws a `TypeError` before anything is sent. If a
synchronous callback returns a promise, the SDK reports it once per handler per subscription through
`console.error`. The SDK observes every rejection, but cannot cancel the work, and never awaits a
callback. Use `subscribeEvents` for async consumers. The SDK copies the handlers when you create the
subscription, so later assignments do not change that subscription.

## Discover namespaces

`namespaceEvents` tells a subscriber which namespaces under a prefix are announced. A call room, a
set of cameras, or a channel list can keep its roster this way without a server of its own. It is the
same bounded iterator as `subscribeEvents`, with the same capacity, signal, overflow, and error
classes. It owns one request, never the connection.

```ts
const roster = new Map<string, readonly string[]>();
for await (const event of subscriber.namespaceEvents({ prefix: ['rooms', 'lobby'], signal })) {
  const key = JSON.stringify(event.namespace); // e.g. ['rooms', 'lobby', 'alice']
  if (event.type === 'announced') roster.set(key, event.namespace);
  else roster.delete(key);
}
```

Namespaces already announced arrive first, about one round trip after the call, with no marker
between them and later changes. A publisher's clean `close()` shows up as `unannounced` within a few
hundred milliseconds. A crashed publisher shows up only at the relay's QUIC idle timeout, about 12 s
later. An announce on another relay of the cluster looks the same as a local one.

A prefix matches whole fields. A prefix the token does not cover stays silent rather than being
rejected. One connection discovers each prefix tree once. A prefix that equals, contains, or falls
under one already being discovered throws `PREFIX_OVERLAP` until that iterator ends. If the relay
ends the request, the iterator first reports every remaining namespace `unannounced`, then finishes.
`announced` means the relay routes the namespace, not that its tracks have content.

Subscribing on the same connection once `announced` arrives avoids the announce race, so it needs no
timed retry. On moq-relay 0.17.0 the relay never rejected such a subscribe as `unroutable` (0 of 160
trials on 2026-10-06). A subscribe sent as soon as the publisher's `announced` settled was rejected 6
to 14 times in 40 on the publisher's relay, and every time on another relay
([measurements](../spec/relay-interop.md#announce-propagation)). A later `unroutable` means the
namespace has gone since. Wait for its next `announced` rather than retrying on a timer. See the
[discovery contract](../spec/behavior.md#discovery).

## Stay connected: supervised connections

`connectRelayPublisher` and `connectRelaySubscriber` never reconnect or resubscribe. A live
application that has to survive network loss can use the supervisors instead. A browser's QUIC
connection ends after about 5 s without connectivity ([browser transport limits](#browser-transport-limits)).
The supervisors take the same connection options and resolve once their first connection is up.

```js
import { connectSupervisedPublisher, connectSupervisedSubscriber } from '@caprid/relay';

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

The supervised publisher reconnects with backoff, from 500 ms up to 10 s, whenever its connection
ends. Each connection is a new **generation** with its own namespace: the root plus a field that
sorts by start time. Each one gets a fresh namespace because moq-relay 0.17.0 never hands existing
subscriptions to a reconnected publisher, and can keep serving a dead one's cached objects
([publisher recovery](../spec/publisher-recovery.md)). The publisher registers every track again on
each generation. It never replays writes. While no generation is up, a write fails at once with
`RECONNECTING`. An operation that the connection's end interrupted fails the same way. Write the
next object, or the next keyframe, when the publisher is live again.

`want(key, ...)` keeps one track subscribed until `unwant(key)`. It retries the announce race. It
resubscribes after `PUBLISH_DONE`, or after a rejection that waiting may fix, with backoff from
250 ms up to 8 s. It moves to the replacement connection. With `staleAfterMs`, it also resubscribes
a track that went silent. Set that only for tracks that are never silent while live. With
`generations: true`, it discovers the publisher's root and stays on the newest generation, moving as
soon as a newer one is announced, with no application channel. A rejection that no wait fixes, such
as bad credentials or a malformed request, reaches `onError`, and the want stops. See the
[supervised contract](../spec/behavior.md#supervised-connections).

A move to a newer generation is make-before-break, as with `switchTo`. The old subscription keeps
delivering until the new generation delivers, and `onSwitched` runs at the cutover instead of
`onRestarted`. Wanting the key again with another track or other parameters moves the same way, so a
supervised viewer changes layers without a gap:

```js
// Cut over once the new layer reaches what was already shown, and let the old one finish in-flight groups.
const caughtUp = (object) => timestampOf(object) >= lastShownTimestamp;
subscriber.want('alice/video', { namespace, track: 'video-hi', generations: true, until: caughtUp, drainMs: 1000 }, handlers);
```

`until` and `drainMs` mean what they mean on [`switchTo`](#switching-tracks), and apply to every move
of the want. Compare your own timeline, never group IDs, because each generation and track numbers
its own groups. If you want the key again and change only `until`, `drainMs`, `staleAfterMs`,
`retry`, or the handlers, the want swaps them without resubscribing. Another namespace, or turning
`generations` on or off, replaces the want.

Both supervisors **rotate** before the token expires ([token expiry](#token-expiry)), so a long
session never hits the relay's deadline. At `rotateBeforeMs` (default 60 s) before `expiresAt`, they
call `getToken` again and open the replacement while the old connection still serves. The
publisher announces its next generation, moves each track to it once viewers subscribe there, and
closes the old generation. Each want moves its subscription to the new connection the same way it
moves between generations. When its target is unchanged, the cutover is invisible: no callback runs,
and nothing is lost or repeated. Call `rotate()` to rotate now, for example after you revoke a token.
In a local test with 12 s tokens, 989 objects written every 40 ms over 9 rotations all arrived, with
no gap longer than 42 ms. See [rotation](../spec/behavior.md#rotation).

## Publishing lifecycle

A dedicated publisher connection serves one namespace with multiple tracks. It uses a separate
transport from subscriber connections. It needs no camera, microphone, catalog, or timestamps.

Publishing follows Relay's announce-and-serve ingest flow. `registerTrack()` registers the track and
sends nothing. `write()` waits for the relay to subscribe upstream (demand) before it sends bytes.
Start a subscriber to create that demand. The default 10-second write deadline covers both the wait
for demand and the transport writes. A failed or cancelled write resets its stream, and leaves the
track open while the publisher connection stays open. The next explicit write uses a new group ID.
After a timeout or a change in the relay's subscription, choose fresh data or the next random-access
point, because you cannot withdraw bytes already delivered. If demand disappeared, the next write
waits for a new relay subscription.

The publisher announces the namespace once you register the first track. `publisher.announced`
settles when that announce has reached the wire. The relay acknowledges nothing more, and it routes
subscribes to the namespace about a round trip later. It rejects a subscribe that arrives before then
with request error 16 (`DOES_NOT_EXIST`), reason `unroutable`, on moq-relay 0.15.8 and later. Relays
through 0.15.1 answered `404 Broadcast not found`. A subscriber on another relay of the cluster waits
longer still, until its relay learns the route.

`subscribe()` does not retry. A subscriber has three ways to handle the race:

- wait for [discovery](#discover-namespaces) to report the namespace `announced`, which avoids the race.
- retry that rejection within its own budget, as the demo and the live smoke test do.
- use a supervised subscriber's `want`, which retries it for you.

A page that owns both sides awaits `announced` before it subscribes, which narrows the race but does
not close it. `announced` stays pending while no solicitation covers the namespace, and the
connection's closure rejects it if that comes first. The SDK handles its rejection in advance, so
you do not have to await it.

Each write copies its bytes and sends one independent group containing one object. The payload is a
`Uint8Array`. The SDK accepts any other `ArrayBufferView` and copies the raw bytes it spans. A
string, an `ArrayBuffer`, or an array throws a `TypeError` before the write starts, so the track stays
usable. Await each write, because a concurrent call rejects rather than growing a queue. Completion
means the transport accepted the complete group, not that a viewer acknowledged it. The publisher
neither replays nor retains an object after it is sent. Late-subscriber behavior also depends on
relay caching and subscription filters. A failed write may have delivered partial data, and nothing
retries it.

Optional `trackProperties` on `registerTrack()` and `properties` on `write()` carry explicit protocol
metadata. Property types, and the values of even types, must be non-negative integers up to
`Number.MAX_SAFE_INTEGER`, the largest value the core's varint carries. Values of odd types must be
`ArrayBufferView`s, and the SDK copies their raw bytes. An invalid property throws a `TypeError`
naming the property index before the track registers or the write starts. A `RangeError` at the same
point covers:

- a byte value longer than 65535 bytes.
- `trackProperties` that encode to more than 65525 bytes (what `SUBSCRIBE_OK` has room for).
- `trackProperties` with more than 64 pairs (the relay's limit).
- `trackProperties` with a `0x0E` above 255, or a `0x22` other than 1 or 2 (values draft 22 rules out).
- object `properties` on `write()` that encode to more than 64 KiB, which the relay drops silently.

The three `trackProperties` cases would leave the track unsubscribable. The SDK measures the encoding
with the core's own encoder, so an oversized property never opens a stream. The SDK adds no default
timescale, timestamp, or LOC media packaging. An object's payload is limited to 16 MiB.

The SDK encodes properties as supplied, but subscribers see the relay's output. moq-relay 0.17.5 and
earlier strip object properties (fixed upstream in moq #5494, not yet released,
[#262](https://github.com/muxinc/relay-sdks/issues/262)). Only the object Timestamp (`0x10`, or the
older `0x06`) survives, rescaled to microseconds by the declared Timescale (`0x08`). The relay drops
every other object property, Video Config included. Tracks without a declared Timescale, and objects
without a Timestamp, get relay-clock timestamps. Declaring
`trackProperties: [{ type: 0x08, value: 1_000_000 }]` keeps supplied Timestamp values unchanged.

Every `SUBSCRIBE_OK` carries the relay's track properties, not the publisher's: `0x08` = 1,000,000,
`0x0E` = 128 unless the publisher set it, and `0x22` = 2. The relay would refuse every subscriber to a
track whose `0x0E` or `0x22` is out of range, or that has more than 64 track properties. That is why
`registerTrack()` refuses those ([#252](https://github.com/muxinc/relay-sdks/issues/252)). A
subscription with `parameters: { includeProperties: 0 }` gets no track properties, and objects with
no properties at all, not even a Timestamp. Keep application metadata that must arrive intact in
payloads, and read the received track timescale when you interpret timestamps. See
[Relay property handling](../spec/behavior.md#relay-property-handling) for the observations and media
guidance.

`track.close()` cancels an outstanding write and ends that track, and other tracks keep running.
`publisher.close()` ends all tracks, retracts the namespace, and waits for the transport to shut
down. You cannot reuse a track name within a publisher connection, even after its writer closes.

### Text and JSON

Track and group writers provide `writeText(text, options?)` and `writeJSON(value, options?)`.
They encode one complete object as UTF-8, and accept the same options as `write()`, including
properties and the track write's abort signal. The group helpers also accept an object status.
Await each call. The usual demand, deadline, cancellation, and encoded byte-size limits apply. An
encoding or serialization error leaves the writer usable.

Call the exported readers explicitly in an object callback:

```ts
import { type MoqtObject, readJSON, readText } from '@caprid/relay';

// For a text track:
const onTextObject = (object: MoqtObject) => console.log(readText(object));
// For a JSON track:
const onJSONObject = (object: MoqtObject) => console.log(readJSON(object));
```

`readText(object)` returns a string, keeps a leading U+FEFF, and throws on malformed UTF-8.
`readJSON(object)` returns `unknown`, and throws on invalid UTF-8 or JSON, so validate its shape in
application code. To skip malformed objects, catch decoding errors in the callback. Empty text is
valid, and an empty JSON payload is not. Neither reader changes the object or its metadata.

`writeJSON` uses `JSON.stringify`, with its usual omission and numeric conversion rules. A top-level
value that produces no JSON (`undefined`, a function, or a symbol) throws a `TypeError`, as do
unsupported values such as cyclic objects or a bigint. `readJSON` uses `JSON.parse`. Neither adds
schema validation or content-type properties. `writeText` uses `TextEncoder`, which turns unpaired
UTF-16 surrogates into replacement characters. Raw byte `write()` and subscriptions stay available.

### Multi-object groups and relay demand

For media, use `openGroup()` to put multiple objects on one stream. Each object gets the next ID,
starting at 0, and `group.close()` sends the group's FIN. A group, such as a video GOP, then pays for
one stream instead of waiting for a FIN on every frame. Pass the first object to
`openGroup({ payload, properties })` to send it as object 0. Its delivery timeout then counts the
wait for the stream, which a bare `openGroup()` has no object to charge.

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

Only one write or open group may occupy a track, and you must await a group's writes and close one
at a time. The deadline applies separately to opening the group (including demand), to each object
write, and to closing it. Idle time between group operations has no limit. These reset the group and
free the track for another group: `group.abort(reason)`, the group's optional `AbortSignal`, a relay
re-subscription, and an operation failure. Start the next group at a random-access point. A local
payload or property validation error leaves the group usable. `group.closed`, `group.groupId`, and
`group.objectCount` report its state, and the count includes only objects the transport accepted.
Empty payloads may carry `status: 'end-of-group'` or `'end-of-track'`. At this level, application
queues and frame dropping are up to the application. For encoded media, the frame writer below
handles both.

`track.hasDemand` reports whether the relay has an upstream subscription for the track now, so that
a write would proceed at once. `track.closed` tells a retired track apart from a track that waits for
demand. `track.demandChanges()` yields the current demand state first, then one value per change in
the relay's subscription. It yields `true` when the relay subscribes or replaces its subscription
(so `true` can repeat), and `false` when that subscription ends. It ends when the track ends. For
example, a catalog can be sent again on each relay subscription:

```ts
for await (const demanded of catalog.demandChanges()) {
  if (demanded) await catalog.write(catalogBytes);
}
```

Breaking the loop releases its observer. A consumer that drives the iterator by hand can call
`return()` to release the observer, including a pending `next()`.

Demand is the relay's own subscription. It is not a viewer count, and it is not a liveness signal.
The relay fans one upstream subscription out to every downstream viewer, so a second viewer produces
no new `true`, and the relay serves it from its cache. moq-relay 0.15 and later cancel the
subscription about one round trip after the last viewer leaves. The next viewer then brings a new
`true` and a re-send. A viewer that resubscribes on the same connection within about 30 seconds can
receive the relay's cached copy before the re-sent one. `true` does not prove anyone is watching.
Write only state that is safe to serve from cache and to receive more than once.

Pausing an encoder while there is no demand is safe, because a frame written then reaches no viewer.
Resume at a keyframe, which a frame writer asks for through `keyframeNeeded`. When the first viewer
is on another relay, or is a moq-lite viewer (such as `@moq/watch`) on any relay, the publisher's
relay subscribes once to look up the track's info, cancels about 50 ms later, and subscribes again.
Demand goes `true`, `false`, `true`. Objects written during that first subscription reach no viewer,
so do not rely on the first objects after demand begins. See
[relay demand](../spec/relay-interop.md#relay-demand) and
[a viewer on another relay](../spec/relay-interop.md#a-viewer-on-another-relay).

`@caprid/relay/media` has both loops ready-made. It is the media publishing policy that the
[WebCodecs example](examples/webcodecs/README.md) runs on:

- `createCatalogService(track, document)` sends a document again on every relay subscription, and
  retries a failed send while demand remains.
- `createDocumentService(track)` does the same for a document you replace with `update(document)`,
  such as a media timeline. It also sends each update while the relay is subscribed.
- `createFrameWriter(track)` maps encoded frames onto groups. By default a keyframe opens a group and
  deltas append to it. `grouping: 'frame'` gives one group per frame. The writer skips frames the
  relay has no subscription for instead of waiting on the deadline, and resumes at the next keyframe
  after any failure.

A frame writer's `write(frame)` resolves to what became of the frame (`'sent'`, `'skipped'`,
`'stale'`, `'dropped'`, `'interrupted'`, or `'ended'`) rather than rejecting. It rejects only for a
caller error, such as a malformed or oversized frame or an exception from your `stale` predicate,
and with `CLOSED` after your own `close()`. You may call it from a
WebCodecs output callback without awaiting it. The writer serves frames in call order. When more
than `maxBacklog` frames wait behind the one being written, it drops frames rather than queueing
them. `writer.keyframeNeeded` and `onKeyframeNeeded` say when the next frame must be a keyframe, so
an encoder can force one. That happens after a demand change, a failure, a stale frame, or a drop.
`@caprid/moq-media`'s `LocFrame` is a frame in the shape the writer takes.

```ts
import { createFrameWriter } from '@caprid/relay/media';

let forceKeyframe = false;
const writer = createFrameWriter(video, { onKeyframeNeeded: () => (forceKeyframe = true) });
// VideoEncoder output callback:
void writer.write(chunkToLocFrame(chunk, { videoConfig })).then((outcome) => {
  if (outcome.kind === 'ended') stop(outcome.error);
});
```

A custom transport whose sink writes or closes asynchronously must honor the writable controller's
abort signal, to interrupt them and reset the underlying stream, as browser WebTransport does. A
generic `WritableStream` sink that ignores the signal can finish its in-flight close after an abort.

## Publishing media

The optional [@caprid/moq-media format package](packages/moq-media/README.md) provides LOC
properties, independent MSF catalogs, source URLs, and mapping for native WebCodecs chunks and
configs. It depends only on `@caprid/moq-core`. Install it beside `@caprid/relay` to publish media.
For live browser video, start with the [direct WebCodecs example](examples/webcodecs/README.md),
which turns encoded chunks into LOC payloads without a muxer or media engine. For existing media
files, the optional [Mediabunny adapter](packages/moq-mediabunny/README.md) maps demuxed packets. See
the [file publishing example](examples/mediabunny/README.md).

## Subscribing to media

A player reads the same tracks back. Read the catalog first. `parseMsfCatalog` from
[@caprid/moq-media](packages/moq-media/README.md) lists the tracks and their decoder configurations.
After a stall, a track's objects can arrive out of order across group streams, and a raw
subscription delivers them as they arrive.

`createFrameSequencer({ clock })` from `@caprid/moq-media` turns one track's objects into frames in
decode order, and drops what is too late against the broadcast's playout clock. Pass
`grouping: 'frame'` for audio, where every group is one independently decodable frame.
`createFrameReader(sequencer, { onFrame })` from `@caprid/relay/media` returns subscription handlers
that drive the sequencer. It feeds the sequencer each event, calls `onFrame` synchronously with each
ready frame in decode order, and polls again when a wait the sequencer reports ends. Spread it into
`subscribe`, or into a supervised `want`, beside your own `onError`. It handles a want's
`onRestarted` and `onSwitched` too, so keep the want's `drainMs` at 0, the default. `reader.close()`
stops delivery. Cancel the subscription as well.

```ts
import { createFrameSequencer, createPlayoutClock } from '@caprid/moq-media';
import { createFrameReader, mediaSubscribeParameters } from '@caprid/relay/media';

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
of video when your downlink is congested: 16 for audio and 96 for video, the values
`mediaTrackProperties` gives the publisher. moq-relay orders what it sends you by your subscriber
priority alone and ignores the publisher's, so without it audio waits behind video's backlog. Choose
the priority when you subscribe, because an `update()` ends the subscription. Pass other parameters
as the second argument, such as `{ locationFilter }`. A `subscriberPriority` there replaces the
default.

A frame's `timestampUs` is its LOC timestamp on the broadcast timeline, scaled by whichever
Timescale the object or the track declares. The clock's target latency is the lateness budget for
every track. By default it is twice the longest of the last four keyframe intervals the sequencer
reports, or 2 s before there are any. Under keyframe grouping, the sequencer drops a frame more than
that behind the clock's position along with its group, and resumes delivery at a later keyframe.
Under frame grouping, it drops a frame later than the clock's `lateDropUs` (40 ms by default). The
first frame after a skip, a resync, or a restart carries `discontinuity`, so reset the decoder there.
`sequencer.stats` counts delivered, late, and untimed frames, skipped groups, and resyncs.

The reader paces decoding, not display. Audio is the master. Report what is audible with
`clock.reportAudio(timestampUs, atUs)`. Before you show each decoded video frame, ask
`clock.due(frame.timestamp)` whether to `wait` (for `waitUs`), `show`, or `drop` it. Until audio is
reported or `due` is first asked, the clock is idle and nothing is late. With no audio, it paces by
the wall clock. The [@caprid/moq-media README](packages/moq-media/README.md) shows how to compute the
audible timestamp. See [media subscribing](../spec/behavior.md#media-subscribing) for the
sequencer's contract.

A broadcast with no video never reports a keyframe interval, so its clock would keep the 2 s
default. A lasting delay increase would then cost 2 s of silence before the reader resyncs. Create
an audio-only clock with `createPlayoutClock({ targetLatencyUs: AUDIO_ONLY_TARGET_LATENCY_US })`
(500 ms).

### Playing audio

`createAudioPlayer({ context })` from `@caprid/moq-media` plays decoded audio, with no UI. Each
`player.addInput({ clock, decoderConfig })`, usually one per speaker, decodes the frames you push to
it. It holds them in an adaptive jitter buffer in an `AudioWorklet`, and reports what is audible to
`clock` about every 20 ms, so the clock follows the device and stays the master for any video. Pass
`input.push` as a frame reader's `onFrame`. Every input goes through its own `gain` into
`player.output`, one node for the whole mix, which you connect:

```ts
import {
  AUDIO_ONLY_TARGET_LATENCY_US,
  audioDecoderConfigFromTrack,
  createAudioPlayer,
  createFrameSequencer,
  createPlayoutClock,
} from '@caprid/moq-media';
import { createFrameReader } from '@caprid/relay/media';

const context = new AudioContext({ sampleRate: 48_000, latencyHint: 'interactive' }); // in a user gesture
const player = await createAudioPlayer({ context }); // one per context, shared by every speaker
player.output.connect(context.destination);

const clock = createPlayoutClock({ targetLatencyUs: AUDIO_ONLY_TARGET_LATENCY_US }); // one per broadcast
const input = player.addInput({ clock, decoderConfig: audioDecoderConfigFromTrack(audioTrack) });
const reader = createFrameReader(createFrameSequencer({ clock, grouping: 'frame' }), { onFrame: input.push });
subscriber.subscribe({ namespace, track: 'audio' }, { ...reader, onError });
// Later: reader.close(), cancel the subscription, input.remove(); player.close() when the call ends.
```

The buffer starts at 60 ms. It grows by 40 ms after each underrun, and shrinks by 10 ms after each
2 s of smooth playback, never below 20 ms. It trims audio it held but never needed. Decoded audio
reaches the worklet from the main thread, so a page whose main thread stalls for tens of
milliseconds can underrun at that floor. Such a page can pass `minJitterTargetUs: 40_000`, the floor
in earlier releases. The buffer stays under 400 ms, and under the clock's target latency less the
output delay and 40 ms. `input.stats` holds the underruns, the buffer depth and target, the frames
dropped as late, and the audio that was trimmed, concealed with silence, or cut.

The player fills a lost Opus frame rather than playing silence. It decodes Opus with libopus
compiled to WebAssembly, which rebuilds the frame from the next frame's in-band FEC when the
publisher's encoder sent one, and otherwise conceals it (`stats.recoveredUs`, `stats.synthesizedUs`).
To get rebuilt frames, publish with in-band FEC on and an expected loss above 0.
`createAudioPlayer({ context, lossConcealment: false })` decodes with WebCodecs instead, which resets
its decoder at every discontinuity and timestamp jump. A `resync` or `restart` flushes the buffer.

`input.cut({ fromUs, untilUs })` drops received audio in a timeline range, whether held or arriving
later, as when a talker is interrupted. A talker that can stop mid-utterance, such as a voice bot,
lists an `interrupt` data track in its catalog, and writes an interrupt there when it stops. Apply
each one to the input that plays its track:

```ts
import { INTERRUPT_TRACK, parseInterrupt } from '@caprid/moq-media';

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
import { createTranscript, parseTranscriptSegment, TRANSCRIPT_TRACK } from '@caprid/moq-media';

const transcript = createTranscript();
subscriber.subscribe(
  { namespace, track: TRANSCRIPT_TRACK },
  { onObject: (object) => transcript.apply(parseTranscriptSegment(object.payload)) },
);
// With each interrupt: transcript.cut(interrupt, clock.position()).
const said = transcript.linesAt(clock.position()).map((line) => line.text);
```

See [spec/media-layer.md](../spec/media-layer.md#audio-player) for the rules and the reasons behind
them, and its [Interrupt track](../spec/media-layer.md#interrupt-track) and
[Transcript track](../spec/media-layer.md#transcript-track) sections for the conventions.

**Echo cancellation.** In a call the page also captures a microphone. Chromium's echo canceller
removes only audio that WebRTC played, so audio played straight from an `AudioContext` reaches the
other side again through the speakers. In Chromium, route the mix through a local WebRTC loopback
instead of connecting it to the destination:

```ts
import { createEchoCancellationLoopback } from '@caprid/moq-media';

const loopback = await createEchoCancellationLoopback(context, player.output); // rejects if it cannot connect
// loopback.setSinkId(deviceId) picks the output device; loopback.close() stops it.
```

Firefox and Safari cancel echo from all page output, so play directly there. The loopback adds the
WebRTC receiver's buffering, which no API reports. Set `player.outputDelayUs` if video must stay in
sync with the audio. The [WebCodecs example](examples/webcodecs/README.md)'s `listen-audio.ts`
plays its microphone track this way. The [voice bot page](examples/voice-bot/README.md) plays a
Python bot through the loopback while it publishes the microphone.

### Voice: delivery per direction

Voice needs different things in each direction. Each track's sequencer takes its own policy without
touching the clock that the broadcast's other tracks share. moq-relay 0.17.5 (measured) and 0.17.7
(read in source) deliver every group however late, whatever delivery timeout a subscriber sends, and
answer FETCH without timestamps
([relay-interop.md](../spec/relay-interop.md#voice-delivery-on-0175-deadlines-fetch-and-group-order)).
On those releases both policies live in the subscriber. The options work with and without a relay
that honours the subscriber's delivery timeout
([moq-dev/moq#5881](https://github.com/moq-dev/moq/issues/5881)). Such a relay drops what is late,
and the subscriber's options handle whatever still arrives late or out of order.

**To a listener** (an agent's or a caller's voice to a person), the goal is low, steady latency,
where a gap beats a stall. Drop audio that is late against the playout target, and catch up when it
keeps arriving late. `catchUpAfterUs` makes the sequencer ask for a catch-up. A supervised
subscriber's `catchUp(key)` then replaces the subscription at the live edge, which drops the relay's
backlog. The want's `onRestarted` (reason `'catch-up'`) restarts the reader. Keep `catchUpAfterUs`
below the clock's target latency, or a resync re-anchors on the late audio first. The subscriber's
own delivery timeout goes on the SUBSCRIBE as well, for a relay that honours it. moq-relay 0.17.5 and
0.17.7 ignore it.

```ts
// subscriber is a supervised subscriber (connectSupervisedSubscriber).
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

**To an agent** (a person's voice to speech-to-text), the goal is every word, soon after the person
stops. Completeness comes from the person's publisher and the reorder window, not from FETCH. Give
the person's audio track a longer delivery timeout of 1 to 2 s instead of audio's 500 ms, with
`mediaTrackProperties('audio', { subgroupTimeoutUs: 2_000_000 })`, so the publisher's transport keeps
resending through loss. On the agent, drop nothing as late. Wait for a missing group at least as
long as that timeout, because a resent group can arrive that long after the one that follows it. A
frame after a group that never came carries `missingGroups`, the group IDs to act on.

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

**From a text-to-speech agent** that sends its reply faster than real time, each frame stamped with
its slot, hold what arrives early and play it at its timestamp. Without `timestampPlayout` a frame more
than the clock's target latency ahead resyncs the clock forward and the player drops most of the reply.
The lead is how far ahead of the clock the player wants frames: pass the input's own.

```ts
const input = player.addInput({ clock, decoderConfig: audioDecoderConfigFromTrack(audioTrack) });
const reply = createFrameReader(
  createFrameSequencer({ clock, grouping: 'frame', timestampPlayout: { leadUs: () => input.playoutLeadUs } }),
  { onFrame: input.push },
);
```

A live talker's frames are never early, so they play as without it. Delay that underruns add while a
queue builds is given back once frames come early again, as without it.

## Errors and recovery

Branch on `code` and `phase`, not on message wording. SDK lifecycle errors use these stable codes:

| Code | Application response |
| --- | --- |
| `CONNECTION_TIMEOUT` | Connecting exceeded its deadline. Consider a new connection with backoff. |
| `WRITE_TIMEOUT` | Inspect the phase below, then choose the next object or group explicitly. |
| `DELIVERY_TIMEOUT` | An object outlived its delivery timeout before it reached the transport. The data is stale. Do not retry it as it is. Choose fresh data or the next random-access point. See [delivery timeouts](../spec/behavior.md#delivery-timeouts). |
| `CLOSED` | The handle is closed. Use an open connection, track, or group. |
| `WRITE_PENDING` | Await the outstanding operation, or finish or abort the open group, before you start another. |
| `NAMESPACE_NOT_SOLICITED` | Received namespace solicitations do not cover this publisher. Check the namespace and token configuration. A later matching solicitation can restore demand. |
| `NAMESPACE_ENDED` | The relay ended the namespace announcement. Open a replacement publisher if appropriate. |
| `SUBSCRIPTION_REQUEST_FAILED` | An async subscription request failed. Inspect its preserved MOQT request error and the connection state. |
| `SUBSCRIPTION_OVERFLOW` | The event buffer filled, and the SDK cancelled the subscription. Choose a suitable capacity or consume faster before you subscribe again. |
| `SUBSCRIPTION_CHANGED` | Upstream demand ended or changed during an operation. Choose fresh data or a new random-access point. |
| `UPDATE_QUEUE_FULL` | Eight updates already wait for the subscription's acceptance. Wait for `subscribed` before you update again. |
| `PREFIX_OVERLAP` | The prefix equals, contains, or falls under one this connection already discovers. Widen one iterator's prefix and filter, or use a second connection. |
| `RECONNECTING` | A supervised publisher had no live generation for the write. Write the next object, or the next keyframe, once it is live again. |
| `GOAWAY` | The relay requested migration. Open a replacement connection. |
| `TRANSPORT_ERROR` | Inspect the original cause and the connection state before you decide whether to reconnect or publish again. |
| `TRANSPORT_UNAVAILABLE` | Supply a transport implementation in a runtime without browser WebTransport. In Node, import from `@caprid/relay/node` with `@caprid/relay-quic` installed. |
| `UNAUTHORIZED` | The relay refused the connection's credentials: no token, a bad, malformed, or expired one, or an endpoint path the token does not grant. Supply a valid token before you connect again. In phase `connection`, the credential of a live session expired or its grant was revoked ([token expiry](#token-expiry)). Browsers and native QUIC (`@caprid/relay/node`) report it. Against relays before moq-relay 0.17.0, a browser does not see the refusal, so it arrives as `TRANSPORT_ERROR` (#14). |

`connecting` covers token resolution and connection setup. `connection` marks a connection-wide
failure, and makes no claim about individual writes that were in flight. `subscription` marks an
async subscription request failure or a local event-buffer overflow. Publishing records
`waiting-for-demand`, `opening-stream`, `writing`, or `closing` before it enters each step. A
`WRITE_TIMEOUT` or `DELIVERY_TIMEOUT` in `waiting-for-demand` means this attempt sent no object
bytes. For a timeout, or a transport or subscription failure, in a transport phase, allow for partial
delivery. `closing` can fail after all object bytes were sent. Opening a group can start sending its
header before the first object write. A phase records where the failure started, and a later call on
the failed handle can report that same error.

For `WRITE_PENDING`, and for calls rejected because a handle is already closed, the phase describes
the refused operation, and that call sends nothing. A write or group refused with `CLOSED` because
its track or group is already closed reports phase `waiting-for-demand`, the phase that means nothing
was sent. `CLOSED` can also interrupt an operation already in flight. It then carries that
operation's phase, so the code alone does not rule out partial delivery.

No code or phase promises safe replay. Account for previous attempts, data freshness, and your
application's deduplication policy. A successful write means the transport accepted it, not that a
viewer acknowledged it. A plain connection never retries or reconnects. The
[supervisors](#stay-connected-supervised-connections) reconnect, but never replay a write.

`UNAUTHORIZED` is the one code that says the relay refused the credentials. The relay closed the
session with MOQT session termination code `UNAUTHORIZED` (0x2). `MALFORMED_AUTH_TOKEN` (0x16) and
`EXPIRED_AUTH_TOKEN` (0x18) map to it too. In phase `connecting` the relay refused to connect. In
phase `connection` it ended a live session whose credential expired or whose grant was revoked
([token expiry](#token-expiry)). The SDK reports it in browsers and over native QUIC. Against relays
older than moq-relay 0.17.0, a browser does not see the relay's close, so a refusal there arrives as
`TRANSPORT_ERROR` (#14). No other connection failure, and no uncovered namespace, proves that the
relay denied authentication. The relay refuses a subscription outside the token's grant per request,
through `onError` (MOQT `UNAUTHORIZED`, 0x1), and leaves the connection open.

A clean connection shutdown still resolves `closed` without an error, and later operations fail
with `CLOSED`. After a failed connection, `closed` and later connection operations expose the same
failure. Caller cancellation reasons and token-provider exceptions keep their identity. Argument and
serialization errors keep their usual JavaScript types. Subscription `onError` callbacks keep the
separate MOQT `RequestError` contract and numeric protocol codes.

`@caprid/relay` exports `RelayError`, `RelayErrorCode`, and `RelayErrorPhase`. A wrapped transport or
session failure keeps the original error in `cause`.

```ts
import { RelayError } from '@caprid/relay';

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

- `@caprid/relay` (the `ts/` root package) owns the Relay product API and the authentication
  convention.
- `@caprid/moq-core` (`packages/moq-core`) is a generic protocol package under Apache-2.0 that builds
  on its own. It uses standard streams, has no runtime dependencies, and imports no player, UI, or
  media engine.
- `@caprid/relay-quic` and `@caprid/relay-websocket` are transport adapters that the application
  passes as `createTransport`. `@caprid/relay` depends on neither. The WebSocket adapter is the one
  package with a third-party runtime dependency, `@moq/qmux` at an exact version
  ([ADR 0005](../docs/adr/0005-websocket-adapter-wraps-moq-qmux.md)).
- `subscriber.trackSource` exposes the generic `MoqTrackSource` contract for player integrations.
  It covers track requests in protocol terms, with the core's `onOk` and `onDone` handler names, and
  closure observation. It has no track listing, and no operation to close the shared connection.

`@caprid/moq-core` started from an attributed snapshot of the Video.js protocol modules and their
regression tests. `packages/moq-core/UPSTREAM.json` records the source.

## Initial scope

The SDK supports reliable-stream track subscriptions, subscription updates and cancellation,
[namespace discovery](#discover-namespaces), and publication on a dedicated connection, of data or
of encoded media through `@caprid/relay/media`. The [supervisors](#stay-connected-supervised-connections)
add reconnection, resubscription, and token rotation on a live session. The borrowed generic
interface also exposes FETCH and TRACK_STATUS. The SDK offers `moqt-22`, then `moqt-20` as a
fallback, and speaks whichever the relay selects.

The SDK does not implement datagram reception, discovery of tracks within a namespace, or a Video.js
adapter. On a plain connection, a GOAWAY ends the connection with an error. Subscriptions still
waiting for their first response end with it, never through `onError`: `subscribed` rejects with
`SubscriptionConnectionClosedError`, and `ended` resolves `terminated`. You can open a replacement,
and a supervisor does that for you.

A custom `createTransport` factory supports tests, and runtimes with a compatible transport. The
default uses browser WebTransport. A factory whose transport carries MOQT directly on QUIC, rather
than on a WebTransport session, returns `framing: 'native-quic'`. The core then sends the endpoint's
path, token, and authority in SETUP. `@caprid/relay-websocket`'s factories return
`framing: 'websocket'`, which sends none of them, as on WebTransport. See
[networks without UDP](#networks-without-udp-qmux-over-websocket).

## Development

Contributor setup, checks, formatting, and the opt-in live Relay smoke test for this workspace are
in the root [README](../README.md#development).
