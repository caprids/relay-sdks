# Relay JavaScript SDK

Publish and subscribe to arbitrary track objects through Relay without Video.js, a media catalog,
or a player. The default transport requires **browser WebTransport in a secure context** (HTTPS or
localhost). Node 24+ runs the build tools and example launcher; a plain Node script cannot connect
with the default transport. The launcher below runs the SDK in headless Chromium. Other runtimes
must supply a compatible `createTransport` factory; installing this package does not supply one.

This is the TypeScript SDK in the Relay SDKs repository. The shared protocol pins, behavior
contract, and test vectors live in [spec](../spec/README.md); the [root README](../README.md) describes the suite. Package names
are provisional and publication is disabled.

## First round trip

The packages are **unpublished**. Use the checked-out pnpm workspace and its browser runner below;
there is no registry install step. You need Git and [mise](https://mise.jdx.dev/getting-started.html)
available on your PATH. Commands below run in Bash from the repository root.

### Endpoint and credentials

Ask the team operating your Relay deployment (or your application's backend owner) for an HTTPS
WebTransport endpoint, the exact authorized namespace fields including any root prefix, and a
short-lived Relay JWT authorized to **publish and subscribe** there. The SDK consumes tokens; it
does not issue them or provide a public token signup flow. Do not use a Mux API access token in
place of a Relay JWT. The hosted demo defaults to `https://sjc.relay.mux.global`, which still requires
authorized credentials. For another deployment, use the endpoint supplied by its operator.

The runner reads `RELAY_TOKEN` when set, otherwise prompts with hidden input. Prefer the prompt for
manual runs; automation can inject the environment variable through its secret manager. Do not put
tokens in commands, source files, URLs, or committed `.env` files. Tokens are passed to `getToken`
inside the demo and are redacted from its output.

### Browser token exposure

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

### Install and run

```sh
git clone https://github.com/muxinc/relay-sdks.git
cd relay-sdks
mise install
mise run ts:install
mise exec -- pnpm --dir ts exec playwright install chromium
```

On Linux, if Chromium reports missing system libraries, provision Playwright's system dependencies
for your machine (`playwright install-deps chromium`) before continuing.

A publisher's `write()` waits for relay demand: a matching subscriber must be running. Even when
publishing connects first, a fresh namespace can briefly reject subscriptions with `404 Broadcast
not found` while its announcement propagates. The combined demo starts both sides, retries only
that subscription rejection with capped backoff, and waits for demand before writing. Once a write
starts, a failure stops the run; it never automatically replays potentially delivered bytes.

Enter the endpoint and a **fresh namespace authorized by your token** when prompted below. The CLI
splits `team/quickstart/unique-run` into `['team', 'quickstart', 'unique-run']`; the SDK itself sends
fields verbatim. Both connections use those same fields and track `ticker`. If your token authorizes
only one exact namespace, use that and avoid overlapping publishers or stale cached objects.

```sh
read -r -p 'Relay HTTPS endpoint: ' RELAY_URL
read -r -p 'Authorized namespace (slash-separated): ' RELAY_NAMESPACE
mise run ts:example:mux-global -- --url "$RELAY_URL" --namespace "$RELAY_NAMESPACE" --track ticker --count 1 --timeout 30
# Enter the Relay token at the hidden prompt.
```

The task builds both local packages, serves their ESM output with an import map on localhost,
and launches Chromium. The complete SDK example, including imports and cleanup, is
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
`finally` closes the connections, browser, and local server on success or failure. A write completing
only means the transport accepted it; the `received` line establishes this demo's round trip.

### Use the local packages in your application

The supported first-use path is the workspace above. To use the build in an existing browser app,
serve `ts/dist/` and `ts/packages/moq-core/dist/` as JavaScript modules and map **both** bare imports
as the runner does:

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
60 seconds of each other, in either order. A subscriber started first retries fresh-namespace 404s;
a publisher started alone waits for demand and then fails at the overall deadline.

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
- Repeated `404 Broadcast not found`: check that the publisher is still running and both sides
  use the exact same authorized namespace and track. The demo's backoff stops at the run deadline.
- `waiting ... no relay demand yet` until the deadline: start the matching subscriber, and verify
  the token's namespace prefix and publish/subscribe permissions.
- A write failure after demand: delivery may be partial. The demo stops without resending. Decide
  on fresh data or an explicit application replay policy before trying again.

## Subscription lifecycle

`subscribe` returns a cancellable handle immediately; acceptance is reported by
`onOk` and rejection by `onError`. Observe the connection's `closed`
promise as well: an accepted subscription is no longer usable once the connection closes.
Cancelling a subscription stops that consumer (including trailing objects after `onDone`),
while closing the connection stops all of its subscriptions. `onDone` alone neither closes the
connection nor proves delivery has drained. The demo waits for its object count or the deadline.
Separate readiness/termination awaitables are proposed in
[#68](https://github.com/muxinc/relay-sdks/issues/68); they are not APIs in this checkout.

Namespace fields are preserved verbatim. Known tracks can be subscribed to directly; the SDK
neither interprets the payload nor assumes that a track contains audio, video, or timestamps.
An accepted data subscription has no application inactivity deadline. The request timeout only
bounds its initial response. An idle connection still depends on peer traffic or transport-provided
keepalive; the SDK does not schedule its own keepalive. Subscriptions have independent cancellation; the application owns connection shutdown.

Object callbacks run synchronously as streams deliver objects. The SDK adds no application queue,
ordering across streams, automatic JSON decoding, clock synchronization, or media scheduling. Handle
subgroup resets and connection closure explicitly; this API makes no exactly-once or gap-free delivery promise.
`onDone` is the publisher's control notification, and may precede trailing objects in transit.

Re-subscribing after `onDone` creates a new request and can replay cached objects followed
by another done notification while the publisher is absent. Apply retry backoff to done callbacks
as well as retryable request errors, and define any continuity policy in the application. See
[publisher restarts and resubscription](../spec/relay-interop.md#publisher-restarts-and-resubscription)
for retry, trailing-object, and publisher-generation guidance.

`connectRelay` has a deadline covering token resolution and both handshakes. Its optional abort
signal controls the connection's entire lifetime. An explicit `jwt` query parameter takes precedence
over `getToken`, matching the current Relay connection authentication convention. Every deadline
(`connectTimeoutMs`, `requestTimeoutMs`, and `writeTimeoutMs`) must be between 0 (exclusive) and
2147483647 milliseconds, the largest delay a JavaScript timer holds. A larger value is a `RangeError`
rather than a deadline that fires immediately.

Namespaces contain 1–32 non-empty string fields, sent verbatim without splitting on `/`.
The 32-field protocol limit is pinned in [protocol.json](../spec/protocol.json). Oversized namespaces are
rejected before opening a publisher connection or sending a subscription; track names must
be non-empty strings.

## Publishing lifecycle

A dedicated publisher connection serves one namespace with multiple tracks. It uses a separate
transport from subscriber connections. No camera, microphone, catalog, or timestamps are required.

Publishing follows Relay's announce-and-serve ingest flow. `publish()` registers the track;
`write()` waits for the relay to subscribe upstream before sending bytes. Start a subscriber to
create that demand. The default 10-second write deadline covers both demand and transport writes.
A failed or cancelled write resets its stream and leaves the track open while the publisher
connection remains open. The next explicit write uses a new group ID. After a timeout or relay
subscription change, choose fresh data or the next random-access point; already delivered bytes
cannot be withdrawn. If demand disappeared, the next write waits for a new relay subscription.

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
`TypeError` naming the property index before registering a track or starting a write. The SDK adds
no default timescale, timestamp, or LOC media packaging. Objects are limited to 16 MiB.

Properties are encoded as supplied, but subscribers see the relay's output. With relay 0.14.17,
tracks without a declared Timescale (`0x08`) received relay-generated timestamps and a microsecond
timescale. Declaring `trackProperties: [{ type: 0x08, value: 1_000_000 }]` preserved supplied
Timestamp (`0x10`) values in the tests; other tested object properties, including Video Config,
were dropped. Missing timestamps were synthesized even on declared tracks. Keep application
metadata that must arrive intact in payloads, and read the received track timescale when interpreting
timestamps. See [Relay property handling](../spec/behavior.md#relay-property-handling) for the
version-specific observations and media guidance.

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
resource discovery, and a Video.js adapter are not implemented yet. A GOAWAY ends the connection with an error and fails requests still awaiting their first response via `onError`;
callers can open a replacement. A custom `createTransport`
factory supports tests or runtimes with a compatible transport; the default uses browser WebTransport.

## Live Relay smoke test

This opt-in Chromium test publishes and subscribes against a real relay. It is excluded from
`pnpm check` and needs a token authorized for both operations. The token can be supplied through
hidden stdin input or the `RELAY_TOKEN` environment variable; it is never written to a file.

Set `RELAY_URL` and `RELAY_NAMESPACE` using the quickstart prompts first. Your token must
authorize both operations under that namespace prefix, including children: the test appends a
random UUID. Running this command explicitly opts into contacting that endpoint.

```sh
mise exec -- pnpm --dir ts exec playwright install chromium   # once
RELAY_URL="$RELAY_URL" RELAY_NAMESPACE_PREFIX="$RELAY_NAMESPACE" mise run ts:test:relay
```

The test checks binary
and empty payloads plus independent track closure, then closes both connections. It defaults to
`https://sjc.relay.mux.global`; override that with `RELAY_URL` if needed. Namespace fields sent by
the SDK are absolute: include the token's root prefix when required by the relay.

The live test passed against `sjc.relay.mux.global` on 2026-09-09: three objects on `events` and two
on `status`, with the second status object delivered after the events track closed.

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
