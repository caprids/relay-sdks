# Publishing and subscribing

Relay routes arbitrary byte payloads grouped into **tracks** under **namespaces**. You do not need a media catalog, codec, or player to send data.

## Publish and subscribe with supervised connections

A supervised publisher registers tracks whose writers survive reconnects and token rotations. A supervised subscriber registers named **wants** (`want()`) that automatically retry transient `unroutable` rejections, resubscribe across reconnects, and follow publisher generations.

<!-- tabs -->
<!-- tab: TypeScript -->

```ts
import {
  connectSupervisedPublisher,
  connectSupervisedSubscriber,
  readJSON,
} from '@caprid/relay'; // or '@caprid/relay/node'

const namespace = ['room', 'demo', 'sensor-1'];

// Publisher: register tracks once; writers outlive individual connections.
const publisher = await connectSupervisedPublisher({ namespace, getToken });
const readings = publisher.registerTrack({ track: 'readings' });

// Subscriber: want() follows the publisher's generations automatically.
const subscriber = await connectSupervisedSubscriber({ getToken });
subscriber.want(
  'sensor-1',
  { namespace, track: 'readings', generations: true },
  {
    onObject: (object) => console.log('reading:', readJSON(object)),
    onSubscribed: (info, { namespace: gen }) => console.log('subscribed to', gen),
    onRestarted: ({ reason }) => console.log('restarted:', reason),
    onError: (err) => console.error('permanent error:', err),
  },
);

await readings.writeJSON({ tempC: 21.4, at: Date.now() });

// Later:
subscriber.unwant('sensor-1');
subscriber.close();
await publisher.close();
```

<!-- tab: Python -->

```python
import time
from caprid_relay import (
    WantHandlers,
    connect_supervised_publisher,
    connect_supervised_subscriber,
    read_json,
)

namespace = ["room", "demo", "sensor-1"]

# Publisher: register tracks once; writers outlive individual connections.
publisher = await connect_supervised_publisher(namespace=namespace, get_token=get_token)
readings = publisher.register_track("readings")

# Subscriber: want() follows the publisher's generations automatically.
subscriber = await connect_supervised_subscriber(get_token=get_token)
subscriber.want(
    "sensor-1",
    namespace,
    "readings",
    WantHandlers(
        on_object=lambda obj: print("reading:", read_json(obj)),
        on_subscribed=lambda info, gen: print("subscribed to", gen),
        on_restarted=lambda reason, gen: print("restarted:", reason),
        on_error=lambda err: print("permanent error:", err),
    ),
    generations=True,
)

await readings.write_json({"tempC": 21.4, "at": time.time()})

# Later:
subscriber.unwant("sensor-1")
subscriber.close()
await publisher.close()
```

<!-- /tabs -->

### Why `generations: true` matters

A supervised publisher announces each connection as `[...namespace, generation]`. Setting `generations: true` / `generations=True` on `want()` tells the subscriber to discover the publisher's latest generation under `namespace` and seamlessly cut over whenever the publisher reconnects or rotates its token. Omit `generations` only when subscribing to an unsupervised publisher that announces `namespace` directly.

## Publish and subscribe on one connection

A participant in a room or two-way app can publish any number of namespaces and subscribe to others over a single connection and token with `connectSupervisedRelay` / `connect_supervised_relay`:

<!-- tabs -->
<!-- tab: TypeScript -->

```ts
import { connectSupervisedRelay } from '@caprid/relay';

const relay = await connectSupervisedRelay({ getToken });
const me = relay.publish(['room', roomId, myId]); // served as room/<roomId>/<myId>/<generation>
const chat = me.registerTrack({ track: 'chat' });

relay.want('peer-chat', { namespace: ['room', roomId, peerId], track: 'chat', generations: true }, {
  onObject: (object) => console.log(object),
});

await chat.writeText('hello');
await relay.close();
```

<!-- tab: Python -->

```python
from caprid_relay import WantHandlers, connect_supervised_relay

relay = await connect_supervised_relay(get_token=get_token)
me = relay.publish(["room", room_id, my_id])  # served as room/<room_id>/<my_id>/<generation>
chat = me.register_track("chat")

relay.want(
    "peer-chat",
    ["room", room_id, peer_id],
    "chat",
    WantHandlers(on_object=lambda obj: print(obj)),
    generations=True,
)

await chat.write_text("hello")
await relay.close()
```

<!-- /tabs -->

The relay never routes a connection's own announcements back to it, so discovery on `relay` excludes your own roots and subscribing to your own track on the same connection is rejected with request error 16.

## Text, JSON, and raw bytes

Track and group writers support three payload helpers:

- `write(bytes)` — copies any `ArrayBufferView` (TS) or buffer-protocol object such as `bytes`/`memoryview` (Python) and sends one independent group holding one object (up to 16 MiB).
- `writeText(str)` / `write_text(str)` and `readText(obj)` / `read_text(obj)` — strict UTF-8 text. Empty strings are valid.
- `writeJSON(value)` / `write_json(value)` and `readJSON(obj)` / `read_json(obj)` — UTF-8 JSON without schema validation. Empty payloads and `NaN`/`Infinity` are rejected. Catch parse errors inside `onObject` to ignore malformed payloads.

Await each `write()` before starting the next. A concurrent write on the same track fails immediately with `WRITE_PENDING` instead of growing an unbounded queue.

## Relay demand

Relay follows an announce-and-serve model:

1. `registerTrack()` / `register_track()` registers the track locally. The first registered track triggers the namespace announcement.
2. `write()` waits until the relay has an upstream subscription for the track (**demand**) before sending bytes. By default `writeTimeoutMs` / `write_timeout` is 10 s and covers both waiting for demand and the transport write.
3. If no subscriber appears before the deadline, `write()` fails with `RelayError` code `WRITE_TIMEOUT` and `phase: 'waiting-for-demand'`, meaning **no bytes were sent**. If the relay's solicitations exclude your namespace, it fails with `NAMESPACE_NOT_SOLICITED`.

Check `track.hasDemand` / `track.has_demand` or iterate `track.demandChanges()` / `track.demand_changes()` to send state only when the relay is subscribed (for example, re-sending a catalog or snapshot on each new relay subscription):

<!-- tabs -->
<!-- tab: TypeScript -->

```ts
for await (const demanded of catalog.demandChanges()) {
  if (demanded) await catalog.write(catalogBytes);
}
```

<!-- tab: Python -->

```python
async for demanded in catalog.demand_changes():
    if demanded:
        await catalog.write(catalog_bytes)
```

<!-- /tabs -->

Demand is the relay's single upstream subscription, not a viewer count: a second viewer on the same relay is served from cache and produces no new `true`. When the first viewer is on another relay (or a `moq-lite` viewer), demand briefly toggles `true → false → true` while the relay inspects the track ([relay demand](../../spec/relay-interop.md#relay-demand)).

## Multi-object groups

Use `openGroup()` / `open_group()` when multiple objects belong together on a single QUIC stream (such as a video GOP starting with a keyframe). Pass the first object to `openGroup` so its delivery timeout covers the stream open:

<!-- tabs -->
<!-- tab: TypeScript -->

```ts
const group = await video.openGroup({ payload: keyframeBytes, properties: keyframeProps });
try {
  for (const delta of deltas) {
    await group.write(delta.bytes, { properties: delta.properties });
  }
  await group.close();
} catch (error) {
  group.abort(error);
  throw error;
}
```

<!-- tab: Python -->

```python
group = await video.open_group(keyframe_bytes, properties=keyframe_props)
try:
    for delta in deltas:
        await group.write(delta.bytes, properties=delta.properties)
    await group.close()
except BaseException:
    group.abort()
    raise
```

<!-- /tabs -->

For encoded video and audio, prefer the higher-level `createFrameWriter` / `FrameWriter`, which handles demand skipping, backlog dropping, and keyframe recovery automatically. See [Browser audio & video](browser-media.md) or [Python media & bots](python-media.md).

## Location filters, properties, and delivery timeouts

- **Location filters**: Without a `locationFilter`, subscriptions start at the live edge. To fetch recent cached groups from the relay, pass `parameters: { locationFilter: { type: 'relative-group', groupsBeforeNext: N } }` (TS) or `MessageParameters(location_filter=RelativeGroupLocationFilter(groups_before_next=N))` (Python), or an `absolute` start. See [late subscribers](../../spec/relay-interop.md#late-subscribers-and-location-filters).
- **Staleness**: Set `staleAfterMs` / `stale_after` on `want()` to automatically resubscribe a track that goes silent for that duration (triggers `onRestarted` with reason `'stale'`). Use it only for continuously active tracks, not sparse event tracks.
- **Properties & delivery timeouts**: Pass `trackProperties` / `track_properties` on `registerTrack()` and `properties` on `write()`. Properties `0x02` (`OBJECT_DELIVERY_TIMEOUT`) and `0x06` (`SUBGROUP_DELIVERY_TIMEOUT`) bound object age and FIN acknowledgement in milliseconds on the publisher's link. Note that `moq-relay` 0.17.5 and earlier strip most object properties other than Timestamp (`0x10`, scaled by Timescale `0x08`); keep application metadata that must survive intact inside the payload. See [Relay property handling](../../spec/behavior.md#relay-property-handling) and [Delivery timeouts](../../spec/behavior.md#delivery-timeouts).
