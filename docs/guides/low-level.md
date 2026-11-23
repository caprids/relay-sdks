# Low-level connections

Most applications should use the [supervised connections](connecting.md) (`connectSupervised*` / `connect_supervised_*`). Use the unsupervised connections documented here when you want direct control over connection lifetime without automatic reconnection or generation suffixes—for example in short-lived CLI tools, custom retry policies, or integrating `subscriber.trackSource` / `subscriber.track_source` with an external player.

## Unsupervised connect functions

- `connectRelayPublisher` / `connect_relay_publisher`: publishes one namespace directly (no generation segment appended).
- `connectRelaySubscriber` / `connect_relay_subscriber`: subscribes to tracks and discovers namespaces.
- `connectRelay` / `connect_relay`: returns a `RelaySession` that both subscribes and publishes any number of namespaces on one connection via `session.publish(namespace)`.

None of these functions reconnect after a drop, rotate expiring tokens, or retry rejected subscriptions. A `GOAWAY` closes the connection with `RelayError` code `GOAWAY`.

<!-- tabs -->
<!-- tab: TypeScript -->

```ts
import { connectRelay, readText } from '@caprid/relay';

const session = await connectRelay({ getToken });
const pub = session.publish(['room', 'demo', myId]);
const track = pub.registerTrack({ track: 'chat' });
await pub.announced;

const sub = session.subscribe(
  { namespace: ['room', 'demo', peerId], track: 'chat' },
  {
    onObject: (object) => console.log(readText(object)),
    onPublisherDone: (done) => console.log('publisher done:', done.statusCode),
    onError: (err) => console.error('request error:', err),
  },
);
await sub.subscribed;
sub.ended.then((end) => console.log('subscription ended:', end.kind, end.error));

await track.writeText('hello');
sub.cancel();
await session.close();
```

<!-- tab: Python -->

```python
from caprid_relay import RelaySubscriptionHandlers, connect_relay, read_text

async with await connect_relay(get_token=get_token) as session:
    pub = session.publish(["room", "demo", my_id])
    track = pub.register_track("chat")
    await pub.announced

    sub = session.subscribe(
        ["room", "demo", peer_id],
        "chat",
        RelaySubscriptionHandlers(
            on_object=lambda obj: print(read_text(obj)),
            on_publisher_done=lambda done: print("publisher done:", done.status_code),
            on_error=lambda err: print("request error:", err),
        ),
    )
    await sub.subscribed
    await track.write_text("hello")
    sub.cancel()
```

<!-- /tabs -->

## Subscription lifecycle

`subscribe()` returns a `RelaySubscription` immediately:

- `subscription.subscribed` resolves on acceptance and rejects with `SubscriptionRequestError` (rejection or request timeout), `SubscriptionConnectionClosedError` (connection closed first), or your cancellation reason.
- `subscription.ended` resolves once (never rejects) with `kind` (`'rejected'`, `'terminated'`, or `'cancelled'`), `cause`, and `error`.
- `onPublisherDone` / `on_publisher_done` notifies you that the publisher finished the track, but **trailing objects can still arrive** and the subscription remains open until you call `cancel()` or the connection closes.
- **Do not call `subscription.update()` against `moq-relay`**: `moq-relay` does not implement `REQUEST_UPDATE` and terminates the subscription within one round trip with `onPublisherDone` (status 2). To pause, cancel and resubscribe later; to change track, priority, or filter without a gap, use `subscribeSwitchable` below. Subscribing with `forward: 0` is also rejected with request error `3`.

See the full state table in [Subscription lifecycle](../../spec/behavior.md#subscription-lifecycle).

## Async event iterators

If you prefer `for await` / `async for` over callbacks, use `subscribeEvents()` / `subscribe_events()`:

<!-- tabs -->
<!-- tab: TypeScript -->

```ts
for await (const event of subscriber.subscribeEvents({ namespace, track: 'chat' }, { capacity: 128 })) {
  if (event.type === 'object') console.log(readText(event.object));
}
```

<!-- tab: Python -->

```python
async with subscriber.subscribe_events(namespace, "chat", capacity=128) as events:
    async for event in events:
        if event.type == "object":
            print(read_text(event.object))
```

<!-- /tabs -->

If the consumer falls behind and the local buffer fills past `capacity`, the SDK cancels the subscription and raises `RelayError` with code `SUBSCRIPTION_OVERFLOW`.

## Raw namespace discovery and the announce race

A publisher announces its namespace when you register its first track; `publisher.announced` settles once that announce reaches the wire, roughly one round trip before the relay routes incoming subscriptions (longer across regions). Subscribing before the route propagates fails with request error 16 (`DOES_NOT_EXIST`, reason `unroutable`).

`namespaceEvents()` / `namespace_events()` streams raw `announced`, `unannounced`, and `loaded` events for a prefix so you can subscribe only after the relay confirms the route:

<!-- tabs -->
<!-- tab: TypeScript -->

```ts
for await (const event of subscriber.namespaceEvents({ prefix: ['room', 'demo'] })) {
  if (event.type === 'announced') {
    subscriber.subscribe({ namespace: event.namespace, track: 'chat' }, handlers);
  }
}
```

<!-- tab: Python -->

```python
async with subscriber.namespace_events(["room", "demo"]) as events:
    async for event in events:
        if event.type == "announced" and event.namespace is not None:
            subscriber.subscribe(event.namespace, "chat", handlers)
```

<!-- /tabs -->

On an unsupervised connection, `namespaceEvents()` rejects any prefix that equals, contains, or falls under an already-active discovery prefix with `RelayError` code `PREFIX_OVERLAP`.

## Switchable subscriptions

For make-before-break track switching on an unsupervised subscriber, use `subscribeSwitchable()` / `subscribe_switchable()` and `switchTo()` / `switch_to()`:

<!-- tabs -->
<!-- tab: TypeScript -->

```ts
const switchable = subscriber.subscribeSwitchable(
  { namespace, track: 'video-mid' },
  {
    onSwitched: () => decoder.reset(),
    onObject: (object, track) => decoder.decode(object, track),
  },
);
await switchable.subscribed;

await switchable.switchTo('video-hi', {
  until: (object) => object.objectId === 0 && readTimestampUs(object) > lastTimestampUs,
});
```

<!-- tab: Python -->

```python
from caprid_relay import SwitchableHandlers

switchable = subscriber.subscribe_switchable(
    namespace,
    "video-mid",
    SwitchableHandlers(
        on_switched=lambda _info: decoder.reset(),
        on_object=lambda obj, track: decoder.decode(obj, track),
    ),
)
await switchable.subscribed

await switchable.switch_to(
    "video-hi",
    until=lambda obj: obj.object_id == 0 and read_timestamp_us(obj) > last_timestamp_us,
)
```

<!-- /tabs -->

## Generic `trackSource` interface

`subscriber.trackSource` / `subscriber.track_source` exposes the protocol-level `MoqTrackSource` contract for external player integrations (track requests in protocol terms and closure observation, with no ability to close the shared connection).
