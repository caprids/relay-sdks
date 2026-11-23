# Switching tracks and layers

In MOQT, `REQUEST_UPDATE` cannot change a subscription's track (and `moq-relay` terminates a subscription on any `REQUEST_UPDATE`). Moving a viewer between renditions (`video-hi`, `video-mid`, `video-lo`), cameras, or publisher generations requires opening a new subscription and cutting over cleanly.

A supervised subscriber's `want()` handles this **make-before-break**: call `want()` again with the same key and the new target track. The old track keeps delivering until the new track is ready, and then the subscriber cancels the old subscription and cuts over.

## Switch tracks with `want()`

Pass an `until` predicate to decide which object on the new track triggers the cutover, and handle `onSwitched` / `on_switched` to reset decoders or update UI state.

<!-- tabs -->
<!-- tab: TypeScript -->

```ts
import { connectSupervisedSubscriber } from '@caprid/relay';

const subscriber = await connectSupervisedSubscriber({ getToken });
let lastTimestampUs = -1;

// Start on the medium rendition.
subscriber.want(
  'video',
  { namespace, track: 'video-mid', generations: true },
  {
    onSwitched: ({ from, to }) => {
      console.log(`switched ${from.track} -> ${to.track}`);
      decoder.reset();
    },
    onObject: (object) => {
      lastTimestampUs = readTimestampUs(object);
      decoder.decode(object);
    },
    onError: (err) => console.error(err),
  },
);

// Later: switch make-before-break to the high rendition on a keyframe ahead of what we already decoded.
subscriber.want(
  'video',
  {
    namespace,
    track: 'video-hi',
    generations: true,
    until: (object) => object.objectId === 0 && readTimestampUs(object) > lastTimestampUs,
  },
  handlers,
);
```

<!-- tab: Python -->

```python
from caprid_relay import WantHandlers, connect_supervised_subscriber

subscriber = await connect_supervised_subscriber(get_token=get_token)
last_timestamp_us = -1


def on_object(obj) -> None:
    global last_timestamp_us
    last_timestamp_us = read_timestamp_us(obj)
    decoder.decode(obj)


handlers = WantHandlers(
    on_switched=lambda info: (print(f"switched {info.from_track} -> {info.to_track}"), decoder.reset()),
    on_object=on_object,
    on_error=lambda err: print("error:", err),
)

# Start on the medium rendition.
subscriber.want("video", namespace, "video-mid", handlers, generations=True)

# Later: switch make-before-break to the high rendition on a keyframe ahead of what we already decoded.
subscriber.want(
    "video",
    namespace,
    "video-hi",
    handlers,
    generations=True,
    until=lambda obj: obj.object_id == 0 and read_timestamp_us(obj) > last_timestamp_us,
)
```

<!-- /tabs -->

## Choosing the cutover predicate (`until`)

Without `until`, the switch cuts over on the very first object delivered by the new track. For media tracks, **always supply `until`** comparing media timestamps (and checking `objectId === 0` for a video keyframe):

- **Group IDs do not align across tracks.** Each track numbers its own groups independently; never compare `groupId` between `video-mid` and `video-hi`.
- **The first object may be stale or mid-GOP.** Cutting over on an arbitrary cached object can jump backward or feed a delta frame to a reset decoder.
- **If the new track fails before `until` matches**, the old subscription stays active and keeps delivering uninterrupted.

When switching video renditions with `@caprid/moq-media`'s `createVideoPlayer`, call `player.configure(newDecoderConfig)` before the new track's first keyframe arrives so the decoder reconfigures seamlessly on the cutover keyframe.

## Draining the old track (`drainMs` / `drain`)

By default, the old subscription is cancelled immediately at cutover (`drainMs: 0` / `drain=0`). If you are switching a non-media stream where in-flight objects on the old track should finish arriving briefly after cutover, set `drainMs` (TS) or `drain` in seconds (Python).

Keep `drainMs` / `drain` at `0` when using `createFrameReader` / `FrameReader` or `createLatestValueReader` / `LatestValueReader`—those readers reset group tracking on `onSwitched` and expect no trailing objects from the old subscription.

## Low-level switching

If you are using an unsupervised `connectRelaySubscriber`, use `subscribeSwitchable()` / `subscribe_switchable()` and `switchTo()` / `switch_to()` for the same make-before-break mechanism without automatic reconnection. See [Low-level connections](low-level.md#switchable-subscriptions).
