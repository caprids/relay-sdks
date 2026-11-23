# Discovery and presence

Relay knows which namespaces its publishers have announced. Your application can read that live roster directly from the relay—without running a separate presence or signaling server—using `publishingRoots()` / `publishing_roots()`.

## Follow publishing roots

Have every participant publish under a shared prefix at its own unique ID (`[...prefix, myId]`). Any subscriber that wants the roster opens a publishing-roots view on `prefix`.

A publisher announces its namespace when it registers its **first track**. A publisher with zero registered tracks is not listed as a root.

<!-- tabs -->
<!-- tab: TypeScript -->

```ts
import { connectSupervisedPublisher, connectSupervisedSubscriber } from '@caprid/relay';

const prefix = ['room', roomId];

// Every party publishes under the prefix at its own ID.
const publisher = await connectSupervisedPublisher({ namespace: [...prefix, myId], getToken });
const camera = publisher.registerTrack({ track: 'video' });

// Follow every publishing root under the prefix.
const subscriber = await connectSupervisedSubscriber({ getToken });
const roots = subscriber.publishingRoots(
  { prefix },
  {
    onChanged({ added, removed, present, initial }) {
      for (const root of added) {
        if (root.id === myId) continue; // separate subscriber sees your own publisher too
        subscriber.want(
          `video/${root.id}`,
          { namespace: root.namespace, track: 'video', generations: true },
          { onObject: (object) => showFrame(root.id, object) },
        );
      }
      for (const root of removed) {
        subscriber.unwant(`video/${root.id}`);
      }
      console.log(initial ? 'loaded' : 'changed', [...present.keys()]);
    },
    onError: (error) => console.error('discovery failed permanently:', error),
  },
);
```

<!-- tab: Python -->

```python
from caprid_relay import (
    MoqtObject,
    PublishingRootsChange,
    PublishingRootsHandlers,
    WantHandlers,
    connect_supervised_publisher,
    connect_supervised_subscriber,
)

prefix = ["room", room_id]

# Every party publishes under the prefix at its own ID.
publisher = await connect_supervised_publisher(namespace=[*prefix, my_id], get_token=get_token)
camera = publisher.register_track("video")

# Follow every publishing root under the prefix.
subscriber = await connect_supervised_subscriber(get_token=get_token)


def on_changed(change: PublishingRootsChange) -> None:
    for root in change.added:
        if root.id == my_id:
            continue  # separate subscriber sees your own publisher too
        subscriber.want(
            f"video/{root.id}",
            root.namespace,
            "video",
            WantHandlers(on_object=lambda obj, rid=root.id: show_frame(rid, obj)),
            generations=True,
        )
    for root in change.removed:
        subscriber.unwant(f"video/{root.id}")
    print("loaded" if change.initial else "changed", [root.id for root in change.present])


roots = subscriber.publishing_roots(
    prefix,
    PublishingRootsHandlers(
        on_changed=on_changed,
        on_error=lambda error: print("discovery failed permanently:", error),
    ),
)
```

<!-- /tabs -->

Each `onChanged` / `on_changed` receives:
- `added`: roots that started publishing (on the initial load, all roots already publishing).
- `removed`: roots that stopped publishing (empty on the initial load).
- `present`: all currently publishing roots (`Map<string, PublishingRoot>` in TS, `tuple[PublishingRoot, ...]` in Python).
- `initial`: `true` on the first call only (always fires, even if the prefix is empty).

The view handle also exposes `roots.loaded`, `roots.present`, and `roots.close()`.

### Partitioning by role with a label

Place a role label in the namespace **before** the ID so the relay filters server-side by prefix:

- Participants: `['room', roomId, 'participant', userId]`
- Bots: `['room', roomId, 'bot', botId]`

Opening `publishingRoots` on `['room', roomId, 'participant']` discovers only participants; the relay sends nothing about bots. Note that the prefix must sit exactly one field above the IDs—a view on `['room', roomId]` with this layout loads empty because generations sit three fields below it.

## Presence: metadata and fast staleness detection

A publishing root tells you a publisher is announced, but not its profile metadata—and a crashed or frozen publisher remains announced until the relay's idle timeout (10–13 s).

Presence adds a `presence` track where each party publishes a small JSON document and resends it every 2 s as a heartbeat, letting viewers detect stale peers within 6 s.

<!-- tabs -->
<!-- tab: TypeScript -->

`@caprid/relay/media` provides `publishPresence` and `createPresenceView`:

```ts
import { createPresenceView, publishPresence } from '@caprid/relay/media';

interface Profile {
  name: string;
  handRaised: boolean;
}

// Publish your profile (resends on change and every 2 s as a heartbeat).
const me = publishPresence<Profile>(publisher, { name: 'Ada', handRaised: false });

// Validate incoming documents—any publisher authorized under the prefix can write arbitrary JSON.
function parseProfile(value: unknown): Profile {
  const { name, handRaised } = value as Partial<Profile>;
  if (typeof name !== 'string' || typeof handRaised !== 'boolean') throw new TypeError('invalid profile');
  return { name, handRaised };
}

const room = createPresenceView(
  subscriber,
  { prefix, parse: parseProfile },
  {
    onChanged({ joined, left, updated, stale, resumed, initial }) {
      for (const m of joined) console.log(initial ? 'here' : 'joined', m.id, m.document.name);
      for (const m of updated) console.log('updated', m.id, m.document);
      for (const m of stale) console.log('stale', m.id);
      for (const m of resumed) console.log('resumed', m.id);
      for (const m of left) console.log('left', m.id);
    },
  },
);

// Update your document (sent only when JSON changes).
me.update({ ...me.document, handRaised: true });
```

Close the publisher (`await publisher.close()`) to leave cleanly; viewers receive `left` once the relay withdraws the namespace (~105 ms on the same relay).

<!-- tab: Python -->

The Python SDK does not ship a dedicated presence helper yet, but the `presence` track is plain JSON (one document per group). Combine `publishing_roots()` with a `want()` on each root's `presence` track, tracking `(generation, group_id)` so cached copies replayed on resubscribe do not reset your staleness timer:

```python
import time
from caprid_relay import (
    MoqtObject,
    PublishingRootsChange,
    PublishingRootsHandlers,
    WantHandlers,
    read_json,
)

STALE_AFTER = 6.0  # 3x the 2 s heartbeat
documents: dict[str, object] = {}
heard: dict[str, float] = {}
route: dict[str, str] = {}
newest: dict[str, tuple[str, int]] = {}


def is_newer(root_id: str, group_id: int) -> bool:
    gen = route.get(root_id)
    if gen is None:
        return False
    if root_id not in newest:
        return True
    last_gen, last_group = newest[root_id]
    if gen == last_gen:
        return group_id > last_group
    return gen > last_gen or (time.monotonic() - heard[root_id] >= STALE_AFTER)


def on_presence(root_id: str, obj: MoqtObject) -> None:
    if obj.status != "normal" or not is_newer(root_id, obj.group_id):
        return
    newest[root_id] = (route[root_id], obj.group_id)
    try:
        documents[root_id] = read_json(obj)
    except ValueError:
        return
    heard[root_id] = time.monotonic()


def on_changed(change: PublishingRootsChange) -> None:
    for root in change.added:
        subscriber.want(
            f"presence/{root.id}",
            root.namespace,
            "presence",
            WantHandlers(
                on_object=lambda obj, rid=root.id: on_presence(rid, obj),
                on_subscribed=lambda _info, ns, rid=root.id: route.__setitem__(rid, ns[-1]),
                on_switched=lambda info, rid=root.id: route.__setitem__(rid, info.to_namespace[-1]),
            ),
            generations=True,
        )
    for root in change.removed:
        subscriber.unwant(f"presence/{root.id}")
        for state in (documents, heard, route, newest):
            state.pop(root.id, None)


roots = subscriber.publishing_roots(prefix, PublishingRootsHandlers(on_changed=on_changed))
```

<!-- /tabs -->

See [Presence track](../../spec/media-layer.md#presence-track) for the wire format.

## Things to check

- **Empty list with no error can mean a token grant mismatch.** If the subscriber's token does not grant `prefix`, the relay accepts the discovery request and reports nothing (`onError` never fires).
- **Separate connections see themselves; single connections do not.** A separate `connectSupervisedSubscriber` sees your `connectSupervisedPublisher` as a root (skip `root.id === myId`). A combined `connectSupervisedRelay` never sees its own roots because the relay never echoes announcements back to the same connection.
- **Overlapping views and wants are allowed.** One supervised subscriber can hold multiple `publishingRoots()` views and `want()` subscriptions on overlapping prefixes—they share one underlying discovery request. (Only low-level `namespaceEvents()` rejects overlapping prefixes with `PREFIX_OVERLAP`.)
- **Rotation vs reconnect.** Token rotation overlaps the old and new connections, so viewers see no removal. A hard disconnect may remove the root and add it back when the publisher reconnects; re-wanting in `added` handles both cleanly.
- **Leave timings**: clean `close()` removes a root in ~105 ms on the same relay (~210 ms cross-relay); a crashed publisher drops from presence after 6 s and from `publishingRoots` after 10–13 s (relay idle timeout). See [Publishing-roots contract](../../spec/behavior.md#publishing-roots).
