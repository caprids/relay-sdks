# Control and telemetry

Applications that stream live media often carry small data tracks alongside it: sensor telemetry, cursor state, or remote control commands. Choose the delivery pattern per track:

- **Latest-wins state** (telemetry, pose, steering setpoints): a newer value supersedes every older one.
- **Reliable commands** (emergency stop, mode switches): every message matters even when another follows.

## Bidirectional pattern

Use `connectSupervisedRelay` / `connect_supervised_relay` so each peer publishes its own namespace and subscribes to the other peer's namespace over one connection (or use separate publisher and subscriber connections). For example, a robot publishes `video` and `telemetry` under `['room', 'robot']` and subscribes to `control` under `['room', 'cockpit']`.

<!-- tabs -->
<!-- tab: TypeScript -->

```ts
import { connectSupervisedRelay, readJSON } from '@caprid/relay';
import { createDocumentService, createLatestValueReader } from '@caprid/relay/media';

const relay = await connectSupervisedRelay({ getToken });

// Publish latest-wins telemetry
const me = relay.publish(['room', 'robot']);
const telemetry = createDocumentService(me.registerTrack({ track: 'telemetry' }), undefined, {
  retryDelayMs: 100,
});
telemetry.serve({ signal }).catch(onError);
setInterval(() => telemetry.update(new TextEncoder().encode(JSON.stringify(readSensors()))), 50);

// Subscribe to latest-wins control commands ahead of video priority
const control = createLatestValueReader({
  onValue: (object) => applyControl(readJSON(object)),
  late: (object) => Date.now() - (readJSON(object) as { sentAt: number }).sentAt > 500,
  onCatchUp: () => relay.catchUp('control'),
});
relay.want(
  'control',
  {
    namespace: ['room', 'cockpit'],
    track: 'control',
    generations: true,
    parameters: { subscriberPriority: 0 },
  },
  { ...control, onError },
);
```

<!-- tab: Python -->

```python
import asyncio
import json
import time
from caprid_relay import MessageParameters, connect_supervised_relay, read_json
from caprid_relay.media import DocumentService, LatestValueReader

relay = await connect_supervised_relay(get_token=get_token)

# Publish latest-wins telemetry
me = relay.publish(["room", "robot"])
telemetry = DocumentService(me.register_track("telemetry"), retry_delay=0.1)
serving = asyncio.create_task(telemetry.serve())


async def report() -> None:
    while True:
        telemetry.update(json.dumps(read_sensors()).encode())
        await asyncio.sleep(0.05)


# Subscribe to latest-wins control commands ahead of video priority
control = LatestValueReader(
    lambda obj: apply_control(read_json(obj)),
    late=lambda obj: time.time() - read_json(obj)["sent_at"] > 0.5,
    on_catch_up=lambda: relay.catch_up("control"),
)
relay.want(
    "control",
    ["room", "cockpit"],
    "control",
    control.want_handlers(on_error=on_error),
    generations=True,
    parameters=MessageParameters(subscriber_priority=0),
)
```

<!-- /tabs -->

## Latest-wins values

Send each state snapshot as a complete, self-contained object in its own group—never as a delta against the previous object—because late joiners start at the live edge and receivers drop superseded values.

- **Publishing**: `createDocumentService` / `DocumentService` holds only the latest value passed to `update()`. While a write is in flight, newer updates overwrite older ones so no queue builds up, and the service automatically re-sends the current snapshot whenever relay demand restarts. Lower `retryDelayMs` / `retry_delay` (default 1 s) for fast-changing telemetry.
- **Subscribing under congestion**: `moq-relay` 0.17 delivers every queued group on a congested downlink regardless of subscriber delivery timeouts ([measurements](../../spec/relay-interop.md#voice-delivery-on-0175-deadlines-fetch-and-group-order)). Handle backlog on the receiver:
  1. **`createLatestValueReader` / `LatestValueReader`** drops any object older than the highest group ID seen on the current subscription and resets automatically on `onRestarted` and `onSwitched`. Keep `drainMs` / `drain` at `0`.
  2. **`late` + `catchUp(key)` / `catch_up(key)`**: include a timestamp in the payload (assuming synchronized clocks) and pass a `late` predicate. On the first late value after a delivered one, `onCatchUp` fires; call `catchUp(key)` to replace the subscription at the live edge and flush the relay's backlog. Set the `late` threshold above normal link jitter so values immediately after catch-up are not rejected too.
  3. **`maxAge` / `max_age`**: on relays that advertise support (`maxAgeSupported` / `max_age_supported`), setting `maxAge` (ms) lets the relay drop superseded groups server-side. The SDK sends it only to supporting relays, so it is safe to set now ([subscriber max age](../../spec/relay-interop.md#subscriber-max-age-max_age)).
  4. **Prioritize ahead of media**: the relay schedules downlink delivery strictly by `subscriberPriority` / `subscriber_priority` (lower is more urgent). Use `0` for control/telemetry so it preempts audio (`16`) and video (`96`).

## Reliable commands

For messages that must not be skipped (for example, an emergency stop or state transition):

- Await `track.write()` or `writeJSON()` / `write_json()` for each command, queueing commands in your own application code (concurrent writes fail with `WRITE_PENDING`).
- Check `error.phase` on `WRITE_TIMEOUT`: only `'waiting-for-demand'` proves zero bytes were sent.
- Include a monotonically increasing sequence number in the payload because groups travel on independent QUIC streams and may arrive out of order.
- A completed `write()` means only that the local transport accepted the bytes—if you need end-to-end acknowledgement, have the receiver reply on its own track.

## Latency and staleness settings at a glance

Several publisher and subscriber options have similar names but control different things:

| Setting | Side | What it controls |
| --- | --- | --- |
| `createPlayoutClock({ targetLatencyUs })` / `PlayoutClock(target_latency_us=...)` | Viewer | Lateness budget for dropping video groups and triggering automatic `resync`. Does not add playout delay. Defaults to 2x recent keyframe interval (or 2 s; 500 ms audio-only). |
| `catchUpAfterUs` (TS) with `catchUp(key)` | Viewer | Replaces a media subscription at the live edge when frames stay late (`onRestarted` reason `'catch-up'`). |
| `late` + `onCatchUp` on `LatestValueReader` | Viewer | Drops late data objects and calls `catchUp(key)` to clear relay backlog. |
| `staleAfterMs` / `stale_after` on `want()` | Viewer | Resubscribes a track silent for that duration (`'stale'`). Use only on continuously active tracks; does not fix backlog. |
| `mediaTrackProperties(kind, { targetLatencyUs })` | Publisher | Sizes the publisher's subgroup delivery timeout for video on the uplink only. Does not set viewer latency. |
| Delivery timeouts (`0x02`, `0x06`) | Publisher | Fails objects waiting too long before transport (`0x02`) and resets unacknowledged finished groups (`0x06`) on the publisher's link ([delivery timeouts](../../spec/behavior.md#delivery-timeouts)). |
