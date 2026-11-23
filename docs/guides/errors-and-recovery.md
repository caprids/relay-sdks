# Errors and recovery

Always branch on `error.code` and `error.phase`, never on message text. Both SDKs raise `RelayError` with identical code and phase strings.

<!-- tabs -->
<!-- tab: TypeScript -->

```ts
import { RelayError } from '@caprid/relay';

try {
  await track.writeText('state');
} catch (error) {
  if (error instanceof RelayError && error.code === 'WRITE_TIMEOUT') {
    if (error.phase === 'waiting-for-demand') {
      // Zero bytes were sent; safe to treat as unsent.
    } else {
      // Partial delivery is possible; send fresh state or apply your dedup policy.
    }
  } else {
    throw error; // Caller AbortSignal reasons pass through unchanged.
  }
}
```

<!-- tab: Python -->

```python
from caprid_relay import RelayError

try:
    await track.write_text("state")
except RelayError as error:
    if error.code == "WRITE_TIMEOUT":
        if error.phase == "waiting-for-demand":
            # Zero bytes were sent; safe to treat as unsent.
            pass
        else:
            # Partial delivery is possible; send fresh state or apply your dedup policy.
            pass
    else:
        raise  # asyncio.CancelledError and token-provider errors pass through unchanged.
```

<!-- /tabs -->

## Error codes

| Code | What happened and how to respond |
| --- | --- |
| `UNAUTHORIZED` | The relay rejected credentials (missing, malformed, expired, revoked, or path outside the token's grant). In `connecting`, the handshake was refused; in `connection`, a live session's token expired or was revoked. Fetch a fresh Client token before reconnecting. *(Browsers see `TRANSPORT_ERROR` instead against relays older than `moq-relay` 0.17.0.)* |
| `CONNECTION_TIMEOUT` | Connecting exceeded `connectTimeoutMs` / `connect_timeout`. Retry with backoff (supervised connections do this automatically). |
| `TRANSPORT_ERROR` | Underlying QUIC/WebTransport/WebSocket failure. Inspect `error.cause` / `error.__cause__` and connection state. In browsers, a `connecting`-phase `TRANSPORT_ERROR` usually means the browser abandoned the handshake; treat it like `CONNECTION_TIMEOUT`. |
| `TRANSPORT_UNAVAILABLE` | No usable transport in this runtime. In Node, import from `@caprid/relay/node` with `@caprid/relay-quic` installed; in browsers without WebTransport protocol negotiation, use `@caprid/relay-websocket`. |
| `GOAWAY` | The relay requested migration. Open a replacement connection (supervised connections handle this automatically). |
| `RECONNECTING` | A supervised publisher or relay had no live generation during the write. Nothing was replayed; write the next fresh object or keyframe once live. |
| `WRITE_TIMEOUT` | A write or group open exceeded `writeTimeoutMs` / `write_timeout`. Inspect `phase` before deciding what to write next. |
| `DELIVERY_TIMEOUT` | An object outlived its `0x02` delivery timeout before reaching the transport, resetting its group. Data is stale—write fresh data or the next keyframe. |
| `WRITE_PENDING` | Another write or open group is already active on this track or group. Await or abort it first. |
| `CLOSED` | The connection, track, or group is already closed. |
| `NAMESPACE_NOT_SOLICITED` | The relay's solicitations do not cover this publisher's namespace. Verify the token grant and namespace prefix. |
| `NAMESPACE_ENDED` | The relay terminated the namespace announcement. Open a replacement publisher if appropriate. |
| `SUBSCRIPTION_CHANGED` | Upstream relay demand ended or changed mid-write. Start the next group at a fresh object or keyframe. |
| `SUBSCRIPTION_REQUEST_FAILED` | An async subscription request was rejected. Inspect the preserved MOQT `RequestError`. |
| `SUBSCRIPTION_OVERFLOW` | An async event iterator (`subscribeEvents` / `subscribe_events`) filled its buffer and cancelled the subscription. Increase `capacity` or consume faster. |
| `UPDATE_QUEUE_FULL` | Eight `update()` calls are already queued waiting for acceptance. |
| `PREFIX_OVERLAP` | Low-level `namespaceEvents()` / `namespace_events()` rejected an overlapping prefix. Widen one prefix or use `publishingRoots()`. |

## Error phases and partial delivery

`error.phase` tells you where the operation failed:

- `connecting`: token resolution and connection handshake.
- `connection`: connection-wide failure (makes no claim about individual in-flight writes).
- `subscription`: async subscription request failure or local buffer overflow.
- `waiting-for-demand` → `opening-stream` → `writing` → `closing`: publishing pipeline stages.

**Only `waiting-for-demand` guarantees zero bytes left the client.** Any failure in `opening-stream`, `writing`, or `closing` may have delivered partial bytes. Neither SDK replays writes automatically—even supervised connections drop in-flight writes across reconnects so your application controls freshness and deduplication.

Note that per-request authorization failures (subscribing to a namespace outside the token's grant) do **not** close the connection; they are reported to the subscription's `onError` / `on_error` callback as a MOQT `RequestError` with numeric code `1` (`UNAUTHORIZED`).

## Inspecting transport causes

When `error.code === 'TRANSPORT_ERROR'`, the wrapped transport error is preserved in `error.cause` (TS) or `error.__cause__` (Python). Inspect its shape rather than private class names:

- `source === 'stream'`: peer reset or stopped a single stream (`streamErrorCode` / `stream_error_code`).
- `source === 'session'`: connection terminated (`closeCode` / `close_code`, `reason`).

In Python, `moq_core.transport.is_stream_error` and `is_session_error` check these shapes.

## Browser transport limits

Browser WebTransport stacks are owned by the browser, and applications cannot configure QUIC congestion control or idle timers:

- **Short idle/outage timeout**: Chromium closes a QUIC connection that receives no response for ~5 s while data is outstanding, even if the network recovers afterward. Supervised connections open a fresh generation automatically.
- **Opaque handshake failures**: when a handshake fails or times out, browsers often surface a generic `WebTransportError` without the server's close code, which maps to `connecting`-phase `TRANSPORT_ERROR`.
- **No UDP / Safari**: pass `withWebSocketFallback()` from `@caprid/relay-websocket` as `createTransport` to fall back to QMux over WebSocket on TCP 443.

For loss, congestion, prioritization, FEC, and bitrate adaptation, see [Unreliable networks](../networks.md).

## Test recovery by dropping the transport

You can test reconnection and generation switching deterministically without pulling network cables by wrapping `createTransport` so your test can close every open transport on demand:

<!-- tabs -->
<!-- tab: TypeScript -->

```ts
import {
  connectSupervisedSubscriber,
  type MoqtTransport,
  type RelayTransportFactory,
  webTransport,
} from '@caprid/relay';

const dial = webTransport(); // or nativeQuicTransport() in Node / withWebSocketFallback()
const open = new Set<MoqtTransport>();
const createTransport: RelayTransportFactory = (url, protocols) => {
  const connection = dial(url, protocols);
  open.add(connection.transport);
  const forget = () => open.delete(connection.transport);
  connection.transport.closed.then(forget, forget);
  return connection;
};

const subscriber = await connectSupervisedSubscriber({ getToken, createTransport });

// Trigger a simulated drop:
for (const transport of open) transport.close({ closeCode: 0, reason: 'simulated drop' });
```

<!-- tab: Python -->

```python
from caprid_relay import connect_supervised_subscriber
from caprid_relay.quic import connect_quic

open_transports = set()


async def create_transport(url: str, protocols: tuple[str, ...]):
    transport = await connect_quic(url, protocols)
    open_transports.add(transport)
    return transport


subscriber = await connect_supervised_subscriber(get_token=get_token, create_transport=create_transport)

# Trigger a simulated drop:
for t in list(open_transports):
    t.close(0, "simulated drop")
```

<!-- /tabs -->

After dropping the transport, supervised connections transition through `reconnecting` back to `live`, opening a new generation and restoring every active `want()` and `publishingRoots()` view.
