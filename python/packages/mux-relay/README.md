# mux-relay

The Relay Python SDK. `connect_relay_subscriber` subscribes to track objects and
`connect_relay_publisher` publishes them, through Relay, Mux's Media over QUIC (MoQ) relay service.
Payloads are bytes the SDK does not interpret; no browser, player, or media catalog is needed.

## Requirements

- Python 3.11+ and asyncio.
- The default transport is native QUIC (MOQT directly on QUIC) through aioquic, installed as a dependency. The
  endpoint and network must allow QUIC/UDP traffic. Other transports can be passed as
  `create_transport`.
- A Relay endpoint and a short-lived Relay JWT authorized for the namespace you use. The SDK
  consumes tokens; it does not issue them.

## Install

The package is unpublished and its name is provisional. Install it together with its local
`mux-moq-core` dependency from a checkout of this repository, as described in
[Use the local packages in your application](../../README.md#use-the-local-packages-in-your-application).

## Publish and subscribe

```python
import asyncio

from mux_relay import (
    MoqtObject,
    RelaySubscriptionHandlers,
    connect_relay_publisher,
    connect_relay_subscriber,
    read_text,
)


async def main(token: str) -> None:
    url = "https://sjc.relay.mux.global"
    namespace = ["team", "quickstart", "unique-run"]  # a namespace your token authorizes
    arrived: asyncio.Future[str] = asyncio.get_running_loop().create_future()

    def on_object(obj: MoqtObject) -> None:
        if obj.status == "normal" and not arrived.done():
            arrived.set_result(read_text(obj))

    async with (
        await connect_relay_publisher(url, namespace=namespace, get_token=lambda: token) as publisher,
        await connect_relay_subscriber(url, get_token=lambda: token) as subscriber,
    ):
        ticker = publisher.register_track("ticker")
        await publisher.announced
        subscription = subscriber.subscribe(namespace, "ticker", RelaySubscriptionHandlers(on_object=on_object))
        await subscription.subscribed  # a fresh namespace can briefly reject this with error 16; retry it
        await ticker.write_text("hello from mux-relay")  # waits for the subscriber's demand
        print(await arrived)
```

The [guide's first round trip](../../README.md#first-round-trip) runs the complete version of this,
with credential handling, the subscribe retry, a deadline, and cleanup on failure.

## Stability

Before 1.0, a minor version can break the API, and [CHANGELOG.md](../../CHANGELOG.md) says what
changed. `mux-relay`, `mux-moq-core` and `mux-moq-media` always release together at the same
version, and `mux-relay` pins `mux-moq-core` exactly. A release supports Python 3.11 or later, and
MOQT draft 22 (`moqt-22`), with draft 20 (`moqt-20`) as an offered fallback, against moq-relay
0.15.4 or later.

## Documentation

- [Python guide](../../README.md): the quickstart, subscription and publishing lifecycles, async
  consumption, the aioquic transport, and the Video.js media example.
- API reference: run `mise run python:docs` to build it from the source.
- [Errors and recovery](../../README.md#errors-and-recovery): stable `RelayError.code` and `phase`
  values, preserved exception subclasses and transport causes, and recovering without assuming
  delivery or safe replay.
- [Shared specification](../../../spec/README.md): the protocol pins, behavior contract, and test
  vectors every Relay SDK implements.
