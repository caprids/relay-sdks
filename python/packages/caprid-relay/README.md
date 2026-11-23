# caprid-relay

The Relay Python SDK. `connect_supervised_publisher` and `connect_supervised_subscriber` publish
and subscribe to track objects through Relay, Caprid's Media over QUIC (MoQ) relay service, with
automatic reconnection and credential rotation. Payloads are raw bytes—you need no browser, player,
or media catalog.

## Requirements

- Python 3.11 or later, and `asyncio`.
- Network access to the relay over QUIC on UDP. The default transport is native QUIC (MOQT directly
  on QUIC) through `aioquic`, which installs as a dependency. Pass `create_transport` to supply a
  custom transport.
- A short-lived Relay JWT authorized for the namespace you use (and an explicit `url` only for a
  relay other than the hosted one). The SDK consumes tokens; it does not issue them.

## Install

```sh
uv add caprid-relay
```

`pip install caprid-relay` works too. Optional companion packages (install matching versions):

| Package | When to add it |
| --- | --- |
| `caprid-relay` | Relay client (`connect_supervised_*`, `connect_relay_*`, `caprid_relay.media`) |
| `caprid-moq-media` | LOC framing, MSF catalogs, playout clock, frame sequencer |
| `caprid-moq-media[opus]` | Opus encoding and decoding via PyAV for voice bots |
| `caprid-moq-core` | Low-level MOQT wire codec (installed automatically by `caprid-relay`) |

## Publish and subscribe

```python
import asyncio

from caprid_relay import (
    MoqtObject,
    RelayWantHandlers,
    connect_supervised_publisher,
    connect_supervised_subscriber,
    read_text,
)


async def main(token: str) -> None:
    namespace = ["quickstart", "unique-run"]  # relative to the token's root
    arrived: asyncio.Future[str] = asyncio.get_running_loop().create_future()

    def on_object(obj: MoqtObject) -> None:
        if obj.status == "normal" and not arrived.done():
            arrived.set_result(read_text(obj))

    async with (
        await connect_supervised_publisher(namespace=namespace, get_token=lambda: token) as publisher,
        await connect_supervised_subscriber(get_token=lambda: token) as subscriber,
    ):
        ticker = publisher.register_track("ticker")
        subscriber.want(
            "ticker",
            namespace,
            "ticker",
            RelayWantHandlers(on_object=on_object),
            generations=True,
        )
        await ticker.write_text("hello from caprid-relay")
        print(await arrived)
```

With no `url`, each connection dials the hosted Relay (`https://relay.mux.global`) rooted at the
token's root, so namespaces are relative to that root. Pass `url` for another relay, region, or
room (`room_endpoint` builds one). See
[Connecting & authentication](https://www.caprid.com/docs/relay/connecting/) and
[Low-level connections](https://www.caprid.com/docs/relay/low-level/) for single-shot
`connect_relay_publisher` / `connect_relay_subscriber` usage.

## Stability

Before 1.0, a minor version can break the API, and
[CHANGELOG.md](https://github.com/caprids/relay-sdks/blob/main/python/CHANGELOG.md) says what changed.
`caprid-relay`, `caprid-moq-core`, and `caprid-moq-media` always release together at the same version,
and `caprid-relay` pins `caprid-moq-core` exactly. A release supports Python 3.11 or later, and MOQT
draft 22 (`moqt-22`) with draft 20 (`moqt-20`) as an offered fallback, against moq-relay 0.15.4 or
later.

## Documentation

- [Quickstart](https://www.caprid.com/docs/getting-started/quickstart/) and
  [Relay guides](https://www.caprid.com/docs/relay/connecting/): connecting, pub/sub, discovery,
  switching tracks, control & telemetry, errors & recovery, and Python media.
- [Python API reference](https://www.caprid.com/docs/reference/python/caprid_relay/): generated from the source.
- [Shared specification](https://www.caprid.com/docs/spec/): protocol pins, behavior contract, and
  test vectors every Relay SDK implements.
- **AI agents**: machine-readable docs at [llms.txt](https://www.caprid.com/llms.txt) and the
  installable Caprid skill at [SKILL.md](https://www.caprid.com/docs/agents/SKILL.md).
