# Relay example: publish and subscribe on mux.global

`demo.js` is the SDK usage. It connects a publisher with `connectRelayPublisher`, registers a
`ticker` track, connects a subscriber with `connectRelay` to the same track, publishes JSON objects
one second apart, and prints each object as it comes back through the relay. It decodes the JSON
itself; the SDK only delivers bytes.

`run.mjs` exists because Node has no WebTransport. It serves the built SDK plus `demo.js` on
localhost, drives headless Chromium through Playwright, and prints the page's log lines. The token is
read from `RELAY_TOKEN` or a hidden prompt and is redacted from output.

## Run it

```sh
cd ts && mise exec -- pnpm exec playwright install chromium   # once
RELAY_TOKEN=... mise run ts:example:mux-global -- --namespace mattypoo/relay-sdk-example
```

The task builds first and runs from `ts/`; `cd ts && mise exec -- pnpm example:mux-global ...` is
equivalent. Use the namespace prefix your token authorizes; the SDK sends
namespace fields verbatim. Without `--namespace`, the run uses `RELAY_NAMESPACE_PREFIX` (or
`relay-sdk-example`) plus a random field, so repeated runs never share relay state. The default
endpoint is `https://sjc.relay.mux.global`; override it with `--url` or `RELAY_URL`.

Expected output:

```
starting             mode=both namespace=mattypoo/relay-sdk-example track=ticker count=5
publisher connected  url=https://sjc.relay.mux.global/ namespace=mattypoo/relay-sdk-example
subscriber connected url=https://sjc.relay.mux.global/
subscribed           namespace=mattypoo/relay-sdk-example track=ticker alias=0
published            seq=1 bytes=84
received             group=0 bytes=84 seq=1 ageMs=38
...
finished             published=5 received=5
```

## Two processes

Run the subscriber and the publisher in separate terminals on a fixed namespace. Start the
subscriber first; a publisher `write()` waits for relay demand and retries after a timeout until a
subscriber appears.

```sh
RELAY_TOKEN=... mise run ts:example:mux-global -- --mode subscribe --namespace mattypoo/relay-sdk-example/room
RELAY_TOKEN=... mise run ts:example:mux-global -- --mode publish   --namespace mattypoo/relay-sdk-example/room
```

Options: `--track`, `--count`, `--interval` (ms), `--timeout` (s). `--help` lists them.
