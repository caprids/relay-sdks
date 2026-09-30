# Relay example: publish and subscribe on mux.global

Start with the [JavaScript quickstart](../../GUIDE.md#first-round-trip) for a fresh checkout,
runtime requirements, local package installation, endpoint and credential setup, and a one-object
round trip. These packages are unpublished. The default endpoint is `https://sjc.relay.mux.global`;
use `--url` or `RELAY_URL` for another deployment. Both operations require an authorized Relay JWT.

[demo.js](demo.js) is the complete SDK example with imports, matching namespace and track,
byte encoding/decoding, error handling, and cleanup. [run.mjs](run.mjs) serves the built SDK and demo
on localhost with an import map, drives headless Chromium through Playwright, and prints its logs.
Node runs the launcher; the SDK's default transport requires browser WebTransport.

The token comes from `RELAY_TOKEN` or a hidden prompt. The CLI never writes it to a file and redacts
it from output. Read the [credential and token exposure guidance](../../GUIDE.md#endpoint-and-credentials)
before collecting diagnostics.

## Startup and retry policy

A publisher registers a track and calls `write()`, which **waits for a matching subscriber** to
create relay demand before it sends anything. In combined mode (`both`, the default), the demo
starts both sides. In separate processes, the namespace fields and track must match exactly. A
subscriber started before the namespace announcement is visible can receive request error 16
`unroutable` (`404 Broadcast not found` on relays before 0.15.8); the demo retries that rejection
with backoff starting at 500 ms and capped at 2 seconds, through `subscribeWithRetry` in
`examples/shared/announce-race.js`, the one copy of that policy the demo and the live smoke test
share. In combined mode it first awaits `publisher.announced`, the announce reaching the wire, which
removes most of the race; the relay still needs about a round trip after it. Other rejections fail
the run. The overall `--timeout` bounds waiting, retries, and reception.

The write deadline (15 seconds in the demo) covers that wait and the transport steps after it.
The demo writes through `writeWithRetry` in `examples/shared/demand-timeout.js`, which branches on
the error's structured `code` and `phase`, never on its message:

- `WRITE_TIMEOUT` in phase `waiting-for-demand` means no subscriber asked for the track in time and
  nothing was sent. The demo logs `waiting` ("nothing was sent, writing again") and writes the same
  object again, until the overall `--timeout` cancels the wait.
- A failure in a sending phase (`opening-stream`, `writing`, `closing`) may follow partial or even
  complete delivery. The demo logs `write failed` with the code and phase, does not resend the
  payload, and ends the run through its failure path. Choosing fresh data or a new group instead is
  the application's decision.
- Anything else, such as `CLOSED` or `NAMESPACE_NOT_SOLICITED` while waiting, ends the run without a
  retry.

A successful write means transport acceptance, not viewer acknowledgment. The combined run waits
for the requested object count to come back through the subscriber before reporting success.

Publisher completion (`onPublisherDone` / `on_publisher_done`) is logged independently: trailing objects can still
arrive afterward. The demo observes connection closure, does not treat publisher completion as
all data received, and does not automatically resubscribe on completion. Additional readiness and
termination APIs are tracked in [#68](https://github.com/muxinc/relay-sdks/issues/68).

This example does not reconnect after the connection ends, for example after a network outage
longer than about 5 s in Chromium. See [publisher recovery](../../../spec/publisher-recovery.md)
for what a replacement publisher can and cannot restore.

## Run it

After the quickstart's setup, run from the repository root. The launcher drives Playwright's
Chromium, installed once with `mise exec -- pnpm --dir ts exec playwright install chromium`. On
Linux, if Chromium reports missing system libraries, provision Playwright's system dependencies for
your machine (`playwright install-deps chromium`) first.

These prompts request actual deployment values, not example credentials:

```sh
read -r -p 'Relay HTTPS endpoint: ' RELAY_URL
read -r -p 'Authorized namespace (slash-separated): ' RELAY_NAMESPACE
mise run ts:example:mux-global -- --url "$RELAY_URL" --namespace "$RELAY_NAMESPACE" --track ticker --count 1 --timeout 30
```

Expected output includes `published seq=1`, `received ... seq=1`, and
`finished published=1 received=1` (spacing and ordering vary). A redacted `failed` line and nonzero
exit status mean failure. `publisher done` may report different status codes/reasons across relay
versions; do not use it as the round-trip success signal.

## Two processes

Set the same `RELAY_URL` and `RELAY_NAMESPACE` in both terminals and start these within 60 seconds
of each other, in either order. Each process prompts for its token. A publisher alone waits for
demand; a subscriber alone retries a missing namespace until its deadline.

```sh
mise run ts:example:mux-global -- --mode subscribe --url "$RELAY_URL" --namespace "$RELAY_NAMESPACE" --track ticker --count 1 --timeout 60
mise run ts:example:mux-global -- --mode publish --url "$RELAY_URL" --namespace "$RELAY_NAMESPACE" --track ticker --count 1 --timeout 60
```

Success is `finished published=0 received=1` for the subscriber and
`finished published=1 received=0` for the publisher.

## Options

`--help` lists all options without requesting credentials or connecting:

```sh
mise run ts:example:mux-global -- --help
```

- `--mode`: `both` (default), `publish`, or `subscribe`.
- `--url`: defaults to `RELAY_URL` or `https://sjc.relay.mux.global`.
- `--namespace`: slash-separated fields, including the token's root prefix. Without it, the CLI
  appends a random field to `RELAY_NAMESPACE_PREFIX` (or `relay-sdk-example`). That generated
  namespace must still be authorized by the token. Use a fixed matching namespace for two processes.
- `--track`: `ticker` by default.
- `--count`: objects to publish/receive (default 5).
- `--interval`: milliseconds between objects (default 1000).
- `--timeout`: overall run deadline in seconds (default 60).

Use a fresh authorized namespace for independent runs to avoid cached objects from prior publishers.
