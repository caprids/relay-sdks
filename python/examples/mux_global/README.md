# Relay example: publish and subscribe on mux.global (Python)

Start with the [Python quickstart](../../README.md#first-round-trip). It covers a fresh checkout,
runtime requirements, endpoint and credential setup, and a one-object round trip. The default
endpoint is `https://relay.mux.global`. Use `--url` or `RELAY_URL` for another deployment. Both
operations need an authorized Relay JWT.

[demo.py](demo.py) is the complete SDK example, with imports, a matching namespace and track, byte
encoding and decoding, error handling, and cleanup. It runs in asyncio over native QUIC through
aioquic. `asyncio.timeout` bounds the run. `AsyncExitStack` enters the SDK connections' context
managers and closes them on success or failure, and the demo cancels and awaits its background
tasks.

The token comes from `RELAY_TOKEN` or a hidden prompt. The demo never writes it to a file and redacts
it from output. Read the [credential and token exposure guidance](../../README.md#endpoint-and-credentials)
before you collect diagnostics.

## Startup and retry policy

A publisher registers a track and calls `write()`, which **waits for a matching subscriber** to
create relay demand before it sends anything. In combined mode (`both`, the default), the demo starts
both sides. In separate processes, the namespace fields and the track must match exactly.

A subscriber that starts before the namespace announcement is visible can receive request error 16
`unroutable` (`404 Broadcast not found` on relays before 0.15.8). The demo subscribes through a
[supervised subscriber](../../README.md#stay-connected-supervised-connections)'s `want`. The want
retries that rejection with backoff from 250 ms up to 8 seconds, resubscribes after a publisher
restart, and reconnects a lost subscriber connection. The live smoke test does the same. A subscriber
that waits for discovery to report the namespace `announced` avoids the race and needs no retry
([discovery and the announce race](../../README.md#publishing-lifecycle)). Only a rejection that
waiting does not fix, such as an unauthorized track, reaches `on_error` and fails the run. The
overall `--timeout` bounds waiting, retries, and reception.

The write deadline, 15 seconds in the demo, covers the wait for demand and the transport steps after
it. The demo writes through `write_with_retry` in `examples/shared/demand_timeout.py`, which branches
on the error's `code` and `phase`, never on its message:

- `WRITE_TIMEOUT` in phase `waiting-for-demand` means no subscriber asked for the track in time and
  nothing was sent. The demo logs `waiting` ("nothing was sent, writing again") and writes the same
  object again, until the overall `--timeout` cancels the wait.
- A failure in a sending phase (`opening-stream`, `writing`, `closing`) may follow partial or even
  complete delivery. The demo logs `write failed` with the code and phase, does not resend the
  payload, and ends the run as a failure. Whether to send fresh data or start a new group instead is
  the application's decision.
- Any other error, such as `CLOSED` or `NAMESPACE_NOT_SOLICITED` while waiting, ends the run without a
  retry.

A successful write means the transport accepted the object, not that a viewer acknowledged it. The
combined run reports success only after the requested number of objects comes back through the
subscriber.

The demo logs publisher completion (`on_publisher_done`) on its own line, because trailing objects
can still arrive afterward, and does not treat it as all data received. Its subscriber is a supervised
`want`: it retries the announce race with backoff, resubscribes after a publisher restart, and
reconnects, until the requested objects arrive or the run's deadline passes.

## Run it

After the quickstart's setup, run these commands from the repository root. The prompts ask for your
deployment's real values:

```sh
read -r -p 'Relay HTTPS endpoint: ' RELAY_URL
read -r -p 'Authorized namespace (slash-separated): ' RELAY_NAMESPACE
mise run python:example:mux-global -- --url "$RELAY_URL" --namespace "$RELAY_NAMESPACE" --track ticker --count 1 --timeout 30
```

The output includes `published seq=1`, `received ... seq=1`, and `finished published=1 received=1`.
Spacing and ordering vary. A redacted `failed` line and a nonzero exit status mean failure. The
`publisher done` line may report different status codes and reasons on different relay versions, so
do not use it as the success signal.

## Two processes

Set the same `RELAY_URL` and `RELAY_NAMESPACE` in both terminals, and start these commands within 60
seconds of each other, in either order. Each process prompts for its token. A publisher alone waits
for demand. A subscriber alone retries a missing namespace until its deadline.

```sh
mise run python:example:mux-global -- --mode subscribe --url "$RELAY_URL" --namespace "$RELAY_NAMESPACE" --track ticker --count 1 --timeout 60
mise run python:example:mux-global -- --mode publish --url "$RELAY_URL" --namespace "$RELAY_NAMESPACE" --track ticker --count 1 --timeout 60
```

The subscriber succeeds with `finished published=0 received=1` and the publisher with
`finished published=1 received=0`.

## Options

`--help` lists all options without asking for credentials or connecting:

```sh
mise run python:example:mux-global -- --help
```

- `--mode`: `both` (default), `publish`, or `subscribe`.
- `--url`: defaults to `RELAY_URL` or the bare host `https://relay.mux.global`. Prefer an endpoint
  whose path is your token's root, such as `https://relay.mux.global/acme/rooms/42`.
- `--room`: slash-separated fields that `room_endpoint` appends to the `--url` path, such as your
  token's root on the bare host (`--room acme/rooms/42`). `--namespace` is then relative to them. The
  demo refuses fields that a URL path cannot carry unchanged
  ([room-rooted endpoints](../../../spec/behavior.md#room-rooted-endpoints)).
- `--namespace`: slash-separated fields, relative to the `--url` path. On the bare host, include the
  token's root prefix ([namespaces](../../../spec/behavior.md#subscribing)). Without this option, the
  demo appends a random field to `RELAY_NAMESPACE_PREFIX` (or `relay-sdk-example`). The token must
  still authorize that generated namespace. Use a fixed matching namespace for two processes.
- `--track`: `ticker` by default.
- `--count`: objects to publish or receive (default 5).
- `--interval`: milliseconds between objects (default 1000).
- `--timeout`: overall run deadline in seconds (default 60).

Use a fresh authorized namespace for each independent run, so that cached objects from earlier
publishers do not arrive.
