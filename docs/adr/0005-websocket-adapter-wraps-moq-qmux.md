---
status: accepted
date: 2026-10-17
---

# The WebSocket transport adapter wraps @moq/qmux in its own package, and the fallback is opt-in

> The package names below predate the rename in [#222](https://github.com/muxinc/relay-sdks/issues/222): `@mux/*` is now `@caprid/*` on npm, `mux-*` is now `caprid-*` on PyPI, and `mux_relay` is now `caprid_relay`.

A browser on a network that blocks UDP, or without a WebTransport that can negotiate protocols (Safari), cannot reach
the relay over WebTransport. Both fleets already serve MOQT on QMux over WebSocket on TCP 443 (moq-relay's
`[web]` listener, `rs/moq-relay/src/websocket.rs`). For #321 we decided, with Adam on 2026-10-17:

- **QMux comes from `@moq/qmux`**, pinned to an exact version (0.3.3), and the adapter wraps it to the transport
  adapter contract (`spec/transport-adapter.md`). It is about 3,500 lines (MIT or Apache-2.0) by the author of
  moq-relay's own QMux crate, so the two ends are developed together, and it already has the W3C `WebTransport` shape
  the seam resembles.
- **It ships as its own package, `@mux/relay-websocket`**, exporting `createWebSocketTransport()` and
  `withWebSocketFallback()`. `@mux/relay` gains no dependency, and stays the package with the core alone; the
  application passes the adapter as `createTransport`, as it does `@mux/relay-quic`'s.
- **The fallback is opt-in.** The default transport stays the browser's WebTransport, and nothing falls back unless
  the application passes `withWebSocketFallback()`. It dials WebTransport first and WebSocket after a failed handshake
  or one that outlasts a timeout (4 s by default), one dial after the other.
- **The factory declares a third framing, `'websocket'`**, which sends the same SETUP as `'webtransport'` (no PATH or
  AUTHORITY: the upgrade request carries the URL), rather than passing itself off as a WebTransport session.

## Considered options

- **Depend on `@moq/qmux`, wrapped (chosen).** The least code to own. The wrapper checks reset codes (TA-17), settles a
  blocked write at once when the stream is aborted or stopped (TA-16, TA-20), reports session ends with the contract's
  shape (TA-12, TA-22, TA-24), and refuses a selection it did not offer (TA-7). What it cannot fix is recorded as Known
  deviations: a create waits for stream credit (TA-21), a peer reset behind an unread chunk lands at the next read
  (TA-19), and a reset behind a blocked write goes out once that write finishes (TA-16).
- **Copy a pinned `@moq/qmux` into the repository and patch it**, as `moq-core` was copied from Video.js. It would
  close those deviations, at the cost of maintaining a fork of a library that changes with the relay.
- **Write our own QMux**, about 1,500 to 2,000 lines. Full control of every rule, but the longest path, and one more
  QMux implementation to test against the relay's.

None of them can meet TA-15: QMux over TCP has no acknowledgement an application can see, so a send stream's close
resolves at hand-over to the socket. That is the binding's, not the library's.

For packaging, putting the adapter inside `@mux/relay` would have spared callers an import, but given the SDK its first
third-party runtime dependency. For the default, falling back automatically would connect users on UDP-blocked networks
without a code change, but silently over TCP, with head-of-line blocking, and with a refused token reported as
`TRANSPORT_ERROR` (the relay answers it with HTTP 401 at the upgrade, which browsers do not expose).

## Consequences

- `scripts/check-boundaries.mjs` allows exactly one third-party runtime dependency, `@moq/qmux` at its pinned version,
  and only in `@mux/relay-websocket`. Moving the pin is a deliberate change, made with the conformance suite.
- The conformance suite gains a Node accepting end for QMux over WebSocket (`ts/scripts/websocket-peer.js`, `@moq/qmux`
  over `ws`), and runs the adapter in Node, Chromium, Firefox and WebKit. That peer and the adapter share a library, so
  the live scenario against moq-relay is the check against an independent implementation.
- The Python SDK accepts `framing="websocket"`, so the shared SETUP vectors hold in both languages, but ships no
  WebSocket adapter.
- Revisit copying the library in if its deviations start to matter, for example if a relay grants so few streams that
  TA-21's wait shows.
