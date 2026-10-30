# Transport adapter conformance suite

The suite checks a transport adapter against the contract in `spec/transport-adapter.md`. It is written once, against
`MoqtTransport` and the factory result, and runs against every target. Each case is named after the rule it checks
(`TA-n …`), and every MUST in the contract has at least one case.

| File | What it is |
| --- | --- |
| `suite.js` | The cases, `planCase`, `runCase` and `runSuite`. Runtime-neutral: it uses only APIs browsers and Node both have. |
| `vitest.js` | `describeConformance(target)`: one vitest test per case. |
| `pair.test.js` | The suite against the in-memory pair, in `pnpm test` and so in `pnpm check`. |
| `targets/pair.js` | The in-memory pair as a target, with an in-process peer. |
| `targets/chromium.js` | The browser's native `WebTransport` as a target, run in a page. Its peer is the Python accepting end. |
| `targets/relay-quic.js` | `@caprid/relay-quic`, the Node adapter over native QUIC, as a target. Its peer is the Python accepting end with `--framing native-quic`. |
| `targets/relay-quic-lifetime.js` | The child Node process behind that target's TA-25 and TA-26 checks, and its check that a cancelled dial (TA-11) lets the process exit. |
| `targets/websocket.js` | `@caprid/relay-websocket`, QMux over WebSocket (#321), as a target. Runtime-neutral: the same target runs in Node and in a browser page. Its peer is the Node accepting end, `scripts/websocket-peer.js`. |
| `targets/websocket-lifetime.js` | The child Node process behind the WebSocket target's TA-25, TA-26 and cancelled-dial checks in Node. |
| `targets/lifetime.js` | Runs a lifetime child and checks how it ended, for both Node adapters. |
| `targets/remote-peer.js` | The peer methods as requests to the Python accepting end, for both targets that use it. |
| `relay-quic.conformance.js` | The suite against `@caprid/relay-quic` under vitest, plus its trust checks (`ca`, an untrusted certificate, bad options) and its incoming stream credit checks (the credit granted by default and by option, and more than 100 streams held open at once), and its qlog check (one trace per connection in `qlogDir`). |
| `websocket.conformance.js` | The suite against `@caprid/relay-websocket` in Node under vitest, plus `withWebSocketFallback` where Node has no WebTransport. `vitest.websocket.config.js` makes the peer's certificate first, so Node trusts it from the start. |
| `../scripts/test-conformance-websocket.mjs` | The browser launcher for the WebSocket target: Chromium, Firefox and WebKit through Playwright. |
| `../scripts/websocket-peer.js` | The accepting end for QMux over WebSocket: `@moq/qmux`'s accepting role over `ws`, over TLS, speaking the Python accepting end's JSON lines. |
| `../scripts/test-conformance.mjs` | The Chromium launcher: it starts the accepting end, serves the suite to a Playwright page, runs it and prints the results. |
| `../scripts/conformance-peer.js` | Starts the Python accepting end for either launcher. |
| `python/…/relay_helpers/conformance_peer.py` | The scriptable accepting end: aioquic, out of process, driven over JSON lines. |
| `python/…/tests/test_transport_conformance.py` | The suite ported to pytest, against the Python dialing ends (#230). It drives the same accepting end through `relay_helpers/conformance_client.py`. |

## Running it

- `pnpm exec vitest run conformance` runs it against the pair. It is also part of `pnpm test`.
- `pnpm test:conformance` (`mise run ts:test:conformance`) runs it in headless Chromium against the Python accepting
  end. It needs the Python workspace (`mise run python:install`) and Playwright's Chromium
  (`pnpm exec playwright install chromium`). It takes about 15 s. `CONFORMANCE_GREP=TA-15` runs only the cases whose
  names contain that text. CI runs it as the TypeScript workflow's `conformance` job.
- `pnpm test:conformance:websocket` (`mise run ts:test:conformance:websocket`) runs it in Node against
  `@caprid/relay-websocket` and the Node accepting end. It needs only Node and `openssl`, and takes about 15 s.
  `pnpm test:conformance:websocket:browsers` runs it in Chromium, Firefox, and WebKit, with Playwright's browsers
  installed. `BROWSERS=chromium,webkit` picks browsers, and `CONFORMANCE_GREP` works as above. The pages ignore the
  peer's self-signed certificate, because a page cannot pin a certificate for a WebSocket. CI runs both as the `conformance-websocket` job.
- `pnpm test:conformance:node` (`mise run ts:test:conformance:node`) runs it in Node against `@caprid/relay-quic` and the
  Python accepting end in its native QUIC mode. It builds the native addon first, with the Rust toolchain the mise
  task pins, and needs the Python workspace. After the build it takes about 25 s, most of it the TA-9 connection-loss
  case waiting out the idle timeout. With the addon built,
  `pnpm exec vitest run --config conformance/vitest.relay-quic.config.js -t TA-15` runs only the matching cases. CI
  runs it as the `conformance-node` job, which also checks that `packages/relay-quic/src/native.d.ts` matches the
  Rust.

## Targets

A target is a plain object:

| Member | Meaning |
| --- | --- |
| `name` | Shown in the test names. |
| `framing` | `'webtransport'`, `'native-quic'` or `'websocket'`: what TA-1 expects the factory to declare. |
| `capabilities` | What the target's peer and runtime can do, listed below. A case that `needs` one the target lacks is skipped. |
| `gaps` | For each missing capability, the reason the skip reports. |
| `expectations` | Keyed by case name: `{ skip: '<reason>' }` where a rule does not apply, or `{ fails: '<reason>' }` where the target is known not to meet it. A `fails` reason cites a Known deviation in the spec, or the issue that closes the gap. |
| `resetReason(code)` | Optional. The reason the core hands `abort()` or `cancel()` for `code`. A browser target uses the core's `streamResetReason`, since the native `WebTransport` reads codes only from a `WebTransportError` (TA-17). |
| `session(options)` | Starts a peer and returns `{ dial, peer, dispose }`. `options` are the case's `session` options: `handshake` (`'accept'`, `'hold'` or `'refuse'`), `select` (`'first'`, `'none'` or `'unoffered'`) and `maxStreams`. |
| `checkProcessLifetime(kind)` | Only with the `process` capability. Runs the TA-25 (`'keeps-alive'`) and TA-26 (`'exits-after-close'`) checks in a child process. |
| `host` | Only on native QUIC. The host TA-3 expects as the TLS server name. |

`dial({ path, protocols })` calls the adapter's factory with the target's `https://` origin plus `path` (default
`/conformance`) and `protocols` (default `['moqt-22']`). It returns the factory result plus the `url` it dialled.

### Capabilities

| Capability | Meaning |
| --- | --- |
| `webtransport`, `native-quic`, `websocket` | The target's framing. |
| `selectProtocol` | The peer can select no protocol, or one that was not offered (TA-7). |
| `holdHandshake` | The peer can hold the handshake until `releaseHandshake()` (TA-7, TA-11). |
| `refuseHandshake` | The peer can refuse the session (TA-9). |
| `quicClose` | The peer can close the connection with a QUIC-layer CONNECTION_CLOSE (TA-9, TA-12). |
| `connectionLoss` | The peer can vanish without a close (TA-9). |
| `pause` | The peer can drop every datagram from the dialer until it resumes, so it neither receives nor acknowledges (TA-14 to TA-16). |
| `maxStreams` | The peer can limit the dialer's streams (TA-21). |
| `largeCodes` | The peer can reset with a code above 2⁵³−1 (TA-22; native QUIC only). |
| `wireCodes` | The peer can put a stream code on the wire as it is, and report the code the dialer's reset or stop carried there (TA-27; native QUIC only). |
| `process` | The adapter runs in a process the target can watch (TA-25, TA-26). |

## The peer

Cases drive the accepting end only through `ctx.peer`. Every method returns a promise. Stream ids are opaque, and
bytes are a `Uint8Array` going in and an array of numbers coming out, so a remote peer can carry them as JSON.

| Method | Meaning |
| --- | --- |
| `acceptUni()`, `acceptBi()` | The id of the next stream the dialer opened. |
| `openUni()`, `openBi()` | Open a stream to the dialer and return its id. |
| `openUnis(count, bytes)` | Open `count` unidirectional streams to the dialer and write `bytes` on each, finishing none, so the dialer must let them stay open at once. Returns the ids. |
| `dialerStreamCredit()` | Resolves `{ uni, bidi }`: the stream limits the dialer has granted the peer so far (its MAX_STREAMS, RFC 9000 §4.6). |
| `write(id, bytes)`, `finish(id)` | Write on the peer's send half, then send the FIN. |
| `reset(id, code, { wire })`, `stop(id, code, { wire })` | RESET_STREAM the peer's send half, or STOP_SENDING its receive half. With `wire` (`wireCodes`), `code` goes on the wire as it is, unencoded, and may be a decimal string above 2⁵³−1 (`largeCodes`). |
| `read(id, { bytes, discard })` | Read until the FIN, a reset, or `bytes` bytes. Resolves `{ bytes, byteLength, fin, resetCode }`. `resetCode` is the dialer's code, or `null`. With `discard`, `bytes` comes back empty. |
| `stopped(id)` | Resolves with the code once the dialer stops the peer's send half. |
| `wireCode(id)` | With `wireCodes`: the code the dialer's last reset or stop of the stream carried on the wire, as a decimal string, or `null`. |
| `close(code, reason)` | The application close: CLOSE_WEBTRANSPORT_SESSION, or CONNECTION_CLOSE 0x1d. |
| `closed()` | Resolves `{ closeCode, reason }` once the dialer closed the session. |
| `quicClose()`, `loseConnection()`, `pause()`, `resume()`, `releaseHandshake()` | Need the matching capability. |
| `observed()` | What the peer saw of the dial: `{ path, protocols }` on a WebTransport session (the CONNECT's path and query, and `WT-Available-Protocols`), `{ serverName, protocols }` on native QUIC (the SNI and the ALPN list). |

## Results

`runCase` reports each case as one of:

- `passed`
- `failed`
- `skipped`, with the reason
- `expected-failure`: it failed as `fails` said it would
- `unexpected-pass`: it passed although `fails` says it should not. Promote it by removing the expectation.
- `unmet`: a SHOULD that failed. It is reported but does not fail the run.

Under vitest:

- `failed` and `unexpected-pass` fail the test.
- `skipped` is a skipped test whose name gives the reason.
- `unmet` is skipped at run time with what went wrong.

The suite owns these results. The spec does not keep a per-adapter matrix.

## Not covered here

TA-6 is the core's rule, not the adapter's: the core sends PATH and AUTHORITY in CLIENT_SETUP. The core's SETUP tests
and `spec/vectors/` cover it (#191).

## The Python accepting end

`conformance_peer.py` prints `{"event": "listening", "port", "certificateHash", "certificatePem"}` once it listens. It
then takes one JSON request per stdin line, `{"id", "method", "params"}`, and answers each on stdout with
`{"id", "result"}` or `{"id", "error"}`. Requests may overlap. Its certificate is a 10-day P-256 leaf, so a browser
accepts it through `serverCertificateHashes`.

- `prepare({ handshake, select, maxStreams })` sets up the next connection and forgets the previous session.
  `dispose()` closes the current connection.
- The other methods are the peer methods above, with these differences in the wire spelling:
  - parameters are named: `{ stream, data }` for `write`, `{ stream, count, discard }` for `read`, and
    `{ code, reason }` for `close`
  - `observed()` is `observedDial`
- With `maxStreams`, the dialer gets that many streams of each kind beyond HTTP/3's own. Credit returns one stream at
  a time, as the peer reads a dialer's stream to its FIN. aioquic would otherwise double the limit by itself.
- `pause()` drops every datagram from the dialer, so nothing is acknowledged.
- `close()` while paused sends CLOSE_WEBTRANSPORT_SESSION at once. Otherwise the session waits first for the dialer to
  acknowledge what is in flight, as the SDK's own close does (#209), and a paused peer would only wait out that bound.
- `loseConnection()` drops traffic both ways until the dialer's idle timeout fires. The peer's own idle timeout is
  10 s.
- `quicClose()` sends CONNECTION_CLOSE 0x1c.

With `--framing native-quic` it serves MOQT straight on QUIC instead (`relay_helpers/native_quic.py`), behind the same
methods:

- It listens on `::`, which takes IPv4 too, so a dialer that resolves `localhost` to either address reaches it.
- It selects the protocol in TLS, from the ClientHello's ALPN list: the first offered, none, or `moqt-unoffered`.
  `observedDial` reports the ClientHello's SNI and ALPN list as `{ serverName, protocols }`.
- `hold` holds every datagram from the dialer until `releaseHandshake()`, and `refuse` fails the handshake with a TLS
  handshake_failure alert.
- `maxStreams` is the dialer's whole stream credit, with no HTTP/3 streams to add.
- Stream codes cross in the HTTP/3 encoding, as with moq-relay (TA-27): `reset` and `stop` encode the code, and the
  peer decodes the dialer's, so a code outside the encoding reads as `null`, the way the relay reads it. With `wire`,
  `reset` and `stop` put the code on the wire as given, up to 2⁶²−1, and `wireCode` reports the dialer's code as it
  arrived.
- `close()` is an application CONNECTION_CLOSE (0x1d), and `closed()` reports the dialer's.

## The WebSocket accepting end

`scripts/websocket-peer.js` takes the same requests as the Python accepting end, over the same JSON lines, for QMux
over WebSocket. `scripts/conformance-peer.js` starts it for `framing: 'websocket'`, with a fresh self-signed certificate
unless `WEBSOCKET_PEER_CERTIFICATE` and `WEBSOCKET_PEER_KEY` name one. It listens on `::`, which takes IPv4 too.

- `select` picks the first subprotocol offered, none (`Sec-WebSocket-Protocol` absent), or `qmux-01.moqt-unoffered`;
  `observedDial` reports the upgrade's path and query and the subprotocols offered, as sent (`qmux-01.moqt-22`).
- `hold` holds every upgrade until `releaseHandshake()`; `refuse` answers the upgrade with 401.
- `maxStreams` is the dialer's MAX_STREAMS of each kind. Credit returns as the peer accepts streams.
- `pause()` stops reading the TCP connection, so flow control and TCP hold the dialer; nothing is acknowledged either
  way, since QMux has no acknowledgement.
- `quicClose()` sends a QMux CONNECTION_CLOSE (0x1c) and closes the WebSocket; `loseConnection()` destroys the TCP
  connection with no close of either kind.
- It shares `@moq/qmux` with the adapter, so it checks the wrapper against the contract, not the library against
  moq-relay. The live scenario (`pnpm test:relay` over WebSocket) does that.

## Adding a target

A target needs a peer that implements the methods above, and a runner for its runtime. In Node,
`relay-quic.conformance.js` and `websocket.conformance.js` run under vitest, as the pair does, each with its own config
so that `pnpm test` leaves them out. Another Node adapter can reuse the shape of `targets/relay-quic.js` and the native
QUIC peer as they are. In a browser, `../scripts/test-conformance.mjs` and `../scripts/test-conformance-websocket.mjs`
serve the suite to a Playwright page.
