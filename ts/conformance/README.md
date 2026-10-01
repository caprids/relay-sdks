# Transport adapter conformance suite

The suite checks a transport adapter against the contract in `spec/transport-adapter.md`. It is written once, against
`MoqtTransport` and the factory result, and run against every target. Each case is named after the rule it checks
(`TA-n …`), and every MUST in the contract has at least one case.

| File | What it is |
| --- | --- |
| `suite.js` | The cases, `planCase`, `runCase` and `runSuite`. Runtime-neutral: it uses only APIs browsers and Node both have. |
| `vitest.js` | `describeConformance(target)`: one vitest test per case. |
| `pair.test.js` | The suite against the in-memory pair, in `pnpm test` and so in `pnpm check`. |
| `targets/pair.js` | The in-memory pair as a target, with an in-process peer. |

## Running it

`pnpm exec vitest run conformance` runs it against the pair. It is also part of `pnpm test`.

## Targets

A target is a plain object:

| Member | Meaning |
| --- | --- |
| `name` | Shown in the test names. |
| `framing` | `'webtransport'` or `'native-quic'`: what TA-1 expects the factory to declare. |
| `capabilities` | What the target's peer and runtime can do, listed below. A case that `needs` one the target lacks is skipped. |
| `gaps` | For each missing capability, the reason the skip reports. |
| `expectations` | Keyed by case name: `{ skip: '<reason>' }` where a rule does not apply, or `{ fails: '<reason>' }` where the target is known not to meet it. A `fails` reason cites a Known deviation in the spec, or the issue that closes the gap. |
| `resetReason(code)` | Optional. The reason the core hands `abort()` or `cancel()` for `code`. A browser target uses the core's `streamResetReason`, since the native `WebTransport` reads codes only from a `WebTransportError` (TA-17). |
| `session(options)` | Starts a peer and returns `{ dial, peer, dispose }`. `options` are the case's `session` options: `handshake` (`'accept'`, `'hold'` or `'refuse'`), `select` (`'first'`, `'none'` or `'unoffered'`) and `maxStreams`. |
| `checkProcessLifetime(kind)` | Only with the `process` capability. Runs the TA-25 (`'keeps-alive'`) and TA-26 (`'exits-after-close'`) checks in a child process. |
| `host` | Only on native QUIC. The host TA-3 expects as the TLS server name. |

`dial({ path, protocols })` calls the adapter's factory with the target's `https://` origin plus `path` (default
`/conformance`) and `protocols` (default `['moqt-20']`). It returns the factory result plus the `url` it dialled.

### Capabilities

| Capability | Meaning |
| --- | --- |
| `webtransport`, `native-quic` | The target's framing. |
| `selectProtocol` | The peer can select no protocol, or one that was not offered (TA-7). |
| `holdHandshake` | The peer can hold the handshake until `releaseHandshake()` (TA-7, TA-11). |
| `refuseHandshake` | The peer can refuse the session (TA-9). |
| `quicClose` | The peer can close the connection with a QUIC-layer CONNECTION_CLOSE (TA-9, TA-12). |
| `connectionLoss` | The peer can vanish without a close (TA-9). |
| `pause` | The peer can stop acknowledging until it resumes (TA-14 to TA-16). |
| `maxStreams` | The peer can limit the dialer's streams (TA-21). |
| `largeCodes` | The peer can reset with a code above 2⁵³−1 (TA-22; native QUIC only). |
| `process` | The adapter runs in a process the target can watch (TA-25, TA-26). |

## The peer

Cases drive the accepting end only through `ctx.peer`. Every method returns a promise. Stream ids are opaque, and
bytes are a `Uint8Array` going in and an array of numbers coming out, so a remote peer can carry them as JSON.

| Method | Meaning |
| --- | --- |
| `acceptUni()`, `acceptBi()` | The id of the next stream the dialer opened. |
| `openUni()`, `openBi()` | Open a stream to the dialer and return its id. |
| `write(id, bytes)`, `finish(id)` | Write on the peer's send half, then send the FIN. |
| `reset(id, code)`, `stop(id, code)` | RESET_STREAM the peer's send half, or STOP_SENDING its receive half. `code` may be a decimal string above 2⁵³−1 with `largeCodes`. |
| `read(id, { bytes })` | Read until the FIN, a reset, or `bytes` bytes. Resolves `{ bytes, fin, resetCode }`. `resetCode` is the dialer's code, or `null`. |
| `stopped(id)` | Resolves with the code once the dialer stops the peer's send half. |
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

## Adding a target

A target needs a peer that implements the methods above, and a runner for its runtime.

- **Node (`@mux/relay-quic`, #194):**
  - It runs under vitest, like the pair: a `relay-quic.test.js` that calls `describeConformance` with a native QUIC
    target.
  - Its peer is the same scriptable Python accepting end that the Chromium target uses, in a native QUIC mode.
  - It adds `process` and implements `checkProcessLifetime` with a child Node script.
- **Chromium:** it runs the cases in the page with `runSuite`, against the scriptable Python accepting end.
