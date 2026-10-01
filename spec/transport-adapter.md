# Transport adapter contract

A Relay connection runs over a **transport adapter** (see `CONTEXT.md`): the browser's native
`WebTransport`, the Python aioquic WebTransport client, the `@mux/relay-quic` native QUIC adapter
for Node, and the in-memory pairs the tests use. This document states what every adapter must do
for the SDK core to behave as `behavior.md` says. Until now that was only the TypeScript
`MoqtTransport` interface plus behaviour the core relied on without writing it down.

## Sources of truth

The protocols underneath are the authority. This contract adds no wire behaviour and overrides
nothing in them. It only narrows what an adapter exposes at the seam the core depends on:

- [draft-ietf-moq-transport-20] (MOQT, wire identifier `moqt-20`, pinned in `protocol.json`)
- [RFC 9000] (QUIC streams, RESET_STREAM, STOP_SENDING, MAX_STREAMS, acknowledgement)
- [RFC 7301] (ALPN)
- [draft-ietf-webtrans-http3] and the [W3C WebTransport] API, for a WebTransport session

Every rule cites the upstream section it narrows. If a rule seems to conflict with one of these
specifications, the specification wins and the rule is a bug in this document.

[draft-ietf-moq-transport-20]: https://datatracker.ietf.org/doc/draft-ietf-moq-transport/20/
[RFC 9000]: https://www.rfc-editor.org/rfc/rfc9000
[RFC 7301]: https://www.rfc-editor.org/rfc/rfc7301
[draft-ietf-webtrans-http3]: https://datatracker.ietf.org/doc/draft-ietf-webtrans-http3/
[W3C WebTransport]: https://www.w3.org/TR/webtransport/

## Conventions

The key words MUST, MUST NOT, SHOULD, SHOULD NOT and MAY are to be interpreted as described in
BCP 14 ([RFC 2119], [RFC 8174]) when, and only when, they appear in all capitals.

Rules are numbered `TA-n`. The numbers are stable: a rule that is withdrawn keeps its number, marked
withdrawn, and new rules take new numbers. The conformance suite (#193) names each test after the
rule it checks. Each rule may be followed by:

- **Why**: the core behaviour that depends on it (informative)
- **Evidence**: what was observed in a real adapter (informative)
- **Not yet implemented**: the rule is ahead of the code, with the issue that closes the gap

The terms Transport adapter, Native QUIC, WebTransport session, Dialing end and Accepting end are
defined in `CONTEXT.md`. The SDKs ship only the dialing end. An accepting end exists in test helpers
only.

[RFC 2119]: https://www.rfc-editor.org/rfc/rfc2119
[RFC 8174]: https://www.rfc-editor.org/rfc/rfc8174

## Scope

The rules are stated in terms of the protocols (send stream, receive stream, reset, FIN
acknowledgement) and are normative for the TypeScript SDK, whose binding is the next section. The
[Python mapping](#python-mapping-informative) records how `moq_core.transport` meets the same rules
and where its aioquic adapter differs today. It is informative: moving Python to native QUIC is
#199, not this contract.

Datagrams are outside the contract. MOQT requires the QUIC DATAGRAM extension
(draft-20 §3.1), and the QUIC stack negotiates it, but the core never sends or reads datagrams.

## TypeScript binding

The seam is the factory a connection calls and the transport it returns.

| Member | Meaning |
| --- | --- |
| `createTransport(url, protocols)` | `RelayTransportFactory`. Called once per connection with the endpoint URL (the token already in its `jwt` query parameter) and the MOQT protocol identifiers to offer, in preference order. Returns `{ transport, ready, framing }`. |
| `ready` | Resolves once the transport can open streams and a protocol was selected; a rejection fails the connect call. |
| `framing` | `'webtransport'` or `'native-quic'`: what the transport carries. Optional, default `'webtransport'`. |
| `transport.protocol` | The protocol identifier the peer selected (TA-8). A browser's `WebTransport` has it. |
| `transport.incomingUnidirectionalStreams` | `ReadableStream` of the peer's unidirectional streams, each a `ReadableStream<Uint8Array>`. |
| `transport.incomingBidirectionalStreams` | `ReadableStream` of the peer's bidirectional streams, each `{ readable, writable }`. |
| `transport.createUnidirectionalStream()` | Opens a send stream: `Promise<WritableStream<Uint8Array>>`. |
| `transport.createBidirectionalStream()` | Opens a bidirectional stream: `Promise<{ readable, writable }>`. |
| `transport.close({ closeCode, reason })` | Closes the session. |
| `transport.closed` | Settles when the session has ended. |

On a send stream, "write" is `writer.write()`, "close" is `writer.close()` (FIN), "abort" is
`writer.abort(reason)` (RESET_STREAM), and `writer.closed` reports how the stream ended. On a
receive stream, "read" is `reader.read()` and "cancel" is `reader.cancel(reason)` (STOP_SENDING).
The same holds for a bidirectional stream's two halves.

## Rules

### Dialing and framing

**TA-1.** The adapter MUST declare its framing in the factory result: `'webtransport'` when the
transport carries a WebTransport session, `'native-quic'` when it carries MOQT directly on a QUIC
connection. An adapter that declares nothing is a WebTransport session.
**Why:** the framing decides what CLIENT_SETUP contains (TA-5, TA-6).

**TA-2.** On a WebTransport session the adapter MUST send the extended CONNECT to the URL exactly
as it was given, path and query included, so the token reaches the relay in the `jwt` query
parameter (draft-20 §3.1.4; draft-ietf-webtrans-http3 §3).

**TA-3.** On native QUIC the adapter MUST connect to the URL's host and port (443 when the URL gives
none), with the host as the TLS server name (draft-20 §3.1.3, §3.1.5). It MUST NOT send the URL's
path or query anywhere, and MUST NOT read, validate or log them: the path and the token travel only
in the core's SETUP (TA-6).

**TA-4.** The adapter MUST accept the `https:` URL the core hands it, in every framing. It MUST NOT
require a `moqt:` URL.
**Why:** callers pass the same URL in every runtime (#186). draft-20 §3.1.4 derives the `https` URI
from a `moqt` one by changing the scheme, so they name the same server.

### Version negotiation

**TA-5.** The adapter MUST offer exactly the factory's `protocols`, in the order given: as
`WT-Available-Protocols` on a WebTransport session (draft-ietf-webtrans-http3 §3.3), as the ALPN
list on native QUIC (RFC 7301 §3.1). On native QUIC it MUST NOT add `h3` or any other ALPN, even
though draft-20 §3.1.3 lets a client offer `h3` beside its MOQT ALPNs.
**Why:** the SDK speaks one MOQT version. A fallback to `h3` would turn a native QUIC adapter into
a WebTransport session it cannot run.

**TA-6.** On native QUIC the core, not the adapter, sends the AUTHORITY setup option (the URL's
authority) and the PATH setup option (the URL's path-abempty, then `?` and the query when present,
the resolved `jwt` included) in CLIENT_SETUP. On a WebTransport session it MUST NOT send either
(draft-20 §10.3.1.1, §10.3.1.2).
The cases, from the URL and token to the exact option values, are in
`vectors/setup-options.json`.

**TA-7.** The adapter MUST NOT resolve `ready` until the peer has explicitly selected one of the
offered protocols: the negotiated ALPN on native QUIC, the `WT-Protocol` response header on a
WebTransport session. A peer that selects nothing, or selects an identifier that was not offered,
MUST make `ready` reject with a session error (TA-22). The adapter MUST NOT assume a default
protocol.
**Why:** the relay's default when a client selects nothing is moq-lite, which the SDK does not
speak. A silent default fails late, as an obscure SETUP error, instead of at `ready`.

**TA-8.** Once `ready` resolves, `transport.protocol` MUST be the selected identifier.
**Why:** `protocol` is a required, read-only member of `MoqtTransport`, and the core fails the
connection unless it equals `MOQT_PROTOCOL_ID` (#203). That check backs up TA-7; it does not
replace it.

### Session lifetime

**TA-9.** `closed` MUST resolve, with `{ closeCode, reason }`, when the session ends by an
application close from either end: a local `close()`, or the peer's application CONNECTION_CLOSE
(native QUIC, frame type 0x1d, RFC 9000 §19.19) or CLOSE_WEBTRANSPORT_SESSION capsule
(draft-ietf-webtrans-http3 §6), whatever its code. It MUST reject, with a session error (TA-22),
when the session ends any other way: a QUIC-layer CONNECTION_CLOSE (frame type 0x1c, a transport
error such as FLOW_CONTROL_ERROR, RFC 9000 §20.1) from either end, connection loss, idle timeout,
a stateless reset, or a failed handshake or dial. `closed` MAY reject at the same time as `ready`.
**Why:** the core treats a resolution as a clean end and a rejection as a failure
(`#handleClosed`), and after SETUP it reports no error for a resolution. A transport failure that
resolved `closed` would read as an orderly shutdown. The core attaches its rejection handler as
soon as the factory returns (`connect-session.ts`), so a `closed` that rejects with `ready` is never
unhandled. Resolving for any application close code matches W3C WebTransport, where a
CLOSE_WEBTRANSPORT_SESSION capsule resolves `closed` with its code.

**TA-10.** `close({ closeCode, reason })` MUST end the session with that code and reason, using the
framing's own close (draft-20 §3.5). It MUST be idempotent, and MUST NOT throw, including after the
session has ended. A missing `closeCode` means 0 and a missing `reason` means empty.
**Why:** the core closes from teardown paths that cannot tell whether the session already ended.

**TA-11.** `close()` called before `ready` settles MUST cancel the dial: `ready` MUST reject,
`closed` MUST reject, and no session may be established afterwards, even if the handshake was about
to complete.
**Why:** a connect deadline or an aborted signal closes the transport mid-dial. A dial that went on
regardless would leave a session nobody owns.

**TA-12.** Once the session has ended, by either end:
- `createUnidirectionalStream()` and `createBidirectionalStream()` MUST reject.
- Every pending operation on every stream of the session MUST reject with a session error.
- The incoming stream readables MUST close after a clean end and error after any other.

### Streams

**TA-13.** Each stream the peer opens MUST appear on the matching incoming readable, in the order
the transport accepted them. Bytes on a stream MUST be delivered in order, and a FIN MUST end the
readable (`done`) only after every byte before it was delivered.

**TA-14.** A send stream's write MUST resolve once the transport has accepted the bytes, not once
the peer has received or acknowledged them, and MUST apply backpressure: a write stays pending while
flow control holds the stream (RFC 9000 §4.1).

**TA-15.** A send stream's close MUST send the FIN after every accepted write, and MUST resolve only
once the peer has acknowledged every byte and the FIN: the stream has reached the "Data Recvd"
state (RFC 9000 §3.1). It MUST reject if that becomes impossible: the stream is reset, the peer
stops it, or the session ends first.
**Why:** a group's operation completes when its FIN is handed over, and it stays a settling group
until close resolves (`settling-groups.ts`, `subgroup-writer.ts`). `publisher.close()` drains the
settling groups before it closes the session. If close resolved at hand-over, that drain would
prove nothing and groups would be lost silently at shutdown.
**Evidence:** Chromium and the #187 spike both resolve close only on acknowledgement. In the spike,
with the peer frozen, `close()` stayed pending for the whole 1000 ms freeze and resolved 1 ms after
the peer resumed.

**TA-16.** A send stream's abort MUST send RESET_STREAM immediately (RFC 9000 §19.4), in every state
short of "Data Recvd": while a write is blocked on flow control, and while close is waiting for
acknowledgement after the FIN went out (RFC 9000 §3.1 allows Data Sent → Reset Sent). The pending
write, the pending close and `writer.closed` MUST reject.
**Why:** a write timeout or a changed subscription has to free the track's write slot now, not after
the blocked write drains or the FIN is acknowledged.

**TA-17.** The code a reset or a stop carries out MUST be taken from the abort or cancel reason's
`streamErrorCode` when that is an integer from 0 to 2³²−1, and MUST be 0 otherwise (absent, not a
number, not an integer, out of range). An adapter other than a runtime's native `WebTransport` MUST
NOT depend on the reason's class or name. A runtime's native `WebTransport` reads the code only from
a native `WebTransportError` reason and resets with 0 for any other (see
[Known deviations](#known-deviations)), so where that global exists the core MUST hand over a
`WebTransportError` (`streamResetReason` in `stream-reset.ts` does).
**Why:** the core sends MOQT stream reset codes (draft-20 §3.3.4) as `streamErrorCode` on the
reason it hands over (`stream-reset.ts`, `write-slot.ts`): a native `WebTransportError` in a browser,
the SDK's own `MoqtStreamError`, which has the TA-22 shape, elsewhere. 2³²−1 is the largest code a
WebTransport session can carry, so the range is the same in both framings. A conformance test for this rule against a browser hands
the reason `streamResetReason` builds, not a plain object.

**TA-18.** A receive stream's cancel MUST send STOP_SENDING (RFC 9000 §19.5) with the code TA-17
gives, and end the readable. Later reads MUST NOT deliver more bytes.

**TA-19.** A peer RESET_STREAM MUST error the receive stream with a stream error (TA-22) carrying the
peer's code. Bytes delivered before the reset stay delivered; bytes that had not been delivered MAY
be discarded (RFC 9000 §3.2).

**TA-20.** A peer STOP_SENDING MUST error the send stream with a stream error (TA-22) carrying the
peer's code. `writer.closed` MUST reject with it **even when no write or close is in flight**, and
the next write or close MUST reject with it. The adapter's QUIC stack answers the stop with a
RESET_STREAM as RFC 9000 §3.5 requires.
**Why:** the relay stops a subgroup stream when its group is no longer of interest (draft-20
§11.4). The write slot hears of it through `writer.closed` between writes (`isStreamStop`,
`write-slot.ts`) and reports `SUBSCRIPTION_CHANGED`. A stop that surfaced only on the next write
would be reported late, or never on an idle track.

**TA-21.** When the peer's stream limit (MAX_STREAMS, RFC 9000 §4.6, §19.11) is
exhausted, `createUnidirectionalStream()` and `createBidirectionalStream()` MUST reject promptly. They
MUST NOT wait for the peer to raise the limit. Once credit is available again, they MUST succeed
again. A credit rejection's shape is not part of the contract (TA-22 does not apply to it).
**Why:** the core bounds its own use of credit (settling groups, one open stream per track), so a
rejection means the peer is stingier than the core assumed, and the operation fails with
`TRANSPORT_ERROR`. A create that hung would hold the track's write slot until its write timeout,
with no signal of why.
**Evidence:** Chromium rejects (`settling-groups.ts`). The #187 spike polls quinn's `open_uni()`
once. With the peer at MAX_STREAMS 4, the fifth create rejected, and credit returned once a stream
drained.

### Error shape

**TA-22.** Every error an adapter raises for a stream or session failure MUST be an object with a
`source` field:

- `source: 'stream'`: the failure belongs to one stream: the peer reset it (TA-19) or stopped it
  (TA-20). It MUST carry `streamErrorCode`: the peer's code as an integer, or `null` when the
  adapter cannot know it. `null` means unknown, never 0. A code above 2⁵³−1, possible only on native
  QUIC (RFC 9000 §20.2 allows 62 bits), MUST be reported as `null`.
- `source: 'session'`: the session has ended, and this operation failed because of it (TA-9, TA-12),
  or the dial failed (TA-7, TA-11).

The error's class and `name` are not part of the contract. A browser's `WebTransportError` already
has this shape, and other adapters SHOULD use an `Error` subclass so a stack is kept.
**Why:** the core classifies by shape so that an adapter without `WebTransportError` is understood
(#190): `isStreamError` and `isSessionError` in `transport-error.ts` read `source` alone, and
`isStreamStop` (`errors.ts`) and the publish session's announce-loss check build on them.

**TA-23.** `source: 'session'` on a stream operation's error MUST mean the session is over. An
adapter MUST NOT use it for a failure that leaves the session usable.
**Why:** the publish session reads a request stream failing with `source: 'session'` as the
session's own end, not something the peer did to the stream (`isSessionError`). TA-21's credit
rejection is exempt only because it is a create rejection, which the core never classifies.

**TA-24.** A session error that reports the peer's close SHOULD carry the peer's `closeCode` and
`reason`. A session error for a lost connection carries neither.
**Why:** a refusal for credentials is a peer close with MOQT's UNAUTHORIZED code (draft-20 §3.5),
which the SDK reports as `UNAUTHORIZED`. Browsers do not expose it (#14), so it is not a MUST.

### Process lifetime

**TA-25.** In a runtime with a process event loop (Node), an adapter MUST keep the process alive from
the start of the dial until `closed` settles, and MUST NOT let an open session be idle-unreferenced.
**Why:** a publisher script with nothing else scheduled must not exit while it is connected.
**Evidence:** in the #187 spike, the SDK's always-pending accept is a pending napi promise. It kept a
child process with an open session alive past 2.5 s.

**TA-26.** Once `closed` has settled, the adapter MUST hold nothing that keeps the process alive:
sockets, timers, threads or pending native calls. It is not required to release them within any
fixed time.
**Evidence:** in the #187 spike the process exited about 125 ms after `close()`.

Browsers have no process to keep alive, so TA-25 and TA-26 do not apply to them.

## What the core does in return

The contract has two sides. Against an adapter that follows the rules above, the core:

- attaches a handler to `closed` as soon as the factory returns, so TA-9's rejection alongside
  `ready` is never unhandled;
- sends PATH and AUTHORITY only on native QUIC (TA-6), and fails the connection with
  `TRANSPORT_ERROR`, closing the transport, when `framing` is neither value TA-1 allows;
- checks `protocol` after `ready` and, unless it is `moqt-20`, fails the connection with
  `TRANSPORT_ERROR` in phase `connecting`, naming the protocol received (or "none"), closes the
  transport and never sends CLIENT_SETUP (TA-7, TA-8);
- classifies errors by `source`, not by class or name, and hands over `streamErrorCode` on the
  reasons it resets and stops with (TA-17, TA-22, `transport-error.ts`);
- never concludes the session has ended from a create rejection alone, only from `closed` or a
  `source: 'session'` error on a stream;
- keeps reading the [Chromium deviations](#known-deviations) below as their conforming equivalents.

## Known deviations

These are adapters, as they exist, that do not meet a rule. The core tolerates each one; new
adapters MUST NOT copy them.

- **Chromium STOP_SENDING as `NetworkError` (TA-20, TA-22).** Depending on which of its own
  notifications lands first, Chromium reports a peer STOP_SENDING either as a `WebTransportError`
  with `source: 'stream'` ("Received STOP_SENDING."), or as a `NetworkError` `DOMException` with the
  message "The stream was aborted by the remote server", which has no `source` and no code. The core
  MUST keep accepting the second form as a stop (`isStreamStop` in `errors.ts`).
- **Browser reset reasons (TA-17).** A browser's native `WebTransport` takes a reset or stop code
  only from a native `WebTransportError` reason, and resets with code 0 for a plain object or
  `Error` carrying `streamErrorCode`. The core works around it by building a `WebTransportError`
  wherever the global exists.
- **Chromium `WebTransportError` constructor (TA-17).** Chromium still implements the earlier
  dictionary constructor, `new WebTransportError({ message, streamErrorCode })`. `streamResetReason`
  falls back to it, so the reason still carries the code.
- **Browser session close codes (TA-24).** A browser does not expose the peer's close code on a
  refused connection (#14), so a credentials refusal reaches the SDK as `TRANSPORT_ERROR`.
- **Browser stream error codes (TA-22).** A browser's `streamErrorCode` is nullable and is `null`
  when the browser does not know the code, which TA-22 allows.
- **Chromium `ready` without a selected protocol (TA-7).** When the peer's response names no
  protocol, or one that was not offered, Chromium resolves `ready` anyway and sets `protocol` to the
  empty string. The core's check of `protocol` after `ready` (TA-8) turns that into a connect
  failure before SETUP.
  **Evidence:** the conformance suite, in Chromium 147.0.7727.15 against the Python accepting end.
- **Chromium 8-bit send codes (TA-17, TA-18).** Chromium's `WebTransportError` constructor clamps
  `streamErrorCode` to 255, so a reset or a stop it sends carries at most 255. Codes it receives are
  not limited: a peer reset with 2³²−1 arrives intact (TA-19). This is harmless today, since every
  stream reset code the core sends is 0x12 or below (draft-20 §3.3.4).
  **Evidence:** in Chromium 147.0.7727.15,
  `new WebTransportError({ message, streamErrorCode: 256 }).streamErrorCode` is 255, and the
  conformance suite's peer sees 255 for a reset or stop sent with 256, 65536 or 2³²−1.

## Test doubles

The in-memory transport pair (`ts/packages/moq-core/src/tests/helpers/transport-pair.ts`) is a test
double, not an adapter the product ships. Its streams are in-process pipes with a receive buffer, and
`createTransportPairFactory` puts a factory in front of it with `ready`, `framing` and `protocol`. It
passes the conformance suite except where it has no QUIC to model, and the suite skips those cases
for it:

- no stream credit: creates never reject for credit (TA-21)
- no acknowledgement: close resolves once the FIN is in the peer's buffer, and a peer cannot
  withhold acknowledgement (TA-15, and the cases of TA-14 and TA-16 that need a paused peer)

`close()` reaches both ends at once, with no time in flight. That is harmless: no rule depends on how
long a close takes to arrive.

A test that depends on credit or acknowledgement uses an adapter that has it: Chromium against the
conformance suite's Python accepting end, the Python loopback server, or `@mux/relay-quic`.

## Python mapping (informative)

`moq_core.transport.MoqtTransport` is the same seam spelled for asyncio, and `mux_relay.webtransport`
is its aioquic WebTransport session adapter. How the rules map:

| Rule | Python |
| --- | --- |
| TA-1 to TA-6 | WebTransport session only (`connect_webtransport`); the CONNECT carries the URL and `WT-Available-Protocols`. Native QUIC is #199. |
| TA-7, TA-8 | `protocol_id` holds the selection, and an unoffered selection fails the dial. **Differs:** a response that names no protocol is accepted, with `protocol_id` set to `None`. |
| TA-9 | `wait_closed()` returns after a clean end and raises the error otherwise. |
| TA-10 | `close(close_code=0, reason="")` is idempotent and ignored after the session ends. |
| TA-15 | **Split in two:** `SendStream.close()` returns once the FIN is handed over, and `AcknowledgedSendStream.wait_acknowledged()` returns on acknowledgement. Together they meet the rule. A stream without `wait_acknowledged` counts as acknowledged at once, which only test doubles do. |
| TA-16 | `abort()` resets until the peer acknowledges the FIN, including after the FIN was transmitted, and `wait_acknowledged()` then raises the reason. Once the FIN is acknowledged it does nothing (the stream is in Data Recvd). A settling group's delivery timeout depends on this (#167). |
| TA-17, TA-18 | `abort(reason)` and `cancel(reason)` take the code from a `WebTransportError`'s `code`, else from a `stream_error_code` attribute, else from an integer reason, each within 0 to 2³²−1, else 0. |
| TA-19, TA-20, TA-22 | **Name-based, not shape-based:** a stream failure is a `WebTransportError` with the peer's code in `code`, and a peer STOP_SENDING is its `StreamStopped` subclass. There is no `source` field. |
| TA-24 | `peer_closed`, `code` and `reason` carry the peer's CLOSE_WEBTRANSPORT_SESSION, and the SDK reports `UNAUTHORIZED` from it. |
| TA-25, TA-26 | asyncio holds no process open by itself: the application's own `asyncio.run` decides. |

## Conformance

The conformance suite (`ts/conformance/`, #193) is written against `MoqtTransport` once and run against
every target:

- the in-memory pair in vitest, as part of `pnpm check`
- Chromium's native `WebTransport`, in `pnpm test:conformance`, against a scriptable Python
  accepting end (`python/packages/mux-relay/tests/relay_helpers/conformance_peer.py`) that can
  reset, stop, close at either layer, withhold acknowledgement and limit stream credit
- `@mux/relay-quic` in Node, in `pnpm test:conformance:node`, against the same accepting end in its
  native QUIC mode (`--framing native-quic`)

Each MUST has at least one test named after its rule. TA-6 is the exception: it binds the core, not the
adapter, and the core's SETUP tests cover it. For TA-26 the test is a Node script that has to exit by
itself after `close()`.

A target declares what it cannot exercise, and which rules it is known to fail. A skip names its
reason. A known failure names its Known deviation, or the issue that closes the gap, and its test goes
red once the target starts to pass, so the expectation gets removed. The suite owns the results; this
document does not carry a per-adapter matrix.

A rule that a real adapter cannot meet is either fixed in the adapter or moved, with its evidence,
to [Known deviations](#known-deviations). It is never removed silently.
