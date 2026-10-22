# Transport adapter contract

A Relay connection runs over a **transport adapter** (see `CONTEXT.md`): the browser's native
`WebTransport`, the Python aioquic native QUIC client, the `@caprid/relay-quic`
native QUIC adapter for Node, the `@caprid/relay-websocket` QMux-over-WebSocket adapter for browsers and
Node, and the in-memory pairs the tests use. This document states what every adapter must do
for the SDK core to behave as `behavior.md` says. Until now that was only the TypeScript
`MoqtTransport` interface plus behaviour the core relied on without writing it down.

## Sources of truth

The protocols underneath are the authority. This contract adds no wire behaviour and overrides
nothing in them. It only narrows what an adapter exposes at the seam the core depends on:

- [draft-ietf-moq-transport-22] (MOQT, wire identifier `moqt-22`, pinned in `protocol.json`, with
  `moqt-20` offered as a fallback)
- [RFC 9000] (QUIC streams, RESET_STREAM, STOP_SENDING, MAX_STREAMS, acknowledgement)
- [RFC 7301] (ALPN)
- [draft-ietf-webtrans-http3] and the [W3C WebTransport] API, for a WebTransport session
- [draft-ietf-quic-qmux] (QUIC streams over a reliable byte stream), [draft-lcurley-qmux-websocket]
  (its WebSocket binding, which moq-relay serves) and [RFC 6455] (WebSocket), for QMux over WebSocket

Every rule cites the upstream section it narrows. If a rule seems to conflict with one of these
specifications, the specification wins and the rule is a bug in this document. TA-27 is the one
exception: on native QUIC it follows moq-relay's stream error codes, which differ from draft-22.

QMux over WebSocket has QUIC's stream semantics (STREAM, RESET_STREAM, STOP_SENDING, MAX_STREAMS and
flow control, as QMux frames) but no QUIC underneath: every stream shares one TCP connection, so a lost
packet holds up every stream behind it (head-of-line blocking), and nothing is acknowledged above TCP.
Where this document names a QUIC frame, it means the QMux frame of the same name on a WebSocket.

[draft-ietf-moq-transport-22]: https://datatracker.ietf.org/doc/draft-ietf-moq-transport/22/
[RFC 9000]: https://www.rfc-editor.org/rfc/rfc9000
[RFC 7301]: https://www.rfc-editor.org/rfc/rfc7301
[draft-ietf-webtrans-http3]: https://datatracker.ietf.org/doc/draft-ietf-webtrans-http3/
[W3C WebTransport]: https://www.w3.org/TR/webtransport/
[draft-ietf-quic-qmux]: https://datatracker.ietf.org/doc/draft-ietf-quic-qmux/
[draft-lcurley-qmux-websocket]: https://github.com/moq-dev/moq/blob/main/drafts/draft-lcurley-qmux-websocket.md
[RFC 6455]: https://www.rfc-editor.org/rfc/rfc6455

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
and where its aioquic adapter differs today. It is informative.

Datagrams are outside the contract. MOQT requires the QUIC DATAGRAM extension
(draft-22 §6.2), and the QUIC stack negotiates it, but the core never sends or reads datagrams.

## TypeScript binding

The seam is the factory a connection calls and the transport it returns.

| Member | Meaning |
| --- | --- |
| `createTransport(url, protocols)` | `RelayTransportFactory`. Called once per connection with the endpoint URL (the token already in its `jwt` query parameter) and the MOQT protocol identifiers to offer, in preference order. Returns `{ transport, ready, framing }`. |
| `ready` | Resolves once the transport can open streams and a protocol was selected; a rejection fails the connect call. |
| `framing` | `'webtransport'`, `'native-quic'` or `'websocket'`: what the transport carries. Optional, default `'webtransport'`. The core reads it once `ready` has resolved. |
| `transport.protocol` | The protocol identifier the peer selected (TA-8). A browser's `WebTransport` has it. |
| `transport.incomingUnidirectionalStreams` | `ReadableStream` of the peer's unidirectional streams, each a `ReadableStream<Uint8Array>`. |
| `transport.incomingBidirectionalStreams` | `ReadableStream` of the peer's bidirectional streams, each `{ readable, writable }`. |
| `transport.createUnidirectionalStream(options?)` | Opens a send stream: `Promise<WritableStream<Uint8Array>>`. `options.sendOrder` is its send order (TA-28). |
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
connection, `'websocket'` when it carries MOQT on QMux over a WebSocket. An adapter that declares
nothing is a WebTransport session.
**Why:** the framing decides what CLIENT_SETUP contains (TA-5, TA-6).

**TA-2.** On a WebTransport session the adapter MUST send the extended CONNECT to the URL exactly
as it was given, path and query included, so the token reaches the relay in the `jwt` query
parameter (draft-22 §6.2.1; draft-ietf-webtrans-http3 §3). Over WebSocket it MUST send the opening
handshake (RFC 6455 §4.1) to the same URL with the scheme `wss:` in place of `https:`, path and query
unchanged: moq-relay reads the path and the token from the upgrade request, as it does from a CONNECT.

**TA-3.** On native QUIC the adapter MUST connect to the URL's host and port (443 when the URL gives
none), with the host as the TLS server name (draft-22 §6.1.2, §6.2.2). It MUST NOT send the URL's
path or query anywhere, and MUST NOT read, validate or log them: the path and the token travel only
in the core's SETUP (TA-6).

**TA-4.** The adapter MUST accept the `https:` URL the core hands it, in every framing. It MUST NOT
require a `moqt:` URL.
**Why:** callers pass the same URL in every runtime (#186). draft-22 §6.2.1 derives the `https` URI
from a `moqt` one by changing the scheme, so they name the same server.

### Version negotiation

**TA-5.** The adapter MUST offer exactly the factory's `protocols`, in the order given: as
`WT-Available-Protocols` on a WebTransport session (draft-ietf-webtrans-http3 §3.3), as the ALPN
list on native QUIC (RFC 7301 §3.1), and over WebSocket as `Sec-WebSocket-Protocol`, each protocol
prefixed with the QMux wire format that carries it, `qmux-01.<protocol>` (draft-lcurley-qmux-websocket,
"Subprotocol Negotiation"). On native QUIC it MUST NOT add `h3` or any other ALPN, even
though draft-22 §6.1.2 lets a client offer `h3` beside its MOQT ALPNs. Over WebSocket it MUST NOT add
a bare wire format (`qmux-01`, `qmux-00`, `webtransport`).
**Why:** the SDK speaks one MOQT version. A fallback to `h3` would turn a native QUIC adapter into
a WebTransport session it cannot run, and a bare wire format lets moq-relay pick moq-lite in SETUP.
moq-relay pairs `moqt-18` and newer only with `qmux-01` (`QMUX01_ONLY_ALPNS` in
`rs/moq-relay/src/websocket.rs`), and fails the upgrade with 400 when nothing offered is its own.

**TA-6.** On native QUIC the core, not the adapter, sends the AUTHORITY setup option (the URL's
authority) and the PATH setup option (the URL's path-abempty, then `?` and the query when present,
the resolved `jwt` included) in CLIENT_SETUP. On a WebTransport session, and over WebSocket, it MUST
NOT send either (draft-22 §9.1.1, §9.1.2): the request URL carries them.
The cases, from the URL and token to the exact option values, are in
`vectors/setup-options.json`.

**TA-7.** The adapter MUST NOT resolve `ready` until the peer has explicitly selected one of the
offered protocols: the negotiated ALPN on native QUIC, the `WT-Protocol` response header on a
WebTransport session, the `Sec-WebSocket-Protocol` response header over WebSocket. A peer that selects nothing, or selects an identifier that was not offered,
MUST make `ready` reject with a session error (TA-22). The adapter MUST NOT assume a default
protocol.
**Why:** the relay's default when a client selects nothing is moq-lite, which the SDK does not
speak. A silent default fails late, as an obscure SETUP error, instead of at `ready`.

**TA-8.** Once `ready` resolves, `transport.protocol` MUST be the selected identifier: over WebSocket,
the MOQT protocol without its `qmux-01.` prefix.
**Why:** `protocol` is a required, read-only member of `MoqtTransport`, and the core fails the
connection unless it equals `MOQT_PROTOCOL_ID` (#203). That check backs up TA-7; it does not
replace it.

### Session lifetime

**TA-9.** `closed` MUST resolve, with `{ closeCode, reason }`, when the session ends by an
application close from either end: a local `close()`, or the peer's application CONNECTION_CLOSE
(native QUIC, frame type 0x1d, RFC 9000 §19.19; over WebSocket the QMux frame of that type) or
CLOSE_WEBTRANSPORT_SESSION capsule (draft-ietf-webtrans-http3 §6), whatever its code. It MUST
reject, with a session error (TA-22), when the session ends any other way: a QUIC-layer
CONNECTION_CLOSE (frame type 0x1c, a transport error such as FLOW_CONTROL_ERROR, RFC 9000 §20.1)
from either end, connection loss, idle timeout, a stateless reset, a WebSocket that closes or drops
without a QMux close, or a failed handshake or dial. `closed` MAY reject at the same time as `ready`.
**Why:** the core treats a resolution as a clean end and a rejection as a failure
(`#handleClosed`), and after SETUP it reports no error for a resolution. A transport failure that
resolved `closed` would read as an orderly shutdown. The core attaches its rejection handler as
soon as the factory returns (`connect-session.ts`), so a `closed` that rejects with `ready` is never
unhandled. Resolving for any application close code matches W3C WebTransport, where a
CLOSE_WEBTRANSPORT_SESSION capsule resolves `closed` with its code.

**TA-10.** `close({ closeCode, reason })` MUST end the session with that code and reason, using the
framing's own close (draft-22 §6.6). It MUST be idempotent, and MUST NOT throw, including after the
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
**Why:** the core sends MOQT stream reset codes (draft-22 §12.5) as `streamErrorCode` on the
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
**Why:** the relay stops a subgroup stream when its group is no longer of interest (draft-22
§11.3.2). The write slot hears of it through `writer.closed` between writes (`isStreamStop`,
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

**TA-27.** On native QUIC, stream codes MUST cross the wire in the HTTP/3 encoding
draft-ietf-webtrans-http3 gives WebTransport application error codes, as moq-relay uses them there.
A reset or a stop with code *n* (TA-17, TA-18) MUST carry `0x52E4A40FA8DB + n + floor(n / 0x1E)`.
A peer's RESET_STREAM or STOP_SENDING code from `0x52E4A40FA8DB` to `0x52E5AC983162` MUST be decoded
before TA-19, TA-20 and TA-22 report it: *c* − `0x52E4A40FA8DB` = *s* gives *s* − floor(*s* / 0x1F),
and a GREASE slot (*s* mod 0x1F = 0x1E) carries no code and MUST be reported as `null`. A code
outside that range MUST be reported as it arrived. Session close codes (TA-9, TA-10, TA-24) MUST NOT
be mapped. This rule does not apply to a WebTransport session, whose runtime maps codes itself, nor
over WebSocket, where QMux carries stream codes as they are: moq-relay's QMux sessions do not map
them (`SendStream::reset` in qmux 0.5.2, which moq-relay 0.17.5 uses), and a forwarded subgroup reset
reaches a WebSocket subscriber as code 1 (measured on a local 0.17.5, 2026-10-17).
**Why:** `streamErrorCode` must mean the same thing in both framings, and the relay must be able to
read the codes the core sends. moq-relay reaches raw QUIC through web-transport-moq, whose stream
wrappers map every reset and stop code through `error_to_http3` and `error_from_http3` (moq-relay
0.17.0 uses web-transport-moq 1.3.3, `send.rs` and `recv.rs`), while its session close stays raw.
draft-22 puts stream codes on native QUIC unmapped (§12.5, RFC 9000 §19.4, §19.5), so this follows
the relay rather than the draft (#248). Decoding is lenient: a peer that sends the draft's raw codes
is still read correctly, since none of them reaches the range. The encoding on the way out is what
would have to change if web-transport-moq stopped mapping on raw QUIC.
**Evidence:** on 2026-10-05, production 0.17.0 reset forwarded subgroups with `0x52E4A40FA8DC`
(code 1) in 3/3 runs per case, from Python and TS subscribers over native QUIC. A local
`moqdev/moq-relay:0.17.0` logged a raw reset with code 7 from our publisher as
`transport: invalid RESET_STREAM: 7`, and the encoded one as `code=7`. Either way the relay forwards
the reset with code 1 rather than the publisher's (#156), so subscribers read 1 today.

**TA-28.** `createUnidirectionalStream(options)` MUST accept `{ sendOrder }`, an integer where a
higher value is more urgent and 0 is what every stream opened without one gets, and MUST open the
stream as it would without it. An adapter whose transport can schedule streams SHOULD hold a stream's
queued bytes while a stream with a higher send order has bytes queued, as a WebTransport send order
does within its send group; one that cannot MUST ignore the option.
**Why:** the publisher opens each data stream of a track that declares a Default Publisher Priority
with a send order below 0 ([behavior.md, "Send order"](behavior.md#send-order)), so that under a
congested uplink the publisher's own transport sends audio before video, and both after the control
and request streams.
**Evidence:** a browser's `WebTransport` takes `sendOrder` itself (W3C WebTransport,
`WebTransportSendOptions`): Firefox 155 schedules by it, and Chromium 153 ignores it (see
[Known deviations](#known-deviations)). `@moq/qmux` 0.3.3 schedules stream data
highest `sendOrder` first, round-robin among equals (`scheduler.d.ts`). quinn 0.11's
`SendStream::set_priority` takes an `i32`, higher sent first and 0 by default; `@caprid/relay-quic`
clamps the send order to that range.

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
**Why:** a refusal for credentials is a peer close with MOQT's UNAUTHORIZED code (draft-22 §12.2),
which the SDK reports as `UNAUTHORIZED` while connecting. Browsers did not expose it against relays
before moq-relay 0.17.0 (#14), so it is not a MUST.

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
  `TRANSPORT_ERROR`, closing the transport, when `framing` is none of the values TA-1 allows;
- checks `protocol` after `ready` and, unless it is one it offered (`moqt-22` or `moqt-20`), fails the connection with
  `TRANSPORT_ERROR` in phase `connecting`, naming the protocol received (or "none"), closes the
  transport and never sends CLIENT_SETUP (TA-7, TA-8);
- classifies errors by `source`, not by class or name, and hands over `streamErrorCode` on the
  reasons it resets and stops with (TA-17, TA-22, `transport-error.ts`);
- never concludes the session has ended from a create rejection alone, only from `closed` or a
  `source: 'session'` error on a stream;
- fails the connection with `UNAUTHORIZED` when the peer closes with MOQT's UNAUTHORIZED (0x2),
  MALFORMED_AUTH_TOKEN (0x16) or EXPIRED_AUTH_TOKEN (0x18) before its SETUP, read from `closed`'s
  resolution (TA-9) or a session error's `closeCode` (TA-24); the same close after SETUP is not a
  refusal;
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
- **Browser session close codes (TA-9, TA-24), fixed from moq-relay 0.17.0.** Against moq-relay
  0.15.8 and earlier, a browser did not expose the peer's close code on a refused connection: the
  relay tore down HTTP/3 in the same flight as its CLOSE_WEBTRANSPORT_SESSION capsule, so `closed`
  rejected with "Connection lost." and no `closeCode`, and a credentials refusal reached the SDK as
  `TRANSPORT_ERROR` (#14). moq-relay 0.17.0 keeps the session alive until the capsule is delivered
  (upstream `b14a61f17`, moq-dev/moq#4429), so `closed` resolves with code 2 and reason
  `unauthorized` and the SDK reports `UNAUTHORIZED` in phase `connecting`. The core needs no change;
  only a browser against an older relay still sees the deviation.
  **Evidence:** Chromium against `sjc.relay.mux.global` on 2026-10-02 (0.15.8): no token, an empty
  token, a tampered signature and a wrong path each gave "Connection lost." in 3/3 runs. On
  2026-10-05 (0.17.0): a missing, malformed, tampered, expired-looking or wrong-path token each gave
  `UNAUTHORIZED` in phase `connecting` in 3/3 runs.
- **Chromium send order (TA-28).** Chromium 153 has no `WebTransportSendStream` and no
  `createSendGroup()`, and drops `sendOrder` from the options: its streams are scheduled alike.
  Firefox 155 has both and schedules by it. **Evidence:** on 2026-10-20, through an 800 kbit/s
  uplink cap (`ts/scripts/probe-send-order.mjs`), turning the media streams' order around starved
  a Firefox publisher's audio and left a Chromium publisher's unchanged
  ([behavior.md, "Send order"](behavior.md#send-order)).
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
  stream reset code the core sends is 0x12 or below (draft-22 §12.5).
  **Evidence:** in Chromium 147.0.7727.15,
  `new WebTransportError({ message, streamErrorCode: 256 }).streamErrorCode` is 255, and the
  conformance suite's peer sees 255 for a reset or stop sent with 256, 65536 or 2³²−1.

### QMux over WebSocket (`@caprid/relay-websocket`)

The adapter wraps `@moq/qmux` 0.3.3 (ADR 0005). It meets every other rule; the conformance suite
runs it in Node, Chromium, Firefox and WebKit (`conformance/targets/websocket.js`).

- **No acknowledgement (TA-15, and TA-16 while close waits).** QMux runs over TCP and acknowledges
  nothing an application can see, so a send stream's close resolves once its FIN is handed to the
  WebSocket, not once the peer has it. A settling group's wait therefore ends at hand-over, and
  `publisher.close()` drains nothing a closing socket can still lose. This is the binding, not the
  library: no QMux-over-WebSocket adapter can meet TA-15.
- **A reset behind a blocked write (TA-16).** The pending write and `writer.closed` reject at once,
  so the write slot is free at once, but the RESET_STREAM goes out only once `@moq/qmux` has finished
  the write it already started, when the peer next grants credit.
- **A peer reset behind an unread chunk (TA-19).** `@moq/qmux` reports a peer RESET_STREAM only to a
  read. The adapter keeps one read outstanding, so a reset lands without a read pending unless a chunk
  the application has not read is already waiting; then the next read delivers that chunk and the
  one after rejects. RFC 9000 §3.2 lets the chunk be discarded; here it is delivered.
- **Long close reasons (TA-10).** A reason is cut to 1024 UTF-8 bytes, on a character boundary:
  `@moq/qmux` refuses to close at all when the close frame would not fit one QMux record. `closed`
  resolves with the reason as sent. The core's own reasons are far shorter.
- **Stream credit (TA-21).** A create waits for MAX_STREAMS credit instead of rejecting. moq-relay
  grants far more streams than the core uses at once, so the wait is not seen against the relay.

## Test doubles

The in-memory transport pair (`ts/packages/moq-core/src/tests/helpers/transport-pair.ts`) is a test
double, not an adapter the product ships. Its streams are in-process pipes with a receive buffer, and
`createTransportPairFactory` puts a factory in front of it with `ready`, `framing` and `protocol`. It
passes the conformance suite except where it has no QUIC to model, and the suite skips those cases
for it:

- no stream credit: creates never reject for credit (TA-21)
- no acknowledgement: close resolves once the FIN is in the peer's buffer, and a peer cannot pause:
  drop what the dialer sends so that nothing is acknowledged (TA-15, and the cases of TA-14 and TA-16
  that need a paused peer)

`close()` reaches both ends at once, with no time in flight. That is harmless: no rule depends on how
long a close takes to arrive.

A test that depends on credit or acknowledgement uses an adapter that has it: Chromium against the
conformance suite's Python accepting end, the Python loopback server, or `@caprid/relay-quic`.

## Python mapping (informative)

`moq_core.transport.MoqtTransport` is the same seam spelled for asyncio. `caprid_relay` has one aioquic
adapter for it, `caprid_relay.quic` (`connect_quic`): native QUIC, and the default (ADR 0004). The package
ships no WebTransport session, so the rules only a WebTransport session has (TA-2, and TA-5's
`WT-Available-Protocols`) have no Python row; a `create_transport` factory may still declare that framing
(TA-1). The test suite keeps a WebTransport session as the conformance peer's accepting end, on the same
stream halves and QUIC plumbing (#232). How the rules map:

| Rule | Python |
| --- | --- |
| TA-1 | `ConnectingTransport.framing`, `"webtransport"` when a factory leaves it out. `connect_quic`, the default factory, declares `"native-quic"`. `"websocket"` sends what `"webtransport"` does, though the package ships no WebSocket adapter. Any other value fails the connection with `TRANSPORT_ERROR`, closing the transport. |
| TA-3, TA-4 | Native QUIC: `connect_quic` takes the `https:` URL, dials its host and port (443 by default) with the host as the server name, and reads nothing else from it. |
| TA-5 | The ALPN list is exactly `protocols`, with no `h3`. |
| TA-6 | `open_relay_session` sends PATH and AUTHORITY through moq_core's `setup_options` on native QUIC only (`test_setup_options.py`, against `vectors/setup-options.json`). |
| TA-7, TA-8 | `protocol_id` is the negotiated ALPN. A handshake that negotiates none, or one that was not offered, fails the dial. |
| TA-9 | `wait_closed()` returns after a clean end and raises the error otherwise. A peer's application close (0x1d) is a clean end whatever its code, and `close_info` holds its `(code, reason)`; a QUIC-layer close, an idle timeout or a failed dial raises. |
| TA-10 | `close(close_code=0, reason="")` is idempotent and ignored after the session ends. It waits, within a bound, for every send stream to be acknowledged first (#209), then ends the connection with an application CONNECTION_CLOSE carrying the code and reason. |
| TA-11 | `close()` before `ready` cancels the dial: `ready` and `wait_closed()` raise. |
| TA-12 | The incoming readables are async iterators. After a clean end they raise `StopAsyncIteration`; after any other, they raise the session's error, the same `source="session"` error `wait_closed()` raises. Either way, streams that arrived before the end are yielded first. |
| TA-15 | **Split in two:** `SendStream.close()` returns once the FIN is handed over, and `AcknowledgedSendStream.wait_acknowledged()` returns on acknowledgement. Together they meet the rule. `wait_acknowledged()` also raises when the stream fails between writes, so it stands in for `writer.closed` (TA-12, TA-16, TA-20). A stream without `wait_acknowledged` counts as acknowledged at once, which only test doubles do. |
| TA-16 | `abort()` resets until the peer acknowledges the FIN, including after the FIN was transmitted, and `wait_acknowledged()` then raises the reason. Once the FIN is acknowledged it does nothing (the stream is in Data Recvd). A settling group's delivery timeout depends on this (#167). |
| TA-17, TA-18 | `abort(reason)` and `cancel(reason)` take the code from a `stream_error_code` attribute (the write slot's `StreamReset`, or a stream error), else from an integer reason, each within 0 to 2³²−1, else 0. The code goes on the wire as TA-27 encodes it. |
| TA-19, TA-20, TA-22 | Errors have the TA-22 shape, spelled `source` and `stream_error_code`: `caprid_relay.quic` raises a private `QuicTransportError` with them. A peer STOP_SENDING is a `source="stream"` error on the send half, with no subclass of its own. The core and the SDK classify by `source` alone (`moq_core.transport.is_stream_error`, `is_session_error`). An exhausted stream limit (TA-21), and a write to a stream this end already closed or reset, carry `source=None`. |
| TA-21 | A create rejects at once when the peer's MAX_STREAMS is used up, and succeeds again once the peer raises it. |
| TA-24 | `close_info` carries the peer's application close, and so does the session error an operation raises once it has ended the session (`close_code` and `reason`). The SDK reports `UNAUTHORIZED` from either, in any phase. |
| TA-25, TA-26 | asyncio holds no process open by itself: the application's own `asyncio.run` decides. |
| TA-27 | `QuicSession` encodes and decodes stream codes in its `_to_wire_code` and `_from_wire_code` hooks (`caprid_relay.quic._errors`). The test suite's native QUIC accepting ends map them the same way, as the relay does. |
| TA-28 | **Not bound:** `create_unidirectional_stream()` takes no options, since the Python publisher sets no send order and aioquic has no stream priorities. |

## Conformance

The conformance suite (`ts/conformance/`, #193) is written against `MoqtTransport` once and run against
every target:

- the in-memory pair in vitest, as part of `pnpm check`
- Chromium's native `WebTransport`, in `pnpm test:conformance`, against a scriptable Python
  accepting end (`python/packages/caprid-relay/tests/relay_helpers/conformance_peer.py`) that can
  reset, stop, close at either layer, withhold acknowledgement and limit stream credit
- `@caprid/relay-quic` in Node, in `pnpm test:conformance:node`, against the same accepting end in its
  native QUIC mode (`--framing native-quic`)
- `@caprid/relay-websocket` in Node, in `pnpm test:conformance:websocket`, and in Chromium, Firefox and
  WebKit, in `pnpm test:conformance:websocket:browsers`, against a Node accepting end for QMux over
  WebSocket (`ts/scripts/websocket-peer.js`, `@moq/qmux` over `ws`) that speaks the Python one's
  protocol

A pytest port (`python/packages/caprid-relay/tests/test_transport_conformance.py`, #230) runs the same cases
against `moq_core.transport.MoqtTransport`, spelled as the [Python mapping](#python-mapping-informative) says,
in the Python SDK's `uv run pytest`. Its target (`tests/relay_helpers/conformance_targets.py`) is the native QUIC
dialing end (`connect_quic`), against `--framing native-quic`; the cases only a WebTransport session has are
skipped, since the package ships none (#232).

The Python mapping has no **Differs** row, so the native QUIC target has no known failure. A Differs row
added later is a known failure there, naming the issue that closes it. TA-6 and TA-25/TA-26 are skipped,
with the reasons the mapping gives.

Each MUST has at least one test named after its rule. TA-6 is the exception: it binds the core, not the
adapter, and the core's SETUP tests cover it. For TA-26 the test is a Node script that has to exit by
itself after `close()`.

A target declares what it cannot exercise, and which rules it is known to fail. A skip names its
reason. A known failure names its Known deviation, or the issue that closes the gap, and its test goes
red once the target starts to pass, so the expectation gets removed. The suite owns the results; this
document does not carry a per-adapter matrix.

A rule that a real adapter cannot meet is either fixed in the adapter or moved, with its evidence,
to [Known deviations](#known-deviations). It is never removed silently.
