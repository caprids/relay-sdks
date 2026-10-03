---
status: accepted
date: 2026-10-02
---

# The Python dialing end speaks native QUIC, and replaces the WebTransport session before 0.1.0

The Python SDK dials the relay with its own WebTransport session: HTTP/3 and WebTransport layers
written over aioquic, in `mux_relay.webtransport`. The TypeScript SDK on Node already speaks native
QUIC to the same relay (ADR 0002): MOQT on QUIC with ALPN `moqt-20`, and the endpoint's path, token
and authority in SETUP. We decided (#199) that the Python dialing end moves to native QUIC too, and
that it **replaces** the WebTransport session rather than sitting beside it. It happens before the
first PyPI release (0.1.0), while removing `connect_webtransport` from the public API costs nothing.

The work is staged. `mux_relay.quic.connect_quic` arrives first, through `create_transport` (#229).
It becomes the default once a pytest port of the conformance suite covers it and the relay's
behaviour has been re-checked on it (#230, #231). Then the WebTransport session leaves the package
and stays only as a test accepting end, which the TypeScript suite's Chromium target needs (#232,
#208). Classifying transport errors by shape follows the rename (#233).

## Considered options

- **Replace the WebTransport session with native QUIC (chosen).** Python and Node use one wire path
  to the relay. The shipped package loses HTTP/3 and the private `H3Connection` members it reaches
  into. A credentials refusal becomes a plain application close (code 0x2) instead of a
  CLOSE_WEBTRANSPORT_SESSION capsule that moq-relay 0.15.8 sends in the same flight as an HTTP/3
  control-stream FIN, which the adapter has to hold back to hear the capsule at all (#14).
- **Keep both, with native QUIC as the default.** Every line of the WebTransport session stays, and
  there is a second path to test and keep working, for a fallback no user asked for.
- **Keep the WebTransport session as the default.** Python stays on a wire path no other SDK
  dialing a relay from a process uses, with its HTTP/3 workarounds.
- **Do nothing.** Same as the above, and native QUIC never ships in Python.

## Consequences

- The payoff is smaller than it first looks. Most aioquic workarounds are QUIC-level or
  socket-level and stay, now shared by both framings: the dropped-FIN retry (#47), resets and
  stop-sendings served first (#160), the PING keepalive (#158), the ICMP refusal reports during the
  handshake (#163), and the drain before close, since a CONNECTION_CLOSE drops unacknowledged data
  just as the capsule did (#209). What goes is the HTTP/3 and WebTransport layer: the extended
  CONNECT, capsules, the control-stream FIN grace, the HTTP/3 error-code mapping and the
  `H3Connection` private members, roughly a third of the adapter.
- `ConnectingTransport` gains `framing`, as TA-1 has it, and the connection sends PATH and AUTHORITY
  in SETUP only on native QUIC (TA-6). moq_core needs no wire change: it already takes
  `setup_options`.
- A peer's application close ends a native QUIC session cleanly whatever its code (TA-9), so the
  SDK reads a credentials refusal from the code that close carried, not from the error the session
  ended with.
- Given up: reaching a relay through a middlebox that passes only `h3`. No user or example depends
  on that today. Revisit if one does.
