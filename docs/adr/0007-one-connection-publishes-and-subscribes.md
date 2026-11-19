---
status: accepted
date: 2026-11-18
---

# One connection publishes and subscribes: a duplex core session under `connectRelay`

A room client that both sends and watches media used to need several relay connections: a publisher connection per
namespace it announced, plus a subscriber connection. A participant held 3 and a host 4, each with its own token
refresh, reconnect and congestion controller. Neither MOQT nor the relay asks for that. The split came from the SDK.

- **The draft has no per-session role.** In draft-22 a subscription "can be established by either endpoint", and
  request IDs are numbered across the whole session, with even IDs from the client and odd IDs from the server
  (§6.4.2.1). PREFIX_OVERLAP applies per direction, so the client's own SUBSCRIBE_NAMESPACE can sit inside the
  relay's solicitation of the client's announces.
- **The relay builds every session with both halves.** moq-relay scopes a session's publish half and subscribe half
  from the token's `put` and `get` patterns, and runs both loops on every session. SOLICIT (0x40b5a) only stops
  unsolicited PUBLISH_NAMESPACE toward the client. The relay still solicits the client's announces from its publish
  grant.
- **The SDK core had two one-sided drivers**, inherited from Video.js v10: `session.ts` only subscribes and
  `publish-session.ts` only publishes, and each owned the whole transport. The publish driver also held a single
  namespace, which forced one connection per announced namespace.

A probe on ord (moq-relay 0.17, 2026-11-18) ran both drivers on one native QUIC connection, routing the relay's
requests by message type. Two peers each announced a namespace, opened discovery on the shared root and subscribed to
each other. Both directions delivered 30 of 30 objects, in every run. The shipped `connectRelay` passed the same check
with two namespaces per peer: all four tracks delivered 30 of 30 objects in 3 of 3 runs (`spec/behavior.md`).

We decided (#525):

- **moq-core gains `createMoqtDuplexSession`**, which runs the two drivers on one transport rather than merging them.
  The subscribe driver owns SETUP, the client's own requests and every incoming unidirectional stream. A router reads
  the message type that opens each bidirectional stream the relay starts, and sends SUBSCRIBE_NAMESPACE, SUBSCRIBE,
  TRACK_STATUS and FETCH to the publish driver. The publish driver gets a replayed SETUP and GOAWAY. The two drivers
  share one set of the peer's request IDs.
- **The publish driver holds a set of namespaces**: `announce()` adds one and `withdraw()` sends NAMESPACE_DONE for
  it. Track keys and loss reports carry the namespace.
- **`connectRelay` returns a `RelaySession`**: the `RelaySubscriber` surface plus `publish(namespace)`, which returns
  a `RelayPublication` with its own `announced`, `registerTrack`, `closed` and `close`. `connectRelayPublisher` and
  `connectRelaySubscriber` stay as they are. The name `connectRelay` used to belong to the subscriber entry point,
  which was renamed `connectRelaySubscriber`. Before 1.0 it can come back as the name of the connection that does
  everything a session can (`spec/vocabulary.md`, "Connections").
- **The session does not declare ACTIVE_COUNT**, because the publish driver doesn't send counts on the REQUEST_OK
  that answers a solicitation. Plain subscriber connections still declare it.

## Considered options

- **Wrap both drivers in a duplex session (chosen).** It reuses two drivers that already pass conformance, and leaves
  the one-sided connections unchanged. The cost is a router plus a synthetic control stream for the publish driver,
  and two teardown paths to keep in step (#527).
- **Merge the drivers into one MOQT session.** This would give one request path, one control reader and one teardown.
  It means rewriting about 3,500 lines in TypeScript and the same again in Python, for a request mix that had never
  been tested. We can still do it behind the same `connectRelay` surface.
- **Multi-namespace publisher only.** Letting one publisher connection announce several namespaces takes a host from
  4 connections to 2, but keeps two connections, two rotation clocks and two reconnect loops. Once a duplex session
  exists, its multi-namespace publisher API would be an interim shape to deprecate.

## Consequences

- **The relay hides a session's own routes from it (split horizon).** Discovery on a shared session doesn't list the
  client's own namespaces, and subscribing to its own track is refused with request error 16. Code that confirmed
  its own presence through discovery needs another way. `spec/behavior.md` records this, with the ord measurements.
- **A failure in either direction ends both.** A protocol error ends the whole connection, and a session GOAWAY
  from the relay moves both directions at once. The two halves already shared a relay, a network path and a token,
  so they mostly failed together anyway.
- **Each driver's callbacks are meant to end at its own `onClosed`, but don't always yet.** Both drivers have paths
  that can still call back after it (#527). The duplex session keeps the transport open while the publish driver
  drains, which widens that window.
- **Supervision is still per direction.** The supervised publisher and subscriber each open their own connection.
  The planned supervised `connectRelay` makes one connection one generation: every namespace it announces carries
  that generation, a reconnect brings everything back together as the next generation, and a rotation moves the
  publications and the wants to the new connection before the old one closes.
- **Python has it too, unsupervised.** `connect_relay` and `create_moqt_duplex_session` port the same
  design over the Python core's two drivers, and passed the same ord check (`spec/behavior.md`). A
  Python `RelaySession` is not a `RelaySubscriber` instance, because its `close()` is a coroutine and a
  subscriber's is not. The supervised Python counterpart is not built yet.
