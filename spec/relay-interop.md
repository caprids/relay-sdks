# moq-relay interoperability

Both SDKs offer `moqt-22`, then `moqt-20`, and speak whichever the relay selects; moq-relay selects
`moqt-22` from 0.15.0 ([draft-22 LOCATION_FILTER framing](#draft-22-location_filter-framing)).
Their `LARGEST_OBJECT` message parameter is two bare
varints (Group, Object), matching draft-22 §9.20 and moq-relay from **0.14.17** through
**0.17.0**, the production relay since 2026-10-05 (`SubscribeOk` in `rs/moq-net/src/ietf/subscribe.rs`
at tag `moq-relay-v0.17.0`). [Upstream fix](https://github.com/moq-dev/moq/pull/3561).

Older moq-relay releases used a length prefix for this parameter. That encoding cannot
be safely auto-detected: coordinate adoption of these SDK builds with the relay upgrade.
Old SDK builds fail on subscriptions to populated tracks after the relay upgrade; these
SDK builds misdecode or reject the old relay's populated-track responses.

`INCLUDE_PROPERTIES` (`0x35`) is one raw byte with no length, matching draft-22 §9.20.21 and
moq-relay **0.17.0** (`Subscribe` in `rs/moq-net/src/ietf/subscribe.rs` at tag `moq-relay-v0.17.0`).
0.15.8 and earlier length-prefixed it. Neither relay version sends it upstream, so only an
application that sets `include_properties` / `includeProperties` on a SUBSCRIBE sees the change.
An SDK build from before [PR #246](https://github.com/muxinc/relay-sdks/pull/246) that sets it
against 0.17.0 gets its whole session closed with PROTOCOL_VIOLATION (0x3) `wrong frame size`
(measured on production on 2026-10-05, 3 of 3); a current build against a relay older than 0.17.0
is misread the other way.

## Draft-22 LOCATION_FILTER framing

Draft-22's one wire change from draft-20 is the LOCATION_FILTER parameter (`0x21`): it opens with a
Location Filter Type that says which fields follow and has no length
([draft-22 §9.20.9](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#section-9.20.9)).
Draft-20 length-prefixed up to four fields and read the meaning from how many were present
([docs/research/moqt-22-delta.md](../docs/research/moqt-22-delta.md)). The SDKs encode the filter
for the negotiated draft. moq-relay negotiates `moqt-22` from 0.15.0, but through 0.17.2 it frames
LOCATION_FILTER the draft-20 way on every draft from 20 on (`Filter::is_draft20` and
`Param for Filter` in `rs/moq-net/src/ietf/filter.rs` at tag `moq-relay-v0.17.0`). moq-relay
0.17.3 fixed this: it frames the filter by the draft-22 rules on `moqt-22`
([moq-dev/moq#5080](https://github.com/moq-dev/moq/pull/5080), for
[moq-dev/moq#4847](https://github.com/moq-dev/moq/issues/4847)). As of 2026-10-20 the OCI relays
run 0.17.5 and have the fix, but `*.relay.mux.global` and the Fly r1 fleet run 0.17.0 and do not
([issue #275](https://github.com/muxinc/relay-sdks/issues/275)). On 0.17.0 the two spellings are
the same bytes when the field count equals the type and every field is under 128
(`spec/vectors/location-filter.json`), so most filters work; the ones that do not are:

| filter | draft-22 bytes the SDK sends | what moq-relay 0.17.0 does on `moqt-22` |
| --- | --- | --- |
| Next Object (`next-object` / `NextObjectFilter`) | `21 05` | reads `05` as a length and closes the session with PROTOCOL_VIOLATION (0x3) `short buffer` |
| open-ended absolute `{0,0}` | `21 02 00 00` | reads it as Next Object, the draft-20 meaning of those bytes |
| any start group, start object, end delta or end object of 128 or more | type, then multi-byte fields | misreads the length and the fields that follow |

Measured on production (sjc, 0.17.0) on 2026-10-06: a subscription with no filter, the explicit
no-filter type `0x00`, `relative-group` 1 and an absolute start at group 3 were all accepted and
delivered on `moqt-22`; Next Object closed the session as above. The relay's own upstream SUBSCRIBE
to an SDK publisher carries `21 02 00 00` (Next Object, in the draft-20 framing) and a fill of
`21 01 01`, which read under draft-22 as an absolute start at `{0,0}` and a relative start of 1. No
SDK publisher acts on a subscription's filter, so that changes nothing. On `moqt-20` every shape
works, Next Object included. Until production runs 0.17.3 or later, a live-edge subscriber uses a
relative start of 0 (the next group) rather than Next Object, as the live smoke tests do, or leaves
the filter out ([issue #275](https://github.com/muxinc/relay-sdks/issues/275)).

Publishers still retain no history and advertise no `LARGEST_OBJECT` in SUBSCRIBE_OK (their
TRACK_STATUS_OK does carry it, [below](#track_status-on-moq-relay-main)). The relay's
Next Object + current-group `FILL_PARAMETERS` pull (since 0.14.17, unchanged through 0.17.0, whose
upstream SUBSCRIBE was observed live on 2026-10-05) therefore has no cached publisher
content to fill; the publisher serves subsequent writes. Adding publisher retention and
backfill is a separate feature. The test fixtures send the pull parameters of 0.14.17 to 0.17.0.

## Publisher restarts and resubscription

Measured against production moq-relay 0.17.0 on 2026-10-05 with the Python SDK, three runs per
case and six for a crash (killed or stopped with SIGSTOP); on 0.15.8 on 2026-09-28 the
resubscription rows were measured with TypeScript too ([issue
#34](https://github.com/muxinc/relay-sdks/issues/34), [issue
#21](https://github.com/muxinc/relay-sdks/issues/21)). A replacement publisher announced the same
namespace about 1 s after the first one ended.

| how the publisher ends | existing subscriptions | new subscribes |
| --- | --- | --- |
| `close()` on the publisher | done(2) `track ended` within 0.1 s (3 of 3; on 0.15.8 some runs gave 0 `internal error`) | request error 16 `unroutable` until the replacement announces, then served by it at once |
| its connection closes without MoQ cleanup | done(0) within 0.1 s | the same |
| the process is killed or stopped | silent, then done(0) `internal error` after about 11 s (10.9-11.35 s, 6 of 6), the relay's QUIC idle timeout; 30-35 s on 0.15.8 | accepted, but served only the dead publisher's cached last object until the replacement announces; after that, served by the replacement at once in 3 of 6 runs, and in the other 3 still the dead publisher's cached object until the idle timeout |

In every case the existing subscriptions are never moved to the replacement: they end with the old
publisher, and only a new subscribe reaches the new one. Until the idle timeout the relay still
holds the crashed publisher's announcement next to the replacement's, and from 0.17.0 it does not
prefer the newer of two (see [duplicate announce](#duplicate-announce) below), so a new subscribe
can land on the dead one. A crashed peer is noticed only at the QUIC idle timeout, 10 s from 0.17.0
(`DEFAULT_IDLE_TIMEOUT` in `rs/moq-tokio/src/quic.rs:90` at tag `moq-relay-v0.17.0`, upstream
`a834dfdea`; 30 s at 0.15.8).

Re-subscribing to a finished track gets one of two answers, depending on whether its publisher is
still there, and neither delivers new content:

- **The publisher has gone.** Every re-subscribe gets request error 16 `unroutable`, because the
  relay answers `Unroutable` for an ended broadcast (`rs/moq-net/src/model/broadcast.rs:813-830` at
  tag `moq-relay-v0.17.0`).
  A loop that re-subscribes from the done callback runs only at the application's own retry cadence:
  over five publisher generations each subscription got exactly one done and every object arrived
  once (75 / 75), with and without cancelling, at 10 ms and 300 ms retry delays (0.15.8). Against 0.14.17 a
  closing broadcast still served its tracks, so the same loop replayed cached objects and done at
  round-trip cadence; that was the storm reported in issue #21.
- **The publisher stays connected and announced after the track ends cleanly** (its track writer
  closed). The relay keeps a finished track in a live broadcast cached and serves it again by design
  ("Publish new content under a new name", `broadcast.rs:836-850`): every re-subscribe gets
  SUBSCRIBE_OK, the cached last group again, an end-of-track object and PUBLISH_DONE status 2, about
  one round trip (50 ms) after the OK. Re-subscribing from the done callback, with the publisher
  held for 3 s over three track generations (Python rows identical on 0.15.8 and on 0.17.0, 3 of 3
  runs each; TypeScript rows measured on 0.15.8):

  | SDK | retry delay | dones | objects delivered / published |
  | --- | --- | --- | --- |
  | Python | 10 ms | 57 | 99 / 45 |
  | Python | 300 ms | 21 | 63 / 45 |
  | TypeScript | 10 ms | 51 | 93 / 45 |
  | TypeScript | 300 ms | 21 | 63 / 45 |

  A loop that keeps re-subscribing keeps the track cached: with the publisher held for 40 s at a
  300 ms retry delay, it replayed the track 89 times. From 0.17.0 a finished track that nobody reads
  for about 30 s (`TRACK_IDLE_LINGER`, upstream `166f9e87b`) is forgotten, and a subscribe then gets
  request error 16 `not found` while the publisher stays announced, and `unroutable` once it has
  gone (measured on 2026-10-05, 3 of 3: a re-subscribe 1 s after the end was replayed, 36 s after
  it was `not found`).

A done with status 2 does not say on its own why the subscription ended: moq-relay also sends it
when this viewer calls `update()` on a live track
([deviation 4](behavior.md#relay-interop-deviations)). Tell the two apart by what the application
knows:

- **The done followed this subscription's own `update()`.** The track is still live; re-subscribe
  with the new parameters. That is the documented way to change them against moq-relay.
- **The publisher ended the track** (the application knows from the publisher side, its catalog or
  its own discovery). Do not re-subscribe to it: wait for a signal that new content exists (a new
  track name in the catalog, or a new namespace or announce seen through [discovery](#namespace-discovery)),
  and back off any re-subscribe you do make. Publish restarted content under a new track name rather
  than reusing the finished one.

Each `subscribe()` creates an independent request, including its starting location.
It does not resume the preceding subscription. The SDK does not retry automatically
or suppress objects received by new subscriptions.

Applications that follow a restarting publisher should apply these policies:

- Keep at most one pending retry per logical track. Schedule retries for done
  notifications as well as retryable request errors; a delay used only for the
  fresh-namespace rejection does not bound an accepted-subscription/done loop. Cancel pending retries
  when the application stops, changes source, or closes the connection.
- Use capped exponential backoff with jitter and an application retry budget. For
  example, start around 500 ms and grow to a 5 s cap. Do not reset backoff solely on
  `SUBSCRIBE_OK` or a cached object: neither proves that a new publisher is live.
  Reset only on application-defined progress, such as advancing sequence numbers
  from the expected publisher generation. Backoff limits request load; it does not
  eliminate replay or guarantee recovery.
- Treat done as a control notification, not a data-drained signal. Trailing objects
  may arrive afterward. Define how long to drain a retired subscription before
  cancelling it, or explicitly accept losing trailing objects when cancelling
  immediately. Bound the number and lifetime of retired subscriptions.
- If continuity or duplicate suppression matters, put a publisher generation and
  sequence number in the payload and define the application's replay policy.
  Group/object IDs alone are insufficient across restarts because a new publisher
  can restart its counters. Do not discard all lower group IDs as stale.
- Choose subscription location filters to match the application's history needs.
  A new subscription's filter is not an exactly-once resume token across publisher
  generations, and opening another connection is not a cache invalidation guarantee.

Record done status and reason for diagnosis. Around a publisher shutdown both `track ended` (2)
and `internal error` (0) occur, and a crash ends with 0; neither identifies a new publisher
generation. Preserve actual errors rather than interpreting all status-0 responses as harmless
shutdowns.

## Duplicate announce

When two publishers announce the same namespace, moq-relay 0.17.0 routes new subscribes to one of
the two connections, effectively at random: `route_order` (`rs/moq-net/src/model/origin.rs:633-649`
at tag `moq-relay-v0.17.0`) ranks equal routes by a hash of the namespace and the route's hop chain,
and each publisher connection's announcements carry a random hop id (`stamp: Hop::random()` in
`rs/moq-net/src/ietf/subscriber.rs`), before it looks at which announcement is newer. Measured on
production on 2026-10-05 with the Python SDK
([issue #37](https://github.com/muxinc/relay-sdks/issues/37)), the second publisher won 6 of 12
runs. The choice was stable while both stayed connected, and the losing publisher got no demand.
When the serving publisher closed, its viewers got done(2) and did not move, and new subscribes
went to the other one at once. When it crashed, new subscribes kept getting its cached last object
until the idle timeout, and its viewers then got done(0). A local moq-relay 0.15.8 gave every new
subscribe to the newest announcer (6 of 6).

## Announce propagation

The relay acknowledges a NAMESPACE entry with nothing, and it routes subscribes to the namespace
only once it has processed the entry. Measured against `sjc.relay.mux.global` on 2026-09-23 with the
Python SDK: a fresh namespace per trial, the subscriber connection already open, three interleaved
runs of twelve trials per mode. The relay was moq-relay 0.15.1, which answered the race with 404.

| subscribe issued | rejected 404 |
| --- | --- |
| at once after `register_track`, before the announce left | 13 / 36 |
| after `announced` settled (within 1 ms of `register_track`) | 4 / 36 |
| after `announced` + 100 ms | 0 / 12 |
| after `announced` + 250 ms | 0 / 12 |
| after `announced` + 500 ms | 0 / 12 |

The relay answered every subscribe in about 50 ms, one round trip from that vantage point. So the
window after `announced` is about one round trip, no publisher-side fact can close it, and the
retry belongs to the application ([behavior.md](behavior.md#subscribing)). A subscriber in another
process can observe the announce only through [discovery](#namespace-discovery), which closes the
race on 0.17.0 (below). Earlier text said "about a second"; that was the retry
interval of the original reproduction, not the window.

moq-relay 0.15.8 (production from 2026-09-28) still raced, but answered request error 16 (0x10,
`DOES_NOT_EXIST`) with reason `unroutable` instead of 404
([issue #136](https://github.com/muxinc/relay-sdks/issues/136)). It sends the same code with reason
`not found` for an unknown track in an announced namespace, so only the reason tells the race apart
from a request no wait can satisfy. Measured on 2026-09-28 with the Python SDK through the examples'
retry, ten trials per mode: at once after `register_track`, 6 / 10 first attempts were rejected
`unroutable`; after `announced`, 3 / 10. Every rejected trial was accepted on the second attempt,
100 ms later, and a subscribe to an unknown track in each namespace came back `not found` once,
unretried.

moq-relay 0.17.0 (production from 2026-10-05) races less often but answers the same way, so the
retry is still needed. Measured on 2026-10-05 through the examples' retry: with the Python SDK, 2 / 30
first attempts at once after `register_track` and 1 / 30 after `announced` were rejected
`unroutable`; with the TypeScript SDK, 4 / 10 and 0 / 10. Every rejected Python trial was accepted
on the second attempt, and an unknown track still came back `not found` in both SDKs.

A subscribe sent on the subscriber's own connection once its [discovery](#namespace-discovery)
reports the namespace `announced` was never rejected
([issue #269](https://github.com/muxinc/relay-sdks/issues/269)). Measured on 2026-10-06 against
moq-relay 0.17.0, publisher on `sjc.relay.mux.global`, subscriber on sjc or on `fra.relay.mux.global`:
a fresh namespace and publisher connection per trial, the subscriber connections already open, one
subscribe at once after the trigger, then retries every 100 ms. Both SDKs (TypeScript in Node over
native QUIC), four interleaved runs of ten trial pairs per SDK and relay, the two modes alternating
within each pair:

| subscribe issued | subscriber on sjc | subscriber on fra |
| --- | --- | --- |
| at once after the publisher's `announced` settles (baseline, Python) | 6 / 40 rejected | 40 / 40 rejected |
| at once after the publisher's `announced` settles (baseline, TypeScript) | 14 / 40 rejected | 40 / 40 rejected |
| at once after discovery reports `announced` (Python) | 0 / 40 rejected | 0 / 40 rejected |
| at once after discovery reports `announced` (TypeScript) | 0 / 40 rejected | 0 / 40 rejected |

Every rejection was request error 16 `unroutable`, and every rejected trial was accepted on the
second attempt, 100 ms later. Discovery reported the namespace a median 50 ms after the publisher's
`register_track` / `registerTrack` on sjc and 147 to 151 ms (at most 283 ms) on fra. The window on
another relay is correspondingly wider: a subscriber there that subscribed as soon as the
publisher's `announced` settled lost the race every time. The relay builds NAMESPACE entries from the same origin it
routes subscribes on (`run_subscribe_namespace_stream` → `origin.announced()` in
`rs/moq-net/src/ietf/publisher.rs` at `moq-relay-v0.17.0`), including a route learned from a cluster
peer, which matches what was measured. That is an observation of 0.17.0, not a guarantee the
protocol makes.

## Namespace discovery

moq-relay 0.17.0 serves SUBSCRIBE_NAMESPACE from client sessions
(`run_subscribe_namespace_stream` in `rs/moq-net/src/ietf/publisher.rs` at tag
`moq-relay-v0.17.0`). On draft-20 and draft-22 it answers REQUEST_OK without a request id, then writes NAMESPACE
and NAMESPACE_DONE entries named relative to the prefix. Its source is the relay's origin, which
includes routes learned from cluster peers.

- Which of two paths carries the advertisements depends on the peer's SETUP
  (`rs/moq-net/src/ietf/solicit.rs`). A peer that sends no SOLICIT option gets an unsolicited
  PUBLISH_NAMESPACE for everything in scope right after SETUP, and its SUBSCRIBE_NAMESPACE streams
  stay open but carry nothing. A peer that sends SOLICIT=1 gets only the solicited path. The relay
  itself always sends SOLICIT=1. A peer that writes the option at all, even as 0, declares it
  implements the extension, and the relay then holds it to never advertising unasked. The subscriber
  connection sends SOLICIT=1 ([behavior.md](behavior.md#discovery)). Before this, both SDKs refused
  the pushed PUBLISH_NAMESPACE with NOT_SUPPORTED and retry interval 0, which the relay reads as
  "never offer this again" (`refusal()` in `publisher.rs`) for the rest of the session.
- A prefix outside the session's grant gets REQUEST_OK and no entries, not an error.
- It does not answer an overlapping SUBSCRIBE_NAMESPACE with PREFIX_OVERLAP, as draft-22 §9.15
  requires: a probe on 2026-10-06 held `[a, b, c]` and `[a, b, c, pre]` open on one session, and
  both received the namespace they share. The SDKs refuse an overlapping prefix before sending it, so
  applications see the same rule against any relay.
- The relay writes NAMESPACE again for a namespace it already advertised when that namespace's
  route changes, so entries are not edge-triggered on the wire. The SDKs drop those repeats.
- Measured on 2026-10-06 against sjc, and from fra for a publisher on sjc (TypeScript SDK, Node):
  the snapshot arrived one round trip after the request (~50 ms, ~110 ms). A new announce arrived
  ~50 ms (~140 ms) after the publisher's `announced`, and a clean close was withdrawn within
  ~400 ms. A SIGKILLed publisher was withdrawn after ~12.2 s (~12.3 s), at the QUIC idle timeout.
- A subscribe on the same connection at once after `announced` was never rejected `unroutable`:
  0 of 160 trials on 2026-10-06, in both SDKs, on the publisher's relay and on another one, against
  6 to 14 of 40 (same relay) and 40 of 40 (other relay) for a subscribe right after the publisher's
  `announced` ([announce propagation](#announce-propagation)).

## Endpoint paths

moq-relay takes the endpoint URL's path, split on `/`, as the namespace prefix of a connection
([room-rooted endpoints](behavior.md#room-rooted-endpoints)). It does not percent-decode the
segments. Measured on 2026-10-20 on `ord.relay.mux.global` (moq-relay 0.17.5) and
`sjc.relay.mux.global` (0.17.0), for [issue #348](https://github.com/muxinc/relay-sdks/issues/348).
A Python publisher (native QUIC) connected to `<host>/<root>/<run>/<segment>` with the namespace
`['t']`, and bare-host discovery named the namespace the relay derived. The TypeScript smoke test,
with a room field `a b`, failed in the same way on Node (native QUIC) and Chromium (WebTransport).
Its room publisher at `.../a%20b/...` never appeared under the bare `a b` namespace.

| Path segment | Field the relay derived |
| --- | --- |
| `a%20b`, `a%25b`, `a%2Fb`, `a%2fb`, `a%2Eb`, `%2E%2E`, `%C3%A9` | the same text, `%` sequences included: never `a b`, `a%b`, `a/b`, `a.b`, `..` or `é` |
| raw `é` (UTF-8 bytes in PATH, native QUIC only; a browser URL encodes it) | `é` |
| `a~b`, `a!b`, `a$b`, `a&b`, `a'b`, `a(b)`, `a+b`, `a,b`, `a;b`, `a=b`, `a:b`, `a@b` (ord only) | the same text |
| `a*b` (ord only) | none: the connection fails with `UNAUTHORIZED` |

An encoded `/` is a literal part of a field, not a separator and not a refusal. A browser cannot
send a raw space or non-ASCII character in a path, and the WHATWG URL parser folds `.`, `..` and
their encoded forms into the path before the request leaves. `roomEndpoint` / `room_endpoint`
therefore refuses those fields instead of encoding them. The live smoke test passed on ord with a
room field holding every character the helper allows
(`RELAY_NAMESPACE_PREFIX="<root>/a-._~!$&'()+,;=:@Z9"`), in Python and on all four TypeScript
runtimes, QMux over WebSocket included. Encoded segments were not measured over WebSocket.

## Supervised generations

Measured on 2026-10-06 against `sjc.relay.mux.global` (moq-relay 0.17.0) with the TypeScript SDK
([behavior.md](behavior.md#supervised-connections), [issue #260](https://github.com/muxinc/relay-sdks/issues/260)):

- **A newer generation while the old one is still up.** A want following a root moved to a second
  supervised publisher's generation, and received its objects, about 200 ms after that publisher
  connected. That was one smoke run in Node and one in Chromium. The relay still held the first
  generation's announce, so this is the
  [duplicate announce](#duplicate-announce) case resolved by the generation order rather than the
  relay's hash ranking.
- **In-process reconnect.** The supervised publisher's native QUIC transport was closed from
  underneath it while it wrote one object every 50 ms. Reconnection backoff started at 200 ms. In
  5 of 5 runs the next generation was up 515 to 595 ms after the close, and the subscriber's want
  received its objects 656 to 742 ms after it. Writes in between failed at once with
  `RECONNECTING` (10 or 11 per run), and nothing was replayed. The want restarted with reason
  `generation` when the old generation was withdrawn, or `publisher-done` when the done arrived
  first.
- **The same with the Python SDK** (native QUIC on aioquic, same day and relay). The generation
  switch took 202 to 203 ms in 4 of 4 smoke runs. In the in-process reconnect, 5 of 5 runs had the
  next generation up 624 to 741 ms after the close and the want receiving its objects 764 to 881 ms
  after it, with 6 to 8 writes failing at once with `RECONNECTING`. The write in flight at the close
  failed with `TRANSPORT_ERROR` instead: closing the transport locally makes aioquic drain for about
  300 ms before the session ends, longer than the supervisor waits for the end to explain a failure.
  A relay's close or an idle timeout ends the session in the same turn its streams fail.

A transport closed this way still sends a QUIC close, so the relay withdraws the old generation
within a round trip. A crash leaves it announced until the idle timeout; the newer generation
still wins, as in the first case.

## Token expiry

A session lasts until its credential expires, and a live session cannot take a new one
([behavior.md](behavior.md#credential-lifetime), [issue #261](https://github.com/muxinc/relay-sdks/issues/261)).
From moq-relay 0.17.0's source:

- The session holds a lease on its grant. At the grant's deadline, `Lease::ended()` returns
  `Expired` and the connection calls `session.abort(Unauthorized)` (`rs/moq-relay/src/auth.rs`,
  `Lease::ended`; `connection.rs`, the `lease.ended()` arm). The deadline is the JWT's `exp` with
  the reference auth server (`moq auth serve`), and Switchboard caps every grant at the token's `exp`
  too.
- AUTHORIZATION_TOKEN on a later request is ignored: the session's grant authorizes every request
  (`rs/moq-net/src/ietf/subscribe.rs`). Revalidation can move the deadline, but it re-checks the
  connect request the session was admitted with, not a new credential.

Measured on 2026-10-06 against a local moq-relay 0.17.0 (`--auth-url` pointing at a test auth
server that granted until the token's `exp`), with the TypeScript SDK over native QUIC:

- **Expiry.** A publisher and a subscriber connected with a token 6 s from its `exp`. Both sessions
  closed 81 to 83 ms after it, with session termination code 0x2 and reason `unauthorized`, which
  the SDK reports as `UNAUTHORIZED` in phase `connection`. The subscriber's transport reported the
  close through `closed`, the publisher's as a session error on a stream. The auth server received
  `end` with reason `expired`. The Python SDK measured the same, 82 ms after `exp`. In headless
  Chromium the subscriber reported `UNAUTHORIZED` too, within a few milliseconds of `exp`, but the
  publisher's control stream failed (`NetworkError: The stream was aborted by the remote server`)
  before WebTransport's `closed` carried code 0x2, so it reported `TRANSPORT_ERROR`
  ([issue #279](https://github.com/muxinc/relay-sdks/issues/279)).
- **Rotation.** A supervised publisher wrote one object per group every 40 ms for 40 s, and a
  supervised subscriber followed its root. Both took 12 s tokens (the subscriber's 15 s) and rotated
  5 s before expiry. The publisher rotated 5 times and the subscriber 4 times. All 989 objects
  arrived once each, in order, with no gap longer than the 42 ms between writes, no failed write,
  and the status `live` throughout. Each publisher rotation restarted the want with reason
  `generation`, each subscriber rotation with `rotation`. Before the replacement subscription
  skipped what the old one had delivered, each subscriber rotation repeated one object. The Python
  SDK, run the same way, delivered 964 of 964 objects with no gap longer than 43 ms. So did headless
  Chromium over WebTransport for 30 s: 4 publisher and 3 subscriber rotations, 737 of 737 objects,
  no gap longer than 43 ms.

To reproduce, run moq-relay with `--auth-url` pointing at `moq auth serve --key ...`, and sign
short-lived tokens with `moq auth sign --expires`.

## Relay demand

A publisher's demand is the relay's one upstream subscription for a track, not a count of
downstream viewers. Measured against `sjc.relay.mux.global` (moq-relay 0.15.8) on 2026-09-28 with
the Python SDK ([issue #137](https://github.com/muxinc/relay-sdks/issues/137)); on 0.17.0 on
2026-10-05 demand still ended about 50 ms after the last subscriber left:

| scenario | result |
| --- | --- |
| the only subscriber cancels, connection kept open | demand ends 50-51 ms later (3 of 3) |
| the only subscriber closes its connection | demand ends 50-51 ms later (3 of 3) |
| a second subscriber joins while the first is present | no new demanded value; the second receives the catalog from the relay's cache |
| the first of two subscribers leaves | demand stays; it ends 50 ms after the second leaves |
| a subscriber joins 2-35 s after the last one left | a new demanded value, so the catalog is re-sent (12 of 12) |
| the same connection resubscribes within about 30 s of cancelling | it receives the previous group from the relay's cache, then the re-sent one (6 of 6 at gaps of 2-20 s; 0 of 2 at 35 s; 0 of 4 from a new connection) |
| 200 subscribe/receive-one/cancel cycles in 19.3 s on one connection | 185 demanded values and 185 releases; the last release 51 ms after the last cancel |

So the relay coalesces concurrent viewers into one subscription and cancels it as soon as the last
one leaves, about one round trip later. moq-net 0.15.0 removed the hold: the relay's front drops
the source track when its last reader leaves, and its 30-second `TRACK_IDLE_LINGER` now keeps only
a cached copy of delivered groups, holding no upstream subscription (`rs/moq-net/src/model/origin.rs`
at `moq-relay-v0.15.8`, still 30 s at `moq-relay-v0.17.0`). `has_demand` / `hasDemand` is not a
liveness signal and not a reason to
keep or stop encoding, and a catalog re-sent on each demanded value is what the relay caches for
every later viewer: write only state that is safe to serve from cache, and expect a viewer to see
a document more than once.

### A viewer on another relay

When the first viewer of a track is on a different relay from the publisher, or is a moq-lite
viewer on any relay, the publisher sees two relay subscriptions, not one
([issue #243](https://github.com/muxinc/relay-sdks/issues/243),
[issue #150](https://github.com/muxinc/relay-sdks/issues/150)). Relays mesh over moq-lite, and a
lite-05 subscriber fetches a track's TRACK_INFO before it subscribes (`TrackInfoFetch` in
`rs/moq-net/src/lite/subscriber.rs` at `moq-relay-v0.17.0`). The publisher's relay can learn the
info of an IETF publisher's track only from a SUBSCRIBE_OK, so it subscribes to answer the query
(`TrackInfoServe` in `rs/moq-net/src/lite/publisher.rs`, `run_subscribe` in
`rs/moq-net/src/ietf/subscriber.rs`). Nothing reads that track, so the relay cancels the
subscription as unused about 50 ms later, and the downstream relay's real SUBSCRIBE starts a new one
after the info has made the trip back. Measured on the r1 fleet on 0.15.8 (sjc publisher, two-hop
path to fra):

```text
59.642 sjc ietf subscriber: subscribe started   track=events   (answers the TRACK_INFO query)
59.692 sjc ietf subscriber: subscribe cancelled track=events
60.072 sjc lite publisher:  subscribed started                 (the mesh SUBSCRIBE from iad)
60.072 sjc ietf subscriber: subscribe started   track=events
```

Objects written to the first subscription reach no viewer. A write still in flight when it is
cancelled fails with `SUBSCRIPTION_CHANGED`; a write that completed before the cancel was accepted by
the transport and is dropped by the relay without an error. Against local relays the first object
of a continuous publisher was lost in every run, and on the fleet the remote viewer's first object
was seq 1 or 2 on three of four paths. An IETF viewer (either SDK) on the publisher's own relay
sends no TRACK_INFO query and gets one subscription (3 of 3 on 0.17.0 on 2026-10-05). A moq-lite
viewer (the `moq` CLI, `@moq/watch`) is a lite subscriber itself, so it causes the same subscribe,
cancel, subscribe on the publisher's own relay: 11 of 11 runs on 0.17.0 had two upstream
SUBSCRIBEs, the first cancelled about 50 ms after it started
([issue #150](https://github.com/muxinc/relay-sdks/issues/150)). The SDKs cannot tell the two
subscriptions apart: each is a demanded value, the first followed by `false` ([ADR 0001](../docs/adr/0001-one-relay-subscription-per-track.md)).
Because publishers retain no history, the second subscription's current-group fill has nothing to
recover either. A live publisher therefore cannot count on its first objects after demand begins
reaching a viewer: write state-shaped tracks on every demanded value (the catalog service does), and
start media at a keyframe after any change (the frame writer does). The live smoke tests repeat a
warm-up marker on each track until the subscriber receives one before writing what they assert, and
`RELAY_SUB_URL` puts their subscribers on another relay
([issue #244](https://github.com/muxinc/relay-sdks/issues/244)).

#### TRACK_STATUS on moq-relay main

moq-relay `main` from [moq-dev/moq#5991](https://github.com/moq-dev/moq/pull/5991) (`916cd4543`,
2026-10-20; in no release through 0.17.7) learns the track another relay asks about with
TRACK_STATUS instead of a SUBSCRIBE: when the publisher's relay has no subscription for the track,
its IETF subscriber sends the publisher TRACK_STATUS (`track_status` in
`rs/moq-net/src/ietf/subscriber.rs` at `8810d7365`), answers the downstream relay's TRACK_INFO from
the TRACK_STATUS_OK, and subscribes only once the downstream relay does. A refusal fails the
downstream request, and #5991 removed the fallback to SUBSCRIBE that #5409 had added, so the
downstream relay retries forever. Draft-22 lets a relay do either
([Section 9.13](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#section-9.13):
relays "MAY forward TRACK_STATUS ... or MAY initiate a subscription"), and lets a non-relay endpoint
answer it NOT_SUPPORTED ([Section 1.6](https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html#section-1.6)),
which is what both SDKs' publishers did until they answered it
([behavior](behavior.md#publishing)).

Measured on 2026-10-26 on two local relays built from `main` at `8810d7365` (the voice benchmark's
relay configuration with its `[cluster.api]` keys renamed by hand, its Switchboard and its CA;
publisher on one relay, subscriber on the other, over `moqt-22`), with the live smoke tests:

| publisher | one hop | two hops | the publisher's relay logs, per track asked about |
| --- | --- | --- | --- |
| refuses TRACK_STATUS (`NOT_SUPPORTED`) | pass | fail in every runtime: Node over native QUIC, Chromium WebTransport, Node and Chromium WebSocket (TypeScript), and aioquic (Python) | `track status refused err=unsupported`, then `track info error ... err=unsupported` to the other relay, which logs `track info failed`, once per retried SUBSCRIBE |
| answers TRACK_STATUS | pass | pass, in the same five runtimes | `track status known`, then one upstream `subscribe started` under a millisecond later |

On moq-relay 0.17.7 (`e2c5b50`) the relay never sends TRACK_STATUS, and both publishers pass one
and two hops the same way; the publisher's relay there still subscribes twice per track asked
about from the other relay (subscribe, cancel, subscribe, as above), where `main` subscribes once
(32 of 32 tracks each). The early-object loss above is therefore a 0.17 behavior; on `main` with a
publisher that answers TRACK_STATUS it was not seen, though the smoke tests' warm-up markers would
hide a single lost object.

Relays before 0.15 held the upstream subscription for about 30 seconds after the last viewer left
(measured against the same endpoint on 2026-09-15, [issue #24](https://github.com/muxinc/relay-sdks/issues/24));
that is no longer the case.

## Late subscribers and location filters

A plain SUBSCRIBE, with no LOCATION_FILTER or an explicit zero-field one, starts at the beginning
of the relay's latest group: a subscriber that joins an ongoing track receives that group's cached
objects and everything published after, and nothing older. `NextObjectFilter` is stricter and starts
after the relay's largest object, so it skips the rest of the current group. A relative-group or
absolute filter reaches further back into the relay's cache. moq-relay 0.17.0 resolves the filters
this way in `filter_range` (`rs/moq-net/src/ietf/publisher.rs` at tag `moq-relay-v0.17.0`).
Measured against production on 2026-09-28, on 0.15.8
([issue #138](https://github.com/muxinc/relay-sdks/issues/138)), and again on 0.17.0 on 2026-10-05
(3 of 3 runs): a publisher wrote groups up to 19, and every late subscriber joined between groups 9
and 10.

| subscriber | filter | groups received on 0.17.0 | on 0.15.8 |
| --- | --- | --- | --- |
| late | none (no LOCATION_FILTER sent), or explicit `NoLocationFilter()` | `[9..19]` | the same |
| late | `NextObjectFilter` | `[10..19]` | the same |
| late | `RelativeGroupFilter(groups_before_next=3)` | `[7..19]` | the same |
| late | `AbsoluteLocationFilter(start=group 2)` | `[2..19]` | `[4..19]` |

On 0.17.0 an absolute start at group 0 received `[1..19]`: the relay serves from the requested group
as far back as it still caches. On 2026-09-15, before production moved to 0.15.8
([issue #26](https://github.com/muxinc/relay-sdks/issues/26)), `NextObjectFilter` behaved the same
as no filter; from 0.15.8 it does not.

The draft defines an unfiltered subscription as passing all Objects published or received via
upstream subscriptions ([behavior.md](behavior.md#subscribing)); it does not require a relay to
replay what it cached before the subscription, so starting at the latest group is a legitimate
reading and not an SDK encoding problem. A late joiner that needs a decodable start or recent
history subscribes with `RelativeGroupFilter` / `{ type: 'relative-group' }` (`groups_before_next` /
`groupsBeforeNext` of `1` starts at the current group) or with an absolute start. How far back
either reaches depends on what the relay still holds; on 0.15.8 the absolute case above started
later than it asked for.

The table holds only for tracks published over moqt, as both SDKs publish. When the track's
publisher speaks moq-lite (the `moq` CLI, moq-rs, `@moq/net`), the relay treats the **first**
moqt subscriber differently: its SUBSCRIBE_OK carries no LARGEST_OBJECT, and within about 100 ms it
receives every group the publisher still retains, in one burst and out of order, whatever filter it
sent (none, `NextObjectFilter` or `RelativeGroupFilter`, on `moqt-20`; groups 0-8 in 3 of 3 runs
on 0.17.0, against 2 of 2 correct with the same producer publishing over moqt). Later subscribers are filtered
as the table says. A subscriber to a third-party moq-lite broadcast that wants the live edge drops
any group older than the newest it has seen. The fix belongs upstream in moq-relay
([issue #153](https://github.com/muxinc/relay-sdks/issues/153)).

## Subscription parameters

`MessageParameters` exposes every draft-22 SUBSCRIBE parameter, and both SDKs send whatever the
application sets. moq-relay decides which it acts on. Read from the source at tag
`moq-relay-v0.17.0` (`Subscribe` in `rs/moq-net/src/ietf/subscribe.rs`, the SUBSCRIBE serve path in
`rs/moq-net/src/ietf/publisher.rs`) and measured on production 0.17.0 on 2026-10-06
([issue #265](https://github.com/muxinc/relay-sdks/issues/265)):

| parameter | moq-relay 0.17.0 | effect for an application |
| --- | --- | --- |
| SUBSCRIBER_PRIORITY (`0x20`) | honored on SUBSCRIBE: it sets the priority the relay serves this viewer with. It is not passed upstream (the relay's own SUBSCRIBE carried 255 when the viewer asked for 7) | set it on the SUBSCRIBE; changing it later with `update()` ends the subscription ([deviation 4](behavior.md#relay-interop-deviations)) |
| publisher priority (track property `0x0E`) | carried in group headers and SUBSCRIBE_OK, but **not used** to order delivery; under congestion only subscriber priority does. Every group stream header carries the track's `0x0E`, or 128 when the track has none, whatever SUBSCRIBER_PRIORITY the viewer sent, and so does the SUBSCRIBE_OK a viewer gets unless it asked for no properties ([INCLUDE_PROPERTIES](behavior.md#relay-property-handling) = 0 empties the list): with and without `0x0E`, each subscribed with the media priorities, without SUBSCRIBER_PRIORITY and inverted, 3 of 3 per case on 0.17.0 (sjc) and 0.17.5 (ord) on 2026-10-20 ([#344](https://github.com/muxinc/relay-sdks/issues/344)). On ord's capped downlink, delivery followed SUBSCRIBER_PRIORITY in both cases: inverted, audio starved even when every header said 128 ([source](#group-stream-priority-in-source)) | an app protects audio by setting its subscriber priority. The publisher's is ignored even when subscriber priorities tie (3 of 3 on 0.17.0, [#154](https://github.com/muxinc/relay-sdks/issues/154)), so the media defaults (audio 16, video 96) do not change it ([behavior.md](behavior.md#media-publishing)); a viewer sets the same values as its subscriber priority with `mediaSubscribeParameters` ([behavior.md](behavior.md#media-subscribing)) |
| LOCATION_FILTER (`0x21`) | honored, with draft-20 framing on every draft (fixed in 0.17.3) | see [late subscribers](#late-subscribers-and-location-filters) and [LOCATION_FILTER framing](#draft-22-location_filter-framing) |
| GROUP_ORDER (`0x22`) | decoded and **ignored**: SUBSCRIBE_OK states Descending whatever the viewer asked for and whatever the publisher declared (DEFAULT_PUBLISHER_GROUP_ORDER), and the relay's upstream SUBSCRIBE always asks for Descending | do not rely on group order either way. A late subscriber that asked for Ascending got its cached groups in no consistent order in 9 of 10 runs (for example `[2..10, 1, 11, 12]`), and ascending once. Upstream `main` applies it ([moq-dev/moq#5385](https://github.com/moq-dev/moq/pull/5385)), in no release through 0.17.7 |
| NEW_GROUP_REQUEST (`0x32`) | decoded and **ignored**, on SUBSCRIBE and REQUEST_UPDATE: never forwarded upstream, neither in the SUBSCRIBE for a track without a subscription nor as a REQUEST_UPDATE for one already served, which draft-22 §9.20.19 requires of a relay | there is no protocol key-frame request through this relay. An application that needs one (a decoder error, a layer switch) carries the request on its own track back to the publisher |
| OBJECT_DELIVERY_TIMEOUT (`0x02`), SUBGROUP_DELIVERY_TIMEOUT (`0x06`) | decoded and ignored; not passed upstream | only the publisher's own values apply ([delivery timeouts](behavior.md#delivery-timeouts)) |
| AUTHORIZATION_TOKEN (`0x03`) on a request | ignored: the session's grant authorizes every request | authorize at connect time |
| FORWARD (`0x10`) = 0 | refused per request: REQUEST_ERROR 3 `FORWARD=0 not supported`, session stays open | see [deviation 4](behavior.md#relay-interop-deviations) |
| range filters (`0x25` to `0x28`) | refused per request: REQUEST_ERROR 3 `range filters not supported` | — |
| INCLUDE_PROPERTIES (`0x35`) | honored | one raw byte from 0.17.0; see the top of this page |

Neither SDK's publisher declares DYNAMIC_GROUPS (`0x30`) or acts on a NEW_GROUP_REQUEST, so a relay
that forwarded it would not change the outcome for an SDK publisher yet. The relay's upstream
SUBSCRIBE to an SDK publisher carries FORWARD 1, its own priority, GROUP_ORDER Descending, a
LOCATION_FILTER and a FILL_PARAMETERS, and nothing the viewer asked for beyond the track.

To re-check after a relay upgrade, run the probe below. It subscribes with each ignored parameter
set, records the relay's upstream SUBSCRIBE and any REQUEST_UPDATE as the publisher receives them,
and the group order SUBSCRIBE_OK states, and it exits non-zero, with the differing rows, when any
of those no longer match this table. It also prints the order a late subscriber's cached groups
arrive in, for reading only: QUIC streams have no cross-stream order, so that is not compared. It is
a relay fact, not an SDK contract, so only the Python SDK runs it.

```sh
RELAY_URL="$RELAY_URL" RELAY_NAMESPACE_PREFIX="$RELAY_NAMESPACE" mise run python:probe:relay-parameters
```

### Group stream priority in source

The header priority and the send priority come from different values. Read at tag
`moq-relay-v0.17.7` (the same code at `moq-relay-v0.17.5` and `moq-relay-v0.17.0`), in
`rs/moq-net/src/ietf/`:

- The relay keeps the publisher's `0x0E` from the SUBSCRIBE_OK it gets upstream
  (`subscriber.rs:2184`, `priority: msg.properties.priority`), and uses wire 128 when there is none
  (`subscriber.rs:1787`, `priority.unwrap_or(128)`).
- It stamps that value on every group stream header it sends a viewer (`publisher.rs:2499`,
  `publisher_priority: ...to_wire(self.track.info().priority)`). Its SUBSCRIBE_OK to the viewer
  carries the same value as `0x0E` (`publisher.rs:695`), unless the viewer set INCLUDE_PROPERTIES
  to 0, which empties the SUBSCRIBE_OK properties; the headers carry it either way.
- It opens each group stream at the viewer's SUBSCRIBER_PRIORITY (`publisher.rs:2711`,
  `stream.set_priority(self.applied)`, from the subscription's priority at `publisher.rs:2362`).
  That is the value the QUIC send scheduler orders by.

To re-measure the header priority, run the TypeScript probe with and without the media track
properties. It prints each track's SUBSCRIBE_OK `0x0E` and the header priorities that arrived
(`headerPriorities`). Run it with `SUBSCRIBER_PRIORITY` set to `on`, `off` and `invert`, from `ts/`
after `mise run ts:build` and `mise run ts:build:native`:

```sh
RELAY_URL="$RELAY_URL" RELAY_TOKEN="$RELAY_TOKEN" RELAY_NAMESPACE_PREFIX="$RELAY_NAMESPACE" \
  PRIORITY_TRACK_PROPERTIES=off SUBSCRIBER_PRIORITY=invert node scripts/probe-subscriber-priority.mjs
```

## Subgroup resets

moq-relay 0.15.8 forwards a subgroup reset once it has begun forwarding that subgroup, but not at
once. Measured against production on 2026-09-28
([issue #138](https://github.com/muxinc/relay-sdks/issues/138)): a publisher wrote objects to a
group and aborted it after the relay had forwarded some of them. All 10 of 10 such aborts reached
the subscriber as a subgroup reset, and each arrived only when the publisher's next group began:
about 0.5 s after the abort at the publisher's normal cadence, and 3 s after it when the publisher
paused 3 s before its next group. A subscriber that must react to an abandoned group sooner than the
next group cannot rely on the reset's timing. On 0.17.0 on 2026-10-05, every partly forwarded group
aborted in the reset-code runs below reached the subscriber as a reset, about 0.55 s after the abort;
the timing relative to the next group was not re-measured.

A reset for a subgroup the relay never began forwarding is unverified on 0.15.8. On 2026-09-15,
before production moved to 0.15.8 ([issue #38](https://github.com/muxinc/relay-sdks/issues/38)), a publisher
wrote two small objects to a group and aborted it: one subscriber received the reset, and in another
run a subscriber received nothing for that group, neither objects nor a reset, and its first group
was the next one. In the close path of [issue #30](https://github.com/muxinc/relay-sdks/issues/30),
where 8 MiB objects had been partly forwarded, the relay did propagate the reset. The exact
condition (bytes forwarded, time since the first object, or cache policy) was not isolated.

A subscriber therefore cannot rely on a reset to learn that a group was abandoned. A missing group
ID is the only trace, and it is indistinguishable from a group the publisher never sent. An
application that must detect loss carries its own sequence numbers in payloads.

The reset code does not carry the publisher's reason. moq-relay re-codes every subgroup reset it
forwards as 1, whatever code the publisher sent, and a PUBLISH_DONE status other than 2
`track ended` reaches viewers as 0 `internal error`
([issue #156](https://github.com/muxinc/relay-sdks/issues/156)). Over native QUIC (the Python SDK,
and the TypeScript SDK in Node) the code also arrives in its WebTransport (HTTP/3) encoding, because
the relay's raw-QUIC sessions still map codes as for WebTransport: code 1 arrives as
`0x52E4A40FA8DC` (91141958510812) in `stream_error_code` / `streamErrorCode`
([issue #248](https://github.com/muxinc/relay-sdks/issues/248)). Measured on production 0.17.0 on
2026-10-05 (3 of 3 per case, both SDKs) and the same on a local 0.15.8. Treat a subgroup reset as
"this group was abandoned" and nothing more.

## Voice delivery on 0.17.5: deadlines, FETCH and group order

What a voice application can tune on the relay's side, measured on a local moq-relay 0.17.5 (tag
`moq-relay-v0.17.5`, the OCI fleet's image, with no cache settings, as the fleet runs it) on
2026-10-17 with the Python probe below, and read in its source (`rs/moq-net/src/ietf/subscribe.rs`,
`publisher.rs`). The publisher wrote one-object groups every 20 ms, with track Timescale 1,000,000
and a LOC Timestamp on every object. For the slow-link rows the subscriber's link went through
`moq-shaper --rate 300000 --queue 1s` with 1000-byte objects, so the relay built a backlog.

| question | moq-relay 0.17.5 | effect for an application |
| --- | --- | --- |
| Does a subscriber's OBJECT_ / SUBGROUP_DELIVERY_TIMEOUT bound delivery? | no: decoded and dropped. With 100 ms set, and with neither set, the slow link delivered 600 of 600 groups, lateness p50 about 2.2 s and max about 11.5 s; a 24 s run delivered every group, the last about 24 s late. The track's 5 s default retention did not cut it short either | the relay never drops for a late subscriber. A listener bounds its own delay: the sequencer's late drop and catch-up ([behavior.md](behavior.md#media-subscribing)) |
| How does a group the relay drops show? | a group never opened leaves only a missing group ID; a stream it stops mid-group is reset with code 1 (CANCELLED), never 0x2 (DELIVERY_TIMEOUT). None occurred in the runs above | a missing group ID is the trace; the sequencer reports it as `missingGroups` |
| GROUP_ORDER (`0x22`) on SUBSCRIBE | ignored by this release, as by 0.17.0: SUBSCRIBE_OK states 2 (descending), and each group stream is sent at the subscriber priority alone, with nothing for its group's place in the order (its header carries the track's `0x0E`, or 128, [above](#subscription-parameters)). On the slow link, Descending and Ascending gave the same arrival order, oldest backlog first | there is no newest-group-first delivery to ask for on this relay |
| FETCH of a recent group | served, one group per FETCH: a two-group range is refused with REQUEST_ERROR 3 `FETCH spanning several groups not supported`. Single groups 40 ms to 29 s old (the oldest tried) came back in 1-2 ms on a clean link; with no cache capacity or duration set, as on the fleet, the 5 s retention does not age groups out of the cache | a gap can be filled from the relay group by group, but on a slow link the FETCH queues behind the live backlog (2-5 s in the runs above) |
| Properties on FETCH objects | none: a standalone FETCH answers without a Timescale, so the relay writes no properties and the LOC Timestamp is lost | a fetched frame cannot be placed on the timeline from its own properties |

These rows are facts about the releases named, not about moq-relay in general. **0.17.7**, read in
source and not measured, has the same code on each path: the delivery timeouts are decoded into
unused bindings, a standalone FETCH answers unstamped, and group streams are sent at the subscriber
priority alone, with the track's `0x0E` in their headers ([group stream priority in source](#group-stream-priority-in-source)). Upstream **`main`** differs, read in source and not measured: it applies GROUP_ORDER on
the IETF publisher, and FETCH keeps object timestamps
([moq-dev/moq#5385](https://github.com/moq-dev/moq/pull/5385),
[#5494](https://github.com/moq-dev/moq/pull/5494), in neither 0.17.6 nor 0.17.7). Its delivery
timeouts are still advisory by design (`doc/concept/standard.md` at 7a13b8f8b: they "do not drop
objects or reset subgroups"), and nothing maps an IETF subscriber's budget onto the relay's
presentation-time `max_delay`: asked for in
[moq-dev/moq#5881](https://github.com/moq-dev/moq/issues/5881), and a bench relay that honours the
timeout is being built for the voice benchmark. The SDK's delivery options
([behavior.md](behavior.md#media-subscribing)) are meant to work with and without such a relay: the
subscriber's timeout goes on the SUBSCRIBE either way, and the late drop, reorder window and
catch-up act on whatever the relay sends.

To re-measure, against a local relay as in [Reproduce against a local relay](#reproduce-against-a-local-relay)
or a deployed one, and through an impairer for the slow-link rows (`RELAY_SUB_URL` is the
subscriber's endpoint, `VOICE_*` set the payload, timing, delivery timeouts and group order; the
script's docstring lists them):

```sh
RELAY_URL="$RELAY_URL" mise run python:probe:voice-delivery
```

## Draft-22 deviations

The relay behavior behind the [relay interop deviations](behavior.md#relay-interop-deviations).
The constraints came into this repository with the Video.js core extraction
([`UPSTREAM.json`](../ts/packages/moq-core/UPSTREAM.json)) as findings against moq-lite-rs, without
a recorded relay version. On 2026-09-28 they were checked by reading the moq-relay source (crate
`rs/moq-net`) at tags `moq-relay-v0.14.17` (commit `b2f26b92`), `moq-relay-v0.15.1` and
`moq-relay-v0.15.8`. The PUBLISH_DONE row was also measured live the same day, below. Close codes
for 0.14.17 are moq-net's own `Error::to_code` values; from 0.15.1 a session close carries the
draft-22 Section 12.2 code.

| SDK choice | relay behavior by version | where | effect |
| --- | --- | --- | --- |
| PUBLISH_DONE, then FIN | 0.14.17 to 0.15.3: after SUBSCRIBE_OK the relay waits for the request stream to close, and `Reader::closed` returns a decode error if any byte arrives first. 0.15.4 and later (upstream `a342004d9`, #4083): `read_publish_done` expects PUBLISH_DONE, `Error::ProtocolViolation` on a FIN without one, then waits for Stream Count data streams or a grace period; only status `TRACK_ENDED` is a clean end | `ietf/subscriber.rs` `run_subscribe`, `read_publish_done`; `coding/reader.rs` `closed`; `ietf/publish.rs` `PublishDone::end` | up to 0.15.3: a bare FIN finishes the relay's track, and a PUBLISH_DONE, or any other byte, aborts it for every downstream viewer. From 0.15.4: the reverse |
| no GOAWAY | 0.14.17: a GOAWAY on the client's control stream is decoded and answered with `Error::Unsupported`. 0.15.1 and 0.15.8: it is decoded, logged and recorded, and the relay keeps reading the stream; a second GOAWAY is `Error::ProtocolViolation` | `ietf/session.rs` `run_goaway`; from 0.15.1 also `goaway.rs` `record` | 0.14.17: the session closes with code 17. 0.15.1 and later: the session stays open |
| alias = request ID | the same at 0.14.17, 0.15.1 and 0.15.8: an alias already active for another request ID is `Error::Duplicate` when the two name different tracks, and `Error::Unsupported` for the new subscription when they name the same track | `ietf/subscriber.rs` `insert_track_alias`, `register_alias` | different tracks: the session closes (code 12 on 0.14.17; PROTOCOL_VIOLATION, 0x3, from 0.15.1); same track: that subscription is cancelled |
| `update()` sends REQUEST_UPDATE | the same at 0.14.17, 0.15.1 and 0.15.8: after SUBSCRIBE_OK the relay only polls the request stream for closure, and `Reader::poll_closed` counts any inbound byte as a close; it never sends REQUEST_OK or REQUEST_ERROR, and the poll ending takes the normal end path with `TRACK_ENDED` | `ietf/publisher.rs` subscribe serve loop (`poll_closed`, then `PublishDone`); `coding/reader.rs` `poll_closed` | any update ends the subscription: the viewer gets PUBLISH_DONE status 2 `track ended` and no further objects, and the relay drops its upstream demand; the session stays open |

Live check of the REQUEST_UPDATE row against production 0.15.8 on 2026-09-28 (Python, 4 of 4
runs): with `subscriber_priority=10`, `forward=0` or an empty update, the relay sent PUBLISH_DONE
status 2 `track ended`, stream count 2, within 50 ms and stopped delivering; the publisher's later
writes timed out waiting for demand; the session stayed open and a new subscribe on the same
connection worked. The SDK's `ended` settled only when the connection closed, as after any done
([issue #94](https://github.com/muxinc/relay-sdks/issues/94)).

Re-check on production 0.17.0 on 2026-10-05, live: a track writer's close still reached viewers as
PUBLISH_DONE status 2 (Python 24 of 24, TypeScript 20 of 20); a client GOAWAY still left the session
open, and a second one still closed it with PROTOCOL_VIOLATION (0x3); and any REQUEST_UPDATE still
ended the subscription with PUBLISH_DONE status 2 within about 50 ms (19 of 19). The alias row was
not re-checked. New on 0.17.0: a SUBSCRIBE with `FORWARD` = 0 is refused per request with
REQUEST_ERROR 3 (`NOT_SUPPORTED`) `FORWARD=0 not supported`, and the session stays open; 0.15.8
closed the session (upstream `9e2b77d08`, "decode every legal request and refuse per request").

The alias row refines what the core headers said before 0.15, that any alias bound to two live
request IDs closes the session. That holds only for two different tracks, which draft-22 Section
3.1.3 makes fatal anyway; for one track the relay cancels the newer subscription, because it cannot
demultiplex subscriptions with identical filters. Unique aliases avoid both.

Live check of the PUBLISH_DONE row on 2026-09-28: a publisher wrote three objects, then closed the
track writer or the publisher, and a subscriber on the same relay recorded what reached it. Both
SDKs gave the same results (Python over aioquic, TypeScript in Chromium 147). "Bare FIN" is the
previous behavior, reproduced by making the core's PUBLISH_DONE encode to nothing.

| relay | publisher sends | the viewer receives |
| --- | --- | --- |
| local `moqdev/moq-relay:0.15.8` | PUBLISH_DONE, then FIN | an end-of-track object, then PUBLISH_DONE status 2 `track ended`, stream count 4 |
| local `moqdev/moq-relay:0.15.8` | bare FIN | PUBLISH_DONE status 0 `internal error` |
| local `moqdev/moq-relay:0.15.1` | PUBLISH_DONE, then FIN | PUBLISH_DONE status 0 `internal error` |
| local `moqdev/moq-relay:0.15.1` | bare FIN | PUBLISH_DONE status 2 `track ended` |
| `sjc.relay.mux.global` (0.15.1) | PUBLISH_DONE, then FIN | PUBLISH_DONE status 0 `internal error` |
| `sjc.relay.mux.global` (0.15.1) | bare FIN | status 2 after a writer close; status 0 after a publisher close |

The production publisher-close row matches
[issue #25](https://github.com/muxinc/relay-sdks/issues/25): with a bare FIN the relay wrote status
2 on some runs and 0 on others. From the subscriber's side, what may follow SUBSCRIBE_OK on a
request stream is the REQUEST_UPDATE row: against this relay, nothing that should keep the
subscription ([issue #94](https://github.com/muxinc/relay-sdks/issues/94)).
Both cores' publish-session tests pin the three choices against in-process raw peers: every end
path writes PUBLISH_DONE with its status and stream count after the data streams close, then FINs,
an orderly close puts SETUP and nothing else on the control stream, and SUBSCRIBE_OK carries the
request ID as the alias. Both cores' session tests pin the same control stream contents for a
subscriber connection's close.

To re-test after a relay upgrade, run the local relay below at the new version and, for each
choice, make the one-line change in a scratch build of either core:

- for PUBLISH_DONE, close a track writer and check that a subscriber receives every object, then an
  end-of-track object and a done notification with status 2, rather than status 0 or a reset;
- GOAWAY is not sent because the SDK has no drain behavior, not because of the relay alone, so
  dropping this deviation means adding a drain. Against 0.14.17 any client GOAWAY closes the
  session; from 0.15.1, write GOAWAY (timeout 0, empty URI) on the publisher's control stream
  before NAMESPACE_DONE, and on a subscriber connection's control stream before its transport
  close, then check that each connection's closure carries no error;
- REQUEST_UPDATE needs no core change: subscribe, call `update()` with `forward` 1 once objects
  arrive, and check for an update acknowledgement and continued delivery rather than a done
  notification; only then can the READMEs recommend `update()` against moq-relay;
- the alias choice needs no re-test while the SDK serves one subscription per track
  ([ADR-0001](../docs/adr/0001-one-relay-subscription-per-track.md)); if that changes, check whether
  the relay accepts one alias shared by two subscriptions to the same track.

A change that passes can drop its deviation from [behavior.md](behavior.md#relay-interop-deviations)
in the same pull request that changes both cores.

## Browser QUIC limits

These are limits of the browser's QUIC stack, not of moq-relay, recorded here with the other
transport findings. The TypeScript guide's
[browser transport limits](../ts/GUIDE.md#browser-transport-limits) gives the application-facing
rules. Observed in Chromium 147 (2026-09); they may change between versions, and other browsers are
untested.

- **Handshake idle timeout, about 4 s.** The effective connect deadline is the smaller of
  `connectTimeoutMs` and about 4 s of silence, so a longer `connectTimeoutMs` never fires on a
  silent path ([#157](https://github.com/muxinc/relay-sdks/issues/157)).
- **Blackhole detection, about 5 s.** A connection with data outstanding that hears nothing for
  about 5 s is closed even if the path has come back. Chromium logs `Network blackhole detected`
  (QUIC error 85); the page's `WebTransportError` carries no QUIC code
  ([#148](https://github.com/muxinc/relay-sdks/issues/148)).
- **1250-byte Initial datagrams.** Chromium's QUIC Initial packets are 1250 bytes, so a path that
  carries only the 1200-byte minimum from RFC 9000 section 14 never connects. A cap applied after
  the connection is up does not end it.

The Python SDK's aioquic transport behaves differently: it survives outages until the relay's idle
timeout (10 s from moq-relay 0.17.0, 30 s before), waits out the full connect deadline, and sends
1200-byte Initials.

## QMux over WebSocket

Both fleets serve MOQT over QMux over WebSocket on TCP 443, beside QUIC on UDP 443, through
moq-relay's `[web]` listener (`--web-https-listen`, with `--web-ws` on by default;
`rs/moq-relay/src/websocket.rs`). The TypeScript SDK reaches it through `@caprid/relay-websocket`
([behavior](behavior.md#transport-fallback)). What differs from QUIC, measured on 2026-10-17 against a
local moq-relay 0.17.5 (the production version) built from tag `moq-relay-v0.17.5`, with QUIC and the
WebSocket listener on the same port, unless a row says it is read from the source:

| Behaviour | QUIC (WebTransport, native QUIC) | QMux over WebSocket |
| --- | --- | --- |
| Version negotiation | ALPN / `WT-Available-Protocols`: `moqt-22`, `moqt-20` | `Sec-WebSocket-Protocol`: `qmux-01.moqt-22`, `qmux-01.moqt-20`. The relay pairs `moqt-18` and newer only with `qmux-01` (`QMUX01_ONLY_ALPNS`); an offer with none of its own, a bare `moqt-22` included, fails the upgrade with 400. A bare `qmux-01` or `webtransport` would let it pick moq-lite in SETUP, so the SDK never offers one. |
| Path and token | WebTransport: the CONNECT URL. Native QUIC: PATH in SETUP | The upgrade request's path and query, as on WebTransport (the relay's `Binding::Uri`); no PATH or AUTHORITY in SETUP |
| A refused token at connect | the session is closed with code 0x2 `unauthorized`, which the SDK reports as `UNAUTHORIZED` in phase `connecting` | the relay asks its auth source before it upgrades, and answers a refusal with HTTP 401 (`serve_ws`). A WebSocket client sees only a failed handshake, so the SDK reports `TRANSPORT_ERROR` in phase `connecting`: a token that is bad and a relay that is unreachable look the same. Measured with an `--auth-url` server refusing everything but one token: a bad and an empty token each failed in about 0.1 s with `TRANSPORT_ERROR`, and the auth server logged the refusal with `"transport":"websocket"` |
| A grant's expiry on a live session | closed with 0x2 `unauthorized`: `UNAUTHORIZED` in phase `connection` | the same: a QMux application close with 0x2, `UNAUTHORIZED` in phase `connection`, about 3 s after a grant that expired 3 s on (publisher and subscriber, Node) |
| A publisher's process is killed | silent until the relay's QUIC idle timeout: viewers get done(0) `internal error` 13.0–13.1 s later (6 of 6) | the kernel closes the TCP connection, and viewers get done(0) `internal error` 10–17 ms later (3 of 3) |
| A publisher's process is stopped (SIGSTOP) | the same 13 s (3 of 3) | the kernel still acknowledges TCP, so only the relay's WebSocket keep-alive notices (a ping every 5 s, 30 s without a frame): done(0) 29.96 s later (3 of 3) |
| Subgroup reset codes | native QUIC: in the HTTP/3 encoding ([subgroup resets](#subgroup-resets), TA-27) | as they are: a forwarded reset arrives as code 1, whichever transport the publisher used (3 of 3, publisher over WebSocket and over native QUIC); QMux does not map codes (qmux 0.5.2) |
| Stream multiplexing | independent QUIC streams | one TCP connection: a lost packet holds up every stream behind it (head-of-line blocking); a send stream's close resolves at hand-over to the socket (TA-15) |
| Datagrams | negotiated by QUIC | unavailable to the SDK: QMux can carry them, but the adapter offers none |

The crash rows had the subscriber on WebSocket in every run; only the publisher's transport changed.
A dial that falls back to WebSocket costs the time the WebTransport attempt took: in Chromium 147
against the local relay, a connect took 8–14 ms over WebTransport, 48–60 ms straight over WebSocket,
64–72 ms through `withWebSocketFallback` when UDP to the port was rejected (ICMP port unreachable),
and 4.05–4.08 s when it was silently dropped, the default `handshakeTimeoutMs` plus the WebSocket
connect (3 of 3 each, UDP filtered with nftables on the relay host).

The live scenario ([behavior](behavior.md#live-smoke-test)) passed over WebSocket in Node 24,
Chromium 147, Firefox 148 and Playwright's WebKit (Safari 26.4's engine, on Linux), and through the
fallback in Chromium, against the same relay. That says nothing about Safari on macOS or iOS, and the
fleets themselves could not be reached from where this was measured.

## Reproduce against a local relay

Requires Docker, OpenSSL, the repository's installed dependencies, and Playwright Chromium
(`cd ts && pnpm exec playwright install chromium`). Run from the repository root. These commands
create an anonymous `moqdev/moq-relay:0.17.0` test relay, the production version, on loopback and
a short-lived ECDSA certificate trusted explicitly by the test clients. The certificate is copied
into the container rather than bind-mounted, so the recipe also works when the Docker daemon cannot
see the local filesystem.

```sh
interop_dir=$(mktemp -d)
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout "$interop_dir/key.pem" -out "$interop_dir/cert.pem" -days 7 \
  -subj /CN=localhost -addext subjectAltName=DNS:localhost,IP:127.0.0.1

docker create --name relay-sdk-interop -p 127.0.0.1:4443:4443/udp moqdev/moq-relay:0.17.0 \
  --listen '[::]:4443' --listen-tls-cert /tls/cert.pem --listen-tls-key /tls/key.pem \
  --auth-public '**'
docker cp "$interop_dir/." relay-sdk-interop:/tls
docker start relay-sdk-interop

export RELAY_ANONYMOUS=1 RELAY_URL=https://127.0.0.1:4443
unset RELAY_TOKEN
export RELAY_CA_FILE="$interop_dir/cert.pem"
export RELAY_CERTIFICATE_HASH=$(openssl x509 -in "$interop_dir/cert.pem" -outform DER | openssl dgst -sha256 -r | cut -d ' ' -f 1)
mise run python:test:relay
mise run ts:test:relay

docker rm -f relay-sdk-interop
rm -r "$interop_dir"
unset RELAY_ANONYMOUS RELAY_URL RELAY_CA_FILE RELAY_CERTIFICATE_HASH
```

The relay URL is an IPv4 literal because the port is published on `127.0.0.1` only. Where
`localhost` resolves to `::1` first, nothing listens there: the Node run falls back to the next
address and passes, but Chromium and aioquic do not, so the TypeScript summary reads `DIVERGED`. The
certificate carries `IP:127.0.0.1` for Python's `RELAY_CA_FILE` hostname check, and keeps
`DNS:localhost` for hosts where `localhost` is IPv4.

If the Docker daemon runs in another network namespace, such as a sidecar or a VM whose published
ports do not reach this machine's loopback, replace `-p 127.0.0.1:4443:4443/udp` with
`--network host`; the relay listens on `[::]`, so `127.0.0.1` still reaches it. On 2026-10-01 the
recipe passed for Python, Node and Chromium both as written and with `--network host`, on a host
where `localhost` resolves to `::1` first; on 2026-10-05 it passed for all three as written with
the 0.17.0 image.

Both tests publish binary, text, and empty payloads, then connect a second subscriber
**after** the relay has received content. They require `SUBSCRIBE_OK` to contain
`LARGEST_OBJECT` and verify that the late subscriber receives the next live object.
This exercises the relay's encoder independently of the SDK codecs and in-process hubs.
Successful output includes `lateSubscriberLargestObject`.

Current SDK builds need moq-relay 0.15.4 or later to end a track cleanly (see the PUBLISH_DONE row
above), so the image tag should not go below that. The flags are those of 0.15.x and 0.17.0:
moq-relay 0.14.17 took `--server-bind`, `--tls-cert`, `--tls-key` and `--web-http-listen` instead,
and `--auth-public ''` for an anonymous relay, which 0.15.8 rejects. The PUBLISH_DONE table above
was measured against local 0.15.x images; to reproduce it, use those tags.

`RELAY_ANONYMOUS=1` skips the token prompt; if `RELAY_TOKEN` is set, it is still used.
`RELAY_CA_FILE` adds the local certificate as a Python trust root.
`RELAY_CERTIFICATE_HASH` pins its SHA-256 DER hash for browser WebTransport and for the TypeScript
smoke test's Node run over native QUIC. That Node run uses `RELAY_CA_FILE` as its `ca` only when no
hash is set, and only with a CA-signed leaf: the self-signed certificate above is not accepted as its
own trust anchor. These are smoke-test options; normal SDK TLS verification remains unchanged.

### Over WebSocket

The recipe above publishes only the relay's UDP port, so it reaches QUIC alone. To run the WebSocket
runtimes as well, add the relay's WebSocket listener on the same port and publish its TCP side:
`-p 127.0.0.1:4443:4443/tcp` beside the UDP mapping, and
`--web-https-listen '[::]:4443' --web-https-cert /tls/cert.pem --web-https-key /tls/key.pem` beside
the other flags (those of 0.17.x). Then trust the certificate in Node, which reads extra roots only
as it starts, and pick the runtimes:

```sh
export NODE_EXTRA_CA_CERTS="$interop_dir/cert.pem"
RELAY_RUNTIME=node-websocket,chromium-websocket,webkit-websocket,firefox-websocket,chromium-fallback \
  mise run ts:test:relay
unset NODE_EXTRA_CA_CERTS
```

The browsers ignore certificate errors on their WebSockets whenever `RELAY_CERTIFICATE_HASH` or
`RELAY_CA_FILE` is set, since a page cannot pin a WebSocket's certificate. `chromium-fallback` dials
WebTransport without the hash, so against this relay its WebTransport handshake fails and it falls
back; each fallback is reported as a `fallback` phase. WebKit and Firefox need Playwright's builds
(`pnpm exec playwright install webkit firefox`). The table in
[QMux over WebSocket](#qmux-over-websocket) was measured this way with a moq-relay 0.17.5 binary
built from source rather than the image.
