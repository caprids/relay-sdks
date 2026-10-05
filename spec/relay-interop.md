# moq-relay interoperability

Both SDKs speak only `moqt-20`. Their `LARGEST_OBJECT` message parameter is two bare
varints (Group, Object), matching draft-20 §10.2 and moq-relay from **0.14.17** through
**0.17.0**, the production relay since 2026-10-05 (`SubscribeOk` in `rs/moq-net/src/ietf/subscribe.rs`
at tag `moq-relay-v0.17.0`). [Upstream fix](https://github.com/moq-dev/moq/pull/3561).

Older moq-relay releases used a length prefix for this parameter. That encoding cannot
be safely auto-detected: coordinate adoption of these SDK builds with the relay upgrade.
Old SDK builds fail on subscriptions to populated tracks after the relay upgrade; these
SDK builds misdecode or reject the old relay's populated-track responses.

`INCLUDE_PROPERTIES` (`0x35`) is one raw byte with no length, matching draft-20 §10.2.21 and
moq-relay **0.17.0** (`Subscribe` in `rs/moq-net/src/ietf/subscribe.rs` at tag `moq-relay-v0.17.0`).
0.15.8 and earlier length-prefixed it. Neither relay version sends it upstream, so only an
application that sets `include_properties` / `includeProperties` on a SUBSCRIBE sees the change.
An SDK build from before [PR #246](https://github.com/muxinc/relay-sdks/pull/246) that sets it
against 0.17.0 gets its whole session closed with PROTOCOL_VIOLATION (0x3) `wrong frame size`
(measured on production on 2026-10-05, 3 of 3); a current build against a relay older than 0.17.0
is misread the other way.

Publishers still retain no history and advertise no `LARGEST_OBJECT`. The relay's
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
  track name in the catalog, a new namespace, or a new announce seen through your own discovery),
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
window after `announced` is about one round trip, no SDK-side fact can close it, and a subscriber
in another process cannot observe the announce at all: the retry belongs to the application
([behavior.md](behavior.md#subscribing)). Earlier text said "about a second"; that was the retry
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

When the first viewer of a track is on a different relay from the publisher, the publisher sees
two relay subscriptions, not one
([issue #243](https://github.com/muxinc/relay-sdks/issues/243)). Relays mesh over moq-lite, and a
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
was seq 1 or 2 on three of four paths. A viewer on the publisher's own relay sends no TRACK_INFO
query and gets one subscription (3 of 3 on 0.17.0 on 2026-10-05). The SDKs cannot tell the two
subscriptions apart: each is a demanded value, the first followed by `false` ([ADR 0001](../docs/adr/0001-one-relay-subscription-per-track.md)).
Because publishers retain no history, the second subscription's current-group fill has nothing to
recover either. A live publisher therefore cannot count on its first objects after demand begins
reaching a viewer: write state-shaped tracks on every demanded value (the catalog service does), and
start media at a keyframe after any change (the frame writer does). The live smoke tests repeat a
warm-up marker on each track until the subscriber receives one before writing what they assert, and
`RELAY_SUB_URL` puts their subscribers on another relay
([issue #244](https://github.com/muxinc/relay-sdks/issues/244)).

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

## Draft-20 deviations

The relay behavior behind the [relay interop deviations](behavior.md#relay-interop-deviations).
The constraints came into this repository with the Video.js core extraction
([`UPSTREAM.json`](../ts/packages/moq-core/UPSTREAM.json)) as findings against moq-lite-rs, without
a recorded relay version. On 2026-09-28 they were checked by reading the moq-relay source (crate
`rs/moq-net`) at tags `moq-relay-v0.14.17` (commit `b2f26b92`), `moq-relay-v0.15.1` and
`moq-relay-v0.15.8`. The PUBLISH_DONE row was also measured live the same day, below. Close codes
for 0.14.17 are moq-net's own `Error::to_code` values; from 0.15.1 a session close carries the
draft-20 Section 3.5 code.

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
request IDs closes the session. That holds only for two different tracks, which draft-20 Section
11.1 makes fatal anyway; for one track the relay cancels the newer subscription, because it cannot
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
