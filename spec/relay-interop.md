# moq-relay interoperability

Both SDKs speak only `moqt-20`. Their `LARGEST_OBJECT` message parameter is two bare
varints (Group, Object), matching draft-20 §10.2 and moq-relay from **0.14.17** through
**0.15.8**, the production relay since 2026-09-28 (`SubscribeOk` in `rs/moq-net/src/ietf/subscribe.rs`
at tag `moq-relay-v0.15.8`). [Upstream fix](https://github.com/moq-dev/moq/pull/3561).

Older moq-relay releases used a length prefix for this parameter. That encoding cannot
be safely auto-detected: coordinate adoption of these SDK builds with the relay upgrade.
Old SDK builds fail on subscriptions to populated tracks after the relay upgrade; these
SDK builds misdecode or reject the old relay's populated-track responses. `INCLUDE_PROPERTIES`
remains length-prefixed on 0.15.8.

Publishers still retain no history and advertise no `LARGEST_OBJECT`. The relay's
Next Object + current-group `FILL_PARAMETERS` pull (since 0.14.17, unchanged in 0.15.8's
`subscribe_join` in `rs/moq-net/src/ietf/subscriber.rs`) therefore has no cached publisher
content to fill; the publisher serves subsequent writes. Adding publisher retention and
backfill is a separate feature. The test fixtures send the pull parameters of 0.14.17 and 0.15.8.

## Publisher restarts and resubscription

Against moq-relay 0.14.17, re-subscribing to the same namespace and track after
`on_publisher_done` / `onPublisherDone` can replay cached objects and immediately receive another
`PUBLISH_DONE`. An application that re-subscribes from every done callback can
therefore generate a request loop while the publisher is absent. See
[issue #21](https://github.com/muxinc/relay-sdks/issues/21) for reproduction evidence
and relay follow-up.

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

Record done status and reason for diagnosis. Both `track ended` (2) and `internal
error` (0) have been observed around publisher shutdown; neither identifies a new
publisher generation. Preserve actual errors rather than interpreting all status-0
responses as harmless shutdowns.

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

moq-relay 0.15.8 (production from 2026-09-28) still races, but answers request error 16 (0x10,
`DOES_NOT_EXIST`) with reason `unroutable` instead of 404
([issue #136](https://github.com/muxinc/relay-sdks/issues/136)). It sends the same code with reason
`not found` for an unknown track in an announced namespace, so only the reason tells the race apart
from a request no wait can satisfy. Measured on 2026-09-28 with the Python SDK through the examples'
retry, ten trials per mode: at once after `register_track`, 6 / 10 first attempts were rejected
`unroutable`; after `announced`, 3 / 10. Every rejected trial was accepted on the second attempt,
100 ms later, and a subscribe to an unknown track in each namespace came back `not found` once,
unretried.

## Relay demand

A publisher's demand is the relay's one upstream subscription for a track, not a count of
downstream viewers. Measured against `sjc.relay.mux.global` on 2026-09-15 with the Python SDK
([issue #24](https://github.com/muxinc/relay-sdks/issues/24), 2 of 2 runs):

| scenario | result |
| --- | --- |
| the only subscriber closes its connection | demand stays for 30.07 s, then ends |
| the only subscriber cancels, connection kept open | demand still present after 20 s |
| a second subscriber joins while the first is present | no new demanded value; the second receives the catalog from the relay's cache |
| writes while demanded but no viewer present | succeed and reach no one |
| 200 subscribe/receive-one/cancel cycles in about 66 s | one demanded value and one release, about 30 s after the churn stopped |

So the relay coalesces viewers into one subscription and releases it about 30 seconds after the
last one leaves. `has_demand` / `hasDemand` is not a liveness signal and not a reason to keep or
stop encoding, and a catalog re-sent on each demanded value is what the relay caches for every
later viewer: write only state that is safe to serve from cache.

## Late subscribers and location filters

A plain SUBSCRIBE, with no LOCATION_FILTER or an explicit zero-field one, starts at the beginning
of the relay's latest group: a subscriber that joins an ongoing track receives that group's cached
objects and everything published after, and nothing older. `NextObjectFilter` is stricter and starts
after the relay's largest object, so it skips the rest of the current group. A relative-group or
absolute filter reaches further back into the relay's cache. moq-relay 0.15.8 resolves the filters
this way in `filter_range` (`rs/moq-net/src/ietf/publisher.rs` at tag `moq-relay-v0.15.8`).
Measured against production on 2026-09-28, on 0.15.8
([issue #138](https://github.com/muxinc/relay-sdks/issues/138)): a publisher wrote groups up to 19,
and every late subscriber joined between groups 9 and 10.

| subscriber | filter | groups received |
| --- | --- | --- |
| late | none (no LOCATION_FILTER sent), or explicit `NoLocationFilter()` | `[9..19]` |
| late | `NextObjectFilter` | `[10..19]` |
| late | `RelativeGroupFilter(groups_before_next=3)` | `[7..19]` |
| late | `AbsoluteLocationFilter(start=group 2)` | `[4..19]` |

On 2026-09-15, before production moved to 0.15.8
([issue #26](https://github.com/muxinc/relay-sdks/issues/26)), `NextObjectFilter` behaved the same
as no filter; on 0.15.8 it does not.

The draft defines an unfiltered subscription as passing all Objects published or received via
upstream subscriptions ([behavior.md](behavior.md#subscribing)); it does not require a relay to
replay what it cached before the subscription, so starting at the latest group is a legitimate
reading and not an SDK encoding problem. A late joiner that needs a decodable start or recent
history subscribes with `RelativeGroupFilter` / `{ type: 'relative-group' }` (`groups_before_next` /
`groupsBeforeNext` of `1` starts at the current group) or with an absolute start. How far back
either reaches depends on what the relay still holds, and the absolute case above started later
than it asked for.

## Subgroup resets

moq-relay 0.15.8 forwards a subgroup reset once it has begun forwarding that subgroup, but not at
once. Measured against production on 2026-09-28
([issue #138](https://github.com/muxinc/relay-sdks/issues/138)): a publisher wrote objects to a
group and aborted it after the relay had forwarded some of them. All 10 of 10 such aborts reached
the subscriber as a subgroup reset, and each arrived only when the publisher's next group began:
about 0.5 s after the abort at the publisher's normal cadence, and 3 s after it when the publisher
paused 3 s before its next group. A subscriber that must react to an abandoned group sooner than the
next group cannot rely on the reset's timing.

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
2 on some runs and 0 on others. What may follow SUBSCRIBE_OK on a request stream is also the open
question in [issue #94](https://github.com/muxinc/relay-sdks/issues/94), from the subscriber's side.
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
- the alias choice needs no re-test while the SDK serves one subscription per track
  ([ADR-0001](../docs/adr/0001-one-relay-subscription-per-track.md)); if that changes, check whether
  the relay accepts one alias shared by two subscriptions to the same track.

A change that passes can drop its deviation from [behavior.md](behavior.md#relay-interop-deviations)
in the same pull request that changes both cores.

## Reproduce against a local relay

Requires Docker, OpenSSL, the repository's installed dependencies, and Playwright Chromium
(`cd ts && pnpm exec playwright install chromium`). Run from the repository root. These commands
create an anonymous `moqdev/moq-relay:0.15.8` test relay, the production version, on loopback and
a short-lived ECDSA certificate trusted explicitly by the test clients. The certificate is copied
into the container rather than bind-mounted, so the recipe also works when the Docker daemon cannot
see the local filesystem.

```sh
interop_dir=$(mktemp -d)
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout "$interop_dir/key.pem" -out "$interop_dir/cert.pem" -days 7 \
  -subj /CN=localhost -addext subjectAltName=DNS:localhost

docker create --name relay-sdk-interop -p 127.0.0.1:4443:4443/udp moqdev/moq-relay:0.15.8 \
  --listen '[::]:4443' --listen-tls-cert /tls/cert.pem --listen-tls-key /tls/key.pem \
  --auth-public '**'
docker cp "$interop_dir/." relay-sdk-interop:/tls
docker start relay-sdk-interop

export RELAY_ANONYMOUS=1 RELAY_URL=https://localhost:4443
unset RELAY_TOKEN
export RELAY_CA_FILE="$interop_dir/cert.pem"
export RELAY_CERTIFICATE_HASH=$(openssl x509 -in "$interop_dir/cert.pem" -outform DER | openssl dgst -sha256 -r | cut -d ' ' -f 1)
mise run python:test:relay
mise run ts:test:relay

docker rm -f relay-sdk-interop
rm -r "$interop_dir"
unset RELAY_ANONYMOUS RELAY_URL RELAY_CA_FILE RELAY_CERTIFICATE_HASH
```

If the Docker daemon runs in another network namespace, such as a sidecar or a VM whose published
ports do not reach this machine's loopback, replace `-p 127.0.0.1:4443:4443/udp` with
`--network host`. Both smoke tests passed that way on 2026-09-28, in a workspace whose daemon is a
sidecar.

Both tests publish binary, text, and empty payloads, then connect a second subscriber
**after** the relay has received content. They require `SUBSCRIBE_OK` to contain
`LARGEST_OBJECT` and verify that the late subscriber receives the next live object.
This exercises the relay's encoder independently of the SDK codecs and in-process hubs.
Successful output includes `lateSubscriberLargestObject`.

Current SDK builds need moq-relay 0.15.4 or later to end a track cleanly (see the PUBLISH_DONE row
above), so the image tag should not go below that. The flags are those of 0.15.x: moq-relay 0.14.17
took `--server-bind`, `--tls-cert`, `--tls-key` and `--web-http-listen` instead, and `--auth-public ''`
for an anonymous relay, which 0.15.8 rejects.

`RELAY_ANONYMOUS=1` skips the token prompt; if `RELAY_TOKEN` is set, it is still used.
`RELAY_CA_FILE` adds the local certificate as a Python trust root.
`RELAY_CERTIFICATE_HASH` pins its SHA-256 DER hash for browser WebTransport. These are
smoke-test options; normal SDK TLS verification remains unchanged.
