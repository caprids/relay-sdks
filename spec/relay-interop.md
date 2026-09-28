# moq-relay interoperability

Both SDKs speak only `moqt-20`. Their `LARGEST_OBJECT` message parameter is two bare
varints (Group, Object), matching moq-relay **0.14.17** and draft-20 §10.2.
[Upstream fix](https://github.com/moq-dev/moq/pull/3561).

Older moq-relay releases used a length prefix for this parameter. That encoding cannot
be safely auto-detected: coordinate adoption of these SDK builds with the relay upgrade.
Old SDK builds fail on subscriptions to populated tracks after the relay upgrade; these
SDK builds misdecode or reject the old relay's populated-track responses. `INCLUDE_PROPERTIES`
remains length-prefixed.

Publishers still retain no history and advertise no `LARGEST_OBJECT`. The relay's new
Next Object + current-group `FILL_PARAMETERS` pull therefore has no cached publisher
content to fill; the publisher serves subsequent writes. Adding publisher retention and
backfill is a separate feature. The test fixtures now send the 0.14.17 pull parameters.

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
  notifications as well as retryable request errors; a delay used only for 404
  responses does not bound an accepted-subscription/done loop. Cancel pending retries
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
runs of twelve trials per mode.

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

A plain SUBSCRIBE, with no LOCATION_FILTER or an explicit zero-field one, is served from the live
edge: a subscriber that joins an ongoing track receives only objects published after it joined.
The relay does retain history, and it reaches into its cache for a relative-group or absolute
filter. Measured against `sjc.relay.mux.global` on 2026-09-15 with the Python SDK
([issue #26](https://github.com/muxinc/relay-sdks/issues/26)): a publisher wrote one object per
second for about 20 s, and each late subscriber joined 8-10 s in (2 of 2 runs).

| subscriber | filter | groups received |
| --- | --- | --- |
| early, before the first write | none | all, `[0..17]` of `[0..17]` |
| late | none (no LOCATION_FILTER sent) | `[9..17]` of `[0..17]` |
| late | explicit `NoLocationFilter()` | `[10..19]` of `[0..19]`, the same as sending none |
| late | `NextObjectFilter` | the same as none |
| late | `RelativeGroupFilter(groups_before_next=3)` | about 3 groups before the join |
| late | `AbsoluteLocationFilter(start=group 2)` | from about group 4-8, depending on timing |

The draft defines an unfiltered subscription as passing all Objects published or received via
upstream subscriptions ([behavior.md](behavior.md#subscribing)); it does not require a relay to
replay what it cached before the subscription, so this is a legitimate reading and not an SDK
encoding problem. A late joiner that needs a decodable start or recent history subscribes with
`RelativeGroupFilter` / `{ type: 'relative-group' }` (`groups_before_next` / `groupsBeforeNext` of
`1` starts at the current group) or with an absolute start. How far back either reaches depends on
what the relay still holds, and the absolute case above started later than it asked for.

## Subgroup resets

The relay forwards a subgroup reset only when it has already begun forwarding that subgroup.
Measured against `sjc.relay.mux.global` on 2026-09-15
([issue #38](https://github.com/muxinc/relay-sdks/issues/38)): a Python publisher wrote two small
objects to a group and aborted it. A Python subscriber received `on_subgroup_reset` within 0.05 s
in one run; a TypeScript subscriber in another run received nothing for that group, neither objects
nor `onSubgroupReset`, and its first group was the next one. In the close path of
[issue #30](https://github.com/muxinc/relay-sdks/issues/30), where 8 MiB objects had been partly
forwarded, the relay did propagate the reset. The exact condition (bytes forwarded, time since the
first object, or cache policy) was not isolated.

A subscriber therefore cannot rely on a reset to learn that a group was abandoned. A missing group
ID is the only trace, and it is indistinguishable from a group the publisher never sent. An
application that must detect loss carries its own sequence numbers in payloads.

## Reproduce against a local relay

Requires Docker, OpenSSL, the repository's installed dependencies, and Playwright Chromium
(`cd ts && pnpm exec playwright install chromium`). Run from the repository root with a
local Docker daemon. These commands create an anonymous test relay on loopback and a
short-lived ECDSA certificate trusted explicitly by the test clients.

```sh
interop_dir=$(mktemp -d)
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout "$interop_dir/key.pem" -out "$interop_dir/cert.pem" -days 7 \
  -subj /CN=localhost -addext subjectAltName=DNS:localhost

docker run -d --name relay-sdk-interop \
  -p 127.0.0.1:4443:4443/udp -p 127.0.0.1:4443:4443/tcp \
  -v "$interop_dir:/tls:ro" moqdev/moq-relay:0.14.17 \
  --server-bind '[::]:4443' --tls-cert /tls/cert.pem --tls-key /tls/key.pem \
  --web-http-listen '[::]:4443' --auth-public ''

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

Both tests publish binary, text, and empty payloads, then connect a second subscriber
**after** the relay has received content. They require `SUBSCRIBE_OK` to contain
`LARGEST_OBJECT` and verify that the late subscriber receives the next live object.
This exercises the relay's encoder independently of the SDK codecs and in-process hubs.
Successful output includes `lateSubscriberLargestObject`.

`RELAY_ANONYMOUS=1` skips the token prompt; if `RELAY_TOKEN` is set, it is still used.
`RELAY_CA_FILE` adds the local certificate as a Python trust root.
`RELAY_CERTIFICATE_HASH` pins its SHA-256 DER hash for browser WebTransport. These are
smoke-test options; normal SDK TLS verification remains unchanged.
