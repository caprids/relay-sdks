# SDK vocabulary

Every Relay SDK spells the same concepts with the same words, in that language's casing. This
document is the record of those words: what each public name means, what it must not be read to
mean, and why it was chosen over the alternatives. `behavior.md` says what must be true; the
language READMEs show the API; this file fixes the nouns and verbs both use.

Rename a concept here first, then in every SDK in the same change. A name that exists in one SDK
without a row here is a bug.

## The names

| Concept | TypeScript (`@mux/relay`) | Python (`mux_relay`) |
| --- | --- | --- |
| Open a connection that can only subscribe | `connectRelaySubscriber(options)` returns `RelaySubscriber` | `connect_relay_subscriber(url, ...)` returns `RelaySubscriber` |
| Open a connection that can only publish one namespace | `connectRelayPublisher(options)` returns `RelayPublisher` | `connect_relay_publisher(url, namespace=...)` returns `RelayPublisher` |
| Ask the relay for a track's objects | `subscriber.subscribe(options, handlers)` returns `RelaySubscription` | `subscriber.subscribe(namespace, track, handlers, parameters=...)` returns `RelaySubscription` |
| Await the relay's answer to a subscription | `subscription.subscribed` resolves `SubscribeOk` | `await subscription.subscribed` returns `SubscribeOk` |
| Learn that a subscription ended, and how | `subscription.ended` resolves `SubscriptionEnd` | `await subscription.ended` returns `SubscriptionEnd` |
| The type of `subscribed`, `ended`, `announced` and a connection's `closed` | `Promise<T>` | `RelayAwaitable[T]`: awaitable by any number of isolated waiters, with `done()`, `result()`, `exception()` and `add_done_callback()`; not a cancellable `Future` |
| Events on one subscription | `RelaySubscriptionHandlers` | `RelaySubscriptionHandlers` |
| The relay accepted the subscription | `onSubscribed(info)` | `on_subscribed(info)` |
| The relay rejected it, or the request itself failed before acceptance | `onError(error)` | `on_error(error)` |
| An object arrived | `onObject(object)` | `on_object(object)` |
| A subgroup stream ended cleanly or was reset | `onSubgroupEnd(info)`, `onSubgroupReset(info)` | `on_subgroup_end(info)`, `on_subgroup_reset(info)` |
| The publisher said it is finished with the track | `onPublisherDone(info)` | `on_publisher_done(info)` |
| Protocol notifications on the request stream | `onUpdateOk()`, `onStateNotify(parameters)`, `onGoaway(goaway)` | `on_update_ok()`, `on_state_notify(parameters)`, `on_goaway(goaway)` |
| Register a track this publisher will serve | `publisher.registerTrack(options)` returns `RelayTrackWriter` | `publisher.register_track(track, ...)` returns `RelayTrackWriter` |
| Await the namespace announce reaching the wire | `publisher.announced` resolves `void` | `await publisher.announced` returns `None` |
| Send one object as its own group | `writer.write(bytes, options)` | `await writer.write(payload, properties=...)` |
| Send one UTF-8 text object | `writer.writeText(text, options)` | `await writer.write_text(text, properties=...)` |
| Send one JSON object | `writer.writeJSON(value, options)` | `await writer.write_json(value, properties=...)` |
| Read an object as UTF-8 text or JSON | `readText(object)`, `readJSON(object)` | `read_text(obj)`, `read_json(obj)` |
| SDK failure with a stable code and operation phase | `RelayError`, `RelayErrorCode`, `RelayErrorPhase` | `RelayError`, `RelayErrorCode`, `RelayErrorPhase` |
| Send several objects in one group | `writer.openGroup()` returns `RelayGroupWriter` | `await writer.open_group()` returns `RelayGroupWriter` |
| Does the relay currently want this track | `writer.hasDemand` | `writer.has_demand` |
| Follow every change in relay demand | `writer.demandChanges()` | `writer.demand_changes()` |
| The track ended and cannot be written again | `writer.closed` | `writer.closed` |
| Borrowed protocol-level track access for integrations | `subscriber.trackSource` (a `MoqTrackSource`) | `subscriber.track_source` (a `MoqTrackSource`) |
| The connection ended, and why | `connection.closed` resolves `{ error? }` | `await connection.closed` returns `Closed(error=...)` |
| Cancel a connection's whole lifetime from outside | `AbortSignal` (platform), given as `signal` | `CancellationToken`, given as `cancellation=`; `Cancelled` is the reason it raises when `cancel()` was given none |
| Options every connection takes: endpoint, token provider, lifetime cancellation, connection deadline, transport | `ConnectRelayOptions` (`url`, `getToken`, `signal`, `connectTimeoutMs`, `createTransport`) | the shared keyword arguments of both entry points (`url`, `get_token`, `cancellation`, `connect_timeout`, `create_transport`) |
| Options only a subscriber connection takes | `ConnectRelaySubscriberOptions`: the shared options plus `requestTimeoutMs` | `connect_relay_subscriber`'s shared keyword arguments plus `request_timeout` |
| Options only a publisher connection takes | `ConnectRelayPublisherOptions`: the shared options plus `namespace`; no request deadline | `connect_relay_publisher`'s shared keyword arguments plus `namespace`; no request deadline |
| Supply the transport | `createTransport(url, protocols)` returns `{ transport, ready, framing? }` (`RelayTransportFactory`); browser WebTransport by default | `create_transport(url, protocols)` returns `ConnectingTransport(transport, ready)` (`RelayTransportFactory`); `connect_webtransport`, the aioquic factory, by default |
| Consume one subscription as an async iterator | `subscriber.subscribeEvents(options)` returns `SubscriptionEvents` | `subscriber.subscribe_events(namespace, track, ...)` returns `SubscriptionEvents` |
| One event from the events iterator | `SubscriptionEvent`, discriminated by `type` | `SubscriptionEvent`, the union of `SubscribedEvent`, `ObjectEvent`, `PublisherDoneEvent`, `SubgroupEndEvent`, `SubgroupResetEvent`, `StateNotifyEvent`, `GoawayEvent` and `UpdateOkEvent`, each also carrying `type` |
| Protocol types borrowed from the core | `Goaway`, `KeyValuePair`, `LocationFilter`, `MessageParameters`, `MoqTrackSource`, `MoqtObject`, `MoqtTransport`, `PublishDone`, `RequestError`, `SubscribeOk`, `Subscription`, `SubscriptionHandlers`, re-exported as types | the same names re-exported from `moq_core`, plus `Closed`; every other core name is imported from `moq_core` |
| The hosted Relay endpoint pinned in `spec/protocol.json` | not exported; the example carries it | `DEFAULT_ENDPOINT` |

Casing is the only difference between languages: camelCase in TypeScript, snake_case in Python.
Boolean state is a property (`hasDemand`, `closed`); observing change over time is a method that
returns an async iterator (`demandChanges()`); everything that sends a request or bytes is a verb.

## Connection roles: `connectRelaySubscriber`, `connectRelayPublisher`

A Relay connection has one role. A subscriber connection can subscribe, fetch, and query track
status; it cannot publish. A publisher connection announces one namespace and serves tracks in it;
it cannot subscribe. Each uses its own transport and its own token, and the application owns both
connections' shutdown.

`connectRelay` was the subscriber entry point. Next to `connectRelayPublisher` it read as the
general or bidirectional way in, and its result type `RelayConnection` said nothing about what the
connection could do. The names now carry the role: `connectRelaySubscriber` returns a
`RelaySubscriber`, `connectRelayPublisher` returns a `RelayPublisher`, and a reader who sees either
knows which token it needs and which operations it has.

Not adopted: keeping `connectRelay` as the short, default entry point (subscribing is not more
fundamental than publishing, and the asymmetry is what caused the confusion); `RelayConnection` as a
shared base type (the two roles share a lifetime implementation, not an API).

The connection options follow the same split
([issue #72](https://github.com/muxinc/relay-sdks/issues/72)). `ConnectRelayOptions` is what both
roles take: endpoint, token provider, lifetime signal, connection deadline, and transport. Each
role's type extends it with what only that role uses. `ConnectRelaySubscriberOptions` adds
`requestTimeoutMs`, the deadline for a subscription's initial response.
`ConnectRelayPublisherOptions` adds `namespace` and no request deadline, because a publisher sends
no request that deadline would bound; each track's `writeTimeoutMs` bounds waiting for demand and
writing. A `requestTimeoutMs` given to a publisher is a compile-time error and is ignored at
runtime, as is any other key the call does not take.

Not adopted: renaming the shared type `RelayConnectionOptions` (that is the shared-base name the
role decision above set aside, and the two roles share a lifetime, not an API); keeping
`requestTimeoutMs` on the shared type (a publisher would advertise, and validate, a deadline that
changes nothing); rejecting a stray `requestTimeoutMs` on a publisher at runtime (the SDK checks
the options a call uses, not the absence of others).

## Track registration is not sending: `registerTrack`, `write`

Publishing follows the relay's announce-and-serve flow. `registerTrack` adds a track to the set
this publisher connection will serve and returns its writer. Nothing is sent: the relay learns
about the track only when a subscriber asks for it, and the first bytes leave when the application
calls `write()` or opens a group, which first waits for that relay demand and then sends.

The method used to be `publish()`. `const events = publisher.publish({ track: 'events' })` reads
as an act of publishing, so a reader expects bytes to move and is surprised that the real sending
verb is `events.write()`. The original review comment on this repository made exactly that point.
`registerTrack` is the verb `behavior.md` already uses ("Registering a track announces it"), it is
the verb the generic core uses for the same step, and it cannot be misread as transmission.

Not adopted: `publishTrack` (still the verb that suggests sending); `createTrack` (implies a
resource is created on the relay, but the relay holds no track until a subscriber requests it);
`openTrack` (nothing opens on the wire until a write, and `openGroup` does open a stream, so the two
would suggest the same thing and mean different things).

The TypeScript options type follows the method: `RegisterTrackOptions` (track name, track
properties, write deadline). The generic core has a type of the same name for the same step one
level down, where the namespace is an explicit field; the product type omits it because the
publisher connection already fixes the namespace.

## Relay demand: `hasDemand`, `demandChanges`

Demand is the relay's upstream subscription for one track. Demand is present when the relay has
subscribed to the track on this publisher connection and is forwarding; while it is present a write
proceeds at once, and while it is absent a write waits for it (up to the write deadline). Demand is
what the relay asks for on its own behalf; it is not a count of viewers and not a liveness signal:

- The relay fans one upstream subscription out to every downstream viewer. A second viewer joining
  produces no new `true`; later viewers are served from the relay's cache of the last objects.
- The relay decides when its upstream subscription ends. moq-relay 0.15.x cancels it about one
  round trip after the last viewer leaves; relays before 0.15 held it for about 30 seconds (see
  [relay demand](relay-interop.md#relay-demand)). `true` does not prove anyone is watching, and
  `false` says only that the relay stopped forwarding.
- A replacement subscription, such as the relay re-subscribing after its own reconnect, is a new
  relay subscription: `demandChanges()` yields `true` again even though `hasDemand` never became
  `false`. A write in flight when that happens fails, because its bytes were addressed to the old
  subscription; the track stays open.

`hasDemand` answers "would a write proceed right now". `demandChanges()` yields the current state
first and then one value per relay subscription change: `true` when the relay subscribes or
replaces its subscription, `false` when its subscription ends. It finishes when the track ends,
however it ended. This is how a state-shaped track such as a catalog re-sends its document on
every relay subscription, and the repeated `true` is deliberate: skipping it would lose the
replacement subscription that needs the document again.

The properties used to be `bound` and `bindings()`. Those are the core's words for the alias
binding between a track and the relay's request, which is an implementation detail; at the product
level "bound" invited the reading "a viewer is bound to this track", which is exactly what the
value does not mean.

Not adopted: any name containing "subscriber" or "viewer" (`subscribers()`, `hasViewers`), because
the value is per relay subscription, not per viewer; `demand()` for the iterator (too close to the
property, and a noun does not say that it yields over time).

## Publisher completion is not termination: `onPublisherDone`

`PUBLISH_DONE` is a control message from the publisher's side saying that it has finished with the
track. It is delivered on the control path, so objects still in transit on data streams can arrive
after it. It does not end the subscription: the request stays open until the application cancels it
or the connection ends, and the SDK does not resubscribe on the application's behalf.

The handler used to be `onDone`. That reads as "this subscription is done, nothing more will come",
which is wrong in both halves: trailing objects can still come, and the subscription is not over.
`onPublisherDone` names the party that is done. A distinct terminal signal for the subscription
itself, settling once when it can no longer deliver, is
[issue #68](https://github.com/muxinc/relay-sdks/issues/68); it must not be named "drained" or
"complete" because the protocol cannot establish that property.

## Subscription acceptance: `onSubscribed`

`SUBSCRIBE_OK` means the relay accepted the request and will forward objects, and its payload
carries the track alias, the relay's parameters, and the track properties as the relay sends them.
The handler used to be `onOk`, the core's name for the OK response on any request (subscribe,
fetch, track status). At the product level it did not say what became ready. `onSubscribed` does.
`subscription.subscribed`, the awaitable readiness result, settles on the same event, so the two
names agree.

`onError` keeps its name: the relay answered the request with REQUEST_ERROR, because it rejected the
subscription or the request itself failed before acceptance (request timeout, or the request stream
failing). A REQUEST_ERROR can also arrive after acceptance, when a relay refuses an `update()`, and
it ends the subscription then too ([issue #94](https://github.com/muxinc/relay-sdks/issues/94)).
moq-relay sends neither REQUEST_OK nor REQUEST_ERROR for an update: it ends the subscription with
PUBLISH_DONE instead, so against it `onUpdateOk` / `on_update_ok` never fires and an update reaches
`onPublisherDone` / `on_publisher_done`, not `onError`
([relay interop deviation 4](behavior.md#relay-interop-deviations)).
Connection shutdown and GOAWAY never reach `onError`: a pending subscription ends through
`subscribed` and `ended` as terminated by the closure, so a lost connection is never mistaken for
a rejection, and after acceptance closure surfaces as subgroup resets and connection closure.

## Borrowed protocol access: `trackSource`

`subscriber.trackSource` is the same connection seen through the generic `MoqTrackSource`
contract: `subscribe`, `fetch`, and `trackStatus` in protocol terms (namespace tuple, track name,
message parameters, and the core's `SubscriptionHandlers` with `onOk` and `onDone`), plus the
`closed` promise. It exists so an integration such as a player can be handed track access without
being handed the connection: the object has no `close`, and the application that opened the
connection keeps shutdown. It lists nothing and discovers nothing. It is not a collection of tracks.

The accessor used to be `tracks`, which reads as an enumerable collection or a discovery API.
`trackSource` names the contract the object implements.

The core's names inside that contract are unchanged on purpose. `MoqTrackSource` and
`SubscriptionHandlers` are the upstream-facing generic interface that the Video.js protocol modules
and this repository share; renaming them to product vocabulary would fork the generic contract to
fix a product naming problem. The product handler type `RelaySubscriptionHandlers` maps onto the
core's handlers one to one, and `subscriber.subscribe()` performs the mapping.

## Renames without aliases

The old names were removed rather than kept as deprecated aliases. Both packages are unreleased
and marked provisional, nothing outside this repository imports them, and a deprecated alias would
keep the misleading name in autocomplete next to the one that replaced it. Consumers inside the
repository (examples, live smoke tests, the Video.js streaming example) moved in the same change.
Once a package is published, a rename of this kind needs a deprecation cycle; record that policy
here when the first release happens.

## One error identity: `RelayError`, `code`, `phase`

Both SDKs raise one `RelayError` whose `code` says what happened and whose `phase` says where, the
identity `spec/behavior.md` tells callers to branch on. The three subscription errors
(`SubscriptionRequestError`, `SubscriptionOverflowError`, `SubscriptionConnectionClosedError`)
subclass it in both SDKs because they carry payload the code cannot: the relay's `RequestError`, or
the connection's failure as the cause.

Not adopted: Python-only exception subclasses per code (`RelayClosedError`, `RelayTimeoutError`,
`RelayNamespaceError`, `RelayGoawayError`, `RelayWritePendingError`, and the never-raised alias
`RelayTrackEndedError`). They duplicated the `code` discriminant under names TypeScript did not have,
so a caller switching languages met two identities for one failure, and `except RelayError as
error: if error.code == "WRITE_TIMEOUT"` is one line longer than a subclass catch. Also not adopted:
a `RELAY_ERROR` fallback code for a `RelayError` built without one (`code` is required in both SDKs),
and a transport error that is itself a Relay error (`WebTransportError` is the transport layer's own
exception, wrapped as `TRANSPORT_ERROR` at the Relay boundary). Removed without aliases under the
policy above; reversible before the first release, so no ADR.

`UNAUTHORIZED` names a connection whose credentials the relay refused, after the MOQT session
termination code the relay closes with (0x2). Both SDKs list it, as both list `TRANSPORT_UNAVAILABLE`,
which only TypeScript raises; only Python raises `UNAUTHORIZED` until browsers can see the relay's close
code (#14). Not adopted: `AUTHENTICATION_FAILED` or `FORBIDDEN`. The relay closes with the same code
for a missing, invalid, expired or out-of-scope token, so one name covers them, and it is the
protocol's. A subscription refused for its grant stays a MOQT `RequestError` on `onError` / `on_error`.

## Async subscription consumption

`subscriber.subscribeEvents()` / `subscriber.subscribe_events()` creates a bounded iterator of
object and lifecycle events. The `subscribed` and `publisherDone` / `publisher_done` events use the
same vocabulary as the product callbacks. It owns its subscription, never the connection. See the
[async consumption contract](behavior.md#async-subscription-consumption) for capacity, cancellation,
and termination semantics. Product and core callback objects are validated and snapshotted when
subscribing; callbacks remain synchronous, and async consumers use this iterator.

## Subscription readiness and termination: `RelaySubscription`, `subscribed`, `ended`

`subscribe()` returns a `RelaySubscription`: the core handle (`requestId`, `update()`, `cancel()`)
plus two awaitables. `subscribed` settles when the relay answers the request: it resolves with the
acceptance info `onSubscribed` receives, and rejects with the same error classes the events
iterator raises (`SubscriptionRequestError` for REQUEST_ERROR before acceptance, including the
request timeout; `SubscriptionConnectionClosedError` when the connection ended first) or with the
reason given to `cancel()`. `ended` settles once, when the subscription can no longer deliver, and
never rejects: its `SubscriptionEnd` value carries `kind` (`rejected`, `terminated`, `cancelled`;
the type is `SubscriptionEndKind`), the protocol `cause`, whether the subscription had been
`accepted`, and the `error`. That mirrors the connection's `closed`, which resolves with the error
as data. A connection that ends before the relay answers is a `terminated` ending with the
connection error, never a `rejected` one, although the core fails the pending request first.

In Python the two are `RelayAwaitable[T]` rather than bare futures, as are the publisher's
`announced` and both connections' `closed`: a task cancelled while awaiting a future cancels the
future itself, and `asyncio.wait_for` does the same on timeout, so a shared future would let one
impatient waiter destroy the signal for every later one. The awaitable isolates each waiter and
reads status without allocating. `Future` was rejected for that reason; `Signal` and `Promise` for
naming a mechanism rather than the fact the caller awaits. The type was `SubscriptionAwaitable`
until `announced` and `closed` joined it; a name that said "subscription" would have been wrong for
three of its four uses.

Publisher completion settles neither. `onPublisherDone` is a control notification; trailing objects
can still arrive and the request stays open, so a `subscribed` or `ended` that moved on it would
promise what the protocol cannot establish.

Not adopted: `ready` or `accepted` for readiness (the callback is `onSubscribed`, and the two must
agree); `closed`, `done`, `drained` or `complete` for termination (`closed` collides with the
connection's word for a different scope, `done` with publisher completion, and the last two claim a
delivery property the protocol does not give); an `ended` that rejects (an ending is data about the
subscription, not a failure of the caller's await, and the connection's `closed` set the precedent).

## Announce readiness: `announced`

`connectRelayPublisher` announces the namespace once the first track is registered, and the relay
acknowledges an announce with nothing: the strongest fact the publisher can have is that the
NAMESPACE entry was handed to the transport on a solicitation that covers the namespace.
`announced` settles on that fact, once, with no value. The relay routes subscribes to the namespace
about a round trip later, so a subscribe that races the announce is rejected (16 `unroutable` on
moq-relay 0.15.8, 404 before) and the application retries it (see `behavior.md`); a process that
owns both sides awaits `announced` before subscribing, which removes most of that race. It stays
pending while no solicitation covers the namespace, where the first write's
`NAMESPACE_NOT_SOLICITED` is the diagnostic, and the connection's closure terminates it if that
comes first, as it does `subscribed`.

It is a property awaitable like `closed` and `subscribed`; in Python a `RelayAwaitable[None]`.
Not adopted: `announce()` as an awaitable verb (announcing is part of connecting and registering,
not a step the application takes); `ready` (the connection is ready long before the announce, and
the namespace is never "ready" for a viewer the relay has not routed yet); `solicited` (the fact the
publisher observes is its own announce leaving, not the relay's request for it); resolving with the
namespace (the publisher's own, `publisher.namespace` in Python and the caller's option in
TypeScript).

## Names considered and left alone

- `RelayTrackOptions`, the subscribe request shape (`namespace`, `track`, `parameters`), and
  `ConnectRelayOptions`, the connection options shared by both roles: issue #72 kept the name for
  the shared shape and added the role types beside it.
- `SubscriptionEnd`'s field names `kind`, `cause`, `accepted`, `error`: plain words for a value most
  callers only log or branch on once.
- `RelayTrackWriter`, `RelayGroupWriter`, `write`, `openGroup`, `close`, `abort`: these say what
  they do.
- Everything in `@mux/moq-core` and `moq_core`: protocol vocabulary (`onOk`, `onDone`,
  `registerTrack`, `TrackBinding`) stays as the drafts and the upstream modules spell it. The
  product packages re-export the same short list of core names (the "Protocol types borrowed from
  the core" row; Python adds `Closed`, which TypeScript spells inline) and nothing else;
  `python/packages/mux-relay/tests/test_exports.py` checks that list against `ts/src/index.ts`.

## Media format helpers

Stage 1 media format names. Python exports these from `moq_media`; TypeScript exports them from
`@mux/moq-media`. The source-URL composer and catalog-generation timestamp retain their language-specific
names from the original examples; the mappings below are explicit.

| Concept | TypeScript | Python |
| --- | --- | --- |
| Video description | `VideoTrackInfo` | `VideoTrackInfo` |
| Audio description | `AudioTrackInfo` | `AudioTrackInfo` |
| Data description | `DataTrackInfo` | `DataTrackInfo` |
| Catalog input | `MsfCatalogInput` | `build_msf_catalog` keyword arguments |
| Build independent catalog bytes | `buildMsfCatalog` | `build_msf_catalog` |
| Read an independent catalog object's payload | `parseMsfCatalog(payload, { catalogNamespace })` | `parse_msf_catalog(payload, catalog_namespace=...)` |
| Catalog as read | `MsfCatalog` | `MsfCatalog` |
| Track description as read | `MsfCatalogTrack` | `MsfCatalogTrack` |
| Reader options | `ParseMsfCatalogOptions` | `parse_msf_catalog` keyword arguments |
| LOC track properties | `locTrackProperties` | `loc_track_properties` |
| LOC object properties from microseconds | `locProperties` / `LocPropertiesOptions` | `loc_properties` keyword arguments |
| Compose source URL | `composeMoqSource` / `ComposeMoqSourceOptions` | `compose_player_url` keyword arguments |
| Escape UTF-8 component | `encodeNameComponent` | `encode_name_component` |
| Encode namespace plus track name | `encodeNamespaceName` | `encode_namespace_name` |
| H.264 configuration record | — (Python only) | `H264Config` |
| Configuration from Annex B | — (Python only) | `H264Config.from_annexb` |
| Configuration bytes / codec string | — (Python only) | `H264Config.avcc` / `.codec` |
| Configuration's access-unit conversion | — (Python only) | `H264Config.to_avcc` |
| Split Annex B NALs | — (Python only) | `iter_annexb_nal_units` |
| NAL type | — (Python only) | `nal_unit_type` |
| Extract SPS/PPS | — (Python only) | `extract_parameter_sets` |
| Build avcC | — (Python only) | `build_avcc` |
| Codec string from avcC header | `avcCodecString` | `avc_codec_string` |
| Convert Annex B access unit | — (Python only) | `annexb_to_avcc` |

Constants use the same spelling in both languages: `LOC_DOCUMENT`, `MSF_DOCUMENT`,
`MSF_CATALOG_VERSION`, `MICROSECONDS_PER_SECOND`, `CATALOG_TRACK`, `VIDEO_TRACK`, `AUDIO_TRACK`,
`LOC_TIMESTAMP`, `LOC_TIMESCALE`, `LOC_VIDEO_CONFIG`, and `LOC_AUDIO_CONFIG`.
Python also retains the example's `TRACK_TIMESCALE_PROPERTIES` as a tuple; new code should call
`loc_track_properties()` for fresh properties.

Track fields use camelCase in TypeScript and snake_case in Python (`initData` / `init_data`,
`channelConfig` / `channel_config`, `isLive` / `is_live`). Catalog generation time is `generatedAt` /
`generated_at_ms`; completeness is `isComplete` / `is_complete`.

The catalog reader mirrors the builder: `parse` because it reads a document the SDK did not
produce, `Msf` because it reads msf-01's catalog, not a Relay concept. Its results are reader-side
types rather than the builder's `VideoTrackInfo` / `AudioTrackInfo` / `DataTrackInfo`, because a
read track may be any role and any packaging (moq-rs `cmaf`, `legacy`) and carries the namespace,
packaging, and live status the builder fixes. The TypeScript parser was internal as
`parseIndependentCatalog` (with `MoqCatalog` / `MoqCatalogTrack` from Video.js); those names are gone,
with no alias. Media publishing-handle and object-to-frame reader names remain provisional.

The TypeScript-only WebCodecs helpers in `@mux/moq-media` are `videoTrackFromDecoderConfig`,
`audioTrackFromDecoderConfig`, `chunkToLocFrame`, `LocFrame`, and `ChunkToLocFrameOptions`.
They consume standard decoder configs and native encoded video/audio chunks. The optional
`@mux/moq-mediabunny` file-input integration exports `packetToLocFrame` and
`PacketToLocFrameOptions`; it retains compatibility re-exports of the config mappers and `LocFrame`.
Python keeps its PyAV Annex B conversion helpers.

## Media publishing: `FrameWriter`, `CatalogService`

The media publishing policy the Video.js example proved, lifted into the Python product package as
`mux_relay.media` ([issue #84](https://github.com/muxinc/relay-sdks/issues/84)); its contract is the
"Media publishing" section of [behavior.md](behavior.md#media-publishing). The TypeScript column is
empty until that SDK gains the module. The root package exports none of these names in either
language; `python/packages/mux-relay/tests/test_exports.py` pins the module's list.

| Concept | TypeScript | Python |
| --- | --- | --- |
| Serve a state-shaped track: send its document on every relay subscription | — (not yet implemented) | `CatalogService(track, document, retry_delay=1.0)` |
| Run the service until cancelled or the track ends | — | `await service.serve()` |
| Documents accepted, and failures that were retried | — | `service.sends`, `service.failures` |
| Map encoded frames onto a track's groups | — | `FrameWriter(track, grouping="keyframe", stale=None)`; `grouping="frame"` for one group per frame |
| Send one frame if the relay wants it | — | `await writer.write(frame)` returns a `FrameOutcome` |
| What became of a frame | — | `FrameOutcome` with `kind` (a `FrameOutcomeKind`: `sent`, `skipped`, `stale`, `interrupted`, `ended`), `error`, `group_id` |
| One encoded frame as the writer sees it | — | `Frame`, a protocol: `payload`, `key`, `properties`, `discontinuity` |
| FIN the open group and refuse further frames | — | `await writer.close()` |
| The writer ended, and how | — | `writer.closed`, `writer.ended` |
| Groups opened and objects accepted | — | `writer.groups`, `writer.objects` |

`FrameWriter.write` keeps the SDK's one verb for sending bytes, one tier above
`RelayTrackWriter.write`: it sends when the relay wants the frame, and its return value is what
says this write can decline (skip, drop, report) instead of waiting for demand or raising. The
failures it reports are data because an encoder loop keeps producing after any of them; catching an
exception per frame would be the caller's whole loop.

`Frame` is a protocol, not a class. The application's own encoded-sample type qualifies once it
carries the frame's object properties, so nothing is converted between the encoder and the writer.

Not adopted: `FramePublisher.publish()` ("publisher" is the connection role, and `publish()` was
already rejected as the name of a step that does not send); `MediaTrackWriter.write_frame()`
(redundant on a type named for frames); an events iterator for the catalog service (two counters
answer what the example's `finished` line asks); exceptions for interrupted and ended frames (see
above). The `register_media` handle, `update()` of a served document, and the readers in
[media-layer.md](media-layer.md) remain proposals.
