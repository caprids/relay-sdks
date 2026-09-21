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
| Ask the relay for a track's objects | `subscriber.subscribe(options, handlers)` returns `Subscription` | `subscriber.subscribe(namespace, track, handlers, parameters=...)` returns `Subscription` |
| Events on one subscription | `RelaySubscriptionHandlers` | `RelaySubscriptionHandlers` |
| The relay accepted the subscription | `onSubscribed(info)` | `on_subscribed(info)` |
| The relay rejected it, or it failed before acceptance | `onError(error)` | `on_error(error)` |
| An object arrived | `onObject(object)` | `on_object(object)` |
| A subgroup stream ended cleanly or was reset | `onSubgroupEnd(info)`, `onSubgroupReset(info)` | `on_subgroup_end(info)`, `on_subgroup_reset(info)` |
| The publisher said it is finished with the track | `onPublisherDone(info)` | `on_publisher_done(info)` |
| Protocol notifications on the request stream | `onUpdateOk()`, `onStateNotify(parameters)`, `onGoaway(goaway)` | `on_update_ok()`, `on_state_notify(parameters)`, `on_goaway(goaway)` |
| Register a track this publisher will serve | `publisher.registerTrack(options)` returns `RelayTrackWriter` | `publisher.register_track(track, ...)` returns `RelayTrackWriter` |
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

The connection options types are not part of this decision. Publisher options currently extend
the subscriber options; separating shared connection options from subscriber-only ones is
[issue #72](https://github.com/muxinc/relay-sdks/issues/72).

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
- The relay keeps its upstream subscription for a while after the last viewer leaves (about 30
  seconds against the tested relay; see [issue #24](https://github.com/muxinc/relay-sdks/issues/24)).
  `hasDemand` therefore lags the last viewer, and `true` does not prove anyone is watching.
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
The awaitable readiness result planned in issue #68 settles on the same event, so the two names
will agree.

`onError` keeps its name: the relay rejected the subscription, or it failed before acceptance
(request timeout, connection shutdown, GOAWAY). It is not called for failures after acceptance;
those surface as subgroup resets and connection closure.

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

## Async subscription consumption

`subscriber.subscribeEvents()` / `subscriber.subscribe_events()` creates a bounded iterator of
object and lifecycle events. The `subscribed` and `publisherDone` / `publisher_done` events use the
same vocabulary as the product callbacks. It owns its subscription, never the connection. See the
[async consumption contract](behavior.md#async-subscription-consumption) for capacity, cancellation,
and termination semantics. Product and core callback objects are validated and snapshotted when
subscribing; callbacks remain synchronous, and async consumers use this iterator.

## Names considered and left alone

- `RelayTrackOptions`, the subscribe request shape (`namespace`, `track`, `parameters`), and
  `ConnectRelayOptions`, the connection options shared by both roles. Issue #72 separates the
  options types by role and can rename them then.
- `Subscription`, the handle `subscribe()` returns (`requestId`, `update()`, `cancel()`). Issue #68
  extends it with readiness and termination and may introduce a product handle type.
- `RelayTrackWriter`, `RelayGroupWriter`, `write`, `openGroup`, `close`, `abort`: these say what
  they do.
- Everything in `@mux/moq-core` and `moq_core`: protocol vocabulary (`onOk`, `onDone`,
  `registerTrack`, `TrackBinding`) stays as the drafts and the upstream modules spell it.

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
| LOC track properties | `locTrackProperties` | `loc_track_properties` |
| LOC object properties from microseconds | `locProperties` / `LocPropertiesOptions` | `loc_properties` keyword arguments |
| Compose source URL | `composeMoqSource` / `ComposeMoqSourceOptions` | `compose_player_url` keyword arguments |
| Escape UTF-8 component | `encodeNameComponent` | `encode_name_component` |
| Encode namespace plus track name | `encodeNamespaceName` | `encode_namespace_name` |
| H.264 configuration record | `H264Config` | `H264Config` |
| Configuration from Annex B | `H264Config.fromAnnexb` | `H264Config.from_annexb` |
| Configuration bytes / codec string | `H264Config.avcc` / `.codec` | `H264Config.avcc` / `.codec` |
| Configuration's access-unit conversion | `H264Config.toAvcc` | `H264Config.to_avcc` |
| Split Annex B NALs | `iterAnnexbNalUnits` | `iter_annexb_nal_units` |
| NAL type | `nalUnitType` | `nal_unit_type` |
| Extract SPS/PPS | `extractParameterSets` | `extract_parameter_sets` |
| Build avcC | `buildAvcc` | `build_avcc` |
| Codec string from avcC header | `avcCodecString` | `avc_codec_string` |
| Convert Annex B access unit | `annexbToAvcc` | `annexb_to_avcc` |

Constants use the same spelling in both languages: `LOC_DOCUMENT`, `MSF_DOCUMENT`,
`MSF_CATALOG_VERSION`, `MICROSECONDS_PER_SECOND`, `CATALOG_TRACK`, `VIDEO_TRACK`, `AUDIO_TRACK`,
`LOC_TIMESTAMP`, `LOC_TIMESCALE`, `LOC_VIDEO_CONFIG`, and `LOC_AUDIO_CONFIG`.
Python also retains the example's `TRACK_TIMESCALE_PROPERTIES` as a tuple; new code should call
`loc_track_properties()` for fresh properties.

Track fields use camelCase in TypeScript and snake_case in Python (`initData` / `init_data`,
`channelConfig` / `channel_config`). Catalog generation time is `generatedAt` / `generated_at_ms`.
Media publishing-handle and reader names remain provisional.
