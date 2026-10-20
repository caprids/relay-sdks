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
| Subscribe through a handle that can move to another track make-before-break | `subscriber.subscribeSwitchable(options, SwitchableHandlers)` returns `SwitchableSubscription` | `subscriber.subscribe_switchable(namespace, track, SwitchableHandlers, parameters=...)` returns `SwitchableSubscription` |
| Move it, and learn of the cutover | `switchable.switchTo(track, SwitchOptions)` (`parameters`, `until`, `drainMs`, `signal`) resolves `SwitchedInfo` (`from`, `to`, `subscribed`); `onSwitched(info)`; `switchable.track`, `switchable.pending` | `switchable.switch_to(track, parameters=..., until=..., drain=..., cancellation=...)` returns `RelayAwaitable[SwitchedInfo]` (`from_track`, `to_track`, `subscribed`); `on_switched(info)`; `switchable.track`, `switchable.pending` |
| Learn which namespaces under a prefix are announced | `subscriber.namespaceEvents({ prefix })` returns `NamespaceEvents`, yielding `NamespaceEvent` | `subscriber.namespace_events(prefix)` returns `NamespaceEvents`, yielding `NamespaceEvent` |
| A namespace was announced, or its announce withdrawn | `{ type: 'announced' \| 'unannounced', namespace }` | `NamespaceEvent(type='announced' \| 'unannounced', namespace=...)` |
| Register a track this publisher will serve | `publisher.registerTrack(options)` returns `RelayTrackWriter` | `publisher.register_track(track, ...)` returns `RelayTrackWriter` |
| Await the namespace announce reaching the wire | `publisher.announced` resolves `void` | `await publisher.announced` returns `None` |
| Send one object as its own group | `writer.write(bytes, options)` | `await writer.write(payload, properties=...)` |
| Send one UTF-8 text object | `writer.writeText(text, options)` | `await writer.write_text(text, properties=...)` |
| Send one JSON object | `writer.writeJSON(value, options)` | `await writer.write_json(value, properties=...)` |
| Read an object as UTF-8 text or JSON | `readText(object)`, `readJSON(object)` | `read_text(obj)`, `read_json(obj)` |
| SDK failure with a stable code and operation phase | `RelayError`, `RelayErrorCode`, `RelayErrorPhase` | `RelayError`, `RelayErrorCode`, `RelayErrorPhase` |
| Send several objects in one group | `writer.openGroup()` returns `RelayGroupWriter`; `openGroup({ payload, properties })` opens it with object 0 | `await writer.open_group()` returns `RelayGroupWriter`; `open_group(payload, properties=...)` opens it with object 0 |
| Does the relay currently want this track | `writer.hasDemand` | `writer.has_demand` |
| Follow every change in relay demand | `writer.demandChanges()` | `writer.demand_changes()` |
| The track ended and cannot be written again | `writer.closed` | `writer.closed` |
| Borrowed protocol-level track access for integrations | `subscriber.trackSource` (a `MoqTrackSource`) | `subscriber.track_source` (a `MoqTrackSource`) |
| The connection ended, and why | `connection.closed` resolves `{ error? }` | `await connection.closed` returns `Closed(error=...)` |
| Cancel a connection's whole lifetime from outside | `AbortSignal` (platform), given as `signal` | `CancellationToken`, given as `cancellation=`; `Cancelled` is the reason it raises when `cancel()` was given none |
| Options every connection takes: endpoint, token provider, lifetime cancellation, connection deadline, transport | `ConnectRelayOptions` (`url`, `getToken`, `signal`, `connectTimeoutMs`, `createTransport`) | the shared keyword arguments of both entry points (`url`, `get_token`, `cancellation`, `connect_timeout`, `create_transport`) |
| A token with the time the relay stops honoring it, which a token provider may return | `RelayCredential` (`token`, `expiresAt`: a `Date`) | `RelayCredential` (`token`, `expires_at`: an aware `datetime`) |
| When the relay will end a connection for its credential's expiry | `connection.expiresAt` (`Date \| undefined`) | `connection.expires_at` (`datetime \| None`) |
| Options only a subscriber connection takes | `ConnectRelaySubscriberOptions`: the shared options plus `requestTimeoutMs` | `connect_relay_subscriber`'s shared keyword arguments plus `request_timeout` |
| Options only a publisher connection takes | `ConnectRelayPublisherOptions`: the shared options plus `namespace`; no request deadline | `connect_relay_publisher`'s shared keyword arguments plus `namespace`; no request deadline |
| Connect from Node over native QUIC, and the trust options that takes | `@mux/relay/node`: the same exports, its connect functions taking `RelayNodeOptions<T>`: `RelayNodeTrustOptions` (`ca`, `serverCertificateHashes`, each hash a `RelayCertificateHash`) or `createTransport`, not both | not applicable: the Python SDK has its own QUIC stack |
| Supply the transport | `createTransport(url, protocols)` returns `{ transport, ready, framing? }` (`RelayTransportFactory`); browser WebTransport by default | `create_transport(url, protocols)` returns `ConnectingTransport(transport, ready, framing="webtransport")` (`RelayTransportFactory`); `mux_relay.quic.connect_quic`, the aioquic native QUIC factory, by default |
| A transport factory that speaks native QUIC: MOQT on QUIC, ALPN `moqt-22` then `moqt-20`, PATH and AUTHORITY in SETUP | `nativeQuicTransport(options)` from `@mux/relay-quic` | `mux_relay.quic.connect_quic(url, protocols, configuration=...)`, given as `create_transport`; not the default until #231, and the name stays open to #222 |
| A transport factory that speaks QMux over WebSocket: subprotocols `qmux-01.moqt-22` then `qmux-01.moqt-20`, path and token in the upgrade URL, `framing: 'websocket'` | `createWebSocketTransport()` from `@mux/relay-websocket` | not applicable: the Python SDK ships no WebSocket adapter (#321) |
| WebTransport first, WebSocket when its handshake fails or takes longer than a timeout, and its options | `withWebSocketFallback(options)` from `@mux/relay-websocket`: `WebSocketFallbackOptions` (`handshakeTimeoutMs`, default 4000; `onFallback`) | not applicable |
| Consume one subscription as an async iterator | `subscriber.subscribeEvents(options)` returns `SubscriptionEvents` | `subscriber.subscribe_events(namespace, track, ...)` returns `SubscriptionEvents` |
| One event from the events iterator | `SubscriptionEvent`, discriminated by `type` | `SubscriptionEvent`, the union of `SubscribedEvent`, `ObjectEvent`, `PublisherDoneEvent`, `SubgroupEndEvent`, `SubgroupResetEvent`, `StateNotifyEvent`, `GoawayEvent` and `UpdateOkEvent`, each also carrying `type` |
| Protocol types borrowed from the core | `Goaway`, `KeyValuePair`, `LocationFilter`, `MessageParameters`, `MoqTrackSource`, `MoqtObject`, `MoqtTransport`, `PublishDone`, `RequestError`, `SubscribeOk`, `Subscription`, `SubscriptionHandlers`, re-exported as types | the same names re-exported from `moq_core`, plus `Closed`; every other core name is imported from `moq_core` |
| Open a publisher that reconnects, one generation per connection | `connectSupervisedPublisher(options)` returns `SupervisedPublisher` | `connect_supervised_publisher(url, namespace=...)` returns `SupervisedPublisher` |
| Open a subscriber that reconnects and keeps tracks subscribed | `connectSupervisedSubscriber(options)` returns `SupervisedSubscriber` | `connect_supervised_subscriber(url, ...)` returns `SupervisedSubscriber` |
| One supervised publisher connection and the namespace it serves | `publisher.generation` is a `PublisherGeneration` (`generation`, `namespace`); `onGeneration(info)` | `publisher.generation` is a `PublisherGeneration` (`generation`, `namespace`); `on_generation(info)` |
| Where a supervisor stands | `SupervisedStatus`: `'connecting' \| 'live' \| 'reconnecting' \| 'closed'`, reported to `onStatus(status, error)` | `SupervisedStatus`, the same strings, to `on_status(status, error)` |
| Keep a track subscribed until told otherwise | `subscriber.want(key, WantOptions, WantHandlers)`, `subscriber.unwant(key)` | `subscriber.want(key, namespace, track, WantHandlers, ...)`, `subscriber.unwant(key)` |
| Follow a supervised publisher's newest generation | `generations: true` on `WantOptions` | `generations=True` |
| Resubscribe a track that went silent | `staleAfterMs` | `stale_after` (seconds) |
| Replace a want's subscription at the live edge because it delivers too late | `subscriber.catchUp(key)`; `onRestarted` reason `'catch-up'` | not yet (TypeScript only) |
| A want replaced its subscription; drop state that assumed continuity | `onRestarted({ reason, namespace })`, `RestartReason`; `onSubscribed(info, { namespace })` | `on_restarted(reason, namespace)`, `RestartReason`; `on_subscribed(info, namespace)` |
| A want moved make-before-break (a newer generation, or another track or parameters) | `until` and `drainMs` on `WantOptions`; `onSwitched(info)` with `WantSwitchedInfo` (`from`, `to`, each a `WantTarget` of `namespace` and `track`; `subscribed`) | `until=` and `drain=` (seconds) on `want`; `on_switched(info)` with `WantSwitchedInfo` (`from_namespace`, `from_track`, `to_namespace`, `to_track`, `subscribed`) |
| Replace a supervisor's connection make-before-break before its credential expires | `rotateBeforeMs` (default 60 000); `publisher.rotate()`, `subscriber.rotate()` | `rotate_before` (seconds, default 60); `publisher.rotate()`, `subscriber.rotate()` |
| Backoff bounds between attempts | `SupervisionBackoff` (`initialDelayMs`, `maxDelayMs`), given as `reconnect` or `retry` | `SupervisionBackoff` (`initial_delay`, `max_delay`, seconds), as `reconnect=` or `retry=` |
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
- The relay decides when its upstream subscription ends. moq-relay 0.15 and later cancel it about one
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
and a transport error that is itself a Relay error (the transport layer's own exception, a browser's
`WebTransportError` or Python's `QuicTransportError`, is wrapped as `TRANSPORT_ERROR` at the Relay
boundary). Removed without aliases under the
policy above; reversible before the first release, so no ADR.

`UNAUTHORIZED` names a connection whose credentials the relay refused, after the MOQT session
termination code the relay closes with (0x2). Both SDKs list it, as both list `TRANSPORT_UNAVAILABLE`,
which only TypeScript raises. Both raise `UNAUTHORIZED`; TypeScript in a browser only against moq-relay
0.17.0 or later, since older relays' close never reached the page (#14). Not adopted:
`AUTHENTICATION_FAILED` or `FORBIDDEN`. The relay closes with the same code for a missing, invalid,
expired or out-of-scope token, so one name covers them, and it is the protocol's. A subscription
refused for its grant stays a MOQT `RequestError` on `onError` / `on_error`.

`DELIVERY_TIMEOUT` names a write whose object outlived the draft's delivery timeout (draft-22 §5.2)
before it reached the transport, after the stream reset code the publisher sends with it (0x2). The
term is in `CONTEXT.md`. Not adopted: reusing `WRITE_TIMEOUT`, because the write deadline is the
SDK's own bound on one operation and calls for a different response (data that missed a write
deadline may still be worth sending; data past its delivery timeout is stale and is not retried as
it is), nor `OBJECT_EXPIRED` or `STALE_OBJECT`, which name no protocol term. A settling group reset
by its subgroup timeout reports nothing, so the code only ever comes from the object timeout.
Both SDKs raise it.

## Async subscription consumption

`subscriber.subscribeEvents()` / `subscriber.subscribe_events()` creates a bounded iterator of
object and lifecycle events. The `subscribed` and `publisherDone` / `publisher_done` events use the
same vocabulary as the product callbacks. It owns its subscription, never the connection. See the
[async consumption contract](behavior.md#async-subscription-consumption) for capacity, cancellation,
and termination semantics. Product and core callback objects are validated and snapshotted when
subscribing; callbacks remain synchronous, and async consumers use this iterator.

## Namespace discovery: `namespaceEvents`, `announced`, `unannounced`

`subscriber.namespaceEvents({ prefix })` / `subscriber.namespace_events(prefix)` is the subscriber's
discovery entry point ([behavior.md](behavior.md#discovery)). It is named after `subscribeEvents`,
because it is the same bounded iterator over one owned request, and after what it yields. The
events reuse the publisher's word: `announced` means a namespace's announce reached the relay's
routing table, as `publisher.announced` means it left the publisher. `unannounced` is its
withdrawal, by a clean close or by the relay noticing a crash.

The iterator ends with the subscription error classes (`SubscriptionRequestError`,
`SubscriptionConnectionClosedError`, `SubscriptionOverflowError`): SUBSCRIBE_NAMESPACE is a
subscription in the draft's terms, and a second set of classes with the same meanings would only
add names. The prefix is a namespace prefix, matched field by field, never a string prefix.

Not adopted: `announcements()` (CONTEXT.md keeps "announcement" out because it names the act and
the relay's state interchangeably); `watchNamespaces()` (a verb, where every other iterator is named
for what it yields); `subscribeNamespace()` (the core's protocol name, which `trackSource` already
keeps on the protocol side); `published` / `unpublished` (the draft's PUBLISH is a different
message).

## Switching tracks: `subscribeSwitchable`, `switchTo`, `onSwitched`

A switchable subscription is the primitive for changing what a viewer receives
([behavior.md](behavior.md#switching-tracks)). The entry point is a verb, `subscribeSwitchable`,
because it sends a SUBSCRIBE like `subscribe`; the handle is named for what it can do. `switchTo`
names the act the application asks for; `onSwitched` names the cutover once it has happened. Not
adopted: `switchable()` (an adjective for a call that sends a request), `rendition` (a media word
for what is, to the SDK, any track), `update` (REQUEST_UPDATE, which cannot change the track and
which moq-relay answers by ending the subscription).

Handlers take the active track's name as their last argument rather than a context record, so one
handler set serves every track. Python spells `SwitchedInfo`'s fields `from_track` and `to_track`
because `from` is a keyword, and gives the drain in seconds (`drain=`) as it does every duration.

## Supervised connections: `connectSupervised*`, generation, `want`, `onRestarted`

The supervisors are opt-in policy over the primitives ([behavior.md](behavior.md#supervised-connections)).
Their entry points keep the `connect` verb, because each resolves once a connection is up, and add
`Supervised` instead of an option on `connectRelay*`: they return different types (a `generation`
and `want`, which the primitives lack), and one function with two return shapes would hide that.
They live in `@mux/relay` and `mux_relay` themselves, not in a separate package.

**Generation** names one supervised publisher connection and the namespace field it serves. Each
reconnect is a new publisher to the relay, and the word says so: it is not a session, an epoch, or a
reconnect count.

`want(key, ...)` is declarative: it states which track the application wants, keyed by a name the
application chooses, and the supervisor owns how that stays true. `unwant` undoes it. Not adopted:
`subscribe` (the primitive's verb, which returns one subscription that ends); `follow` and `watch`
(they suggest observing without receiving data); `ensureSubscribed` (a verb phrase for a standing
wish).

Python's `want` takes the namespace and track as arguments, as `subscribe` does, and its handlers
receive the namespace as a second argument (`on_subscribed(info, namespace)`,
`on_restarted(reason, namespace)`), as `on_status(status, error)` does, rather than through a record
type of their own.

**Rotation** names replacing a supervisor's connection before its credential expires, with the old
one serving until the new one has taken over. It is not a reconnect (nothing was lost) and not a
refresh (the relay cannot take a new token on a live session, so the connection itself is
replaced). `RelayCredential` is the token with its expiry, not a second kind of token; `expiresAt`
says when the relay ends the session, which is the fact an application plans around. Not adopted:
`refreshToken` and `onTokenExpiring` (no token is refreshed in place), `renew`, `cycle`,
`tokenExpiry` (the session's end is what matters, and the grant can end it before `exp`).

`onRestarted` tells a decoder that the next object starts a new subscription. It is named for that
fact rather than for the cause, which `reason` carries.

A want's make-before-break move reuses the switchable's words: `until`, `drainMs` / `drain` and
`onSwitched` mean what they mean on `switchTo`, so the two cutovers read the same. A move is not a
restart, because nothing was lost in between, so it gets `onSwitched` and not an `onRestarted`
flag. A want's record is `WantSwitchedInfo` rather than `SwitchedInfo`, because a generation move
changes the namespace as well as the track. Not adopted: a `want(key).switchTo(...)` verb. `want`
is declarative, and wanting the key again with another track already states the change. `RECONNECTING` names the publisher's state
when a write fails between generations, and keeps it apart from `CLOSED`, which retires a frame
writer.

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
about a round trip later, so a subscribe that races the announce is rejected (16 `unroutable` from
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

## Transport errors by shape: `source`, `QuicTransportError`

A transport's stream or session failure is classified by its fields, not its class
(`spec/transport-adapter.md` TA-22): `source` is `stream` or `session`, a stream error carries the
peer's code as `streamErrorCode` / `stream_error_code` (`null` / `None` when unknown, never 0), and a
session error that reports the peer's close carries `closeCode` and `reason` / `close_code` and
`reason` (TA-24). TypeScript reads them with `isStreamError` from `@mux/moq-core`; Python with
`is_stream_error` and `is_session_error` from `moq_core.transport`. A peer STOP_SENDING is a stream
error raised on a send stream; where it surfaces makes it a stop.

`QuicTransportError` is the exception `mux_relay.quic` raises with that shape, the counterpart of
`@mux/relay-quic`'s internal `NativeQuicSessionError` and the core's `MoqtStreamError` in one class:
the Python core hands its own reset reasons over (`StreamReset`, with `stream_error_code`), so it
needs no stream error class. It is private
(`mux_relay.quic._errors`, not in `mux_relay.quic.__all__`), as `NativeQuicSessionError` is: the
shape is the contract, and a caller who catches the class instead would be classifying by name. It
replaced `WebTransportError` and its `StreamStopped` subclass without aliases, under the policy
above: the package no longer dials WebTransport (#232), so the old name was a misnomer, and the
subclass made a stop a type rather than a place. Not adopted: `TransportError`, which reads as the
`TRANSPORT_ERROR` code it is wrapped in, and a `StreamStopped` subclass kept beside the shape.

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
| Build the end-of-broadcast catalog (msf-01 §11.3) | `buildMsfEndCatalog` / `MsfEndCatalogInput` | `build_msf_end_catalog` keyword arguments |
| Read an independent catalog object's payload | `parseMsfCatalog(payload, { catalogNamespace })` | `parse_msf_catalog(payload, catalog_namespace=...)` |
| Catalog as read | `MsfCatalog` | `MsfCatalog` |
| Track description as read | `MsfCatalogTrack` | `MsfCatalogTrack` |
| Reader options | `ParseMsfCatalogOptions` | `parse_msf_catalog` keyword arguments |
| LOC track properties | `locTrackProperties` | `loc_track_properties` |
| LOC object properties from microseconds | `locProperties` / `LocPropertiesOptions` | `loc_properties` keyword arguments |
| An object's LOC Timestamp in microseconds | `locTimestampUs(properties, { trackProperties? })` (`LocTimestampOptions`) | `loc_timestamp_us(properties, *, track_properties=())` |
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
`channelConfig` / `channel_config`, `isLive` / `is_live`, the read-only `renderGroup` / `render_group`). Catalog generation time is `generatedAt` /
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

## Shared timeline and playout clock

The broadcast and playout clocks of [media-layer.md](media-layer.md#shared-timeline-and-playout-clock)
([issue #285](https://github.com/muxinc/relay-sdks/issues/285)), exported from `@mux/moq-media` and
`moq_media`. TypeScript uses factories that return interfaces, like the rest of that package; Python
uses classes, like `CatalogService` and `FrameWriter`.

| Concept | TypeScript | Python |
| --- | --- | --- |
| The publisher's one clock for every track | `createBroadcastClock(options?)` → `BroadcastClock` (`BroadcastClockOptions`) | `BroadcastClock(now_us=None, origin_us=None, origin_epoch_us=None)` |
| The origin on the source clock | `clock.originUs` | `clock.origin_us` |
| The origin as Unix-epoch microseconds | `clock.originEpochUs` | `clock.origin_epoch_us` |
| The timeline now: Unix-epoch microseconds | `clock.now()` | `clock.now_us()` |
| Map a time on the clock's own source onto the timeline | `clock.fromSourceUs(t)` | `clock.from_source_us(t)` |
| Map a clock with another origin and rate | `clock.sourceTimeline({ toleranceUs, captureLatencyUs })` → `SourceTimeline` (`SourceTimelineOptions`) | `clock.source_timeline(tolerance_us=5_000, capture_latency_us=0)` → `SourceTimeline` |
| A device's input delay, subtracted from each timestamp | `timeline.captureLatencyUs` (settable) | `timeline.capture_latency_us` (settable) |
| A measured (source time, clock time) pair | `timeline.calibrate(s, atUs?)` → re-anchored | `timeline.calibrate(s, at_us=None)` → re-anchored |
| Source time to timestamp; current anchor | `timeline.timestampUs(s)`; `timeline.anchorSourceUs` | `timeline.timestamp_us(s)`; `timeline.anchor_source_us` |
| Anchor a sample counter | `clock.sampleTimeline(rate, { firstSampleUs })` → `SampleTimeline` (`SampleTimelineOptions`) | `clock.sample_timeline(rate, first_sample_us=None)` → `SampleTimeline` |
| Sample index to timestamp | `timeline.timestampUs(n)` | `timeline.timestamp_us(n)` |
| The player's clock | `createPlayoutClock(options?)` → `PlayoutClock` (`PlayoutClockOptions`) | `PlayoutClock(now_us=None, late_drop_us=40_000, max_video_delay_us=1_000_000, audio_timeout_us=500_000, target_latency_us=None)` |
| Audio with this timestamp is audible | `reportAudio(timestampUs, atUs?)` | `report_audio(timestamp_us, at_us=None)` |
| Audio muted or ended | `stopAudio()` | `stop_audio()` |
| When to show a video frame | `due(timestampUs)` → `PlayoutDecision` (`PlayoutWait` / `PlayoutShow` / `PlayoutDrop`) | `due(timestamp_us)` → `PlayoutDecision(action, offset_us, wait_us)` |
| Decision kinds | `action: 'wait' \| 'show' \| 'drop'` | `PlayoutAction`: `"wait"`, `"show"`, `"drop"` |
| What paces the clock | `mode`: `PlayoutMode` (`'idle'`, `'audio'`, `'wall'`) | `mode`: `PlayoutMode` |
| Position now / forget anchors | `position()`, `reset()` | `position_us()`, `reset()` |
| The broadcast's latency budget: the option, or twice the longest of the last four keyframe intervals | `targetLatencyUs` (option and property) | `target_latency_us` (option and property) |
| Report the interval between consecutive keyframes | `reportKeyframeInterval(intervalUs)` | `report_keyframe_interval(interval_us)` |
| The late-drop threshold, read back | `lateDropUs` | `late_drop_us` |
| How many times the clock was reset, so a frame reader sees the timeline change | `resets` | `resets` |
| The clock's local time source now, which a frame reader measures its waits in | `localNow()` | `local_now_us()` |
| Injected time source | `now: MicrosecondClock` | `now_us: Callable[[], int]` |

"Playout", not "playback": the clock decides when to present, and does not play anything. Python's
`now_us()` and `position_us()` carry the unit because there is no type to say it; TypeScript's
`now()` and `position()` follow `performance.now()`. The issue's `timeline: "shared"` catalog field
was not added. msf-01 defines no such field, so the builder uses msf-01's own `renderGroup`
instead.

## Media Timeline track

msf-01 §7's Media Timeline track, which maps a broadcast's media time to wall-clock time
([media-layer.md](media-layer.md#media-timeline-track),
[issue #291](https://github.com/muxinc/relay-sdks/issues/291)). Exported from `@mux/moq-media` and
`moq_media`.

| Concept | TypeScript | Python |
| --- | --- | --- |
| One record: media time, location, wall-clock time | `MediaTimelineEntry { mediaTimeMs, group, object, wallclockMs }` | `MediaTimelineEntry(media_time_ms, group, object, wallclock_ms)` |
| The record for a timeline timestamp | `mediaTimelineEntry({ timestampUs, group, object?, wallclockUs? })` (`MediaTimelineEntryInput`) | `media_timeline_entry(timestamp_us, group, *, object=0, wallclock_us=None)` |
| Build a timeline object's bytes | `buildMediaTimeline(entries)` | `build_media_timeline(entries)` |
| Read a timeline object's payload | `parseMediaTimeline(payload)` | `parse_media_timeline(payload)` |
| Announce the track in the catalog | `MediaTimelineTrackInfo { name?, depends? }`, `MsfCatalogInput.mediaTimeline` | `MediaTimelineTrackInfo(name=, depends=)`, `build_msf_catalog(media_timeline=)` |
| Catalog fields as read | `MsfCatalogTrack.depends`, `.mimetype` | `MsfCatalogTrack.depends`, `.mimetype` |
| Default track name | `MEDIA_TIMELINE_TRACK` (`timeline`) | `MEDIA_TIMELINE_TRACK` |
| One broadcast's retained entries | `createMediaTimeline({ maxEntries? })` (`MediaTimelineOptions`) → `MediaTimeline` | `MediaTimeline(max_entries=512)` |
| Record a sent group's first object; the new document | `timeline.add({ timestampUs, group, object?, wallclockUs? })` → bytes | `timeline.add(timestamp_us, group, object=0, wallclock_us=None)` → bytes |
| Retained entries; their document | `timeline.entries`; `timeline.document` | `timeline.entries`; `timeline.document` |

"Wallclock" is msf-01's spelling for Unix-epoch time and appears only in the entry field that
carries it. The clock's property says `Epoch`, because `CONTEXT.md` keeps "wall clock" for the local
monotonic source. `depends` keeps msf-01's JSON name rather than Video.js's `dependencies`, like
the reader's other fields. `MediaTimeline` is the [glossary](../CONTEXT.md)'s media timeline:
"timeline" alone also names the clock's source and sample timelines. Its `add` takes what
`mediaTimelineEntry` takes, and it has no `clear()`: a supervised publisher's
next generation is a new broadcast with a new timeline
([issue #292](https://github.com/muxinc/relay-sdks/issues/292)). Not added: helpers for the §7.4
template, incremental documents, and a reader for MSF_COMPRESSION payloads.

## Media subscribing: `FrameSequencer`, `FrameReader`

Reading a track as frames in decode order within the playout clock's target latency
([issue #303](https://github.com/muxinc/relay-sdks/issues/303)); its contract is the "Media
subscribing" section of [behavior.md](behavior.md#media-subscribing). The sequencer is format-aware
and lives in `@mux/moq-media` and `moq_media`; the reader is transport glue and lives in
`@mux/relay/media` and `mux_relay.media`, which do not depend on the format packages, so it takes the
sequencer as an argument.

| Concept | TypeScript | Python |
| --- | --- | --- |
| One track's ordering and deadlines | `createFrameSequencer({ clock, grouping?, trackProperties? })` → `FrameSequencer` (`FrameSequencerOptions`) | `FrameSequencer(clock, grouping="keyframe", track_properties=())` |
| Feed it a subscription's events | `object(o)`, `groupEnded(id)`, `groupReset(id)`, `restart()`, `trackProperties` | `object(o)`, `group_ended(id)`, `group_reset(id)`, `restart()`, `track_properties` |
| Take the ready frames and the next wait | `poll(maxFrames?)` → `SequencerPoll { frames, waitUs }` | `poll(max_frames=None)` → `SequencerPoll(frames, wait_us)` |
| One frame in decode order | `SequencedFrame { groupId, objectId, timestampUs, key, payload, properties, discontinuity?, missingGroups? }` | `SequencedFrame(group_id, object_id, timestamp_us, key, payload, properties, discontinuity)` |
| Groups of which nothing was delivered before this frame | `missingGroups`: `MissingGroups { firstGroupId, lastGroupId }`, inclusive | not yet (TypeScript only) |
| One track's delivery policy | `lateDropUs`, `reorderWindowUs`, `catchUpAfterUs` on `FrameSequencerOptions` | not yet (TypeScript only) |
| The sequencer asks for its subscription to be replaced | `SequencerPoll.catchUp`; the reader's `onCatchUp()` on `FrameReaderOptions` | not yet (TypeScript only) |
| Why a decoder resets before this frame | `FrameDiscontinuity`: `'gap'`, `'resync'`, `'restart'` | `FrameDiscontinuity`: `"gap"`, `"resync"`, `"restart"` |
| Counters | `stats`: `FrameSequencerStats { delivered, late, skippedGroups, resyncs, untimed }` | `stats`: `FrameSequencerStats(delivered, late, skipped_groups, resyncs, untimed)` |
| A subscription's or a want's handlers that read frames | `createFrameReader(sequencer, { onFrame })` → `FrameReader` (`FrameReaderOptions`); spread into `subscribe` or `want` handlers | `FrameReader(sequencer)`; `subscription_handlers(on_error=, on_publisher_done=)`, `want_handlers(...)`; `async for frame in reader` |
| What a reader drives | `FrameReaderSequencer<F>` | `FrameReaderSequencer`, `FrameReaderPoll` (protocols) |
| Stop reading | `reader.close()` | `reader.close()`, `reader.closed` |

"Sequencer", not "reorderer" or "jitter buffer": it puts frames in decode order and decides what is
too late, but holds nothing for smoothing, which the audio player's jitter buffer does. "Reader"
pairs with the frame writer. "Target latency" follows msf-01's catalog field in name only: msf-01
§5.2.8 measures end-to-end wall-clock latency, while this budget is how far behind the playout
position a frame may still be delivered, so the catalog field is not read. Not adopted from
[issue #303](https://github.com/muxinc/relay-sdks/issues/303): `targetLatencyMs` (every clock option
is in microseconds); the reader taking the clock and grouping itself (the product package cannot
import the format package).

"Catch-up", not "skip", "flush" or "resync": a resync re-anchors the clock on late frames and keeps
the delay, while a catch-up replaces the subscription so the delay goes away. It is a want's restart
(CONTEXT.md) the application asks for, so it reports through `onRestarted`. "Reorder window", not
"jitter buffer" (the audio path's smoothing) or "deadline" (one frame's): it is how long newer frames
wait for an earlier group. `lateDropUs` on the sequencer keeps the clock option's name because it is
the same limit, for one track. `missingGroups` names groups, not frames, because a group ID is what
a relay can be asked about.

## Audio playout: `createAudioPlayer`, `AudioPlayerInput`

A headless player for decoded audio: WebCodecs decoding, an adaptive jitter buffer in an
`AudioWorklet`, a mix, and reports to the playout clock
([issue #314](https://github.com/muxinc/relay-sdks/issues/314)); its design is in
[media-layer.md](media-layer.md#audio-player). TypeScript only, in `@mux/moq-media`: a Python bot has
no output device. It takes frames, not a subscription, so a frame reader's `onFrame` feeds it and
the format package still does not depend on the product package.

| Concept | TypeScript | Python |
| --- | --- | --- |
| One player per `AudioContext` | `await createAudioPlayer({ context })` → `AudioPlayer` (`AudioPlayerOptions`) | — |
| The mix, which the application routes | `player.output` (the context's own gain node type) | — |
| Delay after the output that the context does not report | `player.outputDelayUs` (settable) | — |
| One track's playback, usually one speaker | `player.addInput({ clock, decoderConfig, gain, playbackRate, jitterTargetUs, minJitterTargetUs, maxJitterTargetUs, onError })` → `AudioPlayerInput` (`AudioInputOptions`); `player.inputs` | — |
| What an input calls on its clock; a `PlayoutClock` qualifies | `AudioInputClock { targetLatencyUs, localNow(), reportAudio(timestampUs, atUs, stats), stopAudio() }` | — |
| Hand it a frame in decode order | `input.push(frame)` (`AudioPlayerFrame`: `timestampUs`, `payload`, `discontinuity?`; a `SequencedFrame` qualifies) | — |
| Drop received audio in a timeline range | `input.cut({ fromUs?, untilUs? })` (`AudioCutRange`, which a parsed `Interrupt` is); `cut()` drops everything held | — |
| Per-input gain; stop | `input.gain`; `input.remove()`, `input.removed`; `player.close()` | — |
| Held audio played per output frame, 0.5 to 2 | `input.playbackRate` (settable) | — |
| Counters and buffer state | `input.stats`: `AudioInputStats { decoded, late, decoderResets, errors, underruns, playing, playStarts, heldUntilUs, dryAtUs, bufferedUs, jitterTargetUs, trimmedUs, concealedUs, cutUs, reports }` | — |
| What the player needs of Web Audio | `AudioPlayerContext`, `AudioPlayerNode`, `AudioPlayerGainNode` | — |
| A catalog audio track's decoder config | `audioDecoderConfigFromTrack(track)` | — |
| An Opus packet's duration from its TOC byte | `opusPacketDurationUs(packet)` | `opus_packet_duration_us(packet)` (`moq_media`; see below) |
| The target latency for a broadcast with no video | `AUDIO_ONLY_TARGET_LATENCY_US` (500,000) | `AUDIO_ONLY_TARGET_LATENCY_US` (500,000; `moq_media`), passed as `PlayoutClock(target_latency_us=...)` |
| Play a node where Chromium's echo canceller hears it | `await createEchoCancellationLoopback(context, node, { sinkId, timeoutMs })` → `EchoCancellationLoopback { setSinkId, close }` (`EchoCancellationContext`, `EchoCancellationLoopbackOptions`) | — |
| When a context renders, on `performance.now()`'s clock | `createRenderClock({ windowUs })` → `RenderClock { observe, best, renderedAtUs }` (`RenderClockOptions`, `RenderObservation`) | — |

"Input", not the issue's "source": `composeMoqSource`, `MoqTrackSource` and the clock's source
timeline already use "source", and a mixer's inputs are what this is. "Jitter buffer" is the
[glossary](../CONTEXT.md)'s name for what the audio path holds for smoothing, which the frame
reader does not; its options and counters say `jitterTarget` so they do not read as the clock's
target latency. "Cut", not "flush" or "interrupt": it names a timeline range dropped
([issue #320](https://github.com/muxinc/relay-sdks/issues/320)'s interruption is one use), and
`flush` reads as WebCodecs' "finish pending work". "Loopback" is the word
[media-layer.md](media-layer.md#playout-clock-player) already uses for the WebRTC path. The render
clock moved here from the WebCodecs example, which still re-exports it. Not adopted from
issue #314: `addSource(frameReader, …)` (the format package cannot take a frame reader), a player
method for the loopback (it is a function over any node, so the v10 renderer can use it too), and a
`createPlayoutClock({ audioOnly: true })` preset or an automatic audio-only default (the clock is
pinned by shared vectors in both SDKs; the constant changes nothing for Python).

`playbackRate` is Web Audio's and `HTMLMediaElement`'s name for the same ratio
([issue #329](https://github.com/muxinc/relay-sdks/issues/329)); the Video.js renderer applied its
latency nudges to `AudioBufferSourceNode.playbackRate` before it used the player. `AudioInputClock`
names the members the input calls, rather than `Pick<PlayoutClock, …>`, because its `reportAudio`
takes a third argument that `PlayoutClock` does not: the input's stats, refreshed with the report.
Issue #329 asked for `{ playStarts }` there; the whole stats object also gives a clock `dryAtUs` and
`heldUntilUs` as of that report without a reference to the input, which a clock created before
`addInput` does not have. `PlayoutClock` stays as it is, pinned by the shared vectors, and still
qualifies, since a function that ignores an argument is assignable. `playStarts`, not `reports`
bumped per start: `reports` counts reports, and two meanings in one counter would make both harder
to read. `dryAtUs` and `heldUntilUs` are timeline timestamps, so they end in `Us` like `fromUs` and
`untilUs`.

## Interrupts: `INTERRUPT_TRACK`, `buildInterrupt`, `parseInterrupt`

The talker's side of a cut ([issue #320](https://github.com/muxinc/relay-sdks/issues/320)): one
message on a data track that tells listeners to drop audio they already hold. Its design is in
[media-layer.md](media-layer.md#interrupt-track). Both format packages.

| Concept | TypeScript | Python |
| --- | --- | --- |
| The interrupt track's default name | `INTERRUPT_TRACK` (`'interrupt'`) | `INTERRUPT_TRACK` (`"interrupt"`) |
| One interrupt | `Interrupt { track, fromUs?, untilUs }`, an `AudioCutRange` as it is | `Interrupt(track=, until_us=, from_us=None)` |
| Build an interrupt object's payload | `buildInterrupt({ track?, fromUs?, untilUs })` (`InterruptInput`) | `build_interrupt(*, until_us, from_us=None, track=AUDIO_TRACK)` |
| Read one | `parseInterrupt(payload)` | `parse_interrupt(payload)` |
| List the track in the WebCodecs example's catalog | `publishCanvas(..., { data: [{ name: INTERRUPT_TRACK }] })` (example code) | — |
| Hear about an applied interrupt in the example's listener | `listenAudio(..., { onInterrupt })` (example code) | — |
| Publish a microphone with no video, as a voice participant | `publishMicrophone(publisher, namespace, { signal, microphone, data })` (example code) | — |
| Talk to a bot: publish the microphone, play the bot, cut on its interrupts | `talkToBot(publisher, subscriber, { userNamespace, botNamespace, context, stream, onInterrupt })` (example code) | — |
| A bot that answers after a pause and interrupts itself when talked over | — | `run_bot(publisher, subscriber, user_namespace=, bot_namespace=, respond=, gate=)`, with `Respond`, `Utterance` and `EnergyGate` (example code) |

"Interrupt" is the talker's act, as the [glossary](../CONTEXT.md)'s entry for a cut already said;
the listener's response stays `cut`. It is not the frame writer's "interruption" (a failure that
resets a group) or its `'interrupted'` outcome, which concern a publisher's own writes. Not adopted:
"flush" (issue #320's word, which reads as WebCodecs' "finish pending work" and as the player's
drop-everything), "barge-in" (one cause, the user talking over a bot, not the message), and a
`seq` field (an interrupt only drops audio before its `untilUs`, so order does not matter; see
media-layer.md). The JSON fields are `fromUs` and `untilUs` in both languages, the names
`AudioCutRange` already uses, so a TypeScript listener passes a parsed interrupt to `cut` unchanged.
No player method takes interrupts: an input does not know its track's name, and the one line that
matches them belongs to the listener.

## Transcripts: `TRANSCRIPT_TRACK`, `buildTranscriptSegment`, `Transcript`

The text of a talker's audio, placed on its timeline
([issue #320](https://github.com/muxinc/relay-sdks/issues/320)). Its design is in
[media-layer.md](media-layer.md#transcript-track). Both format packages.

| Concept | TypeScript | Python |
| --- | --- | --- |
| The transcript track's default name | `TRANSCRIPT_TRACK` (`'transcript'`) | `TRANSCRIPT_TRACK` (`"transcript"`) |
| One version of a segment | `TranscriptSegment { seq, track, fromUs, untilUs?, text, final }` | `TranscriptSegment(seq=, track=, from_us=, until_us=None, text=, final=False)` |
| Build a segment object's payload | `buildTranscriptSegment({ seq, track?, fromUs, untilUs?, text, final? })` (`TranscriptSegmentInput`) | `build_transcript_segment(*, seq, from_us, text, until_us=None, final=False, track=AUDIO_TRACK)` |
| Read one | `parseTranscriptSegment(payload)` | `parse_transcript_segment(payload)` |
| A listener's transcript of one audio track | `createTranscript()` → `Transcript` | `Transcript()` |
| Keep a segment's version | `transcript.apply(segment)` → `boolean` | `transcript.apply(segment)` → `bool` |
| Retract what an interrupt cut, at the playout position | `transcript.cut(interrupt, clock.position())` (`TranscriptCutRange`) | `transcript.cut(until_us=, from_us=None, position_us=clock.position_us)` |
| What has been said by a position; everything held | `transcript.linesAt(positionUs)`; `transcript.lines` → `TranscriptLine[]` | `transcript.lines_at(position_us)`; `transcript.lines` → `list[TranscriptLine]` |
| A segment as held, and whether an interrupt cut it | `TranscriptLine` (a `TranscriptSegment` with `cut`) | `TranscriptLine(..., cut=)` |
| Keep the bot's transcript in the example's listener | `listenAudio(..., { onTranscript })`, `listener.transcript` (example code) | — |
| Give a reply's text to the example bot | — | `Respond` yields `str` before the audio it is the text of (example code) |

"Segment", as captioning and STT services name a timed piece of text, and "final", the issue's word
and theirs, for the last version. "Line" is a segment as a listener holds and shows it. "Cut", the
audio player's word, for what an interrupt does to it. `seq` names a segment, so its versions find
each other; it is not an order across segments, which the timeline gives. Not adopted: "caption"
(a rendering, and WebVTT's word), "utterance" (the example bot's word for what the user said),
`isFinal` (the wire's other fields have no `is`), a separate retraction message (the interrupt
already says what was cut), and a player method (the transcript is text, not audio).

## Opus encoding: `OpusEncoder`, `OpusDecoder`

PCM to Opus and back for a server-side talker such as a voice bot
([issue #320](https://github.com/muxinc/relay-sdks/issues/320)). Python only, in `moq_media.opus`,
on PyAV's libopus and installed with the `opus` extra (`mux-moq-media[opus]`, [ADR 0006](../docs/adr/0006-opus-codec-behind-an-extra.md)).
A browser has WebCodecs' `AudioEncoder` and `AudioDecoder`, which the TypeScript helpers already
take.

| Concept | TypeScript | Python |
| --- | --- | --- |
| Encode s16 PCM, or a PyAV frame, into 20 ms packets on the broadcast timeline | — (WebCodecs `AudioEncoder`) | `OpusEncoder(input_rate=48_000, input_channels=1, channels=1, bitrate=32_000, application="voip")`; `encoder.encode(pcm, timestamp_us=...)` → `list[OpusPacket]` |
| libopus tuning | — | `OpusApplication` (`"voip"`, `"audio"`, `"lowdelay"`) |
| End a run of audio, such as an utterance | — | `encoder.flush()` → `list[OpusPacket]` |
| Where the next sample lands, or nothing between runs | — | `encoder.next_timestamp_us` |
| One encoded packet, which a frame writer takes as a frame | — (WebCodecs `EncodedAudioChunk`) | `OpusPacket(timestamp_us, payload, duration_us)`, with `key`, `discontinuity` and LOC `properties` |
| Decode packets into s16 PCM at a chosen rate | — (WebCodecs `AudioDecoder`) | `OpusDecoder(rate=48_000, channels=1)`; `decoder.decode(payload)` → `bytes`; `decoder.reset()` |
| An Opus packet's duration from its TOC byte | `opusPacketDurationUs(packet)` | `opus_packet_duration_us(packet)` (`moq_media`, no PyAV) |
| Opus's decode rate; the encoder's frame length | — | `OPUS_SAMPLE_RATE` (48,000), `OPUS_FRAME_US` (20,000) (`moq_media`) |

"Run", not "utterance" or "segment": the encoder knows only contiguous audio, and a bot's utterance
is one use. A run's first `encode` takes the timeline timestamp and later calls continue by sample
count, because a timestamp per call from a clock that drifts against the sample rate would gap or
overlap packets. `flush` follows PyAV's and libavcodec's drain, which is what it does to the
encoder; it is not the audio player's `cut`, which drops what a listener holds. "Packet" is RFC
6716's word for what one Opus encode produces, and keeps clear of the frame writer's `Frame`.
The decoder returns bytes and no timestamps: the caller has each frame's timestamp, and
a resampler's output length varies from packet to packet.

## Media publishing: `FrameWriter`, `CatalogService`, `mediaTrackProperties`

The media publishing policy the Video.js example proved, lifted into each product package
([issue #84](https://github.com/muxinc/relay-sdks/issues/84) for Python as `mux_relay.media`,
[issue #263](https://github.com/muxinc/relay-sdks/issues/263) for TypeScript as the
`@mux/relay/media` subpath); its contract is the "Media publishing" section of
[behavior.md](behavior.md#media-publishing). Neither root package exports these names; the
`media` exports are pinned by `python/packages/mux-relay/tests/test_exports.py` and by the
TypeScript package's export test. TypeScript uses factories that return interfaces, like
`@mux/moq-media`; Python uses classes.

| Concept | TypeScript | Python |
| --- | --- | --- |
| Serve a state-shaped track: send its document on every relay subscription | `createCatalogService(track, document, { retryDelayMs })` → `CatalogService` (`CatalogServiceOptions`; default 1000) | `CatalogService(track, document, retry_delay=1.0)` |
| Run the service until cancelled or the track ends | `await service.serve({ signal })` (`ServeOptions`) | `await service.serve()` |
| Documents accepted, and failures that were retried | `service.sends`, `service.failures` | `service.sends`, `service.failures` |
| Serve a state-shaped track whose document the application replaces | `createDocumentService(track, document?, { retryDelayMs })` → `DocumentService` (`DocumentServiceOptions`); `serve`, `sends`, `failures` as above | `DocumentService(track, document=None, retry_delay=1.0)`; `serve()`, `sends`, `failures` as above |
| Replace the document and send it while the relay is subscribed; the latest | `service.update(document)`; `service.document` | `service.update(document)`; `service.document` |
| Map encoded frames onto a track's groups | `createFrameWriter(track, { grouping, stale, maxBacklog, onKeyframeNeeded })` → `FrameWriter` (`FrameWriterOptions`); `grouping: 'frame'` for one group per frame (`FrameGrouping`) | `FrameWriter(track, grouping="keyframe", stale=None, max_backlog=None, on_keyframe_needed=None)`; `grouping="frame"` for one group per frame |
| Send one frame if the relay wants it | `writer.write(frame)` → `Promise<FrameOutcome>`, callable without awaiting | `await writer.write(frame)` returns a `FrameOutcome`; concurrent calls are served in order |
| What became of a frame | `FrameOutcome { kind, error?, groupId? }`, `kind` a `FrameOutcomeKind`: `'sent'`, `'skipped'`, `'stale'`, `'dropped'`, `'interrupted'`, `'ended'` | `FrameOutcome` with `kind` (a `FrameOutcomeKind`: `sent`, `skipped`, `stale`, `dropped`, `interrupted`, `ended`), `error`, `group_id` |
| One encoded frame as the writer sees it | `Frame { payload, key, properties?, discontinuity? }`; `@mux/moq-media`'s `LocFrame` qualifies | `Frame`, a protocol: `payload`, `key`, `properties`, `discontinuity` |
| Frames waiting behind the one being written; their bound | `writer.backlog`; `maxBacklog` (default 8, or 25 under frame grouping) | `writer.backlog`; `max_backlog` (same defaults) |
| The next frame sent must be a keyframe; told on each entry | `writer.keyframeNeeded`; `onKeyframeNeeded()` | `writer.keyframe_needed`; `on_keyframe_needed()` |
| FIN the open group and refuse further frames | `await writer.close()` | `await writer.close()` |
| A media track's default delivery timeout and publisher priority, as track properties | `mediaTrackProperties(kind, { keyframeIntervalUs, targetLatencyUs, subgroupTimeoutUs, publisherPriority })` (`MediaKind`: `'audio'`, `'video'`; `MediaTrackPropertiesOptions`) | `media_track_properties(kind, *, keyframe_interval_us=None, target_latency_us=None, subgroup_timeout_us=None, publisher_priority=None)` (`MediaKind`) |
| The writer ended, and how | `writer.closed`, `writer.ended` | `writer.closed`, `writer.ended` |
| Groups opened and objects accepted | `writer.groups`, `writer.objects` | `writer.groups`, `writer.objects` |

`FrameWriter.write` keeps the SDK's one verb for sending bytes, one tier above
`RelayTrackWriter.write`: it sends when the relay wants the frame, and its return value is what
says this write can decline (skip, drop, report) instead of waiting for demand or raising. The
failures it reports are data because an encoder loop keeps producing after any of them; catching an
exception per frame would be the caller's whole loop.

`Frame` is a protocol, not a class. The application's own encoded-sample type qualifies once it
carries the frame's object properties, so nothing is converted between the encoder and the writer.

`dropped` is not `stale`: a drop says the transport fell behind, while staleness is the application's
own clock. "Backlog", not "queue": the writer queues nothing it would rather send late, and its
bound is what it drops at. "Keyframe needed", not "keyframe request": NEW_GROUP_REQUEST is the
relay's request, which the SDKs do not send.

Not adopted from [issue #263](https://github.com/muxinc/relay-sdks/issues/263): `createMediaSink`
and `push()` (the frame writer already is that concept, and `write` is the SDK's one verb for
sending); `sink.hasDemand` (the track has it, and its `demandChanges()`); `sink.attach(writer)` (a
supervised publisher's writers outlive its generations, and a move to a new one is a demand change);
`maxQueue` (the bound is the backlog's).

`mediaTrackProperties` says what it returns, like `locTrackProperties`, and what it is for: the
media tier's part of a media track's properties. "Target latency" is the reader's name for the same
budget ([#303](https://github.com/muxinc/relay-sdks/issues/303)), so one setting reads the same on
both sides. Not adopted: a `timescale` option returning the LOC Timescale too (the product package
does not depend on `@mux/moq-media`, so the caller adds `locTrackProperties()`); an option on
`registerTrack` (it knows nothing about media); an option on the frame writer (it receives a track
already registered, and an object-level `0x06` reaches viewers as a Timestamp); `deliveryTimeoutMs`
(media durations are microseconds, and the budget is what the application knows); an object
timeout (see behavior.md).

`publisherPriority` is draft-22's name for what `0x0E` carries (Default Publisher Priority), so it
reads as the protocol value it sets, not as a scheduling promise: on moq-relay 0.17 only the
viewer's subscriber priority orders delivery ([#316](https://github.com/muxinc/relay-sdks/issues/316)).

Not adopted: `FramePublisher.publish()` ("publisher" is the connection role, and `publish()` was
already rejected as the name of a step that does not send); `MediaTrackWriter.write_frame()`
(redundant on a type named for frames); an events iterator for the catalog service (two counters
answer what the example's `finished` line asks); exceptions for interrupted and ended frames (see
above); `update()` on `CatalogService` (its document is fixed, and the media timeline, which
changes, gets `DocumentService` instead of a mode). The `register_media` handle in
[media-layer.md](media-layer.md) remains a proposal.
