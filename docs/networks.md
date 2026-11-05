# Unreliable networks

This page covers what Relay and its SDKs do about loss, delay, and too little bandwidth, and what
your application has to do itself. The transport resends lost data. Each group of frames travels on
its own stream, so one loss holds up only that group. The SDKs report congestion and can switch a
viewer between renditions. They do not estimate bandwidth or change your encoder's bitrate, and
nothing adds packet-level forward error correction (FEC).

Names below are the TypeScript SDK's. [Vocabulary](../spec/vocabulary.md) gives the Python name of
each, and says which are TypeScript only.

| Capability | Status |
| --- | --- |
| [Retransmission](#retransmission) | Yes, by QUIC per stream, or by TCP over WebSocket. |
| [Bounded lateness](#lateness-drop-instead-of-stall) | Yes. The viewer drops what is too late, and the publisher can give up on old groups. |
| [Prioritization](#prioritization) | Yes. Audio goes ahead of video by default, except in some browsers and relay releases. |
| [Forward error correction](#forward-error-correction) | Opus in-band FEC only, decoded by the TypeScript audio player. No packet-level FEC. |
| [Loss concealment and jitter buffering](#audio-concealment-and-jitter-buffer) | Yes, in the TypeScript audio player. |
| [Encoding adaptation](#encoding-adaptation) | Backlog and keyframe signals, renditions in the catalog, and track switching without a gap. No built-in bandwidth estimate or ABR policy. |
| [Connection loss](#connection-loss) | Supervised connections reconnect and resubscribe, but do not replay what was lost. An opt-in WebSocket fallback in TypeScript covers networks without UDP. |

The measured figures below come from emulated paths and from production relays, and each links to
its measurements. The relay is [moq-relay](https://github.com/moq-dev/moq). Where its behavior
matters, this page names the release.

## Retransmission

MOQT runs over QUIC. Browsers use WebTransport, and Node and Python use native QUIC. QUIC detects
lost packets, resends their data, and runs congestion control. A web page cannot configure either,
because the browser's QUIC stack owns the connection
([browser transport limits](../ts/GUIDE.md#browser-transport-limits)).

Resending does not guarantee delivery. When a [delivery timeout](#lateness-drop-instead-of-stall)
resets a stream, or the connection drops, the unsent data is gone. A write that completed can still
be lost without an error. The publisher keeps nothing to replay, and a reconnect restores
subscriptions, not missing objects ([publishing](../spec/behavior.md#publishing)). Where every
message matters, number the messages and have the receiver reply
([control and telemetry](../ts/GUIDE.md#control-and-telemetry)).

Each group travels on its own QUIC stream. A packet lost from one group's stream holds up only that
group. Later groups keep arriving, so a viewer can receive them out of order. The SDK's
[frame sequencer](#lateness-drop-instead-of-stall) puts them back in order or drops the ones that
are too late.

On a network that blocks UDP, the TypeScript SDK can carry MOQT on QMux over a WebSocket instead, if
you opt in with the `@caprid/relay-websocket` package
([networks without UDP](../ts/GUIDE.md#networks-without-udp-qmux-over-websocket)). The Python SDK has
no equivalent. A WebSocket runs over TCP, which also resends, but every track shares one ordered byte
stream, so one lost packet holds up all of them until TCP resends it. The publisher also cannot see
the relay's acknowledgements over WebSocket, so its delivery timeouts cannot stop TCP from resending
data already handed to the socket.

Nothing uses QUIC datagrams. The transport sends all media reliably, and the SDKs bound lateness by
dropping, as described below.

## Lateness: drop instead of stall

Resending costs delay. A live player that waits for every resent frame falls further behind the
live edge, so both ends set a limit on how late data may be.

The viewer decides what is too late, and on current relays it is the only side that can. moq-relay
0.17 sends a viewer every group, however late, so a congested downlink builds a backlog at the relay
([measured](../spec/relay-interop.md#voice-delivery-on-0175-deadlines-fetch-and-group-order)). The
SDK's frame sequencer, paced by a playout clock that the broadcast's tracks share, handles the
backlog:

- The sequencer drops a video frame later than the clock's target latency, with the rest of its
  group, and resumes at a later keyframe. It drops an audio frame later than `lateDropUs` (40 ms by
  default), and the player conceals the gap
  ([subscribing to media](../ts/GUIDE.md#subscribing-to-media)).
- When the delay rises and stays up, the clock re-anchors (a `resync`) instead of staying behind.
- In TypeScript, `catchUpAfterUs` with a supervised subscriber's `catchUp()` replaces a subscription
  that keeps delivering late with a new one at the live edge, which drops the relay's backlog.
- For data that is not media, such as telemetry, the viewer drops superseded values itself
  ([latest-wins values](../ts/GUIDE.md#latest-wins-values)).

A relay that supports the MoQ Timestamp extension's max age drops, for each viewer, any group that
a newer group superseded more than the viewer's `maxAge` ago. The SDK sends `maxAge` only to a relay
that declares support, so you can set it now
([subscriber max age](../spec/relay-interop.md#subscriber-max-age-max_age)).

The publisher limits how long its own transport keeps resending a finished group.
`mediaTrackProperties` gives each media track a subgroup delivery timeout. Audio gets 500 ms. Video
gets the target latency less the keyframe interval, and never less than the interval. The timer
starts when the publisher hands a group's end to the transport, and the publisher resets the group if
the relay has not acknowledged it in time. The timeout does not limit a frame's age. An open video
group has no timer until the next keyframe closes it, so its frames can wait out a stall. The
timeout also covers only the publisher's link to the relay
([delivery timeouts](../spec/behavior.md#delivery-timeouts)).

Where every object must arrive, such as a person's speech going to speech-to-text, both ends change.
The publisher raises the audio timeout to 1 to 2 s, so its transport keeps resending through loss.
The receiver drops nothing as late (`lateDropUs: Infinity`) and waits for a missing group at least as
long as that timeout (`reorderWindowUs`), because a resent group can arrive after the groups that
follow it. These sequencer options are TypeScript only
([voice: delivery per direction](../ts/GUIDE.md#voice-delivery-per-direction)).

## Prioritization

When there is not enough bandwidth for everything, audio should go first.

- **Viewer downlink.** The relay orders what it sends each viewer by that viewer's subscriber
  priority. `mediaSubscribeParameters('audio' | 'video')` sets 16 for audio and 96 for video. In a
  live test on a capped downlink, audio's p99 latency fell from 255 ms to 74 ms
  ([#337](https://github.com/muxinc/relay-sdks/issues/337),
  [subscribing to media](../ts/GUIDE.md#subscribing-to-media)).
- **Publisher uplink.** `mediaTrackProperties` gives tracks the same publisher priorities, and the
  TypeScript publisher turns them into stream send order. Firefox 155 and later, Node
  (`@caprid/relay-quic`), and the WebSocket transport follow it. Chromium's WebTransport and the
  Python SDK do not yet ([send order](../spec/behavior.md#send-order)). In Firefox at 800 kbit/s,
  audio p99 latency was 108 ms with send order and 264 to 302 ms without it
  ([#336](https://github.com/muxinc/relay-sdks/issues/336)).
- Priority decides only what goes out next. It does not reorder data already in the network.

## Forward error correction

There is no packet-level FEC, meaning no redundant packets that rebuild lost ones. QUIC recovers
loss by resending, which costs a round trip.

For voice, the browser supports Opus in-band FEC end to end. With FEC on, the encoder can put a
low-bitrate copy of a frame (LBRR) into the next packet. It decides packet by packet, and packets in
CELT mode carry no copy. The TypeScript audio player decodes Opus with libopus compiled to
WebAssembly. When the packet after a loss carries a copy, the player rebuilds the lost frame from it
at once. The player drops rebuilt audio that would arrive after its buffer has run dry, because
playing it would only add delay. To use FEC, turn it on in the publisher's encoder and set an
expected loss above 0. With WebCodecs:

```ts
encoder.configure({ codec: 'opus', sampleRate: 48_000, numberOfChannels: 1, opus: { useinbandfec: true, packetlossperc: 10 } });
```

At 2% random loss, measured offline, audible drops fell from about 56 a minute to 16 with
concealment alone, and to 3 with in-band FEC ([audio player](../spec/media-layer.md#audio-player)).
The Python SDK's `OpusEncoder` has no FEC option yet. Video has no FEC. The transport resends a lost
video group, or the viewer drops it as late and resumes at the next keyframe.

## Audio concealment and jitter buffer

The TypeScript audio player (`createAudioPlayer`, [playing audio](../ts/GUIDE.md#playing-audio))
fills what is missing instead of playing silence.

- **Loss concealment.** libopus's packet loss concealment synthesizes a gap of up to 120 ms that FEC
  cannot rebuild.
- **Adaptive jitter buffer.** The buffer starts at 60 ms, grows by 40 ms after each underrun, and
  shrinks by 10 ms after each 2 s of smooth playback. `minJitterTargetUs` raises its floor. On a
  voice benchmark, an 80 ms floor added about 50 ms of delay and cut underruns by 60 to 90%.
- **Trimming.** The player trims audio the buffer held but never needed, so a burst after a stall
  does not leave a lasting delay. `input.playbackRate`, from 0.5 to 2, lets an application adjust
  latency itself.

Its `stats` report how much audio the player rebuilt (`recoveredUs`) and synthesized
(`synthesizedUs`), and how often the buffer underran.

## Encoding adaptation

The SDKs do not measure bandwidth or change an encoder's settings, because your application owns the
encoder. They provide signals and track switching.

On the publisher, a frame writer (`createFrameWriter`) reports congestion.

- When the transport falls behind, frames wait behind the one being written. Past `maxBacklog` (8
  frames, or 25 for one-frame groups), the writer drops frames rather than queue seconds of video.
  Each `write()` resolves to what happened to the frame (`'sent'`, `'dropped'`, `'stale'`, and
  others), and `writer.backlog` counts the frames waiting.
- After a drop, a failure, or a change in demand, `onKeyframeNeeded` asks for a keyframe, so viewers
  resume at a keyframe ([publishing lifecycle](../ts/GUIDE.md#publishing-lifecycle)).
- The writer skips frames that no viewer has asked for, and `track.hasDemand` tells you when to pause
  the encoder because nobody is watching.

A rising backlog or repeated drops means you should lower the encoder's bitrate, frame rate, or
resolution. A clear backlog does not prove the network has recovered, though. It counts only frames
waiting in the SDK, not data in the transport's buffers or the viewers' bandwidth. In Chromium,
writes completed in under 1 ms through a 2 s blackout, and a group that its delivery timeout resets
later reports nothing to the writer ([media publishing](../spec/behavior.md#media-publishing)). Raise
quality in small steps, and use the viewer-side signals below to choose each viewer's rendition.

For viewers, publish several renditions as separate tracks (`video-hi`, `video-lo`), and mark them
in the catalog as alternates of one another with `altGroup`
([catalogs](../ts/packages/moq-media/README.md#catalogs)). Each viewer then chooses its own
rendition. `subscribeSwitchable` with `switchTo` moves a viewer between renditions make-before-break.
It subscribes to the new track while the old one keeps playing, and cuts over on the first object of
the new track that passes your `until` test. Write a test that compares your media timestamps with
what was already shown. The default, the first object, can be a stale cached group or can skip media,
and group IDs do not line up across tracks ([switching tracks](../ts/GUIDE.md#switching-tracks)). A
supervised subscriber's `want()` switches the same way, with the same `until`, when you want a key
again with another track. Your application decides when to switch. The frame sequencer's `stats`
(late frames, skipped groups, resyncs) and the audio player's underruns are signals to switch on.

The relay forwards what it receives and does not transcode. Publish every rendition a viewer may
need.

## Connection loss

- **Supervised connections.** `connectSupervisedPublisher` and `connectSupervisedSubscriber`
  reconnect with backoff, register your tracks again on each new connection, and resubscribe what you
  asked for ([stay connected](../ts/GUIDE.md#stay-connected-supervised-connections)). `staleAfterMs`
  resubscribes a track that went silent.
- **Browser limits.** Chromium 147 closed a QUIC connection that heard nothing for about 5 s with
  data outstanding, even after the network came back. Other browsers are untested
  ([browser transport limits](../ts/GUIDE.md#browser-transport-limits)). A longer outage needs a new
  connection, and a supervised connection opens one for you
  ([publisher recovery](../spec/publisher-recovery.md)).
- **Networks without UDP.** In TypeScript, opt in with `withWebSocketFallback()` from
  `@caprid/relay-websocket`. It starts a WebSocket alongside WebTransport after 250 ms, so a network
  that blocks UDP still connects, with the [costs above](#retransmission). Default connections never
  fall back, and Python has no WebSocket transport.

## Not available yet

- Bandwidth estimation exposed to the application, and a built-in adaptive bitrate policy.
- Packet-level FEC, and media over unreliable datagrams.
- Relay-side dropping of late groups on moq-relay 0.17, which ignores subscriber delivery timeouts
  ([moq-dev/moq#5881](https://github.com/moq-dev/moq/issues/5881)). The viewer drops them instead.
- Send-order prioritization from Chromium and from the Python publisher.
- In Python, an FEC option on `OpusEncoder`, an audio player, the per-track delivery policy
  (`lateDropUs`, `reorderWindowUs`, `catchUp`), and a WebSocket transport.
