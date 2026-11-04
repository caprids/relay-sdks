# Unreliable networks

What Relay and its SDKs do about loss, delay, and too little bandwidth, and what is left to your
application. In short: the transport resends lost data, media is split so that one loss does not hold
up everything behind it, and the SDKs give you the signals and the moving parts for adaptation. They
do not estimate bandwidth or change your encoder's bitrate for you, and nothing adds packet-level
forward error correction (FEC).

Names below are the TypeScript SDK's. [Vocabulary](../spec/vocabulary.md) gives the Python name of
each, and says which are TypeScript only.

| Capability | Status |
| --- | --- |
| [Retransmission](#retransmission) | Yes, by QUIC (or TCP over WebSocket), per stream. |
| [Bounded lateness](#lateness-drop-instead-of-stall) | Yes. The viewer drops what is too late, and the publisher can give up on old groups. |
| [Prioritization](#prioritization) | Yes. Audio ahead of video by default, with gaps by browser and relay release. |
| [Forward error correction](#forward-error-correction) | Opus in-band FEC only, decoded by the TypeScript audio player. No packet-level FEC. |
| [Loss concealment and jitter buffering](#audio-concealment-and-jitter-buffer) | Yes, in the TypeScript audio player. |
| [Encoding adaptation](#encoding-adaptation) | Building blocks: backlog and keyframe signals, renditions in the catalog, make-before-break track switching. No built-in bandwidth estimate or ABR policy. |
| [Connection loss](#connection-loss) | Supervised connections reconnect and resubscribe, but do not replay what was lost. An opt-in WebSocket fallback (TypeScript) covers networks without UDP. |

Measured figures below come from emulated paths and from production relays. Each links to its
measurements. The relay is [moq-relay](https://github.com/moq-dev/moq). Where its behavior matters,
the release is named.

## Retransmission

MOQT runs over QUIC: WebTransport in the browser, native QUIC in Node and Python. QUIC detects lost
packets and resends their data, and runs congestion control. Neither is configurable from a web
page: the browser's QUIC stack owns the connection
([browser transport limits](../ts/GUIDE.md#browser-transport-limits)).

Resending is not a delivery guarantee. A stream reset by a [delivery timeout](#lateness-drop-instead-of-stall)
or a lost connection takes its unsent data with it, and a write that completed can still be lost
without an error. The publisher keeps nothing to replay, and a reconnect restores subscriptions, not
missing objects ([publishing](../spec/behavior.md#publishing)). Where every message matters, number them and have
the receiver reply ([control and telemetry](../ts/GUIDE.md#control-and-telemetry)).

Each group travels on its own QUIC stream. A packet lost from one group's stream holds up only that
group. Later groups keep arriving, so a viewer can see them out of order, and the SDK's
[frame sequencer](#lateness-drop-instead-of-stall) puts them back in order or drops what is too late.

On a network that blocks UDP, the TypeScript SDK can carry MOQT on QMux over a WebSocket instead,
if you opt in with the `@caprid/relay-websocket` package
([networks without UDP](../ts/GUIDE.md#networks-without-udp-qmux-over-websocket)). The Python SDK has
no equivalent. That is TCP: it still resends, but every track shares one ordered byte stream, so one
lost packet holds up all of them until TCP resends it. The relay's acknowledgements are not visible
there either, so the publisher's delivery timeouts cannot bound TCP resending of data already handed
to the socket.

Nothing uses QUIC datagrams. All media is sent reliably, and lateness is handled by dropping, as below,
rather than by unreliable delivery.

## Lateness: drop instead of stall

Resending trades delay for completeness. For live media, data that arrives too late is worse than
data that never arrives, so both ends bound how late they accept.

**The viewer** decides what is too late, and on current relays it is the only side that can.
moq-relay 0.17 sends a viewer every group, however late, so a congested downlink builds a backlog at
the relay ([measured](../spec/relay-interop.md#voice-delivery-on-0175-deadlines-fetch-and-group-order)).
The SDK's frame sequencer, paced by a shared playout clock, handles it:

- Video frames later than the clock's target latency are dropped with their group, and playback
  resumes at a later keyframe. Audio frames later than `lateDropUs` (40 ms by default) are dropped
  and concealed ([subscribing to media](../ts/GUIDE.md#subscribing-to-media)).
- When delay rises for good, the clock re-anchors (a `resync`) instead of staying behind.
- In TypeScript, `catchUpAfterUs` with a supervised subscriber's `catchUp()` replaces a subscription
  that keeps delivering late at the live edge, which drops the relay's backlog.
- For data that is not media, such as telemetry, the viewer drops superseded values itself
  ([latest-wins values](../ts/GUIDE.md#latest-wins-values)).

A relay that supports the MoQ Timestamp extension's max age drops, for each viewer, groups a newer
one has superseded for longer than the viewer's `maxAge`. The SDK sends it only to a relay that
declares it, so it is safe to set now ([subscriber max age](../spec/relay-interop.md#subscriber-max-age-max_age)).

**The publisher** bounds how long its own transport keeps resending a finished group.
`mediaTrackProperties` gives media tracks a subgroup delivery timeout: 500 ms for audio, and for
video the target latency less the keyframe interval, but never less than the interval. The timer
starts when a group's end is handed to the transport, and the publisher then resets a group the
relay has not acknowledged in time. It is not a bound on a frame's age: an open video group has no
timer until the next keyframe closes it, so its frames can wait out a stall. It also bounds only the
publisher's link to the relay ([delivery timeouts](../spec/behavior.md#delivery-timeouts)).

Where every object must arrive, such as a person's speech going to speech-to-text, both ends change.
The publisher raises the audio timeout to 1 to 2 s so its transport keeps resending through loss.
The receiver drops nothing as late (`lateDropUs: Infinity`) and waits for a missing group at least
as long as that timeout (`reorderWindowUs`), since a resent group can arrive after the ones that
follow it. These sequencer options are TypeScript only
([voice: delivery per direction](../ts/GUIDE.md#voice-delivery-per-direction)).

## Prioritization

When there is not enough bandwidth for everything, audio should go first.

- **Viewer downlink.** The relay orders what it sends each viewer by that viewer's subscriber
  priority. `mediaSubscribeParameters('audio' | 'video')` sets 16 for audio and 96 for video. In a
  live test on a capped downlink, audio's p99 latency fell from 255 ms to 74 ms
  ([#337](https://github.com/muxinc/relay-sdks/issues/337), [subscribing to media](../ts/GUIDE.md#subscribing-to-media)).
- **Publisher uplink.** `mediaTrackProperties` gives tracks the same publisher priorities, and the
  TypeScript publisher turns them into stream send order. Firefox 155+, Node (`@caprid/relay-quic`)
  and the WebSocket transport honor it. Chromium's WebTransport and the Python SDK do not yet
  ([send order](../spec/behavior.md#send-order)). In Firefox at 800 kbit/s, audio p99 latency was
  108 ms against 264 to 302 ms without it ([#336](https://github.com/muxinc/relay-sdks/issues/336)).
- Data on a congested link that is already in the network is not reordered. Priority decides only
  what is sent next.

## Forward error correction

There is no packet-level FEC (no redundant packets that rebuild lost ones). QUIC recovers loss by
resending, which costs a round trip.

For voice, Opus in-band FEC is supported end to end in the browser. With it on, the encoder can put a
low-bitrate copy of a frame (LBRR) into the next packet; it decides per packet, and packets coded
in CELT mode carry none. The TypeScript audio player decodes Opus with libopus (WebAssembly) and,
when the packet after a loss carries a copy, rebuilds the lost frame from it without waiting for
anything more. Rebuilt audio that would arrive after the buffer has already run dry is dropped, as
it would only add delay. To use it, turn it on in the publisher's encoder, with an expected loss
above 0. With WebCodecs:

```ts
encoder.configure({ codec: 'opus', sampleRate: 48_000, numberOfChannels: 1, opus: { useinbandfec: true, packetlossperc: 10 } });
```

At 2% random loss, measured offline, audible drops fell from about 56 a minute to 16 with
concealment alone, and to 3 with in-band FEC
([audio player](../spec/media-layer.md#audio-player)). The Python SDK's `OpusEncoder` does not yet expose an
FEC option. Video has no FEC: a lost video group is resent, or dropped as late and recovered at the
next keyframe.

## Audio concealment and jitter buffer

The TypeScript audio player (`createAudioPlayer`, [playing audio](../ts/GUIDE.md#playing-audio))
fills what is missing instead of playing silence:

- **Loss concealment.** A gap of up to 120 ms that FEC cannot rebuild is synthesized by libopus's
  packet loss concealment.
- **Adaptive jitter buffer.** It starts at 60 ms, grows by 40 ms after each underrun, and shrinks
  by 10 ms after each 2 s of smooth playback. `minJitterTargetUs` raises the floor on jittery paths, trading about 50 ms of
  delay for 60 to 90% fewer underruns on a voice benchmark.
- **Trimming.** Audio the buffer held but never needed is trimmed, so a burst after a stall does
  not stay as lasting delay. `input.playbackRate` (0.5 to 2) lets an application nudge latency
  itself.

Its `stats` report what was rebuilt (`recoveredUs`), synthesized (`synthesizedUs`), and how often it
underran, so you can see what a network costs your users.

## Encoding adaptation

The SDKs do not measure bandwidth or change an encoder's settings: your application owns the
encoder. What they provide:

**On the publisher**, a frame writer (`createFrameWriter`) is a congestion signal:

- When the transport falls behind, frames queue behind the one being written. Past `maxBacklog`
  (8 frames, or 25 for one-frame groups) the writer drops frames rather than queue seconds of
  video. Each `write()` tells you what became of the frame (`'sent'`, `'dropped'`, `'stale'`, and so
  on), and `writer.backlog` is how far behind it is.
- After a drop, a failure, or a change in demand, `onKeyframeNeeded` asks for a keyframe, so playback
  resumes cleanly ([publishing lifecycle](../ts/GUIDE.md#publishing-lifecycle)).
- The writer skips frames no viewer has asked for, and `track.hasDemand` lets you pause the encoder
  while nobody watches.

A rising backlog or repeated drops is your cue to lower the encoder's bitrate, frame rate, or
resolution. A clear backlog does not prove the network has recovered, though. It counts only frames
waiting in the SDK, not data in the transport's buffers or the viewers' bandwidth: in Chromium,
writes completed in under 1 ms through a 2 s blackout, and a group reset later by its delivery
timeout reports nothing to the writer ([media publishing](../spec/behavior.md#media-publishing)). Raise quality again
cautiously, and prefer viewer-side signals (below) for the rendition each viewer gets.

**For viewers**, publish several renditions as separate tracks (`video-hi`, `video-lo`) and mark
them in the catalog as alternates of one another with `altGroup`
([catalogs](../ts/packages/moq-media/README.md#catalogs)). Each viewer then chooses its own
rendition, and `subscribeSwitchable` with `switchTo` moves it between them make-before-break: the
new track is subscribed while the old one keeps playing, and it cuts over on the first new-track
object that passes your `until` test. Give it one that compares your media timestamps with what was
already shown: the default, the first object, can be a stale cached group or skip media, and group
IDs do not line up across tracks ([switching tracks](../ts/GUIDE.md#switching-tracks)). A supervised
subscriber's `want()` switches the same way, with the same `until`, when you want a key again with
another track. When to switch is yours. Useful
signals are the frame sequencer's `stats` (late frames, skipped groups, resyncs) and the audio
player's underruns.

The relay forwards what it receives. It does not transcode, so a rendition a viewer needs must be
published.

## Connection loss

- **Supervised connections** (`connectSupervisedPublisher`, `connectSupervisedSubscriber`) reconnect
  with backoff, register your tracks again on each new connection, and resubscribe what you asked for
  ([stay connected](../ts/GUIDE.md#stay-connected-supervised-connections)). `staleAfterMs` resubscribes a
  track that went silent.
- **Browser limits.** Chromium 147 closed a QUIC connection that heard nothing for about 5 s with
  data outstanding, even after the network came back; other browsers are untested
  ([browser transport limits](../ts/GUIDE.md#browser-transport-limits)). Longer outages need a new
  connection, which a supervised connection opens for you
  ([publisher recovery](../spec/publisher-recovery.md)).
- **Networks without UDP.** In TypeScript, opt in with `withWebSocketFallback()` from
  `@caprid/relay-websocket`. It races a WebSocket against WebTransport after 250 ms, so a network
  that blocks UDP still connects, with the [costs above](#retransmission). Default connections never
  fall back, and Python has no WebSocket transport.

## Not available yet

- Bandwidth estimation exposed to the application, and a built-in adaptive bitrate policy.
- Packet-level FEC, and media over unreliable datagrams.
- Relay-side dropping of late groups on moq-relay 0.17 (it ignores subscriber delivery timeouts;
  [moq-dev/moq#5881](https://github.com/moq-dev/moq/issues/5881)). The viewer drops them instead.
- Send-order prioritization from Chromium and from the Python publisher.
- In Python: an FEC option on `OpusEncoder`, an audio player, the per-track delivery policy
  (`lateDropUs`, `reorderWindowUs`, `catchUp`), and a WebSocket transport.
