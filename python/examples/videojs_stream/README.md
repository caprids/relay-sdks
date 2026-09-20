# Stream video to Relay for a Video.js player

`stream.py` publishes a live H.264 stream (and optional Opus audio) to a Relay namespace in the
exact shape the Video.js v10 MoQ engine consumes, then prints the `moqt://` source URL to hand the
player. By default it generates a test pattern (color bars, a bouncing square, and a clock so you can
read the glass-to-glass delay), with a short beep whenever the square hits a wall so you can check
audio/video sync manually. Unmute the player and compare each beep with the square's bounce; the
first beep is at two seconds. With `--input` it transcodes a media file instead.

Media encoding uses PyAV's bundled FFmpeg (libx264, libopus), installed by the workspace's
`examples` dependency group. No system ffmpeg is needed.

## Run it

```sh
RELAY_TOKEN=... mise run python:example:videojs -- --namespace mattypoo/relay-sdk-example/tv --show-token
```

Use a namespace your token authorizes to publish. The script prints two lines worth copying:

```
player url             src=moqt://sjc.relay.mux.global/#msf:mattypoo-relay.2dsdk.2dexample-tv--catalog&c4m=<RELAY_TOKEN>
moqt://sjc.relay.mux.global/#msf:mattypoo-relay.2dsdk.2dexample-tv--catalog&c4m=eyJ...   (only with --show-token)
```

The second line is the complete source URL including your token; give it to a Video.js v10 player
as its source (the `moqt://` scheme selects the MoQ engine). The player moves the `c4m` token onto
the relay connect URL's `jwt` parameter, subscribes to the `catalog` track, then to the media tracks
it lists. Press play in the player once `video flowing` appears in the script's output; a viewer
joining an active publisher later starts at the current GOP. Restarting a publisher on the same
namespace does not guarantee that attached viewers switch to the replacement; see
[Publisher replacement](#publisher-replacement).

Options:

| Flag | Meaning |
| --- | --- |
| `--input FILE` | Transcode a media file (video scaled to `--width`/`--height`; its audio as Opus with `--audio auto`) |
| `--loop` | Loop the input file |
| `--width`, `--height`, `--fps`, `--bitrate`, `--gop` | Test-pattern size and encoder settings (defaults 640x360, 30 fps, 1.5 Mbit/s target, 2 s GOP) |
| `--rate-control abr\|cbr` | `abr` (default): content-dependent bitrate target; `cbr`: add filler to approach the requested video bitrate for bandwidth tests |
| `--audio auto\|tone\|off` | `auto` (default): the file's audio, or wall-hit beeps for the pattern; `tone`: a 440 Hz sine; `off`: video only |
| `--duration SECONDS` | Stop after a while (default: until Ctrl-C) |
| `--on-track-end stop\|continue` | What to do if a track closes mid-run: `stop` (default) fails the run; `continue` keeps publishing the surviving tracks |
| `--verify` | Also subscribe in-process and report the catalog, groups, keyframes, and timestamps received |
| `--show-token` | Print the player URL with the token filled in |

## Bitrate and bandwidth tests

The default `--rate-control abr` asks x264 for an average bitrate. Simple content can use far less:
color bars at the 1.5 Mbit/s target may encode to roughly 130 kbit/s. The target is not a guaranteed
rate or a strict peak limit. Use `--rate-control cbr` for bandwidth/congestion tests:

```sh
RELAY_TOKEN=... mise run python:example:videojs -- --namespace mattypoo/relay-sdk-example/tv \
  --rate-control cbr --bitrate 1500000 --audio off --duration 30
```

CBR enables a one-second VBV buffer and H.264 HRD/filler NALs. Encoded video approaches the target
over several seconds; startup and individual frames can vary. Audio and transport overhead are
additional. A viewer must subscribe before the example publishes media, and backpressure or skipped
frames can still reduce the delivered rate. `--input` keeps the file's frame timing, so a source with
irregular timing can differ from the encoder's configured `--fps` rate.

`media progress` and `finished` print `encodedVideoKbps` / `encodedAudioKbps` and
`publishedVideoKbps` / `publishedAudioKbps`. These are cumulative payload kilobits per wall-clock
second since the producer started (including startup, skipped frames, and shutdown). Published rates
count completed writes; they exclude protocol overhead and do not prove receiver delivery. Compare
encoded and published rates to distinguish a compressible source from absent demand or a slow link.

`--duration` ends successfully after a normal deadline, including cancellation of a write in flight.
An unrelated encoder or publishing error still fails the run even if it coincides with the deadline.

## What goes on the wire

| Track | Shape |
| --- | --- |
| `catalog` | One MSF `draft-01` catalog JSON per group, re-sent whenever the relay (re)subscribes. Lists the `video` (and `audio`) tracks with `packaging: "loc"`, the `avc1.PPCCLL` codec string derived from the encoder's SPS, and the avcC decoder configuration inline in `initDataList`. |
| `video` | LOC-packaged H.264 in AVCC form (length-prefixed NAL units, parameter sets out of band). One MOQT group per GOP: object 0 is the keyframe, later objects the P-frames. Object properties: Timestamp (0x10, microseconds), Timescale (0x08), and Video Config (0x0D, the avcC) on keyframes. |
| `audio` | LOC-packaged Opus at 48 kHz stereo, one 20 ms frame per group (every audio frame is a random-access point). |

The table describes what this publisher sends. Relay 0.14.17 preserved the declared track timescale
and object timestamps in the tested media flow, but dropped the object-level Timescale and Video
Config properties. The example retains those properties for consumers that support them; playback
through this relay relies on the catalog's `initDataList` for decoder configuration. See
[Relay property handling](../../../spec/behavior.md#relay-property-handling).

All three tracks declare Timescale = 1,000,000 as a track property, matching the Video.js publisher.
Video is written only while the relay has a live subscription for the track; each new subscription
starts at the next keyframe, and a subscription that changes mid-GOP resets that group and resumes
at the next keyframe. The encoder runs `libx264` `veryfast` / `zerolatency` with a fixed GOP and no
B-frames so timestamps are monotonic.

## Recovering from a slow or interrupted uplink

Audio and video have independent writer tasks. A blocked audio frame cannot hold up video, and
neither track builds an unlimited backlog. The producer hands each batch to the event loop before
encoding the next one, so pending thread callbacks cannot become another unbounded queue.

| Buffer | Maximum queued frames | Maximum queued payload | Stale after |
| --- | --- | --- | --- |
| Video | 120 | 16 MiB | One GOP (`--gop`, default 2 seconds) |
| Audio | 100 | 1 MiB | 250 ms |

The first limit reached triggers discard. These bounds cover queued payloads; the encoder and one
write in flight per track also use memory. Lag is measured against the producer's monotonic start
clock and each frame's media timestamp. Old audio is discarded individually. A video overflow or
stale frame resets the current GOP, skips its dependent frames, and resumes at a fresh keyframe.
Timestamps keep their original timeline; the publisher does not replay an outage at reduced speed.

Audio still uses one group per frame on the wire, now through `open_group()` / `write()` / `close()`.
A five-second group-operation deadline resets an interrupted audio frame or video GOP while keeping
the track usable. Once the link recovers, audio resumes with recent frames and video with the next
usable keyframe. A connection that closes still ends the run; this example does not reconnect.

Every second a `media progress` line reports queued frame counts, current lag (including a blocked
write), cumulative sent/skipped/dropped/stale counts, and the maximum observed lag. `staleVideo` and
`staleAudio` include buffer overflow and frames discarded while waiting for a recovery keyframe.
The shutdown path joins the producer and drains both queues so its final encoded frames are counted.

## Publisher replacement

A stopped or killed publisher can leave an attached viewer silent while its subscriber connection
stays open, and relay publisher-death notification can be delayed. Starting this script again on
the same namespace
does not guarantee immediate recovery for attached viewers. Subscribing soon after an orderly
retract/re-announce can also fail transiently.

A player should report stalls using its own media-progress check, retire the old subscription under
an explicit trailing-data policy, and retry with backoff and a total recovery deadline. After a
publisher generation changes, it should reload the catalog and resume at a usable keyframe. A
`ready` connection alone does not establish that media is advancing. See the shared
[publisher recovery guide](../../../spec/publisher-recovery.md) for the callback semantics, retry
policy, and the option of switching viewers to a fresh generation-specific namespace.

## When a track ends under you

The catalog uses a single-object write. Failed writes and media groups reset their stream and keep
the track open while the publisher connection remains open. The example drops failed media and
retries current catalog state after a one-second pause while demand remains; it keeps only one
pending catalog document. Shutdown cancels the pause. Data that already reached the relay cannot
be withdrawn.
A closed track or connection is terminal. The example checks the writer's `closed` state after a
write failure (`bound` cannot distinguish a closed track from one with no current subscribers).

An ended track is reported once, with the error, on its own `audio track ended` / `video track ended`
/ `catalog track ended` line, and the run then **fails with a non-zero exit**. That is the default
because the catalog still advertises the track: carrying on would serve viewers a stream that
silently no longer matches its own catalog — video with no audio, and no error anywhere. Pass
`--on-track-end continue` to keep publishing the tracks that survived instead. Re-registering the
track on a fresh publisher connection can restore publishing; it does not guarantee that existing
viewers switch to the replacement. The example does not reconnect.

The `finished` summary accounts for every sample the encoder produced, and it is printed even when
the run fails:

```
finished  videoGroups=30 videoObjects=1800 audioObjects=2989 skippedVideo=0 skippedAudio=11 droppedVideo=0 droppedAudio=0 discardedVideo=0 discardedAudio=0 staleVideo=0 staleAudio=0 maxLagSeconds=0.01 catalogSends=1 catalogFailures=0
```

| Counter | Meaning |
| --- | --- |
| `videoObjects`, `audioObjects` | Sent to the relay |
| `skippedVideo`, `skippedAudio` | Not sent because the relay had no subscription for the track, because a write failed before it bound, or because the group they belonged to was reset — the track is fine and publishing resumes at the next sample or keyframe |
| `droppedVideo`, `droppedAudio` | Not sent because the track had ended; these samples are gone for good |
| `discardedVideo`, `discardedAudio` | Encoded but abandoned while queued or in flight when the run stopped |
| `staleVideo`, `staleAudio` | Discarded because of buffer overflow, excessive age, or waiting for a recovery keyframe |
| `maxLagSeconds` | Largest observed frame age relative to the producer clock |
| `catalogSends`, `catalogFailures` | Catalog documents accepted, and retryable catalog write failures |
| `endedTracks` | Present only when a track ended, listing which |

`--verify` runs an in-process subscriber on the same tracks and prints what a player would see, which
is how the example is checked without a browser. With relay 0.14.17, expect
`keyframesWithConfig=0`: its output omits the Video Config object property, so players configure
their decoder from the avcC in the catalog's `initDataList`. This counter does not indicate a missing
publisher configuration or a decoder failure; another relay may forward the property.

Verified against `sjc.relay.mux.global` on 2026-09-11: the catalog, one video group per GOP with a
keyframe at object 0, and 50 Opus objects per second all arrived at the in-process subscriber for
both the test pattern and a transcoded MP4 with AAC audio.

The format details come from the Video.js v10 sources (`packages/spf/src/media/moq`: `loc.ts`,
`build-catalog.ts`, `parse-catalog.ts`, `parse-source.ts`) at the revision pinned in
[moq-media/UPSTREAM.json](../../packages/moq-media/UPSTREAM.json).

The format helpers now come from the workspace's [moq-media package](../../packages/moq-media/README.md).
The example retains its encoder, pacing, queues, catalog service, and group recovery logic. Stage 1
of the [media proposal](../../../spec/media-layer.md) is implemented; stages 2 and 3 describe the
remaining catalog service, frame writers, and reader helpers.
