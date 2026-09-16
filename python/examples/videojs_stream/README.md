# Stream video to Relay for a Video.js player

`stream.py` publishes a live H.264 stream (and optional Opus audio) to a Relay namespace in the
exact shape the Video.js v10 MoQ engine consumes, then prints the `moqt://` source URL to hand the
player. By default it generates a test pattern (color bars, a bouncing square, and a clock so you can
read the glass-to-glass delay); with `--input` it transcodes a media file instead.

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
joining later starts at the current GOP.

Options:

| Flag | Meaning |
| --- | --- |
| `--input FILE` | Transcode a media file (video scaled to `--width`/`--height`; its audio as Opus with `--audio auto`) |
| `--loop` | Loop the input file |
| `--width`, `--height`, `--fps`, `--bitrate`, `--gop` | Test-pattern size and encoder settings (defaults 640x360, 30 fps, 1.5 Mbit/s, 2 s GOP) |
| `--audio auto\|tone\|off` | `auto`: the file's audio, none for the pattern; `tone`: a 440 Hz sine; `off`: video only |
| `--duration SECONDS` | Stop after a while (default: until Ctrl-C) |
| `--on-track-end stop\|continue` | What to do if a track closes mid-run: `stop` (default) fails the run; `continue` keeps publishing the surviving tracks |
| `--verify` | Also subscribe in-process and report the catalog, groups, keyframes, and timestamps received |
| `--show-token` | Print the player URL with the token filled in |

## What goes on the wire

| Track | Shape |
| --- | --- |
| `catalog` | One MSF `draft-01` catalog JSON per group, re-sent whenever the relay (re)subscribes. Lists the `video` (and `audio`) tracks with `packaging: "loc"`, the `avc1.PPCCLL` codec string derived from the encoder's SPS, and the avcC decoder configuration inline in `initDataList`. |
| `video` | LOC-packaged H.264 in AVCC form (length-prefixed NAL units, parameter sets out of band). One MOQT group per GOP: object 0 is the keyframe, later objects the P-frames. Object properties: Timestamp (0x10, microseconds), Timescale (0x08), and Video Config (0x0D, the avcC) on keyframes. |
| `audio` | LOC-packaged Opus at 48 kHz stereo, one 20 ms frame per group (every audio frame is a random-access point). |

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
track on a fresh publisher connection is what a real application should do; the example does not.

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
is how the example is checked without a browser. Expect `keyframesWithConfig=0`: the relay drops the
odd-typed Video Config object property on its way through, so players configure their decoder from
the avcC in the catalog's `initDataList` (which is why the catalog carries it).

Verified against `sjc.relay.mux.global` on 2026-09-11: the catalog, one video group per GOP with a
keyframe at object 0, and 50 Opus objects per second all arrived at the in-process subscriber for
both the test pattern and a transcoded MP4 with AAC audio.

The format details come from the Video.js v10 sources (`packages/spf/src/media/moq`: `loc.ts`,
`build-catalog.ts`, `parse-catalog.ts`, `parse-source.ts`) at the revision pinned in
`python/packages/moq-core/UPSTREAM.json`.
