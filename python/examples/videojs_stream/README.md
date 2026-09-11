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
