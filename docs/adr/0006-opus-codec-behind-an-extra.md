---
status: accepted
date: 2026-10-19
---

# The Python Opus encoder and decoder live in moq_media behind an optional extra

> The package names below predate the rename in [#222](https://github.com/muxinc/relay-sdks/issues/222): `@mux/*` is now `@caprid/*` on npm, `mux-*` is now `caprid-*` on PyPI, and `mux_relay` is now `caprid_relay`.

Until now the media format packages owned no encoder. `spec/media-layer.md` said "Encoders stay out of
both" and `spec/behavior.md` said the format package "owns no encoder". The one Opus encoder was example
code, a PyAV `libopus` class in the Video.js streaming example. A Python voice bot needs Opus both ways
(#320): it decodes a browser user's microphone for speech recognition and encodes synthesized speech to
send back. Every bot would otherwise copy the same PyAV setup, 20 ms re-chunking, timeline stamping and
end-of-utterance padding. We decided:

- **`moq_media.opus` holds `OpusEncoder` and `OpusDecoder`**, built on PyAV, which is the package's only
  third-party dependency and is declared as the optional extra `opus` (`mux-moq-media[opus]`).
- **Nothing else in `moq_media` imports PyAV, and the package root does not import `moq_media.opus`.**
  `import moq_media` still needs nothing beyond `mux-moq-core`. An import-linter contract keeps PyAV out of
  every other module, and a test imports the package root with PyAV blocked. Importing `moq_media.opus`
  without the extra raises an `ImportError` that names it.
- **Facts read from the packet bytes stay pure**: `opus_packet_duration_us` is in `moq_media.audio`,
  exported from the root, and held to shared vectors with TypeScript's `opusPacketDurationUs`.
- **TypeScript gets no counterpart.** Browsers have WebCodecs' `AudioEncoder` and `AudioDecoder`, which
  `@mux/moq-media` already takes, and the TypeScript server-side runtimes have no bot use yet.

## Considered options

- **A module in `moq_media` behind an extra (chosen).** Installing one more package line is all it costs,
  and the encoder stamps packets on the same broadcast timeline the clock and LOC helpers in the same
  package define.
- **A separate distribution, `mux-moq-opus`.** Cleaner dependency lines, but one more package to release on
  the Python train for two classes, and an extra is the standard Python way to say "this part needs a
  heavy optional dependency".
- **`mux_relay.media`.** The product package would gain a media dependency, and an encoder has nothing to
  do with the relay.
- **Leave it in the examples.** Keeps the old rule, and every bot keeps copying the class.

## Consequences

- `spec/behavior.md` "Package boundaries" and `spec/media-layer.md` "Package placement" now let the format
  package own an encoder in a module behind an extra, so long as the package root does not import it.
- The development workspace depends on `mux-moq-media[opus]`, so `mise run python:check` tests the codec.
- An encoder for another codec (AAC, or video through libx264) would follow the same pattern: one module,
  one extra.
