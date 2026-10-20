# Talk to a voice bot over Relay

[`bot.py`](bot.py) is a parrot: it listens to a browser user's microphone, waits for a pause, and
says what it heard back. Talk over it and it stops at once, and sends an
[interrupt](../../../spec/media-layer.md#interrupt-track) so the user's player drops the reply audio
it still holds. The text of each reply goes on the
[transcript track](../../../spec/media-layer.md#transcript-track), for the page to show as it plays. The user's side is the [voice bot page](../../../ts/examples/voice-bot/README.md).

Everything between the microphone and the speaker is the SDKs':

```text
browser page                          Relay                       bot.py
captureMicrophone → publishMicrophone ──▶ <namespace>/user/audio ──▶ want → FrameReader → OpusDecoder (16 kHz)
                                                                     → EnergyGate → respond(utterance)
listenAudio ◀── <namespace>/bot/audio ◀── FrameWriter ◀── OpusEncoder, paced in real time
  input.cut ◀── <namespace>/bot/interrupt ◀── build_interrupt(until_us=end of what was sent)
 transcript ◀── <namespace>/bot/transcript ◀── build_transcript_segment(seq, from_us, text, until_us, final)
```

## Run it

Start the bot, then the page (in another terminal), with the same namespace:

```sh
RELAY_TOKEN=... mise run python:example:voice-bot -- --namespace mattypoo/voice-bot
RELAY_TOKEN=... mise run ts:example:voice-bot -- --namespace mattypoo/voice-bot
```

Open http://127.0.0.1:8320/ in Chromium, press **Talk**, say something and pause. The bot answers
about 0.7 s after you stop. Talk while it answers and it goes quiet. Use a namespace your token can
publish and subscribe in; without `--namespace`, both default to `RELAY_NAMESPACE_PREFIX` (or
`relay-sdk-example`) plus `voice-bot`. Either side can start first, and the bot keeps waiting for
the page across reloads. The bot's own publisher is not supervised: if its connection closes, the
bot exits with the error, and you start it again.

| Flag | Meaning |
| --- | --- |
| `--namespace` | The conversation: the page publishes at `<namespace>/user`, the bot at `<namespace>/bot` |
| `--threshold` | Speech is louder than this, in dBFS (default -45); raise it in a noisy room |
| `--url` | Relay endpoint (default `RELAY_URL` or `https://sjc.relay.mux.global`) |
| `--room` | Fields appended to the `--url` path with `room_endpoint`, such as your token's root on the bare host; `--namespace` is then relative to them. The page's `--room` must match |

Its output shows each turn:

```text
listening        user=mattypoo/voice-bot/user bot=mattypoo/voice-bot/bot
user subscribed
heard speech
utterance        ms=1520
heard speech     barge-in=True
interrupted      untilUs=1792443548311433
utterance        ms=660
replied          ms=680
```

## Plug in a real brain

STT, a language model and TTS go in one async function, `Respond`: it gets an `Utterance` (mono s16
PCM at 16 kHz, and where it lies on the user's timeline) and yields the reply as PCM at the same
rate, in chunks of any size. The bot speaks each chunk as it arrives and cancels the iterator when
it is talked over. A `str` it yields is the text of the audio after it, up to the next `str`: the
bot publishes it on the transcript track where that audio starts.

```python
async def respond(utterance: Utterance) -> AsyncIterator[bytes | str]:
    text = await speech_to_text(utterance.pcm, rate=utterance.rate)
    for sentence in split_sentences(await reply_to(text)):
        yield sentence
        async for pcm in text_to_speech(sentence, rate=utterance.rate):
            yield pcm


await run_bot(publisher, subscriber, user_namespace=..., bot_namespace=..., respond=respond, log=log)
```

## How it works

- **Listening.** A supervised subscriber wants the user's `audio` track, so a page that reloads or
  starts late is picked up again. A frame reader with an idle playout clock orders the frames and
  never judges one late: nothing plays them. `OpusDecoder` decodes at 16 kHz and resets at every
  discontinuity.
- **Turns.** `EnergyGate` is the example's voice activity detection, not SDK code: an utterance
  starts after 100 ms louder than the threshold and ends after 700 ms of quiet (or 15 s). The
  utterance keeps the 100 ms before the gate opened and drops the quiet that closed it.
- **Speaking.** Each reply is one `OpusEncoder` run, stamped from the bot's broadcast clock when its
  first audio arrives, and each packet is written when the clock reaches its timestamp, as a
  microphone would send it. A listener then holds little of a reply at any moment, and a
  `FrameWriter` maps one packet per group. A reply that comes more than two frames too slowly, as
  from a TTS that stalls, starts a new run at the clock rather than send late audio, and the frame
  writer drops a packet that a stalled write held back more than 100 ms (`stale`). Between replies
  the bot sends nothing; the player treats the gap as the talker going quiet.
- **Barge-in.** Speech that starts while the bot is speaking, or within 500 ms of the end of a reply
  (the most a listener on the audio-only target latency still holds), cancels the reply, discards
  what was encoded and not sent, and writes one interrupt with `until_us` at the end of the last packet
  written (counted before the write, so a write cut off mid-way is covered). It leaves `from_us`
  out: the listener drops everything it has not played. The next reply starts at or after
  `until_us`, as the interrupt track requires. A reply still being thought up when the user talks
  is cancelled with no interrupt: nothing of it was sent.
- **Transcript.** Each `str` from `Respond` becomes a transcript segment. The bot sends its interim
  version just before the segment's first audio, so the page has it when that audio plays, and the
  final one, with where the audio ended, once its last packet is sent; at the boundary it flushes the
  encoder, so each segment ends where its own audio does. Segments go out in order from a queue of
  their own, so a slow transcript write never holds the audio back. An interrupted segment gets no
  final version: the page cuts it with the interrupt, and text whose audio a stall dropped is
  dropped with it. Segment numbers start at the clock's millisecond, so
  a restarted bot does not reuse them. The parrot cannot tell what the user said, so its text says
  how long it was.
- **Echo.** The page plays the bot through Chromium's echo canceller, so the bot does not hear itself
  and take it for the user talking over it.

`examples/voice_bot/tests` runs the bot over the in-memory relay hub against a scripted microphone:
it answers after a pause, interrupts itself when talked over, and every packet of the first reply
lies before the interrupt's `until_us` and every packet of the next at or after it. Each reply's
transcript segment starts at its first packet, and only the uninterrupted one gets a final version.

Measured on 2026-10-19 against `sjc.relay.mux.global`, with the page in headless Chromium on a
scripted microphone (1.5 s of speech, a 1 s pause, 0.6 s talking over the reply): over four runs of
three turns each, every interrupt reached the page's player 32 to 50 ms after the bot's last audio,
the player cut 117 to 140 ms of held reply audio per run, and playback had no underrun. With the
transcript track, over four more runs: interrupts landed 32 to 51 ms after the bot's last audio, each
reply's text reached the page 65 to 72 ms before its audio played, and each cut reply's line was shown
marked cut.
