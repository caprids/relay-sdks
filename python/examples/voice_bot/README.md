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
listenAudio ◀── <namespace>/bot/<gen>/audio ◀── FrameWriter ◀── OpusEncoder, paced in real time
  input.cut ◀── <namespace>/bot/<gen>/interrupt ◀── build_interrupt(until_us=end of what was sent)
 transcript ◀── <namespace>/bot/<gen>/transcript ◀── build_transcript_segment(seq, from_us, text, until_us, final)
```

`<gen>` is the bot's generation. The bot publishes through a supervised publisher, which serves each
connection one field below `<namespace>/bot`, and the page plays the newest one.

## Run it

Start the bot, then the page (in another terminal), with the same namespace:

```sh
RELAY_TOKEN=... mise run python:example:voice-bot -- --namespace acme/voice-bot
RELAY_TOKEN=... mise run ts:example:voice-bot -- --namespace acme/voice-bot
```

Open http://127.0.0.1:8320/ in Chromium, press **Talk**, say something and pause. The bot answers
about 0.7 s after you stop. Talk while it answers and it goes quiet. Replace `acme/voice-bot` with a namespace your token can
publish and subscribe in. Without `--namespace`, both sides default to `RELAY_NAMESPACE_PREFIX` (or
`relay-sdk-example`) plus `voice-bot`, for example `relay-sdk-example/voice-bot`. Either side can start first, and the bot keeps waiting for
the page across reloads. If the bot's connection drops, it reconnects as a new generation and the
page switches to it, so the bot answers the next thing you say.

| Flag | Meaning |
| --- | --- |
| `--namespace` | The conversation. The page publishes at `<namespace>/user` and the bot one generation below `<namespace>/bot` |
| `--threshold` | Speech is louder than this, in dBFS (default -45). Raise it in a noisy room |
| `--url` | Relay endpoint (default `RELAY_URL` or `https://relay.mux.global`) |
| `--room` | Fields appended to the `--url` path with `room_endpoint`, such as your token's root on the bare host. `--namespace` is then relative to them. The page's `--room` must match |

The bot's output shows each turn:

```text
publishing       bot=acme/voice-bot/bot/0mw40kc9g3qh6
listening        user=acme/voice-bot/user
user subscribed
heard speech
utterance        ms=1520
heard speech     barge-in=True
interrupted      untilUs=1792443548311433
utterance        ms=660
replied          ms=680
```

After a dropped connection, it logs the error and the new generation, then carries on:

```text
reconnecting     error=RelayError: QUIC connection terminated: Idle timeout (code 0x1)
publishing       bot=acme/voice-bot/bot/0mw40kq2jrg7q
```

### Without mise

`bot.py` is a single file. Copy it into a project (`uv init` makes one) and install the packages it
imports:

```sh
uv add caprid-relay "caprid-moq-media[opus]" numpy   # or: pip install caprid-relay "caprid-moq-media[opus]" numpy
RELAY_TOKEN=... uv run python bot.py --namespace acme/voice-bot
```

`caprid-relay` includes the native QUIC transport. The `opus` extra installs PyAV, whose wheels bundle
FFmpeg and are large, so plan for that on small hosts ([ADR 0006](../../../docs/adr/0006-opus-codec-behind-an-extra.md)).
The page still runs from a checkout of this repository. To serve your own, build it on `talkToBot`.

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


publisher = await connect_supervised_publisher(url, namespace=[*conversation, "bot"], get_token=get_token)
subscriber = await connect_supervised_subscriber(url, get_token=get_token)
await run_bot(publisher, subscriber, user_namespace=[*conversation, "user"], respond=respond, log=log)
```

Await your STT, LLM and TTS calls, or run blocking ones with `asyncio.to_thread`. A synchronous call
blocks the event loop, so the bot's connections stop answering the relay. After 10 s the relay's QUIC
idle timeout ends them with `Idle timeout`. The supervisors reconnect, but the user hears nothing
until they do.

## Drive it from a script

The browser's microphone sends audio all the time, so this never comes up on the page. A test harness
or a server-side talker that sends one short utterance as soon as it connects may get no answer. The
bot's subscription joins at the live edge, so it doesn't get anything sent before the subscription
was in place, and hears only the silence after it. Before you speak, wait for the bot to subscribe
(your microphone track's `has_demand`, logged by the bot as `user subscribed`), or send a second or
two of silence first. Keep sending silence between utterances, as a microphone does, so the bot's
pause detection sees each one end. `Talker` in [the tests](tests/test_voice_bot.py) does both.

## How it works

- **Staying up.** The bot publishes through `connect_supervised_publisher`. When the connection is
  lost, the bot's writers raise `RECONNECTING` until the next generation is up, and then move to it.
  The frame writer counts those packets as interrupted and carries on. Each generation is a new
  broadcast at its own namespace. The bot's catalog names no namespace, so each listener reads its
  tracks as being in the namespace it fetched the catalog from. The page finds the newest
  generation through namespace discovery and moves to it.
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
  the bot sends nothing. The player treats the gap as the talker going quiet.
- **Barge-in.** Speech that starts while the bot is speaking, or within 500 ms of the end of a reply
  (the most a listener on the audio-only target latency still holds), cancels the reply, discards
  what was encoded and not sent, and writes one interrupt with `until_us` at the end of the last packet
  written (counted before the write, so a write cut off mid-way is covered). It leaves `from_us`
  out: the listener drops everything it has not played. The next reply starts at or after
  `until_us`, as the interrupt track requires. A reply still being thought up when the user talks
  is cancelled with no interrupt: nothing of it was sent.
- **Transcript.** Each `str` from `Respond` becomes a transcript segment. The bot sends its interim
  version just before the segment's first audio, so the page has it when that audio plays, and the
  final one, with where the audio ended, once its last packet is sent. At the boundary it flushes the
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
When the connection under the bot's publisher closes, the bot answers the next utterance on the new
generation, which serves the same catalog.

Measured on 2026-11-05 against `ord.relay.mux.global`, with the page in headless Chromium on a
scripted microphone: dropping every packet the bot's publisher received ended its connection with
`Idle timeout` 8.3 s later. The next generation was up 0.4 s after that, and the page played the
bot's answer to the next utterance and showed its transcript. An idle publisher with nothing to send
stayed connected for 5 minutes, so an idle bot doesn't need a keepalive of its own.

Measured on 2026-10-19 against `sjc.relay.mux.global`, with the page in headless Chromium on a
scripted microphone (1.5 s of speech, a 1 s pause, 0.6 s talking over the reply): over four runs of
three turns each, every interrupt reached the page's player 32 to 50 ms after the bot's last audio,
the player cut 117 to 140 ms of held reply audio per run, and playback had no underrun. With the
transcript track, over four more runs: interrupts landed 32 to 51 ms after the bot's last audio, each
reply's text reached the page 65 to 72 ms before its audio played, and each cut reply's line was shown
marked cut.
