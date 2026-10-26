# Talk to a voice bot in the browser

The user's side of the [Python voice bot](../../../python/examples/voice_bot/README.md). The page
publishes the microphone alone and plays the bot through `@caprid/moq-media`'s audio player, showing the
text of each reply as its audio plays. When the bot is talked over and sends an interrupt, the
player cuts the reply audio it still holds, and the transcript the words it did not say.

[`talk-to-bot.ts`](talk-to-bot.ts) is the part to copy. It is the WebCodecs example's pieces put
together:

```ts
import { connectRelayPublisher, connectRelaySubscriber } from '@caprid/relay';
import { talkToBot } from './talk-to-bot.js';

// In a click handler, so the context may play and the microphone prompt shows.
const context = new AudioContext({ sampleRate: 48_000, latencyHint: 'interactive' });
const stream = await navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: true } });
const userNamespace = ['demo', 'voice-bot', 'user'];
const publisher = await connectRelayPublisher({ url, namespace: userNamespace, getToken });
const subscriber = await connectRelaySubscriber({ url, getToken });
await talkToBot(publisher, subscriber, {
  signal: hangUp.signal,
  userNamespace,
  botNamespace: ['demo', 'voice-bot', 'bot'],
  context,
  stream,
  onInterrupt: (interrupt) => console.log('the bot stopped', interrupt.untilUs),
});
```

- [`publishMicrophone`](../webcodecs/publish-microphone.ts) publishes a catalog with one audio track
  and mono Opus from `captureMicrophone`, one group per frame. `publishCanvas` encodes its
  microphone through the same code.
- [`listenAudio`](../webcodecs/listen-audio.ts) plays the bot's `audio` track with the audio-only
  target latency and, because the bot's catalog lists the interrupt track, cuts on each interrupt.
  `talkToBot` passes `echoCancellation: true`, so playback goes through Chromium's echo canceller
  and the bot does not hear itself.
- Because the catalog lists the transcript track, the listener keeps the bot's
  [transcript](../../../spec/media-layer.md#transcript-track): `listener.transcript.linesAt(listener.clock.position())`
  is what the bot has said so far, each line marked `cut` where it was talked over.
- The bot can start before or after the page: until its catalog arrives, `talkToBot` retries every
  second and reports each failed attempt to `onError`.

## Run it

[`page.ts`](page.ts) wraps `talkToBot` in a page with Talk and Hang up buttons, and
[`serve.mjs`](serve.mjs) serves it on http://127.0.0.1:8320, a secure context as WebTransport
needs. Start the bot first or after, with the same namespace:

```sh
RELAY_TOKEN=... mise run python:example:voice-bot -- --namespace mattypoo/voice-bot
RELAY_TOKEN=... mise run ts:example:voice-bot -- --namespace mattypoo/voice-bot
```

`--port` picks another port, and `--room` roots `RELAY_URL` at the given fields with `roomEndpoint`, as the bot's `--room` does. With `RELAY_TOKEN` set the page holds the token, so the server listens
on the loopback interface only and never prints it; without it, paste one into the page. The status
line shows how much of the bot's audio the player has played, its buffer, underruns, interrupts and
how much they cut; below it, the bot's words appear as they play, with `—` where it was talked over.
The log says how long after the bot's last audio each interrupt landed.
