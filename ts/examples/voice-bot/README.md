# Talk to a voice bot in the browser

This page is the user's side of the [Python voice bot](../../../python/examples/voice_bot/README.md).
It publishes the microphone alone and plays the bot through `@caprid/moq-media`'s audio player, and
shows the text of each reply as its audio plays. When the user talks over the bot, the bot sends an
interrupt. The player then cuts the reply audio it still holds, and the transcript cuts the words the
bot did not say.

[`talk-to-bot.ts`](talk-to-bot.ts) is the part to copy. It puts together pieces of the WebCodecs
example:

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
needs. Start the bot before or after the page, with the same namespace inside your token's root:

```sh
RELAY_TOKEN=... mise run python:example:voice-bot -- --namespace acme/voice-bot
RELAY_TOKEN=... mise run ts:example:voice-bot -- --namespace acme/voice-bot
```

Without `--namespace`, both default to `RELAY_NAMESPACE_PREFIX` (or `relay-sdk-example`) plus
`voice-bot`. `--port` picks another port. `--room` roots `RELAY_URL` at the given fields with
`roomEndpoint`, as the bot's `--room` does.

With `RELAY_TOKEN` set, the page holds the token. The server listens on the loopback interface only
and never prints the token. Without `RELAY_TOKEN`, paste a token into the page.

The status line shows how much of the bot's audio the player has played, its buffer, underruns,
interrupts, and how much they cut. Below it, the bot's words appear as they play, with a dash where
the user talked over the bot. The log says how long after the bot's last audio each interrupt
landed.
