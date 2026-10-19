/**
 * The voice bot page served by serve.mjs: a Talk button that connects to the Relay, publishes the microphone and plays
 * the bot, and a log of what happens. serve.mjs puts the endpoint, the conversation's namespace and, from RELAY_TOKEN,
 * the token in `window.voiceBot`.
 */
import { connectRelayPublisher, connectRelaySubscriber } from '@mux/relay';
import type { AudioListener } from '../webcodecs/listen-audio.js';
import { talkToBot } from './talk-to-bot.js';

declare global {
  interface Window {
    voiceBot: { url: string; namespace: string[]; token?: string };
  }
}

const element = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;
const talk = element<HTMLButtonElement>('talk');
const hangUp = element<HTMLButtonElement>('hang-up');
const token = element<HTMLInputElement>('token');
const status = element<HTMLElement>('status');
const log = element<HTMLElement>('log');
const { url, namespace } = window.voiceBot;
token.value = window.voiceBot.token ?? '';
element<HTMLElement>('where').textContent = `${namespace.join('/')} on ${url}`;

const say = (line: string) => {
  log.textContent = `${new Date().toLocaleTimeString()} ${line}\n${log.textContent}`;
};
const describe = (error: unknown) => (error instanceof Error ? `${error.name}: ${error.message}` : String(error));

let call: AbortController | undefined;

talk.addEventListener('click', async () => {
  talk.disabled = true;
  hangUp.disabled = false;
  const stop = new AbortController();
  call = stop;
  // Created in the click, so the browser lets it play.
  const context = new AudioContext({ sampleRate: 48_000, latencyHint: 'interactive' });
  let stats: ReturnType<typeof setInterval> | undefined;
  const release: (() => unknown)[] = [() => context.close()];
  try {
    const stream = await navigator.mediaDevices.getUserMedia({
      audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true },
    });
    release.push(() => {
      for (const track of stream.getTracks()) track.stop();
    });
    stop.signal.throwIfAborted();
    const getToken = () => token.value || undefined;
    const userNamespace = [...namespace, 'user'];
    // Hanging up while connecting ends the attempts.
    const publisher = await connectRelayPublisher({ url, namespace: userNamespace, getToken, signal: stop.signal });
    release.push(() => publisher.close());
    const subscriber = await connectRelaySubscriber({ url, getToken, signal: stop.signal });
    release.push(() => subscriber.close());
    say('connected; waiting for the bot');
    let interrupts = 0;
    await talkToBot(publisher, subscriber, {
      signal: stop.signal,
      userNamespace,
      botNamespace: [...namespace, 'bot'],
      context,
      stream,
      onListening(listener: AudioListener) {
        say('the bot is on: say something, pause, and it answers; talk over it and it stops');
        stats = setInterval(() => {
          const { decoded, bufferedUs, underruns, cutUs } = listener.input.stats;
          status.textContent =
            `heard ${(decoded * 0.02).toFixed(1)} s · buffered ${Math.round(bufferedUs / 1000)} ms · ` +
            `underruns ${underruns} · interrupts ${interrupts} · cut ${Math.round(cutUs / 1000)} ms`;
        }, 250);
      },
      onInterrupt(interrupt) {
        interrupts++;
        // untilUs is about when the bot stopped, on its clock: how long the interrupt took to land, give or take skew.
        const lateMs = Math.round(Date.now() - interrupt.untilUs / 1000);
        say(`interrupted: the bot stopped and the player dropped what it held, ${lateMs} ms after its last audio`);
      },
      onError: (error) => say(describe(error)),
    });
  } catch (error) {
    if (!stop.signal.aborted) say(`stopped: ${describe(error)}`);
  } finally {
    clearInterval(stats);
    for (const undo of release.reverse()) {
      try {
        await undo();
      } catch {}
    }
    say('hung up');
    talk.disabled = false;
    hangUp.disabled = true;
  }
});

hangUp.addEventListener('click', () => call?.abort(new Error('Hung up')));
