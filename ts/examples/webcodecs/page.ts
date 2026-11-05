/**
 * The WebCodecs example page served by serve.mjs. Publish sends a test pattern (a sweeping bar that flashes white once a
 * second) and the microphone; Watch plays the broadcast back, video with `watchVideo` and audio with `listenAudio` on
 * one playout clock. Watching starts on a click, or at once when the URL has `?watch`: without a gesture the
 * `AudioContext` starts suspended, so video plays on wall pacing and audio waits for the Unmute click. serve.mjs puts
 * the endpoint, the namespace and, from RELAY_TOKEN, the token in `window.webcodecs`.
 */
import { createBroadcastClock, createPlayoutClock, type PlayoutClock } from '@caprid/moq-media';
import { connectRelayPublisher, connectRelaySubscriber, type RelaySubscriber } from '@caprid/relay';
import { captureMicrophone } from './capture-microphone.js';
import { type AudioListener, listenAudio, whenRunning } from './listen-audio.js';
import { publishCanvas } from './publish-canvas.js';
import { startTestPattern } from './test-pattern.js';
import { type VideoWatcher, watchVideo } from './watch-video.js';

declare global {
  interface Window {
    webcodecs: { url: string; namespace: string[]; token?: string };
  }
}

const element = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;
const token = element<HTMLInputElement>('token');
const publishButton = element<HTMLButtonElement>('publish');
const stopPublishing = element<HTMLButtonElement>('stop-publishing');
const watchButton = element<HTMLButtonElement>('watch');
const unmute = element<HTMLButtonElement>('unmute');
const stopWatching = element<HTMLButtonElement>('stop-watching');
const source = element<HTMLCanvasElement>('source');
const watched = element<HTMLCanvasElement>('watched');
const status = element<HTMLElement>('status');
const log = element<HTMLElement>('log');
const { url, namespace } = window.webcodecs;
token.value = window.webcodecs.token ?? '';
element<HTMLElement>('where').textContent = `${namespace.join('/')} on ${url}`;
element<HTMLAnchorElement>('watch-link').href = '?watch';
const getToken = () => token.value || undefined;

const say = (line: string) => {
  log.textContent = `${new Date().toLocaleTimeString()} ${line}\n${log.textContent}`;
};
const describe = (error: unknown) => {
  if (error instanceof Error) return `${error.name}: ${error.message}`;
  try {
    return typeof error === 'object' && error !== null ? JSON.stringify(error) : String(error);
  } catch {
    return String(error);
  }
};

/** Run `release` steps in reverse, ignoring failures: what a stop or a failed start leaves to clean up. */
async function releaseAll(release: (() => unknown)[]) {
  for (const undo of release.reverse()) {
    try {
      await undo();
    } catch {}
  }
}

let publishing: AbortController | undefined;
publishButton.addEventListener('click', async () => {
  publishButton.disabled = true;
  stopPublishing.disabled = false;
  const stop = new AbortController();
  publishing = stop;
  // Created in the click, so the browser lets it run.
  const context = new AudioContext({ sampleRate: 48_000 });
  const release: (() => unknown)[] = [() => context.close()];
  try {
    const stream = await navigator.mediaDevices.getUserMedia({
      audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true },
    });
    release.push(() => {
      for (const track of stream.getTracks()) track.stop();
    });
    const pattern = startTestPattern(source);
    const flashes = setInterval(() => pattern.flash(), 1000);
    release.push(() => {
      clearInterval(flashes);
      pattern.stop();
    });
    const microphone = await captureMicrophone(createBroadcastClock(), stream, { context });
    release.push(() => microphone.close());
    const publisher = await connectRelayPublisher({ url, namespace, getToken, signal: stop.signal });
    release.push(() => publisher.close());
    say('publishing');
    await publishCanvas(source, publisher, namespace, { signal: stop.signal, microphone });
  } catch (error) {
    if (!stop.signal.aborted) say(`publishing stopped: ${describe(error)}`);
  } finally {
    await releaseAll(release);
    say('stopped publishing');
    publishButton.disabled = false;
    stopPublishing.disabled = true;
  }
});
stopPublishing.addEventListener('click', () => publishing?.abort(new Error('Stopped')));

/**
 * Start with `start` until it succeeds, once a second: a watch that starts before the publisher's announce is
 * rejected (the announce race), and a catalog subscription the relay accepts can deliver nothing for a while.
 */
async function retry<T>(what: string, signal: AbortSignal, start: (signal: AbortSignal) => Promise<T>): Promise<T> {
  for (;;) {
    try {
      return await start(AbortSignal.any([signal, AbortSignal.timeout(5000)]));
    } catch (error) {
      signal.throwIfAborted();
      say(`${what}: ${describe(error)}; trying again`);
      await new Promise((resolve) => setTimeout(resolve, 1000));
      signal.throwIfAborted();
    }
  }
}

interface Watching {
  stop: AbortController;
  context: AudioContext;
  clock: PlayoutClock;
  watcher?: VideoWatcher;
  listener?: AudioListener;
  muted: boolean;
}
let watching: Watching | undefined;

const showStatus = ({ clock, watcher, listener, context, muted }: Watching) => {
  const parts = [`clock ${clock.mode}`];
  if (watcher) {
    const { shown, late, overflowed, lastOffsetUs } = watcher.stats;
    const offset = lastOffsetUs === undefined ? '–' : `${Math.round(lastOffsetUs / 1000)} ms`;
    parts.push(`video offset ${offset} · shown ${shown} · late ${late} · overflowed ${overflowed}`);
  }
  if (listener) {
    const { underruns, bufferedUs } = listener.input.stats;
    parts.push(
      `audio ${muted ? 'muted' : 'on'} · buffered ${Math.round(bufferedUs / 1000)} ms · underruns ${underruns}`,
    );
  } else if (context.state !== 'running') parts.push('audio waits for Unmute');
  status.textContent = parts.join(' · ');
  unmute.textContent = context.state === 'running' && !muted ? 'Mute' : 'Unmute';
};

async function watch() {
  watchButton.disabled = true;
  stopWatching.disabled = false;
  unmute.disabled = false;
  const stop = new AbortController();
  // Without a user gesture (?watch), the context starts suspended.
  const context = new AudioContext({ sampleRate: 48_000, latencyHint: 'interactive' });
  const clock = createPlayoutClock();
  const current: Watching = { stop, context, clock, muted: false };
  watching = current;
  const release: (() => unknown)[] = [() => context.close()];
  const ticker = setInterval(() => showStatus(current), 250);
  release.push(() => clearInterval(ticker));
  const onError = (error: unknown) => say(describe(error));
  try {
    const subscriber: RelaySubscriber = await connectRelaySubscriber({ url, getToken, signal: stop.signal });
    release.push(() => subscriber.close());
    // Audio starts once the context runs: a listener on a suspended context would hold every frame until it resumed.
    const audio = (async () => {
      await whenRunning(context);
      current.listener = await retry('audio', stop.signal, (signal) =>
        listenAudio(subscriber, namespace, { context, clock, onError, signal }),
      );
      release.push(() => current.listener?.stop());
      if (current.muted) current.listener.input.gain = 0;
      say('audio playing');
    })();
    current.watcher = await retry('video', stop.signal, (signal) =>
      watchVideo(subscriber, namespace, watched, { clock, onError, signal }),
    );
    release.push(() => current.watcher?.stop());
    say(`video playing${context.state === 'running' ? '' : '; click Unmute for sound'}`);
    await Promise.all([audio, new Promise((_, reject) => stop.signal.addEventListener('abort', reject))]);
  } catch (error) {
    if (!stop.signal.aborted) say(`watching stopped: ${describe(error)}`);
  } finally {
    await releaseAll(release);
    say('stopped watching');
    watching = undefined;
    watchButton.disabled = false;
    unmute.disabled = true;
    stopWatching.disabled = true;
  }
}

watchButton.addEventListener('click', watch);
stopWatching.addEventListener('click', () => watching?.stop.abort(new Error('Stopped')));
// A suspended context needs a gesture to run; after that the button mutes by gain, so audio still drives the clock.
unmute.addEventListener('click', async () => {
  if (!watching) return;
  const { context, listener } = watching;
  if (context.state !== 'running') {
    watching.muted = false;
    await context.resume();
    return;
  }
  watching.muted = !watching.muted;
  if (listener) listener.input.gain = watching.muted ? 0 : 1;
  showStatus(watching);
});
if (new URLSearchParams(location.search).has('watch')) watch();
