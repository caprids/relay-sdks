/**
 * The voice bot page's listening half, through `talkToBot`, on a real supervised subscriber over scripted connections:
 * the bot is a supervised publisher, so each restart is a newer generation under its root, and a crashed one stays
 * announced until the relay's idle timeout (#404, and Bugbot's finding on #471).
 */
import { buildMsfCatalog, INTERRUPT_TRACK, LOC_TIMESTAMP, TRANSCRIPT_TRACK } from '@caprid/moq-media';
import type { RelayPublisher } from '@caprid/relay';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { talkToBot } from '../examples/voice-bot/talk-to-bot.js';
import type { AudioListener } from '../examples/webcodecs/listen-audio.js';
import { superviseRelaySubscriber } from '../src/supervised-subscriber.js';
import { newGeneration } from '../src/supervision.js';
import { FakeContext, OPUS_PACKETS, stubAudioGlobals } from './helpers/fake-audio.js';
import { type FakeSubscriber, type FakeSubscription, fakeSubscriber, until } from './helpers/supervision-fakes.js';

// The microphone half needs a browser; these tests are about the listening half.
vi.mock('../examples/webcodecs/capture-microphone.js', () => ({
  captureMicrophone: vi.fn(async () => ({ close: vi.fn() })),
}));
vi.mock('../examples/webcodecs/publish-microphone.js', () => ({
  publishMicrophone: vi.fn(
    (_publisher: unknown, _namespace: unknown, { signal }: { signal: AbortSignal }) =>
      new Promise<void>((resolve) => signal.addEventListener('abort', () => resolve(), { once: true })),
  ),
}));
// Chromium's echo canceller runs through a WebRTC loopback, which Node has not got.
vi.mock('@caprid/moq-media', async (original) => ({
  ...(await original<typeof import('@caprid/moq-media')>()),
  createEchoCancellationLoopback: vi.fn(async () => ({ close: vi.fn() })),
}));

const ROOT = ['room', 'bot'];
const fast = { initialDelayMs: 0, maxDelayMs: 0 };

/** The bot's catalog: its tracks name no namespace, so each is in its generation's (python/examples/voice_bot). */
function botCatalog() {
  const catalog = JSON.parse(
    new TextDecoder().decode(
      buildMsfCatalog({ namespace: ROOT, audio: {}, data: [{ name: INTERRUPT_TRACK }, { name: TRANSCRIPT_TRACK }] }),
    ),
  ) as { tracks: Record<string, unknown>[] };
  for (const track of catalog.tracks) delete track['namespace'];
  return new TextEncoder().encode(JSON.stringify(catalog));
}

const cleanup: (() => unknown)[] = [];
beforeEach(stubAudioGlobals);
afterEach(async () => {
  for (const undo of cleanup.splice(0).reverse()) await undo();
  vi.unstubAllGlobals();
});

/** Talk to the bot on a supervised subscriber whose connections the test scripts. */
async function talk() {
  const connections: FakeSubscriber[] = [];
  const subscriber = await superviseRelaySubscriber(
    async () => {
      const fake = fakeSubscriber();
      connections.push(fake);
      return fake;
    },
    { url: 'https://relay.example.com/moq', reconnect: fast },
  );
  const hangUp = new AbortController();
  const listening: AudioListener[] = [];
  const restarts: { reason: string; namespace: string[] }[] = [];
  const errors: unknown[] = [];
  const call = talkToBot({} as RelayPublisher, subscriber, {
    signal: hangUp.signal,
    botNamespace: ROOT,
    userNamespace: ['room', 'user'],
    context: new FakeContext() as unknown as AudioContext,
    stream: {} as MediaStream,
    onListening: (listener) => listening.push(listener),
    onRestarted: (info) => restarts.push(info),
    onError: (error) => errors.push(error),
  });
  call.catch(() => {});
  cleanup.push(async () => {
    hangUp.abort(new Error('Hung up'));
    await call.catch(() => {});
    subscriber.close();
  });
  await until(() => connections[0]?.discoveries.length === 1, 'discovery of the bot root');
  const connection = connections[0] as FakeSubscriber;
  const discovery = () => connection.discoveries.at(-1) as FakeSubscriber['discoveries'][number];
  const on = (gen: string, track: string) =>
    connection.subscriptions.filter(
      ({ options }) => options.track === track && options.namespace.join('/') === [...ROOT, gen].join('/'),
    );
  const live = (gen: string, track: string) => on(gen, track).filter(({ cancelled }) => !cancelled);
  /** Every subscription not cancelled, by generation field and track. */
  const open = () =>
    connection.subscriptions
      .filter(({ cancelled }) => !cancelled)
      .map(({ options }) => `${options.namespace.at(-1)}/${options.track}`)
      .sort();
  return { connection, discovery, on, live, open, listening, restarts, errors };
}

/** One Opus frame of the bot's audio, in group `groupId`. */
function speak(audio: FakeSubscription, groupId: number) {
  audio.handlers.onObject?.({
    groupId,
    objectId: 0,
    subgroupId: 0,
    status: 'normal',
    properties: [{ type: LOC_TIMESTAMP, value: Date.now() * 1000 }],
    payload: OPUS_PACKETS[0],
  } as never);
}

/** What a live generation does: accept, send its catalog, and one audio frame. */
function serve(catalog: FakeSubscription, audio: FakeSubscription) {
  catalog.accept();
  catalog.handlers.onObject?.({ groupId: 0, objectId: 0, status: 'normal', payload: botCatalog() } as never);
  audio.accept();
  speak(audio, 0);
}

it('lands on the newer generation within one announcement when an older one is announced first', async () => {
  const bot = await talk();
  const crashed = newGeneration(1_000);
  const restarted = newGeneration(2_000);
  // A crashed generation is still announced, and its subscriptions are never answered.
  bot.discovery().push({ type: 'announced', namespace: [...ROOT, crashed] });
  await until(() => bot.live(crashed, 'catalog').length === 1 && bot.live(crashed, 'audio').length === 1, 'crashed');

  // The restarted bot is announced: the page goes there at once rather than waiting out the dying one.
  const announcedAt = performance.now();
  bot.discovery().push({ type: 'announced', namespace: [...ROOT, restarted] });
  await until(
    () => bot.live(restarted, 'catalog').length === 1 && bot.live(restarted, 'audio').length === 1,
    'the restarted generation',
  );
  serve(bot.live(restarted, 'catalog')[0] as FakeSubscription, bot.live(restarted, 'audio')[0] as FakeSubscription);
  await until(() => bot.listening.length === 1, 'listening');
  // #471's page waited up to 5 s for a catalog on the dying generation before trying the next one.
  expect(performance.now() - announcedAt).toBeLessThan(1_000);
  await until(() => bot.live(restarted, INTERRUPT_TRACK).length === 1, 'the interrupt want');
  expect(bot.open().filter((key) => key.startsWith(crashed))).toEqual([]);
  expect(bot.open().every((key) => key.startsWith(restarted))).toBe(true);
  expect(bot.errors).toEqual([]);
});

it('moves the same listener to a restarted bot while the crashed generation is still announced', async () => {
  const bot = await talk();
  const crashed = newGeneration(1_000);
  const restarted = newGeneration(2_000);
  bot.discovery().push({ type: 'announced', namespace: [...ROOT, crashed] });
  await until(() => bot.live(crashed, 'audio').length === 1, 'the first generation');
  serve(bot.live(crashed, 'catalog')[0] as FakeSubscription, bot.live(crashed, 'audio')[0] as FakeSubscription);
  await until(() => bot.listening.length === 1, 'listening');

  // The bot crashed and came back as a newer generation; the old one is still announced, and still accepted.
  bot.discovery().push({ type: 'announced', namespace: [...ROOT, restarted] });
  await until(() => bot.live(restarted, 'audio').length === 1, 'the move to the restarted generation');
  serve(bot.live(restarted, 'catalog')[0] as FakeSubscription, bot.live(restarted, 'audio')[0] as FakeSubscription);
  await until(() => bot.restarts.length === 1, 'the restart reported');
  expect(bot.restarts).toEqual([{ reason: 'generation', namespace: [...ROOT, crashed] }]);
  expect(bot.listening).toHaveLength(1);
  expect(bot.live(crashed, 'audio')).toEqual([]);
  expect(bot.live(crashed, 'catalog')).toEqual([]);

  // The crashed generation's withdrawal moves nothing back.
  const made = bot.connection.subscriptions.length;
  bot.discovery().push({ type: 'unannounced', namespace: [...ROOT, crashed] });
  await new Promise((resolve) => setTimeout(resolve, 20));
  expect(bot.connection.subscriptions).toHaveLength(made);
  expect(bot.errors).toEqual([]);
});

it('ends on the newer generation after discovery restarts and replays the older one first', async () => {
  const bot = await talk();
  const crashed = newGeneration(1_000);
  const restarted = newGeneration(2_000);
  bot.discovery().push({ type: 'announced', namespace: [...ROOT, crashed] });
  bot.discovery().push({ type: 'announced', namespace: [...ROOT, restarted] });
  await until(() => bot.live(restarted, 'audio').length === 1, 'the newest generation');
  serve(bot.live(restarted, 'catalog')[0] as FakeSubscription, bot.live(restarted, 'audio')[0] as FakeSubscription);
  await until(() => bot.listening.length === 1, 'listening');

  // The relay ends discovery, and the new one replays the older generation in a turn of its own before the newer one.
  // The supervised subscriber restarts the want and can try the older generation in between (issue #482); the page
  // has no loop of its own left to stay there.
  bot.discovery().fail(undefined);
  await until(() => bot.connection.discoveries.length === 2, 'discovery again');
  bot.discovery().push({ type: 'announced', namespace: [...ROOT, crashed] });
  await new Promise((resolve) => setTimeout(resolve, 5));
  bot.discovery().push({ type: 'announced', namespace: [...ROOT, restarted] });
  await until(() => bot.live(restarted, 'audio').length === 1, 'the newest generation again');
  for (const track of ['catalog', 'audio']) for (const sub of bot.live(restarted, track)) sub.accept();
  speak(bot.live(restarted, 'audio')[0] as FakeSubscription, 1);
  await new Promise((resolve) => setTimeout(resolve, 20));
  expect(bot.open().filter((key) => key.startsWith(crashed))).toEqual([]);
  expect(bot.listening).toHaveLength(1);
});
