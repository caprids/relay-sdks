// Opt-in live probe of publishing-roots discovery (issue #422): which roots are publishing under a namespace prefix,
// from the subscriber's side, across publisher rotations, crashes, clean departures and the subscriber's own rotations
// and reconnects. Run after `pnpm build` and `pnpm --filter @caprid/relay-quic build:native`.
//
// Each root p0..p{N-1} is a child process (this script with --publisher-child) running a supervised publisher on
// [...prefix, id] with one track, `beat`, written every 100 ms. The subscriber runs here, in one of two views:
//   --view roots    SupervisedSubscriber.publishingRoots({ prefix }): additions and removals as the SDK reports them,
//                   logged as `joined` and `left`.
//   --view raw      connectRelaySubscriber + namespaceEvents on the prefix, unsupervised; the probe folds the
//                   generation namespaces into roots itself (a root joins on its first namespace and leaves on its
//                   last). Works without the `loaded` event, which it waits for only briefly.
// Every event is one JSON line on stdout with `t`, ms since the probe started; the last line is {"summary":{...}}.
// `duplicates` counts the joins and leaves the ground truth does not explain: a join for a root already present, a
// second join (no publisher here ever restarts), a leave for a root not present or whose publisher is still up,
// and any event for an id with no publisher. The exit code is 0 unless the probe itself fails.
//
// Flags:
//   --view roots|raw (default roots)        --publishers N (default 5)  --prefix a/b (default <uuid>/p)
//   --subscriber-after                       start the subscriber once every publisher is up (default: before)
//   --rotate-publishers K                    rotate every publisher K times, one at a time
//   --rotate-subscriber K                    SupervisedSubscriber.rotate() K times (roots view)
//   --reconnect-subscriber                   close the subscriber's QUIC transport and refuse dials for --outage-ms
//   --outage-ms MS (default 3000)            --depart-during-outage M   M publishers close during the outage
//   --kill-publisher M                       SIGKILL M publishers        --close-publisher M   M publishers close
//   --want-generations                       want [...prefix, 'p0'] track `beat` with generations: true
//   --latency --trials T (default 5)         T new publishers: generation-up to joined (roots) or announced (raw),
//                                            and a fresh subscriber's connect-to-loaded per trial
//   --settle-ms MS (default 15000)           how long to wait for each expected join or leave
//   --end close|keep (default close)         close every remaining publisher and wait for its leave, or not
//   --out PATH                               also write {argv, events, summary} as one JSON file
//
// Environment: RELAY_URL (a room-rooted endpoint), RELAY_SUB_URL (default RELAY_URL), RELAY_TOKEN or RELAY_ANONYMOUS=1,
// RELAY_CERTIFICATE_HASH (hex SHA-256 of a local relay's certificate).
//
//   RELAY_URL=https://ord.relay.mux.global/mattypoo node scripts/probe-publishing-roots.mjs --publishers 5 --rotate-publishers 3
import { fork } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { parseArgs } from 'node:util';
import { connectRelaySubscriber, connectSupervisedPublisher, connectSupervisedSubscriber } from '@caprid/relay/node';
import { nativeQuicTransport } from '@caprid/relay-quic';

const ROTATE_SETTLE_MS = 1000;
const RAW_LOADED_WAIT_MS = 3000;
const ROTATION_WAIT_MS = 15_000;
const OUTAGE_RETRY_MS = 250;
const BEAT_MS = 100;

const { values: flags } = parseArgs({
  options: {
    'publisher-child': { type: 'string' },
    view: { type: 'string', default: 'roots' },
    publishers: { type: 'string', default: '5' },
    prefix: { type: 'string' },
    'subscriber-after': { type: 'boolean', default: false },
    'rotate-publishers': { type: 'string', default: '0' },
    'rotate-subscriber': { type: 'string', default: '0' },
    'reconnect-subscriber': { type: 'boolean', default: false },
    'outage-ms': { type: 'string', default: '3000' },
    'depart-during-outage': { type: 'string', default: '0' },
    'kill-publisher': { type: 'string', default: '0' },
    'close-publisher': { type: 'string', default: '0' },
    'want-generations': { type: 'boolean', default: false },
    latency: { type: 'boolean', default: false },
    trials: { type: 'string', default: '5' },
    'settle-ms': { type: 'string', default: '15000' },
    end: { type: 'string', default: 'close' },
    out: { type: 'string' },
  },
});
const count = (name) => {
  const value = Number(flags[name]);
  if (!Number.isSafeInteger(value) || value < 0) throw new Error(`--${name} must be a whole number`);
  return value;
};

const env = process.env;
const url = env.RELAY_URL;
if (!url) throw new Error('RELAY_URL is required');
const subUrl = env.RELAY_SUB_URL ?? url;
const token = env.RELAY_TOKEN;
if (!token && env.RELAY_ANONYMOUS !== '1') throw new Error('Set RELAY_TOKEN, or RELAY_ANONYMOUS=1 for a local relay');
const certificateHash = env.RELAY_CERTIFICATE_HASH;
if (certificateHash && !/^[0-9a-f]{64}$/i.test(certificateHash)) throw new Error('RELAY_CERTIFICATE_HASH must be hex');
const trust = certificateHash
  ? { serverCertificateHashes: [{ algorithm: 'sha-256', value: Buffer.from(certificateHash, 'hex') }] }
  : {};
const getToken = token ? () => token : undefined;
const prefix = flags.prefix ? flags.prefix.split('/').filter(Boolean) : [randomUUID(), 'p'];
const describe = (error) => (error?.code ? `${error.code}: ${error.message}` : String(error));
const children = new Set();
const killChildren = () => {
  for (const child of children) child.kill('SIGKILL');
};

if (flags['publisher-child'] !== undefined) await runPublisher(flags['publisher-child']);
else {
  try {
    await runProbe();
  } catch (error) {
    console.error(JSON.stringify({ error: describe(error), stack: error?.stack }));
    killChildren();
    process.exit(1);
  }
}

async function runPublisher(id) {
  process.on('disconnect', () => process.exit(0));
  const at = () => performance.timeOrigin + performance.now();
  const send = (message, done) => (process.connected ? process.send(message, done) : done?.());
  const publisher = await connectSupervisedPublisher({
    url,
    getToken,
    ...trust,
    namespace: [...prefix, id],
    onGeneration: ({ generation }) => send({ type: 'generation-up', generation, at: at() }),
    onStatus: (status, error) => send({ type: 'status', status, ...(error ? { error: describe(error) } : {}) }),
  });
  const beat = publisher.registerTrack({ track: 'beat' });
  let closing = false;
  process.on('message', async ({ cmd }) => {
    if (cmd === 'rotate') publisher.rotate();
    if (cmd === 'close') {
      closing = true;
      await publisher.close();
      send({ type: 'closed', at: at() }, () => process.exit(0));
    }
  });
  for (let n = 0; !closing; n++) {
    if (beat.hasDemand) await beat.writeText(String(n)).catch(() => {});
    await sleep(BEAT_MS);
  }
}

async function runProbe() {
  const view = flags.view;
  if (!['roots', 'raw'].includes(view)) throw new Error('--view must be roots or raw');
  if (!['close', 'keep'].includes(flags.end)) throw new Error('--end must be close or keep');
  const publisherCount = count('publishers');
  const rotatePublishers = count('rotate-publishers');
  const rotateSubscriber = count('rotate-subscriber');
  const outageMs = count('outage-ms');
  const departDuringOutage = count('depart-during-outage');
  const killCount = count('kill-publisher');
  const closeCount = count('close-publisher');
  const trials = flags.latency ? count('trials') : 0;
  const settleMs = count('settle-ms');
  const loadedWaitMs = view === 'raw' ? RAW_LOADED_WAIT_MS : settleMs;
  if (view === 'raw' && (rotateSubscriber || flags['reconnect-subscriber'])) {
    throw new Error('--rotate-subscriber and --reconnect-subscriber need --view roots: the raw view is unsupervised');
  }
  if (departDuringOutage && !flags['reconnect-subscriber']) {
    throw new Error('--depart-during-outage needs --reconnect-subscriber');
  }
  if (departDuringOutage + killCount + closeCount > publisherCount)
    throw new Error('More departures than --publishers');
  if (flags['want-generations'] && publisherCount < 1) throw new Error('--want-generations needs a publisher p0');

  const start = performance.now();
  const sinceStart = (ms) => Math.round((ms - start) * 10) / 10;
  const tOfAbsolute = (abs) => sinceStart(abs - performance.timeOrigin);
  const events = [];
  const waiters = new Set();
  const emit = (type, fields = {}, t = sinceStart(performance.now())) => {
    const event = { t, type, ...fields };
    events.push(event);
    process.stdout.write(`${JSON.stringify(event)}\n`);
    for (const waiter of [...waiters]) waiter(event);
    return event;
  };
  const waitFor = (match, timeoutMs, from = 0) => {
    const past = events.slice(from).find(match);
    if (past) return Promise.resolve(past);
    return new Promise((resolve) => {
      const done = (event) => {
        clearTimeout(timer);
        waiters.delete(waiter);
        resolve(event);
      };
      const waiter = (event) => match(event) && done(event);
      const timer = setTimeout(() => done(undefined), timeoutMs);
      waiters.add(waiter);
    });
  };
  const expect = async (what, match, timeoutMs, from) => {
    const event = await waitFor(match, timeoutMs, from);
    if (!event) emit('timeout', { waitingFor: what, afterMs: timeoutMs });
    return event;
  };

  const publishers = new Map();
  let nextIndex = 0;
  const startPublisher = () => {
    const id = `p${nextIndex++}`;
    const child = fork(process.argv[1], ['--publisher-child', id, '--prefix', prefix.join('/')], {
      stdio: ['ignore', 'ignore', 'inherit', 'ipc'],
    });
    const publisher = { id, child, generations: [], departure: undefined, exited: false };
    publishers.set(id, publisher);
    children.add(child);
    child.on('message', (message) => {
      if (message.type === 'generation-up') {
        publisher.generations.push(message.generation);
        emit(
          'publisher-up',
          { id, generation: message.generation, first: publisher.generations.length === 1 },
          tOfAbsolute(message.at),
        );
      } else if (message.type === 'status')
        emit('publisher-status', { id, status: message.status, error: message.error });
      else if (message.type === 'closed') emit('publisher-closed', { id }, tOfAbsolute(message.at));
    });
    child.on('exit', (code, signal) => {
      publisher.exited = true;
      children.delete(child);
      if (!publisher.departure) emit('publisher-exit', { id, code, signal });
    });
    return publisher;
  };
  const alive = () => [...publishers.values()].filter((publisher) => !publisher.departure);
  const pickDepartures = (n) => alive().reverse().slice(0, n);
  const depart = (publisher, kind) => {
    publisher.departure = { kind, from: events.length };
    publisher.departure.event = emit(kind === 'kill' ? 'kill' : 'close', { id: publisher.id, kind });
    if (kind === 'kill') publisher.child.kill('SIGKILL');
    else publisher.child.send({ cmd: 'close' });
  };
  const awaitLeft = (departed) =>
    Promise.all(
      departed.map((publisher) =>
        expect(
          `left ${publisher.id}`,
          (e) => e.type === 'left' && e.id === publisher.id,
          settleMs,
          publisher.departure.from,
        ),
      ),
    );

  const present = new Set();
  const everJoined = new Set();
  const counts = { joined: 0, left: 0 };
  const duplicates = [];
  const observe = (type, id, fields = {}) => {
    const publisher = publishers.get(id);
    let unexplained;
    if (!publisher) unexplained = 'no such publisher';
    else if (type === 'joined' && present.has(id)) unexplained = 'already present';
    else if (type === 'joined' && everJoined.has(id)) unexplained = 'joined again';
    else if (type === 'left' && !present.has(id)) unexplained = 'not present';
    else if (type === 'left' && !publisher.departure) unexplained = 'publisher still up';
    counts[type] += 1;
    if (type === 'joined') {
      present.add(id);
      everJoined.add(id);
    } else present.delete(id);
    const event = emit(type, { id, view, ...fields, ...(unexplained ? { unexplained } : {}) });
    if (unexplained) duplicates.push(event);
  };

  process.on('exit', killChildren);
  for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => process.exit(130));

  let outageUntil = 0;
  const openView = async (sink, { primary = false } = {}) => {
    const dial = nativeQuicTransport(trust);
    const transports = [];
    const createTransport = (transportUrl, protocols) => {
      if (primary && performance.now() < outageUntil) {
        emit('subscriber-dial', { refused: true });
        throw new Error('probe-publishing-roots outage: dial refused');
      }
      const opened = dial(transportUrl, protocols);
      transports.push(opened);
      if (primary) emit('subscriber-dial', { connection: transports.length });
      return opened;
    };
    const connectAt = performance.now();
    let resolveLoaded;
    const loaded = new Promise((resolve) => {
      resolveLoaded = (ids) =>
        resolve({ ids, connectToLoadedMs: Math.round((performance.now() - connectAt) * 10) / 10 });
    });
    if (view === 'roots') {
      const supervised = await connectSupervisedSubscriber({
        url: subUrl,
        getToken,
        createTransport,
        ...(primary && flags['reconnect-subscriber']
          ? { reconnect: { initialDelayMs: OUTAGE_RETRY_MS, maxDelayMs: OUTAGE_RETRY_MS } }
          : {}),
        onStatus: (status, error) => primary && emit('subscriber-status', { status, error: error && describe(error) }),
      });
      if (typeof supervised.publishingRoots !== 'function') {
        supervised.close();
        throw new Error('SupervisedSubscriber.publishingRoots() is not in this build; use --view raw');
      }
      const rootsView = supervised.publishingRoots(
        { prefix },
        {
          onChanged({ added, removed, initial }) {
            if (initial) {
              const ids = added.map((root) => root.id);
              resolveLoaded(ids);
              sink.loaded?.(ids);
            }
            for (const root of added) sink.joined?.(root.id, initial ? { initial: true } : {});
            for (const root of removed) sink.left?.(root.id);
          },
          onError: (error) => sink.error?.(error),
        },
      );
      return {
        supervised,
        transports,
        loaded,
        close: () => {
          rootsView.close();
          supervised.close();
        },
      };
    }
    const subscriber = await connectRelaySubscriber({ url: subUrl, getToken, createTransport });
    const namespaces = subscriber.namespaceEvents({ prefix });
    const generations = new Map();
    void (async () => {
      for await (const event of namespaces) {
        if (event.type === 'loaded') {
          const ids = [...generations.keys()];
          resolveLoaded(ids);
          sink.loaded?.(ids);
          continue;
        }
        const [id, generation, ...rest] = event.namespace.slice(prefix.length);
        if (id === undefined || rest.length > 0) continue;
        sink.raw?.(event.type, id, generation);
        const held = generations.get(id) ?? new Set();
        if (event.type === 'announced') {
          held.add(generation ?? '');
          generations.set(id, held);
          if (held.size === 1) sink.joined?.(id);
        } else if (held.delete(generation ?? '') && held.size === 0) {
          generations.delete(id);
          sink.left?.(id);
        }
      }
    })().catch((error) => sink.error?.(error));
    return {
      transports,
      loaded,
      close: () => {
        namespaces.close();
        subscriber.close();
      },
    };
  };

  const mainSink = {
    loaded: (ids) => emit('loaded', { view, roots: ids }),
    joined: (id, fields) => observe('joined', id, fields),
    left: (id) => observe('left', id),
    raw: (type, id, generation) => emit(type, { id, generation }),
    error: (error) => emit('subscriber-error', { error: describe(error) }),
  };
  let main;
  let connectToLoadedMs = null;
  let loadedRoots = null;
  const startSubscriber = async () => {
    main = await openView(mainSink, { primary: true });
    const loaded = await Promise.race([main.loaded, sleep(loadedWaitMs).then(() => undefined)]);
    if (!loaded) emit('loaded-missing', { view, afterMs: loadedWaitMs });
    else ({ ids: loadedRoots, connectToLoadedMs } = loaded);
  };
  const startPublishers = async (n) => {
    const started = Array.from({ length: n }, () => ({ publisher: startPublisher(), from: events.length }));
    await Promise.all(
      started.map(({ publisher, from }) =>
        expect(`publisher-up ${publisher.id}`, (e) => e.type === 'publisher-up' && e.id === publisher.id, 30_000, from),
      ),
    );
    return started;
  };
  const awaitJoined = (started) =>
    Promise.all(
      started.map(({ publisher, from }) =>
        expect(`joined ${publisher.id}`, (e) => e.type === 'joined' && e.id === publisher.id, settleMs, from),
      ),
    );

  if (!flags['subscriber-after']) await startSubscriber();
  const initial = await startPublishers(publisherCount);
  if (flags['subscriber-after']) await startSubscriber();
  await awaitJoined(initial);

  const want = { objects: 0, subscribed: 0, switched: 0, restarted: 0, errors: 0 };
  let wantSubscriber;
  if (flags['want-generations']) {
    wantSubscriber = main.supervised ?? (await connectSupervisedSubscriber({ url: subUrl, getToken, ...trust }));
    wantSubscriber.want(
      'beat',
      { namespace: [...prefix, 'p0'], track: 'beat', generations: true },
      {
        onObject: () => {
          want.objects += 1;
        },
        onSubscribed: (_info, { namespace }) => {
          want.subscribed += 1;
          emit('want-subscribed', { generation: namespace.at(-1), objects: want.objects });
        },
        onSwitched: ({ from, to }) => {
          want.switched += 1;
          emit('want-switched', { from: from.namespace.at(-1), to: to.namespace.at(-1), objects: want.objects });
        },
        onRestarted: ({ reason, namespace }) => {
          want.restarted += 1;
          emit('want-restarted', { reason, generation: namespace.at(-1), objects: want.objects });
        },
        onError: (error) => {
          want.errors += 1;
          emit('want-error', { error: describe(error) });
        },
      },
    );
    await expect('want-subscribed', (e) => e.type === 'want-subscribed', settleMs);
  }

  let publisherRotations = 0;
  for (let round = 1; round <= rotatePublishers; round++) {
    for (const publisher of alive()) {
      const from = events.length;
      const rotate = emit('rotate', { id: publisher.id, round });
      publisher.child.send({ cmd: 'rotate' });
      const up = await expect(
        `rotation of ${publisher.id}`,
        (e) => e.type === 'publisher-up' && e.id === publisher.id,
        ROTATION_WAIT_MS,
        from,
      );
      if (up) publisherRotations += 1;
      if (up) emit('rotated', { id: publisher.id, round, rotateMs: Math.round((up.t - rotate.t) * 10) / 10 });
      await sleep(ROTATE_SETTLE_MS);
    }
  }

  const subscriberRotations = [];
  for (let n = 1; n <= rotateSubscriber; n++) {
    const old = main.transports.at(-1);
    const from = events.length;
    const rotate = emit('subscriber-rotate', { n });
    main.supervised.rotate();
    const dialed = await expect(
      'subscriber rotation dial',
      (e) => e.type === 'subscriber-dial',
      ROTATION_WAIT_MS,
      from,
    );
    const elapsed = () => Math.round((performance.now() - start - rotate.t) * 10) / 10;
    const settle = (promise) =>
      Promise.race([promise.then(elapsed, elapsed), sleep(ROTATION_WAIT_MS).then(() => null)]);
    const newUpMs = dialed ? await settle(main.transports.at(-1).ready) : null;
    const oldClosedMs = await settle(old.transport.closed);
    subscriberRotations.push({ n, newUpMs, oldClosedMs });
    emit('subscriber-rotated', { n, newUpMs, oldClosedMs });
    await sleep(ROTATE_SETTLE_MS);
  }

  let reconnect = null;
  if (flags['reconnect-subscriber']) {
    const from = events.length;
    outageUntil = performance.now() + outageMs;
    const cut = emit('subscriber-reconnect', { phase: 'cut', outageMs });
    main.transports.at(-1).transport.close({ closeCode: 0, reason: 'probe-publishing-roots outage' });
    const departed = pickDepartures(departDuringOutage);
    for (const publisher of departed) depart(publisher, 'outage-close');
    const live = await expect(
      'subscriber live after outage',
      (e) => e.type === 'subscriber-status' && e.status === 'live',
      outageMs + settleMs,
      from,
    );
    reconnect = { outageMs: live ? Math.round((live.t - cut.t) * 10) / 10 : null };
    emit('subscriber-reconnect', { phase: 'live', ...reconnect });
    await awaitLeft(departed);
  }

  const killed = pickDepartures(killCount);
  for (const publisher of killed) depart(publisher, 'kill');
  await awaitLeft(killed);
  const closed = pickDepartures(closeCount);
  for (const publisher of closed) depart(publisher, 'close');
  await awaitLeft(closed);

  const latencyTrials = [];
  for (let trial = 1; trial <= trials; trial++) {
    const [started] = await startPublishers(1);
    const { publisher, from } = started;
    const up = events.slice(from).find((e) => e.type === 'publisher-up' && e.id === publisher.id);
    const seenType = view === 'roots' ? 'joined' : 'announced';
    const seen = await expect(
      `${seenType} ${publisher.id}`,
      (e) => e.type === seenType && e.id === publisher.id,
      settleMs,
      from,
    );
    const fresh = await openView({ error: (error) => emit('trial-subscriber-error', { error: describe(error) }) });
    const loaded = await Promise.race([fresh.loaded, sleep(loadedWaitMs).then(() => undefined)]);
    fresh.close();
    const result = {
      trial,
      id: publisher.id,
      joinMs: up && seen ? Math.round((seen.t - up.t) * 10) / 10 : null,
      connectToLoadedMs: loaded?.connectToLoadedMs ?? null,
      loadedHasAll: loaded ? alive().every(({ id }) => loaded.ids.includes(id)) : null,
    };
    latencyTrials.push(result);
    emit('latency-trial', result);
  }

  let ended = [];
  if (flags.end === 'close') {
    ended = alive();
    for (const publisher of ended) depart(publisher, 'end-close');
    await awaitLeft(ended);
  }

  const departures = [...publishers.values()]
    .filter((publisher) => publisher.departure)
    .sort((a, b) => a.departure.event.t - b.departure.event.t)
    .map(({ id, departure }) => {
      const left = events.slice(departure.from).find((e) => e.type === 'left' && e.id === id);
      return {
        id,
        kind: departure.kind,
        departToLeftMs: left ? Math.round((left.t - departure.event.t) * 10) / 10 : null,
      };
    });
  const stats = (values) => {
    const sorted = values.filter((value) => value !== null).sort((a, b) => a - b);
    const rank = (p) => sorted[Math.min(sorted.length - 1, Math.ceil(p * sorted.length) - 1)] ?? null;
    return { n: sorted.length, median: rank(0.5), p95: rank(0.95) };
  };
  const summary = {
    view,
    prefix,
    joined: counts.joined,
    left: counts.left,
    duplicates: duplicates.length,
    duplicateEvents: duplicates,
    loadedRoots,
    connectToLoadedMs,
    departToLeftMs: departures.map(({ departToLeftMs }) => departToLeftMs),
    departures,
    missingLeft: departures.filter(({ departToLeftMs }) => departToLeftMs === null).map(({ id }) => id),
    presentAtEnd: [...present],
    expectedAtEnd: alive().map(({ id }) => id),
    publisherRotations,
    subscriberRotations,
    reconnect,
    ...(flags['want-generations'] ? { want } : {}),
    ...(view === 'raw'
      ? {
          raw: {
            announced: events.filter((e) => e.type === 'announced').length,
            unannounced: events.filter((e) => e.type === 'unannounced').length,
            loadedSeen: events.some((e) => e.type === 'loaded'),
          },
        }
      : {}),
    ...(flags.latency
      ? {
          latency: {
            trials: latencyTrials,
            joinMs: stats(latencyTrials.map((trial) => trial.joinMs)),
            connectToLoadedMs: stats(latencyTrials.map((trial) => trial.connectToLoadedMs)),
          },
        }
      : {}),
    errors: events.filter((e) => /error|timeout|missing|exit$/.test(e.type)).length,
  };

  if (wantSubscriber && wantSubscriber !== main.supervised) wantSubscriber.close();
  main.close();
  killChildren();
  process.stdout.write(`${JSON.stringify({ summary })}\n`);
  if (flags.out) {
    await mkdir(dirname(flags.out), { recursive: true });
    await writeFile(flags.out, `${JSON.stringify({ argv: process.argv.slice(2), events, summary }, null, 2)}\n`);
  }
  process.exit(0);
}
