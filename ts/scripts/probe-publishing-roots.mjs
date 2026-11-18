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
// Presence (the media presence helper): with --presence each publisher runs publishPresence with the document
// { name: 'Member <id>', joinedAt, host } instead of the `beat` track, and every listener is a presence view on the
// prefix. Its events are presence-loaded (members with documents), presence-joined (initial: true at the load),
// presence-left, presence-updated, presence-stale and presence-resumed, each tagged with its listener: node0 (the
// primary, which --rotate-subscriber and --reconnect-subscriber act on), node1.., and browser. `duplicates` then
// counts the presence events the ground truth does not explain: a join for a member already present or with a document
// its publisher never sent, an update to anything but the current document, a stale for a publisher neither frozen nor
// killed, a resume for one still frozen, a leave for one still up or resumed in time. Before --end, the probe waits for every listener's roster to
// match the publishers and reports each roster and host set. After --rotate-publishers or --rotate-subscriber, every
// publisher changes its name once and every listener must report it within 2 s (`rotationUpdates`), since a want lost
// in a rotation keeps its cached document until the stale window runs out.
//   --presence                               presence mode
//   --presence-view helper|hand (default helper)  hand: connectRelaySubscriber + namespaceEvents, then a subscribe to
//                                            each announced generation's `presence` track until its first object
//   --listeners N (default 1)                N presence views, each on its own supervised subscriber
//   --browser                                one more listener in headless Chromium over WebTransport
//   --host IDS                               comma list of ids whose document says host: true
//   --update-member M                        after the load, M publishers change their name once
//   --freeze-publisher M --freeze-ms MS      after the load and a heartbeat, SIGSTOP M publishers and SIGCONT them
//                                            after MS (default 0: never, so the relay's idle timeout withdraws them);
//                                            a listener that reported one stale must then report it resumed
//   With --latency, joinMs runs from generation-up to the primary listener's presence-joined.
//
// Environment: RELAY_URL (a room-rooted endpoint), RELAY_SUB_URL (default RELAY_URL), RELAY_TOKEN or RELAY_ANONYMOUS=1,
// RELAY_CERTIFICATE_HASH (hex SHA-256 of a local relay's certificate).
//
//   RELAY_URL=https://ord.relay.mux.global/mattypoo node scripts/probe-publishing-roots.mjs --publishers 5 --rotate-publishers 3
//   RELAY_URL=... node scripts/probe-publishing-roots.mjs --presence --publishers 3 --host p0 --listeners 2 --browser
import { fork } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { connectRelaySubscriber, connectSupervisedPublisher, connectSupervisedSubscriber } from '@caprid/relay/node';
import { nativeQuicTransport } from '@caprid/relay-quic';
import { startModuleServer } from './module-server.js';

const ROTATE_SETTLE_MS = 1000;
const RAW_LOADED_WAIT_MS = 3000;
const ROTATION_WAIT_MS = 15_000;
const OUTAGE_RETRY_MS = 250;
const BEAT_MS = 100;
const PRIMARY = 'node0';
const PRESENCE_KINDS = ['joined', 'updated', 'resumed', 'stale', 'left'];
const PRESENCE_SUBSCRIBER_PRIORITY = 32;
const FROZEN_LEFT_WAIT_MS = 30_000;
const LATE_LEFT_WATCH_MS = 5000;
const CONVERGE_POLL_MS = 100;
const ROTATION_UPDATE_MS = 2000;

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
    presence: { type: 'boolean', default: false },
    'presence-document': { type: 'string' },
    'presence-view': { type: 'string', default: 'helper' },
    listeners: { type: 'string', default: '1' },
    browser: { type: 'boolean', default: false },
    host: { type: 'string', default: '' },
    'update-member': { type: 'string', default: '0' },
    'freeze-publisher': { type: 'string', default: '0' },
    'freeze-ms': { type: 'string', default: '0' },
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
const redact = (value) => (token ? String(value).replaceAll(token, '[redacted]') : String(value));
const describe = (error) => redact(error?.code ? `${error.code}: ${error.message}` : String(error));
const children = new Set();
const killChildren = () => {
  for (const child of children) child.kill('SIGKILL');
};

// A presence change as plain JSON, the same from Node and from the browser page (which receives this source).
function plainChange(change) {
  const entry = ({ id, document, stale, since }) => ({ id, document, stale, since });
  return {
    initial: change.initial,
    joined: change.joined.map(entry),
    updated: change.updated.map(entry),
    resumed: change.resumed.map(entry),
    stale: change.stale.map(entry),
    left: change.left.map(entry),
    present: [...change.present.values()].map(entry),
  };
}

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
  const presenceDocument = flags['presence-document'];
  const me =
    presenceDocument === undefined
      ? undefined
      : (await import('@caprid/relay/media')).publishPresence(publisher, JSON.parse(presenceDocument));
  const beat = me ? undefined : publisher.registerTrack({ track: 'beat' });
  let closing = false;
  process.on('message', async ({ cmd, document }) => {
    if (cmd === 'update') me.update(document);
    if (cmd === 'rotate') publisher.rotate();
    if (cmd === 'close') {
      closing = true;
      await publisher.close();
      send({ type: 'closed', at: at() }, () => process.exit(0));
    }
  });
  for (let n = 0; beat && !closing; n++) {
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
  const presence = flags.presence;
  const presenceView = flags['presence-view'];
  const listenerCount = count('listeners');
  const hosts = new Set(flags.host.split(',').filter(Boolean));
  const updateCount = count('update-member');
  const freezeCount = count('freeze-publisher');
  const freezeMs = count('freeze-ms');
  const unsupervised = presence ? presenceView === 'hand' : view === 'raw';
  const loadedWaitMs = unsupervised ? RAW_LOADED_WAIT_MS : settleMs;
  if (!['helper', 'hand'].includes(presenceView)) throw new Error('--presence-view must be helper or hand');
  if (presence && view !== 'roots') throw new Error('--presence replaces --view');
  const presenceOnly = ['presence-view', 'listeners', 'browser', 'host', 'update-member', 'freeze-publisher'];
  if (!presence && presenceOnly.some((name) => process.argv.includes(`--${name}`))) {
    throw new Error(`${presenceOnly.map((name) => `--${name}`).join(', ')} need --presence`);
  }
  if (presence && flags['want-generations']) throw new Error('--want-generations wants `beat`, which --presence drops');
  if (listenerCount < 1) throw new Error('--listeners must be at least 1');
  if (flags.browser && certificateHash) throw new Error('--browser cannot trust RELAY_CERTIFICATE_HASH');
  if (unsupervised && (rotateSubscriber || flags['reconnect-subscriber'])) {
    throw new Error(
      '--rotate-subscriber and --reconnect-subscriber need a supervised view: the raw and hand views are not',
    );
  }
  const media = presence ? await import('@caprid/relay/media') : undefined;
  if (presence && typeof media.createPresenceView !== 'function') {
    throw new Error('createPresenceView is not in this build of @caprid/relay/media');
  }
  const presenceTrack = media?.PRESENCE_TRACK ?? 'presence';
  const heartbeatMs = media?.PRESENCE_HEARTBEAT_MS ?? 2000;
  if (departDuringOutage && !flags['reconnect-subscriber']) {
    throw new Error('--depart-during-outage needs --reconnect-subscriber');
  }
  if (departDuringOutage + killCount + closeCount + freezeCount > publisherCount)
    throw new Error('More departures than --publishers');
  if (updateCount > publisherCount) throw new Error('More updates than --publishers');
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
  const JOINED = presence ? 'presence-joined' : 'joined';
  const LEFT = presence ? 'presence-left' : 'left';
  const primaryEvent = (type, id) => (e) => e.type === type && e.id === id && (!presence || e.listener === PRIMARY);
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
    const document = presence ? { name: `Member ${id}`, joinedAt: Date.now(), host: hosts.has(id) } : undefined;
    const args = ['--publisher-child', id, '--prefix', prefix.join('/')];
    if (presence) args.push('--presence-document', JSON.stringify(document));
    const child = fork(process.argv[1], args, { stdio: ['ignore', 'ignore', 'inherit', 'ipc'] });
    const publisher = {
      id,
      child,
      generations: [],
      departure: undefined,
      exited: false,
      document,
      sent: [document],
      freeze: undefined,
    };
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
          primaryEvent(LEFT, publisher.id),
          publisher.departure.kind === 'freeze' ? Math.max(settleMs, FROZEN_LEFT_WAIT_MS) : settleMs,
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

  const rosters = new Map();
  const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
  const frozen = (publisher) => publisher.freeze !== undefined && publisher.freeze.resumed === undefined;
  const explainPresence = (kind, entry, held, publisher) => {
    if (!publisher) return 'no such publisher';
    if (kind === 'joined') {
      if (held) return 'already present';
      return publisher.sent.some((document) => same(entry.document, document)) ? undefined : 'never sent';
    }
    if (!held) return 'not present';
    if (kind === 'updated') {
      if (same(entry.document, held.document)) return 'no change';
      return same(entry.document, publisher.document) ? undefined : 'not the current document';
    }
    if (kind === 'stale') {
      if (held.stale) return 'already stale';
      return frozen(publisher) || publisher.departure?.kind === 'kill' ? undefined : 'publisher not frozen';
    }
    if (kind === 'resumed') {
      if (!held.stale) return 'not stale';
      return publisher.freeze?.resumed ? undefined : 'publisher still frozen';
    }
    if (publisher.departure || frozen(publisher)) return undefined;
    return publisher.freeze ? 'publisher resumed' : 'publisher still up';
  };
  const deliverPresence = (listener, change) => {
    const roster = rosters.get(listener) ?? new Map();
    if (change.initial) {
      emit('presence-loaded', {
        listener,
        members: change.joined.map(({ id, document, stale }) => ({ id, document, stale })),
      });
    }
    for (const kind of PRESENCE_KINDS) {
      for (const entry of change[kind]) {
        const unexplained = explainPresence(kind, entry, roster.get(entry.id), publishers.get(entry.id));
        const event = emit(`presence-${kind}`, {
          listener,
          id: entry.id,
          ...(['joined', 'updated'].includes(kind) ? { document: entry.document } : { stale: entry.stale }),
          ...(change.initial ? { initial: true } : {}),
          ...(unexplained ? { unexplained } : {}),
        });
        if (unexplained) duplicates.push(event);
      }
    }
    rosters.set(listener, new Map(change.present.map((entry) => [entry.id, entry])));
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
    const connectSupervised = () =>
      connectSupervisedSubscriber({
        url: subUrl,
        getToken,
        createTransport,
        ...(primary && flags['reconnect-subscriber']
          ? { reconnect: { initialDelayMs: OUTAGE_RETRY_MS, maxDelayMs: OUTAGE_RETRY_MS } }
          : {}),
        onStatus: (status, error) => primary && emit('subscriber-status', { status, error: error && describe(error) }),
      });
    if (presence && presenceView === 'hand')
      return openHandPresence(createTransport, transports, sink, resolveLoaded, loaded);
    if (presence) {
      const supervised = await connectSupervised();
      const room = media.createPresenceView(
        supervised,
        { prefix },
        {
          onChanged(change) {
            if (change.initial) resolveLoaded(change.joined.map((member) => member.id));
            sink.change?.(plainChange(change));
          },
          onError: (error) => sink.error?.(error),
        },
      );
      return {
        supervised,
        transports,
        loaded,
        close: () => {
          room.close();
          supervised.close();
        },
      };
    }
    if (view === 'roots') {
      const supervised = await connectSupervised();
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

  // The hand-built path a presence view replaces: discovery, then each announced generation's `presence` track until
  // its first object. A member joins on its first document and leaves with its last generation.
  const openHandPresence = async (createTransport, transports, sink, resolveLoaded, loaded) => {
    const subscriber = await connectRelaySubscriber({ url: subUrl, getToken, createTransport });
    const namespaces = subscriber.namespaceEvents({ prefix });
    const generations = new Map();
    const present = new Map();
    let isLoaded = false;
    const report = (change) =>
      sink.change?.({
        initial: false,
        joined: [],
        updated: [],
        resumed: [],
        stale: [],
        left: [],
        ...change,
        present: [...present.values()],
      });
    const readFirst = (namespace, id) => {
      const subscription = subscriber.subscribe(
        { namespace, track: presenceTrack, parameters: { subscriberPriority: PRESENCE_SUBSCRIBER_PRIORITY } },
        {
          onObject: ({ payload }) => {
            subscription.cancel();
            if (present.has(id) || !generations.get(id)?.has(namespace.at(-1))) return;
            const document = JSON.parse(new TextDecoder().decode(payload));
            const entry = { id, document, stale: false, since: Date.now() };
            present.set(id, entry);
            if (isLoaded) report({ joined: [entry] });
          },
        },
      );
      subscription.subscribed.catch(() => {});
    };
    void (async () => {
      for await (const event of namespaces) {
        if (event.type === 'loaded') {
          isLoaded = true;
          resolveLoaded([...present.keys()]);
          report({ initial: true, joined: [...present.values()] });
          continue;
        }
        const [id, generation, ...rest] = event.namespace.slice(prefix.length);
        if (id === undefined || generation === undefined || rest.length > 0) continue;
        const held = generations.get(id) ?? new Set();
        if (event.type === 'announced') {
          held.add(generation);
          generations.set(id, held);
          readFirst(event.namespace, id);
        } else if (held.delete(generation) && held.size === 0) {
          generations.delete(id);
          const entry = present.get(id);
          present.delete(id);
          if (entry && isLoaded) report({ left: [entry] });
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

  // The browser listener: a presence view in headless Chromium over WebTransport, reporting plain changes back.
  const openBrowserPresence = async (sink) => {
    const base = resolve(dirname(fileURLToPath(import.meta.url)), '..');
    const server = await startModuleServer({
      modules: { '/sdk/': resolve(base, 'dist'), '/packages/': resolve(base, 'packages') },
      importMap: {
        '@caprid/relay': '/sdk/index.js',
        '@caprid/relay/media': '/sdk/media.js',
        '@caprid/moq-core': '/packages/moq-core/dist/index.js',
        '@caprid/moq-media': '/packages/moq-media/dist/index.js',
      },
    });
    const { chromium } = await import('playwright');
    const browser = await chromium.launch({ headless: true });
    const close = async () => {
      await browser.close();
      await new Promise((done) => server.close(done));
    };
    let resolveLoaded;
    const loaded = new Promise((done) => {
      resolveLoaded = done;
    });
    try {
      const page = await browser.newPage();
      page.on('pageerror', (error) => sink.error?.(error));
      await page.exposeFunction('reportPresence', (change) => {
        if (change.initial) resolveLoaded({ ids: change.joined.map(({ id }) => id) });
        sink.change?.(change);
      });
      await page.exposeFunction('reportBrowser', (fields) =>
        emit('browser-status', Object.fromEntries(Object.entries(fields).map(([key, value]) => [key, redact(value)]))),
      );
      await page.goto(`http://127.0.0.1:${server.address().port}/`);
      await page.evaluate(`window.plainChange = ${plainChange.toString()}`);
      await page.evaluate(
        async ({ url, token, prefix }) => {
          const { connectSupervisedSubscriber } = await import('/sdk/index.js');
          const { createPresenceView } = await import('/sdk/media.js');
          const subscriber = await connectSupervisedSubscriber({
            url,
            ...(token ? { getToken: () => token } : {}),
            onStatus: (status, error) =>
              window.reportBrowser({ status, ...(error ? { error: String(error?.message ?? error) } : {}) }),
          });
          createPresenceView(
            subscriber,
            { prefix },
            {
              onChanged: (change) => window.reportPresence(window.plainChange(change)),
              onError: (error) => window.reportBrowser({ error: String(error?.message ?? error) }),
            },
          );
        },
        { url: subUrl, token, prefix },
      );
    } catch (error) {
      await close();
      throw error;
    }
    return { loaded, close };
  };

  const presenceSink = (listener) => ({
    change: (change) => deliverPresence(listener, change),
    error: (error) => emit('subscriber-error', { listener, error: describe(error) }),
  });
  const mainSink = presence
    ? presenceSink(PRIMARY)
    : {
        loaded: (ids) => emit('loaded', { view, roots: ids }),
        joined: (id, fields) => observe('joined', id, fields),
        left: (id) => observe('left', id),
        raw: (type, id, generation) => emit(type, { id, generation }),
        error: (error) => emit('subscriber-error', { error: describe(error) }),
      };
  let main;
  let others = [];
  const listeners = [];
  let connectToLoadedMs = null;
  let loadedRoots = null;
  const waitLoaded = async (opened, listener) => {
    const loaded = await Promise.race([opened.loaded, sleep(loadedWaitMs).then(() => undefined)]);
    if (!loaded) emit('loaded-missing', { view, ...(presence ? { listener } : {}), afterMs: loadedWaitMs });
    return loaded;
  };
  const startSubscriber = async () => {
    emit('subscriber-start');
    const opening = [openView(mainSink, { primary: true })];
    for (let n = 1; n < listenerCount; n++) opening.push(openView(presenceSink(`node${n}`)));
    const browser = flags.browser
      ? openBrowserPresence(presenceSink('browser')).catch((error) => {
          emit('browser-error', { error: describe(error) });
          return undefined;
        })
      : undefined;
    [main, ...others] = await Promise.all(opening);
    listeners.push(...opening.map((_, n) => `node${n}`));
    const loaded = await waitLoaded(main, PRIMARY);
    if (loaded) ({ ids: loadedRoots, connectToLoadedMs } = loaded);
    await Promise.all(others.map((other, n) => waitLoaded(other, `node${n + 1}`)));
    if (browser) {
      const opened = await browser;
      if (opened) {
        others.push(opened);
        listeners.push('browser');
        await waitLoaded(opened, 'browser');
      }
    }
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
        presence
          ? Promise.all(
              listeners.map((listener) =>
                expect(
                  `joined ${publisher.id} at ${listener}`,
                  (e) => e.type === JOINED && e.id === publisher.id && e.listener === listener,
                  settleMs,
                  from,
                ),
              ),
            )
          : expect(`joined ${publisher.id}`, primaryEvent(JOINED, publisher.id), settleMs, from),
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

  const sendUpdates = async (targets, label, timeoutMs) => {
    const sent = targets.map((publisher) => {
      const document = { ...publisher.document, name: `${publisher.document.name} (${label})` };
      publisher.document = document;
      publisher.sent.push(document);
      const from = events.length;
      const event = emit('update-sent', { id: publisher.id, document });
      publisher.child.send({ cmd: 'update', document });
      return { publisher, from, sent: event };
    });
    await Promise.all(
      sent.flatMap(({ publisher, from }) =>
        listeners.map((listener) =>
          expect(
            `updated ${publisher.id} at ${listener}`,
            (e) => e.type === 'presence-updated' && e.id === publisher.id && e.listener === listener,
            timeoutMs,
            from,
          ),
        ),
      ),
    );
    return sent;
  };

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
  const rotationUpdates =
    presence && (rotatePublishers || rotateSubscriber)
      ? await sendUpdates(alive(), 'after rotation', ROTATION_UPDATE_MS)
      : [];

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

  const updates = await sendUpdates(alive().slice(0, updateCount), 'renamed', settleMs);

  const frozenPublishers = [];
  if (freezeCount) {
    await sleep(heartbeatMs);
    for (const publisher of alive().reverse().slice(0, freezeCount)) {
      const from = events.length;
      const event = emit('freeze', { id: publisher.id, freezeMs });
      publisher.child.kill('SIGSTOP');
      publisher.freeze = { from, event, resumed: undefined };
      if (!freezeMs) publisher.departure = { kind: 'freeze', from, event };
      frozenPublishers.push(publisher);
    }
    if (freezeMs) {
      await sleep(freezeMs);
      for (const publisher of frozenPublishers) {
        publisher.child.kill('SIGCONT');
        publisher.freeze.resumed = emit('unfreeze', { id: publisher.id });
      }
      // A freeze shorter than the stale window leaves the member fresh, with nothing to resume.
      const staleAt = (publisher, listener) =>
        events
          .slice(publisher.freeze.from)
          .some((e) => e.type === 'presence-stale' && e.id === publisher.id && e.listener === listener);
      await Promise.all(
        frozenPublishers.flatMap((publisher) =>
          listeners
            .filter((listener) => staleAt(publisher, listener))
            .map((listener) =>
              expect(
                `resumed ${publisher.id} at ${listener}`,
                (e) => e.type === 'presence-resumed' && e.id === publisher.id && e.listener === listener,
                settleMs,
                publisher.freeze.from,
              ),
            ),
        ),
      );
      await sleep(LATE_LEFT_WATCH_MS);
    } else await awaitLeft(frozenPublishers);
  }

  const latencyTrials = [];
  for (let trial = 1; trial <= trials; trial++) {
    const [started] = await startPublishers(1);
    const { publisher, from } = started;
    const up = events.slice(from).find((e) => e.type === 'publisher-up' && e.id === publisher.id);
    const seenType = presence ? JOINED : view === 'roots' ? 'joined' : 'announced';
    const seen = await expect(`${seenType} ${publisher.id}`, primaryEvent(seenType, publisher.id), settleMs, from);
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

  const rosterOf = (listener) =>
    [...(rosters.get(listener)?.values() ?? [])]
      .map(({ id, document, stale }) => ({ id, document, stale }))
      .sort((a, b) => a.id.localeCompare(b.id, 'en', { numeric: true }));
  const truth = () =>
    alive()
      .map(({ id, document }) => ({ id, document, stale: false }))
      .sort((a, b) => a.id.localeCompare(b.id, 'en', { numeric: true }));
  let rostersBeforeEnd = null;
  if (presence) {
    const waitStart = performance.now();
    const converged = () => listeners.every((listener) => same(rosterOf(listener), truth()));
    while (!converged() && performance.now() - waitStart < settleMs) await sleep(CONVERGE_POLL_MS);
    const byListener = Object.fromEntries(listeners.map((listener) => [listener, rosterOf(listener)]));
    const hostsOf = (roster) => roster.filter(({ document }) => document?.host === true).map(({ id }) => id);
    rostersBeforeEnd = {
      converged: converged(),
      waitMs: Math.round((performance.now() - waitStart) * 10) / 10,
      expected: truth(),
      agree: listeners.every((listener) => same(byListener[listener], byListener[PRIMARY])),
      hostsAgree: listeners.every((listener) => same(hostsOf(byListener[listener]), hostsOf(byListener[PRIMARY]))),
      hosts: Object.fromEntries(listeners.map((listener) => [listener, hostsOf(byListener[listener])])),
      rosters: byListener,
    };
    emit('rosters-before-end', { converged: rostersBeforeEnd.converged, waitMs: rostersBeforeEnd.waitMs });
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
      const left = events.slice(departure.from).find(primaryEvent(LEFT, id));
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
  const elapsed = (from, to) => (from && to ? Math.round((to.t - from.t) * 10) / 10 : null);
  const updateTimes = ({ publisher, from, sent }) => ({
    id: publisher.id,
    sentToUpdatedMs: Object.fromEntries(
      listeners.map((listener) => [
        listener,
        elapsed(
          sent,
          events
            .slice(from)
            .find((e) => e.type === 'presence-updated' && e.id === publisher.id && e.listener === listener),
        ),
      ]),
    ),
  });
  const presenceSummary = () => {
    const initialIds = initial.map(({ publisher }) => publisher.id);
    const held = new Set();
    let full;
    for (const e of events) {
      if (e.listener !== PRIMARY) continue;
      if (e.type === 'presence-joined') held.add(e.id);
      if (e.type === 'presence-left') held.delete(e.id);
      if (initialIds.every((id) => held.has(id))) {
        full = e;
        break;
      }
    }
    const ups = events.filter((e) => e.type === 'publisher-up' && e.first && initialIds.includes(e.id));
    const startedAt = events.find((e) => e.type === 'subscriber-start');
    const reference = flags['subscriber-after'] ? startedAt : ups.reduce((a, b) => (b.t > a.t ? b : a), ups[0]);
    const counted = Object.fromEntries(
      listeners.map((listener) => [
        listener,
        Object.fromEntries(
          PRESENCE_KINDS.map((kind) => [
            kind,
            events.filter((e) => e.type === `presence-${kind}` && e.listener === listener && !e.initial).length,
          ]),
        ),
      ]),
    );
    return {
      view: presenceView,
      listeners,
      events: counted,
      initialRosterMs: elapsed(reference, full),
      updates: updates.map(updateTimes),
      rotationUpdates: rotationUpdates.map(updateTimes),
      freezes: frozenPublishers.map(({ id, freeze, departure }) => {
        // A later close, such as --end close, is not the freeze's doing.
        const during = events.slice(
          freeze.from,
          !departure || departure.kind === 'freeze' ? undefined : departure.from,
        );
        const stale = during.find(primaryEvent('presence-stale', id));
        const left = during.find(primaryEvent(LEFT, id));
        const resumed = during.find(primaryEvent('presence-resumed', id));
        return {
          id,
          freezeMs,
          freezeToStaleMs: elapsed(freeze.event, stale),
          freezeToLeftMs: elapsed(freeze.event, left),
          staleBeforeLeft: Boolean(stale) && (!left || stale.t <= left.t),
          resumeToResumedMs: elapsed(freeze.resumed, resumed),
          leftAfterResume: Boolean(freeze.resumed && left && left.t >= freeze.resumed.t),
        };
      }),
      rostersBeforeEnd,
      rostersAtEnd: Object.fromEntries(listeners.map((listener) => [listener, rosterOf(listener)])),
    };
  };
  const primaryCount = (type) => events.filter((e) => e.type === type && e.listener === PRIMARY).length;
  const summary = {
    view: presence ? 'presence' : view,
    prefix,
    joined: presence ? primaryCount(JOINED) : counts.joined,
    left: presence ? primaryCount(LEFT) : counts.left,
    duplicates: duplicates.length,
    duplicateEvents: duplicates,
    loadedRoots,
    connectToLoadedMs,
    departToLeftMs: departures.map(({ departToLeftMs }) => departToLeftMs),
    departures,
    missingLeft: departures.filter(({ departToLeftMs }) => departToLeftMs === null).map(({ id }) => id),
    presentAtEnd: presence ? [...(rosters.get(PRIMARY)?.keys() ?? [])] : [...present],
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
    ...(presence ? { presence: presenceSummary() } : {}),
    errors: events.filter((e) => /error|timeout|missing|exit$/.test(e.type)).length,
  };

  if (wantSubscriber && wantSubscriber !== main.supervised) wantSubscriber.close();
  main.close();
  await Promise.all(others.map((other) => other.close()));
  killChildren();
  process.stdout.write(`${JSON.stringify({ summary })}\n`);
  if (flags.out) {
    await mkdir(dirname(flags.out), { recursive: true });
    await writeFile(flags.out, `${JSON.stringify({ argv: process.argv.slice(2), events, summary }, null, 2)}\n`);
  }
  process.exit(0);
}
