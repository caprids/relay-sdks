import type { MessageParameters, MoqtObject } from '@caprid/moq-core';
import { MAX_TIMEOUT_MS } from './connect-session.js';
import { OBJECT_DELIVERY_TIMEOUT } from './delivery-timeouts.js';
import { createFrameWriter, type Frame, type FrameWriter } from './frame-writer.js';
import { encodeJSON, readJSON } from './payload.js';
import type { RegisterTrackOptions, RelayTrackWriter } from './publish.js';
import type { SupervisedSubscriber } from './supervised-subscriber.js';
import { DEFAULT_PUBLISHER_PRIORITY, MAX_DEFAULT_PUBLISHER_PRIORITY } from './validation.js';
import { SDK_VERSION } from './version.js';

/**
 * Client diagnostics (spec/behavior.md, "Client diagnostics"; TypeScript only): a recorder that holds what a client
 * saw, published on two tracks of a publication of the client's own (by convention `<account>/diag/<app>/<group>/<client>`)
 * while something subscribes, and followed from elsewhere. Every client can report, whether it publishes media or not.
 * `diag.events` carries events, each with a sequence number; `diag.stats` carries a snapshot of every stats section
 * once a second.
 */
export const DIAGNOSTICS_TRACKS = {
  /** Events, as {@link DiagnosticsEventsObject}s. */
  events: 'diag.events',
  /** Stats snapshots, as {@link DiagnosticsStatsObject}s. */
  stats: 'diag.stats',
} as const;

/** One thing a client saw. SDK kinds are plain words (`link`, `want`); an application's start with `app.`. */
export interface DiagnosticEvent {
  /** Wall-clock time, in milliseconds since the epoch. */
  readonly t: number;
  /** The recorder's sequence number, from 0, one per event. */
  readonly seq: number;
  /** What happened. */
  readonly kind: string;
  /** The scope that recorded it, as {@link Diagnostics.scope} named it; absent for the recorder itself. */
  readonly source?: string;
  /** The event's own fields: ids and counters, never names or media. */
  readonly [field: string]: unknown;
}

/** Options for {@link createDiagnostics}. */
export interface DiagnosticsOptions {
  /** How long an event is held for a subscriber that arrives later, in milliseconds. Default 10 minutes. */
  keepMs?: number;
  /** The most events held, oldest dropped first. A positive integer; default 5000. */
  keepEvents?: number;
  /**
   * The application and its build, for the `start` event: a follower then knows which bundle a client ran without
   * asking. `name` is a non-empty string; `build` any string the app keeps, such as a commit or a release.
   */
  app?: DiagnosticsApp;
}

/** The application a recorder runs in, for its `start` event. */
export interface DiagnosticsApp {
  /** The application's name, such as `huddle`. Non-empty. */
  name: string;
  /** Its build: a commit, a release, whatever the application keeps. */
  build?: string;
}

/**
 * A recorder of events and stats sections, or a scope of one. Create one with {@link createDiagnostics}; the SDK
 * pieces that take a `diagnostics` option report into whichever they are given.
 */
export interface Diagnostics {
  /** A random id for this recorder. A follower tells two recorders, or a restarted client, apart by it. */
  readonly instance: string;
  /**
   * Record an event and tell the listeners. `kind` is a non-empty string; `fields` are copied, except `t`, `seq` and
   * `kind`, which the recorder sets, and in a scope `source`. Keep them to ids and counters: they leave the client.
   */
  event(kind: string, fields?: Readonly<Record<string, unknown>>): DiagnosticEvent;
  /**
   * The events still held whose sequence number is at least `seq`, oldest first: the `start` event, which is held for
   * the recorder's life, then the rest. Also what an app can send its own server when the client never connects.
   */
  events(seq?: number): DiagnosticEvent[];
  /** Call `listener` with each event as it is recorded. Returns the function that stops it. */
  listen(listener: (event: DiagnosticEvent) => void): () => void;
  /**
   * Add a stats section: `read` is called for every snapshot and returns something JSON can encode, usually counters.
   * In a scope the name is prefixed with the scope's and a dot. A section of the same name is replaced. Returns the
   * function that removes this one.
   */
  section(name: string, read: () => unknown): () => void;
  /** Every section's value now, by name, the whole recorder's from any scope. A section whose `read` throws reads `{ error }`. */
  snapshot(): Record<string, unknown>;
  /**
   * The same recorder under a name: its events carry `source` (a scope of a scope joins the names with `/`) and its
   * section names start with it. Give each link, reader or writer its own, such as `scope('roster')` or
   * `scope('p2/audio')`.
   */
  scope(name: string): Diagnostics;
}

const DEFAULT_KEEP_MS = 10 * 60_000;
const DEFAULT_KEEP_EVENTS = 5_000;

/**
 * Record what a client sees, for {@link publishDiagnostics}. Pass it to the SDK pieces that report into it, and add
 * your own events and sections.
 *
 * ```ts
 * const diagnostics = createDiagnostics();
 * diagnostics.event('app.joined', { participant: id });
 * diagnostics.section('app.video', () => ({ layer, freezes }));
 * ```
 */
export function createDiagnostics(options: DiagnosticsOptions = {}): Diagnostics {
  if (typeof options !== 'object' || options === null) throw new TypeError('options must be an object');
  const keepMs = options.keepMs ?? DEFAULT_KEEP_MS;
  if (typeof keepMs !== 'number' || !Number.isFinite(keepMs) || keepMs <= 0) {
    throw new RangeError('keepMs must be a positive number');
  }
  const keepEvents = options.keepEvents ?? DEFAULT_KEEP_EVENTS;
  if (!Number.isSafeInteger(keepEvents) || keepEvents < 1) {
    throw new RangeError('keepEvents must be a positive integer');
  }
  const app = appOption(options.app);
  const instance = randomId();
  const held: DiagnosticEvent[] = [];
  const listeners = new Set<(event: DiagnosticEvent) => void>();
  const sections = new Map<string, () => unknown>();
  let next = 0;
  /** The `start` event: held apart from the others and never dropped, so a follower who comes late still gets it. */
  let start: DiagnosticEvent | undefined;

  const events = (seq = 0) => {
    if (typeof seq !== 'number' || Number.isNaN(seq)) throw new TypeError('seq must be a number');
    const rest = held.filter((event) => event.seq >= seq);
    return start && start.seq >= seq ? [start, ...rest] : rest;
  };
  const listen = (listener: (event: DiagnosticEvent) => void) => {
    if (typeof listener !== 'function') throw new TypeError('listener must be a function');
    const own = (event: DiagnosticEvent) => listener(event);
    listeners.add(own);
    return () => {
      listeners.delete(own);
    };
  };
  const snapshot = () => {
    const out: Record<string, unknown> = {};
    for (const [name, read] of sections) {
      try {
        out[name] = read();
      } catch (error) {
        out[name] = { error: error instanceof Error ? error.message : String(error) };
      }
    }
    return out;
  };
  const view = (source: string | undefined): Diagnostics => ({
    instance,
    event(kind, fields = {}) {
      if (typeof kind !== 'string' || kind === '') throw new TypeError('kind must be a non-empty string');
      if (typeof fields !== 'object' || fields === null) throw new TypeError('fields must be an object');
      const { t: _t, seq: _seq, kind: _kind, ...own } = fields;
      const event: DiagnosticEvent = {
        t: Date.now(),
        seq: next++,
        kind,
        ...own,
        ...(source === undefined ? {} : { source }),
      };
      if (start === undefined && kind === 'start' && source === undefined) start = event;
      else held.push(event);
      const cutoff = event.t - keepMs;
      let drop = Math.max(0, held.length - keepEvents);
      while (drop < held.length && (held[drop] as DiagnosticEvent).t < cutoff) drop++;
      if (drop > 0) held.splice(0, drop);
      for (const listener of listeners) {
        try {
          listener(event);
        } catch (error) {
          // biome-ignore lint/suspicious/noConsole: the application's listener failed and has no caller to report to
          console.error(new Error('A diagnostics listener threw; the recorder carries on', { cause: error }));
        }
      }
      return event;
    },
    events,
    listen,
    section(name, read) {
      if (typeof name !== 'string' || name === '') throw new TypeError('section name must be a non-empty string');
      if (typeof read !== 'function') throw new TypeError('read must be a function');
      const full = source === undefined ? name : `${source}.${name}`;
      sections.set(full, read);
      return () => {
        if (sections.get(full) === read) sections.delete(full);
      };
    },
    snapshot,
    scope(name) {
      if (typeof name !== 'string' || name === '') throw new TypeError('scope name must be a non-empty string');
      return view(source === undefined ? name : `${source}/${name}`);
    },
  });
  const recorder = view(undefined);
  recorder.event('start', startFields(app));
  return recorder;
}

/** The `app` option: absent, or a non-empty `name` and an optional string `build`. */
function appOption(value: unknown): { name: string; build?: string } | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== 'object' || value === null) throw new TypeError('app must be an object');
  const { name, build } = value as { name?: unknown; build?: unknown };
  if (typeof name !== 'string' || name === '') throw new TypeError('app.name must be a non-empty string');
  if (build !== undefined && typeof build !== 'string') throw new TypeError('app.build must be a string');
  return build === undefined ? { name } : { name, build };
}

/** What the runtime says of itself in `navigator.userAgentData` (User-Agent Client Hints), where it has them. */
interface UserAgentData {
  brands?: readonly { brand?: unknown; version?: unknown }[];
  platform?: unknown;
  mobile?: unknown;
}

/**
 * The `start` event's fields: the SDK's version, the app's name and build, what the runtime is (its User-Agent Client
 * Hints' brands, platform and mobile where it has them, else its user agent; Node's version in Node), and whether it
 * has `WebTransport`.
 */
function startFields(app: { name: string; build?: string } | undefined): Record<string, unknown> {
  const fields: Record<string, unknown> = { sdk: SDK_VERSION };
  if (app) {
    fields['app'] = app.name;
    if (app.build !== undefined) fields['build'] = app.build;
  }
  const nav = (globalThis as { navigator?: { userAgent?: unknown; userAgentData?: UserAgentData } }).navigator;
  const hints = nav?.userAgentData;
  const brands = Array.isArray(hints?.brands)
    ? hints.brands
        .filter((entry) => typeof entry?.brand === 'string' && !/not.?a.?brand/i.test(entry.brand))
        .map((entry) => `${entry.brand as string} ${String(entry.version ?? '')}`.trim())
    : [];
  if (brands.length > 0) {
    fields['brands'] = brands;
    if (typeof hints?.platform === 'string') fields['platform'] = hints.platform;
    if (typeof hints?.mobile === 'boolean') fields['mobile'] = hints.mobile;
  } else if (typeof nav?.userAgent === 'string') {
    fields['userAgent'] = nav.userAgent;
  }
  const node = (globalThis as { process?: { versions?: { node?: unknown } } }).process?.versions?.node;
  if (typeof node === 'string') fields['node'] = node;
  fields['webTransport'] = typeof (globalThis as { WebTransport?: unknown }).WebTransport === 'function';
  return fields;
}

/**
 * What {@link publishDiagnostics} registers its tracks on: a `RelayPublisher`, a `SupervisedPublisher`, or a root
 * published on a `SupervisedRelay` (the `SupervisedPublication` that `relay.publish(root)` returns).
 */
export interface DiagnosticsPublisher {
  /** Register a track on the publisher's broadcast, as `RelayPublisher.registerTrack` does. */
  registerTrack(options: RegisterTrackOptions): RelayTrackWriter;
}

/** Options for {@link publishDiagnostics}. */
export interface PublishDiagnosticsOptions {
  /**
   * How often a stats snapshot is sent while `diag.stats` has demand, in milliseconds. Default 1000. A snapshot the
   * transport has not taken within two intervals is dropped: the next one supersedes it.
   */
  statsIntervalMs?: number;
  /**
   * The Default Publisher Priority (0x0E) of `diag.events`, an integer from 0 to 255, lower being more urgent.
   * Default 224: behind every media track (audio 16, video 96), so diagnostics never hold media up on a congested
   * uplink.
   */
  eventsPriority?: number;
  /** The Default Publisher Priority (0x0E) of `diag.stats`, as `eventsPriority`. Default 240, behind events. */
  statsPriority?: number;
}

/** The two diagnostics tracks being published. */
export interface DiagnosticsPublication {
  /** Stop publishing and close both tracks. Idempotent. */
  close(): Promise<void>;
}

/** One `diag.events` object: events of one recorder, live or (`backlog`) replayed from what it held. */
export interface DiagnosticsEventsObject {
  /** The recorder's {@link Diagnostics.instance}. */
  instance: string;
  /** Present when the events are a replay of what the recorder held, not live. */
  backlog?: true;
  /** The events, oldest first. */
  events: DiagnosticEvent[];
}

/** One `diag.stats` object: one recorder's sections at one moment. */
export interface DiagnosticsStatsObject {
  /** The recorder's {@link Diagnostics.instance}. */
  instance: string;
  /** When the snapshot was taken: wall clock, milliseconds since the epoch. */
  t: number;
  /** Every section's value, by name, as {@link Diagnostics.snapshot} returns it. */
  stats: Record<string, unknown>;
}

/**
 * Default Publisher Priorities of the two tracks: in draft-jennings-moq-metrics-03's 128 to 240 for metrics, behind
 * every media default (audio 16, presence 32, video 96) and the relay's own 128, and events ahead of stats, which are
 * superseded each interval.
 */
const EVENTS_PRIORITY = 224;
const STATS_PRIORITY = 240;

/** A stats snapshot as the stats writer sees it, with the time it was taken. */
interface StatsFrame extends Frame {
  readonly t: number;
}

/** Events per object when replaying what the recorder holds, and the most objects a replay takes: past 6,400 events
 * held, each object carries more, so a replay always fits the events writer's backlog with room for live events. */
const BACKLOG_CHUNK = 200;
const BACKLOG_OBJECTS = 32;

/**
 * Publish a recorder on `diag.events` and `diag.stats` of the publisher's broadcast. Nothing is sent while the relay
 * has no subscription to a track. Each time the relay subscribes to `diag.events`, everything the recorder holds goes
 * first, marked `backlog`, then each event as it is recorded; a follower drops sequence numbers it has seen. Right
 * after the relay replaces its subscription a few live events can still arrive ahead of the replay, so a follower
 * that needs them in order sorts by `seq`. A
 * snapshot goes on `diag.stats` once a second while it has demand. Track names are unique per publisher connection,
 * so publish one recorder per publisher (on a `SupervisedRelay`, per published root).
 *
 * ```ts
 * const publication = publishDiagnostics(diagnostics, publisher);
 * // ...
 * await publication.close();
 * ```
 */
export function publishDiagnostics(
  diagnostics: Diagnostics,
  publisher: DiagnosticsPublisher,
  options: PublishDiagnosticsOptions = {},
): DiagnosticsPublication {
  if (typeof options !== 'object' || options === null) throw new TypeError('options must be an object');
  const statsIntervalMs = options.statsIntervalMs ?? 1000;
  if (!Number.isSafeInteger(statsIntervalMs) || statsIntervalMs < 1 || statsIntervalMs > 2_147_483_647) {
    throw new RangeError('statsIntervalMs must be an integer from 1 to 2147483647');
  }
  const eventsPriority = priority(options.eventsPriority, EVENTS_PRIORITY, 'eventsPriority');
  const statsPriority = priority(options.statsPriority, STATS_PRIORITY, 'statsPriority');
  // Events carry no delivery timeout: one is not superseded, and a lost one is gone until the next replay. A snapshot
  // the transport has not taken within two intervals is dropped, because the next one replaces it.
  const statsTimeoutMs = Math.min(2 * statsIntervalMs, MAX_TIMEOUT_MS);
  const eventsTrack = publisher.registerTrack({
    track: DIAGNOSTICS_TRACKS.events,
    trackProperties: [{ type: DEFAULT_PUBLISHER_PRIORITY, value: eventsPriority }],
  });
  const statsTrack = publisher.registerTrack({
    track: DIAGNOSTICS_TRACKS.stats,
    trackProperties: [
      { type: DEFAULT_PUBLISHER_PRIORITY, value: statsPriority },
      { type: OBJECT_DELIVERY_TIMEOUT, value: statsTimeoutMs },
    ],
  });
  const events = createFrameWriter(eventsTrack, { grouping: 'frame', maxBacklog: 64 });
  // The track's delivery timeout counts from when the writer serves a snapshot; one that waited behind another is
  // judged by its own age too, and only the newest waits.
  const stats = createFrameWriter<StatsFrame>(statsTrack, {
    grouping: 'frame',
    maxBacklog: 1,
    stale: (frame) => Date.now() - frame.t > statsTimeoutMs,
  });
  const { instance } = diagnostics;
  let closed = false;

  const send = <F extends Frame>(writer: FrameWriter<F>, frame: F) => {
    // The writer reports what became of the object; a closed writer's rejection only means publishing has stopped.
    writer.write(frame).catch(() => {});
  };
  const eventsFrame = (body: DiagnosticsEventsObject): Frame => ({ payload: encodeBody(body), key: true });
  // Live events go out only once the relay's current subscription has had the replay, so a follower gets the held
  // history first; an event recorded meanwhile is held, so the replay carries it.
  let live = false;
  const unlisten = diagnostics.listen((event) => {
    if (!closed && live && eventsTrack.hasDemand) send(events, eventsFrame({ instance, events: [event] }));
  });
  const changes = eventsTrack.demandChanges();
  const watching = (async () => {
    // The current state first, then a repeated `true` for each new or replacement relay subscription.
    for await (const demanded of changes) {
      live = false;
      if (!demanded || closed) continue;
      const held = diagnostics.events();
      const chunk = Math.max(BACKLOG_CHUNK, Math.ceil(held.length / BACKLOG_OBJECTS));
      for (let i = 0; i < held.length; i += chunk) {
        send(events, eventsFrame({ instance, backlog: true, events: held.slice(i, i + chunk) }));
      }
      live = true;
    }
  })().catch(() => {});
  const timer = setInterval(() => {
    if (closed || !statsTrack.hasDemand) return;
    const t = Date.now();
    send(stats, { t, payload: encodeBody({ instance, t, stats: diagnostics.snapshot() }), key: true });
  }, statsIntervalMs);

  return {
    async close() {
      if (closed) return;
      closed = true;
      clearInterval(timer);
      unlisten();
      await changes.return?.();
      await watching;
      await Promise.all([events.close(), stats.close()]);
      eventsTrack.close();
      statsTrack.close();
    },
  };
}

/** Where {@link followDiagnostics} reads: a client's broadcast, or a supervised publisher's root. */
export interface FollowDiagnosticsOptions {
  /** The client's namespace, or with `generations` a supervised publisher's root. */
  namespace: readonly string[];
  /** Follow the newest generation under `namespace`, as a want does. */
  generations?: boolean;
  /** The prefix of the two want keys, `<key>/diag.events` and `<key>/diag.stats`. Default the namespace joined by `/`. */
  key?: string;
  /**
   * Parameters for the `diag.events` subscription, such as a location filter that asks the relay for the groups it
   * still holds: a follower that subscribes while another already does gets no replay from the client.
   */
  eventsParameters?: MessageParameters;
}

/** What {@link followDiagnostics} delivers. Like a want's handlers, they must be synchronous. */
export interface FollowDiagnosticsHandlers {
  /** Events not delivered before, in the order they arrived; `backlog` when they came from a replay. */
  onEvents?(events: DiagnosticEvent[], info: { instance: string; backlog: boolean }): void;
  /** One stats snapshot. */
  onStats?(stats: DiagnosticsStatsObject): void;
  /** A want's failure that it does not retry, as `WantHandlers.onError`. Objects that do not parse are skipped. */
  onError?(error: unknown): void;
}

/** Sequence numbers remembered per recorder before the oldest half is forgotten. */
const SEEN_LIMIT = 4 * DEFAULT_KEEP_EVENTS;

/**
 * Follow a client's diagnostics with a supervised subscriber or a `SupervisedRelay`: want its `diag.events` and `diag.stats`, drop events
 * already delivered (a replay repeats them), and skip objects that do not parse. Returns the function that unwants both.
 *
 * ```ts
 * const unfollow = followDiagnostics(subscriber, { namespace: root, generations: true }, {
 *   onEvents: (events) => events.forEach((event) => log(event)),
 *   onStats: (stats) => store(stats),
 * });
 * ```
 */
export function followDiagnostics(
  subscriber: Pick<SupervisedSubscriber, 'want' | 'unwant'>,
  options: FollowDiagnosticsOptions,
  handlers: FollowDiagnosticsHandlers,
): () => void {
  if (typeof options !== 'object' || options === null) throw new TypeError('options must be an object');
  if (typeof handlers !== 'object' || handlers === null) throw new TypeError('handlers must be an object');
  const { namespace, generations = false, eventsParameters } = options;
  if (!Array.isArray(namespace)) throw new TypeError('namespace must be an array of strings');
  const key = options.key ?? namespace.join('/');
  if (typeof key !== 'string') throw new TypeError('key must be a string');
  const seen = new Map<string, Set<number>>();
  const fresh = (instance: string, events: DiagnosticEvent[]) => {
    let done = seen.get(instance);
    if (!done) {
      done = new Set();
      seen.set(instance, done);
    }
    const out = events.filter((event) => !done.has(event.seq));
    for (const event of out) done.add(event.seq);
    if (done.size > SEEN_LIMIT) {
      const keep = [...done].sort((a, b) => a - b).slice(-SEEN_LIMIT / 2);
      seen.set(instance, new Set(keep));
    }
    return out;
  };
  const onError = handlers.onError ? (error: unknown) => handlers.onError?.(error) : undefined;

  const eventsKey = `${key}/${DIAGNOSTICS_TRACKS.events}`;
  const statsKey = `${key}/${DIAGNOSTICS_TRACKS.stats}`;
  subscriber.want(
    eventsKey,
    {
      namespace,
      track: DIAGNOSTICS_TRACKS.events,
      generations,
      ...(eventsParameters === undefined ? {} : { parameters: eventsParameters }),
    },
    {
      onObject(object) {
        const body = parse(object, isEventsObject);
        if (!body) return;
        const events = fresh(body.instance, body.events);
        if (events.length > 0) handlers.onEvents?.(events, { instance: body.instance, backlog: body.backlog === true });
      },
      ...(onError ? { onError } : {}),
    },
  );
  subscriber.want(
    statsKey,
    { namespace, track: DIAGNOSTICS_TRACKS.stats, generations },
    {
      onObject(object) {
        const body = parse(object, isStatsObject);
        if (body) handlers.onStats?.(body);
      },
      ...(onError ? { onError } : {}),
    },
  );
  return () => {
    subscriber.unwant(eventsKey);
    subscriber.unwant(statsKey);
  };
}

/** What {@link watchAudioInput} reads: `@caprid/moq-media`'s `AudioPlayerInput` qualifies. */
export interface WatchedAudioInput {
  /** The input's cumulative counters; the four below are the ones episodes are measured with. */
  readonly stats: {
    /** Silence played in place of frames that never came, in microseconds. */
    readonly concealedUs: number;
    /** Audio synthesized in place of frames that never came, in microseconds. */
    readonly synthesizedUs: number;
    /** Times the buffer ran dry while playing. */
    readonly underruns: number;
    /** Frames dropped on arrival. */
    readonly late: number;
  };
}

/** How often {@link watchAudioInput} looks at the input, in milliseconds. */
const AUDIO_WATCH_MS = 100;
/** An episode ends once nothing was filled for this long. */
const AUDIO_EPISODE_QUIET_MS = 300;
/** Filled audio of at least this much is an `audio-conceal` event, and of at least the second an `audio-dropout`. */
const AUDIO_CONCEAL_MS = 60;
const AUDIO_DROPOUT_MS = 250;

/**
 * Watch an audio player input for diagnostics, under a scope such as `diagnostics.scope('p2/audio')`: an `audio`
 * section with its stats, and an event for each episode of filled audio (concealed or synthesized instead of received),
 * once the input has played received audio again for 300 ms: `audio-dropout` for 250 ms or more, `audio-conceal` for
 * 60 ms or more. Returns the function that stops watching.
 *
 * ```ts
 * const input = player.addInput({ clock, decoderConfig });
 * const unwatch = watchAudioInput(diagnostics.scope(`${participant}/audio`), input);
 * ```
 */
export function watchAudioInput(diagnostics: Diagnostics, input: WatchedAudioInput): () => void {
  if (typeof input !== 'object' || input === null || typeof input.stats !== 'object') {
    throw new TypeError('input must have stats, as an AudioPlayerInput does');
  }
  const removeSection = diagnostics.section('audio', () => ({ ...input.stats }));
  let previous = { ...input.stats };
  let episode: { since: number; grewAt: number; base: typeof previous } | undefined;
  const timer = setInterval(() => {
    const now = performance.now();
    const stats = { ...input.stats };
    const grew = stats.concealedUs + stats.synthesizedUs > previous.concealedUs + previous.synthesizedUs;
    if (grew && !episode) episode = { since: now - AUDIO_WATCH_MS, grewAt: now, base: previous };
    else if (grew && episode) episode.grewAt = now;
    else if (episode && now - episode.grewAt > AUDIO_EPISODE_QUIET_MS) {
      const { base, since, grewAt } = episode;
      episode = undefined;
      const concealedMs = (stats.concealedUs - base.concealedUs) / 1000;
      const synthesizedMs = (stats.synthesizedUs - base.synthesizedUs) / 1000;
      const filledMs = concealedMs + synthesizedMs;
      if (filledMs >= AUDIO_CONCEAL_MS) {
        diagnostics.event(filledMs >= AUDIO_DROPOUT_MS ? 'audio-dropout' : 'audio-conceal', {
          ms: Math.round(grewAt - since),
          concealedMs: Math.round(concealedMs),
          synthesizedMs: Math.round(synthesizedMs),
          underruns: stats.underruns - base.underruns,
          late: stats.late - base.late,
        });
      }
    }
    previous = stats;
  }, AUDIO_WATCH_MS);
  return () => {
    clearInterval(timer);
    removeSection();
  };
}

/** What {@link watchPage} reads of `navigator.connection` (the Network Information API), where the browser has it. */
interface NetworkInformationLike extends EventTarget {
  readonly effectiveType?: unknown;
  readonly rtt?: unknown;
  readonly downlink?: unknown;
}

/**
 * Watch the page a client runs in, for diagnostics: a background tab slows its timers and can look like network trouble,
 * and a page that is frozen or put in the back/forward cache looks like a client that left. Records a `page` event on
 * each change, with `what`: `visible` or `hidden` (visibility), `freeze` or `resume`, `pagehide` or `pageshow` (with
 * `persisted`, whether the page goes into or comes from the back/forward cache), `online` or `offline`, and
 * `connection` (`effectiveType`, `rttMs`, `downlinkMbps`) when `navigator.connection` changes, where the browser has it.
 * Adds a `page` section: `visibility`, cumulative `hiddenMs`, `freezes`, `online`, and the connection's fields.
 * Browser only: where there is no `document` it records nothing. Returns the function that stops watching.
 *
 * ```ts
 * const unwatch = watchPage(diagnostics);
 * ```
 */
export function watchPage(diagnostics: Diagnostics): () => void {
  const scope = globalThis as {
    document?: EventTarget & { visibilityState?: unknown };
    navigator?: { onLine?: unknown; connection?: NetworkInformationLike };
    addEventListener?: EventTarget['addEventListener'];
    removeEventListener?: EventTarget['removeEventListener'];
  };
  const doc = scope.document;
  if (typeof doc !== 'object' || doc === null || typeof doc.addEventListener !== 'function') return () => {};
  const hidden = () => doc.visibilityState === 'hidden';
  let hiddenSince = hidden() ? performance.now() : undefined;
  let hiddenMs = 0;
  let freezes = 0;
  const connection = scope.navigator?.connection;
  const network = () => {
    const out: Record<string, unknown> = {};
    if (!connection) return out;
    if (typeof connection.effectiveType === 'string') out['effectiveType'] = connection.effectiveType;
    if (typeof connection.rtt === 'number') out['rttMs'] = connection.rtt;
    if (typeof connection.downlink === 'number') out['downlinkMbps'] = connection.downlink;
    return out;
  };
  const removeSection = diagnostics.section('page', () => ({
    visibility: hidden() ? 'hidden' : 'visible',
    hiddenMs: Math.round(hiddenMs + (hiddenSince === undefined ? 0 : performance.now() - hiddenSince)),
    freezes,
    ...(typeof scope.navigator?.onLine === 'boolean' ? { online: scope.navigator.onLine } : {}),
    ...network(),
  }));
  const listeners: [EventTarget, string, (event: Event) => void][] = [];
  const on = (target: EventTarget | undefined, type: string, listener: (event: Event) => void) => {
    if (!target || typeof target.addEventListener !== 'function') return;
    target.addEventListener(type, listener);
    listeners.push([target, type, listener]);
  };
  on(doc, 'visibilitychange', () => {
    const now = performance.now();
    if (hidden()) hiddenSince ??= now;
    else if (hiddenSince !== undefined) {
      hiddenMs += now - hiddenSince;
      hiddenSince = undefined;
    }
    diagnostics.event('page', { what: hidden() ? 'hidden' : 'visible' });
  });
  on(doc, 'freeze', () => {
    freezes++;
    diagnostics.event('page', { what: 'freeze' });
  });
  on(doc, 'resume', () => diagnostics.event('page', { what: 'resume' }));
  // `window` is the global object in a browser.
  const win = typeof scope.addEventListener === 'function' ? (scope as unknown as EventTarget) : undefined;
  for (const type of ['pagehide', 'pageshow']) {
    on(win, type, (event) =>
      diagnostics.event('page', {
        what: type,
        persisted: (event as Event & { persisted?: unknown }).persisted === true,
      }),
    );
  }
  for (const type of ['online', 'offline']) on(win, type, () => diagnostics.event('page', { what: type }));
  on(connection, 'change', () => diagnostics.event('page', { what: 'connection', ...network() }));
  return () => {
    for (const [target, type, listener] of listeners.splice(0)) target.removeEventListener(type, listener);
    removeSection();
  };
}

/** A Default Publisher Priority option: an integer from 0 to 255, or the default when absent. */
function priority(value: unknown, fallback: number, name: string): number {
  if (value === undefined) return fallback;
  if (typeof value !== 'number') throw new TypeError(`${name} must be a number`);
  if (!Number.isInteger(value) || value < 0 || value > MAX_DEFAULT_PUBLISHER_PRIORITY) {
    throw new RangeError(`${name} must be an integer from 0 to ${MAX_DEFAULT_PUBLISHER_PRIORITY}`);
  }
  return value;
}

/**
 * Encode an object as JSON. An event or a section that JSON cannot encode (a BigInt, a cycle, a throwing `toJSON`)
 * is replaced by `{ error }` in its place, the event keeping `t`, `seq`, `kind` and `source`, so one bad value costs
 * itself and not the object, the replay or the publishing.
 */
function encodeBody(body: DiagnosticsEventsObject | DiagnosticsStatsObject): Uint8Array {
  try {
    return encodeJSON(body);
  } catch {
    if ('events' in body) {
      const events = body.events.map((event) => {
        const { t, seq, kind, source } = event;
        return encodes(event)
          ? event
          : { t, seq, kind, ...(source === undefined ? {} : { source }), error: 'not encodable as JSON' };
      });
      return encodeJSON({ ...body, events });
    }
    const stats: Record<string, unknown> = {};
    for (const [name, value] of Object.entries(body.stats)) {
      stats[name] = encodes(value) ? value : { error: 'not encodable as JSON' };
    }
    return encodeJSON({ ...body, stats });
  }
}

function encodes(value: unknown): boolean {
  try {
    return JSON.stringify(value) !== undefined;
  } catch {
    return false;
  }
}

function parse<T>(object: MoqtObject, is: (value: unknown) => value is T): T | undefined {
  try {
    const value = readJSON(object);
    return is(value) ? value : undefined;
  } catch {
    return undefined;
  }
}

function isEventsObject(value: unknown): value is DiagnosticsEventsObject {
  if (typeof value !== 'object' || value === null) return false;
  const body = value as Partial<DiagnosticsEventsObject>;
  return (
    typeof body.instance === 'string' &&
    Array.isArray(body.events) &&
    body.events.every(
      (event) =>
        typeof event === 'object' &&
        event !== null &&
        Number.isSafeInteger(event.seq) &&
        typeof event.kind === 'string' &&
        typeof event.t === 'number',
    )
  );
}

function isStatsObject(value: unknown): value is DiagnosticsStatsObject {
  if (typeof value !== 'object' || value === null) return false;
  const body = value as Partial<DiagnosticsStatsObject>;
  return (
    typeof body.instance === 'string' &&
    typeof body.t === 'number' &&
    typeof body.stats === 'object' &&
    body.stats !== null
  );
}

/** Twelve random base-36 characters. */
function randomId(): string {
  return Array.from(crypto.getRandomValues(new Uint8Array(12)), (byte) => (byte % 36).toString(36)).join('');
}
