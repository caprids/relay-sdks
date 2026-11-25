import type { MessageParameters, MoqtObject } from '@caprid/moq-core';
import { MAX_TIMEOUT_MS } from './connect-session.js';
import { OBJECT_DELIVERY_TIMEOUT } from './delivery-timeouts.js';
import { createFrameWriter, type Frame, type FrameWriter } from './frame-writer.js';
import { encodeJSON, readJSON } from './payload.js';
import type { RegisterTrackOptions, RelayTrackWriter } from './publish.js';
import type { SupervisedSubscriber } from './supervised-subscriber.js';
import { DEFAULT_PUBLISHER_PRIORITY, MAX_DEFAULT_PUBLISHER_PRIORITY } from './validation.js';

/**
 * Client diagnostics (spec/behavior.md, "Client diagnostics"; TypeScript only): a recorder that holds what a client
 * saw, published on two tracks of the client's own broadcast while something subscribes, and followed from elsewhere.
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
  /** The events still held whose sequence number is at least `seq`, oldest first. */
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
  const instance = randomId();
  const held: DiagnosticEvent[] = [];
  const listeners = new Set<(event: DiagnosticEvent) => void>();
  const sections = new Map<string, () => unknown>();
  let next = 0;

  const events = (seq = 0) => {
    if (typeof seq !== 'number' || Number.isNaN(seq)) throw new TypeError('seq must be a number');
    return held.filter((event) => event.seq >= seq);
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
      held.push(event);
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
  return view(undefined);
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
