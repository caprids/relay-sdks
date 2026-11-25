/**
 * Internal: what the SDK's own pieces report into a `diagnostics` option (spec/behavior.md, "Client diagnostics"). Kept
 * apart from the recorder and the publishing, so a supervisor importing it pulls in no frame writer.
 */
import type { MoqtObject, SubscribeOk } from '@caprid/moq-core';
import type { Diagnostics } from './client-diagnostics.js';
import type { DialReport, TransportFraming } from './connect-session.js';
import { RelayError } from './errors.js';
import type { RestartReason, WantHandlers } from './supervised-subscriber.js';
import type { SupervisedStatus } from './supervision.js';

/**
 * A short, log-safe description of a failure for an event: a RelayError's code and phase, or an error's name, then its
 * message, with tokens redacted and at most 300 characters.
 */
export function describeFailure(error: unknown): string {
  let text: string;
  if (error instanceof RelayError) text = `${error.code} (${error.phase}): ${error.message}`;
  else if (error instanceof Error) text = `${error.name}: ${error.message}`;
  else text = String(error);
  return text
    .replace(/([?&]jwt=)[^&\s"']+/g, '$1[redacted]')
    .replace(/eyJ[\w-]+\.[\w-]+\.[\w-]+/g, '[jwt]')
    .slice(0, 300);
}

/** The host of a relay endpoint, port included: never its path or query, which can carry a token. */
export function endpointHost(endpoint: string): string | undefined {
  try {
    return new URL(endpoint).host || undefined;
  } catch {
    return undefined;
  }
}

/** How a connection attempt ended, for a `dial` event. */
export function dialOutcome(error: unknown): 'timeout' | 'refused' | 'error' {
  if (error instanceof RelayError && error.code === 'CONNECTION_TIMEOUT') return 'timeout';
  if (error instanceof RelayError && error.code === 'UNAUTHORIZED') return 'refused';
  return 'error';
}

/** How often a live connection's transport stats are read, in milliseconds. */
const TRANSPORT_STATS_MS = 1_000;

/** A number rounded to a tenth. */
const tenths = (value: number) => Math.round(value * 10) / 10;

/**
 * The fields of a WebTransport `getStats()` result (WebTransportConnectionStats) the `transport` section reports, under
 * its own names, units in the names. A field the runtime does not give is left out: Chromium gives the three RTTs and
 * the estimated send rate, and none of the byte and packet counters.
 */
const TRANSPORT_FIELDS: readonly [string, string, (value: number) => number][] = [
  ['smoothedRtt', 'rttMs', tenths],
  ['rttVariation', 'rttVarMs', tenths],
  ['minRtt', 'minRttMs', tenths],
  ['bytesSent', 'sentBytes', Math.round],
  ['bytesReceived', 'receivedBytes', Math.round],
  ['packetsSent', 'sentPackets', Math.round],
  ['packetsReceived', 'receivedPackets', Math.round],
  ['packetsLost', 'lostPackets', Math.round],
  ['bytesLost', 'lostBytes', Math.round],
  // Bits per second.
  ['estimatedSendRate', 'sendRateKbps', (bps) => Math.round(bps / 1000)],
];

/** A transport's stats, as the `transport` section reports them: only the fields that are finite numbers (or BigInts). */
export function transportStats(raw: unknown): Record<string, number | boolean> {
  const out: Record<string, number | boolean> = {};
  if (typeof raw !== 'object' || raw === null) return out;
  const stats = raw as Record<string, unknown>;
  for (const [from, to, convert] of TRANSPORT_FIELDS) {
    const value = typeof stats[from] === 'bigint' ? Number(stats[from]) : stats[from];
    if (typeof value === 'number' && Number.isFinite(value)) out[to] = convert(value);
  }
  const capacity = stats['atSendCapacity'];
  if (typeof capacity === 'boolean') out['atSendCapacity'] = capacity;
  return out;
}

/**
 * What a supervisor reports, under the scope it is given: a `link` event on each status change (with what carries the
 * connection and the relay's host when it goes live, and the error that ended the last one), a `dial` event per
 * connection attempt and a `fallback` event when its factory fell back, a `publication` event per root it publishes, a
 * `link` section, and while a connection whose transport has `getStats()` is live, a `transport` section.
 */
export function linkDiagnostics(diagnostics: Diagnostics) {
  let attempts = 0;
  let stopWatching: (() => void) | undefined;
  const state: {
    status: SupervisedStatus;
    carrier?: TransportFraming;
    relay?: string;
    generation?: string;
    connects: number;
  } = { status: 'connecting', connects: 0 };
  diagnostics.section('link', () => ({ ...state }));
  return {
    status(status: SupervisedStatus, error?: unknown) {
      state.status = status;
      if (status === 'live') state.connects++;
      diagnostics.event('link', {
        status,
        ...(status === 'live' && state.carrier ? { carrier: state.carrier } : {}),
        ...(status === 'live' && state.relay ? { relay: state.relay } : {}),
        ...(error === undefined ? {} : { error: describeFailure(error) }),
      });
    },
    /** The connection that is about to go live: what carries it, and the endpoint it reached. */
    carrier(framing: TransportFraming, endpoint?: string) {
      state.carrier = framing;
      const relay = endpoint === undefined ? undefined : endpointHost(endpoint);
      if (relay === undefined) delete state.relay;
      else state.relay = relay;
    },
    generation(generation: string) {
      state.generation = generation;
      diagnostics.event('generation', { generation });
    },
    /**
     * A connection attempt ended: a `dial` event with its number, the carrier and relay host it reached, how long it
     * took from its start, and how it ended; and a `fallback` event first when the factory fell back.
     */
    dial(startedAt: number, report: DialReport | undefined, error?: unknown) {
      const attempt = ++attempts;
      const ms = Math.round(performance.now() - startedAt);
      const relay = report?.endpoint === undefined ? undefined : endpointHost(report.endpoint);
      const fallback = report?.fallback;
      if (fallback) {
        diagnostics.event('fallback', {
          attempt,
          from: fallback.from,
          to: fallback.to,
          reason: fallback.reason,
          ...(fallback.error === undefined ? {} : { error: describeFailure(fallback.error) }),
        });
      }
      // On a failure after a fallback, the carrier last dialed.
      const carrier = error === undefined ? report?.framing : (fallback?.to ?? report?.framing);
      diagnostics.event('dial', {
        attempt,
        ...(carrier === undefined ? {} : { carrier }),
        ...(relay === undefined ? {} : { relay }),
        ms,
        outcome: error === undefined ? 'ok' : dialOutcome(error),
        ...(error === undefined ? {} : { error: describeFailure(error) }),
      });
    },
    /**
     * The connection that just went live: read its transport's `getStats()` about once a second into a `transport`
     * section, until `ended` aborts or another connection goes live. Nothing for a transport without it.
     */
    watch(transport: unknown, ended: AbortSignal) {
      stopWatching?.();
      stopWatching = undefined;
      const getStats = (transport as { getStats?: unknown } | undefined)?.getStats;
      if (typeof getStats !== 'function' || ended.aborted) return;
      let stats: Record<string, number | boolean> = {};
      let reading = false;
      // `getStats()` is async: the section reads what the last call gave, and a call still pending is not repeated.
      const read = () => {
        if (reading) return;
        reading = true;
        Promise.resolve()
          .then(() => getStats.call(transport))
          .then(
            (raw: unknown) => {
              if (!ended.aborted) stats = transportStats(raw);
            },
            () => {},
          )
          .finally(() => {
            reading = false;
          });
      };
      const removeSection = diagnostics.section('transport', () => ({ ...stats }));
      read();
      const timer = setInterval(read, TRANSPORT_STATS_MS);
      const stop = () => {
        clearInterval(timer);
        removeSection();
        ended.removeEventListener('abort', stop);
        if (stopWatching === stop) stopWatching = undefined;
      };
      stopWatching = stop;
      ended.addEventListener('abort', stop, { once: true });
    },
    /** A root published, or its publication closed: a `publication` event with its namespace. */
    publication(root: readonly string[], what: 'published' | 'closed') {
      diagnostics.event('publication', { what, namespace: root.join('/') });
    },
  };
}

/** One want as a supervisor's `wants` section reports it. */
interface WantState {
  track: string;
  subscribed: boolean;
  objects: number;
  restarts: number;
  lastRestart?: RestartReason;
  /** From the want to its first object, once there was one. */
  firstObjectMs?: number;
  /** Groups whose stream was reset before its end, and of them those reset with DELIVERY_TIMEOUT (0x2). */
  groupResets: number;
  deliveryTimeouts: number;
}

/** What a want keeps to say how far behind it was at a catch-up: its arrival delay now and its least, in ms. */
interface WantDelay {
  wantedAt: number;
  trackProperties: SubscribeOk['trackProperties'];
  least?: number;
  latest?: number;
}

/** The MOQT stream reset code DELIVERY_TIMEOUT (draft-22 §12.5). */
const DELIVERY_TIMEOUT_CODE = 0x2;

/**
 * What a supervised subscriber reports about its wants, under the scope it is given: a `want` event at each turn of a
 * want's life (`subscribed`, `first-object` with how long it took, `restarted` with the reason and, for a catch-up,
 * how far behind it was, `switched` with how long the replacement took to its first object, `publisher-done`,
 * `error`), and a `wants` section with each want's track, whether it is subscribed, the objects it delivered, its
 * restarts, its first object's delay and its reset groups.
 */
export function wantDiagnostics(diagnostics: Diagnostics) {
  const wants = new Map<string, WantState>();
  const delays = new Map<string, WantDelay>();
  diagnostics.section('wants', () => Object.fromEntries([...wants].map(([key, state]) => [key, { ...state }])));
  return {
    /**
     * The handlers to deliver to: `handlers`, each lifecycle callback reported first. `switchMs` says how long the
     * move that just cut over took, from its replacement's SUBSCRIBE to the object that cut over.
     */
    observe(key: string, track: string, handlers: WantHandlers, switchMs: () => number | undefined): WantHandlers {
      const state: WantState = wants.get(key) ?? {
        track,
        subscribed: false,
        objects: 0,
        restarts: 0,
        groupResets: 0,
        deliveryTimeouts: 0,
      };
      state.track = track;
      wants.set(key, state);
      const delay: WantDelay = delays.get(key) ?? { wantedAt: performance.now(), trackProperties: [] };
      delays.set(key, delay);
      const fresh = (trackProperties: SubscribeOk['trackProperties']) => {
        delay.trackProperties = trackProperties;
        delete delay.least;
        delete delay.latest;
      };
      return {
        ...handlers,
        onSubscribed(info, context) {
          state.subscribed = true;
          fresh(info.trackProperties);
          diagnostics.event('want', {
            want: key,
            what: 'subscribed',
            namespace: context.namespace.join('/'),
            track: state.track,
          });
          handlers.onSubscribed?.(info, context);
        },
        onObject(object) {
          state.objects++;
          const now = performance.now();
          if (state.firstObjectMs === undefined) {
            state.firstObjectMs = Math.round(now - delay.wantedAt);
            diagnostics.event('want', { want: key, what: 'first-object', firstObjectMs: state.firstObjectMs });
          }
          const mediaMs = mediaTimeMs(object.properties, delay.trackProperties);
          if (mediaMs !== undefined) {
            delay.latest = now - mediaMs;
            delay.least = Math.min(delay.least ?? delay.latest, delay.latest);
          }
          handlers.onObject?.(object);
        },
        onSubgroupReset(info) {
          state.groupResets++;
          if (streamErrorCode(info.error) === DELIVERY_TIMEOUT_CODE) state.deliveryTimeouts++;
          handlers.onSubgroupReset?.(info);
        },
        onPublisherDone(info) {
          diagnostics.event('want', { want: key, what: 'publisher-done', statusCode: info.statusCode });
          handlers.onPublisherDone?.(info);
        },
        onRestarted(info) {
          state.subscribed = false;
          state.restarts++;
          state.lastRestart = info.reason;
          const behind =
            info.reason === 'catch-up' && delay.latest !== undefined && delay.least !== undefined
              ? { behindMs: Math.round(delay.latest - delay.least) }
              : {};
          fresh([]);
          diagnostics.event('want', { want: key, what: 'restarted', reason: info.reason, ...behind });
          handlers.onRestarted?.(info);
        },
        onSwitched(info) {
          fresh(info.subscribed.trackProperties);
          const ms = switchMs();
          diagnostics.event('want', {
            want: key,
            what: 'switched',
            from: [...info.from.namespace, info.from.track].join('/'),
            to: [...info.to.namespace, info.to.track].join('/'),
            ...(ms === undefined ? {} : { firstObjectMs: ms }),
          });
          handlers.onSwitched?.(info);
        },
        onError(error) {
          state.subscribed = false;
          diagnostics.event('want', { want: key, what: 'error', error: describeFailure(error) });
          handlers.onError?.(error);
        },
      };
    },
    /** The want is gone: drop it from the section. */
    forget(key: string) {
      wants.delete(key);
      delays.delete(key);
    },
    /** The subscriber stopped: every want is gone. */
    forgetAll() {
      wants.clear();
      delays.clear();
    },
  };
}

/** The code a stream error carries (TA-22): an integer `streamErrorCode`, or `undefined`. */
function streamErrorCode(error: unknown): number | undefined {
  const code =
    typeof error === 'object' && error !== null ? (error as { streamErrorCode?: unknown }).streamErrorCode : undefined;
  return typeof code === 'number' ? code : undefined;
}

/** LOC's Timestamp and Timescale property types (draft-ietf-moq-loc). */
const LOC_TIMESTAMP = 0x10;
const LOC_TIMESCALE = 0x08;

/** One property as either a publisher or a subscriber holds it. */
type AnyProperty = { readonly type: number; readonly value: unknown };

/** The first integer value of `type` among `properties`. */
function integerProperty(properties: readonly AnyProperty[] | undefined, type: number): number | undefined {
  for (const property of properties ?? []) {
    if (property.type === type && typeof property.value === 'number') return property.value;
  }
  return undefined;
}

/**
 * An object's media time in milliseconds: its LOC Timestamp (0x10) in the Timescale (0x08) its own properties declare,
 * else the track's, else microseconds (LOC's default). `undefined` without a Timestamp.
 */
export function mediaTimeMs(
  properties: readonly AnyProperty[] | undefined,
  trackProperties?: readonly AnyProperty[],
): number | undefined {
  const timestamp = integerProperty(properties, LOC_TIMESTAMP);
  if (timestamp === undefined) return undefined;
  const timescale = integerProperty(properties, LOC_TIMESCALE) ?? integerProperty(trackProperties, LOC_TIMESCALE);
  return (timestamp * 1000) / (timescale && timescale > 0 ? timescale : 1_000_000);
}

/**
 * The shortest interval that is a gap, how many usual intervals one must be, and how many intervals set the usual one
 * (their median) before any gap is judged, so a slow cadence is learned, not reported.
 */
const GAP_MIN_MS = 250;
const GAP_INTERVALS = 8;
const GAP_LEARN = 4;

/**
 * Judges intervals between frames: a gap is over 250 ms and over eight times the usual interval, which is the median
 * of the first four and then a moving average of the intervals that were not gaps. Returns whether `intervalMs` is one.
 */
export function gapJudge(): (intervalMs: number) => boolean {
  let usualMs: number | undefined;
  const learning: number[] = [];
  return (intervalMs) => {
    if (usualMs === undefined) {
      learning.push(intervalMs);
      if (learning.length === GAP_LEARN) usualMs = learning.sort((a, b) => a - b)[Math.floor(GAP_LEARN / 2)];
      return false;
    }
    if (intervalMs > Math.max(GAP_MIN_MS, GAP_INTERVALS * usualMs)) return true;
    // A gap does not move the average.
    usualMs = usualMs * 0.95 + intervalMs * 0.05;
    return false;
  };
}

/** How far back the delay spread looks, and the most samples it keeps. */
const SPREAD_WINDOW_MS = 5_000;
const SPREAD_SAMPLES = 2_048;

/**
 * The spread of arrival delays over the last five seconds: each object's arrival time less its media time, relative to
 * the least in the window, so the two clocks need no sync. `read` gives the median, the 95th percentile and the most,
 * in whole milliseconds, or `undefined` with no samples in the window.
 */
export function delaySpread() {
  const samples: { at: number; delayMs: number }[] = [];
  return {
    add(at: number, delayMs: number) {
      samples.push({ at, delayMs });
      if (samples.length > SPREAD_SAMPLES) samples.shift();
    },
    clear() {
      samples.length = 0;
    },
    read(now: number): { p50: number; p95: number; max: number } | undefined {
      while (samples.length > 0 && (samples[0] as { at: number }).at < now - SPREAD_WINDOW_MS) samples.shift();
      if (samples.length === 0) return undefined;
      const least = Math.min(...samples.map((sample) => sample.delayMs));
      const spread = samples.map((sample) => sample.delayMs - least).sort((a, b) => a - b);
      // Nearest rank: the smallest value with at least that share of the samples at or below it.
      const at = (q: number) => Math.round(spread[Math.max(0, Math.ceil(q * spread.length) - 1)] as number);
      return { p50: at(0.5), p95: at(0.95), max: Math.round(spread[spread.length - 1] as number) };
    },
  };
}

/** How many of the newest locations a reorder count keeps, to say how long an overtaken object waited. */
const REORDER_KEPT = 256;

/**
 * Counts objects that arrive after a later one of the same subscription (by group, then object), and the longest any
 * of them arrived after the first object that overtook it.
 */
export function reorderCount() {
  /** The newest location so far each time it moved, with when: ascending by location. */
  const advances: { group: number; object: number; at: number }[] = [];
  let reordered = 0;
  let longestWaitMs = 0;
  const compare = (a: { group: number; object: number }, b: { group: number; object: number }) =>
    a.group - b.group || a.object - b.object;
  return {
    arrived(object: Pick<MoqtObject, 'groupId' | 'objectId'>, at: number) {
      const here = { group: object.groupId, object: object.objectId };
      const newest = advances[advances.length - 1];
      if (newest === undefined || compare(here, newest) > 0) {
        advances.push({ ...here, at });
        if (advances.length > REORDER_KEPT) advances.shift();
        return;
      }
      if (compare(here, newest) === 0) return;
      reordered++;
      // The first object past this one; an older one than kept is at least as old as the oldest kept.
      const overtook = advances.find((advance) => compare(advance, here) > 0) ?? advances[0];
      if (overtook) longestWaitMs = Math.max(longestWaitMs, at - overtook.at);
    },
    /** A new subscription numbers its own groups. */
    restart() {
      advances.length = 0;
    },
    get reordered() {
      return reordered;
    },
    get longestWaitMs() {
      return Math.round(longestWaitMs);
    },
  };
}

/** A `diagnostics` option: `undefined`, or a recorder or scope from `createDiagnostics`; anything else is a TypeError. */
export function diagnosticsOption(value: unknown): Diagnostics | undefined {
  if (value === undefined) return undefined;
  const given = value as Partial<Diagnostics> | null;
  if (
    typeof given !== 'object' ||
    given === null ||
    typeof given.event !== 'function' ||
    typeof given.section !== 'function' ||
    typeof given.scope !== 'function'
  ) {
    throw new TypeError('diagnostics must come from createDiagnostics');
  }
  return given as Diagnostics;
}
