import type { Member, MembersChange } from './members.js';
import { synchronousCallback } from './subscription.js';
import type { RestartReason, SupervisedSubscriber } from './supervised-subscriber.js';
import { trackName } from './validation.js';

/** The presence track's default name. */
export const PRESENCE_TRACK = 'presence';

/**
 * How often a presence publisher resends an unchanged document by default. A view calls a member stale after three
 * heartbeats with nothing new (6 s), before the relay withdraws a crashed publisher at its idle timeout (about 10 s).
 */
export const PRESENCE_HEARTBEAT_MS = 2000;

/**
 * Presence's Default Publisher Priority and SUBSCRIBER_PRIORITY: between audio (16) and video (96), so heartbeats do
 * not queue behind video on a congested link and turn congestion into false stale reports.
 */
const PRESENCE_PRIORITY = 32;
/** How long a view's first report waits for the documents of the members present when `members()` loaded. */
const PRESENCE_LOAD_LIMIT_MS = 1000;
const STALE_HEARTBEATS = 3;
const MIN_HEARTBEAT_MS = 100;
/** Three heartbeats, the stale window, must fit a timer. */
const MAX_HEARTBEAT_MS = 715_827_882;

/** One member with a presence document, as a view reports it. Replaced, never changed, when anything in it changes. */
export interface PresenceMember<T> extends Member {
  /** The newest document read from the member. */
  readonly document: T;
  /** `Date.now()` when the view first read the member's document; kept across updates, staleness and rotations. */
  readonly since: number;
  /**
   * No new document for three heartbeats while the view's subscription to it was up. The member is still announced;
   * most UIs hide it.
   */
  readonly stale: boolean;
}

/** What `onChanged` receives: what changed since the last call, and who is present now. */
export interface PresenceChange<T> {
  /** Members whose first document arrived. On the first call (`initial`), everyone with one at the load. */
  readonly joined: readonly PresenceMember<T>[];
  /** Reported members that left, as last reported. A member that never had a document leaves unreported. */
  readonly left: readonly PresenceMember<T>[];
  /** Members whose document JSON changed. A stale member with a changed document is in `resumed` too. */
  readonly updated: readonly PresenceMember<T>[];
  /** Members that went stale. */
  readonly stale: readonly PresenceMember<T>[];
  /** Stale members with a new document. */
  readonly resumed: readonly PresenceMember<T>[];
  /**
   * Every reported member after this change, by id, in the order `members()` reported them. The same map as
   * `present`.
   */
  readonly present: ReadonlyMap<string, PresenceMember<T>>;
  /** True on the first call only: the view has just loaded. */
  readonly initial: boolean;
}

/** What a presence view follows. */
export interface PresenceViewOptions<T> {
  /** The root whose members to follow, as for `members()`: each member's supervised publisher serves `<root>/<id>`. */
  root: readonly string[];
  /** The track name. Default {@link PRESENCE_TRACK}. */
  track?: string;
  /**
   * The publishers' heartbeat, in milliseconds: an integer from 100 to 715827882. Default
   * {@link PRESENCE_HEARTBEAT_MS}. A member is stale after three heartbeats with no new document.
   */
  heartbeatMs?: number;
  /**
   * Check a peer's parsed JSON and return its document, or throw. A throw skips that document: the member keeps the
   * one it had, or stays unreported. Without it, documents are the parsed JSON, typed `unknown`.
   */
  parse?(value: unknown): T;
}

/** The callbacks a presence view delivers to. Like `MembersHandlers`, they must be synchronous. */
export interface PresenceViewHandlers<T> {
  /**
   * The view loaded, or a member joined, left, changed its document, went stale or resumed. The first call is the
   * load and runs once, after every member present at `members()`'s load has a document or after 1 s; every later
   * call names at least one member. One input calls it at most once, with everything that input changed. A
   * member's publisher rotating with the same document, the relay repeating a document, and the subscriber's
   * reconnects and rotations call nothing.
   */
  onChanged?(change: PresenceChange<T>): void;
  /** The members view failed in a way no wait fixes, such as the token not allowing the root. The view is closed. */
  onError?(error: unknown): void;
}

/** A live roster: the members under a root with a presence document. Create one with {@link createPresenceView}. */
export interface PresenceView<T> {
  /** False until the first `onChanged`. Stays true across reconnects. */
  readonly loaded: boolean;
  /**
   * Reported members by id; empty until loaded. The same map until the next `onChanged`, so a UI can compare it by
   * identity.
   */
  readonly present: ReadonlyMap<string, PresenceMember<T>>;
  /**
   * Stop: unwant every member's presence track and close the members view. Idempotent; nothing is delivered after it,
   * even when called from `onChanged`.
   */
  close(): void;
}

/**
 * Follow the presence documents of everyone under a root: `subscriber.members({ root })` plus one want per member
 * that follows the member's supervised publisher to its newest generation. A member is reported once its first
 * readable document arrives, and goes stale after three heartbeats with nothing new while the view's subscription is
 * up. Arguments are checked at once; handlers run from the next turn on.
 *
 * ```ts
 * const room = createPresenceView(subscriber, { root: ['room', id], parse: parseProfile }, {
 *   onChanged: ({ present }) => render([...present.values()].filter((member) => !member.stale)),
 * });
 * ```
 */
export function createPresenceView(
  subscriber: Pick<SupervisedSubscriber, 'members' | 'want' | 'unwant'>,
  options: PresenceViewOptions<unknown> & { parse?: undefined },
  handlers?: PresenceViewHandlers<unknown>,
): PresenceView<unknown>;
export function createPresenceView<T>(
  subscriber: Pick<SupervisedSubscriber, 'members' | 'want' | 'unwant'>,
  options: PresenceViewOptions<T> & { parse(value: unknown): T },
  handlers?: PresenceViewHandlers<T>,
): PresenceView<T>;
export function createPresenceView<T>(
  subscriber: Pick<SupervisedSubscriber, 'members' | 'want' | 'unwant'>,
  options: PresenceViewOptions<T>,
  handlers: PresenceViewHandlers<T> = {},
): PresenceView<T> {
  if (typeof options !== 'object' || options === null) throw new TypeError('options must be an object');
  const track = trackName(options.track ?? PRESENCE_TRACK);
  const heartbeatMs = heartbeatOption(options.heartbeatMs);
  const { parse } = options;
  if (parse !== undefined && typeof parse !== 'function') throw new TypeError('parse must be a function');
  if (typeof handlers !== 'object' || handlers === null)
    throw new TypeError('presence view handlers must be an object');
  const onChanged = handler('onChanged', handlers.onChanged, handlers);
  const onError = handler('onError', handlers.onError, handlers);
  const ctx: PresenceContext<T> = { staleAfterMs: STALE_HEARTBEATS * heartbeatMs, decode: decoder(parse) };

  const view = ++views;
  let joins = 0;
  let phase: ViewPhase<T> = { kind: 'loading' };
  const inbox: PresenceInput[] = [];
  let draining = false;
  const wants = new Map<string, string>();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let armedFor: number | undefined;

  const arm = () => {
    const due = deadline(phase);
    if (due === armedFor) return;
    clearTimeout(timer);
    armedFor = due;
    if (due === undefined) return;
    timer = setTimeout(
      () => {
        armedFor = undefined;
        feed({ kind: 'tick', at: performance.now() });
      },
      Math.max(0, due - performance.now()),
    );
  };

  const feed = (input: PresenceInput, first = false) => {
    if (first) inbox.unshift(input);
    else inbox.push(input);
    if (draining) return;
    draining = true;
    try {
      for (let next = inbox.shift(); next; next = inbox.shift()) {
        const step = transition(phase, next, ctx);
        phase = step.phase;
        for (const effect of step.effects) run(effect);
      }
    } finally {
      draining = false;
      arm();
    }
  };

  const open = (member: Member) => {
    const { id } = member;
    const key = `\0presence:${view}:${++joins}:${id}`;
    wants.set(id, key);
    // A late callback from the want of a member that left, even one that has joined again, is not this want's.
    const from = (input: PresenceInput) => {
      if (wants.get(id) === key) feed(input);
    };
    subscriber.want(
      key,
      { namespace: member.namespace, track, generations: true, parameters: { subscriberPriority: PRESENCE_PRIORITY } },
      {
        onSubscribed: (_, { namespace }) =>
          from({ kind: 'subscribed', id, generation: generationOf(namespace), at: performance.now() }),
        onSwitched: ({ to }) => from({ kind: 'switched', id, generation: generationOf(to.namespace) }),
        onRestarted: ({ reason }) => from({ kind: 'restarted', id, reason }),
        onObject: ({ status, groupId, payload }) => {
          if (status !== 'normal') return;
          from({ kind: 'object', id, group: groupId, payload, at: performance.now(), date: Date.now() });
        },
      },
    );
  };

  const run = (effect: PresenceEffect<T>) => {
    switch (effect.kind) {
      case 'want':
        open(effect.member);
        return;
      case 'unwant': {
        const key = wants.get(effect.id);
        wants.delete(effect.id);
        if (key !== undefined) subscriber.unwant(key);
        return;
      }
      case 'emit':
        deliver('onChanged', () => onChanged?.(effect.change));
        return;
      case 'error':
        deliver('onError', () => onError?.(effect.error));
        return;
      case 'close-members':
        members.close();
        return;
    }
  };

  const members = subscriber.members(
    { root: options.root },
    {
      onChanged: (change) => feed({ kind: 'members', change, at: performance.now() }),
      onError: (error) => feed({ kind: 'members-error', error }),
    },
  );
  return {
    get loaded() {
      return presentOf(phase) !== undefined;
    },
    get present() {
      return presentOf(phase) ?? NOBODY;
    },
    close() {
      feed({ kind: 'close' }, true);
    },
  };
}

let views = 0;

function handler<A extends unknown[]>(
  name: string,
  callback: ((...args: A) => void) | undefined,
  receiver: unknown,
): ((...args: A) => void) | undefined {
  if (callback === undefined) return undefined;
  if (typeof callback !== 'function') throw new TypeError(`${name} must be a function`);
  return synchronousCallback(name, callback, receiver);
}

function deliver(name: string, call: () => void): void {
  try {
    call();
  } catch (error) {
    // biome-ignore lint/suspicious/noConsole: the application's callback failed and has no caller to report to
    console.error(new Error(`${name} threw; the presence view carries on`, { cause: error }));
  }
}

function heartbeatOption(value: unknown): number {
  if (value === undefined) return PRESENCE_HEARTBEAT_MS;
  if (typeof value !== 'number') throw new TypeError('heartbeatMs must be a number of milliseconds');
  if (!Number.isInteger(value) || value < MIN_HEARTBEAT_MS || value > MAX_HEARTBEAT_MS)
    throw new RangeError(`heartbeatMs must be an integer from ${MIN_HEARTBEAT_MS} to ${MAX_HEARTBEAT_MS} milliseconds`);
  return value;
}

const DECODER = new TextDecoder('utf-8', { fatal: true });

function decoder<T>(parse: ((value: unknown) => T) | undefined): PresenceContext<T>['decode'] {
  return (payload) => {
    try {
      const value: unknown = JSON.parse(DECODER.decode(payload));
      return { document: parse ? parse(value) : (value as T) };
    } catch {
      return undefined;
    }
  };
}

function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
  return a.length === b.length && a.every((byte, index) => byte === b[index]);
}

function generationOf(namespace: readonly string[]): string {
  return namespace[namespace.length - 1] as string;
}

/** Where an object sits in a member's history: its publisher generation, then its group. */
interface ObjectKey {
  readonly generation: string;
  readonly group: number;
}

/** A live member's stale clock: due at a time, or stopped while the view's own subscription is down. */
type Clock = { readonly kind: 'running'; readonly dueAt: number } | { readonly kind: 'paused' };

/**
 * One member `members()` reports. `route` is the generation of the want's current subscription, which keys every
 * object; `last` is the key of the newest object counted.
 */
type MemberState<T> =
  /** No readable document yet: never reported. */
  | {
      readonly kind: 'awaiting';
      readonly member: Member;
      readonly route: string | undefined;
      readonly last: ObjectKey | undefined;
    }
  /** Reported, and heard from within the stale window. */
  | {
      readonly kind: 'live';
      readonly route: string | undefined;
      readonly last: ObjectKey;
      readonly bytes: Uint8Array;
      readonly entry: PresenceMember<T>;
      readonly clock: Clock;
    }
  /** Reported as stale: only a fresh object brings it back. */
  | {
      readonly kind: 'stale';
      readonly route: string | undefined;
      readonly last: ObjectKey;
      readonly bytes: Uint8Array;
      readonly entry: PresenceMember<T>;
    };

type States<T> = ReadonlyMap<string, MemberState<T>>;

type ViewPhase<T> =
  /** `members()` has not loaded. */
  | { readonly kind: 'loading' }
  /** `members()` loaded; the first report waits for `waiting` (present at that load, no document yet) or `loadBy`. */
  | {
      readonly kind: 'gathering';
      readonly states: States<T>;
      readonly waiting: ReadonlySet<string>;
      readonly loadBy: number;
    }
  | { readonly kind: 'live'; readonly states: States<T>; readonly present: ReadonlyMap<string, PresenceMember<T>> }
  /** `present` is undefined when the view closed before it loaded. */
  | { readonly kind: 'closed'; readonly present: ReadonlyMap<string, PresenceMember<T>> | undefined };

/** What reaches the reducer. `at` is `performance.now()`; `date` is `Date.now()`. */
type PresenceInput =
  | { readonly kind: 'members'; readonly change: MembersChange; readonly at: number }
  | { readonly kind: 'members-error'; readonly error: unknown }
  | { readonly kind: 'subscribed'; readonly id: string; readonly generation: string; readonly at: number }
  | { readonly kind: 'switched'; readonly id: string; readonly generation: string }
  | { readonly kind: 'restarted'; readonly id: string; readonly reason: RestartReason }
  | {
      readonly kind: 'object';
      readonly id: string;
      readonly group: number;
      readonly payload: Uint8Array;
      readonly at: number;
      readonly date: number;
    }
  | { readonly kind: 'tick'; readonly at: number }
  | { readonly kind: 'close' };

type PresenceEffect<T> =
  | { readonly kind: 'want'; readonly member: Member }
  | { readonly kind: 'unwant'; readonly id: string }
  | { readonly kind: 'emit'; readonly change: PresenceChange<T> }
  | { readonly kind: 'error'; readonly error: unknown }
  | { readonly kind: 'close-members' };

interface PresenceContext<T> {
  readonly staleAfterMs: number;
  /** The document in a payload, or `undefined` when it is not one. */
  decode(payload: Uint8Array): { readonly document: T } | undefined;
}

type Step<T> = { phase: ViewPhase<T>; effects: PresenceEffect<T>[] };
type Event = 'joined' | 'left' | 'updated' | 'stale' | 'resumed';
type Report<T> = { readonly event: Event; readonly member: PresenceMember<T> };
type MemberStep<T> = { state: MemberState<T>; reports: Report<T>[] };
type MemberInput = Extract<PresenceInput, { kind: 'subscribed' | 'switched' | 'restarted' | 'object' }>;

const NOBODY: ReadonlyMap<string, never> = new Map<string, never>();
const PAUSED: Clock = { kind: 'paused' };

const stay = <T>(phase: ViewPhase<T>): Step<T> => ({ phase, effects: [] });
const quiet = <T>(state: MemberState<T>): MemberStep<T> => ({ state, reports: [] });

function entryOf<T>(member: Member, document: T, since: number, stale: boolean): PresenceMember<T> {
  return Object.freeze({ id: member.id, namespace: member.namespace, document, since, stale });
}

/**
 * The key of an object in group `group` of the current subscription, when it is newer than what `state` counted. A
 * replay repeats the last key and an out-of-order group is smaller, so neither counts. Generations compare as strings,
 * which sort by start time: a live member ignores an older, retiring generation so its document never regresses, and a
 * stale member takes any other generation, since its publisher came back, even on a clock behind.
 */
function freshKey<T>(state: MemberState<T>, group: number): ObjectKey | undefined {
  const { route, last } = state;
  if (route === undefined) return undefined;
  const key = { generation: route, group };
  if (!last) return key;
  if (route === last.generation) return group > last.group ? key : undefined;
  return state.kind === 'stale' || route > last.generation ? key : undefined;
}

function heard<T>(
  state: MemberState<T>,
  input: Extract<PresenceInput, { kind: 'object' }>,
  ctx: PresenceContext<T>,
): MemberStep<T> {
  const last = freshKey(state, input.group);
  if (!last) return quiet(state);
  const clock: Clock = { kind: 'running', dueAt: input.at + ctx.staleAfterMs };
  if (state.kind === 'awaiting') {
    const read = ctx.decode(input.payload);
    if (!read) return quiet({ ...state, last });
    const entry = entryOf(state.member, read.document, input.date, false);
    return {
      state: { kind: 'live', route: state.route, last, bytes: input.payload, entry, clock },
      reports: [{ event: 'joined', member: entry }],
    };
  }
  const read = sameBytes(state.bytes, input.payload) ? undefined : ctx.decode(input.payload);
  if (state.kind === 'live' && !read) return quiet({ ...state, last, clock });
  const { entry: was } = state;
  const entry = entryOf(was, read ? read.document : was.document, was.since, false);
  const events: Event[] = state.kind === 'stale' ? ['resumed'] : [];
  if (read) events.push('updated');
  return {
    state: { kind: 'live', route: state.route, last, bytes: read ? input.payload : state.bytes, entry, clock },
    reports: events.map((event) => ({ event, member: entry })),
  };
}

function memberStep<T>(state: MemberState<T>, input: MemberInput, ctx: PresenceContext<T>): MemberStep<T> {
  switch (input.kind) {
    case 'subscribed': {
      const routed = { ...state, route: input.generation };
      if (routed.kind !== 'live' || routed.clock.kind === 'running') return quiet(routed);
      return quiet({ ...routed, clock: { kind: 'running', dueAt: input.at + ctx.staleAfterMs } });
    }
    case 'switched':
      return quiet({ ...state, route: input.generation });
    case 'restarted':
      if (state.kind !== 'live' || input.reason !== 'connection') return quiet(state);
      return quiet({ ...state, clock: PAUSED });
    case 'object':
      return heard(state, input, ctx);
  }
}

function due<T>(state: MemberState<T>, at: number): MemberStep<T> {
  if (state.kind !== 'live' || state.clock.kind !== 'running' || at < state.clock.dueAt) return quiet(state);
  const { route, last, bytes, entry: was } = state;
  const entry = entryOf(was, was.document, was.since, true);
  return { state: { kind: 'stale', route, last, bytes, entry }, reports: [{ event: 'stale', member: entry }] };
}

type Applied<T> = { states: States<T>; reports: Report<T>[]; effects: PresenceEffect<T>[] };

function follow<T>(states: States<T>, change: MembersChange): Applied<T> {
  const next = new Map(states);
  const reports: Report<T>[] = [];
  const effects: PresenceEffect<T>[] = [];
  for (const { id } of change.left) {
    const state = next.get(id);
    if (!state) continue;
    next.delete(id);
    effects.push({ kind: 'unwant', id });
    if (state.kind !== 'awaiting') reports.push({ event: 'left', member: state.entry });
  }
  for (const member of change.joined) {
    if (next.has(member.id)) continue;
    next.set(member.id, { kind: 'awaiting', member, route: undefined, last: undefined });
    effects.push({ kind: 'want', member });
  }
  return { states: next, reports, effects };
}

function apply<T>(
  states: States<T>,
  input: Exclude<PresenceInput, { kind: 'close' | 'members-error' }>,
  ctx: PresenceContext<T>,
): Applied<T> {
  if (input.kind === 'members') return follow(states, input.change);
  if (input.kind === 'tick') {
    const next = new Map(states);
    const reports: Report<T>[] = [];
    for (const [id, state] of states) {
      const step = due(state, input.at);
      next.set(id, step.state);
      reports.push(...step.reports);
    }
    return { states: reports.length > 0 ? next : states, reports, effects: [] };
  }
  const state = states.get(input.id);
  if (!state) return { states, reports: [], effects: [] };
  const step = memberStep(state, input, ctx);
  if (step.state === state) return { states, reports: [], effects: [] };
  return { states: new Map(states).set(input.id, step.state), reports: step.reports, effects: [] };
}

function reported<T>(states: States<T>): ReadonlyMap<string, PresenceMember<T>> {
  const present = new Map<string, PresenceMember<T>>();
  for (const [id, state] of states) if (state.kind !== 'awaiting') present.set(id, state.entry);
  return present;
}

function changeOf<T>(
  reports: readonly Report<T>[],
  present: ReadonlyMap<string, PresenceMember<T>>,
  initial: boolean,
): PresenceChange<T> {
  const of = (event: Event) => reports.filter((report) => report.event === event).map((report) => report.member);
  return {
    joined: of('joined'),
    left: of('left'),
    updated: of('updated'),
    stale: of('stale'),
    resumed: of('resumed'),
    present,
    initial,
  };
}

function gather<T>(
  phase: Extract<ViewPhase<T>, { kind: 'gathering' }>,
  states: States<T>,
  at: number | undefined,
  effects: PresenceEffect<T>[],
): Step<T> {
  const waiting = new Set([...phase.waiting].filter((id) => states.get(id)?.kind === 'awaiting'));
  if (waiting.size > 0 && (at === undefined || at < phase.loadBy)) {
    return { phase: { ...phase, states, waiting }, effects };
  }
  const present = reported(states);
  const joined = [...present.values()].map((member): Report<T> => ({ event: 'joined', member }));
  const loaded = changeOf(joined, present, true);
  return { phase: { kind: 'live', states, present }, effects: [...effects, { kind: 'emit', change: loaded }] };
}

/**
 * The view's transition. Pure: times arrive on inputs and effects leave as data. `present` is rebuilt only when a
 * reported member changed, so its identity changes exactly when `onChanged` runs, and the emit comes last so a
 * `close()` from `onChanged` follows every other effect of the step.
 */
function transition<T>(phase: ViewPhase<T>, input: PresenceInput, ctx: PresenceContext<T>): Step<T> {
  if (phase.kind === 'closed') return stay(phase);
  if (input.kind === 'close' || input.kind === 'members-error') {
    const states = phase.kind === 'loading' ? NOBODY : phase.states;
    return {
      phase: { kind: 'closed', present: phase.kind === 'live' ? phase.present : undefined },
      effects: [
        ...[...states.keys()].map((id): PresenceEffect<T> => ({ kind: 'unwant', id })),
        { kind: 'close-members' },
        ...(input.kind === 'members-error' ? [{ kind: 'error' as const, error: input.error }] : []),
      ],
    };
  }
  if (phase.kind === 'loading') {
    if (input.kind !== 'members' || !input.change.initial) return stay(phase);
    const { states, effects } = follow<T>(NOBODY, input.change);
    const gathering = {
      kind: 'gathering',
      states,
      waiting: new Set(states.keys()),
      loadBy: input.at + PRESENCE_LOAD_LIMIT_MS,
    } as const;
    return gather(gathering, states, undefined, effects);
  }
  const { states, reports, effects } = apply(phase.states, input, ctx);
  if (phase.kind === 'gathering') {
    if (states === phase.states && input.kind !== 'tick') return stay(phase);
    return gather(phase, states, input.kind === 'tick' ? input.at : undefined, effects);
  }
  if (reports.length === 0) return states === phase.states ? stay(phase) : { phase: { ...phase, states }, effects };
  const present = reported(states);
  return {
    phase: { kind: 'live', states, present },
    effects: [...effects, { kind: 'emit', change: changeOf(reports, present, false) }],
  };
}

/** When the view's timer is next due: the load limit while gathering, and every running stale clock. */
function deadline<T>(phase: ViewPhase<T>): number | undefined {
  if (phase.kind !== 'gathering' && phase.kind !== 'live') return undefined;
  let next = phase.kind === 'gathering' ? phase.loadBy : undefined;
  for (const state of phase.states.values()) {
    if (state.kind !== 'live' || state.clock.kind !== 'running') continue;
    if (next === undefined || state.clock.dueAt < next) next = state.clock.dueAt;
  }
  return next;
}

function presentOf<T>(phase: ViewPhase<T>): ReadonlyMap<string, PresenceMember<T>> | undefined {
  return phase.kind === 'live' || phase.kind === 'closed' ? phase.present : undefined;
}
