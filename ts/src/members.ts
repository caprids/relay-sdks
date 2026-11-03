/**
 * Who is present under a root, with no roster server: each namespace one field below the root is a member, and a
 * namespace one field further is that member's generation, as a supervised publisher announces it. A view follows the
 * root's discovery across the subscriber's reconnects and rotations and tells the application only when members join
 * or leave. `transition` decides everything; `openMembers` is the shell that turns its effects into watches, timers and
 * handler calls.
 */
import { type DiscoveryIndex, type DiscoveryWatch, discoveryEnding, refusedForGood } from './discovery.js';
import { SubscriptionConnectionClosedError } from './subscription.js';
import { backoffDelay, RESUBSCRIBE_BACKOFF, sleep, whenAborted } from './supervision.js';

/** One participant under a root: a supervised publisher's root, or an unsupervised publisher's namespace. */
export interface Member {
  /** The field one below the root, as the participant's publisher chose it. */
  readonly id: string;
  /** The root followed by `id`. Want it with `generations: true` to follow the member's supervised publisher. */
  readonly namespace: readonly string[];
}

/** What `onChanged` receives: who joined and who left since the last call, and who is present now. */
export interface MembersChange {
  /**
   * Members who arrived. On the first call (`initial`), everyone present when the view loaded, possibly no one: the
   * load is the change from nobody known, so one code path handles "a member is here".
   */
  readonly joined: readonly Member[];
  /** Members who went. Empty on the first call. */
  readonly left: readonly Member[];
  /** Everyone present after this change, by id, in the order the view first saw them. The same map as `present`. */
  readonly present: ReadonlyMap<string, Member>;
  /** True on the first call only: the view has just loaded. A UI that announces joins skips this one. */
  readonly initial: boolean;
}

/** What a members view follows. */
export interface MembersOptions {
  /**
   * The root whose members to follow: each namespace one field below it is a member, and one field further is that
   * member's generation. Same rules as a namespace, with room for the two fields under it. The subscriber's token must
   * allow discovering it.
   */
  root: readonly string[];
}

/** The callbacks a members view delivers to. Like `WantHandlers`, they must be synchronous. */
export interface MembersHandlers {
  /**
   * The view loaded, or members joined or left. The first call is the load and runs once, even for an empty root;
   * every later call names at least one member in `joined` or `left`. A member leaves once it has had no namespace
   * for 100 ms, so a rotation whose withdrawal arrives just before the new generation's announce calls nothing. A
   * member's generations changing, the relay repeating an announce, and the subscriber's reconnects and rotations call
   * nothing; a reconnect calls it with the difference, if there is one.
   */
  onChanged?(change: MembersChange): void;
  /** A discovery failure no wait fixes, such as the token not allowing the root. The view is closed. */
  onError?(error: unknown): void;
}

/** A live view of who is present under a root. Create one with `SupervisedSubscriber.members`. */
export interface Members {
  /**
   * False until the root's first discovery has reported everything announced under it; then `present` is empty only
   * when nobody is there. Stays true across reconnects, while the last settled view is held.
   */
  readonly loaded: boolean;
  /**
   * Who is present, by id: empty until loaded, and the last settled view while a reconnect's discovery loads. A member
   * whose last namespace went stays until it is reported as left. The same map until the next `onChanged`, so a UI can
   * compare it by identity.
   */
  readonly present: ReadonlyMap<string, Member>;
  /** Stop following the root. Idempotent; nothing is delivered after it, even when called from `onChanged`. */
  close(): void;
}

/** Per member id, the fields announced under it: each generation, and '' for the member's own namespace. */
export type Presence = Map<string, Set<string>>;

/**
 * Which member a namespace under `root` is about, and the field it adds: '' for the member's own namespace (an
 * unsupervised publisher), the generation one field deeper. `undefined` for the root itself and anything deeper. ''
 * can never be a generation, since namespace fields are non-empty.
 */
export function memberField(
  root: readonly string[],
  namespace: readonly string[],
): { id: string; field: string } | undefined {
  const id = namespace[root.length];
  if (id === undefined) return undefined;
  if (namespace.length === root.length + 1) return { id, field: '' };
  if (namespace.length === root.length + 2) return { id, field: namespace[root.length + 1] as string };
  return undefined;
}

/**
 * Apply one announce or withdrawal under `root` to `presence`. A member joins when its first field appears and leaves
 * when its last goes, so a make-before-break rotation ({g1}, {g1, g2}, {g2}) and a relay's repeat change nothing.
 */
export function applyNamespace(
  presence: Presence,
  root: readonly string[],
  type: 'announced' | 'unannounced',
  namespace: readonly string[],
): { kind: 'joined' | 'left'; id: string } | undefined {
  const at = memberField(root, namespace);
  if (!at) return undefined;
  const fields = presence.get(at.id);
  if (type === 'announced') {
    if (fields) {
      fields.add(at.field);
      return undefined;
    }
    presence.set(at.id, new Set([at.field]));
    return { kind: 'joined', id: at.id };
  }
  if (!fields?.delete(at.field) || fields.size > 0) return undefined;
  presence.delete(at.id);
  return { kind: 'left', id: at.id };
}

function member(root: readonly string[], id: string): Member {
  return Object.freeze({ id, namespace: Object.freeze([...root, id]) });
}

/** The difference between what the application was told and what a freshly loaded discovery sees. */
export function diffMembers(
  settled: ReadonlyMap<string, Member>,
  loaded: Presence,
  root: readonly string[],
): { joined: Member[]; left: Member[] } {
  const joined = [...loaded.keys()].filter((id) => !settled.has(id)).map((id) => member(root, id));
  const left = [...settled.values()].filter((known) => !loaded.has(known.id));
  return { joined, left };
}

/** What a view needs of a connection: its discovery, its end and its retirement by a rotation. */
export interface MembersConnection {
  readonly discovery: DiscoveryIndex;
  /** Aborts when the connection ends. */
  readonly signal: AbortSignal;
  /** Aborts when a rotation replaced the connection. */
  readonly retiring: AbortSignal;
}

/** One discovery of the root the view follows or loads, on one connection, with the presence it has seen there. */
export interface Source {
  readonly connection: MembersConnection;
  /** Written only by `transition`, from this source's inputs. */
  readonly presence: Presence;
}

type Settled = ReadonlyMap<string, Member>;
/** Ids in `settled` with no namespace left, each waiting out the grace before it is reported as left. */
type Leaving = ReadonlySet<string>;

/**
 * Where a view stands. `settled` is what the application was last told and the only copy: `Members.present` and
 * `Members.loaded` read it. While a source is followed live, settled's ids are its presence's ids plus `leaving`.
 */
export type MembersPhase =
  /** Nothing settled: no `onChanged` yet. `candidate` is the discovery loading, if any. */
  | { readonly kind: 'loading'; readonly candidate: Source | undefined; readonly failures: number }
  /** Settled, following one loaded source's changes. */
  | { readonly kind: 'live'; readonly settled: Settled; readonly leaving: Leaving; readonly source: Source }
  /**
   * Settled view kept while `candidate` loads; at its load the difference is delivered. `source` is the old discovery
   * still reporting changes during a rotation, `undefined` once it ended. `candidate` is `undefined` while waiting for
   * a connection or a retry delay.
   */
  | {
      readonly kind: 'replacing';
      readonly settled: Settled;
      readonly leaving: Leaving;
      readonly source: Source | undefined;
      readonly candidate: Source | undefined;
      readonly failures: number;
    }
  /** Closed by `close()`, the supervisor stopping, or a failure no wait fixes. Keeps what was settled. */
  | { readonly kind: 'closed'; readonly settled: Settled | undefined };

/** What can happen to a view, fed one at a time in arrival order. */
export type MembersInput =
  /** A live connection to load on: the first, the next after a reconnect, or the current one after a retry delay. */
  | { readonly kind: 'connection'; readonly connection: MembersConnection }
  /** A rotation retired `connection`; `next` is the connection the supervisor made current. */
  | { readonly kind: 'retiring'; readonly connection: MembersConnection; readonly next: MembersConnection | undefined }
  | {
      readonly kind: 'namespace';
      readonly source: Source;
      readonly type: 'announced' | 'unannounced';
      readonly namespace: readonly string[];
    }
  | { readonly kind: 'loaded'; readonly source: Source }
  | { readonly kind: 'ended'; readonly source: Source; readonly error: unknown }
  /** A leaving member's grace ran out. */
  | { readonly kind: 'leave-due'; readonly id: string }
  | { readonly kind: 'close' };

export type MembersEffect =
  | { readonly kind: 'open'; readonly source: Source }
  | { readonly kind: 'release'; readonly source: Source }
  | { readonly kind: 'emit'; readonly change: MembersChange }
  /** Feed `leave-due` for `id` once the grace has passed. */
  | { readonly kind: 'schedule-leave'; readonly id: string }
  | { readonly kind: 'cancel-leave'; readonly id: string }
  /** Wait for `after` to report its end, then feed the live connection. */
  | { readonly kind: 'await-connection'; readonly after: MembersConnection }
  /** Wait out the retry backoff for `attempt`, then feed the live connection. */
  | { readonly kind: 'retry'; readonly attempt: number }
  | { readonly kind: 'error'; readonly error: unknown };

type Step = { phase: MembersPhase; effects: MembersEffect[] };
type Loading = Extract<MembersPhase, { kind: 'loading' }>;
type Live = Extract<MembersPhase, { kind: 'live' }>;
type Replacing = Extract<MembersPhase, { kind: 'replacing' }>;
type Arrival = Exclude<MembersInput, { kind: 'close' }>;

const NOBODY_LEAVING: Leaving = new Set();
const stay = (phase: MembersPhase): Step => ({ phase, effects: [] });
const sourceOn = (connection: MembersConnection): Source => ({ connection, presence: new Map() });
const open = (source: Source): MembersEffect => ({ kind: 'open', source });
const release = (source: Source): MembersEffect => ({ kind: 'release', source });
const cancelLeave = (id: string): MembersEffect => ({ kind: 'cancel-leave', id });
const emit = (joined: Member[], left: Member[], present: Settled, initial = false): MembersEffect => ({
  kind: 'emit',
  change: { joined, left, present, initial },
});

function settledOf(phase: MembersPhase): Settled | undefined {
  return phase.kind === 'loading' ? undefined : phase.settled;
}

function sourcesOf(phase: MembersPhase): Source[] {
  switch (phase.kind) {
    case 'loading':
      return phase.candidate ? [phase.candidate] : [];
    case 'live':
      return [phase.source];
    case 'replacing':
      return [phase.source, phase.candidate].filter((source) => source !== undefined);
    case 'closed':
      return [];
  }
}

/** Everything a closing view lets go of: its watches and its pending leaves, none of which is reported. */
function abandon(phase: MembersPhase): MembersEffect[] {
  const leaving = phase.kind === 'live' || phase.kind === 'replacing' ? [...phase.leaving] : [];
  return [...sourcesOf(phase).map(release), ...leaving.map(cancelLeave)];
}

/** The connection a rotation moves `source` to, when `input` retires the one it is on. */
function retiredTo(source: Source, input: Extract<MembersInput, { kind: 'retiring' }>): MembersConnection | undefined {
  if (source.connection !== input.connection || input.next === input.connection) return undefined;
  return input.next;
}

/**
 * A followed source's announce or withdrawal. A member's last namespace going starts its grace instead of reporting it;
 * a namespace of a leaving member coming back ends the grace, and nothing was reported either way.
 */
function follow(
  phase: Live | Replacing,
  source: Source,
  input: Extract<MembersInput, { kind: 'namespace' }>,
  root: readonly string[],
): Step {
  const change = applyNamespace(source.presence, root, input.type, input.namespace);
  if (!change) return stay(phase);
  const { id } = change;
  if (change.kind === 'left') {
    return { phase: { ...phase, leaving: new Set(phase.leaving).add(id) }, effects: [{ kind: 'schedule-leave', id }] };
  }
  if (phase.leaving.has(id)) {
    const leaving = new Set(phase.leaving);
    leaving.delete(id);
    return { phase: { ...phase, leaving }, effects: [cancelLeave(id)] };
  }
  const joined = member(root, id);
  const settled = new Map(phase.settled).set(id, joined);
  return { phase: { ...phase, settled }, effects: [emit([joined], [], settled)] };
}

/** A leaving member's grace ran out: report it as left unless `followed` holds a namespace of it again. */
function leaveDue(phase: Live | Replacing, id: string, followed: Presence | undefined): Step {
  const gone = phase.settled.get(id);
  if (!gone || !phase.leaving.has(id) || followed?.has(id)) return stay(phase);
  const settled = new Map(phase.settled);
  settled.delete(id);
  const leaving = new Set(phase.leaving);
  leaving.delete(id);
  return { phase: { ...phase, settled, leaving }, effects: [emit([], [gone], settled)] };
}

/** A discovery the view relied on ended: wait for the next connection, retry after the backoff, or fail. */
function afterEnded(
  phase: Loading | Replacing,
  ended: Extract<MembersInput, { kind: 'ended' }>,
  effects: MembersEffect[],
): Step {
  switch (discoveryEnding(ended.error)) {
    case 'connection':
      return { phase, effects: [...effects, { kind: 'await-connection', after: ended.source.connection }] };
    case 'retry':
      return {
        phase: { ...phase, failures: phase.failures + 1 },
        effects: [...effects, { kind: 'retry', attempt: phase.failures }],
      };
    case 'fatal':
      return {
        phase: { kind: 'closed', settled: settledOf(phase) },
        effects: [...effects, ...abandon(phase), { kind: 'error', error: ended.error }],
      };
  }
}

function fromLoading(phase: Loading, input: Arrival, root: readonly string[]): Step {
  const { candidate } = phase;
  switch (input.kind) {
    case 'connection': {
      if (candidate) return stay(phase);
      const next = sourceOn(input.connection);
      return { phase: { ...phase, candidate: next }, effects: [open(next)] };
    }
    case 'retiring': {
      const to = candidate && retiredTo(candidate, input);
      if (!candidate || !to) return stay(phase);
      const next = sourceOn(to);
      return { phase: { ...phase, candidate: next }, effects: [release(candidate), open(next)] };
    }
    case 'namespace':
      if (input.source === candidate) applyNamespace(candidate.presence, root, input.type, input.namespace);
      return stay(phase);
    case 'loaded': {
      if (!candidate || input.source !== candidate) return stay(phase);
      const settled: Settled = new Map([...candidate.presence.keys()].map((id) => [id, member(root, id)]));
      return {
        phase: { kind: 'live', settled, leaving: NOBODY_LEAVING, source: candidate },
        effects: [emit([...settled.values()], [], settled, true)],
      };
    }
    case 'ended':
      if (!candidate || input.source !== candidate) return stay(phase);
      return afterEnded({ ...phase, candidate: undefined }, input, [release(candidate)]);
    case 'leave-due':
      return stay(phase);
  }
}

function fromLive(phase: Live, input: Arrival, root: readonly string[]): Step {
  const { source } = phase;
  switch (input.kind) {
    case 'namespace':
      return input.source === source ? follow(phase, source, input, root) : stay(phase);
    case 'leave-due':
      return leaveDue(phase, input.id, source.presence);
    case 'retiring': {
      const to = retiredTo(source, input);
      if (!to) return stay(phase);
      const candidate = sourceOn(to);
      return {
        phase: { kind: 'replacing', settled: phase.settled, leaving: phase.leaving, source, candidate, failures: 0 },
        effects: [open(candidate)],
      };
    }
    case 'ended':
      if (input.source !== source) return stay(phase);
      return afterEnded(
        {
          kind: 'replacing',
          settled: phase.settled,
          leaving: phase.leaving,
          source: undefined,
          candidate: undefined,
          failures: 0,
        },
        input,
        [release(source)],
      );
    case 'connection':
    case 'loaded':
      return stay(phase);
  }
}

/**
 * A new source loaded: a member it shows that the application was not told of joins at once; one the application was
 * told of that it does not show starts its grace, as a live withdrawal would; one it shows again ends its grace.
 */
function adopt(phase: Replacing, candidate: Source, root: readonly string[]): Step {
  const { joined, left } = diffMembers(phase.settled, candidate.presence, root);
  const returning = [...phase.leaving].filter((id) => candidate.presence.has(id));
  const departing = left.filter((gone) => !phase.leaving.has(gone.id));
  const leaving: Leaving = new Set(left.map((gone) => gone.id));
  const settled =
    joined.length === 0 ? phase.settled : new Map([...phase.settled, ...joined.map((m) => [m.id, m] as const)]);
  return {
    phase: { kind: 'live', settled, leaving, source: candidate },
    effects: [
      ...(phase.source ? [release(phase.source)] : []),
      ...returning.map(cancelLeave),
      ...departing.map((gone): MembersEffect => ({ kind: 'schedule-leave', id: gone.id })),
      ...(joined.length === 0 ? [] : [emit(joined, [], settled)]),
    ],
  };
}

function fromReplacing(phase: Replacing, input: Arrival, root: readonly string[]): Step {
  const { source, candidate } = phase;
  switch (input.kind) {
    case 'connection': {
      if (candidate) return stay(phase);
      const next = sourceOn(input.connection);
      return { phase: { ...phase, candidate: next }, effects: [open(next)] };
    }
    case 'retiring': {
      // A candidate's connection retiring in turn moves the candidate; with none, the old source's moves the view.
      const from = candidate ?? source;
      const to = from && retiredTo(from, input);
      if (!from || !to) return stay(phase);
      const next = sourceOn(to);
      return {
        phase: { ...phase, candidate: next },
        effects: [...(candidate ? [release(candidate)] : []), open(next)],
      };
    }
    case 'namespace': {
      if (candidate && input.source === candidate) {
        applyNamespace(candidate.presence, root, input.type, input.namespace);
        return stay(phase);
      }
      return source && input.source === source ? follow(phase, source, input, root) : stay(phase);
    }
    case 'leave-due':
      // With no source followed, the last word on the member was its withdrawal.
      return leaveDue(phase, input.id, source?.presence);
    case 'loaded':
      return candidate && input.source === candidate ? adopt(phase, candidate, root) : stay(phase);
    case 'ended': {
      if (candidate && input.source === candidate) {
        return afterEnded({ ...phase, candidate: undefined }, input, [release(candidate)]);
      }
      if (!source || input.source !== source) return stay(phase);
      // The candidate, if any, carries on: the view holds what it settled until it loads.
      if (candidate) return { phase: { ...phase, source: undefined }, effects: [release(source)] };
      return afterEnded({ ...phase, source: undefined }, input, [release(source)]);
    }
  }
}

/**
 * The view's transition: pure apart from advancing the presence of the source an input names, which only this
 * function writes. An input about a source the phase no longer holds changes nothing. Every `emit` comes last among a
 * step's effects, so a handler that closes the view leaves nothing of the step undone.
 */
export function transition(phase: MembersPhase, input: MembersInput, root: readonly string[]): Step {
  if (phase.kind === 'closed') return stay(phase);
  if (input.kind === 'close') return { phase: { kind: 'closed', settled: settledOf(phase) }, effects: abandon(phase) };
  switch (phase.kind) {
    case 'loading':
      return fromLoading(phase, input, root);
    case 'live':
      return fromLive(phase, input, root);
    case 'replacing':
      return fromReplacing(phase, input, root);
  }
}

/**
 * How long a member stays after its last namespace is withdrawn before it is reported as left. The relay orders
 * nothing across two sessions: with no demand on the rotating publisher, the old generation's withdrawal arrived before
 * the new generation's announce in 24 of 75 rotations measured on ord and fra (2026-11-03), by at most 0.2 ms.
 */
const LEAVE_GRACE_MS = 100;

/** What a view borrows from its supervisor. */
export interface MembersDependencies {
  /** The live connection, waiting for one while reconnecting; `undefined` once `scope` aborts or the supervisor stops. */
  liveConnection(scope: AbortSignal): Promise<MembersConnection | undefined>;
  /** The connection the supervisor made current, which a rotation's `retiring` moves the view to. */
  current(): MembersConnection | undefined;
  /** The view closed, for whatever reason. */
  onClosed(): void;
}

const NOBODY: ReadonlyMap<string, Member> = new Map();

function deliver(name: string, call: () => void): void {
  try {
    call();
  } catch (error) {
    // biome-ignore lint/suspicious/noConsole: the application's callback failed and has no caller to report to
    console.error(new Error(`${name} threw; the members view carries on`, { cause: error }));
  }
}

/** Open a view of `root`, whose fields the caller has admitted. No handler runs inside this call. */
export function openMembers(deps: MembersDependencies, root: readonly string[], handlers: MembersHandlers): Members {
  let phase: MembersPhase = { kind: 'loading', candidate: undefined, failures: 0 };
  const inbox: MembersInput[] = [];
  let draining = false;
  const scope = new AbortController();
  const opened = new Map<Source, { watch: DiscoveryWatch; unobserve(): void }>();
  const leaves = new Map<string, ReturnType<typeof setTimeout>>();

  /** Feed `input`; `first` puts it ahead of what is queued. Inputs raised while one is processed wait their turn. */
  const feed = (input: MembersInput, first = false) => {
    if (first) inbox.unshift(input);
    else inbox.push(input);
    if (draining) return;
    draining = true;
    try {
      for (let next = inbox.shift(); next; next = inbox.shift()) {
        const was = phase;
        const step = transition(phase, next, root);
        phase = step.phase;
        if (phase.kind === 'closed' && was.kind !== 'closed') {
          scope.abort();
          deps.onClosed();
        }
        for (const effect of step.effects) run(effect);
      }
    } finally {
      draining = false;
    }
  };

  const connectAfter = async (wait: Promise<unknown>) => {
    await wait.catch(() => {});
    const connection = await deps.liveConnection(scope.signal);
    if (connection) feed({ kind: 'connection', connection });
  };

  const watch = (source: Source) => {
    const { connection } = source;
    let held: DiscoveryWatch;
    try {
      held = connection.discovery.watch(root, {
        onAnnounced: (namespace) => feed({ kind: 'namespace', source, type: 'announced', namespace }),
        onUnannounced: (namespace) => feed({ kind: 'namespace', source, type: 'unannounced', namespace }),
        onLoaded: () => feed({ kind: 'loaded', source }),
        onEnded: (error) => feed({ kind: 'ended', source, error }),
      });
    } catch (error) {
      // Refused at the call: an argument no connection takes, or the connection ending before the supervisor heard.
      feed({
        kind: 'ended',
        source,
        error: refusedForGood(error) ? error : new SubscriptionConnectionClosedError(error),
      });
      return;
    }
    const retiring = () => feed({ kind: 'retiring', connection, next: deps.current() });
    connection.retiring.addEventListener('abort', retiring, { once: true });
    opened.set(source, { watch: held, unobserve: () => connection.retiring.removeEventListener('abort', retiring) });
    // A request already open on the connection seeds the view as if it had just announced what it holds.
    for (const namespace of held.namespaces()) feed({ kind: 'namespace', source, type: 'announced', namespace });
    if (held.loaded) feed({ kind: 'loaded', source });
    if (connection.retiring.aborted) retiring();
  };

  const run = (effect: MembersEffect) => {
    switch (effect.kind) {
      case 'open':
        watch(effect.source);
        return;
      case 'release': {
        const entry = opened.get(effect.source);
        opened.delete(effect.source);
        entry?.unobserve();
        entry?.watch.release();
        return;
      }
      case 'schedule-leave': {
        const { id } = effect;
        clearTimeout(leaves.get(id));
        leaves.set(
          id,
          setTimeout(() => {
            leaves.delete(id);
            feed({ kind: 'leave-due', id });
          }, LEAVE_GRACE_MS),
        );
        return;
      }
      case 'cancel-leave':
        clearTimeout(leaves.get(effect.id));
        leaves.delete(effect.id);
        return;
      case 'emit':
        deliver('onChanged', () => handlers.onChanged?.(effect.change));
        return;
      case 'error':
        deliver('onError', () => handlers.onError?.(effect.error));
        return;
      case 'await-connection':
        // The request can see the end before the connection reports it; wait for that rather than spin on it.
        void connectAfter(Promise.race([whenAborted(effect.after.signal), sleep(1_000, scope.signal)]));
        return;
      case 'retry':
        void connectAfter(sleep(backoffDelay(effect.attempt, RESUBSCRIBE_BACKOFF), scope.signal));
        return;
    }
  };

  void connectAfter(Promise.resolve());
  return {
    get loaded() {
      return settledOf(phase) !== undefined;
    },
    get present() {
      return settledOf(phase) ?? NOBODY;
    },
    close() {
      feed({ kind: 'close' }, true);
    },
  };
}
