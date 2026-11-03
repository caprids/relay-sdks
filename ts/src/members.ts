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
   * Members who arrived. On the first call (`initial`), everyone present when the view loaded, possibly no one.
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
   * every later call names at least one member in `joined` or `left`. A member whose last namespace is withdrawn
   * leaves once it has had none for 100 ms, so a rotation whose withdrawal arrives just before the new generation's
   * announce calls nothing. A member's generations changing, the relay repeating an announce, and the subscriber's
   * reconnects and rotations call nothing; a reconnect, rotation or retry calls it once with the difference its new
   * discovery shows, if there is one, joins and departures together. The exception is a member the new discovery misses
   * but the old one still showed, or was giving its 100 ms: it leaves in a later call, once the new discovery has shown
   * it nothing for 100 ms.
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

export type Presence = Map<string, Set<string>>;

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

export function diffMembers(
  settled: ReadonlyMap<string, Member>,
  loaded: Presence,
  root: readonly string[],
): { joined: Member[]; left: Member[] } {
  const joined = [...loaded.keys()].filter((id) => !settled.has(id)).map((id) => member(root, id));
  const left = [...settled.values()].filter((known) => !loaded.has(known.id));
  return { joined, left };
}

export interface MembersConnection {
  readonly discovery: DiscoveryIndex;
  readonly signal: AbortSignal;
  readonly retiring: AbortSignal;
}

export interface Source {
  readonly connection: MembersConnection;
  readonly presence: Presence;
}

type Settled = ReadonlyMap<string, Member>;
type Leaving = ReadonlySet<string>;

export type MembersPhase =
  | { readonly kind: 'loading'; readonly candidate: Source | undefined; readonly failures: number }
  | { readonly kind: 'live'; readonly settled: Settled; readonly leaving: Leaving; readonly source: Source }
  | {
      readonly kind: 'replacing';
      readonly settled: Settled;
      readonly leaving: Leaving;
      readonly source: Source | undefined;
      readonly candidate: Source | undefined;
      readonly failures: number;
    }
  | { readonly kind: 'closed'; readonly settled: Settled | undefined };

export type MembersInput =
  | { readonly kind: 'connection'; readonly connection: MembersConnection }
  | { readonly kind: 'retiring'; readonly connection: MembersConnection; readonly next: MembersConnection | undefined }
  | {
      readonly kind: 'namespace';
      readonly source: Source;
      readonly type: 'announced' | 'unannounced';
      readonly namespace: readonly string[];
    }
  | { readonly kind: 'loaded'; readonly source: Source }
  | { readonly kind: 'ended'; readonly source: Source; readonly error: unknown }
  | { readonly kind: 'leave-due'; readonly id: string; readonly following: (source: Source) => boolean }
  | { readonly kind: 'close' };

export type MembersEffect =
  | { readonly kind: 'open'; readonly source: Source }
  | { readonly kind: 'release'; readonly source: Source }
  | { readonly kind: 'emit'; readonly change: MembersChange }
  | { readonly kind: 'schedule-leave'; readonly id: string }
  | { readonly kind: 'cancel-leave'; readonly id: string }
  | { readonly kind: 'await-connection'; readonly after: MembersConnection }
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

function abandon(phase: MembersPhase): MembersEffect[] {
  const leaving = phase.kind === 'live' || phase.kind === 'replacing' ? [...phase.leaving] : [];
  return [...sourcesOf(phase).map(release), ...leaving.map(cancelLeave)];
}

function retiredTo(source: Source, input: Extract<MembersInput, { kind: 'retiring' }>): MembersConnection | undefined {
  if (source.connection !== input.connection || input.next === input.connection) return undefined;
  return input.next;
}

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

function leaveDue(phase: Live | Replacing, id: string, followed: Presence): Step {
  const gone = phase.settled.get(id);
  if (!gone || !phase.leaving.has(id) || followed.has(id)) return stay(phase);
  const settled = new Map(phase.settled);
  settled.delete(id);
  const leaving = new Set(phase.leaving);
  leaving.delete(id);
  return { phase: { ...phase, settled, leaving }, effects: [emit([], [gone], settled)] };
}

function resume(phase: Live | Replacing): Step {
  return { phase, effects: [...phase.leaving].map((id): MembersEffect => ({ kind: 'schedule-leave', id })) };
}

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
      return input.following(source) ? leaveDue(phase, input.id, source.presence) : stay(phase);
    case 'loaded':
      return input.source === source ? resume(phase) : stay(phase);
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
      return stay(phase);
  }
}

/**
 * A member the new snapshot misses leaves at once only when nothing else shows it. One the old discovery still shows,
 * or one already in its grace, may have announced just after the snapshot was taken, so it gets the grace against the
 * new discovery instead.
 */
function adopt(phase: Replacing, candidate: Source, root: readonly string[]): Step {
  const { joined, left: missing } = diffMembers(phase.settled, candidate.presence, root);
  const doubtful = (id: string) => phase.leaving.has(id) || (phase.source?.presence.has(id) ?? false);
  const left = missing.filter((gone) => !doubtful(gone.id));
  const leaving: Leaving = new Set(missing.filter((gone) => doubtful(gone.id)).map((gone) => gone.id));
  let settled = phase.settled;
  if (joined.length > 0 || left.length > 0) {
    const next = new Map(phase.settled);
    for (const gone of left) next.delete(gone.id);
    for (const arrived of joined) next.set(arrived.id, arrived);
    settled = next;
  }
  return {
    phase: { kind: 'live', settled, leaving, source: candidate },
    effects: [
      ...(phase.source ? [release(phase.source)] : []),
      ...[...phase.leaving].filter((id) => !leaving.has(id)).map(cancelLeave),
      ...[...leaving].map((id): MembersEffect => ({ kind: 'schedule-leave', id })),
      ...(settled === phase.settled ? [] : [emit(joined, left, settled)]),
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
      return source && input.following(source) ? leaveDue(phase, input.id, source.presence) : stay(phase);
    case 'loaded':
      if (candidate && input.source === candidate) return adopt(phase, candidate, root);
      return source && input.source === source ? resume(phase) : stay(phase);
    case 'ended': {
      if (candidate && input.source === candidate) {
        return afterEnded({ ...phase, candidate: undefined }, input, [release(candidate)]);
      }
      if (!source || input.source !== source) return stay(phase);
      if (candidate) return { phase: { ...phase, source: undefined }, effects: [release(source)] };
      return afterEnded({ ...phase, source: undefined }, input, [release(source)]);
    }
  }
}

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
 * How long a member stays after its last namespace is withdrawn live before it is reported as left; a newly loaded
 * discovery's snapshot needs none. It counts only while the followed discovery delivers: one that runs out while the
 * request is replaced or its stream has failed waits for the next load, which settles it or starts it again. The relay
 * orders nothing across two sessions: with no demand on the rotating publisher, the old generation's withdrawal
 * arrived before the new generation's announce in 24 of 75 rotations measured on ord and fra (2026-11-03), by at most
 * 0.2 ms.
 */
const LEAVE_GRACE_MS = 100;

export interface MembersDependencies {
  liveConnection(scope: AbortSignal): Promise<MembersConnection | undefined>;
  current(): MembersConnection | undefined;
  /** Keep `connection` open through a rotation until the returned release runs. */
  hold(connection: MembersConnection): () => void;
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

export function openMembers(deps: MembersDependencies, root: readonly string[], handlers: MembersHandlers): Members {
  let phase: MembersPhase = { kind: 'loading', candidate: undefined, failures: 0 };
  const inbox: MembersInput[] = [];
  let draining = false;
  const scope = new AbortController();
  const opened = new Map<Source, { watch: DiscoveryWatch; detach(): void }>();
  const leaves = new Map<string, ReturnType<typeof setTimeout>>();
  const following = (source: Source) => opened.get(source)?.watch.following ?? false;

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
      feed({
        kind: 'ended',
        source,
        error: refusedForGood(error) ? error : new SubscriptionConnectionClosedError(error),
      });
      return;
    }
    // A source holds its connection, so a rotation keeps the old discovery until the new one has loaded.
    const release = deps.hold(connection);
    const retiring = () => feed({ kind: 'retiring', connection, next: deps.current() });
    connection.retiring.addEventListener('abort', retiring, { once: true });
    opened.set(source, {
      watch: held,
      detach: () => {
        connection.retiring.removeEventListener('abort', retiring);
        release();
      },
    });
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
        entry?.watch.release();
        entry?.detach();
        return;
      }
      case 'schedule-leave': {
        const { id } = effect;
        clearTimeout(leaves.get(id));
        leaves.set(
          id,
          setTimeout(() => {
            leaves.delete(id);
            feed({ kind: 'leave-due', id, following });
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
