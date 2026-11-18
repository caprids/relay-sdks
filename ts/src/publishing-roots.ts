import { type DiscoveryIndex, type DiscoveryWatch, discoveryEnding, refusedForGood } from './discovery.js';
import { SubscriptionConnectionClosedError } from './subscription.js';
import { backoffDelay, RESUBSCRIBE_BACKOFF, sleep, whenAborted } from './supervision.js';

/**
 * A root under the prefix: the namespace one field below it, publishing while a namespace at it or one field below it
 * is announced. A supervised publisher announces its generations one field below its root; an unsupervised publisher
 * announces the root itself (depth 1).
 */
export interface PublishingRoot {
  /** The field one below the prefix, as the root's publisher chose it. */
  readonly id: string;
  /** The prefix followed by `id`. Want it with `generations: true` to follow the root's supervised publisher. */
  readonly namespace: readonly string[];
}

/** What `onChanged` receives: which roots were added and removed since the last call, and which are publishing now. */
export interface PublishingRootsChange {
  /**
   * Roots that started publishing. On the first call (`initial`), every root publishing when the view loaded, possibly
   * none.
   */
  readonly added: readonly PublishingRoot[];
  /** Roots that stopped publishing. Empty on the first call. */
  readonly removed: readonly PublishingRoot[];
  /**
   * Every root publishing after this change, by id, in the order the view first saw them. The same map as `present`.
   */
  readonly present: ReadonlyMap<string, PublishingRoot>;
  /** True on the first call only: the view has just loaded. A UI that announces arrivals skips this one. */
  readonly initial: boolean;
}

/** What a publishing-roots view follows. */
export interface PublishingRootsOptions {
  /**
   * The prefix whose roots to follow: each namespace one field below it is a root, and one field further is that
   * root's generation. Same rules as a namespace, with room for the two fields under it. The subscriber's token must
   * allow discovering it.
   */
  prefix: readonly string[];
}

/** The callbacks a publishing-roots view delivers to. Like `WantHandlers`, they must be synchronous. */
export interface PublishingRootsHandlers {
  /**
   * The view loaded, or roots were added or removed. The first call is the load and runs once, even for an empty
   * prefix; every later call names at least one root in `added` or `removed`. A root whose last namespace is withdrawn
   * is removed once it has had none for 100 ms, so a rotation whose withdrawal arrives just before the new generation's
   * announce calls nothing. A root's generations changing, the relay repeating an announce, and the subscriber's
   * reconnects and rotations call nothing. A reconnect, rotation or retry calls it once with the roots its new
   * discovery adds, if there are any. A root the new discovery misses is removed in a later call, once the new
   * discovery has shown it nothing for 100 ms.
   */
  onChanged?(change: PublishingRootsChange): void;
  /**
   * A discovery failure no wait fixes: the relay refusing the request with a REQUEST_ERROR that does not invite a
   * retry, such as NOT_SUPPORTED from a relay without namespace discovery. The view is closed. A prefix outside the
   * token's grant is not one: moq-relay accepts the request and sends nothing, so the view loads empty and this is
   * never called. An empty view with no error can therefore mean the token does not grant the prefix.
   */
  onError?(error: unknown): void;
}

/** A live view of the roots publishing under a prefix. Create one with `SupervisedSubscriber.publishingRoots`. */
export interface PublishingRoots {
  /**
   * False until the prefix's first discovery has reported everything announced under it; then `present` is empty only
   * when no root is publishing. Stays true across reconnects, while the last settled view is held.
   */
  readonly loaded: boolean;
  /**
   * The roots publishing, by id: empty until loaded, and the last settled view while a reconnect's discovery loads. A
   * root whose last namespace went stays until it is reported as removed. The same map until the next `onChanged`, so
   * a UI can compare it by identity.
   */
  readonly present: ReadonlyMap<string, PublishingRoot>;
  /** Stop following the prefix. Idempotent; nothing is delivered after it, even when called from `onChanged`. */
  close(): void;
}

export type Presence = Map<string, Set<string>>;

export function rootField(
  prefix: readonly string[],
  namespace: readonly string[],
): { id: string; field: string } | undefined {
  const id = namespace[prefix.length];
  if (id === undefined) return undefined;
  if (namespace.length === prefix.length + 1) return { id, field: '' };
  if (namespace.length === prefix.length + 2) return { id, field: namespace[prefix.length + 1] as string };
  return undefined;
}

export function applyNamespace(
  presence: Presence,
  prefix: readonly string[],
  type: 'announced' | 'unannounced',
  namespace: readonly string[],
): { kind: 'added' | 'removed'; id: string } | undefined {
  const at = rootField(prefix, namespace);
  if (!at) return undefined;
  const fields = presence.get(at.id);
  if (type === 'announced') {
    if (fields) {
      fields.add(at.field);
      return undefined;
    }
    presence.set(at.id, new Set([at.field]));
    return { kind: 'added', id: at.id };
  }
  if (!fields?.delete(at.field) || fields.size > 0) return undefined;
  presence.delete(at.id);
  return { kind: 'removed', id: at.id };
}

function publishingRoot(prefix: readonly string[], id: string): PublishingRoot {
  return Object.freeze({ id, namespace: Object.freeze([...prefix, id]) });
}

export function diffRoots(
  settled: ReadonlyMap<string, PublishingRoot>,
  loaded: Presence,
  prefix: readonly string[],
): { added: PublishingRoot[]; removed: PublishingRoot[] } {
  const added = [...loaded.keys()].filter((id) => !settled.has(id)).map((id) => publishingRoot(prefix, id));
  const removed = [...settled.values()].filter((known) => !loaded.has(known.id));
  return { added, removed };
}

export interface PublishingRootsConnection {
  readonly discovery: DiscoveryIndex;
  readonly signal: AbortSignal;
  readonly retiring: AbortSignal;
}

export interface Source {
  readonly connection: PublishingRootsConnection;
  readonly presence: Presence;
}

type Settled = ReadonlyMap<string, PublishingRoot>;
type Leaving = ReadonlySet<string>;

export type PublishingRootsPhase =
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

export type PublishingRootsInput =
  | { readonly kind: 'connection'; readonly connection: PublishingRootsConnection }
  | {
      readonly kind: 'retiring';
      readonly connection: PublishingRootsConnection;
      readonly next: PublishingRootsConnection | undefined;
    }
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

export type PublishingRootsEffect =
  | { readonly kind: 'open'; readonly source: Source }
  | { readonly kind: 'release'; readonly source: Source }
  | { readonly kind: 'emit'; readonly change: PublishingRootsChange }
  | { readonly kind: 'schedule-leave'; readonly id: string }
  | { readonly kind: 'cancel-leave'; readonly id: string }
  | { readonly kind: 'await-connection'; readonly after: PublishingRootsConnection }
  | { readonly kind: 'retry'; readonly attempt: number }
  | { readonly kind: 'error'; readonly error: unknown };

type Step = { phase: PublishingRootsPhase; effects: PublishingRootsEffect[] };
type Loading = Extract<PublishingRootsPhase, { kind: 'loading' }>;
type Live = Extract<PublishingRootsPhase, { kind: 'live' }>;
type Replacing = Extract<PublishingRootsPhase, { kind: 'replacing' }>;
type Arrival = Exclude<PublishingRootsInput, { kind: 'close' }>;

const NOBODY_LEAVING: Leaving = new Set();
const stay = (phase: PublishingRootsPhase): Step => ({ phase, effects: [] });
const sourceOn = (connection: PublishingRootsConnection): Source => ({ connection, presence: new Map() });
const open = (source: Source): PublishingRootsEffect => ({ kind: 'open', source });
const release = (source: Source): PublishingRootsEffect => ({ kind: 'release', source });
const cancelLeave = (id: string): PublishingRootsEffect => ({ kind: 'cancel-leave', id });
const emit = (
  added: PublishingRoot[],
  removed: PublishingRoot[],
  present: Settled,
  initial = false,
): PublishingRootsEffect => ({
  kind: 'emit',
  change: { added, removed, present, initial },
});

function settledOf(phase: PublishingRootsPhase): Settled | undefined {
  return phase.kind === 'loading' ? undefined : phase.settled;
}

function sourcesOf(phase: PublishingRootsPhase): Source[] {
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

function abandon(phase: PublishingRootsPhase): PublishingRootsEffect[] {
  const leaving = phase.kind === 'live' || phase.kind === 'replacing' ? [...phase.leaving] : [];
  return [...sourcesOf(phase).map(release), ...leaving.map(cancelLeave)];
}

function retiredTo(
  source: Source,
  input: Extract<PublishingRootsInput, { kind: 'retiring' }>,
): PublishingRootsConnection | undefined {
  if (source.connection !== input.connection || input.next === input.connection) return undefined;
  return input.next;
}

function follow(
  phase: Live | Replacing,
  source: Source,
  input: Extract<PublishingRootsInput, { kind: 'namespace' }>,
  prefix: readonly string[],
): Step {
  const change = applyNamespace(source.presence, prefix, input.type, input.namespace);
  if (!change) return stay(phase);
  const { id } = change;
  if (change.kind === 'removed') {
    return { phase: { ...phase, leaving: new Set(phase.leaving).add(id) }, effects: [{ kind: 'schedule-leave', id }] };
  }
  if (phase.leaving.has(id)) {
    const leaving = new Set(phase.leaving);
    leaving.delete(id);
    return { phase: { ...phase, leaving }, effects: [cancelLeave(id)] };
  }
  const added = publishingRoot(prefix, id);
  const settled = new Map(phase.settled).set(id, added);
  return { phase: { ...phase, settled }, effects: [emit([added], [], settled)] };
}

function leaveDue(phase: Live | Replacing, id: string): Step {
  const gone = phase.settled.get(id);
  if (!gone || !phase.leaving.has(id) || sourcesOf(phase).some(({ presence }) => presence.has(id))) return stay(phase);
  const settled = new Map(phase.settled);
  settled.delete(id);
  const leaving = new Set(phase.leaving);
  leaving.delete(id);
  return { phase: { ...phase, settled, leaving }, effects: [emit([], [gone], settled)] };
}

function resume(phase: Live | Replacing): Step {
  return { phase, effects: [...phase.leaving].map((id): PublishingRootsEffect => ({ kind: 'schedule-leave', id })) };
}

function afterEnded(
  phase: Loading | Replacing,
  ended: Extract<PublishingRootsInput, { kind: 'ended' }>,
  effects: PublishingRootsEffect[],
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

function fromLoading(phase: Loading, input: Arrival, prefix: readonly string[]): Step {
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
      if (input.source === candidate) applyNamespace(candidate.presence, prefix, input.type, input.namespace);
      return stay(phase);
    case 'loaded': {
      if (!candidate || input.source !== candidate) return stay(phase);
      const settled: Settled = new Map([...candidate.presence.keys()].map((id) => [id, publishingRoot(prefix, id)]));
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

function fromLive(phase: Live, input: Arrival, prefix: readonly string[]): Step {
  const { source } = phase;
  switch (input.kind) {
    case 'namespace':
      return input.source === source ? follow(phase, source, input, prefix) : stay(phase);
    case 'leave-due':
      return input.following(source) ? leaveDue(phase, input.id) : stay(phase);
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
 * A root the new snapshot misses is never removed at the load: it gets the grace against the new discovery. The
 * snapshot may have been taken inside a publisher rotation's gap, or just before a root's announce reached this relay.
 */
function adopt(phase: Replacing, candidate: Source, prefix: readonly string[]): Step {
  const { added, removed: missing } = diffRoots(phase.settled, candidate.presence, prefix);
  const leaving: Leaving = new Set(missing.map((gone) => gone.id));
  let settled = phase.settled;
  if (added.length > 0) {
    const next = new Map(phase.settled);
    for (const arrived of added) next.set(arrived.id, arrived);
    settled = next;
  }
  return {
    phase: { kind: 'live', settled, leaving, source: candidate },
    effects: [
      ...(phase.source ? [release(phase.source)] : []),
      ...[...phase.leaving].filter((id) => !leaving.has(id)).map(cancelLeave),
      ...[...leaving].map((id): PublishingRootsEffect => ({ kind: 'schedule-leave', id })),
      ...(settled === phase.settled ? [] : [emit(added, [], settled)]),
    ],
  };
}

function fromReplacing(phase: Replacing, input: Arrival, prefix: readonly string[]): Step {
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
        applyNamespace(candidate.presence, prefix, input.type, input.namespace);
        return stay(phase);
      }
      return source && input.source === source ? follow(phase, source, input, prefix) : stay(phase);
    }
    case 'leave-due':
      return source && input.following(source) ? leaveDue(phase, input.id) : stay(phase);
    case 'loaded':
      if (candidate && input.source === candidate) return adopt(phase, candidate, prefix);
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

export function transition(phase: PublishingRootsPhase, input: PublishingRootsInput, prefix: readonly string[]): Step {
  if (phase.kind === 'closed') return stay(phase);
  if (input.kind === 'close') return { phase: { kind: 'closed', settled: settledOf(phase) }, effects: abandon(phase) };
  switch (phase.kind) {
    case 'loading':
      return fromLoading(phase, input, prefix);
    case 'live':
      return fromLive(phase, input, prefix);
    case 'replacing':
      return fromReplacing(phase, input, prefix);
  }
}

/**
 * How long a root stays before it is reported as removed, after its last namespace is withdrawn live or after a newly
 * loaded discovery's snapshot misses it. It counts only while the followed discovery delivers: one that runs out while
 * the request is replaced or its stream has failed waits for the next load, which settles it or starts it again. The
 * relay orders nothing across two sessions: with no demand on the rotating publisher, the old generation's withdrawal
 * arrived before the new generation's announce in 24 of 75 rotations measured on ord and fra (2026-11-03), by at most
 * 0.2 ms.
 */
const LEAVE_GRACE_MS = 100;

export interface PublishingRootsDependencies {
  liveConnection(scope: AbortSignal): Promise<PublishingRootsConnection | undefined>;
  current(): PublishingRootsConnection | undefined;
  /** Keep `connection` open through a rotation until the returned release runs. */
  hold(connection: PublishingRootsConnection): () => void;
  onClosed(): void;
}

const NOBODY: ReadonlyMap<string, PublishingRoot> = new Map();

function deliver(name: string, call: () => void): void {
  try {
    call();
  } catch (error) {
    // biome-ignore lint/suspicious/noConsole: the application's callback failed and has no caller to report to
    console.error(new Error(`${name} threw; the publishing-roots view carries on`, { cause: error }));
  }
}

export function openPublishingRoots(
  deps: PublishingRootsDependencies,
  prefix: readonly string[],
  handlers: PublishingRootsHandlers,
): PublishingRoots {
  let phase: PublishingRootsPhase = { kind: 'loading', candidate: undefined, failures: 0 };
  const inbox: PublishingRootsInput[] = [];
  let draining = false;
  const scope = new AbortController();
  const opened = new Map<Source, { watch: DiscoveryWatch; detach(): void }>();
  const leaves = new Map<string, ReturnType<typeof setTimeout>>();
  const following = (source: Source) => opened.get(source)?.watch.following ?? false;

  const feed = (input: PublishingRootsInput, first = false) => {
    if (first) inbox.unshift(input);
    else inbox.push(input);
    if (draining) return;
    draining = true;
    try {
      for (let next = inbox.shift(); next; next = inbox.shift()) {
        const was = phase;
        const step = transition(phase, next, prefix);
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
      held = connection.discovery.watch(prefix, {
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

  const run = (effect: PublishingRootsEffect) => {
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
