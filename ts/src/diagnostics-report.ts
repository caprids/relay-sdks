/**
 * Internal: what the SDK's own pieces report into a `diagnostics` option (spec/behavior.md, "Client diagnostics"). Kept
 * apart from the recorder and the publishing, so a supervisor importing it pulls in no frame writer.
 */
import type { Diagnostics } from './client-diagnostics.js';
import type { TransportFraming } from './connect-session.js';
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

/**
 * What a supervisor reports, under the scope it is given: a `link` event on each status change (with what carries the
 * connection when it goes live, and the error that ended the last one), and a `link` section.
 */
export function linkDiagnostics(diagnostics: Diagnostics) {
  const state: {
    status: SupervisedStatus;
    carrier?: TransportFraming;
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
        ...(error === undefined ? {} : { error: describeFailure(error) }),
      });
    },
    carrier(framing: TransportFraming) {
      state.carrier = framing;
    },
    generation(generation: string) {
      state.generation = generation;
      diagnostics.event('generation', { generation });
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
}

/**
 * What a supervised subscriber reports about its wants, under the scope it is given: a `want` event at each turn of a
 * want's life (`subscribed`, `restarted` with the reason, `switched`, `publisher-done`, `error`), and a `wants` section
 * with each want's track, whether it is subscribed, the objects it delivered and its restarts.
 */
export function wantDiagnostics(diagnostics: Diagnostics) {
  const wants = new Map<string, WantState>();
  diagnostics.section('wants', () => Object.fromEntries([...wants].map(([key, state]) => [key, { ...state }])));
  return {
    /** The handlers to deliver to: `handlers`, each lifecycle callback reported first. */
    observe(key: string, track: string, handlers: WantHandlers): WantHandlers {
      const state: WantState = wants.get(key) ?? { track, subscribed: false, objects: 0, restarts: 0 };
      state.track = track;
      wants.set(key, state);
      return {
        ...handlers,
        onSubscribed(info, context) {
          state.subscribed = true;
          diagnostics.event('want', { want: key, what: 'subscribed', namespace: context.namespace.join('/') });
          handlers.onSubscribed?.(info, context);
        },
        onObject(object) {
          state.objects++;
          handlers.onObject?.(object);
        },
        onPublisherDone(info) {
          diagnostics.event('want', { want: key, what: 'publisher-done', statusCode: info.statusCode });
          handlers.onPublisherDone?.(info);
        },
        onRestarted(info) {
          state.subscribed = false;
          state.restarts++;
          state.lastRestart = info.reason;
          diagnostics.event('want', { want: key, what: 'restarted', reason: info.reason });
          handlers.onRestarted?.(info);
        },
        onSwitched(info) {
          diagnostics.event('want', {
            want: key,
            what: 'switched',
            from: [...info.from.namespace, info.from.track].join('/'),
            to: [...info.to.namespace, info.to.track].join('/'),
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
