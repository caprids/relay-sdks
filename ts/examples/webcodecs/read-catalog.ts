import { CATALOG_TRACK, type MsfCatalogTrack, parseMsfCatalog } from '@caprid/moq-media';
import type { SupervisedSubscriber, WantHandlers } from '@caprid/relay';

/**
 * Catalogs after the first: the listener's handlers once it has started, and the newest one that came before. Each
 * comes with the namespace it was read from: following generations, that is the generation's, which a restarted
 * talker changes. An error of the catalog want after the first catalog (a failure it does not retry), or a later
 * catalog that does not parse, goes to `onError`; before the listener has started it is kept, the first one only, in
 * `pendingError` and reported once the handlers exist, as `pending` is kept.
 */
export interface LaterCatalogs {
  onCatalog?(tracks: MsfCatalogTrack[], namespace: string[]): void;
  onError?(error: unknown): void;
  pending?: { tracks: MsfCatalogTrack[]; namespace: string[] };
  pendingError?: { error: unknown };
}

/** The first catalog, the namespace it came from, and how to stop wanting later ones. */
export interface CatalogRead {
  tracks: MsfCatalogTrack[];
  namespace: string[];
  /** Withdraw the catalog want. */
  stop(): void;
}

/**
 * Want the catalog track of `namespace` (with `generations`, of the newest generation under that root) as `key`, and
 * resolve with the first catalog. The want stays: every later catalog, a restarted talker's included, goes to `later`.
 * Rejects with the abort reason, a failure the want does not retry, or a first catalog that does not parse, after
 * withdrawing the want.
 */
export function readCatalog(
  subscriber: SupervisedSubscriber,
  key: string,
  namespace: readonly string[],
  generations: boolean,
  signal: AbortSignal,
  later: LaterCatalogs,
) {
  return new Promise<CatalogRead>((resolve, reject) => {
    signal.throwIfAborted();
    // The namespace the want's subscription is on now: the generation's when following one.
    let at = [...namespace];
    let wanted = false;
    const stop = () => subscriber.unwant(key);
    // The first catalog object, or the first error, decides; later ones change nothing.
    let outcome: { tracks: MsfCatalogTrack[]; namespace: string[] } | { error: unknown; ended: boolean } | undefined;
    // An object may arrive before want() returns, so the outcome waits until the want is in place.
    const settle = () => {
      if (!outcome || !wanted) return;
      signal.removeEventListener('abort', abort);
      if ('tracks' in outcome) resolve({ ...outcome, stop });
      else {
        // A failure ends the want; a catalog that does not parse leaves it in place.
        if (!outcome.ended) stop();
        reject(outcome.error);
      }
    };
    // Aborted before the catalog came: withdraw the want and reject.
    const abort = () => {
      if (outcome) return;
      outcome = { error: signal.reason, ended: false };
      settle();
    };
    const handlers: WantHandlers = {
      onSubscribed(_, context) {
        at = context.namespace;
      },
      onSwitched(info) {
        at = info.to.namespace;
      },
      onObject(object) {
        if (object.status !== 'normal') return;
        if (outcome) {
          // A later catalog: for the listener once it has started, and kept, the newest only, until then.
          if (!('tracks' in outcome)) return;
          let tracks: MsfCatalogTrack[];
          try {
            tracks = parseMsfCatalog(object.payload, { catalogNamespace: at }).tracks;
          } catch (error) {
            if (later.onError) later.onError(error);
            else later.pendingError ??= { error };
            return;
          }
          if (later.onCatalog) later.onCatalog(tracks, [...at]);
          else later.pending = { tracks, namespace: [...at] };
          return;
        }
        try {
          outcome = { tracks: parseMsfCatalog(object.payload, { catalogNamespace: at }).tracks, namespace: [...at] };
        } catch (error) {
          outcome = { error, ended: false };
        }
        settle();
      },
      onError(error) {
        if (outcome) {
          // The want stopped after its first catalog: no replacement can follow, which is reported.
          if ('tracks' in outcome) {
            if (later.onError) later.onError(error);
            else later.pendingError ??= { error };
          }
          return;
        }
        outcome = { error, ended: true };
        settle();
      },
    };
    signal.addEventListener('abort', abort, { once: true });
    try {
      subscriber.want(key, { namespace, track: CATALOG_TRACK, generations }, handlers);
    } catch (error) {
      signal.removeEventListener('abort', abort);
      throw error;
    }
    wanted = true;
    settle();
  });
}
