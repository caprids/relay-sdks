import { CATALOG_TRACK, type MsfCatalogTrack, parseMsfCatalog } from '@caprid/moq-media';
import type { RelaySubscriber, RelaySubscription } from '@caprid/relay';

/**
 * Catalogs after the first: the listener's handlers once it has started, and the newest one that came before. An error
 * of the catalog subscription after the first catalog, which the listener depends on for replacements, or a later
 * catalog that does not parse, goes to `onError`; before the listener has started it is kept, the first one only, in
 * `pendingError` and reported once the handlers exist, as `pending` is kept.
 */
export interface LaterCatalogs {
  onCatalog?(tracks: MsfCatalogTrack[]): void;
  onError?(error: unknown): void;
  pending?: MsfCatalogTrack[];
  pendingError?: { error: unknown };
}

export function readCatalog(
  subscriber: RelaySubscriber,
  namespace: readonly string[],
  signal: AbortSignal,
  later: LaterCatalogs,
) {
  return new Promise<{ tracks: MsfCatalogTrack[]; subscription: RelaySubscription }>((resolve, reject) => {
    signal.throwIfAborted();
    let subscription: RelaySubscription | undefined;
    // The first catalog object, or the first error, decides; later ones change nothing.
    let outcome: { tracks: MsfCatalogTrack[] } | { error: unknown; ended: boolean } | undefined;
    // An object may arrive before subscribe() returns, so the outcome waits until the subscription is in hand.
    const settle = () => {
      if (!outcome || !subscription) return;
      signal.removeEventListener('abort', abort);
      if ('tracks' in outcome) resolve({ tracks: outcome.tracks, subscription });
      else {
        // An error ends the subscription; a catalog that does not parse leaves it open.
        if (!outcome.ended) subscription.cancel();
        reject(outcome.error);
      }
    };
    // Aborted before the catalog came: cancel the subscription and reject.
    const abort = () => {
      if (outcome) return;
      outcome = { error: signal.reason, ended: false };
      settle();
    };
    signal.addEventListener('abort', abort, { once: true });
    subscription = subscriber.subscribe(
      { namespace: [...namespace], track: CATALOG_TRACK },
      {
        onObject(object) {
          if (object.status !== 'normal') return;
          if (outcome) {
            // A later catalog: for the listener once it has started, and kept, the newest only, until then.
            if (!('tracks' in outcome)) return;
            let tracks: MsfCatalogTrack[];
            try {
              tracks = parseMsfCatalog(object.payload, { catalogNamespace: namespace }).tracks;
            } catch (error) {
              if (later.onError) later.onError(error);
              else later.pendingError ??= { error };
              return;
            }
            if (later.onCatalog) later.onCatalog(tracks);
            else later.pending = tracks;
            return;
          }
          try {
            outcome = { tracks: parseMsfCatalog(object.payload, { catalogNamespace: namespace }).tracks };
          } catch (error) {
            outcome = { error, ended: false };
          }
          settle();
        },
        onError(error) {
          if (outcome) {
            // The catalog subscription ended after its first catalog: no replacement can follow, which is reported.
            if ('tracks' in outcome) {
              if (later.onError) later.onError(error);
              else later.pendingError ??= { error };
            }
            return;
          }
          outcome = { error, ended: true };
          settle();
        },
      },
    );
    settle();
  });
}
