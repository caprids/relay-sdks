import { RelayError } from './errors.js';
import type { RelayTrackWriter } from './publish.js';
import { payloadBytes } from './validation.js';

/** Options for {@link createCatalogService}. */
export interface CatalogServiceOptions {
  /** Pause before retrying a write that failed while the track stayed open. Default 1000 ms. */
  retryDelayMs?: number;
}

/** Options for {@link createDocumentService}. */
export type DocumentServiceOptions = CatalogServiceOptions;

/** Options for {@link CatalogService.serve}. */
export interface ServeOptions {
  /** Aborting stops the service, cancelling a write in progress; `serve` then rejects with the signal's reason. */
  signal?: AbortSignal;
}

/**
 * Serves a state-shaped track: sends its document on every relay subscription. Create one with
 * {@link createCatalogService}.
 */
export interface CatalogService {
  /**
   * Run until the signal aborts or the track ends. Resolves when the track ends without a write having failed. Rejects
   * with the `RelayError` that ended the track when a write exposed it, so a retired catalog cannot go unnoticed.
   */
  serve(options?: ServeOptions): Promise<void>;
  /** Documents the transport accepted. */
  readonly sends: number;
  /** Writes that failed while the track stayed open, each followed by a retry or by lost demand. */
  readonly failures: number;
}

/**
 * Serves a state-shaped track whose document the application replaces, such as the media timeline. Create one with
 * {@link createDocumentService}.
 */
export interface DocumentService extends CatalogService {
  /** Replace the document, and send it if the relay is subscribed. Returns at once. */
  update(document: ArrayBufferView): void;
  /** The latest document, or `undefined` before the first. */
  readonly document: Uint8Array | undefined;
}

interface State {
  document: Uint8Array | undefined;
  due: boolean;
  sends: number;
  failures: number;
  notify: (() => void) | undefined;
}

function retryDelay(options: CatalogServiceOptions): number {
  if (typeof options !== 'object' || options === null) throw new TypeError('options must be an object');
  const delay = options.retryDelayMs ?? 1000;
  if (typeof delay !== 'number' || Number.isNaN(delay)) throw new TypeError('retryDelayMs must be a number');
  if (delay < 0 || delay > 2147483647) throw new RangeError('retryDelayMs must be from 0 to 2147483647');
  return delay;
}

/** Wait `ms`, or until `cut` is called; reject with the signal's reason if it aborts. */
function sleep(
  ms: number,
  signal: AbortSignal | undefined,
  onCut: (cut: (() => void) | undefined) => void,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const done = () => {
      clearTimeout(timer);
      signal?.removeEventListener('abort', abort);
      onCut(undefined);
      resolve();
    };
    const abort = () => {
      clearTimeout(timer);
      onCut(undefined);
      reject(signal?.reason);
    };
    const timer = setTimeout(done, ms);
    signal?.addEventListener('abort', abort, { once: true });
    onCut(done);
  });
}

/** Send the latest document on every relay subscription and on every update while the relay is subscribed. */
async function serveDocument(
  track: RelayTrackWriter,
  state: State,
  delayMs: number,
  { signal }: ServeOptions,
): Promise<void> {
  signal?.throwIfAborted();
  const changes = track.demandChanges();
  let wake: (() => void) | undefined;
  /** Ends a retry pause early: set only while one runs. */
  let cutPause: (() => void) | undefined;
  let finished = false;
  const notify = () => {
    const woken = wake;
    wake = undefined;
    woken?.();
  };
  state.notify = notify;
  const watching = (async () => {
    try {
      // The current state first, then a repeated `true` for every new or replacement relay subscription.
      for await (const demanded of changes) {
        if (demanded) {
          state.due = true;
          notify();
        }
      }
    } finally {
      finished = true;
      notify();
      cutPause?.();
    }
  })();
  watching.catch(() => {});
  const abort = () => {
    void changes.return?.();
    notify();
  };
  signal?.addEventListener('abort', abort, { once: true });
  try {
    for (;;) {
      signal?.throwIfAborted();
      const document = state.document;
      if (!state.due || !document || !track.hasDemand) {
        // The track ended (or the demand iterator failed, which is raised here).
        if (finished) return await watching;
        await new Promise<void>((resolve) => {
          wake = resolve;
        });
        continue;
      }
      state.due = false;
      try {
        await track.write(document, signal ? { signal } : {});
        state.sends++;
      } catch (error) {
        signal?.throwIfAborted();
        if (!(error instanceof RelayError) || error.code === 'CLOSED' || track.closed) throw error;
        state.failures++;
        // Losing demand ends the retries; the next relay subscription makes the document due again.
        if (!track.hasDemand) continue;
        state.due = true;
        // The track ending cuts the pause short: the loop then returns rather than wait out the delay.
        await sleep(delayMs, signal, (cut) => {
          cutPause = cut;
        });
      }
    }
  } finally {
    signal?.removeEventListener('abort', abort);
    state.notify = undefined;
    void changes.return?.();
  }
}

/**
 * Serve a state-shaped track, such as an MSF catalog: send `document` as one independent group on the relay's current
 * subscription and again on every later one, including a replacement (spec/behavior.md, "Media publishing"). Viewers
 * who join during one relay subscription get the relay's cached copy. A write that fails while the track stays open is
 * retried after `retryDelayMs` for as long as demand remains; a failure that ends the track ends the service.
 *
 * ```ts
 * const catalog = createCatalogService(publisher.registerTrack({ track: CATALOG_TRACK }), buildMsfCatalog(input));
 * void catalog.serve({ signal });
 * ```
 */
export function createCatalogService(
  track: RelayTrackWriter,
  document: ArrayBufferView,
  options: CatalogServiceOptions = {},
): CatalogService {
  // Arguments first, as everywhere in the SDK: a bad document never reaches the track.
  const state: State = { document: payloadBytes(document), due: false, sends: 0, failures: 0, notify: undefined };
  const delayMs = retryDelay(options);
  return {
    serve: (serveOptions = {}) => serveDocument(track, state, delayMs, serveOptions),
    get sends() {
      return state.sends;
    },
    get failures() {
      return state.failures;
    },
  };
}

/**
 * Serve a state-shaped track whose document the application replaces, such as the msf-01 media timeline: the latest
 * document goes out as a new group on every relay subscription, as {@link createCatalogService} does, and again on
 * every `update()` while the relay is subscribed. Nothing is sent before the first document, and an update without
 * relay demand only replaces it.
 */
export function createDocumentService(
  track: RelayTrackWriter,
  document?: ArrayBufferView,
  options: DocumentServiceOptions = {},
): DocumentService {
  const state: State = {
    document: document === undefined ? undefined : payloadBytes(document),
    due: false,
    sends: 0,
    failures: 0,
    notify: undefined,
  };
  const delayMs = retryDelay(options);
  return {
    serve: (serveOptions = {}) => serveDocument(track, state, delayMs, serveOptions),
    update(next) {
      state.document = payloadBytes(next);
      state.due = true;
      state.notify?.();
    },
    get document() {
      return state.document;
    },
    get sends() {
      return state.sends;
    },
    get failures() {
      return state.failures;
    },
  };
}
