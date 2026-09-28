/**
 * Publish-capable MOQT session (moq-transport draft-20), the publish-side sibling of `network/moqt/session.ts`'s
 * subscribe-only driver.
 *
 * Owns the protocol mechanics over an established transport: the SETUP exchange on paired unidirectional control
 * streams (§3.3), and the announce-and-serve ingest flow — answering the peer's SUBSCRIBE_NAMESPACE solicitation with
 * REQUEST_OK and a NAMESPACE entry for the announced namespace (§10.18–19), then serving the per-track SUBSCRIBEs the
 * peer routes back (answered with SUBSCRIBE_OK carrying a publisher-assigned track alias, §10.8) for as long as each
 * subscription's request stream stays open.
 *
 * Flow choice: the publisher initiates no request streams at all. moq-relay 0.14.7 removed PUBLISH-based ingest
 * (upstream `de336492`: every PUBLISH is answered with a request error), and it solicits namespaces itself — one
 * SUBSCRIBE_NAMESPACE per authorized prefix, sent immediately after SETUP. Announcing rides the solicitation stream as
 * NAMESPACE entries (suffix-relative, parameterless — the relay treats even an empty parameter count as malformed);
 * data flows only on aliases bound by our own SUBSCRIBE_OKs, per subscription.
 *
 * Teardown follows draft-20: every end of an Established subscription the publisher chooses resets the subscription's
 * still-open data streams, then writes PUBLISH_DONE (status, the number of data streams opened for it, reason) and FINs
 * the request stream (§3.3.2, §10.12). moq-relay 0.15.4 and later require that PUBLISH_DONE, and treat a bare FIN as a
 * failed request; 0.15.1 to 0.15.3 abort the track on any byte after SUBSCRIBE_OK, PUBLISH_DONE included, so this core
 * needs a relay at 0.15.4 or later for a clean end. The publisher sends no GOAWAY: 0.14.17 closes the session on a
 * client GOAWAY (code 17), 0.15.1 and later record it and keep the session. Aliases are the peer's own request IDs,
 * unique by construction: moq-relay 0.15.x closes the session (PROTOCOL_VIOLATION) when one alias names two different
 * tracks, and cancels the newer subscription when two live subscriptions to one track share an alias.
 *
 * Deliberately callback-shaped with NO signals, mirroring the subscribe driver — signal awareness enters at the
 * `publish/` behavior layer through the actor below. Lives in `publish/` rather than `network/moqt` so the parent-owned
 * wire layer keeps only additive sibling files.
 */
import { StreamReader, utf8Encode } from './bytes.js';
import {
  type ControlMessage,
  decodeControlMessage,
  encodeNamespace,
  encodeNamespaceDone,
  encodePublishDone,
  encodeRequestError,
  encodeRequestOk,
  encodeSetup,
  encodeSubscribeOk,
  type KeyValuePair,
  MESSAGE_TYPE,
  type MessageParameters,
  PUBLISH_DONE_STATUS,
  REQUEST_ERROR_CODE,
  SETUP_OPTION,
  STREAM_RESET_CODE,
  type TrackNamespace,
} from './control-messages.js';
import { isMoqtProtocolError, MoqtProtocolError, SESSION_ERROR } from './errors.js';
import { isSubgroupHeaderType, STREAM_TYPE } from './object-stream.js';
import type { BidirectionalStreamLike } from './request-stream.js';
import type { Goaway, MoqtTransport } from './session.js';
import { streamResetReason } from './stream-reset.js';

// =============================================================================
// Driver types
// =============================================================================

export interface IncomingSubscribe {
  requestId: number;
  trackNamespace: TrackNamespace;
  trackName: string;
  parameters: MessageParameters;
}

export interface MoqtPublishSessionCallbacks {
  /** The server's SETUP arrived; the session is fully established. */
  onReady?(serverOptions: KeyValuePair[]): void;
  /** GOAWAY (control stream or a request stream): stop initiating, migrate. */
  onGoaway?(goaway: Goaway): void;
  /**
   * The announced namespace reached the wire on an accepted SUBSCRIBE_NAMESPACE solicitation — the peer can now route
   * SUBSCRIBEs to it. NAMESPACE entries have no acknowledgement of their own; this is the strongest "announced" fact
   * the transport draft offers.
   */
  onAnnounced?(info: { namespace: TrackNamespace }): void;
  /**
   * The solicitation stream carrying the announce ended while the session was still live — the peer rejected the
   * announce (unauthorized suffix) or withdrew its namespace interest. Either way the ingest path is gone.
   */
  onAnnounceEnded?(info: { error?: unknown }): void;
  /** The peer subscribed to a registered track (already answered SUBSCRIBE_OK). */
  onSubscribe?(subscribe: IncomingSubscribe): void;
  /** A subscription ended (the peer's FIN or reset, or our PUBLISH_DONE). */
  onSubscribeEnd?(info: { requestId: number }): void;
  /**
   * A track's effective alias binding changed. The most recent live subscription wins the binding (a replaced one ends
   * with PUBLISH_DONE `TRACK_ENDED` — a clean end for that request, covering the peer's resubscribe races), and only
   * while its Forward State
   * permits data (`forward: 0` subscribes without authorizing it; REQUEST_UPDATE toggles it). `undefined` means no live
   * forwarding subscription, so no data streams may be opened for the track.
   */
  onTrackBinding?(info: { trackName: string; trackAlias: number | undefined }): void;
  /**
   * REQUEST_UPDATE on an inbound request stream. `requestId` is the request being updated (the stream's SUBSCRIBE or
   * SUBSCRIBE_NAMESPACE); `updateRequestId` is the update's own, freshly consumed Request ID (§10.1).
   */
  onRequestUpdate?(info: { requestId: number; updateRequestId: number; parameters: MessageParameters }): void;
  /** The session ended — transport closed, or a fatal protocol error. */
  onClosed?(info: { error?: unknown }): void;
}

export interface MoqtPublishSessionConfig {
  /** Extra Setup Options to send (an implementation identifier is added automatically). */
  setupOptions?: KeyValuePair[];
  /** Identifies this implementation in SETUP (§10.3.1.5). */
  implementationName?: string;
  callbacks?: MoqtPublishSessionCallbacks;
}

export interface RegisterTrackOptions {
  /** Track metadata is explicit; raw tracks have no implied timestamp unit. */
  trackProperties?: KeyValuePair[];
  trackNamespace: TrackNamespace;
  trackName: string;
}

export interface RegisteredTrack {
  readonly trackName: string;
  /**
   * End the track: end every live subscription (its data streams closed, then PUBLISH_DONE `TRACK_ENDED` and a FIN on
   * its request stream) and refuse future SUBSCRIBEs with DOES_NOT_EXIST. Idempotent.
   */
  end(): void;
}

export interface MoqtPublishSession {
  /** Resolves when the server's SETUP has been received. */
  readonly ready: Promise<void>;
  /** Whether a current solicitation covers the desired namespace; undefined before any is accepted. */
  readonly namespaceSolicited: boolean | undefined;
  /**
   * Announce the namespace on every matching solicitation — current and future. The wire write additionally waits for
   * the first `registerTrack()`: an announce invites immediate SUBSCRIBEs, and an empty registry would answer them with
   * a terminal DOES_NOT_EXIST. Result via `onAnnounced` / `onAnnounceEnded`.
   */
  announce(trackNamespace: TrackNamespace): void;
  /** Register one servable track; the peer's SUBSCRIBEs are answered from this registry. */
  registerTrack(options: RegisterTrackOptions): RegisteredTrack;
  /**
   * Open a unidirectional data stream for the live subscription bound to `trackAlias`. The stream counts toward that
   * subscription's PUBLISH_DONE Stream Count, and a stream still open when the subscription ends is reset before
   * PUBLISH_DONE is written (§10.12). Rejects once the session is closing, when no live subscription holds the alias,
   * and when the subscription ends while the stream is opening (the stream is reset first).
   */
  openUniStream(trackAlias: number): Promise<WritableStream<Uint8Array>>;
  /**
   * Orderly stop: end every live subscription (PUBLISH_DONE `TRACK_ENDED`, then FIN) and retract the announce
   * (NAMESPACE_DONE), give those writes a bounded window to reach the wire (closing a WebTransport discards queued data), then close the transport.
   * Synchronous callers return immediately; the transport closes within roughly {@link CLOSE_FLUSH_TIMEOUT_MS}.
   */
  close(closeCode?: number, reason?: string): void;
  /** Drain outgoing streams, preserving an optional local closure error. */
  destroy(error?: unknown): void;
}

const DEFAULT_IMPLEMENTATION_NAME = '@mux/moq-core publisher';

/**
 * How long `close()` waits for the PUBLISH_DONE + FIN / NAMESPACE_DONE writes to drain before closing the transport out
 * from under them.
 */
export const CLOSE_FLUSH_TIMEOUT_MS = 250;

// =============================================================================
// Internal records
// =============================================================================

/** One inbound subscription's stream-side plumbing. */
interface SubscriberStream {
  requestId: number;
  trackAlias: number;
  writer: WritableStreamDefaultWriter<Uint8Array>;
  cancel(): Promise<void>;
  /** SUBSCRIBE_OK reached the wire — only accepted subscriptions may carry the binding. */
  accepted: boolean;
  /** `onSubscribe` fired — `onSubscribeEnd` pairs on this, keeping counts balanced. */
  reported: boolean;
  /** Forward State (§ SUBSCRIBE `forward`, togglable via REQUEST_UPDATE) — data flows only while true. */
  forwarding: boolean;
  /** The subscription is ending or over; no new data stream may open on its alias. */
  finished: boolean;
  /** Data streams opened for this subscription: PUBLISH_DONE's Stream Count (§10.12). */
  streamCount: number;
  /** Data streams opened for this subscription that the caller has not closed yet; each entry resets its stream. */
  dataStreams: Set<() => void>;
  /** Data-stream opens in flight; PUBLISH_DONE waits for them to settle (§10.12). */
  opening: Set<Promise<void>>;
}

/**
 * The reason handed to a data stream the session resets when its subscription ends (§3.3.4 CANCELLED): a
 * WebTransportError in a browser, which is the only form WebTransport reads the code from.
 */
function dataStreamReset(): unknown {
  return streamResetReason(STREAM_RESET_CODE.CANCELLED, 'the subscription ended');
}

function resetDataStreams(subscriber: SubscriberStream): void {
  for (const reset of [...subscriber.dataStreams]) reset();
}

/**
 * Hand the caller a data stream that leaves its subscription's open set once closed or reset. The abort signal forwards
 * a reset at once, so a reset never waits behind a backpressured write.
 */
function trackDataStream(stream: WritableStream<Uint8Array>, subscriber: SubscriberStream): WritableStream<Uint8Array> {
  const inner = stream.getWriter();
  const reset = (reason: unknown): void => {
    subscriber.dataStreams.delete(sessionReset);
    inner.abort(reason).catch(() => {});
  };
  const sessionReset = (): void => reset(dataStreamReset());

  subscriber.dataStreams.add(sessionReset);

  return new WritableStream<Uint8Array>({
    start(controller) {
      controller.signal.addEventListener('abort', () => reset(controller.signal.reason));
    },
    write: (chunk) => inner.write(chunk),
    close: () => {
      // The FIN is issued now as far as PUBLISH_DONE's ordering goes: every accepted write is already on this stream.
      subscriber.dataStreams.delete(sessionReset);
      return inner.close();
    },
    abort: (reason) => reset(reason),
  });
}

interface TrackRecord {
  trackProperties: KeyValuePair[];
  key: string;
  trackName: string;
  subscribers: SubscriberStream[];
  done: boolean;
  /**
   * Subscription ends (PUBLISH_DONE + FIN) still in flight, from any end path (pruned as they settle). `#endTrack`
   * folds them into `doneFlushed`, which `close()` drains. An ended subscription is already `finished`, and one whose
   * peer closed its request stream has left `subscribers`, so without this list its queued end would be skipped and
   * discarded by the transport close, turning a clean end into an abrupt one.
   */
  pendingEnds: Promise<void>[];
  /** Settles when the ends queued by `end()` land. */
  doneFlushed?: Promise<void>;
}

/** One accepted inbound SUBSCRIBE_NAMESPACE — the announce carrier. */
interface NamespaceStream {
  prefix: TrackNamespace;
  writer: WritableStreamDefaultWriter<Uint8Array>;
  /** Suffixes announced on this stream (for NAMESPACE_DONE at close). */
  announced: TrackNamespace[];
}

function trackKey(namespace: TrackNamespace, name: string): string {
  return `${namespace.join('/')}--${name}`;
}

function isNamespacePrefix(prefix: TrackNamespace, namespace: TrackNamespace): boolean {
  return prefix.length <= namespace.length && prefix.every((part, i) => namespace[i] === part);
}

// =============================================================================
// Session driver
// =============================================================================

class MoqtPublishSessionImpl implements MoqtPublishSession {
  readonly ready: Promise<void>;

  #transport: MoqtTransport;
  #config: MoqtPublishSessionConfig;
  #callbacks: MoqtPublishSessionCallbacks;

  #tracks = new Map<string, TrackRecord>();
  #namespaceStreams = new Set<NamespaceStream>();
  /**
   * Prefixes whose solicitation is mid-acceptance. Request handlers run concurrently and registration happens only
   * after the REQUEST_OK write settles, so an overlapping solicitation racing that await must be refused off this
   * reservation, not just off the registered set.
   */
  #pendingNamespacePrefixes = new Set<TrackNamespace>();
  /**
   * Every inbound request ID ever seen. Reuse closes the session with INVALID_REQUEST_ID (§10.1), and because aliases ARE the peer's
   * request IDs, a reused ID would hand two subscriptions the same alias and make their subgroup streams
   * indistinguishable — session-fatal, per the draft.
   */
  #peerRequestIds = new Set<number>();
  #desiredNamespace: TrackNamespace | undefined;
  #announcedFired = false;
  #receivedNamespaceSolicitation = false;

  #resolveReady!: () => void;
  #rejectReady!: (error: unknown) => void;
  #controlWriter: WritableStreamDefaultWriter<Uint8Array> | undefined;
  #receivedServerSetup = false;
  #receivedControlGoaway = false;
  /** Any GOAWAY (control or request stream) — the peer is migrating. */
  #receivedGoaway = false;
  #closing = false;
  #closeError: unknown;
  #destroyed = false;

  constructor(transport: MoqtTransport, config: MoqtPublishSessionConfig = {}) {
    this.#transport = transport;
    this.#config = config;
    this.#callbacks = config.callbacks ?? {};
    this.ready = new Promise((resolve, reject) => {
      this.#resolveReady = resolve;
      this.#rejectReady = reject;
    });
    this.ready.catch(() => {});

    void this.#start();
  }

  get namespaceSolicited(): boolean | undefined {
    const namespace = this.#desiredNamespace;
    if (namespace === undefined || !this.#receivedNamespaceSolicitation) return undefined;
    return [...this.#namespaceStreams].some((stream) => isNamespacePrefix(stream.prefix, namespace));
  }

  // ---------------------------------------------------------------------------
  // Session lifecycle
  // ---------------------------------------------------------------------------

  async #start(): Promise<void> {
    void this.#acceptUnidirectionalStreams();
    void this.#acceptBidirectionalStreams();
    void this.#transport.closed.then(
      () => this.#handleClosed(undefined),
      (error) => this.#handleClosed(error)
    );

    try {
      const control = await this.#transport.createUnidirectionalStream();

      this.#controlWriter = control.getWriter();
      // A control stream MUST stay open for the session's lifetime
      // (§3.3) — a peer stopping it has ended the session, so fail now
      // rather than on the next control write or the transport's own
      // `closed` (~an RTT later); the gap is long enough for a busy
      // publisher to keep churning streams against a dying transport.
      // A clean local close resolves the writer instead, and #fatal
      // no-ops once #handleClosed has run.
      void this.#controlWriter.closed.catch((error: unknown) => this.#fatal(error));
      const options: KeyValuePair[] = [
        {
          type: SETUP_OPTION.MOQT_IMPLEMENTATION,
          value: utf8Encode(this.#config.implementationName ?? DEFAULT_IMPLEMENTATION_NAME),
        },
        ...(this.#config.setupOptions ?? []),
      ];

      await this.#controlWriter.write(encodeSetup(options));
    } catch (error) {
      this.#fatal(error);
    }
  }

  #fatal(error: unknown): void {
    // Once an orderly close starts, late control-stream failures must not
    // replace its reason/code or interrupt the bounded FIN/retraction drain.
    if (this.#destroyed || this.#closing) return;

    const closeCode = isMoqtProtocolError(error) ? error.code : SESSION_ERROR.INTERNAL_ERROR;

    try {
      this.#transport.close({ closeCode, reason: error instanceof Error ? error.message : 'session error' });
    } catch {
      // transport already gone
    }

    this.#handleClosed(error);
  }

  #handleClosed(receivedError: unknown, { expected = false }: { expected?: boolean } = {}): void {
    if (this.#destroyed) return;

    this.#destroyed = true;
    const error = receivedError ?? this.#closeError;

    if (!this.#receivedServerSetup) {
      this.#rejectReady(error ?? new MoqtProtocolError('session closed before server SETUP'));
    }

    for (const track of this.#tracks.values()) {
      for (const subscriber of track.subscribers) void subscriber.cancel();
    }

    this.#tracks.clear();
    this.#namespaceStreams.clear();
    this.#controlWriter = undefined;

    // Mirror the subscribe driver: a transport that drops before server
    // SETUP is a session failure even when the close itself was clean.
    const closeError =
      error ??
      (expected || this.#receivedServerSetup ? undefined : new MoqtProtocolError('session closed before server SETUP'));

    this.#callbacks.onClosed?.(closeError === undefined ? {} : { error: closeError });
  }

  close(closeCode = SESSION_ERROR.NO_ERROR, reason = ''): void {
    this.#close(closeCode, reason);
  }

  #close(closeCode: number, reason: string, error?: unknown): void {
    if (this.#destroyed || this.#closing) return;

    this.#closing = true;
    this.#closeError = error;
    void this.#drainAndClose(closeCode, reason, error);
  }

  /**
   * Orderly close: closing the transport discards queued data (WebTransport semantics), so the subscription ends
   * (PUBLISH_DONE + FIN) and NAMESPACE_DONE retractions must be given a bounded window to land first — a peer should
   * observe every track ending cleanly, not an abrupt transport end. No GOAWAY is sent (see the module doc).
   */
  async #drainAndClose(closeCode: number, reason: string, error?: unknown): Promise<void> {
    // One macrotask beat before the last-resort sweep below: track owners
    // tearing down in the same reactive flush (`setupTrackPublishers`'s
    // cleanup runs on a microtask after ours on the unpublish path) get to
    // end their tracks themselves.
    await new Promise((resolve) => setTimeout(resolve, 0));

    if (this.#destroyed) return;

    for (const track of this.#tracks.values()) {
      this.#endTrack(track);
    }

    const writes: Promise<unknown>[] = [];

    for (const track of this.#tracks.values()) {
      if (track.doneFlushed) writes.push(track.doneFlushed);
      // An end started after the track's own end (none should, but a stray one must not be cut off).
      writes.push(...track.pendingEnds);
    }

    for (const stream of this.#namespaceStreams) {
      for (const suffix of stream.announced.splice(0)) {
        writes.push(stream.writer.write(encodeNamespaceDone(suffix)).catch(() => {}));
      }
    }

    let flushTimer: ReturnType<typeof setTimeout> | undefined;

    try {
      await Promise.race([
        Promise.all(writes),
        new Promise<void>((resolve) => {
          flushTimer = setTimeout(resolve, CLOSE_FLUSH_TIMEOUT_MS);
        }),
      ]);
    } finally {
      if (flushTimer !== undefined) clearTimeout(flushTimer);
    }

    if (this.#destroyed) return;

    try {
      this.#transport.close({ closeCode, reason });
    } catch {
      // transport already gone
    }

    this.#handleClosed(error, { expected: error === undefined });
  }

  destroy(error?: unknown): void {
    this.#close(SESSION_ERROR.NO_ERROR, '', error);
  }

  // ---------------------------------------------------------------------------
  // Announcing and the track registry
  // ---------------------------------------------------------------------------

  announce(trackNamespace: TrackNamespace): void {
    if (this.#destroyed || this.#closing) return;

    this.#desiredNamespace = trackNamespace;

    for (const stream of this.#namespaceStreams) {
      this.#reconcileAnnounce(stream);
    }
  }

  /**
   * Write the desired namespace onto a solicitation stream whose prefix covers it. Solicitations, `announce()`, and the
   * first `registerTrack()` arrive in any order — the relay's SUBSCRIBE_NAMESPACE races our post-SETUP setup — so all
   * three paths funnel here. The announce is deferred until at least one track is registered: an announced namespace
   * invites immediate SUBSCRIBEs, and answering one with DOES_NOT_EXIST is terminal on moq-lite-rs (the relay aborts
   * the request rather than retrying), so advertising an empty registry would permanently break the first viewer in the
   * pre-registration window.
   */
  #reconcileAnnounce(stream: NamespaceStream): void {
    const namespace = this.#desiredNamespace;
    if (namespace === undefined || this.#tracks.size === 0) return;

    if (!isNamespacePrefix(stream.prefix, namespace)) return;

    const suffix = namespace.slice(stream.prefix.length);
    if (stream.announced.some((existing) => existing.join('/') === suffix.join('/'))) return;

    stream.announced.push(suffix);
    stream.writer
      .write(encodeNamespace(suffix))
      .then(() => {
        if (this.#destroyed || this.#announcedFired) return;

        this.#announcedFired = true;
        this.#callbacks.onAnnounced?.({ namespace });
      })
      .catch(() => {
        // The response half died alone (a reset the request half may
        // never mirror): the entry never reached the peer, no future
        // write can, and the read loop may stay open forever — the
        // carrier is dead. Deregister it so the peer's replacement
        // solicitation is a fresh start rather than a PREFIX_OVERLAP
        // against a corpse; if another path already swept it, the loss
        // was reported there.
        if (!this.#namespaceStreams.delete(stream)) return;

        stream.announced = stream.announced.filter((existing) => existing !== suffix);
        void stream.writer.close().catch(() => {});
        this.#reportAnnounceLoss();
      });
  }

  /**
   * Fire `onAnnounceEnded` when no solicitation carries the announce anymore — shared by the read-loop cleanup and the
   * response-write failure path. Silent while closing, destroyed, or migrating (a GOAWAY makes the peer's streams
   * ending the orderly drain).
   */
  #reportAnnounceLoss(): void {
    if (this.#destroyed || this.#closing || this.#receivedGoaway) return;

    const stillCarried = [...this.#namespaceStreams].some((stream) => stream.announced.length > 0);
    if (stillCarried) return;

    this.#callbacks.onAnnounceEnded?.({
      error: new MoqtProtocolError('the namespace solicitation carrying the announce ended'),
    });
  }

  registerTrack(options: RegisterTrackOptions): RegisteredTrack {
    if (this.#destroyed || this.#closing) {
      // A closed session must not repopulate `#tracks` — hand back an
      // inert handle so late callers still get the RegisteredTrack shape.
      return { trackName: options.trackName, end: () => {} };
    }

    const track: TrackRecord = {
      trackProperties: options.trackProperties ?? [],
      key: trackKey(options.trackNamespace, options.trackName),
      trackName: options.trackName,
      subscribers: [],
      done: false,
      pendingEnds: [],
    };

    this.#tracks.set(track.key, track);

    // The first registration may be what the deferred announce was
    // waiting for (see #reconcileAnnounce).
    if (this.#tracks.size === 1) {
      for (const stream of this.#namespaceStreams) {
        this.#reconcileAnnounce(stream);
      }
    }

    return {
      trackName: options.trackName,
      end: () => this.#endTrack(track),
    };
  }

  /**
   * End every live subscription with PUBLISH_DONE `TRACK_ENDED` and refuse future SUBSCRIBEs. The aggregated write
   * completion lands on `track.doneFlushed` so `close()` can hold the transport open until the PUBLISH_DONEs and FINs
   * reach the wire.
   */
  #endTrack(track: TrackRecord): void {
    if (track.done) return;

    track.done = true;
    let hadReported = false;

    for (const subscriber of track.subscribers) {
      if (!subscriber.finished)
        this.#endSubscription(track, subscriber, PUBLISH_DONE_STATUS.TRACK_ENDED, 'track ended');

      // The local end is authoritative: report each live subscription
      // ended NOW rather than when the peer eventually closes its
      // request direction — otherwise `subscriberCount` and the binding
      // stay live (and publishers keep opening data streams) until the
      // peer notices the FIN. Clearing `reported` keeps the read loop's
      // own end notification paired: it fires on the same flag.
      if (subscriber.reported) {
        subscriber.reported = false;
        hadReported = true;

        if (!this.#destroyed) this.#callbacks.onSubscribeEnd?.({ requestId: subscriber.requestId });
      }
    }

    if (hadReported && !this.#destroyed) {
      this.#callbacks.onTrackBinding?.({ trackName: track.trackName, trackAlias: undefined });
    }

    track.doneFlushed = Promise.all([...track.pendingEnds]).then(() => {});
  }

  /**
   * End an Established subscription from the publisher side (§3.3.2, §10.12). Marks it finished at once, so no new
   * data stream opens on its alias. Then, in order: wait for data-stream opens in flight, reset every data stream the
   * caller has not closed (a caller that follows the binding has closed them already), write `preceding` (a
   * REQUEST_ERROR answering a failed update) and PUBLISH_DONE with the subscription's Stream Count, and FIN. Settles
   * when the FIN lands or the stream fails; never rejects. It stays in `track.pendingEnds` until then, so `close()`
   * drains it whichever path started it.
   */
  #endSubscription(
    track: TrackRecord,
    subscriber: SubscriberStream,
    status: number,
    reason: string,
    preceding?: Uint8Array
  ): void {
    subscriber.finished = true;

    const ended = (async () => {
      if (subscriber.opening.size > 0) await Promise.all(subscriber.opening);

      resetDataStreams(subscriber);
      const done = encodePublishDone(status, subscriber.streamCount, reason);

      if (preceding) void subscriber.writer.write(preceding).catch(() => {});
      await subscriber.writer.write(done);
      await subscriber.writer.close();
    })().catch(() => {});

    track.pendingEnds.push(ended);
    void ended.then(() => {
      const index = track.pendingEnds.indexOf(ended);

      if (index >= 0) track.pendingEnds.splice(index, 1);
    });
  }

  #liveSubscription(trackAlias: number): SubscriberStream | undefined {
    for (const track of this.#tracks.values()) {
      for (const subscriber of track.subscribers) {
        if (subscriber.trackAlias === trackAlias && subscriber.accepted && !subscriber.finished) return subscriber;
      }
    }

    return undefined;
  }

  async openUniStream(trackAlias: number): Promise<WritableStream<Uint8Array>> {
    // Refuse rather than touch the transport once the session is going
    // away. Encoders keep producing frames for a beat after a peer kills
    // the session, and hammering createUnidirectionalStream() on a
    // torn-down WebTransport segfaults Chromium's renderer (null deref in
    // the native session teardown race) — observed against a relay that
    // resets every stream on protocol disagreement.
    if (this.#destroyed || this.#closing) throw new Error('moq publish session: closed');

    const subscriber = this.#liveSubscription(trackAlias);

    if (!subscriber) throw new Error(`moq publish session: no live subscription for track alias ${trackAlias}`);

    let settle!: () => void;
    const opening = new Promise<void>((resolve) => {
      settle = resolve;
    });

    subscriber.opening.add(opening);

    try {
      const stream = await this.#transport.createUnidirectionalStream();

      if (subscriber.finished) {
        // The subscription ended while this stream was opening. Its PUBLISH_DONE waits on `opening`, so the reset lands
        // before it and the stream is not counted.
        stream.abort(dataStreamReset()).catch(() => {});
        throw new Error(`moq publish session: the subscription for track alias ${trackAlias} ended`);
      }

      subscriber.streamCount += 1;
      return trackDataStream(stream, subscriber);
    } finally {
      subscriber.opening.delete(opening);
      settle();
    }
  }

  // ---------------------------------------------------------------------------
  // Incoming unidirectional streams (control, padding)
  // ---------------------------------------------------------------------------

  async #acceptUnidirectionalStreams(): Promise<void> {
    const reader = this.#transport.incomingUnidirectionalStreams.getReader();

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        void this.#handleUnidirectionalStream(value).catch((error) => {
          if (isMoqtProtocolError(error)) this.#fatal(error);
        });
      }
    } catch {
      // Transport went away; `closed` handles the surfacing.
    } finally {
      reader.releaseLock();
    }
  }

  async #handleUnidirectionalStream(stream: ReadableStream<Uint8Array>): Promise<void> {
    const reader = new StreamReader(stream);
    const streamType = await reader.readVarint();

    if (streamType === STREAM_TYPE.SETUP) {
      await this.#runControlStream(reader);
      return;
    }

    if (streamType === STREAM_TYPE.PADDING) {
      await reader.cancel();
      return;
    }

    await reader.cancel();

    if (isSubgroupHeaderType(streamType) || streamType === STREAM_TYPE.FETCH_HEADER) {
      throw new MoqtProtocolError('publish session received a data stream');
    }

    throw new MoqtProtocolError(`unknown unidirectional stream type 0x${streamType.toString(16)}`);
  }

  /** Read one control-message frame using the pull-based stream reader. */
  async #readControlFrame(reader: StreamReader, type: number): Promise<ControlMessage> {
    const high = await reader.readUint8();
    const low = await reader.readUint8();
    const body = await reader.readBytes(high * 256 + low);

    return decodeControlMessage({ type, body });
  }

  async #runControlStream(reader: StreamReader): Promise<void> {
    // The stream-type varint (0x2F00) doubles as the first message's type.
    const setup = await this.#readControlFrame(reader, MESSAGE_TYPE.SETUP);
    if (setup.kind !== 'setup') throw new MoqtProtocolError('control stream did not begin with SETUP');

    if (this.#receivedServerSetup) {
      throw new MoqtProtocolError('received a second control stream');
    }

    this.#receivedServerSetup = true;
    this.#resolveReady();
    this.#callbacks.onReady?.(setup.options);

    while (!(await reader.atEnd())) {
      const type = await reader.readVarint();
      const message = await this.#readControlFrame(reader, type);
      if (message.kind !== 'goaway') throw new MoqtProtocolError(`unexpected ${message.kind} on the control stream`);

      if (this.#receivedControlGoaway) {
        throw new MoqtProtocolError('received more than one GOAWAY on the control stream');
      }

      this.#receivedControlGoaway = true;
      this.#receivedGoaway = true;
      this.#callbacks.onGoaway?.(message);
    }

    // A closed control stream ends the session (§3.3).
    if (!this.#destroyed) {
      this.#fatal(new MoqtProtocolError('peer closed its control stream'));
    }
  }

  // ---------------------------------------------------------------------------
  // Incoming bidirectional streams (peer-initiated requests)
  // ---------------------------------------------------------------------------

  async #acceptBidirectionalStreams(): Promise<void> {
    const reader = this.#transport.incomingBidirectionalStreams.getReader();

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        void this.#handleIncomingRequest(value).catch((error) => {
          if (isMoqtProtocolError(error)) this.#fatal(error);
        });
      }
    } catch {
      // Transport went away.
    } finally {
      reader.releaseLock();
    }
  }

  async #handleIncomingRequest(stream: BidirectionalStreamLike): Promise<void> {
    const reader = new StreamReader(stream.readable);
    const type = await reader.readVarint();
    const message = await this.#readControlFrame(reader, type);

    // Reserve the request ID for EVERY request-initiating kind — the
    // uniqueness invariant is session-wide, so an ID burned on a
    // rejected PUBLISH or FETCH must not be reusable by a later
    // SUBSCRIBE (whose alias it would become).
    const initiatingRequestId =
      message.kind === 'fetch'
        ? message.request.requestId
        : message.kind === 'subscribe' ||
            message.kind === 'subscribe-namespace' ||
            message.kind === 'publish' ||
            message.kind === 'publish-namespace' ||
            message.kind === 'track-status'
          ? message.requestId
          : undefined;

    if (initiatingRequestId !== undefined) this.#claimPeerRequestId(initiatingRequestId);

    if (message.kind === 'subscribe') {
      await this.#handleIncomingSubscribe(stream, reader, message);
      return;
    }

    if (message.kind === 'subscribe-namespace') {
      await this.#handleSubscribeNamespace(stream, reader, message);
      return;
    }

    // Everything else: this endpoint only serves its own published tracks.
    const respond = async (bytes: Uint8Array) => {
      const writer = stream.writable.getWriter();

      try {
        await writer.write(bytes);
        await writer.close();
      } catch {
        // Peer cancelled; nothing to clean up.
      }

      await reader.cancel();
    };
    const errorCode = message.kind === 'publish' ? REQUEST_ERROR_CODE.UNINTERESTED : REQUEST_ERROR_CODE.NOT_SUPPORTED;

    await respond(encodeRequestError(errorCode, 'publish-only endpoint'));
  }

  /**
   * Request IDs are consumed once per session (§10.1) — by every request-initiating message and, since draft-20 gave it
   * an id of its own, by REQUEST_UPDATE. This endpoint is always the client, so the peer's ids are odd; a reused id or
   * an even one closes the session with INVALID_REQUEST_ID.
   */
  #claimPeerRequestId(requestId: number): void {
    if (requestId % 2 !== 1) {
      throw new MoqtProtocolError(`peer sent even request id ${requestId}`, SESSION_ERROR.INVALID_REQUEST_ID);
    }

    if (this.#peerRequestIds.has(requestId)) {
      throw new MoqtProtocolError(`peer reused request id ${requestId}`, SESSION_ERROR.INVALID_REQUEST_ID);
    }

    this.#peerRequestIds.add(requestId);
  }

  /**
   * A peer's namespace solicitation — the announce carrier. Accepted with REQUEST_OK, then held open for the session's
   * lifetime: NAMESPACE / NAMESPACE_DONE entries for every announced namespace under the prefix ride this stream. A
   * clean requester-side FIN is half-closure, not withdrawal (§3.3.2); only a reset (or a rejected update, §10.9.1)
   * ends the carrier, and its loss while the announce is live means the peer refused or withdrew the ingest path.
   */
  async #handleSubscribeNamespace(
    stream: BidirectionalStreamLike,
    reader: StreamReader,
    subscribeNamespace: Extract<ControlMessage, { kind: 'subscribe-namespace' }>
  ): Promise<void> {
    const writer = stream.writable.getWriter();
    const prefix = subscribeNamespace.trackNamespacePrefix;

    // §10.18: a solicitation overlapping an established one (either
    // prefix containing the other) is refused with PREFIX_OVERLAP —
    // announcing on both would hand the peer duplicate namespace state
    // with inconsistent withdrawals.
    const overlaps =
      [...this.#namespaceStreams].some(
        (existing) => isNamespacePrefix(existing.prefix, prefix) || isNamespacePrefix(prefix, existing.prefix)
      ) ||
      [...this.#pendingNamespacePrefixes].some(
        (pending) => isNamespacePrefix(pending, prefix) || isNamespacePrefix(prefix, pending)
      );

    if (overlaps) {
      try {
        await writer.write(encodeRequestError(REQUEST_ERROR_CODE.PREFIX_OVERLAP, 'overlapping namespace prefix'));
        await writer.close();
      } catch {
        // Peer cancelled.
      }

      await reader.cancel();
      return;
    }

    // Reserve the prefix across the acceptance write; the registration
    // below happens in the same microtask as the release, so a racing
    // overlapping solicitation always sees one or the other.
    this.#pendingNamespacePrefixes.add(prefix);
    let accepted = false;

    try {
      await writer.write(encodeRequestOk());
      accepted = true;
    } catch {
      // Peer cancelled before the acceptance landed.
    } finally {
      this.#pendingNamespacePrefixes.delete(prefix);
    }

    if (!accepted) {
      await reader.cancel();
      return;
    }

    const record: NamespaceStream = {
      prefix,
      writer,
      announced: [],
    };

    this.#receivedNamespaceSolicitation = true;
    this.#namespaceStreams.add(record);
    this.#reconcileAnnounce(record);

    // Hold the stream. The known peers write nothing further on it.
    let failedUpdate = false;

    try {
      while (!(await reader.atEnd())) {
        const type = await reader.readVarint();
        const message = await this.#readControlFrame(reader, type);

        if (message.kind === 'goaway') {
          this.#receivedGoaway = true;
          this.#callbacks.onGoaway?.(message);
        } else if (message.kind === 'request-update') {
          this.#claimPeerRequestId(message.requestId);
          // Namespace subscriptions may legally be updated (§10.9). v1
          // applies none of it (a prefix change would re-base every
          // entry), so per §10.9.1 the update is answered with an error
          // and the request stream closes below — never session-fatal
          // (only the reused Request ID above is).
          this.#callbacks.onRequestUpdate?.({
            requestId: subscribeNamespace.requestId,
            updateRequestId: message.requestId,
            parameters: message.parameters,
          });
          void writer
            .write(encodeRequestError(REQUEST_ERROR_CODE.NOT_SUPPORTED, 'namespace update not supported'))
            .catch(() => {});
          failedUpdate = true;
          break;
        } else {
          throw new MoqtProtocolError(`unexpected ${message.kind} on an incoming subscribe-namespace stream`);
        }
      }

      if (!failedUpdate) {
        // A clean requester-side FIN is NOT a cancellation (§3.3.2 — the
        // peer merely commits to sending no updates): the response side
        // keeps carrying NAMESPACE entries until a reset or the
        // transport ends, so the carrier stays registered and open.
        return;
      }
    } catch (error) {
      // A protocol violation is session-fatal; #handleClosed sweeps the
      // carrier state with everything else.
      if (isMoqtProtocolError(error)) throw error;
      // Reset — the peer withdrew the namespace subscription.
    }

    // Withdrawal (reset) or a failed update: the carrier is over.
    this.#namespaceStreams.delete(record);
    void writer.close().catch(() => {});

    if (record.announced.length > 0) this.#reportAnnounceLoss();
  }

  async #handleIncomingSubscribe(
    stream: BidirectionalStreamLike,
    reader: StreamReader,
    subscribe: Extract<ControlMessage, { kind: 'subscribe' }>
  ): Promise<void> {
    const track = this.#tracks.get(trackKey(subscribe.trackNamespace, subscribe.trackName));
    const writer = stream.writable.getWriter();

    if (!track || track.done) {
      try {
        await writer.write(encodeRequestError(REQUEST_ERROR_CODE.DOES_NOT_EXIST, 'unknown track'));
        await writer.close();
      } catch {
        // Peer cancelled.
      }

      await reader.cancel();
      return;
    }

    // The peer's request IDs are session-unique, so they double as the
    // track alias — binding one alias to two live request IDs is a
    // session-fatal Duplicate on moq-lite-rs.
    const trackAlias = subscribe.requestId;
    const subscriber: SubscriberStream = {
      requestId: subscribe.requestId,
      trackAlias,
      writer,
      cancel: async () => {
        subscriber.finished = true;
        writer.abort().catch(() => {});

        try {
          await reader.cancel();
        } catch {
          // The stream already errored (e.g. the peer reset it first).
        }
      },
      accepted: false,
      reported: false,
      // Forward State defaults to 1; `forward: 0` subscribes without
      // authorizing data until a REQUEST_UPDATE flips it.
      forwarding: subscribe.parameters.forward !== 0,
      finished: false,
      streamCount: 0,
      dataStreams: new Set(),
      opening: new Set(),
    };

    track.subscribers.push(subscriber);

    try {
      await writer.write(encodeSubscribeOk(trackAlias, {}, track.trackProperties));
      subscriber.accepted = true;
    } catch {
      // Peer cancelled before the response landed.
      track.subscribers = track.subscribers.filter((s) => s !== subscriber);
      await reader.cancel();
      return;
    }

    // Overlapping same-track subscriptions settle their SUBSCRIBE_OK
    // writes independently, so a newer subscription may have replaced
    // this one (marking it finished) while this acceptance was still in
    // flight — a stale completion must not steal the binding back onto
    // an alias whose stream is already ending, and a subscription that
    // was never live is never reported (`onSubscribeEnd` pairs on the
    // same flag, keeping subscriber counts balanced).
    if (!subscriber.finished) {
      subscriber.reported = true;
      this.#callbacks.onSubscribe?.(subscribe);

      // The newest subscription wins the binding, and every live
      // PREDECESSOR (arrival order — a successor may accept before an
      // older backpressured write settles, and must not be swept by it)
      // ends cleanly (FIN) so the peer's bookkeeping resolves. One live
      // subscription per track is a deliberate v1 constraint: the spec
      // permits concurrent same-track subscriptions, but serving them
      // means writing every group once per alias, and the known peers
      // (moq-lite-rs relays) hold at most one upstream subscription per
      // track and fan out on their side — the sweep also covers their
      // resubscribe races.
      for (const other of track.subscribers.slice(0, track.subscribers.indexOf(subscriber))) {
        if (other.finished) continue;

        // No status names a replacement. TRACK_ENDED is the one the relay finishes a track on (anything else aborts
        // it), and the newer subscription carries the track on.
        this.#endSubscription(track, other, PUBLISH_DONE_STATUS.TRACK_ENDED, 'superseded by a newer subscription');
      }

      this.#callbacks.onTrackBinding?.({
        trackName: track.trackName,
        trackAlias: subscriber.forwarding ? trackAlias : undefined,
      });
    }

    // Keep serving the request stream: REQUEST_UPDATEs arrive here.
    try {
      while (!(await reader.atEnd())) {
        const type = await reader.readVarint();
        const message = await this.#readControlFrame(reader, type);

        if (message.kind === 'request-update') {
          this.#claimPeerRequestId(message.requestId);
          const { forward, subscriberPriority: _priority, ...unsupported } = message.parameters;

          // Forward State toggles ride REQUEST_UPDATE; the binding must
          // follow so data stops (or starts) with the subscription's
          // authorization. Newest-wins keeps this subscriber the track's
          // only live one, so its state IS the binding.
          if (forward !== undefined) {
            subscriber.forwarding = forward !== 0;

            if (!subscriber.finished && subscriber.accepted) {
              this.#callbacks.onTrackBinding?.({
                trackName: track.trackName,
                trackAlias: subscriber.forwarding ? subscriber.trackAlias : undefined,
              });
            }
          }

          this.#callbacks.onRequestUpdate?.({
            requestId: subscribe.requestId,
            updateRequestId: message.requestId,
            parameters: message.parameters,
          });

          if (!subscriber.finished) {
            if (Object.keys(unsupported).length > 0) {
              // Acknowledging an update we did not apply would leave the
              // peer serving stale expectations (a filter or range it
              // believes is in effect). Forward State is applied above
              // and priority is advisory (the known relays discard ours
              // too); anything else ends this request honestly: the
              // update's REQUEST_ERROR, then PUBLISH_DONE UPDATE_FAILED
              // (§10.9.1).
              this.#endSubscription(
                track,
                subscriber,
                PUBLISH_DONE_STATUS.UPDATE_FAILED,
                'unsupported subscription update',
                encodeRequestError(REQUEST_ERROR_CODE.NOT_SUPPORTED, 'unsupported subscription update')
              );

              if (subscriber.reported) {
                subscriber.reported = false;
                this.#callbacks.onSubscribeEnd?.({ requestId: subscribe.requestId });
                const binding = track.subscribers.filter((s) => !s.finished && s.accepted && s.forwarding).at(-1);

                this.#callbacks.onTrackBinding?.({ trackName: track.trackName, trackAlias: binding?.trackAlias });
              }
            } else {
              // The subscribe driver treats REQUEST_OK as the update's
              // completion (`onUpdateOk`). Strictly reactive: a peer that
              // never sends REQUEST_UPDATE (moq-lite-rs parks on end-of-
              // stream after SUBSCRIBE_OK) never sees a trailing byte.
              void writer.write(encodeRequestOk()).catch(() => {});
            }
          }
        } else if (message.kind === 'goaway') {
          this.#receivedGoaway = true;
          this.#callbacks.onGoaway?.(message);
        } else {
          throw new MoqtProtocolError(`unexpected ${message.kind} on an incoming subscribe stream`);
        }
      }

      // The peer FINed its direction while the subscription was still ours
      // to end. moq-relay pairs that FIN with STOP_SENDING (a cancellation,
      // so the write fails harmlessly); a peer that only half-closes still
      // gets the PUBLISH_DONE §3.3.2 requires before our FIN. No status
      // names this, so it reports the publisher's own limitation.
      if (!subscriber.finished) {
        this.#endSubscription(
          track,
          subscriber,
          PUBLISH_DONE_STATUS.INTERNAL_ERROR,
          'subscriber closed its request stream'
        );
      }
    } catch (error) {
      if (isMoqtProtocolError(error)) throw error;
      // Reset — the subscriber cancelled (§3.3.3); ordinary end-of-request.
      // A cancelled subscription gets no PUBLISH_DONE, and its open data
      // streams are reset (§5.1.1).
      resetDataStreams(subscriber);
    } finally {
      const alreadyEnding = subscriber.finished;

      subscriber.finished = true;
      track.subscribers = track.subscribers.filter((s) => s !== subscriber);

      // The subscription is over both ways — mirror of the namespace
      // stream's cleanup: an unclosed response direction would leak a
      // half-open stream per cancelled request until the transport closes.
      // An already-aborted writer rejects harmlessly. A subscription this
      // side ended closes its writer after PUBLISH_DONE instead.
      if (!alreadyEnding) void writer.close().catch(() => {});

      if (!this.#destroyed && subscriber.reported) {
        this.#callbacks.onSubscribeEnd?.({ requestId: subscribe.requestId });
        // Only accepted, forwarding subscriptions may carry the binding:
        // one whose SUBSCRIBE_OK is still in flight could yet fail (its
        // removal would strand `trackBindings` on an alias the peer never
        // registered), and one with Forward State 0 forbids data. When a
        // pending one accepts, it emits its own binding.
        const binding = track.subscribers.filter((s) => !s.finished && s.accepted && s.forwarding).at(-1);

        this.#callbacks.onTrackBinding?.({ trackName: track.trackName, trackAlias: binding?.trackAlias });
      }
    }
  }
}

/**
 * Create a publish-capable MOQT session driver over an established transport.
 *
 * @example
 *   ```ts
 *   const session = createMoqtPublishSession(transport, {
 *     callbacks: { onSubscribe: (s) => console.log('serving', s.trackName) },
 *   });
 *   await session.ready;
 *   session.announce(['live', 'abc123']);
 *   const track = session.registerTrack({ trackNamespace: ['live', 'abc123'], trackName: 'video' });
 *   ```;
 */
export function createMoqtPublishSession(
  transport: MoqtTransport,
  config: MoqtPublishSessionConfig = {}
): MoqtPublishSession {
  return new MoqtPublishSessionImpl(transport, config);
}
