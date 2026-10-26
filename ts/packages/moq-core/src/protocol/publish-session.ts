/**
 * Publish-capable MOQT session (moq-transport draft-22), the publish-side sibling of `network/moqt/session.ts`'s
 * subscribe-only driver.
 *
 * Owns the protocol mechanics over an established transport: the SETUP exchange on paired unidirectional control
 * streams (§6.3), and the announce-and-serve ingest flow — answering the peer's SUBSCRIBE_NAMESPACE solicitation with
 * REQUEST_OK and a NAMESPACE entry for the announced namespace (§9.15, §9.16), then serving the per-track SUBSCRIBEs the
 * peer routes back (answered with SUBSCRIBE_OK carrying a publisher-assigned track alias, §9.7) for as long as each
 * subscription's request stream stays open. A TRACK_STATUS for a registered track is answered as its SUBSCRIBE would
 * be, with TRACK_STATUS_OK and no subscription (§9.13); every other request is refused.
 *
 * Flow choice: the publisher initiates no request streams at all. moq-relay 0.14.7 removed PUBLISH-based ingest
 * (upstream `de336492`: every PUBLISH is answered with a request error), and it solicits namespaces itself — one
 * SUBSCRIBE_NAMESPACE per authorized prefix, sent immediately after SETUP. Announcing rides the solicitation stream as
 * NAMESPACE entries (suffix-relative, parameterless — the relay treats even an empty parameter count as malformed);
 * data flows only on aliases bound by our own SUBSCRIBE_OKs, per subscription.
 *
 * Teardown follows draft-22: every end of an Established subscription the publisher chooses resets the subscription's
 * still-open data streams, then writes PUBLISH_DONE (status, the number of data streams opened for it, reason) and FINs
 * the request stream (§6.4.2.2, §9.9). moq-relay 0.15.4 and later require that PUBLISH_DONE, and treat a bare FIN as a
 * failed request; 0.15.1 to 0.15.3 abort the track on any byte after SUBSCRIBE_OK, PUBLISH_DONE included, so this core
 * needs a relay at 0.15.4 or later for a clean end. An orderly `close()` holds the transport open, bounded, until the
 * peer closes its side of each ended request stream, which moq-relay does only after it has read the PUBLISH_DONE and
 * finished the track. The publisher sends no GOAWAY: 0.14.17 closes the session on a client GOAWAY (code 17), 0.15.1
 * and later record it and keep the session. Aliases are the peer's own request IDs,
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
  compareLocations,
  decodeControlMessage,
  encodeNamespace,
  encodeNamespaceDone,
  encodePublishDone,
  encodeRequestError,
  encodeRequestOk,
  encodeSetup,
  encodeSubscribeOk,
  type KeyValuePair,
  type Location,
  MAX_CONTROL_MESSAGE_LENGTH,
  MESSAGE_TYPE,
  type MessageParameters,
  type MoqtProtocolId,
  PUBLISH_DONE_STATUS,
  REQUEST_ERROR_CODE,
  resolveMoqtProtocol,
  SETUP_OPTION,
  STREAM_RESET_CODE,
  type TrackNamespace,
} from './control-messages.js';
import { isMoqtProtocolError, MoqtProtocolError, SESSION_ERROR } from './errors.js';
import { isSubgroupHeaderType, STREAM_TYPE } from './object-stream.js';
import type { BidirectionalStreamLike } from './request-stream.js';
import type { Goaway, MoqtSendStreamOptions, MoqtTransport } from './session.js';
import { streamResetReason } from './stream-reset.js';
import { isSessionError } from './transport-error.js';

// =============================================================================
// Driver types
// =============================================================================

/** A SUBSCRIBE from the peer, as passed to `onSubscribe` once it has been answered with SUBSCRIBE_OK. */
export interface IncomingSubscribe {
  /** The SUBSCRIBE's Request ID; the session also uses it as the subscription's Track Alias. */
  requestId: number;
  /** Track namespace requested. */
  trackNamespace: TrackNamespace;
  /** Track name requested. */
  trackName: string;
  /** Message parameters the SUBSCRIBE carried. */
  parameters: MessageParameters;
}

/** Event callbacks for {@link createMoqtPublishSession}. All are optional. */
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
   * The session could not build its SUBSCRIBE_OK for a SUBSCRIBE to a registered track, and answered REQUEST_ERROR
   * `INTERNAL_ERROR` instead. The track stays registered and the session stays up, but a cause that lies in the track
   * (its properties) fails every subscription to it. `error` is the encoder's failure.
   */
  onSubscribeFailed?(info: { requestId: number; trackName: string; error: unknown }): void;
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
   * SUBSCRIBE_NAMESPACE); `updateRequestId` is the update's own, freshly consumed Request ID (§6.4.2.1). A subscription
   * update may carry Forward State, a subscriber priority and the two delivery timeouts (§5.2), which the session leaves
   * to its caller; one carrying anything else is answered with REQUEST_ERROR and ends the subscription.
   */
  onRequestUpdate?(info: { requestId: number; updateRequestId: number; parameters: MessageParameters }): void;
  /** The session ended — transport closed, or a fatal protocol error. */
  onClosed?(info: { error?: unknown }): void;
}

/** Options for {@link createMoqtPublishSession}. */
export interface MoqtPublishSessionConfig {
  /** Extra Setup Options to send (an implementation identifier is added automatically). */
  setupOptions?: KeyValuePair[];
  /** Identifies this implementation in SETUP (§9.1.5). */
  implementationName?: string;
  /** Session event callbacks. */
  callbacks?: MoqtPublishSessionCallbacks;
}

/**
 * The largest encoded track property block every SUBSCRIBE_OK this session sends can carry. The
 * properties ride in SUBSCRIBE_OK, a control message whose length field is 16 bits (§9). The rest
 * of its body is the track alias, a varint of up to 9 bytes on the wire (it is the peer's request
 * ID, so it is unknown until a subscription arrives), and the count of message parameters, of which
 * this session sends none (1 byte). A longer block registers but can never answer a subscription.
 */
export const MAX_TRACK_PROPERTIES_LENGTH = MAX_CONTROL_MESSAGE_LENGTH - 9 - 1;

/** A track for {@link MoqtPublishSession.registerTrack}. */
export interface RegisterTrackOptions {
  /** Track metadata is explicit; raw tracks have no implied timestamp unit. */
  trackProperties?: KeyValuePair[];
  /** Track namespace the peer's SUBSCRIBE must name. */
  trackNamespace: TrackNamespace;
  /** Track name the peer's SUBSCRIBE must name. */
  trackName: string;
}

/** A track registered with {@link MoqtPublishSession.registerTrack}. */
export interface RegisteredTrack {
  /** The registered track name. */
  readonly trackName: string;
  /**
   * End the track: end every live subscription (its data streams closed, then PUBLISH_DONE `TRACK_ENDED` and a FIN on
   * its request stream) and refuse future SUBSCRIBEs and TRACK_STATUS requests with DOES_NOT_EXIST. Idempotent.
   */
  end(): void;
  /**
   * Record that the object at `location` was handed to the transport. The session keeps the largest location recorded
   * (group first, then object) and answers TRACK_STATUS with it as LARGEST_OBJECT (draft-22 §9.20.17); before the first
   * record the answer has none. The session does not see the data streams' contents, so whoever writes them reports
   * here. SUBSCRIBE_OK does not carry it.
   */
  objectPublished(location: Location): void;
}

/** A publish-capable MOQT session over an established transport. Created by {@link createMoqtPublishSession}. */
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
   * PUBLISH_DONE is written (§9.9). Rejects once the session is closing, when no live subscription holds the alias,
   * and when the subscription ends while the stream is opening (the stream is reset first). `options` go to the
   * transport as they are.
   */
  openUniStream(trackAlias: number, options?: MoqtSendStreamOptions): Promise<WritableStream<Uint8Array>>;
  /**
   * Orderly stop: end every live subscription (PUBLISH_DONE `TRACK_ENDED`, then FIN) and retract the announce
   * (NAMESPACE_DONE), give those writes a bounded window to reach the wire (closing a WebTransport discards queued
   * data), then give the peer a bounded window to close each ended subscription's request stream, which it does once it
   * has consumed the PUBLISH_DONE, and close the transport. Synchronous callers return immediately; the transport closes
   * within `CLOSE_FLUSH_TIMEOUT_MS` (250 ms) plus `CLOSE_END_ACK_TIMEOUT_MS` (2 s), and sooner against a peer that
   * answers promptly.
   */
  close(closeCode?: number, reason?: string): void;
  /** Drain outgoing streams, preserving an optional local closure error. */
  destroy(error?: unknown): void;
}

const DEFAULT_IMPLEMENTATION_NAME = '@caprid/moq-core publisher';

/**
 * How long `close()` waits for the PUBLISH_DONE + FIN / NAMESPACE_DONE writes to drain before closing the transport out
 * from under them.
 */
export const CLOSE_FLUSH_TIMEOUT_MS = 250;

/**
 * How long `close()` then waits for the peer to close each ended subscription's request stream. A write reaching the
 * wire does not mean the peer has read it: moq-relay reads PUBLISH_DONE, waits for Stream Count data streams (or a 1 s
 * grace when it cannot count them), finishes the track and only then FINs its side. A transport closed before that
 * aborts the track, which viewers see as PUBLISH_DONE status 0 `internal error`. The bound covers the relay's grace
 * plus a round trip, so a peer that never closes its side costs at most this much.
 */
export const CLOSE_END_ACK_TIMEOUT_MS = 2000;

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
  /** Data streams opened for this subscription: PUBLISH_DONE's Stream Count (§9.9). */
  streamCount: number;
  /** Data streams opened for this subscription that the caller has not closed yet; each entry resets its stream. */
  dataStreams: Set<() => void>;
  /** Data-stream opens in flight; PUBLISH_DONE waits for them to settle (§9.9). */
  opening: Set<Promise<void>>;
  /**
   * Settles once the peer's direction of the request stream ends (FIN or reset) or the session tears it down.
   * `close()` waits on it, bounded, before closing the transport: the peer ends its side only after it has consumed our
   * PUBLISH_DONE.
   */
  requestEnded: Promise<void>;
}

/**
 * The reason handed to a data stream the session resets when its subscription ends (§12.5 CANCELLED): a
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
      // A transport failure (STOP_SENDING, a reset) errors this side too, so a FIN the caller has not handed over yet
      // sees the failure instead of a close that only fails later.
      inner.closed.catch((error: unknown) => controller.error(error));
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
  /** The largest object location `objectPublished` recorded: TRACK_STATUS_OK's LARGEST_OBJECT. */
  largestObject: Location | undefined;
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

const PENDING = Symbol('pending');

/**
 * Whether `promise` had already settled when this was called. One microtask turn, and independent of which handlers
 * run first: a settled promise's reaction is queued ahead of the already-resolved marker's.
 */
async function hasSettled(promise: Promise<unknown>): Promise<boolean> {
  try {
    return (await Promise.race([promise, Promise.resolve(PENDING)])) !== PENDING;
  } catch {
    return true;
  }
}

function trackKey(namespace: TrackNamespace, name: string): string {
  return `${namespace.join('/')}--${name}`;
}

function isNamespacePrefix(prefix: TrackNamespace, namespace: TrackNamespace): boolean {
  return prefix.length <= namespace.length && prefix.every((part, i) => namespace[i] === part);
}

/** Answer a request with REQUEST_ERROR and close it both ways. */
async function refuse(
  writer: WritableStreamDefaultWriter<Uint8Array>,
  reader: StreamReader,
  code: number,
  reason: string
): Promise<void> {
  try {
    await writer.write(encodeRequestError(code, reason));
    await writer.close();
  } catch {
    // Peer cancelled.
  }

  await reader.cancel();
}

// =============================================================================
// Session driver
// =============================================================================

class MoqtPublishSessionImpl implements MoqtPublishSession {
  readonly ready: Promise<void>;

  #transport: MoqtTransport;
  /** The negotiated draft, which selects the parameter codec. */
  #protocol: MoqtProtocolId;
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
   * Every inbound request ID ever seen. Reuse closes the session with INVALID_REQUEST_ID (§6.4.2.1), and because aliases ARE the peer's
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
    this.#protocol = resolveMoqtProtocol(transport.protocol);
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
      // (§6.3) — a peer stopping it has ended the session, so fail now
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

    // The writes are on the wire, not yet read: a peer that has not consumed a PUBLISH_DONE when the transport closes
    // aborts the track instead of finishing it (moq-relay 0.15.4 and later report that downstream as status 0). The
    // peer closes its side of the request stream once it has, so wait for that on every subscription still being
    // served, bounded for a peer that never does.
    const requestEnds = [...this.#tracks.values()].flatMap((track) => track.subscribers.map((s) => s.requestEnded));

    if (requestEnds.length > 0) {
      let endTimer: ReturnType<typeof setTimeout> | undefined;

      try {
        await Promise.race([
          Promise.all(requestEnds),
          new Promise<void>((resolve) => {
            endTimer = setTimeout(resolve, CLOSE_END_ACK_TIMEOUT_MS);
          }),
        ]);
      } finally {
        if (endTimer !== undefined) clearTimeout(endTimer);
      }
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
      .catch((error: unknown) => {
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
        void this.#reportAnnounceLoss(error);
      });
  }

  /**
   * Fire `onAnnounceEnded` when no solicitation carries the announce anymore — shared by the read-loop cleanup and the
   * response-write failure path; `cause` is the stream failure that ended the carrier. Silent while closing,
   * destroyed, or migrating (a GOAWAY makes the peer's streams ending the orderly drain), and once the transport has
   * ended: an ended transport fails every stream as it settles `closed`, and that is connection loss, which
   * `#handleClosed` reports, not a withdrawal. A failure with `source` `'session'` (TA-23) or the settled `closed`
   * decides, not whether the `closed` handler happened to run first.
   */
  async #reportAnnounceLoss(cause: unknown): Promise<void> {
    if (this.#announceLossSilenced() || this.#announceCarried()) return;
    if (isSessionError(cause) || (await hasSettled(this.#transport.closed))) return;
    // A replacement solicitation may have picked the announce up during that turn.
    if (this.#announceLossSilenced() || this.#announceCarried()) return;

    const error = new MoqtProtocolError('the namespace solicitation carrying the announce ended');
    error.cause = cause;
    this.#callbacks.onAnnounceEnded?.({ error });
  }

  #announceLossSilenced(): boolean {
    return this.#destroyed || this.#closing || this.#receivedGoaway;
  }

  #announceCarried(): boolean {
    return [...this.#namespaceStreams].some((stream) => stream.announced.length > 0);
  }

  registerTrack(options: RegisterTrackOptions): RegisteredTrack {
    if (this.#destroyed || this.#closing) {
      // A closed session must not repopulate `#tracks` — hand back an
      // inert handle so late callers still get the RegisteredTrack shape.
      return { trackName: options.trackName, end: () => {}, objectPublished: () => {} };
    }

    const track: TrackRecord = {
      trackProperties: options.trackProperties ?? [],
      largestObject: undefined,
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
      objectPublished: (location) => {
        if (track.largestObject === undefined || compareLocations(location, track.largestObject) > 0) {
          track.largestObject = { group: location.group, object: location.object };
        }
      },
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
   * End an Established subscription from the publisher side (§6.4.2.2, §9.9). Marks it finished at once, so no new
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

  async openUniStream(trackAlias: number, options?: MoqtSendStreamOptions): Promise<WritableStream<Uint8Array>> {
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
      const stream = await this.#transport.createUnidirectionalStream(options);

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

    return decodeControlMessage({ type, body }, this.#protocol);
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

    // A closed control stream ends the session (§6.3).
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

    if (message.kind === 'track-status') {
      await this.#handleTrackStatus(stream, reader, message);
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
   * Request IDs are consumed once per session (§6.4.2.1) — by every request-initiating message and, since draft-20 gave it
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
   * clean requester-side FIN is half-closure, not withdrawal (§6.4.2.2); only a reset (or a rejected update, §9.5.1)
   * ends the carrier, and its loss while the announce is live means the peer refused or withdrew the ingest path.
   */
  async #handleSubscribeNamespace(
    stream: BidirectionalStreamLike,
    reader: StreamReader,
    subscribeNamespace: Extract<ControlMessage, { kind: 'subscribe-namespace' }>
  ): Promise<void> {
    const writer = stream.writable.getWriter();
    const prefix = subscribeNamespace.trackNamespacePrefix;

    // §9.17: a solicitation overlapping an established one (either
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
      await writer.write(encodeRequestOk({}, this.#protocol));
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
    let cause: unknown;

    try {
      while (!(await reader.atEnd())) {
        const type = await reader.readVarint();
        const message = await this.#readControlFrame(reader, type);

        if (message.kind === 'goaway') {
          this.#receivedGoaway = true;
          this.#callbacks.onGoaway?.(message);
        } else if (message.kind === 'request-update') {
          this.#claimPeerRequestId(message.requestId);
          // Namespace subscriptions may legally be updated (§9.5). v1
          // applies none of it (a prefix change would re-base every
          // entry), so per §9.5.1 the update is answered with an error
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
        // A clean requester-side FIN is NOT a cancellation (§6.4.2.2 — the
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
      cause = error;
    }

    // Withdrawal (reset) or a failed update: the carrier is over.
    this.#namespaceStreams.delete(record);
    void writer.close().catch(() => {});

    if (record.announced.length > 0) await this.#reportAnnounceLoss(cause);
  }

  /**
   * TRACK_STATUS (draft-22 §9.13, the same on draft-20): answered as the SUBSCRIBE for the same track would be, without
   * creating subscription state or sending objects. A registered, live track gets TRACK_STATUS_OK (a REQUEST_OK) with
   * LARGEST_OBJECT once an object has been published and the track properties SUBSCRIBE_OK carries; any other track
   * gets the REQUEST_ERROR a SUBSCRIBE would. Either way the response direction FINs and nothing more is read: the
   * request takes no REQUEST_UPDATE and gets no PUBLISH_DONE. No callback fires and the binding is untouched.
   *
   * moq-relay from moq-dev #5991 asks a publisher for a track's status before subscribing on behalf of a relay that
   * has no subscription, and fails the downstream request on a refusal, so answering it is what lets a track cross two
   * relays there.
   */
  async #handleTrackStatus(
    stream: BidirectionalStreamLike,
    reader: StreamReader,
    request: Extract<ControlMessage, { kind: 'track-status' }>
  ): Promise<void> {
    const track = this.#tracks.get(trackKey(request.trackNamespace, request.trackName));
    const writer = stream.writable.getWriter();

    if (!track || track.done) {
      await refuse(writer, reader, REQUEST_ERROR_CODE.DOES_NOT_EXIST, 'unknown track');
      return;
    }

    let trackStatusOk: Uint8Array;
    try {
      trackStatusOk = encodeRequestOk(
        track.largestObject === undefined ? {} : { largestObject: track.largestObject },
        this.#protocol,
        track.trackProperties
      );
    } catch {
      // The answer overflows its frame (properties at the limit and a long LARGEST_OBJECT); the peer did nothing wrong.
      await refuse(writer, reader, REQUEST_ERROR_CODE.INTERNAL_ERROR, 'could not encode TRACK_STATUS_OK');
      return;
    }

    try {
      await writer.write(trackStatusOk);
      await writer.close();
    } catch {
      // Peer cancelled; nothing to clean up.
    }

    try {
      await reader.cancel();
    } catch {
      // The stream already errored (the peer reset it first).
    }
  }

  async #handleIncomingSubscribe(
    stream: BidirectionalStreamLike,
    reader: StreamReader,
    subscribe: Extract<ControlMessage, { kind: 'subscribe' }>
  ): Promise<void> {
    const track = this.#tracks.get(trackKey(subscribe.trackNamespace, subscribe.trackName));
    const writer = stream.writable.getWriter();

    if (!track || track.done) {
      await refuse(writer, reader, REQUEST_ERROR_CODE.DOES_NOT_EXIST, 'unknown track');
      return;
    }

    // The peer's request IDs are session-unique, so they double as the
    // track alias — binding one alias to two live request IDs is a
    // session-fatal Duplicate on moq-lite-rs.
    const trackAlias = subscribe.requestId;

    // Encoded before the write's guard: only a failed write is the peer
    // cancelling. A response this side cannot build (the properties
    // overflow the message, say) is refused promptly, so the peer is not
    // left waiting on a request that never gets an answer.
    let subscribeOk: Uint8Array;
    try {
      subscribeOk = encodeSubscribeOk(trackAlias, {}, track.trackProperties, this.#protocol);
    } catch (error) {
      // The encoder refused the response; the peer did nothing wrong.
      await refuse(writer, reader, REQUEST_ERROR_CODE.INTERNAL_ERROR, 'could not encode SUBSCRIBE_OK');

      if (!this.#destroyed) {
        this.#callbacks.onSubscribeFailed?.({ requestId: subscribe.requestId, trackName: track.trackName, error });
      }
      return;
    }
    let settleRequest!: () => void;
    const requestEnded = new Promise<void>((resolve) => {
      settleRequest = resolve;
    });
    const subscriber: SubscriberStream = {
      requestId: subscribe.requestId,
      trackAlias,
      writer,
      cancel: async () => {
        subscriber.finished = true;
        settleRequest();
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
      requestEnded,
    };

    track.subscribers.push(subscriber);

    try {
      await writer.write(subscribeOk);
      subscriber.accepted = true;
    } catch {
      // Peer cancelled before the response landed.
      track.subscribers = track.subscribers.filter((s) => s !== subscriber);
      settleRequest();
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
          // The delivery timeouts (§5.2) are applied by the caller, through `onRequestUpdate`, to the groups it opens
          // afterwards.
          const {
            forward,
            subscriberPriority: _priority,
            objectDeliveryTimeout: _objectTimeout,
            subgroupDeliveryTimeout: _subgroupTimeout,
            ...unsupported
          } = message.parameters;

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
              // believes is in effect). Forward State is applied above,
              // priority is advisory (the known relays discard ours
              // too), and the caller applies the delivery timeouts;
              // anything else ends this request honestly: the
              // update's REQUEST_ERROR, then PUBLISH_DONE UPDATE_FAILED
              // (§9.5.1).
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
              // never sends REQUEST_UPDATE (moq-relay 0.15.8 writes nothing
              // after SUBSCRIBE until it cancels) never sees a trailing byte.
              void writer.write(encodeRequestOk({}, this.#protocol)).catch(() => {});
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
      // to end. moq-relay (0.15.8 `cancel_subscribe`) pairs that FIN with
      // STOP_SENDING (a cancellation, so the write fails harmlessly); a
      // peer that only half-closes still gets the PUBLISH_DONE §6.4.2.2
      // requires before our FIN. No status names this, so it reports the
      // publisher's own limitation.
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
      // Reset — the subscriber cancelled (§6.4.2.3); ordinary end-of-request.
      // A cancelled subscription gets no PUBLISH_DONE, and its open data
      // streams are reset (§3.1.2).
      resetDataStreams(subscriber);
    } finally {
      const alreadyEnding = subscriber.finished;

      subscriber.finished = true;
      track.subscribers = track.subscribers.filter((s) => s !== subscriber);
      settleRequest();

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
