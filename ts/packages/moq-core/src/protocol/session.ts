/**
 * Pure MOQT session protocol driver (moq-transport draft-20), subscribe side only.
 *
 * Owns the protocol mechanics over an established transport: the SETUP exchange on paired unidirectional control
 * streams (§3.3), request-stream bookkeeping keyed by Request ID (§10.1 — client IDs are even, starting at 0), routing
 * of incoming data streams to subscriptions (by Track Alias) and fetches (by Request ID), GOAWAY handling, and session
 * termination.
 *
 * Deliberately callback-shaped with NO signals (like `onMediaSourceReadyStateChange`) — signal awareness enters at the
 * `playback/` layer, where the moq-session actor binds these callbacks to SPF state. The transport is a structural
 * subset of `WebTransport`, so tests drive the driver with an in-memory fake and the DOM layer passes a real
 * `WebTransport` unchanged.
 *
 * Version negotiation happens at connection time (ALPN / `WT-Available-Protocols`, `MOQT_PROTOCOL_ID`), before this
 * driver sees the transport.
 *
 * Datagram reception is not implemented: MSF maps every object to its own stream, so a subscribe-only MSF engine never
 * legitimately receives OBJECT_DATAGRAMs.
 */
import { StreamReader, utf8Encode } from './bytes.js';
import {
  type ControlMessage,
  decodeControlMessage,
  encodeFetch,
  encodeRequestError,
  encodeRequestOk,
  encodeRequestUpdate,
  encodeSetup,
  encodeSubscribe,
  encodeSubscribeNamespace,
  encodeTrackStatus,
  type FetchRequest,
  type KeyValuePair,
  MESSAGE_TYPE,
  type MessageParameters,
  REQUEST_ERROR_CODE,
  type Redirect,
  SETUP_OPTION,
  type TrackNamespace,
} from './control-messages.js';
import { isMoqtProtocolError, MoqtProtocolError, SESSION_ERROR } from './errors.js';
import {
  type FetchStreamEntry,
  isSubgroupHeaderType,
  type MoqtObject,
  readFetchEntries,
  readFetchHeader,
  readSubgroupHeader,
  readSubgroupObjects,
  STREAM_TYPE,
  type SubgroupHeader,
} from './object-stream.js';
import { type BidirectionalStreamLike, openRequestStream, type RequestStream } from './request-stream.js';

// ============================================================================
// Transport seam
// ============================================================================

/**
 * Structural subset of `WebTransport` the session driver needs. A real `WebTransport` instance satisfies this; tests
 * provide an in-memory fake (the same seam pattern as SPF's fetch injection).
 */
export interface MoqtTransport {
  /**
   * The protocol the peer selected from those offered: the negotiated ALPN on native QUIC, the `WT-Protocol` response
   * header on a WebTransport session. Read once the transport is ready (TA-8).
   */
  readonly protocol: string;
  /** Unidirectional streams the peer opens: its control stream and data streams. */
  readonly incomingUnidirectionalStreams: ReadableStream<ReadableStream<Uint8Array>>;
  /** Bidirectional streams the peer opens: its request streams. */
  readonly incomingBidirectionalStreams: ReadableStream<BidirectionalStreamLike>;
  /** Open a unidirectional stream to the peer. */
  createUnidirectionalStream(): Promise<WritableStream<Uint8Array>>;
  /** Open a bidirectional stream to the peer. */
  createBidirectionalStream(): Promise<BidirectionalStreamLike>;
  /** Close the transport session with an optional application close code and reason. */
  close(closeInfo?: { closeCode?: number; reason?: string }): void;
  /** Settles when the transport session has closed. */
  readonly closed: Promise<unknown>;
}

// ============================================================================
// Public session surface
// ============================================================================

/**
 * A failed request: a REQUEST_ERROR from the peer, or a failure the session synthesized from a stream event (see
 * {@link isStreamFailure}).
 */
export interface RequestError {
  /** A {@link @mux/moq-core!REQUEST_ERROR_CODE} value; the session uses `INTERNAL_ERROR` or `TIMEOUT` for failures it synthesizes. */
  errorCode: number;
  /** REQUEST_ERROR's Retry Interval as received; `0` for failures the session synthesizes. */
  retryInterval: number;
  /** Reason phrase; empty when the peer gave none. */
  reason: string;
  /** Where to re-issue the request, present when `errorCode` is `REDIRECT`. */
  redirect?: Redirect;
}

/** SUBSCRIBE_OK: the peer accepted a subscription. */
export interface SubscribeOk {
  /** Track Alias the publisher assigned; incoming subgroup streams name the subscription by it. */
  trackAlias: number;
  /** Message parameters SUBSCRIBE_OK carried. */
  parameters: MessageParameters;
  /** Track Properties SUBSCRIBE_OK carried. */
  trackProperties: KeyValuePair[];
}

/** PUBLISH_DONE: the publisher ended a subscription. */
export interface PublishDone {
  /** A {@link @mux/moq-core!PUBLISH_DONE_STATUS} value. */
  statusCode: number;
  /** Data streams the publisher opened; `undefined` when it could not count them (§10.12). */
  streamCount: number | undefined;
  /** Reason phrase; empty when the publisher gave none. */
  reason: string;
}

/** Callbacks for {@link MoqtSession.subscribe}. Each must be synchronous; an async function throws `TypeError`. */
export interface SubscriptionHandlers {
  /** SUBSCRIBE_OK arrived. */
  onOk?(ok: SubscribeOk): void;
  /** The subscription failed before or instead of SUBSCRIBE_OK: REQUEST_ERROR, or a stream failure. */
  onError?(error: RequestError): void;
  /** An object arrived on one of the subscription's subgroup streams. */
  onObject?(object: MoqtObject): void;
  /** A subgroup stream finished cleanly (FIN). */
  onSubgroupEnd?(info: { groupId: number; subgroupId: number; endOfGroup: boolean }): void;
  /**
   * A subgroup stream was reset — objects from it may be missing. Reported for every reset the session receives; a
   * relay that drops a reset instead of forwarding it leaves nothing to report.
   */
  onSubgroupReset?(info: { groupId: number; error: unknown }): void;
  /** PUBLISH_DONE arrived. Subgroup streams already in flight still deliver until the subscription is cancelled. */
  onDone?(done: PublishDone): void;
  /** Response to a REQUEST_UPDATE sent on this subscription. */
  onUpdateOk?(): void;
  /**
   * PUBLISH_STATE_NOTIFY (§10.10): the publisher changed subscription state on its own — `parameters` carries only what
   * changed, with LARGEST_OBJECT marking where. Informational; the session sends no reply.
   */
  onStateNotify?(parameters: MessageParameters): void;
  /** GOAWAY on this request stream: re-issue the request (possibly elsewhere). */
  onGoaway?(goaway: Goaway): void;
}

/** Validate once; returned promises are diagnosed and observed, never awaited by delivery. */
function synchronousSubscriptionHandlers(handlers: SubscriptionHandlers): SubscriptionHandlers {
  const result: SubscriptionHandlers = {};
  const names = [
    'onOk',
    'onError',
    'onObject',
    'onSubgroupEnd',
    'onSubgroupReset',
    'onDone',
    'onUpdateOk',
    'onStateNotify',
    'onGoaway',
  ] as const;
  for (const name of names) {
    const callback = handlers[name];
    if (!callback) continue;
    if (Object.prototype.toString.call(callback) === '[object AsyncFunction]') {
      throw new TypeError(`${name} must be synchronous; use Relay subscribeEvents() for async consumption`);
    }
    let reportedAsyncResult = false;
    Object.assign(result, {
      [name]: (...args: unknown[]) => {
        const returned: unknown = Reflect.apply(callback, handlers, args);
        if (
          returned &&
          (typeof returned === 'object' || typeof returned === 'function') &&
          'then' in returned &&
          typeof returned.then === 'function'
        ) {
          // JS promises cannot be cancelled. Observe failures without serializing delivery.
          void Promise.resolve(returned).catch(() => {});
          if (!reportedAsyncResult) {
            reportedAsyncResult = true;
            // biome-ignore lint/suspicious/noConsole: public diagnostic for unsupported async callbacks
            console.error(
              new TypeError(`${name} returned a promise; callbacks must be synchronous; use Relay subscribeEvents()`)
            );
          }
        }
      },
    });
  }
  return result;
}

/** A live subscription returned by {@link MoqtSession.subscribe}. */
export interface Subscription {
  /** Request ID of the SUBSCRIBE. */
  readonly requestId: number;
  /** Modify the subscription (REQUEST_UPDATE, §10.9). */
  update(parameters: MessageParameters): void;
  /** Tear the subscription down. MOQT has no UNSUBSCRIBE message — teardown is the request stream's lifecycle (§3.3.3). */
  cancel(reason?: unknown): void;
}

/** FETCH_OK: the peer accepted a fetch. */
export interface FetchOk {
  /** FETCH_OK's End Of Track flag. */
  endOfTrack: boolean;
  /** FETCH_OK's End Location. */
  endLocation: {
    /** Group ID. */
    group: number;
    /** Object ID within the group. */
    object: number;
  };
  /** Message parameters FETCH_OK carried. */
  parameters: MessageParameters;
  /** Track Properties FETCH_OK carried. */
  trackProperties: KeyValuePair[];
}

/** Callbacks for {@link MoqtSession.fetch}. */
export interface FetchHandlers {
  /** FETCH_OK arrived. Not called when the peer answers with a bare REQUEST_OK instead. */
  onOk?(ok: FetchOk): void;
  /** The fetch failed: REQUEST_ERROR, or a stream failure. */
  onError?(error: RequestError): void;
  /** An entry arrived on the fetch data stream. */
  onEntry?(entry: FetchStreamEntry): void;
  /** The fetch data stream finished cleanly (FIN). */
  onEnd?(): void;
  /** The fetch data stream was reset — delivered entries may be incomplete. */
  onReset?(info: { error: unknown }): void;
  /** GOAWAY on this request stream: re-issue the request (possibly elsewhere). */
  onGoaway?(goaway: Goaway): void;
}

/** A fetch in progress, returned by {@link MoqtSession.fetch}. */
export interface FetchHandle {
  /** Request ID of the FETCH. */
  readonly requestId: number;
  /** Stop the fetch: cancel its request stream and deliver no further entries. */
  cancel(reason?: unknown): void;
}

/** Callbacks for {@link MoqtSession.trackStatus}. */
export interface TrackStatusHandlers {
  /** REQUEST_OK arrived with the track's current parameters and Track Properties. */
  onOk?(status: { parameters: MessageParameters; trackProperties: KeyValuePair[] }): void;
  /** The request failed: REQUEST_ERROR, or a stream failure. */
  onError?(error: RequestError): void;
}

/** The prefix and parameters for a SUBSCRIBE_NAMESPACE. */
export interface MoqtSubscribeNamespaceOptions {
  /** Namespace prefix to discover; NAMESPACE entries name namespaces relative to it. */
  trackNamespacePrefix: TrackNamespace;
  /** Message parameters to send with the request. */
  parameters?: MessageParameters | undefined;
}

/**
 * Callbacks for {@link MoqtSession.subscribeNamespace}. NAMESPACE and NAMESPACE_DONE entries name a namespace by its
 * suffix relative to the requested prefix; an empty suffix is the prefix itself.
 */
export interface NamespaceSubscriptionHandlers {
  /** REQUEST_OK arrived: the peer accepted the request, and entries follow on the same stream. */
  onOk?(): void;
  /** The request failed before or instead of REQUEST_OK: REQUEST_ERROR, or a stream failure. */
  onError?(error: RequestError): void;
  /** NAMESPACE (§10.19): a namespace under the prefix is available. */
  onNamespace?(trackNamespaceSuffix: TrackNamespace): void;
  /** NAMESPACE_DONE (§10.20): a namespace the peer named is no longer available. */
  onNamespaceDone?(trackNamespaceSuffix: TrackNamespace): void;
  /**
   * The accepted request ended: the peer finished its side of the stream (no `error`), or the stream failed (`error`
   * is the transport's). No entry follows. Not called for a local cancel or the session ending.
   */
  onEnd?(info: { error?: unknown }): void;
  /** GOAWAY on this request stream: re-issue the request (possibly elsewhere). */
  onGoaway?(goaway: Goaway): void;
}

/** A namespace subscription returned by {@link MoqtSession.subscribeNamespace}. */
export interface NamespaceSubscription {
  /** Request ID of the SUBSCRIBE_NAMESPACE. */
  readonly requestId: number;
  /** Tear the request down; the peer withdraws nothing it already sent. */
  cancel(reason?: unknown): void;
}

/** GOAWAY: the peer asks the client to stop making requests on this session and move to a new one. */
export interface Goaway {
  /** New Session URI; empty when the peer names none. */
  newSessionUri: string;
  /** GOAWAY's Timeout field as received. */
  timeout: number;
}

/** A server-initiated PUBLISH, passed to `onIncomingPublish`. */
export interface IncomingPublish {
  /** Request ID of the PUBLISH. */
  requestId: number;
  /** Track namespace being published. */
  trackNamespace: TrackNamespace;
  /** Track name being published. */
  trackName: string;
  /** Track Alias the publisher assigned. */
  trackAlias: number;
  /** Message parameters the PUBLISH carried. */
  parameters: MessageParameters;
}

/** Session-level event callbacks for {@link createMoqtSession}. All are optional. */
export interface MoqtSessionCallbacks {
  /** The server's SETUP arrived; the session is fully established. */
  onReady?(serverOptions: KeyValuePair[]): void;
  /** GOAWAY on the control stream: stop initiating requests, migrate. */
  onGoaway?(goaway: Goaway): void;
  /**
   * A server-initiated PUBLISH arrived. Call exactly one of the responders. Absent, the session rejects with
   * UNINTERESTED (a subscribe-only client).
   *
   * `accept` sends a bare PUBLISH_OK: since draft-20 the subscription parameters (FORWARD, LOCATION_FILTER, …) are
   * legal only on PUBLISH and REQUEST_UPDATE, and a parameter in the wrong message is a PROTOCOL_VIOLATION (§10.2.1).
   */
  onIncomingPublish?(
    publish: IncomingPublish,
    respond: { accept(): void; reject(errorCode?: number, reason?: string): void }
  ): void;
  /** The session ended — transport closed, or a fatal protocol error. */
  onClosed?(info: { error?: unknown }): void;
}

/** Options for {@link createMoqtSession}. */
export interface MoqtSessionConfig {
  /** Extra Setup Options to send (an implementation identifier is added automatically). */
  setupOptions?: KeyValuePair[];
  /** Identifies this implementation in SETUP (§10.3.1.5). */
  implementationName?: string;
  /**
   * How long to hold an incoming data stream whose Track Alias has no registered subscription yet (objects can outrace
   * SUBSCRIBE_OK). §11.4.2 allows brief buffering; expired streams are dropped. Default 2000ms.
   */
  unknownAliasTimeoutMs?: number;
  /**
   * How long to wait for a request's first response (SUBSCRIBE_OK / FETCH_OK / REQUEST_OK / REQUEST_ERROR) before
   * failing it. The draft expects implementations to bound control exchanges (§3.5's CONTROL_MESSAGE_TIMEOUT); without
   * this a relay that accepts the stream and then goes quiet leaves the request pending forever. Default 10000ms.
   */
  requestTimeoutMs?: number | undefined;
  /** Session-level event callbacks. */
  callbacks?: MoqtSessionCallbacks | undefined;
}

/** The track and parameters for a SUBSCRIBE (also used by TRACK_STATUS). */
export interface MoqtSubscribeOptions {
  /** Track namespace to request. */
  trackNamespace: TrackNamespace;
  /** Track name to request. */
  trackName: string;
  /** Message parameters to send with the request. */
  parameters?: MessageParameters | undefined;
}

/**
 * FETCH (§10.13) takes the SUBSCRIBE shape; the range is `parameters.locationFilter`, read with fetch rules (§5.1.2): a
 * relative start counts back from Largest Object, an omitted end stops there, no filter means `{0,0}` up to Largest
 * Object. A subscription reads the same filter with subscription rules; see `LocationFilter` in control-messages.ts.
 */
export type MoqtFetchOptions = MoqtSubscribeOptions;

/** A subscribe-side MOQT session over an established transport. Created by {@link createMoqtSession}. */
export interface MoqtSession {
  /** Resolves when the server's SETUP has been received. */
  readonly ready: Promise<void>;
  /** Send SUBSCRIBE (§10.7) on a new request stream. Results arrive through `handlers`. */
  subscribe(options: MoqtSubscribeOptions, handlers?: SubscriptionHandlers): Subscription;
  /** Send FETCH (§10.13) on a new request stream. Results arrive through `handlers`. */
  fetch(options: MoqtFetchOptions, handlers?: FetchHandlers): FetchHandle;
  /** Send TRACK_STATUS (§10.15) on a new request stream and report its one response through `handlers`. */
  trackStatus(options: MoqtSubscribeOptions, handlers?: TrackStatusHandlers): void;
  /**
   * Send SUBSCRIBE_NAMESPACE (§10.18) on a new request stream: the peer answers REQUEST_OK, then names the namespaces
   * under the prefix with NAMESPACE / NAMESPACE_DONE on the same stream. A relay that pushes unsolicited
   * PUBLISH_NAMESPACE instead only stops doing so when SETUP carried `SETUP_OPTION.SOLICIT`.
   */
  subscribeNamespace(
    options: MoqtSubscribeNamespaceOptions,
    handlers?: NamespaceSubscriptionHandlers
  ): NamespaceSubscription;
  /** Close the transport session. Sends no GOAWAY (spec/behavior.md, "Relay interop deviations"). */
  close(closeCode?: number, reason?: string): void;
  /** End locally; an error also fails requests still awaiting their first response. */
  destroy(error?: unknown): void;
}

const DEFAULT_UNKNOWN_ALIAS_TIMEOUT_MS = 2000;
const DEFAULT_REQUEST_TIMEOUT_MS = 10_000;
const DEFAULT_IMPLEMENTATION_NAME = '@mux/moq-core';

/**
 * A request failure that arrives as a _stream_ event — FIN before any response, a transport reset, or the response
 * timeout — rather than as a REQUEST_ERROR message. Shaped as a `RequestError` so consumers have one failure path
 * regardless of how the request died.
 */
function streamFailure(reason: string, errorCode: number = REQUEST_ERROR_CODE.INTERNAL_ERROR): RequestError {
  const failure = { errorCode, retryInterval: 0, reason };

  streamFailures.add(failure);
  return failure;
}

const streamFailures = new WeakSet<RequestError>();

/**
 * Whether the session synthesized this request failure from a stream event (FIN before any response, a stream reset,
 * a failed stream open, the response timeout, or the session ending) rather than receiving a REQUEST_ERROR message.
 * A real transport can fail a request's stream some tasks before it reports its own closure, so a consumer that wants
 * to tell a lost connection from a rejection waits for `closed` before trusting such a failure.
 */
export function isStreamFailure(error: RequestError): boolean {
  return streamFailures.has(error);
}

// ============================================================================
// Internal records
// ============================================================================

/** Bookkeeping `#openRequest` manages for every request kind. */
interface RequestRecordBase {
  cancelled: boolean;
  pendingCancelReason?: unknown;
  stream?: RequestStream;
  /**
   * The request's first response (an OK or a REQUEST_ERROR) arrived. Until it does, a FIN / reset / timeout on the
   * stream is a failure; after it, those are ordinary end-of-request events.
   */
  settled: boolean;
  responseTimer?: ReturnType<typeof setTimeout> | undefined;
}

interface SubscriptionRecord extends RequestRecordBase {
  readers: Set<StreamReader>;
  requestId: number;
  handlers: SubscriptionHandlers;
  trackAlias?: number;
  /** PUBLISH_DONE arrived, so the publisher's FIN that follows completes the request. */
  publisherDone?: boolean;
}

interface FetchRecord extends RequestRecordBase {
  requestId: number;
  handlers: FetchHandlers;
  groupOrder: 'ascending' | 'descending';
}

interface TrackStatusRecord extends RequestRecordBase {
  requestId: number;
  handlers: TrackStatusHandlers;
}

interface NamespaceSubscriptionRecord extends RequestRecordBase {
  requestId: number;
  handlers: NamespaceSubscriptionHandlers;
  /** REQUEST_OK arrived. Settlement alone is not acceptance: REQUEST_ERROR settles the request too. */
  accepted?: boolean;
}

interface AliasWaiter {
  resolve(record: SubscriptionRecord | undefined): void;
  timer: ReturnType<typeof setTimeout>;
}

// ============================================================================
// Session driver
// ============================================================================

class MoqtSessionImpl implements MoqtSession {
  readonly ready: Promise<void>;

  #transport: MoqtTransport;
  #config: MoqtSessionConfig;
  #callbacks: MoqtSessionCallbacks;

  #nextRequestId = 0; // client request IDs: even, starting at 0 (§10.1)
  /** Every inbound request ID ever seen. Reuse, or the client's (even) parity, closes the session (§10.1). */
  #peerRequestIds = new Set<number>();
  #subscriptions = new Map<number, SubscriptionRecord>();
  #fetches = new Map<number, FetchRecord>();
  #trackStatuses = new Map<number, TrackStatusRecord>();
  #namespaceSubscriptions = new Map<number, NamespaceSubscriptionRecord>();
  #aliasRoutes = new Map<number, SubscriptionRecord>();
  #aliasWaiters = new Map<number, AliasWaiter[]>();

  #resolveReady!: () => void;
  #rejectReady!: (error: unknown) => void;
  #controlWriter: WritableStreamDefaultWriter<Uint8Array> | undefined;
  #receivedServerSetup = false;
  #receivedControlGoaway = false;
  #destroyed = false;

  constructor(transport: MoqtTransport, config: MoqtSessionConfig = {}) {
    this.#transport = transport;
    this.#config = config;
    this.#callbacks = config.callbacks ?? {};
    this.ready = new Promise((resolve, reject) => {
      this.#resolveReady = resolve;
      this.#rejectReady = reject;
    });
    // Consumers may observe readiness through `onClosed` alone.
    this.ready.catch(() => {});

    void this.#start();
  }

  // --------------------------------------------------------------------------
  // Session lifecycle
  // --------------------------------------------------------------------------

  async #start(): Promise<void> {
    // Accept loops start first: the peer's streams must be processed even
    // while our own SETUP write is still propagating (either peer may send
    // stream data first, §3.3).
    void this.#acceptUnidirectionalStreams();
    void this.#acceptBidirectionalStreams();
    void this.#transport.closed.then(
      () => this.#handleClosed(undefined),
      (error) => this.#handleClosed(error)
    );

    try {
      const control = await this.#transport.createUnidirectionalStream();

      this.#controlWriter = control.getWriter();
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
    if (this.#destroyed) return;

    const closeCode = isMoqtProtocolError(error) ? error.code : SESSION_ERROR.INTERNAL_ERROR;

    try {
      this.#transport.close({ closeCode, reason: error instanceof Error ? error.message : 'session error' });
    } catch {
      // transport already gone
    }

    this.#handleClosed(error);
  }

  #handleClosed(error: unknown, { expected = false }: { expected?: boolean } = {}): void {
    if (this.#destroyed) return;

    this.#destroyed = true;
    const setupError = this.#receivedServerSetup
      ? undefined
      : (error ?? new MoqtProtocolError('session closed before server SETUP'));

    if (setupError !== undefined) this.#rejectReady(setupError);

    for (const waiters of this.#aliasWaiters.values()) {
      for (const waiter of waiters) {
        clearTimeout(waiter.timer);
        waiter.resolve(undefined);
      }
    }

    this.#aliasWaiters.clear();

    // Requests still awaiting their first response die with the session. An
    // unexpected close (transport drop) is a failure each of them has to
    // hear about — otherwise a dropped transport leaves subscribers pending
    // forever. A deliberate `close()` is ordinary teardown, and cancelled
    // requests already have their answer.
    const records = [
      ...this.#subscriptions.values(),
      ...this.#fetches.values(),
      ...this.#trackStatuses.values(),
      ...this.#namespaceSubscriptions.values(),
    ];
    const pending = expected ? [] : records.filter((r) => !r.settled && !r.cancelled);

    for (const record of records) {
      this.#settleRequest(record);
      record.stream?.cancel(error);
    }

    for (const record of this.#subscriptions.values()) this.#removeSubscription(record);
    this.#subscriptions.clear();
    this.#fetches.clear();
    this.#trackStatuses.clear();
    this.#namespaceSubscriptions.clear();
    this.#aliasRoutes.clear();
    this.#controlWriter = undefined;

    const failure = streamFailure(
      error instanceof Error ? error.message : 'session closed before the request was answered'
    );

    for (const record of pending) record.handlers.onError?.(failure);

    // A transport that drops before server SETUP is a session failure even
    // when the close itself was clean — callback-only consumers observing
    // `onClosed` alone must see the error too. A deliberate local `close()`
    // stays a clean close.
    const closeError = error ?? (expected ? undefined : setupError);

    this.#callbacks.onClosed?.(closeError === undefined ? {} : { error: closeError });
  }

  close(closeCode = SESSION_ERROR.NO_ERROR, reason = ''): void {
    this.#close(closeCode, reason);
  }

  #close(closeCode: number, reason: string, error?: unknown): void {
    if (this.#destroyed) return;

    // No GOAWAY: one written as the transport closes gives the peer nothing to act on, and the
    // session has no drain to offer. The publisher follows the same rule; see spec/behavior.md,
    // "Relay interop deviations".
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

  // --------------------------------------------------------------------------
  // Requests
  // --------------------------------------------------------------------------

  #allocateRequestId(): number {
    const id = this.#nextRequestId;

    this.#nextRequestId += 2;
    return id;
  }

  /**
   * Request IDs are consumed once per session (§10.1), by every request-initiating message, accepted or not. This
   * endpoint is always the client, so the peer's ids are odd; a reused id or an even one closes the session with
   * INVALID_REQUEST_ID.
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

  subscribe(options: MoqtSubscribeOptions, callbacks: SubscriptionHandlers = {}): Subscription {
    const handlers = synchronousSubscriptionHandlers(callbacks);
    const requestId = this.#allocateRequestId();
    const record: SubscriptionRecord = { requestId, handlers, cancelled: false, settled: false, readers: new Set() };

    this.#subscriptions.set(requestId, record);

    const message = encodeSubscribe({
      requestId,
      trackNamespace: options.trackNamespace,
      trackName: options.trackName,
      parameters: options.parameters,
    });

    void this.#openRequest(
      message,
      record,
      (msg) => this.#handleSubscriptionMessage(record, msg),
      (error) => {
        this.#removeSubscription(record);
        handlers.onError?.(error);
      },
      () => {
        // §3.3.2: once the publisher's FIN after PUBLISH_DONE completes the request, the requester
        // SHOULD FIN too. A publisher closing waits for it before closing the transport.
        if (record.publisherDone) record.stream?.finWrite().catch(() => {});
      }
    );

    return {
      requestId,
      update: (parameters) => {
        // REQUEST_UPDATE consumes a Request ID of its own (§10.1); the stream
        // it travels on is what names the subscription. Reusing the
        // subscription's id would be a duplicate the peer MUST close on.
        record.stream?.send(encodeRequestUpdate(this.#allocateRequestId(), parameters)).catch(() => {});
      },
      cancel: (reason) => this.#cancelSubscription(record, reason),
    };
  }

  fetch(options: MoqtFetchOptions, handlers: FetchHandlers = {}): FetchHandle {
    const requestId = this.#allocateRequestId();
    const record: FetchRecord = {
      requestId,
      handlers,
      groupOrder: options.parameters?.groupOrder ?? 'ascending',
      cancelled: false,
      settled: false,
    };

    this.#fetches.set(requestId, record);

    const request: FetchRequest = { requestId, ...options };

    void this.#openRequest(
      encodeFetch(request),
      record,
      (msg) => this.#handleFetchMessage(record, msg),
      (error) => {
        this.#fetches.delete(requestId);
        handlers.onError?.(error);
      }
    );

    return {
      requestId,
      cancel: (reason) => this.#cancelFetch(record, reason),
    };
  }

  trackStatus(options: MoqtSubscribeOptions, handlers: TrackStatusHandlers = {}): void {
    const requestId = this.#allocateRequestId();
    const record: TrackStatusRecord = { requestId, handlers, cancelled: false, settled: false };

    // Tracked so session teardown fails a still-pending TRACK_STATUS the
    // same way it fails pending subscriptions and fetches. Answered
    // requests leave the map — nothing else arrives on their stream.
    this.#trackStatuses.set(requestId, record);

    const message = encodeTrackStatus({
      requestId,
      trackNamespace: options.trackNamespace,
      trackName: options.trackName,
      parameters: options.parameters,
    });

    void this.#openRequest(
      message,
      record,
      (msg) => {
        if (msg.kind === 'request-ok') {
          this.#settleRequest(record);
          this.#trackStatuses.delete(requestId);
          handlers.onOk?.({ parameters: msg.parameters, trackProperties: msg.trackProperties });
          record.stream?.finWrite().catch(() => {});
        } else if (msg.kind === 'request-error') {
          this.#settleRequest(record);
          this.#trackStatuses.delete(requestId);
          handlers.onError?.(msg);
          record.stream?.finWrite().catch(() => {});
        }
      },
      (error) => {
        this.#trackStatuses.delete(requestId);
        handlers.onError?.(error);
      }
    );
  }

  subscribeNamespace(
    options: MoqtSubscribeNamespaceOptions,
    handlers: NamespaceSubscriptionHandlers = {}
  ): NamespaceSubscription {
    const requestId = this.#allocateRequestId();
    const record: NamespaceSubscriptionRecord = { requestId, handlers, cancelled: false, settled: false };
    const message = encodeSubscribeNamespace({
      requestId,
      trackNamespacePrefix: options.trackNamespacePrefix,
      parameters: options.parameters,
    });

    this.#namespaceSubscriptions.set(requestId, record);

    // Once only, and never after REQUEST_ERROR answered the request: that removes the record first.
    const end = (error?: unknown) => {
      if (!this.#namespaceSubscriptions.delete(requestId)) return;
      // A failed stream is dropped here: session teardown only reaches the records it still holds.
      if (error !== undefined) record.stream?.cancel(error);
      handlers.onEnd?.(error === undefined ? {} : { error });
    };

    void this.#openRequest(
      message,
      record,
      (msg) => this.#handleNamespaceMessage(record, msg),
      (error) => {
        // Removing the record puts the stream out of teardown's reach, so drop it now.
        record.stream?.cancel(error);
        this.#namespaceSubscriptions.delete(requestId);
        handlers.onError?.(error);
      },
      () => {
        // §3.3.2: the requester SHOULD FIN once the peer's FIN completes the request.
        record.stream?.finWrite().catch(() => {});
        end();
      },
      end
    );

    return {
      requestId,
      cancel: (reason) => {
        if (record.cancelled) return;

        record.cancelled = true;
        record.pendingCancelReason = reason;
        this.#settleRequest(record);
        record.stream?.cancel(reason);
        this.#namespaceSubscriptions.delete(requestId);
      },
    };
  }

  /**
   * Open a request stream and bind its lifecycle to `record`.
   *
   * `onFailure` is the caller's "this request died without answering me" path — `request-stream` deliberately leaves
   * that judgement here, since only the session knows which messages a given request kind requires. It fires for a FIN
   * before any response, a non-protocol stream error (transport reset), and the response timeout; a protocol error
   * still kills the whole session. Once `record.settled` is set the same events are ordinary end-of-request signals and
   * are ignored, except that a FIN calls `onEnd` and a stream error calls `onSettledError` when given.
   */
  async #openRequest(
    message: Uint8Array,
    record: RequestRecordBase,
    onMessage: (message: ControlMessage) => void,
    onFailure: (error: RequestError) => void,
    onEnd?: () => void,
    onSettledError?: (error: unknown) => void
  ): Promise<void> {
    const fail = (error: RequestError): void => {
      if (record.settled || record.cancelled || this.#destroyed) return;

      this.#settleRequest(record);
      onFailure(error);
    };

    let stream: BidirectionalStreamLike;

    try {
      stream = await this.#transport.createBidirectionalStream();
    } catch (error) {
      // Losing the ability to open streams is a session-level failure, but
      // this request also has to hear about it.
      fail(streamFailure(error instanceof Error ? error.message : 'failed to open request stream'));

      if (!this.#destroyed) this.#fatal(error);

      return;
    }

    const timeout = this.#config.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;

    if (timeout > 0 && !record.settled && !record.cancelled) {
      record.responseTimer = setTimeout(() => {
        const failure = streamFailure('no response before the request timeout', REQUEST_ERROR_CODE.TIMEOUT);

        // The stream dies with the request: left open, a late SUBSCRIBE_OK
        // would fire onOk after onError and re-register the alias route of
        // a subscription already reported dead.
        record.stream?.cancel(failure);
        fail(failure);
      }, timeout);
    }

    record.stream = openRequestStream(stream, message, {
      onMessage,
      onFin: () => {
        if (!record.settled) {
          fail(streamFailure('request stream closed before a response'));
        } else if (!record.cancelled && !this.#destroyed) {
          onEnd?.();
        }
      },
      onError: (error) => {
        if (isMoqtProtocolError(error)) {
          this.#fatal(error);
          return;
        }

        if (record.settled) {
          if (!record.cancelled && !this.#destroyed) onSettledError?.(error);
          return;
        }

        fail(streamFailure(error instanceof Error ? error.message : 'request stream failed'));
      },
    });

    // A cancel that raced the stream opening lands here.
    if (record.cancelled) record.stream.cancel(record.pendingCancelReason);
  }

  /** Mark a request answered (or dead) and disarm its response timeout. */
  #settleRequest(record: RequestRecordBase): void {
    record.settled = true;

    if (record.responseTimer !== undefined) {
      clearTimeout(record.responseTimer);
      record.responseTimer = undefined;
    }
  }

  #handleSubscriptionMessage(record: SubscriptionRecord, message: ControlMessage): void {
    if (record.cancelled) return;
    switch (message.kind) {
      case 'subscribe-ok': {
        if (record.trackAlias !== undefined && record.trackAlias !== message.trackAlias) {
          this.#fatal(new MoqtProtocolError('SUBSCRIBE_OK changed track alias'));
          return;
        }

        const existing = this.#aliasRoutes.get(message.trackAlias);

        if (existing && existing !== record) {
          this.#fatal(new MoqtProtocolError('duplicate track alias', SESSION_ERROR.DUPLICATE_TRACK_ALIAS));
          return;
        }

        this.#settleRequest(record);
        record.trackAlias = message.trackAlias;
        this.#aliasRoutes.set(message.trackAlias, record);
        const waiters = this.#aliasWaiters.get(message.trackAlias);

        if (waiters) {
          this.#aliasWaiters.delete(message.trackAlias);

          for (const waiter of waiters) {
            clearTimeout(waiter.timer);
            waiter.resolve(record);
          }
        }

        record.handlers.onOk?.({
          trackAlias: message.trackAlias,
          parameters: message.parameters,
          trackProperties: message.trackProperties,
        });
        break;
      }
      case 'request-ok':
        record.handlers.onUpdateOk?.();
        break;
      case 'request-error':
        this.#settleRequest(record);
        this.#removeSubscription(record);
        record.handlers.onError?.(message);
        break;
      case 'publish-done':
        // Data streams may still be inbound; the alias route stays until
        // the subscription is cancelled or the session ends, so late
        // subgroups still deliver (§10.12 leaves the timing policy to us).
        record.publisherDone = true;
        record.handlers.onDone?.(message);
        break;
      case 'publish-state-notify':
        // Unilateral (§10.10): no REQUEST_OK back, and it does not count
        // against MAX_REQUEST_UPDATES.
        record.handlers.onStateNotify?.(message.parameters);
        break;
      case 'goaway':
        record.handlers.onGoaway?.(message);
        break;
      default:
        this.#fatal(new MoqtProtocolError(`unexpected ${message.kind} on a subscribe request stream`));
    }
  }

  #handleFetchMessage(record: FetchRecord, message: ControlMessage): void {
    switch (message.kind) {
      case 'fetch-ok':
        this.#settleRequest(record);
        record.handlers.onOk?.({
          endOfTrack: message.endOfTrack,
          endLocation: message.endLocation,
          parameters: message.parameters,
          trackProperties: message.trackProperties,
        });
        break;
      // Some deployed relays accept a FETCH with the generic REQUEST_OK
      // instead of the FETCH_OK §10.13 mandates. It
      // carries no End Location/End Of Track, so `onOk` can't fire — the
      // fetch's actual completion still surfaces via onEntry/onEnd on the
      // data stream, which this response has no bearing on. It is still
      // the response, so it has to settle the request: leaving the timer
      // armed would fail an answered fetch (and drop its data stream)
      // once the deadline passed.
      case 'request-ok':
        this.#settleRequest(record);
        break;
      case 'request-error':
        this.#settleRequest(record);
        this.#fetches.delete(record.requestId);
        record.handlers.onError?.(message);
        break;
      case 'goaway':
        record.handlers.onGoaway?.(message);
        break;
      default:
        this.#fatal(new MoqtProtocolError(`unexpected ${message.kind} on a fetch request stream`));
    }
  }

  #handleNamespaceMessage(record: NamespaceSubscriptionRecord, message: ControlMessage): void {
    if (record.cancelled) return;
    switch (message.kind) {
      case 'request-ok':
        if (record.settled) {
          this.#fatal(new MoqtProtocolError('second REQUEST_OK on a subscribe-namespace request stream'));
          return;
        }

        this.#settleRequest(record);
        record.accepted = true;
        record.handlers.onOk?.();
        break;
      case 'request-error':
        // After REQUEST_OK the response is spent: an error then is outside the stream's grammar.
        if (record.settled) {
          this.#fatal(new MoqtProtocolError('REQUEST_ERROR after REQUEST_OK on a subscribe-namespace request stream'));
          return;
        }

        this.#settleRequest(record);
        this.#namespaceSubscriptions.delete(record.requestId);
        record.handlers.onError?.(message);
        record.stream?.finWrite().catch(() => {});
        break;
      case 'namespace':
      case 'namespace-done':
        // Entries follow REQUEST_OK (§10.18); one before it, or after REQUEST_ERROR, is outside the
        // stream's grammar.
        if (!record.accepted) {
          this.#fatal(
            new MoqtProtocolError(`${message.kind} before REQUEST_OK on a subscribe-namespace request stream`)
          );
          return;
        }

        if (message.kind === 'namespace') record.handlers.onNamespace?.(message.trackNamespaceSuffix);
        else record.handlers.onNamespaceDone?.(message.trackNamespaceSuffix);
        break;
      case 'goaway':
        record.handlers.onGoaway?.(message);
        break;
      default:
        this.#fatal(new MoqtProtocolError(`unexpected ${message.kind} on a subscribe-namespace request stream`));
    }
  }

  #cancelSubscription(record: SubscriptionRecord, reason?: unknown): void {
    if (record.cancelled) return;

    record.cancelled = true;
    record.pendingCancelReason = reason;
    this.#settleRequest(record);
    record.stream?.cancel(reason);
    this.#removeSubscription(record);
  }

  #removeSubscription(record: SubscriptionRecord): void {
    record.cancelled = true;
    record.stream?.cancel(record.pendingCancelReason);
    for (const reader of record.readers) void reader.cancel(record.pendingCancelReason).catch(() => {});
    record.readers.clear();
    this.#subscriptions.delete(record.requestId);

    if (record.trackAlias !== undefined && this.#aliasRoutes.get(record.trackAlias) === record) {
      this.#aliasRoutes.delete(record.trackAlias);
    }
  }

  #cancelFetch(record: FetchRecord, reason?: unknown): void {
    if (record.cancelled) return;

    record.cancelled = true;
    record.pendingCancelReason = reason;
    this.#settleRequest(record);
    record.stream?.cancel(reason);
    this.#fetches.delete(record.requestId);
  }

  // --------------------------------------------------------------------------
  // Incoming unidirectional streams (control, subgroup, fetch, padding)
  // --------------------------------------------------------------------------

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

    if (isSubgroupHeaderType(streamType)) {
      const header = await readSubgroupHeader(reader, streamType);

      await this.#runSubgroupStream(reader, header);
      return;
    }

    if (streamType === STREAM_TYPE.FETCH_HEADER) {
      const { requestId } = await readFetchHeader(reader);

      await this.#runFetchStream(reader, requestId);
      return;
    }

    if (streamType === STREAM_TYPE.PADDING) {
      await reader.cancel();
      return;
    }

    await reader.cancel();
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

    // Subsequent control-stream messages: GOAWAY (at most once).
    while (!(await reader.atEnd())) {
      const type = await reader.readVarint();
      const message = await this.#readControlFrame(reader, type);
      if (message.kind !== 'goaway') throw new MoqtProtocolError(`unexpected ${message.kind} on the control stream`);

      if (this.#receivedControlGoaway) {
        throw new MoqtProtocolError('received more than one GOAWAY on the control stream');
      }

      this.#receivedControlGoaway = true;
      this.#callbacks.onGoaway?.(message);
    }

    // A closed control stream ends the session (§3.3).
    if (!this.#destroyed) {
      this.#fatal(new MoqtProtocolError('peer closed its control stream'));
    }
  }

  /** Wait briefly for a subscription to claim `trackAlias` (§11.4.2 buffering). */
  #waitForAlias(trackAlias: number): Promise<SubscriptionRecord | undefined> {
    const known = this.#aliasRoutes.get(trackAlias);
    if (known || this.#destroyed) return Promise.resolve(known);

    return new Promise((resolve) => {
      const waiters = this.#aliasWaiters.get(trackAlias) ?? [];
      const timeout = this.#config.unknownAliasTimeoutMs ?? DEFAULT_UNKNOWN_ALIAS_TIMEOUT_MS;
      const waiter: AliasWaiter = {
        resolve,
        timer: setTimeout(() => {
          const remaining = this.#aliasWaiters.get(trackAlias)?.filter((w) => w !== waiter);

          if (remaining?.length) this.#aliasWaiters.set(trackAlias, remaining);
          else this.#aliasWaiters.delete(trackAlias);

          resolve(undefined);
        }, timeout),
      };

      waiters.push(waiter);
      this.#aliasWaiters.set(trackAlias, waiters);
    });
  }

  async #runSubgroupStream(reader: StreamReader, header: SubgroupHeader): Promise<void> {
    const record = await this.#waitForAlias(header.trackAlias);

    if (!record || record.cancelled) {
      await reader.cancel();
      return;
    }

    record.readers.add(reader);
    let lastSubgroupId = header.subgroupIdMode === 'explicit' ? header.subgroupId! : 0;

    try {
      for await (const object of readSubgroupObjects(reader, header)) {
        if (record.cancelled) {
          await reader.cancel();
          return;
        }

        lastSubgroupId = object.subgroupId;
        record.handlers.onObject?.(object);
      }

      if (record.cancelled) return;
      record.handlers.onSubgroupEnd?.({
        groupId: header.groupId,
        subgroupId: lastSubgroupId,
        endOfGroup: header.endOfGroup,
      });
    } catch (error) {
      if (record.cancelled) return;
      if (isMoqtProtocolError(error)) throw error;

      // A reset stream is a delivery event (timeout, cancellation), not a
      // session error — surface it to the subscription.
      record.handlers.onSubgroupReset?.({ groupId: header.groupId, error });
    } finally {
      record.readers.delete(reader);
    }
  }

  async #runFetchStream(reader: StreamReader, requestId: number): Promise<void> {
    const record = this.#fetches.get(requestId);

    // A FETCH_HEADER naming no fetch of ours is dropped, not fatal. That
    // includes a fill fetch stream (§5.1.3), keyed by a SUBSCRIBE's Request
    // ID — this subscriber never asks for one, so none is expected.
    if (!record || record.cancelled) {
      await reader.cancel();
      return;
    }

    try {
      for await (const entry of readFetchEntries(reader, record.groupOrder)) {
        if (record.cancelled) {
          await reader.cancel();
          return;
        }

        record.handlers.onEntry?.(entry);
      }

      record.handlers.onEnd?.();
    } catch (error) {
      if (isMoqtProtocolError(error)) throw error;

      // A reset mid-replay means entries may be missing — never report it
      // as the clean FIN `onEnd` promises.
      record.handlers.onReset?.({ error });
    }
  }

  // --------------------------------------------------------------------------
  // Incoming bidirectional streams (server-initiated requests)
  // --------------------------------------------------------------------------

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

    // Before answering: a request this client rejects still consumes its id.
    if (initiatingRequestId !== undefined) this.#claimPeerRequestId(initiatingRequestId);

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

    if (message.kind === 'publish') {
      const callbacks = this.#callbacks;

      if (callbacks.onIncomingPublish) {
        let responded = false;

        callbacks.onIncomingPublish(message, {
          accept: () => {
            if (responded) return;

            responded = true;
            void respond(encodeRequestOk());
          },
          reject: (errorCode = REQUEST_ERROR_CODE.UNINTERESTED, reason = '') => {
            if (responded) return;

            responded = true;
            void respond(encodeRequestError(errorCode, reason));
          },
        });
        return;
      }

      await respond(encodeRequestError(REQUEST_ERROR_CODE.UNINTERESTED, 'subscribe-only client'));
      return;
    }

    // Any other server-initiated request: this client doesn't serve them.
    await respond(encodeRequestError(REQUEST_ERROR_CODE.NOT_SUPPORTED, 'subscribe-only client'));
  }
}

/**
 * Create a MOQT session driver over an established transport.
 *
 * @example
 *   ```ts
 *   // TypeScript's DOM lib does not declare WebTransport's `protocol` yet.
 *   const transport = new WebTransport(url, { protocols: [MOQT_PROTOCOL_ID] }) as WebTransport & MoqtTransport;
 *   await transport.ready;
 *   if (transport.protocol !== MOQT_PROTOCOL_ID) throw new Error(`negotiated ${transport.protocol || 'none'}`);
 *   const session = createMoqtSession(transport, {
 *     callbacks: { onGoaway: (g) => migrate(g.newSessionUri) },
 *   });
 *   const subscription = session.subscribe(
 *     { trackNamespace: ['live', 'stream1'], trackName: 'catalog' },
 *     { onObject: (object) => handleCatalogObject(object) }
 *   );
 *   ```;
 */
export function createMoqtSession(transport: MoqtTransport, config: MoqtSessionConfig = {}): MoqtSession {
  return new MoqtSessionImpl(transport, config);
}
