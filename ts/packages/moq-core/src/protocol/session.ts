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
  readonly incomingUnidirectionalStreams: ReadableStream<ReadableStream<Uint8Array>>;
  readonly incomingBidirectionalStreams: ReadableStream<BidirectionalStreamLike>;
  createUnidirectionalStream(): Promise<WritableStream<Uint8Array>>;
  createBidirectionalStream(): Promise<BidirectionalStreamLike>;
  close(closeInfo?: { closeCode?: number; reason?: string }): void;
  readonly closed: Promise<unknown>;
}

// ============================================================================
// Public session surface
// ============================================================================

export interface RequestError {
  errorCode: number;
  retryInterval: number;
  reason: string;
  redirect?: Redirect;
}

export interface SubscribeOk {
  trackAlias: number;
  parameters: MessageParameters;
  trackProperties: KeyValuePair[];
}

export interface PublishDone {
  statusCode: number;
  /** Data streams the publisher opened; `undefined` when it could not count them (§10.12). */
  streamCount: number | undefined;
  reason: string;
}

export interface SubscriptionHandlers {
  onOk?(ok: SubscribeOk): void;
  onError?(error: RequestError): void;
  onObject?(object: MoqtObject): void;
  /** A subgroup stream finished cleanly (FIN). */
  onSubgroupEnd?(info: { groupId: number; subgroupId: number; endOfGroup: boolean }): void;
  /**
   * A subgroup stream was reset — objects from it may be missing. Reported for every reset the session receives; a
   * relay that drops a reset instead of forwarding it leaves nothing to report.
   */
  onSubgroupReset?(info: { groupId: number; error: unknown }): void;
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

export interface Subscription {
  readonly requestId: number;
  /** Modify the subscription (REQUEST_UPDATE, §10.9). */
  update(parameters: MessageParameters): void;
  /** Tear the subscription down. MOQT has no UNSUBSCRIBE message — teardown is the request stream's lifecycle (§3.3.3). */
  cancel(reason?: unknown): void;
}

export interface FetchOk {
  endOfTrack: boolean;
  endLocation: { group: number; object: number };
  parameters: MessageParameters;
  trackProperties: KeyValuePair[];
}

export interface FetchHandlers {
  onOk?(ok: FetchOk): void;
  onError?(error: RequestError): void;
  onEntry?(entry: FetchStreamEntry): void;
  /** The fetch data stream finished cleanly (FIN). */
  onEnd?(): void;
  /** The fetch data stream was reset — delivered entries may be incomplete. */
  onReset?(info: { error: unknown }): void;
  onGoaway?(goaway: Goaway): void;
}

export interface FetchHandle {
  readonly requestId: number;
  cancel(reason?: unknown): void;
}

export interface TrackStatusHandlers {
  onOk?(status: { parameters: MessageParameters; trackProperties: KeyValuePair[] }): void;
  onError?(error: RequestError): void;
}

export interface Goaway {
  newSessionUri: string;
  timeout: number;
}

export interface IncomingPublish {
  requestId: number;
  trackNamespace: TrackNamespace;
  trackName: string;
  trackAlias: number;
  parameters: MessageParameters;
}

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
  callbacks?: MoqtSessionCallbacks | undefined;
}

export interface MoqtSubscribeOptions {
  trackNamespace: TrackNamespace;
  trackName: string;
  parameters?: MessageParameters | undefined;
}

/**
 * FETCH (§10.13) takes the SUBSCRIBE shape; the range is `parameters.locationFilter`, read with fetch rules (§5.1.2): a
 * relative start counts back from Largest Object, an omitted end stops there, no filter means `{0,0}` up to Largest
 * Object. A subscription reads the same filter with subscription rules; see `LocationFilter` in control-messages.ts.
 */
export type MoqtFetchOptions = MoqtSubscribeOptions;

export interface MoqtSession {
  /** Resolves when the server's SETUP has been received. */
  readonly ready: Promise<void>;
  subscribe(options: MoqtSubscribeOptions, handlers?: SubscriptionHandlers): Subscription;
  fetch(options: MoqtFetchOptions, handlers?: FetchHandlers): FetchHandle;
  trackStatus(options: MoqtSubscribeOptions, handlers?: TrackStatusHandlers): void;
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
  #subscriptions = new Map<number, SubscriptionRecord>();
  #fetches = new Map<number, FetchRecord>();
  #trackStatuses = new Map<number, TrackStatusRecord>();
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
    const records = [...this.#subscriptions.values(), ...this.#fetches.values(), ...this.#trackStatuses.values()];
    const pending = expected ? [] : records.filter((r) => !r.settled && !r.cancelled);

    for (const record of records) {
      this.#settleRequest(record);
      record.stream?.cancel(error);
    }

    for (const record of this.#subscriptions.values()) this.#removeSubscription(record);
    this.#subscriptions.clear();
    this.#fetches.clear();
    this.#trackStatuses.clear();
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

    // No GOAWAY: moq-relay 0.14.17 closes the session on a client GOAWAY (code 17), and one written
    // in the same turn as the transport close gives the peer nothing to act on. The publisher follows
    // the same rule; see spec/behavior.md, "Relay interop deviations".
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

  /**
   * Open a request stream and bind its lifecycle to `record`.
   *
   * `onFailure` is the caller's "this request died without answering me" path — `request-stream` deliberately leaves
   * that judgement here, since only the session knows which messages a given request kind requires. It fires for a FIN
   * before any response, a non-protocol stream error (transport reset), and the response timeout; a protocol error
   * still kills the whole session. Once `record.settled` is set the same events are ordinary end-of-request signals and
   * are ignored.
   */
  async #openRequest(
    message: Uint8Array,
    record: RequestRecordBase,
    onMessage: (message: ControlMessage) => void,
    onFailure: (error: RequestError) => void
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
      onFin: () => fail(streamFailure('request stream closed before a response')),
      onError: (error) => {
        if (isMoqtProtocolError(error)) {
          this.#fatal(error);
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
 *   const transport = new WebTransport(url, { protocols: [MOQT_PROTOCOL_ID] });
 *   await transport.ready;
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
