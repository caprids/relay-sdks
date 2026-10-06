/**
 * What the supervised publisher and subscriber share: their status names, the backoff between attempts, the
 * generation field a supervised publisher appends to its root namespace, and the announce-race classifier. The
 * supervisors are policy the application opts into (spec/behavior.md, "Supervised connections"); everything here is
 * built on the public primitives' facts.
 */
import type { RequestError } from '@mux/moq-core';
import { RelayError } from './errors.js';

/**
 * Where a supervised connection stands: `connecting` until the first connection is up, `live` while one is,
 * `reconnecting` between connections, and `closed` once the application closed it or its signal aborted.
 */
export type SupervisedStatus = 'connecting' | 'live' | 'reconnecting' | 'closed';

/** The delay before each retry: it starts at `initialDelayMs` and doubles up to `maxDelayMs`, with jitter. */
export interface SupervisionBackoff {
  /** The delay before the first retry, in milliseconds. */
  initialDelayMs?: number;
  /** The longest delay between retries, in milliseconds. */
  maxDelayMs?: number;
}

/** Reconnection: from half a second, doubling up to ten. */
export const RECONNECT_BACKOFF = { initialDelayMs: 500, maxDelayMs: 10_000 } as const;
/** Resubscription: from a quarter second, doubling up to eight. */
export const RESUBSCRIBE_BACKOFF = { initialDelayMs: 250, maxDelayMs: 8_000 } as const;

/** Check a backoff's bounds and fill in the defaults. */
export function backoffOptions(
  name: string,
  given: SupervisionBackoff | undefined,
  defaults: Required<SupervisionBackoff>,
): Required<SupervisionBackoff> {
  const initialDelayMs = given?.initialDelayMs ?? defaults.initialDelayMs;
  const maxDelayMs = given?.maxDelayMs ?? Math.max(defaults.maxDelayMs, initialDelayMs);
  for (const [field, value] of [
    ['initialDelayMs', initialDelayMs],
    ['maxDelayMs', maxDelayMs],
  ] as const) {
    if (!Number.isSafeInteger(value) || value < 0 || value > 2_147_483_647) {
      throw new RangeError(`${name}.${field} must be an integer between 0 and 2147483647`);
    }
  }
  if (maxDelayMs < initialDelayMs) throw new RangeError(`${name}.maxDelayMs must not be less than initialDelayMs`);
  return { initialDelayMs, maxDelayMs };
}

/**
 * The delay before retry `attempt` (0 first): the doubled delay, capped, then drawn from its upper half, so that
 * many clients that lost the same relay do not come back in step.
 */
export function backoffDelay(attempt: number, { initialDelayMs, maxDelayMs }: Required<SupervisionBackoff>): number {
  const ceiling = Math.min(maxDelayMs, initialDelayMs * 2 ** Math.min(attempt, 30));
  return Math.round(ceiling / 2 + (Math.random() * ceiling) / 2);
}

/** How long before the credential's expiry a supervisor replaces its connection, by default. */
export const ROTATE_BEFORE_MS = 60_000;
/** The longest a supervisor keeps the replaced connection open while its tracks or wants move over. */
export const ROTATION_OVERLAP_MS = 10_000;

/** Check `rotateBeforeMs`: a whole number of milliseconds a timer can hold. */
export function rotateBeforeOption(value: unknown): number {
  if (value === undefined) return ROTATE_BEFORE_MS;
  if (typeof value !== 'number') throw new TypeError('rotateBeforeMs must be a number of milliseconds');
  if (!Number.isSafeInteger(value) || value < 0 || value > 2_147_483_647) {
    throw new RangeError('rotateBeforeMs must be an integer between 0 and 2147483647');
  }
  return value;
}

/**
 * When a connection whose credential expires at `expiresAt` is replaced: `rotateBeforeMs` before it, but no earlier
 * than halfway through what is left, so a token shorter than the margin still gets a connection's worth of use.
 * `undefined` when the expiry is unknown.
 */
export function rotationDelay(
  expiresAt: Date | undefined,
  rotateBeforeMs: number,
  now = Date.now(),
): number | undefined {
  if (expiresAt === undefined) return undefined;
  const left = expiresAt.getTime() - now;
  return Math.max(0, left - rotateBeforeMs, left / 2);
}

/**
 * When, on the `performance.now()` clock, a connection whose credential expires at `expiresAt` is replaced (see
 * {@link rotationDelay}). Computed once, when it connects, so time spent in a rotation's overlap does not push it back.
 */
export function rotationDeadline(expiresAt: Date | undefined, rotateBeforeMs: number): number | undefined {
  const delay = rotationDelay(expiresAt, rotateBeforeMs);
  return delay === undefined ? undefined : performance.now() + delay;
}

/**
 * Resolve once `deadline` (from {@link rotationDeadline}) passed, or `manual` resolved. Never resolves after `signal`
 * aborts. A wait past what one timer holds is waited out in steps.
 */
export function rotationDue(deadline: number | undefined, manual: Promise<void>, signal: AbortSignal): Promise<void> {
  if (deadline === undefined) return manual;
  const timed = (async () => {
    for (;;) {
      const left = deadline - performance.now();
      if (left <= 0) return;
      await sleep(Math.min(left, 2_147_483_647), signal);
    }
  })();
  // An abort ends the wait without resolving it; nobody is left to hear the rejection.
  return Promise.race([timed.catch(() => new Promise<void>(() => {})), manual]);
}

/** Resolve after `ms`, or reject with the abort reason once `signal` aborts. */
export function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(signal.reason);
      return;
    }
    const abort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', abort);
      resolve();
    }, ms);
    signal.addEventListener('abort', abort, { once: true });
  });
}

/** A promise that resolves once `signal` aborts; it never rejects. */
export function whenAborted(signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => signal.addEventListener('abort', () => resolve(), { once: true }));
}

/** Digits of base-36 milliseconds: enough until the year 5188. */
const GENERATION_TIME_DIGITS = 9;

/**
 * A fresh generation field: the start time in fixed-width base 36, then four random characters. Generations sort by
 * start time as strings, which is how a subscriber following a root picks the newest among those announced.
 */
export function newGeneration(now = Date.now()): string {
  const time = now.toString(36).padStart(GENERATION_TIME_DIGITS, '0');
  const random = Array.from(crypto.getRandomValues(new Uint8Array(4)), (byte) => (byte % 36).toString(36)).join('');
  return `${time}${random}`;
}

/** MOQT's DOES_NOT_EXIST: moq-relay 0.15.8 and later's code for a namespace it has not seen announced. */
const FRESH_NAMESPACE_ERROR_CODE = 0x10;
/** The reason that singles the fresh namespace out of DOES_NOT_EXIST; "not found" is an unknown track instead. */
const FRESH_NAMESPACE_REASON = 'unroutable';
/** moq-relay's literal through 0.15.1 for a namespace it has not seen announced. */
const LEGACY_FRESH_NAMESPACE_ERROR_CODE = 404;

/** Whether the relay refused a namespace it has not seen announced (yet): the announce race. */
export function isFreshNamespaceRejection(requestError: RequestError): boolean {
  if (requestError.errorCode === LEGACY_FRESH_NAMESPACE_ERROR_CODE) return true;
  return requestError.errorCode === FRESH_NAMESPACE_ERROR_CODE && requestError.reason === FRESH_NAMESPACE_REASON;
}

/**
 * REQUEST_ERROR codes no wait fixes: credentials (0x1, 0x4, 0x5), a request the relay does not support (0x3), and a
 * request it cannot serve as made (0x11 INVALID_RANGE, 0x12 MALFORMED_TRACK). A supervised subscription surfaces
 * these and stops; it retries every other code with backoff.
 */
const PERMANENT_REQUEST_ERRORS: ReadonlySet<number> = new Set([0x1, 0x3, 0x4, 0x5, 0x11, 0x12]);

/** Whether a REQUEST_ERROR is one a supervised subscription surfaces instead of retrying. */
export function isPermanentRequestError(requestError: RequestError): boolean {
  return PERMANENT_REQUEST_ERRORS.has(requestError.errorCode);
}

/**
 * Whether a failed connection attempt is one a supervisor does not retry before its first connection: an argument
 * error, or the relay refusing the credentials. Once a connection has been up, a refusal is retried, since the token
 * provider may hand out a fresh token on the next attempt.
 */
export function isSetupFailure(error: unknown): boolean {
  if (error instanceof TypeError || error instanceof RangeError) return true;
  return error instanceof RelayError && error.code === 'UNAUTHORIZED';
}
