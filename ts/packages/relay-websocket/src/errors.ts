/**
 * The contract's error shapes (spec/transport-adapter.md TA-22) for what `@moq/qmux` raises. Its stream errors already
 * have the stream shape and pass through; a session failure becomes a {@link WebSocketSessionError}, which carries the
 * peer's application close when there was one (TA-24).
 */

/** How `closed` resolves after an application close from either end (TA-9). */
export interface CloseInfo {
  /** The application close code. */
  closeCode: number;
  /** The close reason. */
  reason: string;
}

/** A session failure (TA-22): the session is over, or the dial failed. */
export class WebSocketSessionError extends Error {
  /** Always `'WebSocketSessionError'`; not part of the contract, which classifies by `source`. */
  override readonly name = 'WebSocketSessionError';
  /** Always `'session'`. */
  readonly source = 'session';
  /** The peer's application close code, when the peer closed the session (TA-24). */
  declare readonly closeCode?: number;
  /** The peer's close reason, when the peer closed the session (TA-24). */
  declare readonly reason?: string;

  constructor(message: string, options: { cause?: unknown; close?: CloseInfo | undefined } = {}) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    if (options.close) {
      this.closeCode = options.close.closeCode;
      this.reason = options.close.reason;
    }
  }
}

/** The code a reset or a stop carries for `reason` (TA-17): its integer `streamErrorCode` from 0 to 2³²−1, else 0. */
export function resetCode(reason: unknown): number {
  const code =
    typeof reason === 'object' && reason !== null
      ? (reason as { streamErrorCode?: unknown }).streamErrorCode
      : undefined;
  return typeof code === 'number' && Number.isInteger(code) && code >= 0 && code <= 0xffff_ffff ? code : 0;
}

/**
 * The reason handed to `@moq/qmux` for a reset or a stop with `reason`'s code. It reads `streamErrorCode` by field and
 * wraps it to 32 bits (`-1` would go out as 2³²−1), so the code is checked here first.
 */
export function resetReason(reason: unknown): { streamErrorCode: number } {
  return { streamErrorCode: resetCode(reason) };
}

function sourceOf(error: unknown): unknown {
  return typeof error === 'object' && error !== null ? (error as { source?: unknown }).source : undefined;
}

/** Whether `error` is a stream failure: the peer reset or stopped the stream (TA-19, TA-20). */
export function isStreamError(error: unknown): boolean {
  return sourceOf(error) === 'stream';
}
