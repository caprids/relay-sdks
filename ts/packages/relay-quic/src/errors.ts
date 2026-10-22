/**
 * The addon's failures as the contract's error shapes (spec/transport-adapter.md TA-22): a stream error is the core's
 * `MoqtStreamError`, a session error a {@link NativeQuicSessionError}. Codes cross from Rust as decimal strings, since a
 * QUIC varint has 62 bits.
 */
import { MoqtStreamError } from '@caprid/moq-core';
import type { Failure } from './addon.js';

/** A session failure (TA-22): the session is over, or the dial failed. */
export class NativeQuicSessionError extends Error {
  override readonly name = 'NativeQuicSessionError';
  /** Always `'session'`. */
  readonly source = 'session';
  /** What ended the session, as the addon reports it: `application`, `transport`, `timeout`, `cancelled` and so on. */
  readonly kind: string;
  /** The peer's application close code, when the peer closed the session (TA-24). */
  declare readonly closeCode?: number;
  /** The peer's close reason, when the peer closed the session (TA-24). */
  declare readonly reason?: string;

  constructor(message: string, kind: string, close?: { closeCode: number; reason: string }) {
    super(message);
    this.kind = kind;
    if (close) {
      this.closeCode = close.closeCode;
      this.reason = close.reason;
    }
  }
}

const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);

/** A decimal code as a number, or `null` above 2⁵³−1, where a number would round (TA-22). */
export function safeCode(code: string | undefined): number | null {
  if (code === undefined) return null;
  const value = BigInt(code);
  return value <= MAX_SAFE ? Number(value) : null;
}

/** The code a reset or a stop carries for `reason` (TA-17): its integer `streamErrorCode` from 0 to 2³²−1, else 0. */
export function resetCode(reason: unknown): number {
  const code =
    typeof reason === 'object' && reason !== null
      ? (reason as { streamErrorCode?: unknown }).streamErrorCode
      : undefined;
  return typeof code === 'number' && Number.isInteger(code) && code >= 0 && code <= 0xffff_ffff ? code : 0;
}

/** The error an operation rejects with for `failure`. */
export function toError(failure: Failure): Error {
  switch (failure.source) {
    case 'stream': {
      const what = failure.kind === 'stopped' ? 'stopped' : 'reset';
      const detail = failure.code === undefined ? '' : ` (code ${failure.code})`;
      return new MoqtStreamError(`the peer ${what} the stream${detail}`, safeCode(failure.code));
    }
    case 'session': {
      // The peer's close is a session error only on a stream; `closed` itself resolves with it (TA-9).
      const close =
        failure.kind === 'application' ? { closeCode: safeCode(failure.code) ?? 0, reason: failure.reason } : undefined;
      return new NativeQuicSessionError(sessionMessage(failure), failure.kind, close);
    }
    default:
      // `credit` and `closed` are not stream or session failures, so they carry no `source` (TA-21, TA-23).
      return new Error(failure.reason);
  }
}

function sessionMessage({ kind, code, reason }: Failure): string {
  switch (kind) {
    case 'application':
      return `the peer closed the session (code ${code}${reason ? `, ${reason}` : ''})`;
    case 'transport':
      return `the QUIC connection failed: ${reason || 'no reason given'}${code === undefined ? '' : ` (code ${code})`}`;
    default:
      return reason;
  }
}
