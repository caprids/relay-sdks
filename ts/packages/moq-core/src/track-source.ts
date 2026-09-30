import type { MoqtSession } from './protocol/session.js';

/**
 * Borrowed access to tracks on a session. Consumers own their requests; only the
 * connection owner can close the underlying transport. No media format is implied.
 */
export interface MoqTrackSource extends Pick<MoqtSession, 'subscribe' | 'fetch' | 'trackStatus'> {
  /** Resolves when the owning connection ends; an error denotes failure. */
  readonly closed: Promise<{
    /** Why the connection failed; absent when it closed cleanly. */
    error?: unknown;
  }>;
}
