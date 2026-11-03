/**
 * `@caprid/relay/diagnostics`: client diagnostics (spec/behavior.md, "Client diagnostics"; TypeScript only). A
 * recorder holds what a client saw, as events and stats sections; `publishDiagnostics` puts it on two tracks of the
 * client's own broadcast, sent only while something subscribes; `followDiagnostics` reads them from elsewhere, for a
 * collector or an operator.
 *
 * ```ts
 * import { createDiagnostics, publishDiagnostics } from '@caprid/relay/diagnostics';
 *
 * const diagnostics = createDiagnostics();
 * const publisher = await connectSupervisedPublisher({ namespace, getToken });
 * publishDiagnostics(diagnostics, publisher);
 * diagnostics.event('app.joined', { participant: id });
 * ```
 *
 * @packageDocumentation
 */
export {
  createDiagnostics,
  DIAGNOSTICS_TRACKS,
  type DiagnosticEvent,
  type Diagnostics,
  type DiagnosticsEventsObject,
  type DiagnosticsOptions,
  type DiagnosticsPublication,
  type DiagnosticsPublisher,
  type DiagnosticsStatsObject,
  type FollowDiagnosticsHandlers,
  type FollowDiagnosticsOptions,
  followDiagnostics,
  type PublishDiagnosticsOptions,
  publishDiagnostics,
  type WatchedAudioInput,
  watchAudioInput,
} from './client-diagnostics.js';
