/**
 * `@caprid/relay/diagnostics`: client diagnostics (spec/behavior.md, "Client diagnostics"; TypeScript only). A
 * recorder holds what a client saw, as events and stats sections; `publishDiagnostics` puts it on two tracks of a
 * publication of the client's own, by convention `<account>/diag/<app>/<group>/<client>`, sent only while something
 * subscribes; `followDiagnostics` reads them from elsewhere, for a collector or an operator. Every client can report,
 * a player that publishes no media too. `watchPage` and `watchAudioInput` add the page and an audio input.
 *
 * ```ts
 * import { createDiagnostics, publishDiagnostics } from '@caprid/relay/diagnostics';
 *
 * const diagnostics = createDiagnostics({ app: { name: 'huddle', build } });
 * const relay = await connectSupervisedRelay({ getToken, diagnostics });
 * publishDiagnostics(diagnostics, relay.publish(['diag', 'huddle', room, pid]));
 * diagnostics.event('app.joined', { participant: pid });
 * ```
 *
 * @packageDocumentation
 */
export {
  createDiagnostics,
  DIAGNOSTICS_TRACKS,
  type DiagnosticEvent,
  type Diagnostics,
  type DiagnosticsApp,
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
  watchPage,
} from './client-diagnostics.js';
