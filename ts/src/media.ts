/**
 * `@caprid/relay/media`: the media policy over registered tracks and subscriptions (spec/behavior.md, "Media publishing"
 * and "Media subscribing"). A frame writer maps encoded frames onto a track's groups; a catalog service and a document
 * service serve state-shaped tracks, and `mediaTrackProperties` gives a media track its default delivery timeout and publisher
 * priority; `mediaSubscribeParameters` gives a viewer's subscription to it the matching subscriber priority, and a
 * frame reader reads a subscription as frames in decode order. They know no format: a frame carries its own object
 * properties, a document is bytes, and the reader drives a sequencer it is given, so they pair with `@caprid/moq-media`'s
 * LOC and MSF helpers and its `FrameSequencer` without depending on them.
 *
 * ```ts
 * import { createCatalogService, createFrameWriter, mediaTrackProperties } from '@caprid/relay/media';
 *
 * const video = createFrameWriter(
 *   publisher.registerTrack({
 *     track: 'video',
 *     trackProperties: [...locTrackProperties(), ...mediaTrackProperties('video', { keyframeIntervalUs: 1_000_000 })],
 *   }),
 * );
 * const outcome = await video.write(chunkToLocFrame(chunk, { videoConfig }));
 * ```
 *
 * @packageDocumentation
 */
export {
  type CatalogService,
  type CatalogServiceOptions,
  createCatalogService,
  createDocumentService,
  type DocumentService,
  type DocumentServiceOptions,
  type ServeOptions,
} from './document-service.js';
export {
  createFrameReader,
  type FrameReader,
  type FrameReaderOptions,
  type FrameReaderSequencer,
} from './frame-reader.js';
export {
  createFrameWriter,
  type Frame,
  type FrameGrouping,
  type FrameOutcome,
  type FrameOutcomeKind,
  type FrameWriter,
  type FrameWriterOptions,
} from './frame-writer.js';
export {
  type MediaKind,
  type MediaTrackPropertiesOptions,
  mediaSubscribeParameters,
  mediaTrackProperties,
} from './media-track-properties.js';
export {
  createReplaceableTrack,
  type ReplaceableTrack,
  type ReplaceableTrackCatalog,
  type ReplaceableTrackOptions,
  type TrackReplacement,
} from './replaceable-track.js';
