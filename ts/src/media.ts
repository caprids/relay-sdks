/**
 * `@mux/relay/media`: the media publishing policy over registered tracks (spec/behavior.md, "Media publishing"). A
 * frame writer maps encoded frames onto a track's groups; a catalog service and a document service serve state-shaped
 * tracks, and `mediaTrackProperties` gives a media track its default delivery timeout. They know no format: a frame
 * carries its own object properties and a document is bytes, so they pair with `@mux/moq-media`'s LOC and MSF helpers
 * without depending on them.
 *
 * ```ts
 * import { createCatalogService, createFrameWriter, mediaTrackProperties } from '@mux/relay/media';
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
  createFrameWriter,
  type Frame,
  type FrameGrouping,
  type FrameOutcome,
  type FrameOutcomeKind,
  type FrameWriter,
  type FrameWriterOptions,
} from './frame-writer.js';
export { type MediaKind, type MediaTrackPropertiesOptions, mediaTrackProperties } from './media-track-properties.js';
