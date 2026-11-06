/**
 * Measuring hooks for the video player scenario, for the page and its worker alike: counts of the frames decoders
 * output and of their closes, the timestamps encoders output, and pixel reads.
 */

/** How many frames this realm's video decoders output, and how often they were closed. */
export interface DecodedFrameCounts {
  output: number;
  closed: number;
  /** Closes of a frame already closed. */
  closedAgain: number;
}

/** From now on, every `VideoDecoder` created in this realm counts the frames it outputs and each one's closes. */
export function countDecodedFrames(): DecodedFrameCounts {
  const counts: DecodedFrameCounts = { output: 0, closed: 0, closedAgain: 0 };
  const Native = globalThis.VideoDecoder;
  globalThis.VideoDecoder = class extends Native {
    constructor(init: VideoDecoderInit) {
      super({
        error: init.error,
        output(frame) {
          counts.output++;
          const close = frame.close.bind(frame);
          let closed = false;
          frame.close = () => {
            if (closed) counts.closedAgain++;
            else counts.closed++;
            closed = true;
            close();
          };
          init.output(frame);
        },
      });
    }
  };
  return counts;
}

/** From now on, every `VideoEncoder` created in this realm records the timestamp of each chunk it outputs, in order. */
export function recordEncodedTimestamps(): number[] {
  const timestamps: number[] = [];
  const Native = globalThis.VideoEncoder;
  globalThis.VideoEncoder = class extends Native {
    constructor(init: VideoEncoderInit) {
      super({
        error: init.error,
        output(chunk, metadata) {
          timestamps.push(chunk.timestamp);
          init.output(chunk, metadata);
        },
      });
    }
  };
  return timestamps;
}

/** A canvas whose pixels can be read: an `HTMLCanvasElement` or an `OffscreenCanvas` qualifies. */
export interface ReadableCanvas {
  getContext(contextId: '2d'): { getImageData(x: number, y: number, width: number, height: number): ImageData } | null;
}

/** The red, green and blue of one pixel. */
export function readPixel(canvas: ReadableCanvas, x: number, y: number): number[] {
  const context = canvas.getContext('2d');
  if (!context) throw new Error('The canvas has no 2D context');
  return [...context.getImageData(x, y, 1, 1).data.slice(0, 3)];
}
