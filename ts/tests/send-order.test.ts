/**
 * A track's data streams' send order from its Default Publisher Priority (spec/behavior.md, "Send order";
 * spec/transport-adapter.md TA-28).
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { MoqtSendStreamOptions } from '../packages/moq-core/src/index.js';
import { connectRelayPublisher, connectRelaySubscriber, type MoqtObject } from '../src/index.js';
import { mediaTrackProperties } from '../src/media.js';
import { trackSendOptions } from '../src/send-order.js';
import { trackPropertyPairs } from '../src/validation.js';
import { createRelayHub } from './helpers/relay-hub.js';

const DEFAULT_PUBLISHER_PRIORITY = 0x0e;
const url = 'https://relay.example.com/moq';
const namespace = ['testing', 'send-order'];
const cleanup: (() => void | Promise<void>)[] = [];

afterEach(async () => {
  for (const dispose of cleanup.splice(0).reverse()) await dispose();
});

describe('trackSendOptions', () => {
  it('maps a priority p to the send order -1 - p, below the default 0 and in the draft order', () => {
    expect(trackSendOptions([{ type: DEFAULT_PUBLISHER_PRIORITY, value: 0 }])).toEqual({ sendOrder: -1 });
    expect(trackSendOptions([{ type: DEFAULT_PUBLISHER_PRIORITY, value: 128 }])).toEqual({ sendOrder: -129 });
    expect(trackSendOptions([{ type: DEFAULT_PUBLISHER_PRIORITY, value: 255 }])).toEqual({ sendOrder: -256 });
  });

  it('gives audio a higher send order than video with the media defaults', () => {
    const sendOrder = (properties: ReturnType<typeof mediaTrackProperties>) =>
      trackSendOptions(trackPropertyPairs(properties))?.sendOrder;
    const audio = sendOrder(mediaTrackProperties('audio'));
    const video = sendOrder(mediaTrackProperties('video', { keyframeIntervalUs: 1_000_000 }));
    expect([audio, video]).toEqual([-17, -97]);
  });

  it('gives a track without the property no send order', () => {
    expect(trackSendOptions([])).toBeUndefined();
    expect(trackSendOptions([{ type: 0x06, value: 500 }])).toBeUndefined();
  });

  it('takes the first integer value', () => {
    const bytes = { type: DEFAULT_PUBLISHER_PRIORITY, value: new Uint8Array([1]) };
    const first = { type: DEFAULT_PUBLISHER_PRIORITY, value: 16 };
    const second = { type: DEFAULT_PUBLISHER_PRIORITY, value: 96 };
    expect(trackSendOptions([bytes, first, second])).toEqual({ sendOrder: -17 });
  });
});

describe('the publisher', () => {
  it("opens each data stream with its track's send order, and other streams with none", async () => {
    const hub = createRelayHub();
    cleanup.push(() => hub.destroy());
    const opened: (MoqtSendStreamOptions | undefined)[] = [];
    const publisher = await connectRelayPublisher({
      url,
      namespace,
      createTransport: (address, protocols) => {
        const connection = hub.connectPublisher(address, protocols);
        const create = connection.transport.createUnidirectionalStream.bind(connection.transport);
        connection.transport.createUnidirectionalStream = (options) => {
          opened.push(options);
          return create(options);
        };
        return connection;
      },
    });
    const subscriber = await connectRelaySubscriber({ url, createTransport: hub.connectSubscriber });
    cleanup.push(() => subscriber.close());
    cleanup.push(() => publisher.close());
    expect(opened).toEqual([undefined]);

    const tracks = {
      audio: publisher.registerTrack({ track: 'audio', trackProperties: mediaTrackProperties('audio') }),
      video: publisher.registerTrack({
        track: 'video',
        trackProperties: mediaTrackProperties('video', { keyframeIntervalUs: 1_000_000 }),
      }),
      catalog: publisher.registerTrack({ track: 'catalog' }),
    };
    for (const [track, writer] of Object.entries(tracks)) {
      const objects: MoqtObject[] = [];
      subscriber.subscribe({ namespace, track }, { onObject: (object) => objects.push(object) });
      await writer.writeText(track);
      await vi.waitFor(() => expect(objects).toHaveLength(1));
    }
    expect(opened).toEqual([undefined, { sendOrder: -17 }, { sendOrder: -97 }, undefined]);
  });
});
