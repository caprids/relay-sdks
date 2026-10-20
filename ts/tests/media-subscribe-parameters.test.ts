import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import {
  ControlMessageDeframer,
  decodeControlMessage,
  encodeSubscribe,
} from '../packages/moq-core/src/protocol/control-messages.js';
import { mediaSubscribeParameters, mediaTrackProperties } from '../src/media.js';

const PINS = JSON.parse(readFileSync(new URL('../../spec/protocol.json', import.meta.url), 'utf8')) as {
  mediaSubscribeParameters: { subscriberPriority: { audio: number; video: number } };
};
const PRIORITY = PINS.mediaSubscribeParameters.subscriberPriority;
const DEFAULT_PUBLISHER_PRIORITY = 0x0e;

describe('mediaSubscribeParameters', () => {
  it('puts audio ahead of video with the subscriber priorities spec/protocol.json pins', () => {
    expect(PRIORITY).toEqual({ audio: 16, video: 96 });
    expect(mediaSubscribeParameters('audio')).toEqual({ subscriberPriority: PRIORITY.audio });
    expect(mediaSubscribeParameters('video')).toEqual({ subscriberPriority: PRIORITY.video });
  });

  it('matches the publisher priority mediaTrackProperties gives the same kind', () => {
    const publisherPriority = (properties: ReturnType<typeof mediaTrackProperties>) =>
      properties.find((property) => property.type === DEFAULT_PUBLISHER_PRIORITY)?.value;
    expect(mediaSubscribeParameters('audio').subscriberPriority).toBe(publisherPriority(mediaTrackProperties('audio')));
    expect(mediaSubscribeParameters('video').subscriberPriority).toBe(
      publisherPriority(mediaTrackProperties('video', { keyframeIntervalUs: 1_000_000 })),
    );
  });

  it('copies the overrides onto the result without changing them, and takes an explicit priority from 0 to 255', () => {
    const overrides = { locationFilter: { type: 'next-object' as const } };
    expect(mediaSubscribeParameters('video', overrides)).toEqual({
      locationFilter: { type: 'next-object' },
      subscriberPriority: 96,
    });
    expect(overrides).toEqual({ locationFilter: { type: 'next-object' } });
    expect(mediaSubscribeParameters('audio', { subscriberPriority: 0 })).toEqual({ subscriberPriority: 0 });
    expect(mediaSubscribeParameters('video', { subscriberPriority: 255 })).toEqual({ subscriberPriority: 255 });
    expect(mediaSubscribeParameters('audio', { subscriberPriority: undefined } as never)).toEqual({
      subscriberPriority: 16,
    });
  });

  it('returns a new object each time', () => {
    const first = mediaSubscribeParameters('audio');
    first.subscriberPriority = 1;
    expect(mediaSubscribeParameters('audio')).toEqual({ subscriberPriority: 16 });
  });

  it('is carried by SUBSCRIBE as SUBSCRIBER_PRIORITY', () => {
    const bytes = encodeSubscribe({
      requestId: 2,
      trackNamespace: ['room'],
      trackName: 'audio',
      parameters: mediaSubscribeParameters('audio'),
    });
    const [frame] = new ControlMessageDeframer().push(bytes);
    expect(decodeControlMessage(frame!)).toMatchObject({ kind: 'subscribe', parameters: { subscriberPriority: 16 } });
  });

  it('refuses a kind that is not a string with a TypeError and another string with a RangeError', () => {
    expect(() => mediaSubscribeParameters(1 as never)).toThrow(TypeError);
    expect(() => mediaSubscribeParameters('data' as never)).toThrow(RangeError);
  });

  it('refuses overrides that are not an object', () => {
    expect(() => mediaSubscribeParameters('audio', null as never)).toThrow(TypeError);
    expect(() => mediaSubscribeParameters('audio', 16 as never)).toThrow(TypeError);
  });

  it('refuses a subscriber priority that is not an integer from 0 to 255', () => {
    expect(() => mediaSubscribeParameters('audio', { subscriberPriority: '16' as never })).toThrow(TypeError);
    expect(() => mediaSubscribeParameters('audio', { subscriberPriority: true as never })).toThrow(TypeError);
    for (const value of [-1, 256, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(() => mediaSubscribeParameters('audio', { subscriberPriority: value })).toThrow(RangeError);
    }
  });
});
