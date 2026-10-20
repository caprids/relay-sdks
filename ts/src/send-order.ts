/**
 * A track's data streams' WebTransport send order, from its Default Publisher Priority (0x0E), so the publisher's own
 * transport sends a more urgent track first when the uplink is congested (spec/behavior.md, "Send order").
 *
 * Draft-22's priority is lower-is-more-urgent from 0 to 255 (§5.1); WebTransport's send order is higher-first, with 0
 * for every stream opened without one, the control and request streams among them. A declared priority p maps to
 * `-1 - p`, from -1 to -256: below those streams, which a congested media track must never hold up, and in the
 * draft's order among themselves. A track without the property gets no send order, so it goes with the control
 * streams: a catalog or a timeline ahead of the media it describes.
 */
import type { KeyValuePair, MoqtSendStreamOptions } from '@mux/moq-core';
import { DEFAULT_PUBLISHER_PRIORITY } from './validation.js';

/** The send options for each data stream of a track with these properties; the first integer 0x0E counts. */
export function trackSendOptions(properties: readonly KeyValuePair[]): MoqtSendStreamOptions | undefined {
  for (const property of properties) {
    if (property.type === DEFAULT_PUBLISHER_PRIORITY && typeof property.value === 'number') {
      return { sendOrder: -1 - property.value };
    }
  }
  return undefined;
}
