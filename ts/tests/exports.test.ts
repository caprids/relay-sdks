import { describe, expect, it } from 'vitest';
import type { RelayPublisher, RelaySubscriber, RelaySubscriptionHandlers, RelayTrackWriter } from '../src/index.js';
import * as relay from '../src/index.js';

/**
 * The public vocabulary in spec/vocabulary.md. Each literal below must name every key of its type
 * and nothing else, so a rename in the source fails to compile here until the record is updated.
 */
describe('public vocabulary', () => {
  it('exports one entry point per connection role and nothing under the old names', () => {
    expect(Object.keys(relay).sort()).toEqual(['connectRelayPublisher', 'connectRelaySubscriber']);
    expect(relay).not.toHaveProperty('connectRelay');
  });

  it('spells subscription events after what happened, not after the protocol message', () => {
    const handlers = {
      onSubscribed: true,
      onError: true,
      onObject: true,
      onSubgroupEnd: true,
      onSubgroupReset: true,
      onPublisherDone: true,
      onUpdateOk: true,
      onStateNotify: true,
      onGoaway: true,
    } satisfies Record<keyof RelaySubscriptionHandlers, true>;
    expect(handlers).not.toHaveProperty('onOk');
    expect(handlers).not.toHaveProperty('onDone');
  });

  it('names the subscriber, the publisher, and the track writer by role and by demand', () => {
    const subscriber = { subscribe: true, trackSource: true, closed: true, close: true } satisfies Record<
      keyof RelaySubscriber,
      true
    >;
    const publisher = { registerTrack: true, closed: true, close: true } satisfies Record<keyof RelayPublisher, true>;
    const writer = {
      hasDemand: true,
      closed: true,
      demandChanges: true,
      openGroup: true,
      write: true,
      close: true,
    } satisfies Record<keyof RelayTrackWriter, true>;
    expect(subscriber).not.toHaveProperty('tracks');
    expect(publisher).not.toHaveProperty('publish');
    expect(writer).not.toHaveProperty('bound');
    expect(writer).not.toHaveProperty('bindings');
  });
});
