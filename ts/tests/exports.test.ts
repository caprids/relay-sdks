import { describe, expect, it } from 'vitest';
import type {
  ConnectRelayOptions,
  ConnectRelayPublisherOptions,
  ConnectRelaySubscriberOptions,
  RelayPublisher,
  RelaySubscriber,
  RelaySubscriptionHandlers,
  RelayTrackWriter,
} from '../src/index.js';
import * as relay from '../src/index.js';

/**
 * The public vocabulary in spec/vocabulary.md. Each literal below must name every key of its type
 * and nothing else, so a rename in the source fails to compile here until the record is updated.
 */
describe('public vocabulary', () => {
  it('exports one entry point per connection role and nothing under the old names', () => {
    expect(Object.keys(relay).sort()).toEqual([
      'RelayError',
      'SubscriptionConnectionClosedError',
      'SubscriptionOverflowError',
      'SubscriptionRequestError',
      'connectRelayPublisher',
      'connectRelaySubscriber',
      'connectSupervisedPublisher',
      'connectSupervisedSubscriber',
      'readJSON',
      'readText',
    ]);
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
    const subscriber = {
      subscribeEvents: true,
      subscribeSwitchable: true,
      namespaceEvents: true,
      subscribe: true,
      trackSource: true,
      closed: true,
      expiresAt: true,
      close: true,
    } satisfies Record<keyof RelaySubscriber, true>;
    const publisher = {
      registerTrack: true,
      announced: true,
      closed: true,
      expiresAt: true,
      close: true,
    } satisfies Record<keyof RelayPublisher, true>;
    const writer = {
      hasDemand: true,
      closed: true,
      demandChanges: true,
      openGroup: true,
      write: true,
      writeText: true,
      writeJSON: true,
      close: true,
    } satisfies Record<keyof RelayTrackWriter, true>;
    expect(subscriber).not.toHaveProperty('tracks');
    expect(publisher).not.toHaveProperty('publish');
    expect(writer).not.toHaveProperty('bound');
    expect(writer).not.toHaveProperty('bindings');
  });

  it("shares connection options between roles and adds each role's own on top", () => {
    const shared = {
      url: true,
      getToken: true,
      signal: true,
      connectTimeoutMs: true,
      createTransport: true,
    } satisfies Record<keyof ConnectRelayOptions, true>;
    const subscriber = { ...shared, requestTimeoutMs: true } satisfies Record<
      keyof ConnectRelaySubscriberOptions,
      true
    >;
    const publisher = { ...shared, namespace: true } satisfies Record<keyof ConnectRelayPublisherOptions, true>;
    expect(shared).not.toHaveProperty('requestTimeoutMs');
    expect(publisher).not.toHaveProperty('requestTimeoutMs');
    expect(subscriber).not.toHaveProperty('namespace');
  });
});
