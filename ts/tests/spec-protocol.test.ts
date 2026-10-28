/** The core's constants must match the repository-wide pins in ../../spec/protocol.json. */
import { readFile } from 'node:fs/promises';

import { describe, expect, it } from 'vitest';

import {
  MAX_NAMESPACE_FIELDS,
  MAX_OBJECT_PAYLOAD_LENGTH,
  MAX_OBJECT_PROPERTIES_LENGTH,
  MAX_TRACK_PROPERTIES_LENGTH,
  MOQT_PROTOCOL_ID,
  MOQT_PROTOCOL_IDS,
} from '../packages/moq-core/src/index.js';
import { DEFAULT_ENDPOINT } from '../src/room-endpoint.js';
import {
  DEFAULT_PUBLISHER_GROUP_ORDER,
  DEFAULT_PUBLISHER_GROUP_ORDERS,
  DEFAULT_PUBLISHER_PRIORITY,
  MAX_DEFAULT_PUBLISHER_PRIORITY,
  MAX_PROPERTY_VALUE_LENGTH,
  MAX_RELAY_OBJECT_PROPERTIES_LENGTH,
  MAX_RELAY_TRACK_PROPERTIES,
} from '../src/validation.js';

interface ProtocolSpec {
  moqt: { document: string; protocolId: string; protocolIds: string[]; fallbackDocument: string };
  relay: { defaultEndpoint: string; limits: { maxObjectPropertiesBytes: number; maxTrackProperties: number } };
  limits: {
    maxObjectPayloadBytes: number;
    maxObjectPropertiesBytes: number;
    maxTrackPropertiesBytes: number;
    maxPropertyValueBytes: number;
    maxNamespaceFields: number;
  };
  trackProperties: {
    defaultPublisherPriority: { type: number; max: number };
    defaultPublisherGroupOrder: { type: number; values: number[] };
  };
}

const spec = JSON.parse(await readFile(new URL('../../spec/protocol.json', import.meta.url), 'utf8')) as ProtocolSpec;

describe('spec/protocol.json', () => {
  it('pins the hosted Relay endpoint roomEndpoint roots by default', () => {
    expect(DEFAULT_ENDPOINT).toBe(spec.relay.defaultEndpoint);
  });

  it('pins the MOQT draft the core implements', () => {
    expect(MOQT_PROTOCOL_ID).toBe(spec.moqt.protocolId);
  });

  it('pins the protocols a client offers, the default first', () => {
    expect([...MOQT_PROTOCOL_IDS]).toEqual(spec.moqt.protocolIds);
    expect(MOQT_PROTOCOL_IDS[0]).toBe(MOQT_PROTOCOL_ID);
    expect(spec.moqt.fallbackDocument).toBe(`draft-ietf-moq-transport-${MOQT_PROTOCOL_IDS[1].slice('moqt-'.length)}`);
  });

  it('names the draft whose number the wire identifier carries', () => {
    // draft-ietf-moq-transport-22 §6.2: the identifier is "moqt-" followed by the draft number.
    expect(spec.moqt.document).toBe(`draft-ietf-moq-transport-${MOQT_PROTOCOL_ID.slice('moqt-'.length)}`);
  });

  it('pins the namespace field limit', () => {
    expect(MAX_NAMESPACE_FIELDS).toBe(spec.limits.maxNamespaceFields);
  });

  it('pins the object payload limit', () => {
    expect(MAX_OBJECT_PAYLOAD_LENGTH).toBe(spec.limits.maxObjectPayloadBytes);
  });

  it('pins the encoded object properties limit', () => {
    expect(MAX_OBJECT_PROPERTIES_LENGTH).toBe(spec.limits.maxObjectPropertiesBytes);
  });

  it('pins the track property block SUBSCRIBE_OK has room for', () => {
    expect(MAX_TRACK_PROPERTIES_LENGTH).toBe(spec.limits.maxTrackPropertiesBytes);
  });

  it('pins the relay limit on the number of track properties, which the SDK checks at registration', () => {
    expect(MAX_RELAY_TRACK_PROPERTIES).toBe(spec.relay.limits.maxTrackProperties);
  });

  it('pins the draft-20 track property values the SDK checks at registration', () => {
    expect(spec.trackProperties.defaultPublisherPriority).toEqual({
      type: DEFAULT_PUBLISHER_PRIORITY,
      max: MAX_DEFAULT_PUBLISHER_PRIORITY,
    });
    expect(spec.trackProperties.defaultPublisherGroupOrder).toEqual({
      type: DEFAULT_PUBLISHER_GROUP_ORDER,
      values: DEFAULT_PUBLISHER_GROUP_ORDERS,
    });
  });

  it('pins the relay limit on an object property block, which the SDK checks on write', () => {
    expect(MAX_RELAY_OBJECT_PROPERTIES_LENGTH).toBe(spec.relay.limits.maxObjectPropertiesBytes);
  });

  it('pins the property value length the SDK checks before the core encodes it', () => {
    // The core keeps its copy of this bound private; the SDK's validation module holds the one the pin checks.
    expect(MAX_PROPERTY_VALUE_LENGTH).toBe(spec.limits.maxPropertyValueBytes);
  });
});
