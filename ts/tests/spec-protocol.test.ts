/** The core's constants must match the repository-wide pins in ../../spec/protocol.json. */
import { readFile } from 'node:fs/promises';

import { describe, expect, it } from 'vitest';

import {
  MAX_NAMESPACE_FIELDS,
  MAX_OBJECT_PAYLOAD_LENGTH,
  MAX_OBJECT_PROPERTIES_LENGTH,
  MOQT_PROTOCOL_ID,
} from '../packages/moq-core/src/index.js';
import { MAX_PROPERTY_VALUE_LENGTH, MAX_RELAY_OBJECT_PROPERTIES_LENGTH } from '../src/validation.js';

interface ProtocolSpec {
  moqt: { document: string; protocolId: string };
  relay: { limits: { maxObjectPropertiesBytes: number } };
  limits: {
    maxObjectPayloadBytes: number;
    maxObjectPropertiesBytes: number;
    maxPropertyValueBytes: number;
    maxNamespaceFields: number;
  };
}

const spec = JSON.parse(await readFile(new URL('../../spec/protocol.json', import.meta.url), 'utf8')) as ProtocolSpec;

describe('spec/protocol.json', () => {
  it('pins the MOQT draft the core implements', () => {
    expect(MOQT_PROTOCOL_ID).toBe(spec.moqt.protocolId);
  });

  it('names the draft whose number the wire identifier carries', () => {
    // draft-ietf-moq-transport-20 §1.4: the identifier is "moqt-" followed by the draft number.
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

  it('pins the relay limit on an object property block, which the SDK checks on write', () => {
    expect(MAX_RELAY_OBJECT_PROPERTIES_LENGTH).toBe(spec.relay.limits.maxObjectPropertiesBytes);
  });

  it('pins the property value length the SDK checks before the core encodes it', () => {
    // The core keeps its copy of this bound private; the SDK's validation module holds the one the pin checks.
    expect(MAX_PROPERTY_VALUE_LENGTH).toBe(spec.limits.maxPropertyValueBytes);
  });
});
