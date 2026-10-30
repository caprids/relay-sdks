import { describe, expect, it } from 'vitest';

import { nativeQuicTransport } from '../index.js';

describe('nativeQuicTransport options', () => {
  // These throw before the addon loads, so they need no native build; the conformance suite checks the values that
  // pass reach the wire (`conformance/relay-quic.conformance.js`).
  it.each([0, -1, 65_536, 1.5, Number.NaN, Number.POSITIVE_INFINITY])(
    'throws a RangeError for anticipatedConcurrentIncomingUnidirectionalStreams %s',
    (value) => {
      expect(() => nativeQuicTransport({ anticipatedConcurrentIncomingUnidirectionalStreams: value })).toThrow(
        RangeError,
      );
    },
  );
});
