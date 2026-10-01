import type { NativeQuicTransportFactory } from '@mux/relay-quic';
import { expectTypeOf, it } from 'vitest';

import type { ConnectRelayOptions } from '../src/index.js';

it("nativeQuicTransport's factory is one the SDK takes as createTransport", () => {
  expectTypeOf<NativeQuicTransportFactory>().toExtend<NonNullable<ConnectRelayOptions['createTransport']>>();
});
