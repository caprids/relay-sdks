import type { FallbackTransportFactory, WebSocketTransportFactory } from '@caprid/relay-websocket';
import { expectTypeOf, it } from 'vitest';

import type { ConnectRelayOptions } from '../src/index.js';

it("@caprid/relay-websocket's factories are ones the SDK takes as createTransport", () => {
  expectTypeOf<WebSocketTransportFactory>().toExtend<NonNullable<ConnectRelayOptions['createTransport']>>();
  expectTypeOf<FallbackTransportFactory>().toExtend<NonNullable<ConnectRelayOptions['createTransport']>>();
});
