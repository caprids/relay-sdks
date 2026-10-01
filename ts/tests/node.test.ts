import type { NativeQuicTransportOptions } from '@mux/relay-quic';
import { afterEach, beforeEach, describe, expect, expectTypeOf, it, type Mock, vi } from 'vitest';
import type { RelayTransportFactory } from '../src/index.js';
import type * as NodeEntry from '../src/node.js';
import { createRelayHub } from './helpers/relay-hub.js';

/**
 * `@mux/relay/node` against a stand-in for `@mux/relay-quic`, so `pnpm check` needs no native addon: the stand-in's
 * factories route to an in-memory relay. The real adapter is dialled in `conformance/relay-quic.conformance.js`.
 */
const quic = {
  /** What `nativeQuicTransport()` does: throw as the loader does for a missing binary, or for invalid options. */
  nativeQuicTransport: undefined as unknown as Mock<(options?: object) => Mock<RelayTransportFactory>>,
  /** Where the factories the stand-in returns send each dial. */
  route: undefined as unknown as RelayTransportFactory,
};

const url = 'https://relay.example.com/moq';
const namespace = ['testing', 'node'];
const cleanup: (() => void | Promise<void>)[] = [];

/** A fresh copy of the entry point, so the adapter it caches and the warning it gave start over. */
async function entry(): Promise<typeof NodeEntry> {
  vi.resetModules();
  return import('../src/node.js');
}

function hub() {
  const relay = createRelayHub();
  cleanup.push(() => relay.destroy());
  return relay;
}

beforeEach(() => {
  vi.doMock('@mux/relay-quic', () => ({
    nativeQuicTransport: (...args: [object?]) => quic.nativeQuicTransport(...args),
  }));
  quic.nativeQuicTransport = vi.fn(() => vi.fn<RelayTransportFactory>((url, protocols) => quic.route(url, protocols)));
});

afterEach(async () => {
  for (const dispose of cleanup.splice(0).reverse()) await dispose();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe('@mux/relay/node', () => {
  it('connects both roles through one shared native QUIC factory when no transport is given', async () => {
    const { connectRelayPublisher, connectRelaySubscriber } = await entry();
    const relay = hub();

    quic.route = relay.connectPublisher;
    const publisher = await connectRelayPublisher({ url, namespace });
    cleanup.push(() => publisher.close());
    quic.route = relay.connectSubscriber;
    const subscriber = await connectRelaySubscriber({ url });
    cleanup.push(() => subscriber.close());

    expect(quic.nativeQuicTransport).toHaveBeenCalledOnce();
    expect(quic.nativeQuicTransport).toHaveBeenCalledWith();
    const shared = quic.nativeQuicTransport.mock.results[0]!.value;
    expect(shared).toHaveBeenCalledTimes(2);
    expect(shared).toHaveBeenNthCalledWith(1, url, ['moqt-20']);
  });

  it('gives a connection with trust options a factory of its own', async () => {
    const { connectRelaySubscriber } = await entry();
    quic.route = hub().connectSubscriber;
    const ca = '-----BEGIN CERTIFICATE-----';

    const subscriber = await connectRelaySubscriber({ url, ca });
    cleanup.push(() => subscriber.close());

    expect(quic.nativeQuicTransport).toHaveBeenNthCalledWith(1);
    expect(quic.nativeQuicTransport).toHaveBeenNthCalledWith(2, { ca });
    expect(quic.nativeQuicTransport.mock.results[0]!.value).not.toHaveBeenCalled();
    expect(quic.nativeQuicTransport.mock.results[1]!.value).toHaveBeenCalledOnce();
  });

  it("uses the caller's createTransport and leaves the adapter unloaded", async () => {
    const { connectRelaySubscriber } = await entry();
    const createTransport = vi.fn(hub().connectSubscriber);

    const subscriber = await connectRelaySubscriber({ url, createTransport });
    cleanup.push(() => subscriber.close());

    expect(createTransport).toHaveBeenCalledOnce();
    expect(quic.nativeQuicTransport).not.toHaveBeenCalled();
  });

  it('refuses trust options beside a createTransport they would not reach', async () => {
    const { connectRelaySubscriber } = await entry();
    const createTransport = vi.fn<RelayTransportFactory>();
    const options = { url, createTransport, ca: 'pem' } as unknown as Parameters<typeof connectRelaySubscriber>[0];

    await expect(connectRelaySubscriber(options)).rejects.toThrow(TypeError);
    expect(createTransport).not.toHaveBeenCalled();
  });

  it('fails with TRANSPORT_UNAVAILABLE, naming the adapter, when @mux/relay-quic is not installed', async () => {
    vi.doMock('@mux/relay-quic', () => {
      throw new Error("Cannot find package '@mux/relay-quic'");
    });
    const { connectRelaySubscriber, RelayError } = await entry();

    const failure = await connectRelaySubscriber({ url }).catch((error: unknown) => error);

    expect(failure).toBeInstanceOf(RelayError);
    expect(failure).toMatchObject({ code: 'TRANSPORT_UNAVAILABLE', phase: 'connecting' });
    expect((failure as Error).message).toContain('@mux/relay-quic');
    expect((failure as Error).cause).toBeInstanceOf(Error);
  });

  it('fails with TRANSPORT_UNAVAILABLE when the native binary is missing, and tries again on the next call', async () => {
    const { connectRelaySubscriber } = await entry();
    const missing = new Error('@mux/relay-quic has no native binary for this platform');
    quic.nativeQuicTransport.mockImplementationOnce(() => {
      throw missing;
    });

    await expect(connectRelaySubscriber({ url })).rejects.toMatchObject({
      code: 'TRANSPORT_UNAVAILABLE',
      cause: missing,
    });

    quic.route = hub().connectSubscriber;
    const subscriber = await connectRelaySubscriber({ url });
    cleanup.push(() => subscriber.close());
    expect(quic.nativeQuicTransport).toHaveBeenCalledTimes(2);
  });

  it('checks its arguments and signal before reporting a missing adapter, as the browser default does', async () => {
    vi.doMock('@mux/relay-quic', () => {
      throw new Error("Cannot find package '@mux/relay-quic'");
    });
    const { connectRelaySubscriber } = await entry();

    await expect(connectRelaySubscriber({ url: 'not a url' })).rejects.toThrow(TypeError);
    const reason = new Error('cancelled');
    await expect(connectRelaySubscriber({ url, signal: AbortSignal.abort(reason) })).rejects.toBe(reason);
    await expect(connectRelaySubscriber({ url })).rejects.toMatchObject({ code: 'TRANSPORT_UNAVAILABLE' });
  });

  it('applies the signal and the deadline while the adapter is still loading', async () => {
    vi.doMock('@mux/relay-quic', () => new Promise(() => {}));
    const { connectRelaySubscriber } = await entry();

    const owner = new AbortController();
    const aborting = connectRelaySubscriber({ url, signal: owner.signal });
    const reason = new Error('cancelled');
    owner.abort(reason);
    await expect(aborting).rejects.toBe(reason);

    await expect(connectRelaySubscriber({ url, connectTimeoutMs: 50 })).rejects.toMatchObject({
      code: 'CONNECTION_TIMEOUT',
    });
  });

  it('loads the adapter once for connections that start together', async () => {
    const { connectRelayPublisher, connectRelaySubscriber } = await entry();
    const relay = hub();
    let roles = 0;
    quic.route = (url, protocols) => (roles++ === 0 ? relay.connectPublisher : relay.connectSubscriber)(url, protocols);

    const [publisher, subscriber] = await Promise.all([
      connectRelayPublisher({ url, namespace }),
      connectRelaySubscriber({ url }),
    ]);
    cleanup.push(
      () => publisher.close(),
      () => subscriber.close(),
    );

    expect(quic.nativeQuicTransport).toHaveBeenCalledOnce();
  });

  it('passes an invalid trust option through as the adapter throws it', async () => {
    const { connectRelaySubscriber } = await entry();
    const invalid = new TypeError('unsupported certificate hash algorithm sha-1');
    quic.nativeQuicTransport.mockImplementation((options) => {
      if (options) throw invalid;
      return vi.fn<RelayTransportFactory>();
    });

    await expect(
      connectRelaySubscriber({ url, serverCertificateHashes: [{ algorithm: 'sha-256', value: new Uint8Array(32) }] }),
    ).rejects.toBe(invalid);
  });

  describe('when the runtime has its own WebTransport', () => {
    function stubWebTransport({ negotiates }: { negotiates: boolean }) {
      class WebTransport {}
      if (negotiates) Object.defineProperty(WebTransport.prototype, 'protocol', { get: () => '' });
      vi.stubGlobal('WebTransport', WebTransport);
    }

    it('warns once per process that the import can go, and still connects over native QUIC', async () => {
      stubWebTransport({ negotiates: true });
      const emitWarning = vi.spyOn(process, 'emitWarning').mockImplementation(() => {});
      const { connectRelaySubscriber } = await entry();
      quic.route = hub().connectSubscriber;

      for (let i = 0; i < 2; i++) {
        const subscriber = await connectRelaySubscriber({ url });
        cleanup.push(() => subscriber.close());
      }

      expect(emitWarning).toHaveBeenCalledOnce();
      expect(emitWarning).toHaveBeenCalledWith(expect.stringContaining('native WebTransport detected'), {
        type: 'DeprecationWarning',
        code: 'MUX_RELAY_NODE_NATIVE_WEBTRANSPORT',
      });
      expect(quic.nativeQuicTransport.mock.results[0]!.value).toHaveBeenCalledTimes(2);
    });

    it('does not warn about a WebTransport that cannot negotiate protocols, as in Deno', async () => {
      stubWebTransport({ negotiates: false });
      const emitWarning = vi.spyOn(process, 'emitWarning').mockImplementation(() => {});
      const { connectRelaySubscriber } = await entry();
      quic.route = hub().connectSubscriber;

      const subscriber = await connectRelaySubscriber({ url });
      cleanup.push(() => subscriber.close());

      expect(emitWarning).not.toHaveBeenCalled();
    });
  });
});

describe('@mux/relay/node types', () => {
  it("declares the trust options as @mux/relay-quic's own, without importing them", () => {
    expectTypeOf<NodeEntry.RelayNodeTrustOptions>().toEqualTypeOf<NativeQuicTransportOptions>();
  });

  it('takes the trust options, or a createTransport, but not both', () => {
    type Options = Parameters<typeof NodeEntry.connectRelayPublisher>[0];
    const createTransport = {} as RelayTransportFactory;
    expectTypeOf({ url, namespace, ca: 'pem' }).toExtend<Options>();
    expectTypeOf({
      url,
      namespace,
      serverCertificateHashes: [{ algorithm: 'sha-256' as const, value: new Uint8Array(32) }],
    }).toExtend<Options>();
    expectTypeOf({ url, namespace, createTransport }).toExtend<Options>();
    expectTypeOf({ url, namespace, createTransport, ca: 'pem' }).not.toExtend<Options>();
    expectTypeOf({ url, ca: 'pem', requestTimeoutMs: 5 }).toExtend<
      Parameters<typeof NodeEntry.connectRelaySubscriber>[0]
    >();
  });
});
