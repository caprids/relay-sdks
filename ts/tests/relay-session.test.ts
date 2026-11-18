import { afterEach, describe, expect, it, vi } from 'vitest';
import { StreamReader } from '../packages/moq-core/src/protocol/bytes.js';
import {
  ControlMessageDeframer,
  decodeControlMessage,
  encodeGoaway,
  encodeSetup,
  encodeSubscribeOk,
  SETUP_OPTION,
} from '../packages/moq-core/src/protocol/control-messages.js';
import { STREAM_TYPE } from '../packages/moq-core/src/protocol/object-stream.js';
import type { MoqtTransport } from '../packages/moq-core/src/protocol/session.js';
import { rawSubscribe, solicitNamespace } from '../packages/moq-core/src/tests/helpers/raw-peer.js';
import { createTransportPair } from '../packages/moq-core/src/tests/helpers/transport-pair.js';
import { connectRelay, RelayError, type RelaySession } from '../src/index.js';
import { encodeSubgroup } from './helpers/relay.js';

const url = 'https://relay.example.com/room';
const OWN = ['room', 'p', 'alice'];
const HOST = ['room', 'host'];
const OTHER = ['room', 'p', 'bob'];
const cleanup: (() => void | Promise<void>)[] = [];

afterEach(async () => {
  for (const dispose of cleanup.splice(0).reverse()) await dispose();
});

/**
 * A scripted relay on one transport: it sends SETUP, records the client's SETUP options, counts the client's subgroup
 * streams, and answers each client SUBSCRIBE with SUBSCRIBE_OK and one object.
 */
function scriptedRelay(server: MoqtTransport) {
  const seen = { setupOptions: [] as { type: number }[], subgroupStreams: 0, subscribes: [] as string[] };
  void (async () => {
    const reader = server.incomingUnidirectionalStreams.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) return;
      void (async () => {
        const stream = new StreamReader(value);
        const type = await stream.readVarint();
        if (type === STREAM_TYPE.SETUP) {
          const high = await stream.readUint8();
          const low = await stream.readUint8();
          const message = decodeControlMessage({ type, body: await stream.readBytes(high * 256 + low) });
          if (message.kind === 'setup') seen.setupOptions = message.options;
          return;
        }
        seen.subgroupStreams += 1;
        while (!(await stream.atEnd())) await stream.readUint8();
      })().catch(() => {});
    }
  })();
  void (async () => {
    const reader = server.incomingBidirectionalStreams.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) return;
      void (async () => {
        const { value: bytes } = await value.readable.getReader().read();
        const frame = bytes && new ControlMessageDeframer().push(bytes)[0];
        const message = frame && decodeControlMessage(frame);
        if (message?.kind !== 'subscribe') return;
        seen.subscribes.push(`${message.trackNamespace.join('/')}:${message.trackName}`);
        const alias = 100 + message.requestId;
        await value.writable.getWriter().write(encodeSubscribeOk(alias));
        const data = (await server.createUnidirectionalStream()).getWriter();
        await data.write(encodeSubgroup(alias, 0, new TextEncoder().encode('hello')));
        await data.close();
      })().catch(() => {});
    }
  })();
  void (async () => {
    const control = (await server.createUnidirectionalStream()).getWriter();
    await control.write(encodeSetup([{ type: SETUP_OPTION.SOLICIT, value: 1 }]));
  })();
  return seen;
}

async function connect(): Promise<{
  session: RelaySession;
  server: MoqtTransport;
  seen: ReturnType<typeof scriptedRelay>;
}> {
  const pair = createTransportPair();
  const seen = scriptedRelay(pair.server);
  const session = await connectRelay({
    url,
    createTransport: () => ({ transport: pair.client, ready: Promise.resolve() }),
  });
  cleanup.push(() => session.close());
  return { session, server: pair.server, seen };
}

describe('connectRelay', () => {
  it('publishes and subscribes on one connection with one SETUP', async () => {
    const { session, server, seen } = await connect();
    const publication = session.publish(OWN);
    const writer = publication.registerTrack({ track: 'video' });
    const solicitation = await solicitNamespace(server, ['room'], 1);
    await publication.announced;
    expect(solicitation.received.map((m) => m.kind)).toEqual(['request-ok', 'namespace']);

    const upstream = await rawSubscribe(server, OWN, 'video', 3);
    await vi.waitFor(() => expect(upstream.received.map((m) => m.kind)).toEqual(['subscribe-ok']));
    await writer.write(new Uint8Array([1, 2, 3]));
    await vi.waitFor(() => expect(seen.subgroupStreams).toBe(1));

    const payloads: string[] = [];
    session.subscribe(
      { namespace: OTHER, track: 'video' },
      { onObject: (object) => payloads.push(new TextDecoder().decode(object.payload)) },
    );
    await vi.waitFor(() => expect(payloads).toEqual(['hello']));
    expect(seen.subscribes).toEqual(['room/p/bob:video']);
    expect(seen.setupOptions.map(({ type }) => type)).toEqual(
      expect.arrayContaining([SETUP_OPTION.SOLICIT, SETUP_OPTION.ACTIVE_COUNT]),
    );
  });

  it('announces several namespaces and closes one publication without the other', async () => {
    const { session, server } = await connect();
    const own = session.publish(OWN);
    const host = session.publish(HOST);
    own.registerTrack({ track: 'video' });
    host.registerTrack({ track: 'control' });
    expect(() => session.publish(OWN)).toThrow(RangeError);
    const solicitation = await solicitNamespace(server, ['room'], 1);
    await Promise.all([own.announced, host.announced]);

    await host.close();
    await expect(host.closed).resolves.toEqual({});
    await vi.waitFor(() =>
      expect(solicitation.received.at(-1)).toMatchObject({ kind: 'namespace-done', trackNamespaceSuffix: ['host'] }),
    );
    expect(() => host.registerTrack({ track: 'other' })).toThrow(RelayError);
    expect(() => session.publish(HOST)).toThrow(RangeError);
    own.registerTrack({ track: 'audio' });
  });

  it('ends only the publication whose namespace the relay stops soliciting', async () => {
    const { session, server } = await connect();
    const own = session.publish(OWN);
    const host = session.publish(HOST);
    own.registerTrack({ track: 'video' });
    const control = host.registerTrack({ track: 'control' });
    await solicitNamespace(server, ['room', 'p'], 1);
    const hosts = await solicitNamespace(server, ['room', 'host'], 3);
    await Promise.all([own.announced, host.announced]);

    void hosts.reset();
    const { error } = await host.closed;
    expect(error).toBeInstanceOf(RelayError);
    expect((error as RelayError).code).toBe('NAMESPACE_ENDED');
    await expect(control.write(new Uint8Array([1]))).rejects.toBe(error);
    expect(() => host.registerTrack({ track: 'another' })).toThrow(error as RelayError);

    // The connection and the other publication carry on.
    let settled = false;
    void session.closed.then(() => {
      settled = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(settled).toBe(false);
    expect(() => own.registerTrack({ track: 'audio' })).not.toThrow();
  });

  it('retracts every announce on close and ends the connection', async () => {
    const { session, server } = await connect();
    const own = session.publish(OWN);
    own.registerTrack({ track: 'video' });
    const solicitation = await solicitNamespace(server, [], 1);
    await own.announced;

    await session.close();
    await expect(session.closed).resolves.toEqual({});
    await expect(own.closed).resolves.toEqual({});
    await vi.waitFor(() => expect(solicitation.received.map((m) => m.kind)).toContain('namespace-done'));
    expect(() => session.publish(HOST)).toThrow(RelayError);
  });

  it('checks the namespace before connection state', async () => {
    const { session } = await connect();
    expect(() => session.publish(new Array(33).fill('field'))).toThrow(RangeError);
  });
  it('fails an open group with the namespace loss, not a subscription change', async () => {
    const { session, server } = await connect();
    const host = session.publish(HOST);
    const control = host.registerTrack({ track: 'control' });
    const hosts = await solicitNamespace(server, ['room', 'host'], 1);
    await host.announced;
    const upstream = await rawSubscribe(server, HOST, 'control', 3);
    await vi.waitFor(() => expect(upstream.received.map((m) => m.kind)).toEqual(['subscribe-ok']));
    const group = await control.openGroup({ payload: new Uint8Array([1]) });

    void hosts.reset();
    const { error } = await host.closed;
    expect((error as RelayError).code).toBe('NAMESPACE_ENDED');
    await expect(group.write(new Uint8Array([2]))).rejects.toBe(error);
  });

  it('refuses a publication once closing has begun', async () => {
    const { session, server } = await connect();
    const own = session.publish(OWN);
    own.registerTrack({ track: 'video' });
    await solicitNamespace(server, [], 1);
    await own.announced;

    const closing = session.close();
    expect(() => session.publish(HOST)).toThrow(RelayError);
    await closing;
  });
  it('ends the connection on a GOAWAY the relay sends on its solicitation', async () => {
    const { session, server } = await connect();
    const own = session.publish(OWN);
    own.registerTrack({ track: 'video' });
    const solicitation = await solicitNamespace(server, [], 1);
    await own.announced;

    await solicitation.send(encodeGoaway(1000));
    const { error } = await session.closed;
    expect(error).toMatchObject({ code: 'GOAWAY' });
    await expect(own.closed).resolves.toMatchObject({ error });
  });
});
