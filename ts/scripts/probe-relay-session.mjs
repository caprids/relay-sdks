// Opt-in live check of `connectRelay`: one connection per peer that publishes several namespaces and subscribes.
// Run after `pnpm build` and `pnpm --filter @caprid/relay-quic build:native`.
//
// Two peers, A and B, each open ONE connection. Each discovers the root, then publishes two namespaces, <root>/<name>
// (track `ticks`) and <root>/<name>-host (track `control`), writing one object per group every 200 ms on each. Each
// subscribes to both of the other's tracks, and A also to its own `ticks` (loopback).
//
//   RELAY_URL=https://ord.relay.mux.global RELAY_TOKEN=... RELAY_NAMESPACE_PREFIX=... node scripts/probe-relay-session.mjs
import { randomUUID } from 'node:crypto';
import { connectRelay } from '@caprid/relay/node';

const env = process.env;
const url = env.RELAY_URL;
if (!url) throw new Error('RELAY_URL is required');
const token = env.RELAY_TOKEN;
if (!token) throw new Error('RELAY_TOKEN is required');
const runMs = Number(env.PROBE_RUN_S ?? 6) * 1000;
const root = [
  ...(env.RELAY_NAMESPACE_PREFIX ?? 'relay-sdk-probe').split('/').filter(Boolean),
  `session-${randomUUID()}`,
];
const startMs = performance.now();
const at = () => Math.round(performance.now() - startMs);

async function openPeer(name) {
  const session = await connectRelay({ url, getToken: () => token });
  const peer = { name, session, discovery: [], sent: 0, timers: [] };
  void (async () => {
    try {
      for await (const event of session.namespaceEvents({ prefix: root })) {
        if (event.type !== 'loaded')
          peer.discovery.push(`${event.type} ${event.namespace.slice(root.length).join('/')}`);
      }
    } catch {}
  })();
  return peer;
}

function publish(peer, suffix, track) {
  const publication = peer.session.publish([...root, suffix]);
  const writer = publication.registerTrack({ track });
  peer.announced ??= [];
  peer.announced.push(publication.announced.then(() => at()));
  let n = 0;
  peer.timers.push(
    setInterval(() => {
      if (!writer.hasDemand) return;
      writer.writeText(`${suffix}:${n++}`).then(
        () => peer.sent++,
        () => {},
      );
    }, 200),
  );
  return publication;
}

function subscribe(peer, suffix, track, label) {
  const result = { label, objects: 0, first: undefined, error: undefined };
  peer.session.subscribe(
    { namespace: [...root, suffix], track },
    {
      onObject: (object) => {
        result.objects++;
        result.first ??= new TextDecoder().decode(object.payload);
      },
      onError: (error) => {
        result.error = `request error ${error.errorCode}`;
      },
    },
  );
  return result;
}

console.log(`root ${root.join('/')} on ${url}`);
const [a, b] = await Promise.all([openPeer('a'), openPeer('b')]);
await new Promise((resolve) => setTimeout(resolve, 300));
for (const peer of [a, b]) {
  publish(peer, peer.name, 'ticks');
  publish(peer, `${peer.name}-host`, 'control');
}
const announcedAt = await Promise.all([...a.announced, ...b.announced]);
console.log(`all four namespaces announced by ${Math.max(...announcedAt)} ms`);
const deadline = performance.now() + 5000;
while (performance.now() < deadline && !(a.discovery.length >= 2 && b.discovery.length >= 2)) {
  await new Promise((resolve) => setTimeout(resolve, 25));
}

const results = [
  subscribe(a, 'b', 'ticks', 'a ← b/ticks'),
  subscribe(a, 'b-host', 'control', 'a ← b-host/control'),
  subscribe(b, 'a', 'ticks', 'b ← a/ticks'),
  subscribe(b, 'a-host', 'control', 'b ← a-host/control'),
  subscribe(a, 'a', 'ticks', 'a ← a/ticks (loopback)'),
];
await new Promise((resolve) => setTimeout(resolve, runMs));
for (const peer of [a, b]) for (const timer of peer.timers) clearInterval(timer);
await Promise.all([a.session.close(), b.session.close()]);

console.log('\n=== summary ===');
for (const peer of [a, b])
  console.log(`[${peer.name}] sent ${peer.sent}; discovery saw: ${peer.discovery.join(', ') || 'nothing'}`);
for (const r of results)
  console.log(`${r.label}: objects ${r.objects}, first ${JSON.stringify(r.first)}, error ${r.error ?? 'none'}`);
process.exit(0);
