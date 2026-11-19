// Opt-in live check of `connectSupervisedRelay`: one supervised connection per peer that publishes two roots under one
// generation, follows the other peer's roots, and moves everything together on a rotation.
// Run after `pnpm build` and `pnpm --filter @caprid/relay-quic build:native`.
//
// Two peers, A and B. Each publishes <root>/p/<name> (track `ticks`) and <root>/host/<name> (track `control`), writing
// one object per group every 200 ms on each, follows <root>/p with publishingRoots, and wants both of the other's
// tracks with `generations: true`. Halfway through, A rotates. The check: each peer holds one connection per
// generation, both of A's roots carry the same generation before and after, B keeps receiving A's tracks across the
// rotation and moves to A's new generation, and neither peer's roster lists itself.
//
//   RELAY_URL=https://ord.relay.mux.global RELAY_TOKEN=... RELAY_NAMESPACE_PREFIX=... \
//     node scripts/probe-supervised-relay.mjs
import { randomUUID } from 'node:crypto';
import { connectSupervisedRelay } from '@caprid/relay/node';

const env = process.env;
const url = env.RELAY_URL;
if (!url) throw new Error('RELAY_URL is required');
const token = env.RELAY_TOKEN;
if (!token) throw new Error('RELAY_TOKEN is required');
const runMs = Number(env.PROBE_RUN_S ?? 8) * 1000;
const root = [
  ...(env.RELAY_NAMESPACE_PREFIX ?? 'relay-sdk-probe').split('/').filter(Boolean),
  `supervised-${randomUUID()}`,
];
const startMs = performance.now();
const at = () => Math.round(performance.now() - startMs);

async function openPeer(name) {
  const peer = {
    name,
    connects: 0,
    generations: [],
    roster: new Set(),
    rosterEver: new Set(),
    timers: [],
    received: {},
  };
  peer.relay = await connectSupervisedRelay({
    url,
    getToken: () => {
      peer.connects++;
      return token;
    },
    onGeneration: ({ generation }) => peer.generations.push({ generation, at: at() }),
  });
  for (const [prefix, track] of [
    ['p', 'ticks'],
    ['host', 'control'],
  ]) {
    const publication = peer.relay.publish([...root, prefix, name]);
    const writer = publication.registerTrack({ track });
    peer[prefix] = publication;
    let n = 0;
    peer.timers.push(
      setInterval(() => {
        if (!writer.hasDemand) return;
        writer.writeText(`${name}:${n++}`).catch(() => {});
      }, 200),
    );
  }
  peer.relay.publishingRoots(
    { prefix: [...root, 'p'] },
    {
      onChanged({ present }) {
        peer.roster = new Set([...present.keys()]);
        for (const id of present.keys()) peer.rosterEver.add(id);
      },
    },
  );
  return peer;
}

function want(peer, other, prefix, track) {
  const result = { objects: 0, namespaces: new Set(), switched: 0, restarted: [], error: undefined };
  peer.received[`${prefix}/${other}`] = result;
  peer.relay.want(
    `${prefix}/${other}`,
    { namespace: [...root, prefix, other], track, generations: true },
    {
      onSubscribed: (_, { namespace }) => result.namespaces.add(namespace.at(-1)),
      onObject: () => result.objects++,
      onSwitched: ({ to }) => {
        result.switched++;
        result.namespaces.add(to.namespace.at(-1));
      },
      onRestarted: ({ reason }) => result.restarted.push(reason),
      onError: (error) => {
        result.error = String(error);
      },
    },
  );
}

console.log(`root ${root.join('/')} on ${url}`);
const [a, b] = await Promise.all([openPeer('a'), openPeer('b')]);
for (const [peer, other] of [
  [a, b],
  [b, a],
]) {
  want(peer, other.name, 'p', 'ticks');
  want(peer, other.name, 'host', 'control');
}
const before = { p: a.p.namespace?.at(-1), host: a.host.namespace?.at(-1) };
await new Promise((resolve) => setTimeout(resolve, runMs / 2));
const countsAtRotation = Object.fromEntries(Object.entries(b.received).map(([key, value]) => [key, value.objects]));
a.relay.rotate();
await new Promise((resolve) => setTimeout(resolve, runMs / 2));
const after = { p: a.p.namespace?.at(-1), host: a.host.namespace?.at(-1) };

for (const peer of [a, b]) for (const timer of peer.timers) clearInterval(timer);
await Promise.all([a.relay.close(), b.relay.close()]);

const report = {
  aGenerations: a.generations,
  aRootsBefore: before,
  aRootsAfter: after,
  aConnects: a.connects,
  bConnects: b.connects,
  aRoster: [...a.rosterEver],
  bRoster: [...b.rosterEver],
  bReceived: Object.fromEntries(
    Object.entries(b.received).map(([key, value]) => [
      key,
      {
        atRotation: countsAtRotation[key],
        total: value.objects,
        namespaces: [...value.namespaces],
        switched: value.switched,
        restarted: value.restarted,
        error: value.error,
      },
    ]),
  ),
  aReceived: Object.fromEntries(
    Object.entries(a.received).map(([key, value]) => [key, { total: value.objects, error: value.error }]),
  ),
};
console.log(JSON.stringify(report, null, 2));

const checks = {
  'A rotated to a new generation': a.generations.length === 2 && before.p !== after.p,
  "both of A's roots share the generation, before and after": before.p === before.host && after.p === after.host,
  'one connection per generation (A: 2, B: 1)': a.connects === 2 && b.connects === 1,
  "B followed A's ticks to the new generation": b.received['p/a'].namespaces.has(after.p),
  'B kept receiving after the rotation': Object.entries(b.received).every(
    ([key, value]) => value.objects > (countsAtRotation[key] ?? 0) + 5,
  ),
  'A kept receiving B': Object.values(a.received).every((value) => value.objects > 10),
  'no roster lists its own root': !a.rosterEver.has('a') && !b.rosterEver.has('b'),
  'each roster lists the other': a.rosterEver.has('b') && b.rosterEver.has('a'),
};
for (const [check, ok] of Object.entries(checks)) console.log(`${ok ? 'PASS' : 'FAIL'} ${check}`);
process.exitCode = Object.values(checks).every(Boolean) ? 0 : 1;
