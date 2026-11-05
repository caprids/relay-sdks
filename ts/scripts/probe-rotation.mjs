// Opt-in live check of what a generation-following viewer sees across supervised publisher rotations
// (spec/behavior.md, "Rotation"; issue #444), in Node over native QUIC. Run after `pnpm build` and
// `pnpm --filter @caprid/relay-quic build:native`. One process publishes and watches, so send and arrival times share a
// clock.
//
// A supervised publisher writes a 30 fps video track through a frame writer, with a keyframe every
// ROTATION_KEYFRAME_MS, and a 20 Hz telemetry track with one object per group. It calls `rotate()` every
// ROTATION_EVERY_MS. With ROTATION_FORCE_KEYFRAME=1 the encoder honours `onKeyframeNeeded` by making its next frame a
// keyframe, as the WebCodecs example does. A supervised subscriber wants both tracks with `generations: true`, video
// through a frame reader on a playout clock as a player would. For each rotation it prints the longest gap between video
// objects received, between video frames played, and between telemetry objects received, around it.
//
// Environment: RELAY_URL, RELAY_SUB_URL (default RELAY_URL; another relay puts the viewer across relays), RELAY_TOKEN, RELAY_NAMESPACE_PREFIX, ROTATION_RUN_S (default 32), ROTATION_EVERY_MS
// (default 5000), ROTATION_KEYFRAME_MS (default 1000), ROTATION_FORCE_KEYFRAME (default off),
// ROTATION_SUBSCRIBER_OFFSET_MS (unset: the subscriber never rotates; otherwise it rotates this long after each
// publisher rotation, as a viewer on tokens of the same lifetime does).
//
//   RELAY_URL=https://ord.relay.mux.global RELAY_TOKEN=... RELAY_NAMESPACE_PREFIX=... node scripts/probe-rotation.mjs
import { randomUUID } from 'node:crypto';
import { createFrameSequencer, createPlayoutClock, locProperties } from '@caprid/moq-media';
import { createFrameReader, createFrameWriter } from '@caprid/relay/media';
import { connectSupervisedPublisher, connectSupervisedSubscriber } from '@caprid/relay/node';

const env = process.env;
const url = env.RELAY_URL;
if (!url) throw new Error('RELAY_URL is required');
const token = env.RELAY_TOKEN;
if (!token) throw new Error('RELAY_TOKEN is required');
const runMs = Number(env.ROTATION_RUN_S ?? 32) * 1000;
const everyMs = Number(env.ROTATION_EVERY_MS ?? 5000);
const keyframeMs = Number(env.ROTATION_KEYFRAME_MS ?? 1000);
const forceKeyframe = env.ROTATION_FORCE_KEYFRAME === '1';
const subscriberOffsetMs =
  env.ROTATION_SUBSCRIBER_OFFSET_MS === undefined ? undefined : Number(env.ROTATION_SUBSCRIBER_OFFSET_MS);
const getToken = () => token;
const root = [...(env.RELAY_NAMESPACE_PREFIX ?? 'relay-sdk-probe').split('/').filter(Boolean), randomUUID()];
const frameMs = 1000 / 30;
const startMs = performance.now();
const at = () => Math.round(performance.now() - startMs);

const stamp = (sequence) => {
  const payload = new Uint8Array(1200);
  new DataView(payload.buffer).setFloat64(0, performance.now());
  new DataView(payload.buffer).setUint32(8, sequence);
  return payload;
};

const generations = [];
const publisher = await connectSupervisedPublisher({
  url,
  namespace: root,
  getToken,
  onGeneration: ({ generation }) => generations.push([at(), generation]),
});
const video = publisher.registerTrack({ track: 'video' });
const telemetry = publisher.registerTrack({ track: 'telemetry' });

let keyframeRequested = false;
const keyframeRequests = [];
const writer = createFrameWriter(video, {
  onKeyframeNeeded: () => {
    keyframeRequests.push(at());
    if (forceKeyframe) keyframeRequested = true;
  },
});
const outcomes = {};

const arrivals = { video: [], telemetry: [] };
const played = [];
const clock = createPlayoutClock();
const reader = createFrameReader(createFrameSequencer({ clock }), {
  onFrame(frame) {
    // No decoder: the clock paces by wall time from the first frame shown.
    clock.due(frame.timestampUs);
    played.push(at());
  },
});
const switches = [];
// The object each switch cut over on: a new generation's first is object 0 of group 0, its keyframe.
const cutovers = [];
const cutting = new Set();
const subscriber = await connectSupervisedSubscriber({ url: env.RELAY_SUB_URL ?? url, getToken });
for (const name of ['video', 'telemetry']) {
  subscriber.want(
    name,
    { namespace: root, track: name, generations: true },
    {
      ...(name === 'video' ? reader : {}),
      onObject: (object) => {
        arrivals[name].push(at());
        if (cutting.delete(name)) cutovers.push([at(), name, object.groupId, object.objectId]);
        if (name === 'video') reader.onObject(object);
      },
      onSwitched: (info) => {
        switches.push([at(), name]);
        cutting.add(name);
        if (name === 'video') reader.onSwitched(info);
      },
      onError: (error) => console.error(name, error),
    },
  );
}

let frame = 0;
let lastKeyAt = Number.NEGATIVE_INFINITY;
const videoTimer = setInterval(() => {
  const now = performance.now();
  const key = now - lastKeyAt >= keyframeMs || keyframeRequested;
  if (key) {
    lastKeyAt = now;
    keyframeRequested = false;
  }
  void writer
    .write({ payload: stamp(frame++), key, properties: locProperties(Math.round(now * 1000)) })
    .then(({ kind }) => {
      outcomes[kind] = (outcomes[kind] ?? 0) + 1;
    });
}, frameMs);
let tick = 0;
const telemetryTimer = setInterval(async () => {
  try {
    const group = await telemetry.openGroup({ payload: stamp(tick++) });
    await group.close();
  } catch {}
}, 50);

const rotations = [];
// The first rotation waits for the viewer to be receiving, so every rotation is one it watches.
await new Promise((resolve) => setTimeout(resolve, everyMs));
while (at() < runMs - everyMs) {
  rotations.push(at());
  publisher.rotate();
  if (subscriberOffsetMs !== undefined) setTimeout(() => subscriber.rotate(), subscriberOffsetMs);
  await new Promise((resolve) => setTimeout(resolve, everyMs));
}
clearInterval(videoTimer);
clearInterval(telemetryTimer);
await writer.close();
reader.close();
await subscriber.close();
await publisher.close();

const longestGap = (times, from, to) => {
  let longest = 0;
  for (let i = 1; i < times.length; i++) {
    if (times[i] < from || times[i - 1] > to) continue;
    longest = Math.max(longest, times[i] - times[i - 1]);
  }
  return longest;
};
for (const rotatedAt of rotations) {
  const window = [rotatedAt, rotatedAt + everyMs - 500];
  console.log(
    JSON.stringify({
      rotatedAt,
      videoGapMs: longestGap(arrivals.video, ...window),
      playedGapMs: longestGap(played, ...window),
      telemetryGapMs: longestGap(arrivals.telemetry, ...window),
      cutovers: cutovers.filter(([t]) => t >= window[0] && t <= window[1]),
      keyframeRequests: keyframeRequests.filter((t) => t >= window[0] && t <= window[1]),
    }),
  );
}
console.log(
  JSON.stringify({ forceKeyframe, keyframeMs, subscriberOffsetMs, generations: generations.length, outcomes }),
);
process.exit(0);
