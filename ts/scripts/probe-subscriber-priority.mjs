// Opt-in live check of a viewer's subscriber priority (spec/behavior.md, "Media subscribing") against a real relay.
// Run after `pnpm build` and `pnpm --filter @mux/relay-quic build:native`.
//
// A publisher in this process, on RELAY_URL (give it a clean path), publishes one audio and one video track,
// registered with mediaTrackProperties, so both carry the default publisher priorities: 20 ms audio frames of
// PRIORITY_AUDIO_BYTES (default 160), one group per frame, and 30 fps video of PRIORITY_DELTA_BYTES (default 3000) per
// delta with a PRIORITY_KEY_BYTES (default 40000) keyframe every second, about 1 Mbit/s. Every payload starts with its
// capture time (Date.now(), float64). A subscriber in this process, on RELAY_SUB_URL, measures each audio frame's
// capture-to-arrival latency. Point RELAY_SUB_URL through a downlink impairer, so the relay holds a backlog for it.
// SUBSCRIBER_PRIORITY=on subscribes with mediaSubscribeParameters; off subscribes with no parameters, which is a
// viewer before #337; invert overrides the priorities, video ahead of audio (audio 96, video 16): a check that the
// relay orders by them at all. It prints a summary of audio latency after a warm-up from the first frame published, how much video arrived, and the
// SUBSCRIBER_PRIORITY each SUBSCRIBE carried, read off the subscriber's request streams. The priority the relay stamps
// on group streams is no evidence: moq-relay 0.17.5 stamps a track's 0x0E whatever the subscriber asks for.
//
// Environment: RELAY_URL, RELAY_SUB_URL (default RELAY_URL), RELAY_TOKEN, RELAY_NAMESPACE_PREFIX, SUBSCRIBER_PRIORITY
// (on, off or invert; default on), PRIORITY_RUN_S (default 25), PRIORITY_WARM_UP_S (default 5), PRIORITY_RESOLVE (an
// address the subscriber dials instead of RELAY_SUB_URL's host, for an impairer on a local port; TLS still checks the
// host).
//
// With the shared UDP impairment proxy (impair.py, stdlib Python) between the subscriber and ord.relay.mux.global:
//
//   python3 impair.py --port 14433 --upstream ord.relay.mux.global:443 \
//     --set '{"down":{"rate_kbps":800,"queue_ms":100}}' &
//   RELAY_URL=https://ord.relay.mux.global RELAY_SUB_URL=https://ord.relay.mux.global:14433 PRIORITY_RESOLVE=127.0.0.1 \
//     node scripts/probe-subscriber-priority.mjs
import { randomUUID } from 'node:crypto';
import { createFrameWriter, mediaSubscribeParameters, mediaTrackProperties } from '@mux/relay/media';
import { connectRelayPublisher, connectRelaySubscriber } from '@mux/relay/node';
import { nativeQuicTransport } from '@mux/relay-quic';
import { ControlMessageDeframer, decodeControlMessage } from '../packages/moq-core/dist/protocol/control-messages.js';
import { loadAddon } from '../packages/relay-quic/dist/addon.js';

const env = process.env;
const url = env.RELAY_URL;
if (!url) throw new Error('RELAY_URL is required');
const subUrl = env.RELAY_SUB_URL ?? url;
const token = env.RELAY_TOKEN;
if (!token) throw new Error('RELAY_TOKEN is required');
const priority = env.SUBSCRIBER_PRIORITY ?? 'on';
if (!['on', 'off', 'invert'].includes(priority)) throw new Error('SUBSCRIBER_PRIORITY must be on, off or invert');
const inverted = { audio: 96, video: 16 };
const parameters = (track) =>
  priority === 'off'
    ? {}
    : {
        parameters: mediaSubscribeParameters(
          track,
          priority === 'invert' ? { subscriberPriority: inverted[track] } : {},
        ),
      };
const runMs = Number(env.PRIORITY_RUN_S ?? 25) * 1000;
const warmUpMs = Number(env.PRIORITY_WARM_UP_S ?? 5) * 1000;
const sizes = {
  audio: Number(env.PRIORITY_AUDIO_BYTES ?? 160),
  delta: Number(env.PRIORITY_DELTA_BYTES ?? 3000),
  key: Number(env.PRIORITY_KEY_BYTES ?? 40000),
};
const namespace = [...(env.RELAY_NAMESPACE_PREFIX ?? 'relay-sdk-probe').split('/').filter(Boolean), randomUUID()];

const resolveTo = env.PRIORITY_RESOLVE;
let subscriberDialing = false; // set only while the subscriber's transport dials, so the publisher keeps its path
if (resolveTo !== undefined) {
  // Dial the impairer for the subscriber only, keeping the host as the TLS server name.
  const { Client } = loadAddon();
  const dial = Client.prototype.dial;
  Client.prototype.dial = function (host, port, serverName, protocols) {
    return dial.call(this, subscriberDialing ? resolveTo : host, port, serverName, protocols);
  };
}

const arrivals = { audio: [], video: [] }; // [captureMs, arrivalMs]
const sent = {}; // track -> SUBSCRIBER_PRIORITY on its SUBSCRIBE, or null
// Read each SUBSCRIBE off the subscriber's request streams as the session writes it.
const createTransport = (dial) => (transportUrl, protocols) => {
  subscriberDialing = true;
  let opened;
  try {
    opened = dial(transportUrl, protocols); // the native client dials synchronously
  } finally {
    subscriberDialing = false;
  }
  const open = opened.transport.createBidirectionalStream.bind(opened.transport);
  opened.transport.createBidirectionalStream = async () => {
    const stream = await open();
    const deframer = new ControlMessageDeframer();
    const getWriter = stream.writable.getWriter.bind(stream.writable);
    stream.writable.getWriter = () => {
      const writer = getWriter();
      const write = writer.write.bind(writer);
      writer.write = (chunk) => {
        for (const frame of deframer.push(chunk.slice())) {
          const message = decodeControlMessage(frame);
          if (message.kind === 'subscribe') sent[message.trackName] = message.parameters.subscriberPriority ?? null;
        }
        return write(chunk);
      };
      return writer;
    };
    return stream;
  };
  return opened;
};
const publisher = await connectRelayPublisher({ url, namespace, getToken: () => token });
const subscriber = await connectRelaySubscriber({
  url: subUrl,
  getToken: () => token,
  createTransport: createTransport(nativeQuicTransport()),
});
const capture = (payload) => new DataView(payload.buffer, payload.byteOffset, 8).getFloat64(0);
const audio = createFrameWriter(
  publisher.registerTrack({ track: 'audio', trackProperties: mediaTrackProperties('audio') }),
  { grouping: 'frame' },
);
const video = createFrameWriter(
  publisher.registerTrack({
    track: 'video',
    trackProperties: mediaTrackProperties('video', { keyframeIntervalUs: 1_000_000 }),
  }),
);
await publisher.announced;
const subscriptions = ['audio', 'video'].map((track) =>
  subscriber.subscribe(
    { namespace, track, ...parameters(track) },
    {
      onObject(object) {
        arrivals[track].push([capture(object.payload), Date.now()]);
      },
      onError: (error) => console.error(`${track}: ${error}`),
    },
  ),
);
await Promise.all(subscriptions.map((subscription) => subscription.subscribed));

const counts = {};
const count = (track) => (outcome) => {
  const key = `${track}:${outcome.kind}`;
  counts[key] = (counts[key] ?? 0) + 1;
};
const frame = (bytes, key) => {
  const payload = new Uint8Array(bytes);
  new DataView(payload.buffer).setFloat64(0, Date.now());
  return { payload, key };
};
let videoFrames = 0;
const startMs = Date.now(); // the warm-up runs from here, whatever arrives first
const timers = [
  setInterval(() => void audio.write(frame(sizes.audio, true)).then(count('audio')), 20),
  setInterval(() => {
    const key = videoFrames++ % 30 === 0;
    void video.write(frame(key ? sizes.key : sizes.delta, key)).then(count('video'));
  }, 1000 / 30),
];
await new Promise((resolve) => setTimeout(resolve, runMs));
for (const timer of timers) clearInterval(timer);
await new Promise((resolve) => setTimeout(resolve, 2_000));
await Promise.all([audio.close(), video.close()]);
for (const subscription of subscriptions) subscription.cancel();
await Promise.all([publisher.close(), subscriber.close()]);

const measured = (track) => {
  const after = arrivals[track].filter(([captured]) => captured >= startMs + warmUpMs);
  const latencies = after.map(([captured, at]) => at - captured).sort((a, b) => a - b);
  const at = (p) => (latencies.length === 0 ? null : Math.round(latencies[Math.floor((latencies.length - 1) * p)]));
  return { received: after.length, p50Ms: at(0.5), p95Ms: at(0.95), p99Ms: at(0.99), maxMs: at(1) };
};
console.log(
  JSON.stringify({
    result: 'MEASURED',
    subscriberPriority: priority,
    warmUpMs,
    runMs,
    sizes,
    audio: measured('audio'),
    video: measured('video'),
    subscribeSent: sent,
    outcomes: counts,
  }),
);
