// Opt-in live check of a listener's catch-up (spec/behavior.md, "Media subscribing" and "Supervised subscriber")
// against a real relay, in Node over native QUIC. Run after `pnpm build` and `pnpm --filter @caprid/relay-quic
// build:native`. One process publishes and listens, so capture and arrival times share a clock.
//
// The publisher sends one 20 ms audio frame per group, stamped with its capture time on the LOC timeline, to
// RELAY_URL. A supervised subscriber on RELAY_SUB_URL (point it through a link impairer, such as moq-shaper with a
// rate below the track's, so the relay builds a backlog) wants the track through a frame reader whose sequencer drops
// audio more than CATCH_UP_LATE_DROP_MS late and, with CATCH_UP_AFTER_MS set, asks for a catch-up, which the reader's
// onCatchUp hands to the want. Without CATCH_UP_AFTER_MS it measures the same run with no catch-up. It prints one JSON
// line per second of capture time (frames delivered, their capture-to-delivery latency, catch-ups) and a summary.
//
// Environment: RELAY_URL, RELAY_SUB_URL (default RELAY_URL), RELAY_CERTIFICATE_HASH (hex SHA-256 of the relay's DER
// certificate, for a local relay), RELAY_TOKEN or RELAY_ANONYMOUS=1, CATCH_UP_RUN_S (default 20),
// CATCH_UP_PAYLOAD (bytes, default 1000), CATCH_UP_LATE_DROP_MS (default 100), CATCH_UP_AFTER_MS (unset: off).
//
//   RELAY_ANONYMOUS=1 RELAY_URL=https://127.0.0.1:4443 RELAY_SUB_URL=https://127.0.0.1:4444 \
//     RELAY_CERTIFICATE_HASH=... CATCH_UP_AFTER_MS=300 node scripts/probe-catch-up.mjs
//
// The run behind spec/behavior.md's catch-up figures put moq-shaper (moq-dev/moq rs/moq-shaper) between the subscriber
// and a local moq-relay 0.17.5, started afresh for each run since its steps count from its first datagram, with this
// profile: 20 ms each way, the relay's direction limited to 600 kbit/s with a 200 ms queue, and both directions lost
// from 8 s to 11 s.
//
//   [up]                                  [down]
//   delay = "20ms"                        delay = "20ms"
//   jitter_model = "gaussian"             jitter_model = "gaussian"
//   [[up.steps]]                          [down.rate]
//   at = "8s"                             bits_per_second = 600000
//   loss = 1.0                            burst = 3000
//   [[up.steps]]                          queue = "200ms"
//   at = "11s"                            [[down.steps]] at = "8s", loss = 1.0
//   loss = 0.0                            [[down.steps]] at = "11s", loss = 0.0
import { randomUUID } from 'node:crypto';
import { createFrameSequencer, createPlayoutClock, locProperties, locTrackProperties } from '@caprid/moq-media';
import { createFrameReader } from '@caprid/relay/media';
import { connectRelayPublisher, connectSupervisedSubscriber } from '@caprid/relay/node';

const env = process.env;
const url = env.RELAY_URL;
if (!url) throw new Error('RELAY_URL is required');
const subUrl = env.RELAY_SUB_URL ?? url;
const runMs = Number(env.CATCH_UP_RUN_S ?? 20) * 1000;
const payloadBytes = Number(env.CATCH_UP_PAYLOAD ?? 1000);
const lateDropUs = Number(env.CATCH_UP_LATE_DROP_MS ?? 100) * 1000;
const catchUpAfterUs = env.CATCH_UP_AFTER_MS === undefined ? undefined : Number(env.CATCH_UP_AFTER_MS) * 1000;
const token = env.RELAY_TOKEN;
if (!token && env.RELAY_ANONYMOUS !== '1') throw new Error('Set RELAY_TOKEN, or RELAY_ANONYMOUS=1 for a local relay');
const trust = env.RELAY_CERTIFICATE_HASH
  ? { serverCertificateHashes: [{ algorithm: 'sha-256', value: Buffer.from(env.RELAY_CERTIFICATE_HASH, 'hex') }] }
  : {};
const getToken = () => token ?? '';
const nowUs = () => performance.now() * 1000;
const namespace = [...(env.RELAY_NAMESPACE_PREFIX ?? 'relay-sdk-probe').split('/').filter(Boolean), randomUUID()];
const frameUs = 20_000;

const publisher = await connectRelayPublisher({ url, namespace, getToken, ...trust });
const track = publisher.registerTrack({
  track: 'audio',
  trackProperties: locTrackProperties(),
  writeTimeoutMs: 20_000,
});
const subscriber = await connectSupervisedSubscriber({ url: subUrl, getToken, ...trust });

const startUs = nowUs();
const delivered = []; // [captureUs, deliveredUs]
const events = []; // [atUs, what]
const clock = createPlayoutClock();
const sequencer = createFrameSequencer({
  clock,
  grouping: 'frame',
  lateDropUs,
  ...(catchUpAfterUs === undefined ? {} : { catchUpAfterUs }),
});
const reader = createFrameReader(sequencer, {
  onFrame(frame) {
    // No player: the clock paces by wall time from the first frame shown.
    clock.due(frame.timestampUs);
    delivered.push([frame.timestampUs, nowUs()]);
    if (frame.discontinuity) events.push([nowUs(), frame.discontinuity]);
  },
  onCatchUp() {
    events.push([nowUs(), 'catch-up']);
    subscriber.catchUp('audio');
  },
});
let restarts = 0;
subscriber.want(
  'audio',
  { namespace, track: 'audio' },
  {
    ...reader,
    onRestarted(info) {
      restarts++;
      events.push([nowUs(), `restarted ${info.reason}`]);
      reader.onRestarted();
    },
    onError(error) {
      events.push([nowUs(), `error ${error}`]);
    },
  },
);

let sent = 0;
let frames = 0;
const payload = new Uint8Array(payloadBytes);
while (nowUs() - startUs < runMs * 1000) {
  const captureUs = Math.round(nowUs());
  frames++;
  try {
    await track.write(payload, { properties: locProperties(captureUs) });
    sent++;
  } catch (error) {
    events.push([nowUs(), `write ${error?.code ?? error}`]);
  }
  const next = startUs + frames * frameUs;
  const waitMs = (next - nowUs()) / 1000;
  if (waitMs > 0) await new Promise((resolve) => setTimeout(resolve, waitMs));
}
await new Promise((resolve) => setTimeout(resolve, 2_000));

const percentile = (values, p) =>
  values.length === 0 ? null : Math.round(values.sort((a, b) => a - b)[Math.floor((values.length - 1) * p)] / 1000);
for (let second = 0; second * 1_000_000 < runMs * 1000; second++) {
  const from = startUs + second * 1_000_000;
  const inSecond = delivered.filter(([capture]) => capture >= from && capture < from + 1_000_000);
  const latencies = inSecond.map(([capture, at]) => at - capture);
  console.log(
    JSON.stringify({
      second,
      delivered: inSecond.length,
      latencyP50Ms: percentile([...latencies], 0.5),
      latencyMaxMs: percentile([...latencies], 1),
      events: events.filter(([at]) => at >= from && at < from + 1_000_000).map(([, what]) => what),
    }),
  );
}
const latencies = delivered.map(([capture, at]) => at - capture);
console.log(
  JSON.stringify({
    result: 'MEASURED',
    catchUpAfterMs: catchUpAfterUs === undefined ? null : catchUpAfterUs / 1000,
    lateDropMs: lateDropUs / 1000,
    payload: payloadBytes,
    sent,
    delivered: delivered.length,
    late: sequencer.stats.late,
    resyncs: sequencer.stats.resyncs,
    catchUps: events.filter(([, what]) => what === 'catch-up').length,
    restarts,
    latencyP50Ms: percentile([...latencies], 0.5),
    latencyP95Ms: percentile([...latencies], 0.95),
    latencyMaxMs: percentile([...latencies], 1),
  }),
);
reader.close();
subscriber.close();
await publisher.close();
process.exit(0);
