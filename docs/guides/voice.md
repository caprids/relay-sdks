# Voice and two-way media

Two-way voice calls and conversational AI voice agents have different requirements in each direction—plus interruption (barge-in), live transcripts, and browser echo cancellation.

## Delivery policy per direction

Configure each track's `FrameSequencer` independently without changing the shared broadcast `PlayoutClock`.

### 1. To a human listener (low latency beats completeness)

A brief concealed gap sounds much better than a growing stall. Drop late audio frames and catch up at the live edge if a congested downlink builds backlog at the relay:

```ts
import { createFrameSequencer } from '@caprid/moq-media';
import { createFrameReader } from '@caprid/relay/media';

const voice = createFrameReader(
  createFrameSequencer({
    clock,
    grouping: 'frame',
    lateDropUs: 60_000,
    catchUpAfterUs: 300_000, // keep below clock targetLatencyUs so catch-up runs before resync
  }),
  {
    onFrame: input.push,
    onCatchUp: () => subscriber.catchUp('voice'),
  },
);

subscriber.want(
  'voice',
  {
    namespace,
    track: 'audio',
    generations: true,
    parameters: {
      subscriberPriority: 16,
      maxAge: 250, // honored by relays declaring max-age support; safely ignored otherwise
    },
  },
  { ...voice, onError },
);
```

- **Leave `reorderWindowUs` at its default (`0`) for human listeners.** Under real packet loss, holding subsequent frames to wait for a missing frame causes jitter-buffer underruns and raises delay; let libopus in-band FEC and PLC conceal isolated losses instead. Only add a small reorder window if `sequencer.stats.behind` dominates `stats.late` on a very low-loss path.
- **Enable Opus in-band FEC on the publisher** (`opus: { useinbandfec: true, packetlossperc: 10 }`) so the browser audio player can rebuild lost frames from the next packet.

### 2. To an AI agent / speech-to-text (every word matters)

When sending a user's microphone to an STT pipeline, completeness matters more than tight playout latency:

- **Publisher**: increase the audio subgroup delivery timeout from 500 ms to 1–2 s with `mediaTrackProperties('audio', { subgroupTimeoutUs: 2_000_000 })` so QUIC keeps retransmitting through brief loss.
- **Receiver**: disable late dropping (`lateDropUs: Number.POSITIVE_INFINITY`) and set `reorderWindowUs` at least as long as the publisher's timeout so retransmitted groups arriving out of order are reassembled before forwarding to STT:

```ts
const words = createFrameReader(
  createFrameSequencer({
    clock,
    grouping: 'frame',
    lateDropUs: Number.POSITIVE_INFINITY,
    reorderWindowUs: 2_000_000,
  }),
  {
    onFrame(frame) {
      if (frame.missingGroups) {
        noteGap(frame.missingGroups.firstGroupId, frame.missingGroups.lastGroupId);
      }
      transcriber.push(frame);
    },
  },
);
```

### 3. From a faster-than-real-time TTS agent

A text-to-speech engine often generates and sends audio faster than 1x real time, with each packet stamped at its future playback time. Without `timestampPlayout`, early frames trigger a clock resync and get dropped. Enable `timestampPlayout` using the audio input's `playoutLeadUs`:

```ts
const input = player.addInput({ clock, decoderConfig: audioDecoderConfigFromTrack(audioTrack) });
const reply = createFrameReader(
  createFrameSequencer({
    clock,
    grouping: 'frame',
    timestampPlayout: { leadUs: () => input.playoutLeadUs },
  }),
  { onFrame: input.push },
);
```

## Interrupts (barge-in) and transcripts

When a user interrupts a speaking agent, the agent needs listeners to stop playing already-buffered audio immediately. Relay supports two complementary mechanisms (see [Interrupt track](../../spec/media-layer.md#interrupt-track) and [Track replacement](../../spec/media-layer.md#track-replacement)):

1. **Interrupt track (`INTERRUPT_TRACK`)**: the agent writes an interrupt range (`{ fromUs, untilUs }`) when cut off; the browser applies `input.cut(interrupt)` to drop buffered and in-flight audio in that range.
2. **Replaceable track (`createReplaceableTrack`)**: the agent swaps to a fresh audio track (`audio-2`, `audio-3`, …) in the catalog so unsent frames on the old track are discarded immediately; listeners discover the replacement with `findReplacementTrack`.
3. **Transcript track (`TRANSCRIPT_TRACK`)**: the agent publishes timestamped text segments; the browser feeds them to `createTranscript()`, cuts them with the same interrupt, and reads `transcript.linesAt(clock.position())` to display text in sync with audio playback.

```ts
import {
  createTranscript,
  INTERRUPT_TRACK,
  parseInterrupt,
  parseTranscriptSegment,
  TRANSCRIPT_TRACK,
} from '@caprid/moq-media';

const transcript = createTranscript();

subscriber.want('transcript', { namespace, track: TRANSCRIPT_TRACK, generations: true }, {
  onObject: (object) => transcript.apply(parseTranscriptSegment(object.payload)),
});

subscriber.want('interrupt', { namespace, track: INTERRUPT_TRACK, generations: true }, {
  onObject: (object) => {
    const interrupt = parseInterrupt(object.payload);
    if (interrupt.track === 'audio') input.cut(interrupt);
    transcript.cut(interrupt, clock.position());
  },
});
```

For a complete browser + Python voice bot using these tracks, see the [voice bot examples](../../python/examples/voice_bot/README.md) (`python/examples/voice_bot` and `ts/examples/voice-bot`).

## Browser echo cancellation (Chromium)

In a two-way call where the page captures a microphone, **Chromium's acoustic echo canceller removes only audio played through WebRTC**, not audio connected directly to `AudioContext.destination`.

In Chromium, route `player.output` through `createEchoCancellationLoopback` instead of connecting it to `context.destination`:

```ts
import { createEchoCancellationLoopback } from '@caprid/moq-media';

const loopback = await createEchoCancellationLoopback(context, player.output);
// Optional: await loopback.setSinkId(deviceId);
// On cleanup: loopback.close();
```

Firefox and Safari cancel echo from all page audio output, so connect `player.output.connect(context.destination)` directly there. Because the WebRTC loopback adds unreported buffering, set `player.outputDelayUs` if video must stay lip-synced with the looped-back audio.
