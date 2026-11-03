# What our MoQ audio players should take from NetEQ

| | |
| --- | --- |
| As of | 2026-11-03 |
| Our code | relay-sdks `main` at `cbca1c1` (2026-11-02). Every relay-sdks `file:line` below is at that commit, from the repository root |
| NetEQ | as read in [neteq.md](neteq.md), at webrtc `3bdd69e14b22` (2026-11-03); "neteq.md §n" points into it |
| Evidence | relay-dev's voice benchmark findings register (`moq-netbench/results/voice/FINDINGS.md`, dataset `ds-20261030-120627-c7c45d5c`; findings cited by id), relay-dev `moq-netbench/docs/moq-optimisation.md`, and the Huddle soak calls of 2026-11-03 as reported in relay-sdks [#433](https://github.com/muxinc/relay-sdks/pull/433) |
| Written for | relay-3cnp.504 (Adam, 2026-11-03) |

The player in question is the TypeScript one in `@caprid/moq-media`: `audio-player.ts`,
`audio-worklet.ts`, `sequencer.ts` and `clock.ts` under `ts/packages/moq-media/src/`. The Python SDK
has no audio player. Huddle's soak calls (#433) ran this sequencer and player, and the Video.js renderer
uses the same core.

## Summary

Ranked by what a listener would gain for the cost and risk, given the evidence we have:

| Rank | Take from NetEQ | What a listener gets | Cost | Risk |
| --- | --- | --- | --- | --- |
| 1 | Its statistics: per-frame buffer delay, target over time, concealment events, interruptions, arrival delay (neteq.md §9) | Nothing directly; every change below becomes measurable, and comparable with WebRTC's `getStats` in the same rounds | small | low |
| 2 | A missing frame is waited for until its audio is due, then concealed (neteq.md §5) | No frame dropped for arriving 5 ms after its neighbour; no underrun from waiting longer than the buffer holds | small | low |
| 3 | The target set from measured arrival delay, not only from underruns (neteq.md §3.1-3.3) | Far fewer underruns where the path's delay spread is wider than the 20 ms floor | medium | medium: earlier tries did not win on the bench |
| 4 | Concealment and fades at every discontinuity; resume after a short dropout without refilling the whole target (neteq.md §2.5) | No clicks at underruns, trims and cuts; shorter silences after an underrun | small to medium | low |
| 5 | Encoded frames buffered and decoded just in time (neteq.md §1, §2.3) | Late frames usable up to their deadline; no underruns from a busy main thread | high | medium |
| 6 | Time stretching instead of trims (neteq.md §4, §3.5) | Delay shed without cutting audio, if it can be shown to score fairly | medium | high: measured worse once (R-S4A-01) |

The first three to build are at the end. Ideas that do not fit MoQ are listed before them.

## What MoQ changes

NetEQ was shaped by RTP over UDP. Our audio arrives differently, and each difference moves a
recommendation:

- **One group per frame, each on its own QUIC stream.** Audio is written with frame grouping
  (`ts/src/frame-writer.ts:9`, the default for replaceable audio tracks at
  `ts/src/replaceable-track.ts:152`), so neighbouring frames travel on separate streams and can
  arrive swapped. Huddle's soak calls on 2026-11-03 saw about 1.8% of frames arrive after the next
  one, nearly all within 5 ms (#433). In WebRTC reordering is rare outside retransmissions
  ([article]); for us it is structural.
- **Loss turns into lateness.** QUIC retransmits a lost frame until the publisher's delivery timeout
  (500 ms for audio by default, `ts/src/media-track-properties.ts:29`; spec/behavior.md, "Delivery
  timeouts"), so a frame lost on the wire usually arrives about one round trip plus a probe timeout
  late. It is gone only when the publisher resets the group at its timeout, when a relay that honours
  a subscriber's max age drops it (moq-dev main; 0.17.x ignores a subscriber's delivery timeout,
  finding C-OMQ), or when the sequencer drops it as late. A group reset tells the subscriber it will
  not come, which RTP never does.
- **No NACK, no RTCP.** There is nothing to ask the sender for and no loss report back to it. FETCH
  can fill a missing group later, but it arrived a median 2 s after capture on the bench (finding
  C-8): useful for an agent's transcript, never for a listener.
- **We decode on arrival and buffer decoded audio.** The main thread decodes each frame as it is
  pushed (`ts/packages/moq-media/src/audio-player.ts:901-952`) and posts PCM to the worklet, whose ring
  is the jitter buffer (`ts/packages/moq-media/src/audio-worklet.ts:90-104`). NetEQ keeps packets
  encoded until their audio is due.
- **The listener's policy is settled: a gap beats a stall.** Late audio is dropped against the
  playout clock (the clock's 40 ms `lateDropUs`, `ts/packages/moq-media/src/clock.ts:287`; 60 ms in
  the guide's listener settings), and while the buffer is dry the clock runs on, so frames that missed
  their slot are skipped rather than played late (spec/media-layer.md, "Audio player", Clock). NetEQ
  does the opposite: it plays late audio late and stretches the delay away (neteq.md §5). We do not
  reopen that choice here; the recommendations work inside it.

## What we do today, against NetEQ

| NetEQ component | Ours | Where |
| --- | --- | --- |
| Packet buffer, sorted, usable until played | Frame sequencer: in group order; a frame behind what was delivered is dropped on arrival; a reorder window (default 0) holds newer frames for a missing group, timed from the newer group's arrival | `ts/packages/moq-media/src/sequencer.ts:199`, `sequencer.ts:507-524`, `sequencer.ts:748-751` |
| Late packet: discarded once audio after it has played | Dropped when more than `lateDropUs` behind the clock's position | `sequencer.ts:205`, `sequencer.ts:560-567`; `clock.ts:287` |
| Delay manager: target from relative arrival delay | Target from underruns: start 60 ms, +40 ms per underrun, −10 ms per 2 s without one, floor 20 ms, ceiling 400 ms and the clock's budget | `audio-player.ts:536-538`; `audio-worklet.ts:96-98`, `audio-worklet.ts:228-237`, `audio-worklet.ts:402-405` |
| Accelerate | Trim: each second, drop held audio that never dipped to the target, when 5 ms or more; hard bound at 2 × target + 60 ms | `audio-worklet.ts:394-401`, `audio-worklet.ts:344-348` |
| Preemptive expand | None. After running dry the buffer refills to its whole (grown) target before playing | `audio-worklet.ts:336-343` |
| Expand (PLC) | libopus concealment and in-band FEC for a gap, decided when the frame after it arrives, up to 120 ms; silence beyond, and for a gap inside held audio; nothing once the buffer is dry | `audio-player.ts:671-705`, `audio-player.ts:287`; `audio-worklet.ts:220-227`, `audio-worklet.ts:248-256` |
| Merge and Normal-after-expand cross-fades | None: running dry cuts to zero mid-quantum, play resumes at full level, trims and cuts splice without a fade | `audio-worklet.ts:372`, `audio-worklet.ts:381-389`, `audio-worklet.ts:397-398` |
| Statistics | `AudioInputStats` (cumulative counters and snapshots) and `FrameSequencerStats` | `audio-player.ts:138-186`, `sequencer.ts:133-146` |

## Recommendations, ranked

### 1. Count what NetEQ counts

**What NetEQ does** (neteq.md §9). Cumulative counters that answer the questions a listener's
complaint raises: how long each packet waited in the buffer (`jitterBufferDelay` over
`jitterBufferEmittedCount`), what the target was over the same samples (`jitterBufferTargetDelay`),
how many concealment events and how much concealed audio, how many interruptions of 150 ms or more
and their total length, how much was removed or inserted by stretching, and the sum of relative
arrival delays. It also separates an outage that ended in a late packet (*delayed packet outage*)
from one that ended in a loss.

**What we do today.** `AudioInputStats` (`audio-player.ts:138-186`) has underruns, frames dropped as
late, trimmed, concealed (silence), synthesized and recovered audio, all cumulative, but the buffer
depth and target only as snapshots (`bufferedUs`, `jitterTargetUs`). `FrameSequencerStats`
(`sequencer.ts:133-146`) counts delivered, late, skipped groups and resyncs; open #432 splits frames
dropped as *behind* from *late*. Nothing says how long the buffer ran dry each time, how long a frame
waited from arrival to playout on average, or how late frames arrive.

**What it does for a listener.** Nothing by itself. It lets us read a Huddle call or a benchmark round
the way WebRTC's own stats read the WebRTC arm, and tell an underrun-driven gap from a loss.

**What to add.**

- Per frame arrival to playout, summed as frame-time × samples, and the samples, so any interval's
  mean is a difference of two reads (NetEQ's `jitterBufferDelay` shape). Measure from the worklet's
  receipt of the PCM, on the render clock.
- Target × samples, the same way, so the mean target over an interval is known, not a 20 ms
  snapshot.
- Dry spells: count, total length, and the count of 150 ms or longer (NetEQ's interruptions).
- Concealment events (a run of filled or silent audio counts once).
- Relative arrival delay (recommendation 3's measurement), summed, and a coarse histogram.
- Fate of every frame: delivered, dropped late, dropped behind, never arrived (group reset or skipped),
  filled by FEC, filled by PLC. NetEQ cannot tell a loss on the wire from a late packet; we can,
  because a reset group says so.

**How to measure it.** Unit tests in `ts/tests/audio-player.test.ts` and
`ts/tests/frame-sequencer.test.ts` against the existing counters; one Huddle soak call checked against
its recording. The client diagnostics in open #436 and #437 could publish them.

**Cost and risk.** Small: counters in the worklet and its report, fields on the stats objects, spec
and vocabulary entries. Low risk: reports stay cumulative, as now.

### 2. Wait for a missing frame until its audio is due

**What NetEQ does** (neteq.md §2.3, §2.5, §5). Packets sit in timestamp order. A reordered packet is
not late until the audio before it has been played, so there is no reorder window: the window is
whatever audio is ahead of it. When the expected packet is missing and a later one is buffered,
NetEQ conceals exactly the missing slot when it comes due and merges into the later packet
(`decision_logic.cc:288-311`); the missing packet, if it turns up after that, is discarded. It stops
waiting early only if the buffer is 50 ms above target (`decision_logic.cc:293-298`).

**What we do today.** With the default `reorderWindowUs` of 0 (`sequencer.ts:199`) a frame that
arrives after its successor was delivered is dropped on arrival (`sequencer.ts:748-751`), though the
player may still hold 20 to 100 ms of audio before it would have played. Huddle's soak calls: 1.9%
of frames dropped and 22 to 23 s synthesized per 20 minutes without a window, 0.01% and under 1 s with
60 ms (#433; the settled reading of 2026-11-03). A fixed window has the opposite failure: it is
timed from the newer frame's arrival (`sequencer.ts:519-524`), so when a frame is late by a
retransmission the newer frames wait the whole window even if the player holds less, and the player
runs dry. With 60 ms at the 20 ms floor the calls kept 6.1 to 6.5 underruns a minute (#433), which
the window may or may not share in; nothing separates the two today.

**The change.** Make the window what the player holds less a margin, read when the wait starts:
`max(0, bufferedUs − margin)`, with a margin for one report's staleness (about 20 ms) and one render
quantum. When it runs out the newer frame goes out with `gap`, and the player fills the missing slot
from that frame's FEC or with concealment (`audio-player.ts:671-705`), which is NetEQ's merge. Ways
to wire it: let `reorderWindowUs` take a function, as closed #358 did for `lateDropUs`, and have the
listener pass `() => input.stats.bufferedUs - margin`; or give the sequencer the input itself. A
listener with a player could then default to it, since frame grouping always reorders.

**What it does for a listener.** Frames that arrive a few milliseconds out of order always play.
A frame that comes too late costs one concealed slot, never a dry buffer.

**How to measure it.** Sequencer `late` and `behind` (#432), player `synthesizedUs`, `recoveredUs`
and `underruns`. Huddle soak, paired: window 0, 60 ms and the deadline window, at the 20 ms and 80 ms
floors. On the voice benchmark, the `sdk` and `sdk-rw60` cells of stage 4c part 6
(relay-dev `moq-netbench/plans/README.md`, "Stage 4c part 6") are the baseline: drops a minute and
mouth-to-ear p50 on near loss-random-2, near wifi-busy and far clean.

**Cost and risk.** Small to medium: a function-valued option, the spec's frame-reader rules
(spec/behavior.md, "Media subscribing") and the shared vectors (`spec/vectors/frame-sequencer.json`)
if it changes a default, the guide. Risk: the stats it reads are up to one report old, which the
margin covers; under timestamp playout the lead already shapes what the player holds.

**What MoQ changes.** This matters more for us than for WebRTC because our reordering is structural.
A group reset already releases the wait at once (spec/behavior.md, "Media subscribing"), which gives
us what NetEQ never has: certainty that the frame is not coming.

### 3. Set the target from measured arrival delay

**What NetEQ does** (neteq.md §3.1-3.3). Each packet's arrival time less its timestamp, relative to
the fastest packet of the last 2 s, measured on the playout clock, goes into a forgetting histogram;
the target is the delay that at most 5% of half-second windows exceeded. A second histogram, of
reordered packets, raises the target only when the share of packets it would save is worth the delay
at 20 ms per percent. Underruns are not an input at all.

**What we do today.** The target moves only on symptoms: +40 ms when the buffer runs dry and audio
comes back within the ceiling (`audio-worklet.ts:228-237`), −10 ms per 2 s without one
(`audio-worklet.ts:402-405`), never below `minJitterTargetUs`, 20 ms by default
(`audio-player.ts:537`). On a path whose delay spread is wider than the floor this saws: shrink to the
floor, underrun, grow, trim. Huddle's calls on an 80 to 200 ms spread underran 6.5 times a minute at the
20 ms floor and 0.5 at 80 ms, for about 40 ms more held audio (#433; settled reading). The guide
change in #433 tells applications to raise the floor by hand when underruns climb.

**What has been tried.** Two related ideas lost or tied on the bench, and the change has to answer
both:

- A target from a decaying low quantile of each 500 ms's lowest buffer level "followed LTE's and
  handover's long stalls up to 260-380 ms" (relay-dev `moq-netbench/docs/moq-optimisation.md`,
  section 2, "The adaptive buffer").
- Closed #358's `percentile` adaptation measured arrival delay in the worklet on the render clock,
  as NetEQ does, and refused to give back below its 95th percentile over 5 s, with a faster give-back
  (every 250 ms). In stage 4a it was level with the reference (handover R 87.4 against 87.2, LTE 84 against 85.2 with 5.7 against 3.6 drops
  a minute; FINDINGS.md, R-S4A-01's table), and in the retune its pairing with RED failed the drops
  clause (R-T1-01). The bench's profiles did not include a steady spread like Huddle's.

**The change.** Use the arrival-delay estimate as the floor the underrun rule cannot shrink below,
and as the start target, not as the whole controller:

- Measure in the worklet on the render clock, as #358 did: relative delay of each arriving frame
  against the least `arrival − timestamp` of the last 2 s. Leave out filled audio, frames after a
  resync or restart (reset the estimate), and nothing else: DTX needs no special case, since the
  timestamps jump across the silence.
- Choose the delay with NetEQ's reorder-optimizer cost rather than its underrun quantile: the least
  `delay + k × (share of frames later than delay)`, with `k` near NetEQ's 20 ms per percent, over a
  per-frame histogram. NetEQ's per-500 ms maximum would make one resent frame in a window count as
  the whole window: at 2% loss about `1 − 0.98²⁵` ≈ 40% of windows of 25 frames hold a resend, so
  the 95% quantile would sit at a round trip plus a probe timeout, which is what late drop 200 paid
  for nothing once concealment and FEC covered single losses (R-S2b-01: 4.1 drops a minute at 126 ms
  with concealment, against 209 ms waiting for resends).
- Keep our give-back faster than NetEQ's. Its 0.983 per 500 ms remembers about 29 s and takes over a
  minute to come down after a long episode (neteq.md §3.2); our faster give-back is what returned to
  110 ms after a deep queue where WebRTC stayed at 185 to 205 ms (P-12).
- Cap it below the ceiling, so a long stall (LTE, handover) cannot hold the floor up, which is what
  sank the buffer-level quantile.

**What it does for a listener.** On a path with a wide, steady spread, the buffer sits above the
spread instead of sawing through it: far fewer gaps, about 40 ms more delay, chosen per path instead
of by an application constant. On a quiet path the estimate is the floor and nothing changes.

**How to measure it.** Player `underruns`, `jitterTargetUs` (better: recommendation 1's mean
target), `trimmedUs` and held audio. Huddle soak, paired, floor 20 against floor 80 against the
estimate: the 80 ms row of #433 is the bar. Voice benchmark: drops a minute, mouth-to-ear p50 and R
during the flow (P-03 says whole-clip R measures the drain) on near wifi-busy, LTE with an upload,
loss-random-2 and far clean, against WebRTC in the same rounds.

**Cost and risk.** Medium: about 150 lines in the worklet, an option, the media-layer table, tests.
Risk: following long stalls and holding them (mitigated by the cap and the cost function), and a
default change, so behind an option until measured.

**What MoQ changes.** The delay distribution has a retransmission mode a round trip plus a probe
timeout above the base, which is why the cost function, not a quantile of maxima, is the part to take.
Frames later than the late-drop limit never reach the worklet, so the histogram is already truncated
there.

### 4. Conceal and fade at every discontinuity; resume early after a short dropout

**What NetEQ does** (neteq.md §2.5). When it has nothing to play it expands: repeated pitch periods
fading out from the 3rd consecutive call and settling on the speaker's background noise, never digital
silence (`expand.cc:262-308`). When audio returns it fades in from where the expansion had faded to
(`normal.cc:87-161`) or correlates and cross-fades into it (`merge.cc:51-167`). Every stretch is an
overlap-add. After a short dropout it resumes at once; only when the expansion has faded below half
gain does it wait for the buffer to reach half the target (`decision_logic.cc:327-353`).

**What we do today.** Running dry cuts to zero inside the quantum (`audio-worklet.ts:372`,
`audio-worklet.ts:381-389`); concealment arriving once the buffer is dry is dropped
(`audio-worklet.ts:220-227`); playback restarts at full level only when the whole target, just grown
by 40 ms, is held again (`audio-worklet.ts:336-343`), so each underrun's silence is the outage plus
60 ms or more. Trims, the hard bound and cuts drop samples and join what is left with no fade
(`audio-worklet.ts:397-398`, `audio-worklet.ts:346-347`), and a gap inside held audio is filled with
hard-edged silence (`audio-worklet.ts:248-256`).

**The change**, in three separable parts:

- Fade every splice: a raised-cosine cross-fade of a few milliseconds at each trim, hard-bound drop,
  cut, silence fill and resume, and a short fade-out when the buffer runs dry.
- Resume sooner: after a short dry spell start playing at half the target, after a long one at the
  whole target, as PostponeDecode does.
- Fill a dry spell: repeat the last pitch period with a fade in the worklet (NetEQ's expand, simplified),
  instead of silence, for the first tens of milliseconds.

**What it does for a listener.** Underruns, trims and cuts stop clicking, and the silence after an
underrun gets shorter. The policy does not change: late frames are still skipped.

**How to measure it.** ViSQOL and R on the voice benchmark, and the longest drop. The bench's drops
count output that is "silent or uncorrelated" (relay-dev `moq-netbench/docs/voice-benchmark.md`,
section 10), so concealment and silence look alike there and drops a minute may not move. Player
`underruns` with recommendation 1's dry-spell lengths; Huddle soak.

**Cost and risk.** Fades and the earlier resume are small; synthesis in the worklet is medium. Risk is
low for the fades; resuming at half the target risks a second underrun, which NetEQ guards by doing it
only after a short dropout.

### 5. Keep frames encoded and decode just in time

**What NetEQ does** (neteq.md §1, §2.3, §7). Packets wait encoded; decoding, FEC and concealment are
chosen at the 10 ms tick that needs them, so a reordered packet, a duplicate or the primary that
outruns its FEC copy can still take its place until the last moment.

**What we do today.** The main thread decodes on arrival (`audio-player.ts:901-952`, through
`audio-player.ts:707-743`) and posts PCM to the worklet, deciding FEC and concealment when the frame
after a gap arrives (`audio-player.ts:671-705`), up to a buffer's length before that audio plays. A
frame that turns up after its successor was decoded cannot be used, since Opus decodes in order. A
main thread that stalls delays decoded audio on its way to the worklet, which is why the guide tells
busy pages to raise the floor (ts/GUIDE.md, "Playing audio").

**The change.** Run libopus (already WebAssembly) in the `AudioWorkletGlobalScope` over a
timestamp-ordered queue of encoded frames, decoding a frame or two ahead of the read position.
Recommendations 2 and 4 then become decisions at playout time, as in NetEQ.

**What it does for a listener.** Fewer underruns on busy pages; every frame usable up to its deadline.

**How to measure it.** Underruns under a synthetic main-thread load; Huddle soak; worklet CPU per
quantum; time to first audio (relay-dev's join measurements, spec/media-layer.md, "Joining sooner").

**Cost and risk.** High: libopus in the worklet (the worklet ships as a function's source, `audio-worklet.ts:1-6`, and
libopus as embedded base64, spec/media-layer.md, "Loss concealment"), the WebCodecs path stays on the main thread, and a 128-frame quantum at
48 kHz leaves about 2.7 ms for everything. Best done after 2 to 4 show what the decisions should be.

### 6. Time stretching, only once it can be scored fairly

**What NetEQ does** (neteq.md §3.5, §4). WSOLA on 30 ms blocks: remove or insert one pitch period
where two periods correlate above 0.9, or anywhere in quiet audio. It accelerates only when the
playout delay is above the target plus the largest relative delay of the last 2 s plus 20 ms, with
at least 50 ms between stretches, about 100 ms of change a second at most.

**What we do today.** The player trims rather than time-stretches (settled reading of 2026-11-03;
`audio-worklet.ts:394-401`); `playbackRate` resamples with a pitch shift for applications that own
their latency (spec/media-layer.md, "Playback rate"). Closed #358's WSOLA option had more drops in
every condition it ran (R-S4A-01; 16 drops a minute on narrow with a download).

**Why that may not be the last word** [inferred]. The bench aligns output to the reference by
cross-correlation and counts uncorrelated output as drops; its quick scorer is known to fail on time
scaling (relay-dev `moq-netbench/replay/README.md`, "Known limits of score_lite"), and the design's
fault injection includes a 1% time stretch that the analyzer must pass. #358 also accelerated whenever
the lowest level of 250 ms was 5 ms above the target, with no dead band, and decelerated when a frame
was 10 ms overdue: far more stretching than NetEQ's thresholds allow.

**The change.** None now. A retry needs, first, evidence that the analyzer scores a NetEQ-sized
stretch as neither a drop nor a jump, and then NetEQ's dead band and rate limit rather than a
low-water trigger.

**Cost and risk.** Medium to high. P-12 is the reason for caution: our trim gives back a deep queue
at once, where stretching at 100 ms a second would take seconds.

## Ideas that do not fit MoQ

- **The NACK tracker and RTCP NACK** (neteq.md §8). QUIC already retransmits each stream's data until
  the delivery timeout, below anything the application sees, and there is no RTCP to carry a request.
  FETCH is the only application-level request, and it is seconds late (C-8).
- **The reorder optimizer as a model of NACK.** Its cost function is worth taking (recommendation 3);
  its purpose, deciding whether to wait for retransmissions the receiver asked for, has no MoQ
  counterpart.
- **RED splitting and priorities** (neteq.md §7). RED is held (Adam, 2026-10-29, relay-dev
  `moq-netbench/results/voice/relay-sdks-prs.md`), and a frame QUIC resends needs no redundant copy.
  Opus in-band FEC we already decode.
- **Flushing by packet count, and smart flushing** (neteq.md §2.3). Our bounds are in time and in
  meaning: the hard bound, the ceiling, the sequencer's resync and catch-up. A packet count means
  nothing when frames can be 2.5 to 60 ms long.
- **NetEQ's slow forgetting** (0.983 per 500 ms). It is why WebRTC holds a raised target for a minute
  or more after a queue drains (neteq.md §3.2, P-12).
- **Playing late audio late and stretching back.** NetEQ's choice puts completeness first. Ours, for
  listeners, is settled the other way; for agents the sequencer already drops nothing as late
  (`lateDropUs: Infinity`, ts/GUIDE.md, "Voice: delivery per direction").
- **The buffer level filter** (neteq.md §3.6). NetEQ itself stopped deciding from it.
- **The 10 ms pull and its tick timer.** The worklet already pulls a render quantum at a time on the
  device clock, which is what NetEQ's tick gives it.
- **DTMF, RFC 3389 comfort noise, muted state, codec switching and timestamp scaling.** Telephony and
  RTP concerns our tracks do not have. A talker that stops sending (DTX) is already not counted as an
  underrun once the gap exceeds the ceiling (`audio-worklet.ts:228-237`).
- **`jitterBufferTarget`.** We have its equivalent, `minJitterTargetUs`.

## An answer the register asked for

The voice findings register's open question 7 asks where NetEQ's late cut-off is. In the code there
is none (neteq.md §5): a packet is discarded only once audio after it has been played, that is, when a
later packet was already buffered as its slot came due and NetEQ concealed the slot and moved on;
when the buffer is flushed (200 packets, or smart flushing under its field trial); or when NetEQ stops
waiting because the buffer is 50 ms above target. A late packet with nothing newer behind it is played,
late. So "about 30 ms" is not a NetEQ constant; the words WebRTC lost behind a loaded uplink would be
packets that arrived behind newer ones, or a flush, which the arrival traces could confirm. [inferred]

## The first three things to build

1. **The statistics of recommendation 1.** Small, no behaviour change, and every other change needs
   them to be judged. Land them in the worklet report and `AudioInputStats`, have Huddle's soak harness
   and the voice benchmark's listener record them, and publish them through the client diagnostics of
   #436 and #437 when those merge.
2. **The deadline reorder wait of recommendation 2.** Small, aimed at a measured problem, and it removes
   a knob the guide (#433) currently asks every listener to set. Behind a function-valued
   `reorderWindowUs` first; a default for frame-grouped listeners once Huddle's paired calls show
   fewer underruns than a fixed 60 ms at the same drops.
3. **The arrival-delay floor of recommendation 3**, behind an option. Measure it in Huddle's paired soak
   calls against the 20 and 80 ms floors, then in stage-4c-part-6-style cells on the bench against
   WebRTC. The fades of recommendation 4 are small enough to ride along with any of the three.
