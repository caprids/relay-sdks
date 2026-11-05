# NetEQ, WebRTC's audio jitter buffer, read at code level

| | |
| --- | --- |
| As of | 2026-11-03 |
| Pinned source | [webrtc.googlesource.com/src](https://webrtc.googlesource.com/src) at `3bdd69e14b221e45eaccb8a75f98ff8930c8968b`, committed 2026-11-03 ("Require a non-empty Sframe decrypt error callback") |
| Version | 1 |
| Starting point | [How WebRTC's NetEQ Jitter Buffer Provides Smooth Audio](https://webrtchacks.com/how-webrtcs-neteq-jitter-buffer-provides-smooth-audio/), Fengdeng Lyu, webrtcHacks, 2025-06-03 |
| Written for | relay-3cnp.504 (Adam, 2026-11-03). Its companion, [recommendations.md](recommendations.md), says what our MoQ players should take from it |
| Meant to become | one of the MoQ expert's living documents, kept current against upstream (section 12 lists what would make it wrong) |

Every `file:line` is at the pinned commit. A bare file name is under `modules/audio_coding/neteq/`;
other paths are from the repository root. WebRTC is BSD-3-Clause: this page cites lines and quotes
only short excerpts, and copies no code.

Each claim carries where it came from:

- **[code]**: read in the source at the pin.
- **[article]**: taken from the webrtcHacks article and not checked further, or checked and found to
  differ (then section 11 says how).
- **[g3doc]**: the in-tree overview, `g3doc/index.md`, last reviewed 2021-04-13 and stale in places.
- **[inferred]**: our reading of the code: a consequence worked out from it, not a line that says so.

## Contents

1. [What NetEQ is for](#1-what-neteq-is-for)
2. [The receive pipeline end to end](#2-the-receive-pipeline-end-to-end)
3. [The delay manager: how the target is set](#3-the-delay-manager-how-the-target-is-set)
4. [Time stretching (WSOLA)](#4-time-stretching-wsola)
5. [Reordering and late packets](#5-reordering-and-late-packets)
6. [DTX and comfort noise](#6-dtx-and-comfort-noise)
7. [FEC and RED](#7-fec-and-red)
8. [NACK, which has left NetEQ](#8-nack-which-has-left-neteq)
9. [The statistics it feeds](#9-the-statistics-it-feeds)
10. [Defaults a browser call runs with](#10-defaults-a-browser-call-runs-with)
11. [Where the code has moved on from the article](#11-where-the-code-has-moved-on-from-the-article)
12. [What would invalidate this document](#12-what-would-invalidate-this-document)
13. [Sources and method](#13-sources-and-method)

## 1. What NetEQ is for

NetEQ takes RTP audio packets as they arrive, whenever they arrive, and must hand the audio device
exactly 10 ms of audio each time it asks. Between the two it keeps the encoded packets in timestamp
order, decides each 10 ms what to play, and changes the audio itself when the network does not
supply what that decision needs: it synthesizes audio for missing packets, stitches real audio back
onto synthesized audio, and plays decoded audio slightly faster or slower to move its own delay
towards a target. The target is set from how late packets have been arriving. **[code]**
`api/neteq/neteq.h:146-176` lists the operations and the modes they leave behind.

Three shapes follow from that job, and the rest of this page is their detail:

- **It buffers encoded packets and decodes just in time.** A packet stays encoded in the packet
  buffer until the 10 ms tick that decodes it, shortly before it plays, so a packet that arrives out
  of order, a duplicate, or the forward error correction (FEC) copy of a lost one can still take its
  place until then.
  Only a few tens of milliseconds of decoded audio wait in the sync buffer. **[code]**
- **Every decision is made at playout time, once per 10 ms.** Insertion only stores and measures.
  Decoding, concealment and time stretching are chosen in `GetAudio`, from the state at that moment.
  **[code]**
- **The delay is measured on the network, not on the buffer.** Since 2022 the target and the
  stretching decisions use each packet's arrival delay relative to the fastest packet of the last
  2 s, not how much audio is buffered. **[code]**, history **[article]**

## 2. The receive pipeline end to end

```text
RTP packet ──► InsertPacket ──────────────────────────────────────────────┐
                 RED split ─► codec parse (Opus FEC → secondary packet)   │
                 ─► DTMF out ─► PacketBuffer::InsertPacket (sorted)        │
                 ─► controller.PacketArrived (relative delay → target)    │
                                                                          ▼
                                                              ┌──────────────────┐
audio device ◄── 10 ms ◄── SyncBuffer ◄── algorithm buffer ◄──│ GetAudio, 100/s  │
                                         Normal | Merge |     │  GetDecision     │
                                         Expand | Accelerate  │  ExtractPackets  │
                                         | PreemptiveExpand | │  Decode          │
                                         CNG | DTMF           └──────────────────┘
```

### 2.1 Two calls and a tick

- `InsertPacket` (`neteq_impl.cc:183-435`) stores a packet and updates the delay estimate. It
  decodes nothing. **[code]**
- `GetAudio` (`neteq_impl.cc:437-464`, body `GetAudioInternal`, `neteq_impl.cc:668-902`) returns
  exactly 10 ms: the output size is `kOutputSizeMs` = 10 (`neteq_impl.h:205`). **[code]**
- The first thing each `GetAudio` does is advance a tick timer (`neteq_impl.cc:675`) whose tick is
  10 ms (`api/neteq/tick_timer.h:75`). Packet arrival times are read from this timer
  (`packet_arrival_history.cc:27-28`), so arrivals are measured in the playout device's own time,
  in 10 ms steps. [inferred] Clock drift between sender and receiver therefore shows up as a slow
  trend in arrival delay, which the 2 s sliding minimum of section 3.1 absorbs; the g3doc says the
  same of the older design. **[code]**, **[g3doc]**
- Since 2025-10-30 NetEQ is thread-compatible rather than thread-safe: the internal mutex is gone
  and its callers (`audio/channel_receive.cc`) lock around it (commit `57726e55`). **[code]**

### 2.2 Insertion

`InsertPacket` (`neteq_impl.cc:183-435`), in order: **[code]**

1. Wrap the RTP payload as a `Packet` with timestamp, sequence number and payload type
   (`neteq_impl.cc:194-206`). An empty payload is refused (`neteq_impl.cc:188-191`).
2. On the first packet, or after a payload-type change, flush the packet and DTMF buffers and move the
   sync buffer's end timestamp to this packet (`neteq_impl.cc:208-238`).
3. If the payload type is RED (RFC 2198), split it into one packet per redundant block, each with a
   `red_level` priority, and keep only blocks of the main payload type, DTMF and comfort noise
   (`neteq_impl.cc:240-251`; `red_payload_splitter.cc:35-151`, `red_payload_splitter.cc:153-181`).
4. Pull DTMF events out into their own buffer (`neteq_impl.cc:268-289`).
5. Let the codec parse each payload (`neteq_impl.cc:291-346`). Opus returns two results for a packet
   that carries in-band FEC: the FEC copy of the previous frame at `timestamp - duration` with
   `codec_level` priority 1, and the primary frame with priority 0
   (`modules/audio_coding/codecs/opus/audio_decoder_opus.cc:46-64`). Secondary packets are counted
   (`neteq_impl.cc:348-355`).
6. Insert each packet into the packet buffer, passing it the current target
   (`neteq_impl.cc:357-386`), then tell the controller a packet arrived, which updates the relative
   delay and the target (`neteq_impl.cc:387-392`; section 3). The article lists the delay update
   before the insertion; the code inserts first.

### 2.3 The packet buffer

`packet_buffer.cc`, a list of `Packet` kept in timestamp order. **[code]**

- **Sorted insertion.** A new packet is placed by searching from the back for the first packet it
  is not older than (`packet_buffer.cc:184-185`), so a reordered packet lands in its place, not at
  the end.
- **Duplicates and priorities.** If a packet with the same timestamp and higher priority is already
  there, the new one is discarded; if the one there has lower priority, the new one replaces it
  (`packet_buffer.cc:187-203`). Priority orders `codec_level` first, then `red_level`, lower being
  better (`packet.h:47-54`). So a primary packet always wins over an FEC or RED copy of the same
  audio, whichever arrived first.
- **Too old.** Packets older than the *decode frontier*, the sync buffer's end timestamp (the end
  of the audio already decoded, not of the audio already played), are discarded at the start of each
  decision (`neteq_impl.cc:914-918` calling `packet_buffer.cc:268-278`, with a 5 s horizon against
  timestamp wrap) and after each extraction (`neteq_impl.cc:1892-1898`). This is the only "late
  drop": a packet is usable until the audio before it has been decoded, however late it arrived.
  [inferred] The frontier runs ahead of playout by the decoded audio still in the sync buffer: the
  rest of the last packet (10 ms of a 20 ms packet, for example), or up to 30 ms when a stretch has
  decoded ahead (section 2.4). So "until its turn to be decoded" means until shortly before it would
  play, not until the moment it plays.
- **Full.** At `max_number_of_packets_` (200 by default, `api/neteq/neteq.h:135`) the whole buffer is
  flushed (`packet_buffer.cc:162-179`), counted in `packet_buffer_flushes`
  (`statistics_calculator.cc:331-334`).
- **Smart flushing**, added 2026-06-17 behind the field trial `WebRTC-Audio-NetEqSmartFlushing`
  (commit `732e3439`; `packet_buffer.cc:51-70`): when the buffered span reaches a multiple of
  `max(target, 600 ms)`, drop the oldest packets down to the target, never below 600 ms, instead of
  flushing everything (`packet_buffer.cc:104-127`, `packet_buffer.cc:149-179`; defaults in
  `packet_buffer.h:29-36`). Off unless the trial sets `enabled`; whether Chrome's server-side
  configuration turns it on cannot be read from the code. The commit message gives the motive: a
  buffer that bloats and stays bloated for 10 to 15 s.
- **Span.** `GetSpanSamples` (`packet_buffer.cc:318-340`) is the timestamp distance from the first
  to the last packet plus the last packet's duration, or plus how long the last packet has waited.
  The decision logic uses the waiting-time variant to judge how full the buffer is.

### 2.4 GetAudio: one decision per 10 ms

`GetAudioInternal` (`neteq_impl.cc:668-902`): **[code]**

1. Muted state: when enabled, expansion has faded to nothing and the buffer is empty, return a
   muted frame without any work (`neteq_impl.cc:682-699`). Off by default (`api/neteq/neteq.h:139`).
2. `GetDecision` (`neteq_impl.cc:904-1187`) discards old packets, fills a `NetEqStatus`
   (`neteq_impl.cc:975-998`: packet buffer span with and without waiting time, number of packets,
   whether DTX or comfort noise is buffered, the sync buffer's end timestamp as the *target
   timestamp*, the samples left in the sync buffer, the last mode, the expansion's mute factor, the
   noise generated so far, and the next packet's timestamp), and asks the controller for an
   operation (`neteq_impl.cc:999`).
3. It then overrides the controller in three cases:
   - the next packet is DTX: no merge, accelerate or preemptive expand, play it normally
     (`neteq_impl.cc:1001-1011`), because the buffer level is not updated during DTX;
   - the sync buffer already holds 10 ms or more: play normally from it, unless the decision was
     merge or a time stretch (`neteq_impl.cc:1017-1026`);
   - a time stretch needs 30 ms of audio to work on: decode another packet or fall back to normal
     when it cannot get it (`neteq_impl.cc:1098-1147`, `neteq_impl.cc:1175-1183`).
4. `ExtractPackets` (`neteq_impl.cc:1816-1901`) takes the next packet, and more while they are
   contiguous and fewer than the required samples have been taken. It records each packet's waiting
   time for the statistics (`neteq_impl.cc:1841-1842`, `neteq_impl.cc:1877-1879`).
5. `Decode` (`neteq_impl.cc:1189-1295`) decodes them; a decoder error resets the decoder.
6. The operation runs (`neteq_impl.cc:731-788`) into the algorithm buffer, which is appended to the
   sync buffer (`neteq_impl.cc:807`), and the next 10 ms are read from the sync buffer
   (`neteq_impl.cc:812-814`).
7. Background noise is re-estimated only after operations that passed decoded audio through
   unchanged (`neteq_impl.cc:853-861`), and the playout timestamp is updated, by dead reckoning
   during expansion (`neteq_impl.cc:869-882`).

### 2.5 The decision logic and its operations

`DecisionLogic` (`decision_logic.cc`) is the default `NetEqController`
(`api/neteq/neteq_controller.h`); since 2025-06-27 the delay manager inside it sits behind
`DelayManagerInterface` (`api/neteq/delay_manager_interface.h:19-36`, commit `1542bb18`), so an
embedder can replace it. **[code]**

`GetDecision` (`decision_logic.cc:109-156`) branches on the next packet against the target
timestamp, the timestamp that would continue the audio already decoded: **[code]**

| Situation | Operation | Where |
| --- | --- | --- |
| Last call failed | expand, or reset if a packet is there | `decision_logic.cc:120-127` |
| Next packet is RFC 3389 comfort noise | CNG, or keep the last CNG while it is early | `decision_logic.cc:129-131`, `decision_logic.cc:215-245` |
| No packet at all | expand; codec-internal CNG continues for up to 1 s, then expand | `decision_logic.cc:134-136`, `decision_logic.cc:247-262` |
| Just recovered from expand or CNG, and the buffer holds less than half the target | keep expanding (see PostponeDecode below) | `decision_logic.cc:138-140`, `decision_logic.cc:327-353` |
| Next packet is the one expected | normal, accelerate, fast accelerate or preemptive expand, by playout delay (section 3.5) | `decision_logic.cc:145-147`, `decision_logic.cc:264-286` |
| Next packet is later than expected (a gap) | keep expanding until the expansion has covered the gap, then merge (after expand) or normal (after codec PLC or CNG) | `decision_logic.cc:148-152`, `decision_logic.cc:288-320` |
| Next packet is older than expected (new stream) | undefined, which resets | `decision_logic.cc:153-155` |

The operations: **[code]**

- **Normal** (`neteq_impl.cc:1391-1412`, `normal.cc:52-191`): decoded audio straight to the sync
  buffer. After an expansion it does not start abruptly: it fades in from the expansion's mute
  factor, rising by about 0.64 per 20 ms or fast enough to reach full gain within the frame, and
  cross-fades 1 ms from synthesized audio (`normal.cc:87-161`). After codec PLC it only ends the
  concealment event (`normal.cc:81-83`).
- **Expand** (`neteq_impl.cc:1488-1524`, `expand.cc`): packet-loss concealment. On the first call
  it analyses the recent audio for pitch and a voiced/unvoiced mix (`expand.cc:86-92`); each call
  generates one pitch period of a weighted mix of repeated pitch periods and LPC-filtered noise
  (`expand.cc:102-260`). It fades: from the 3rd consecutive expansion the mute slope is at least
  0.0010 per sample at 8 kHz, from the 7th at least 0.0020 (`expand.cc:262-273`), towards zero
  (`expand.cc:275-298`), and background noise estimated from earlier audio is added underneath
  (`expand.cc:300-308`, `background_noise.cc:121-169`), so a long outage ends in the speaker's room
  tone rather than digital silence. Expansions are capped at 200 in a row for the noise model
  (`expand.h:78`). If the decoder offers its own PLC, `DoCodecPlc` uses it instead
  (`neteq_impl.cc:743-747`, `neteq_impl.cc:1446-1486`); for Opus that is off unless the field trial
  `WebRTC-Audio-OpusGeneratePlc` is set (`modules/audio_coding/codecs/opus/audio_decoder_opus.cc:34`,
  `modules/audio_coding/codecs/opus/audio_decoder_opus.cc:134-150`), so by default Opus loss is
  concealed by NetEQ's own expand.
- **Merge** (`neteq_impl.cc:1414-1444`, `merge.cc:51-167`): the first packet after a gap that
  expansion covered. It generates more expansion, finds the lag at which the new audio best
  correlates with it (`merge.cc:90-101`), scales the new audio to the expansion's energy and ramps
  it up (`merge.cc:104-136`), and overlap-adds the two (`merge.cc:138-148`). The expansion samples
  that the merge did not use are taken back out of the concealment count
  (`neteq_impl.cc:1421-1433`).
- **Accelerate** and **fast accelerate** (`neteq_impl.cc:1526-1597`, `accelerate.cc:23-99`): remove
  one pitch period (fast: as many periods as fit in 15 ms) from 30 ms of decoded audio. Section 4.
- **Preemptive expand** (`neteq_impl.cc:1599-1663`, `preemptive_expand.cc:23-106`): insert one pitch
  period. Section 4.
- **RFC 3389 CNG** and **codec-internal CNG** (`neteq_impl.cc:1665-1704`, `comfort_noise.cc:33-129`):
  section 6.
- **DTMF** (`neteq_impl.cc:1706-1814`): tone generation, outside this page's scope.

Two guards shape every recovery from an outage: **[code]**

- **PostponeDecode** (`decision_logic.cc:327-353`): after expansion or CNG, if the buffer holds less
  than half the target (`kPostponeDecodingLevel` = 50, `decision_logic.cc:33`), keep concealing,
  unless DTX or CNG is buffered, and after expansion only if it has already faded below half gain.
  So a short dropout resumes at once with whatever arrived, and only a long one waits for the
  buffer to refill to half the target before speech resumes.
- **PacketTooEarly** (`decision_logic.cc:355-359`, used at `decision_logic.cc:298-300`): when a
  later packet is buffered but the expected one is missing, keep expanding until the noise and
  concealment generated have covered the timestamps up to it, so the later packet plays at its own
  time; but stop waiting when the buffer is more than 50 ms above the target
  (`decision_logic.cc:293-298`, `kTargetLevelWindowMs` = 100 halved).

### 2.6 The sync buffer

`SyncBuffer` (`sync_buffer.h:26`, `sync_buffer.cc`) holds decoded audio: up to 120 ms plus 60 ms at
48 kHz (`neteq_impl.h:206-210`), with a `next_index` that splits what has been played (kept as
history for expand, merge and time stretching to correlate against) from what is still to play
(`FutureLength`). It always keeps an overlap of 5 samples at 8 kHz (0.625 ms) of future audio for the
next cross-fade (`expand.cc:48`, `neteq_impl.cc:823-832`). Its end timestamp is the target timestamp
of section 2.5, and it advances only when packets are extracted, never during expansion
(`sync_buffer.cc:99`, `neteq_impl.cc:1158-1166`). That is how NetEQ knows, after an outage, whether
the next packet is the one it was waiting for (a late packet) or a later one (a loss). **[code]**

The article describes the same role: it holds the rest of a decoded packet longer than 10 ms, and
it is what the DSP borrows from. **[article]**, confirmed **[code]**

## 3. The delay manager: how the target is set

### 3.1 Relative arrival delay

`PacketArrivalHistory` (`packet_arrival_history.h:27-106`, `packet_arrival_history.cc`). **[code]**

- Each packet's arrival is stored with its RTP timestamp and length (`packet_arrival_history.cc:25-64`)
  in a 2 s window (`kPacketHistorySizeMs`, `decision_logic.cc:38`). The window is measured in media
  time: a packet falls out when its RTP timestamp is more than 2 s behind the newest one
  (`packet_arrival_history.cc:114-121`).
- Two monotonic deques track the packets with the least and the most `arrival − timestamp` in the
  window, leaving out reordered packets (`packet_arrival_history.cc:52-62`,
  `packet_arrival_history.h:102-105`).
- A packet's **relative delay** is its `arrival − timestamp` less that of the fastest packet in the
  window, never below 0 (`packet_arrival_history.cc:99-112`). The fastest packet is the reference
  because the sender's clock offset is unknown and cancels out in the difference.
- A packet is **reordered** when it is not the newest RTP timestamp in the history
  (`packet_arrival_history.cc:90-97`, `decision_logic.cc:191-192`). A duplicate, or one fully covered
  by a packet already there, is not inserted at all (`packet_arrival_history.cc:34-36`,
  `packet_arrival_history.cc:123-130`).
- The same history answers "how late is the audio playing now": the delay of the timestamp now at the
  playout point, measured as if it were a packet arriving now (`packet_arrival_history.cc:73-81`,
  `decision_logic.cc:361-366`). That is the **playout delay** of section 3.5.

`DecisionLogic::PacketArrived` (`decision_logic.cc:170-195`) inserts the packet, needs two packets
before it reports a delay, and passes the delay and the reordered flag to the delay manager.
Comfort noise and DTMF are not measured (`decision_logic.cc:174-176`).

### 3.2 The underrun optimizer: a forgetting histogram and a quantile

`UnderrunOptimizer` (`underrun_optimizer.h:22-47`, `underrun_optimizer.cc:37-65`) and `Histogram`
(`histogram.cc`). **[code]**

- 100 buckets of 20 ms: relative delays up to 2 s (`underrun_optimizer.cc:22-23`,
  `underrun_optimizer.cc:58-62`).
- **Resampled every 500 ms**: the histogram is not updated per packet. It takes the largest relative
  delay seen in each 500 ms interval and adds that one value when the interval ends
  (`underrun_optimizer.cc:39-56`; `resample_interval_ms` = 500, `delay_manager.h:36`).
- **Forgetting**: each update multiplies every bucket by the forget factor and adds `1 − factor` to
  the new value's bucket, so the histogram stays a probability distribution
  (`histogram.cc:35-80`). The factor is 0.983 (`delay_manager.h:34`). After a reset it starts at 0
  and rises as `1 − 2/(n+1)` (`start_forget_weight` = 2, `delay_manager.h:35`,
  `histogram.cc:84-103`), so the first few intervals count heavily and the histogram converges fast.
- **The target** is the upper edge of the bucket at which at most 5% of the mass lies above
  (`quantile` = 0.95, `delay_manager.h:33`; `histogram.cc:106-127`;
  `underrun_optimizer.cc:63-64`). Its least value is therefore 20 ms.

[inferred] What those constants mean in time, with one update per 500 ms:

- The histogram's memory is about `1/(1 − 0.983)` ≈ 59 updates, about 29 s.
- A higher delay raises the target once it has been the worst of at least 3 intervals in a row,
  about 1.5 s: `1 − 0.983³` ≈ 5.01%, just over the 5% tail. One bad interval alone does not.
- A target raised by a long episode comes down slowly. If all the mass sat in the high bucket, it
  takes `ln 0.05 / ln 0.983` ≈ 175 updates, about 87 s, to fall below 5%; after 30 s at the higher
  delay, about 75 s. This slow give-back is consistent with WebRTC staying at 185-205 ms in the 30 s
  after a deep-queue download ended in relay-dev's voice benchmark, where our player's faster
  give-back returned to about 110 ms (finding P-12 in `moq-netbench/results/voice/FINDINGS.md`).
- The start target is 80 ms (`kStartDelayMs`, `delay_manager.cc:27`) until the first estimate.

### 3.3 The reorder optimizer: pay delay only where it buys audio

`ReorderOptimizer` (`reorder_optimizer.h:20-41`, `reorder_optimizer.cc:34-75`), on by default
(`use_reorder_optimizer`, `delay_manager.h:38`). **[code]**

- A second forgetting histogram, updated per packet (no resampling) with factor 0.9993
  (`delay_manager.h:39`): an in-order packet adds to bucket 0, a reordered packet to the bucket of
  its relative delay (`reorder_optimizer.cc:37-41`). [inferred] At 50 packets a second its memory is
  about 1,400 packets, about 29 s.
- For each candidate delay it computes a cost: the delay **above the underrun optimizer's target**
  plus 20 ms for each percent of packets that would arrive later than it (`ms_per_loss_percent` = 20,
  `delay_manager.h:40`; `reorder_optimizer.cc:51-75`), and picks the cheapest.
- Reordered packets do not feed the underrun histogram while this optimizer is on
  (`delay_manager.cc:79-81`), and the final target is the larger of the two (`delay_manager.cc:82-88`).

The article reads the cost as `delay_ms + 20ms * loss_percent` with "loss_percent" defaulting to 20.
In the code the 20 is the price of a percent, and the delay counted is only the part above the
underrun target. The article's reading of what it is for stands: in WebRTC, reordering comes mostly
from retransmitted packets, so this is the knob that decides whether waiting for retransmissions is
worth its delay. **[article]**, checked **[code]**

### 3.4 Constraints on the target

`DelayConstraints` (`delay_constraints.cc`), applied in `DecisionLogic::TargetLevelMs`
(`decision_logic.cc:158-160`). **[code]**

- Raised to the effective minimum: the larger of the application's minimum delay (used for audio/video
  sync) and the base minimum delay (`delay_constraints.cc:31-35`, `delay_constraints.cc:99-106`).
  The base minimum is what `RTCRtpReceiver.jitterBufferTarget` sets
  (`pc/audio_rtp_receiver.cc:331-338`).
- Lowered to the maximum delay if one is set, and to 75% of what the packet buffer can hold
  (`delay_constraints.cc:36-41`): 150 packets of 20 ms, 3 s, at the default 200.
- The unclamped value is kept as well and reported (`decision_logic.cc:162-164`; section 9).

### 3.5 Playout delay against the target: the stretch thresholds

`ExpectedPacketAvailable` (`decision_logic.cc:264-286`), the only place accelerate and preemptive
expand are chosen. **[code]**

- No time stretching right after an expansion, while DTMF plays, or when time stretching is disabled
  (`decision_logic.cc:266-267`).
- `low = target`; `high = target + the largest relative delay in the 2 s history + 20 ms`
  (`decision_logic.cc:269-272`). The 20 ms is `kDelayAdjustmentGranularityMs`: one stretch moves the
  delay by up to 15 ms, rounded up to the 10 ms clock (`decision_logic.cc:35-37`).
- Playout delay at or above `4 × high`: fast accelerate (`decision_logic.cc:273-275`). This branch
  comes before the rate limit below, so it is not spaced. Fast mode itself (several periods at a
  lower correlation, section 4) needs the embedder to enable it; when it is off, as in a browser
  (section 10), the operation runs as an ordinary accelerate (`neteq_impl.cc:752-758`). [inferred]
  So far above the band NetEQ accelerates at every decision that has the 30 ms it needs, one pitch
  period at a time, without the 50 ms spacing.
- Otherwise, and only if 5 ticks (50 ms) have passed since the last successful stretch
  (`decision_logic.h:112-113`, `decision_logic.h:136-140`, `decision_logic.cc:111-114`): at or above
  `high`, accelerate; below `low`, preemptive expand (`decision_logic.cc:276-283`). The header
  comment puts the resulting ceiling at "about 100 ms/s" of stretching, which holds inside
  `4 × high`.
- In between: normal. [inferred] The band is wide on purpose. While packets keep arriving up to
  `max delay` late, playing that much above the target is what the next late packet will use, so
  NetEQ does not shave it off only to stretch it back. This is the article's "better way" (section
  11), with a dead band the article does not mention.

### 3.6 The buffer level filter, now a side channel

`BufferLevelFilter` (`buffer_level_filter.cc:30-62`) is an exponential filter of the packet buffer's
span, with a factor from 251/256 to 254/256 by target size, corrected for the samples a stretch added
or removed (`decision_logic.cc:197-213`). It is still updated each decision outside expansion and CNG
(`decision_logic.cc:115-117`) and reset when a gap is jumped (`decision_logic.cc:301-306`), but no
operation is chosen from it any more: `UnderTargetLevel` (`decision_logic.cc:322-325`) has no caller
in the file. Its one consumer is `FilteredCurrentDelayMs` (`neteq_impl.cc:532-539`), the delay
reported for audio/video synchronization and histograms (`audio/channel_receive.cc:1188-1189`,
`audio/channel_receive.cc:449-453`). Before 2022 this filter, against the target, drove the
stretching. **[code]**, history **[article]**

## 4. Time stretching (WSOLA)

`TimeStretch` (`time_stretch.cc:29-164`), specialised by `Accelerate` and `PreemptiveExpand`.
**[code]**

1. Work on 30 ms of decoded audio (`accelerate.cc:28-36`, `preemptive_expand.cc:30-40`), borrowing
   already-decoded samples back from the sync buffer when the new packet is shorter
   (`neteq_impl.cc:1531-1545`, `neteq_impl.cc:1603-1623`). Stereo is analysed on the first channel
   only (`time_stretch.cc:41-56`).
2. Downsample to 4 kHz and autocorrelate lags of 10 to 60 samples, 2.5 to 15 ms
   (`time_stretch.cc:61-65`, `time_stretch.cc:166-178`, `time_stretch.h:85-90`); take the strongest
   peak as the pitch period (`time_stretch.cc:67-84`).
3. Correlate the period ending at 15 ms with the period starting there, normalised by their energies
   (`time_stretch.cc:92-145`).
4. A simple voice activity check compares the energy with 8 times the background noise
   (`time_stretch.cc:180-218`). Quiet audio is stretched whatever the correlation.
5. Stretch only when the normalised correlation is above 0.9 (`kCorrelationThreshold` = 14746 in
   Q14, `time_stretch.h:90`; 0.5 in fast mode, `accelerate.cc:57-60`) or the audio is quiet:
   - accelerate cross-fades the period after 15 ms onto the one before it, removing one period
     (`accelerate.cc:61-93`);
   - preemptive expand cross-fades a copy of the period before 15 ms in, adding one period, and
     needs at least 15 ms of not-yet-played audio (`preemptive_expand.cc:68-100`).
6. Otherwise copy the audio unchanged and record a failed stretch (`accelerate.cc:94-98`,
   `neteq_impl.cc:1552-1566`). Success, low-energy success and failure are separate modes, so the
   decision logic's rate limit counts only real stretches (`decision_logic.cc:41-46`).

The removed and inserted samples are counted (`neteq_impl.cc:1551`, `neteq_impl.cc:1629`) as
`removedSamplesForAcceleration` and `insertedSamplesForDeceleration` (section 9).

## 5. Reordering and late packets

All **[code]** unless marked.

- **A reordered packet is not late until the audio before it has been decoded.** It is inserted in
  timestamp order (section 2.3) and decoded when its turn comes, which is shortly before it plays
  (the decode frontier of section 2.3). There is no reorder window: the window is however much audio
  is ahead of it, less what is already decoded.
- **A missing packet with a later one buffered is a loss, decided at its playout time.** NetEQ
  expands for exactly the missing duration (`decision_logic.cc:288-300`), then merges with the later
  packet (`decision_logic.cc:308-311`). The timeline is kept. If the missing packet arrives after
  that, it is behind the decode frontier and is discarded (`packet_buffer.cc:268-278`), counted in
  `packetsDiscarded` (`packet_buffer.cc:354-360`).
- **A missing packet with nothing buffered is a delay.** NetEQ expands for as long as it takes. When
  the expected packet arrives it is played normally after the expansion
  (`decision_logic.cc:145-147`, `normal.cc:87-90`), so the audio now plays later by the length of the
  expansion. That expansion is counted as a *delayed packet outage* (`expand.cc:329-335`,
  `statistics_calculator.cc:344-353`). The excess delay is then removed by accelerate (section 3.5).
- **Arrival statistics still learn from late and reordered packets.** `PacketArrived` runs when the
  packet is inserted (`neteq_impl.cc:387-392`), before any discard, so a packet that came too late to
  play still raises the reorder histogram, and the target can grow to catch the next one if the
  cost function of section 3.3 says it is worth it. [inferred]
- **A stale backlog is bounded by flushing**, not by a late-drop rule: the full flush at 200 packets
  and, with the trial, smart flushing (section 2.3).

## 6. DTX and comfort noise

**[code]**

- **Opus DTX** packets are flagged by the decoder (`IsDtxPacket`) and decode to codec-internal comfort
  noise. When no packet follows, the decision continues codec-internal CNG for up to 1 s
  (`kCngTimeoutMs`, `decision_logic.cc:39`, `decision_logic.cc:251-257`), then expands.
- **RFC 3389 CNG** (a separate CN payload type) generates noise from SID parameters
  (`comfort_noise.cc:33-129`). If a CN packet would wait more than 1.5 times the target, the noise
  is fast-forwarded to cut the wait (`decision_logic.cc:215-245`); CN packets that would replay a
  timestamp already played are dropped (`neteq_impl.cc:929-946`).
- **No stretching in DTX**: the buffer level is not filtered during CNG or expansion
  (`decision_logic.cc:115-117`), and a DTX packet is never merged or stretched
  (`neteq_impl.cc:1001-1011`). PostponeDecode does not hold back a DTX or CN packet
  (`decision_logic.cc:337-341`). A DTX packet's span counts at least its waiting time
  (`packet_buffer.cc:330-335`).
- [inferred] The relative delay needs no DTX special case: it compares arrival with timestamp, and the
  timestamp jumps across the silence.

## 7. FEC and RED

**[code]**

- **Opus in-band FEC** becomes a secondary packet at insertion (section 2.2). It sits in the packet
  buffer at the lost frame's timestamp and is decoded only if the primary for that timestamp has not
  arrived when its turn to be decoded comes. A primary that arrives before then, earlier or later than
  the copy, takes its place (`packet_buffer.cc:187-203`). [inferred] FEC therefore works only when
  the next packet has arrived before the lost one's turn to be decoded, which the 20 ms least
  target allows for one 20 ms frame.
- A secondary packet is decoded through `DecodeRedundant`
  (`modules/audio_coding/codecs/opus/audio_decoder_opus.cc:81-100`), counted as
  `secondary_decoded_samples` (`neteq_impl.cc:1851-1854`) and in `fecPacketsReceived`; one dropped
  unused counts in `fecPacketsDiscarded` (`packet_buffer.cc:354-360`,
  `statistics_calculator.cc:276-283`).
- **RED** (RFC 2198) is split at insertion into one packet per block with `red_level` priority
  (`red_payload_splitter.cc:35-151`); redundant blocks of another payload type than the main one are
  dropped (`red_payload_splitter.cc:153-181`). The packet buffer's priority rule keeps the primary.

## 8. NACK, which has left NetEQ

**[code]** unless marked.

- `NackTracker` moved from NetEQ to `ChannelReceive` on 2025-11-03 (commit `3d6d0104`), now
  `audio/nack_tracker.{h,cc}`. The NACK methods on the NetEQ interface are deprecated.
- A *fixed delay mode* was added 2025-11-19 (`88083095`), made the default 2026-02-05 (`770eedbe`) and
  the old mode, which weighed the expected playout time and loss rate, removed 2026-04-16
  (`871e440c`).
- Now: every arriving packet updates the list of missing sequence numbers
  (`audio/nack_tracker.cc:48-107`), and the list is requested at once
  (`audio/channel_receive.cc:726-734`). A missing packet is asked for while the time since its
  estimated timestamp plus the round trip is under 1 s (`audio/nack_tracker.cc:26-29`,
  `audio/nack_tracker.cc:129-147`; 100 ms when no RTT is known). The RTCP layer sends new sequence
  numbers at once and the full list again at most every `5 + 1.5 × RTT` ms
  (`modules/rtp_rtcp/source/rtp_rtcp_impl2.cc:573-631`).
- NACK is negotiated per call: the sender keeps 5 s of history when it is on
  (`media/engine/webrtc_voice_engine.cc:128`, `:325`). relay-dev's voice benchmark ran its WebRTC
  cells with it off by default and saw no retransmissions until it was turned on (finding C-1; not
  read in code here).
- **NetEQ does not wait for a retransmission as such.** A retransmitted packet is an ordinary late or
  reordered packet: it plays if it arrives before its turn to be decoded (section 5), and its lateness
  feeds the reorder optimizer, which may raise the target. The article says the same; the code has
  not changed that, only moved the tracker. **[article]**, checked **[code]**

## 9. The statistics it feeds

`StatisticsCalculator` (`statistics_calculator.cc`) keeps lifetime counters
(`api/neteq/neteq.h:62-96`), which `ChannelReceive` copies (`audio/channel_receive.cc:1016-1046`)
and the stats collector exposes on `RTCInboundRtpStreamStats` (`pc/rtc_stats_collector.cc:422-439`,
`pc/rtc_stats_collector.cc:490-531`). **[code]**

| getStats field | What NetEQ counts | Where |
| --- | --- | --- |
| `totalSamplesReceived` | every output sample, once decoded audio has played | `statistics_calculator.cc:285-299` |
| `concealedSamples` | samples produced by expand or codec PLC, less what merge took back | `statistics_calculator.cc:157-193`, `statistics_calculator.cc:218-245` |
| `silentConcealedSamples` | the part that was only noise (expansion muted, or after CNG) | `statistics_calculator.cc:167-175`, `statistics_calculator.cc:238-244` |
| `concealmentEvents` | expansions that start after a non-expansion | `statistics_calculator.cc:164`, `statistics_calculator.cc:174`, `neteq_impl.cc:1494` |
| `insertedSamplesForDeceleration` | samples preemptive expand added | `statistics_calculator.cc:247-254` |
| `removedSamplesForAcceleration` | samples accelerate removed | `statistics_calculator.cc:256-263` |
| `jitterBufferDelay` / `jitterBufferEmittedCount` | per decoded packet, its time in the **packet buffer** times its samples; and the samples | `neteq_impl.cc:1841`, `neteq_impl.cc:1877-1879`, `statistics_calculator.cc:312-325` |
| `jitterBufferTargetDelay` / `jitterBufferMinimumDelay` | the target, and the target before the constraints of section 3.4, each times the samples | `statistics_calculator.cc:318-321` |
| `packetsDiscarded` | primary packets dropped as too old, duplicate or flushed | `packet_buffer.cc:354-360` |
| `fecPacketsReceived` / `fecPacketsDiscarded` | secondary packets parsed; and dropped unused | `statistics_calculator.cc:276-283` |
| `interruptionCount` / `totalInterruptionDuration` | concealment events of at least 150 ms | `statistics_calculator.cc:35`, `statistics_calculator.cc:199-216` |
| (not in the spec) `relativePacketArrivalDelay`, `delayedPacketOutageSamples` | the sum of section 3.1's delays; expansion that ended in a late packet rather than a loss | `statistics_calculator.cc:340-353`, `audio/audio_receive_stream.cc:324-325` |

The output counters (samples received, concealment, stretching, generated noise, interruptions)
count nothing until decoded audio has first played (`decoded_output_played_`,
`statistics_calculator.cc:159-161`), so a slow start is not concealment. The packet counters
(discarded, FEC), the relative arrival delay and `jitterBufferDelay` have no such guard
(`statistics_calculator.cc:272-283`, `statistics_calculator.cc:312-325`,
`statistics_calculator.cc:340-342`). `jitterBufferDelay` covers
only the packet buffer, not the decoded audio in the sync buffer nor the device. [inferred]

## 10. Defaults a browser call runs with

**[code]**, at the pin; field trials and Chrome's server-side configuration can change them.

| Setting | Default | Where |
| --- | --- | --- |
| Packet buffer | 200 packets; a PeerConnection's `audio_jitter_buffer_max_packets` default, never below 20 | `api/neteq/neteq.h:135`, `api/peer_connection_interface.h:496`, `media/engine/webrtc_voice_engine.cc:2309-2311` |
| Fast accelerate | fast mode off; the `4 × high` branch still accelerates, unspaced (section 3.5) | `api/neteq/neteq.h:138`, `api/peer_connection_interface.h:500` |
| Muted state | off | `api/neteq/neteq.h:139` |
| Quantile, forget factor, resampling, reorder optimizer | 0.95, 0.983, 500 ms, on (0.9993, 20 ms per %); field trial `WebRTC-Audio-NetEqDelayManagerConfig` | `delay_manager.h:28-40`, `delay_manager.cc:41-51` |
| Start target, least target | 80 ms, 20 ms | `delay_manager.cc:27`, `underrun_optimizer.cc:64` |
| Arrival history | 2 s | `decision_logic.cc:38` |
| Opus decoder PLC | off (`WebRTC-Audio-OpusGeneratePlc`); NetEQ's expand conceals | `modules/audio_coding/codecs/opus/audio_decoder_opus.cc:34` |
| Smart flushing | off (`WebRTC-Audio-NetEqSmartFlushing`) | `packet_buffer.cc:51-70` |
| NACK | negotiated; when on, fixed delay mode, 1 s | `audio/nack_tracker.cc:29`, `media/engine/webrtc_voice_engine.cc:325` |

## 11. Where the code has moved on from the article

The article (2025-06-03) is the best prose introduction and its architecture still holds: the
three-part split into `neteq_impl`, `delay_manager` and `decision_logic`, relative delay against the
fastest packet in a 2 s window, the two histograms, and playout delay rather than buffer length as
the controlled quantity. Where the pinned code differs:

| The article | The code at the pin | Since |
| --- | --- | --- |
| The underrun histogram is updated per packet; with 20 ms packets the 0.983 forget factor lets a new 20 ms target take over from 140 ms in about 3.5 s (175 packets) | Updated once per 500 ms with that interval's largest delay (`underrun_optimizer.cc:39-56`), so the forget factor acts per 500 ms: memory about 29 s, a full take-over about 87 s (section 3.2) | Already so at the article's date (`delay_manager.h` at `8e2442f1`, 2025-06-03, has `resample_interval_ms = 500`) |
| The underrun histogram covers "the entire session" | It forgets, with about 29 s of memory | Already so |
| Reorder cost `delay_ms + 20ms * loss_percent`, `loss_percent` defaulting to 20 | Cost is the delay above the underrun target plus `ms_per_loss_percent` (20) per percent late (`reorder_optimizer.cc:51-75`) | Already so; a misreading |
| Decision logic accelerates when the playout delay is above the target and decelerates below it | Accelerates only above `target + max relative delay in 2 s + 20 ms`, with 50 ms between stretches, and without that spacing at 4 times the threshold (fast mode only if enabled) (`decision_logic.cc:264-286`) | Already so; simplified |
| `delay_manager` takes the target from the two histograms | Still, but the delay manager is now behind `DelayManagerInterface` and replaceable | 2025-06-27, `1542bb18` |
| NetEQ requests retransmissions with RTCP NACK | The NACK tracker lives in `ChannelReceive`; fixed delay mode (missing packets asked for while less than 1 s old) is the default and the only mode | 2025-11-03 `3d6d0104`; 2026-02-05 `770eedbe`; 2026-04-16 `871e440c` |
| A full buffer is flushed | Still the default; smart flushing to the target exists behind a field trial | 2026-06-17, `732e3439` |
| `InsertPacket` updates the delay manager, then inserts | Inserts, then updates (`neteq_impl.cc:366-392`) | Already so |
| `GetAudio` "might extract packet from Sync Buffer" | Packets come from the packet buffer; the sync buffer holds decoded audio | Already so; a slip |
| (not covered) | The buffer level filter no longer drives any decision (section 3.6) | Since the 2022 rework the article describes |
| (not covered) | PostponeDecode, PacketTooEarly, delayed packet outage versus loss (sections 2.5, 5) | Long-standing |

Smaller changes since the article that do not alter behaviour: NetEQ made thread-compatible
(2025-10-30, `57726e55`); `SetMaximumBufferPackets` and `SetFastAccelerate` on the interface
(2026-01-09, `faea246f`); bounds fixes in peak detection, merge and the cross-fade after CNG or
expand (`52f57b3f`, `9da2684f`, `ff1be2e8`). The in-tree `g3doc/index.md` still describes the
pre-2022 design (inter-arrival times, filtered buffer level against the target).

## 12. What would invalidate this document

Re-read the named lines when any of these changes upstream, and bump the version:

1. `decision_logic.cc` `ExpectedPacketAvailable` or `FuturePacketAvailable`: the stretch thresholds,
   the rate limit, or the gap handling (sections 2.5, 3.5, 5).
2. The `DelayManager::Config` defaults (`delay_manager.h:28-40`), `kStartDelayMs`, the bucket size,
   or the default of `WebRTC-Audio-NetEqDelayManagerConfig`; or Chrome shipping a non-default
   controller through `CustomNetEqControllerFactory` or a new `DelayManagerInterface` (section 3).
3. `PacketArrivalHistory`: the 2 s window, the reference packet, or arrival measured on another clock
   (section 3.1).
4. `WebRTC-Audio-NetEqSmartFlushing` turned on by default (section 2.3).
5. `WebRTC-Audio-OpusGeneratePlc` turned on by default, which moves Opus concealment from NetEQ's
   expand to libopus (section 2.5).
6. The NACK tracker's mode or its 1 s limit (section 8).
7. Any change to how `jitterBufferDelay`, `concealedSamples` or the interruption counters are
   computed, or to the W3C definitions they follow (section 9), since recommendations.md compares
   our stats to them.
8. Packet buffer insertion or priority rules (`packet_buffer.cc:133-206`), which decide how reordered,
   duplicate and FEC packets meet (sections 2.3, 5, 7).

## 13. Sources and method

- **Code**: a shallow, sparse clone of `https://webrtc.googlesource.com/src` at the pin, covering
  `modules/audio_coding/`, `api/neteq/`, `audio/`, `modules/rtp_rtcp/source/` and `pc/`, deepened to
  2025-04-01 for the commit history in section 11. Other files were read with `git show` at the pin.
  Every cited line was read; the unit tests were not run.
- **Article**: Fengdeng Lyu, [How WebRTC's NetEQ Jitter Buffer Provides Smooth Audio](https://webrtchacks.com/how-webrtcs-neteq-jitter-buffer-provides-smooth-audio/),
  webrtcHacks, 2025-06-03, read 2026-11-03.
- **In-tree doc**: `modules/audio_coding/neteq/g3doc/index.md` (reviewed 2021-04-13).
- **Measurements quoted** (not made here): relay-dev `moq-netbench/results/voice/FINDINGS.md`
  (findings C-1 and P-12).

## Changelog

- 2026-11-03, version 1: first version (relay-3cnp.504).
