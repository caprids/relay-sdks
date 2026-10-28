# Device latency lab

A static page that measures what the [browser capture recipe](../../spec/media-layer.md#browser-capture)
cannot see: how late a microphone delivers sound, and whether each browser's
`getOutputTimestamp()` and `outputLatency` report the real output delay. It is the measurement rig
for [issue #290](https://github.com/muxinc/relay-sdks/issues/290). It needs no relay, no build and no
dependencies, and it runs in Chrome, Firefox and Safari.

## Run it (macOS)

From a checkout of this repo:

```sh
tools/device-latency/measure.sh
```

The script serves the page on `http://localhost:8290/`, opens it in Chrome, Firefox and Safari in
turn and says what to do in each, collects the JSON file each browser downloads into
`~/device-latency-results/`, and offers to post them to #290: as a secret gist, with the summary
table as a comment. That step needs `gh`, logged in. Without it, the script opens the issue so you
can drag the files in. Allow about 10 minutes per browser.

Without the script, run `python3 tools/device-latency/serve.py` (Python 3.10 or later) and open
`http://localhost:8290/` in each browser. Work through the runs the page lists, click **Download
JSON**, and attach the files to a comment on #290. `python3 tools/device-latency/summarize.py
<files>` prints the Markdown table.

### What you need

- A Mac with its built-in microphone and speakers.
- AirPods, paired and charged. In System Settings → your AirPods, turn off Automatic Ear Detection
  for the session: several runs have you hold an AirPod against the laptop's microphone.
- A USB microphone, if you have one. The USB run is optional.
- A quiet room, and no other app using the microphone.

Chrome on Windows is welcome too: run `serve.py`, open the page in Chrome, and do the same runs with
whatever headset you have.

### The runs

The page lists them in order and shows each one's steps. Each runs for 15 to 30 clicks.

| Run | Output | Microphone under test | Reference microphone |
| --- | --- | --- | --- |
| Self-test | none: the click loops back digitally through a 100 ms delay | none | none |
| Built-in speakers → built-in mic | built-in speakers | built-in | none |
| Built-in speakers → AirPods mic | built-in speakers | AirPods | built-in |
| AirPods output (music mode) → built-in mic | AirPods, mic closed | built-in | none |
| AirPods output (call mode) → built-in mic | AirPods, mic open | built-in | AirPods |
| Built-in speakers → USB mic | built-in speakers | USB | built-in |
| Switch output mid-run | speakers → AirPods → speakers | built-in | none |
| Switch mic mid-run | built-in speakers | built-in → AirPods → built-in | none |
| Clap | none | AirPods | built-in |

The self-test checks the page itself. Every click must read a loopback of 100.0 ms. If one does not,
don't trust the other runs from that browser, and say so on the issue.

## How it measures

Every two seconds (adjustable) the page plays a 20 ms chirp from 500 Hz to 3 kHz. That band
survives a Bluetooth headset's narrowband call codec. It schedules the chirp on an exact `AudioContext` frame `T`, and
records it through an `AudioWorklet`, the way the recipe captures audio. It finds the chirp in the
recording by correlating against the known waveform, which gives the frame `F` where it arrived.
The page opens microphones with echo cancellation, noise suppression and auto gain off, because echo
cancellation would remove the chirp.

The page logs these quantities for each click:

| Field | Meaning |
| --- | --- |
| `loopbackMs` | `(F − T) / sampleRate`: the time from when the click is rendered to when it is captured, all in context time. It covers the real output delay, the air (about 3 ms per metre) and the real input delay. |
| `claimPerfMs` | When the browser says the click is audible: `performanceTime + (T − contextTime)` from the `getOutputTimestamp()` sample nearest `T`. |
| `stampPerfMs` | The broadcast timestamp the recipe gives the captured click, back on the `performance.now()` scale. The page runs the recipe exactly: it calibrates a `sourceTimeline` once a second with `(contextTime + baseLatency + outputLatency, performanceTime)` and stamps each worklet batch with its first frame. |
| `stampMinusClaimMs` | `stampPerfMs − claimPerfMs`. In a perfect world this is only the air path. In practice it is the input delay the recipe cannot see, plus whatever output delay the browser fails to report. |
| `outputLatencyMs`, `baseLatencyMs` | The context's reported values next to that click. |
| `primaryMinusReferenceMs` | The test mic's loopback minus the reference mic's on the same click. The output path is the same for both, so it cancels: what remains is the difference between the two mics' input delays. |
| `mstp.anchoredMinusClaimMs` | Chromium only. The click found in `MediaStreamTrackProcessor` `AudioData`, mapped the way the recipe maps it (the first buffer's timestamp anchored at its arrival time), minus the claim. |

How the runs combine:

- **Input delay of the AirPods mic relative to the built-in mic:** `primaryMinusReferenceMs` in the
  "Built-in speakers → AirPods mic" run, cross-checked by the clap run.
- **Output delay the browser misses:** compare `stampMinusClaimMs` of "AirPods output → built-in mic"
  with "Built-in speakers → built-in mic". The microphone is the same, so the difference is how much
  more real output delay the AirPods have than the browser reports, relative to the speakers.
- **Absolute numbers** need one assumption: that the built-in speakers' reported delay is right.
  Under it, `stampMinusClaimMs` of the built-in pair is the built-in mic's input delay.
- **Device switch:** the switch runs show whether the error steps, and by how much. `calibrations`
  shows whether the recipe re-anchored (`reanchored: true`) at the output switch, and `events` marks
  when the switch happened.
- **Does `AudioData.timestamp` include device latency?** `mstp[].arrivalMinusEndUs` is how long each
  buffer took to arrive after its last sample's timestamp. It is on two different clocks, so only
  the difference between microphones in one page load means anything. If that difference matches
  the measured input delay difference, Chromium's timestamps already subtract the device latency.

The JSON keeps every raw value: per-click frames, every `getOutputTimestamp()` sample (every
200 ms), every calibration, every device event, track settings (Chrome reports a `latency` setting),
browser features, and whether the page was cross-origin isolated. `serve.py` makes it isolated,
for the finest `performance.now()` resolution. A click counts as matched when its normalized
correlation is at least 0.3 and its peak stands at least 12 dB above the median. Both thresholds
are in the file, and the raw values allow a different cut.

## Limits

- No acoustic reference independent of the computer. Absolute input delay rests on the built-in
  speakers' reported delay. The relative numbers do not.
- Safari may refuse to open two microphones at once. The page logs track `mute` and `ended`
  events. The reference run then needs repeating with Reference: None.
- Firefox and Safari have no `AudioContext.setSinkId`: switch output in the macOS Sound menu and
  type the device into "Output in use". The page records what you type with every click.
- A click with no search window left when you press Stop is kept and marked `incomplete`.
