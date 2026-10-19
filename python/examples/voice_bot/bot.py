"""A parrot voice bot over Relay: it listens to a browser user's microphone, waits for a pause, and says something
back. When the user talks over it, it stops and sends an interrupt, so the user's player drops what it still holds.

    RELAY_TOKEN=... mise run python:example:voice-bot -- --namespace mattypoo/voice-bot

The user's page publishes its microphone at ``<namespace>/user`` and the bot answers at ``<namespace>/bot``, with an
Opus ``audio`` track and the ``interrupt`` track listed in its catalog. Speech-to-text, a language model and
text-to-speech plug in through one async function, :data:`Respond`: it gets what the user said as PCM and yields the
reply as PCM. The default, :func:`parrot`, says the user's words back. Voice activity detection is an energy gate in
this example, not SDK code.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import math
import os
import sys
from collections import deque
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass
from getpass import getpass
from typing import Any, Literal
from urllib.parse import urlsplit

import numpy as np

from moq_media import (
    AUDIO_TRACK,
    CATALOG_TRACK,
    INTERRUPT_TRACK,
    OPUS_FRAME_US,
    AudioTrackInfo,
    BroadcastClock,
    DataTrackInfo,
    FrameSequencer,
    PlayoutClock,
    SequencedFrame,
    build_interrupt,
    build_msf_catalog,
    loc_track_properties,
)
from moq_media.opus import OpusDecoder, OpusEncoder, OpusPacket
from mux_relay import (
    RelayPublisher,
    RelayTrackWriter,
    RestartReason,
    SupervisedSubscriber,
    connect_relay_publisher,
    connect_supervised_subscriber,
)
from mux_relay.media import CatalogService, FrameReader, FrameWriter, media_track_properties

DEFAULT_ENDPOINT = "https://sjc.relay.mux.global"

# How long after the end of a reply its audio may still be playing at a listener: the audio-only target latency, the
# most a listener's jitter buffer holds. Talking over it then still interrupts.
_HELD_US = 500_000
# How far behind the clock a reply's next audio may fall before the bot restamps it: two frames.
_STARVED_US = 2 * OPUS_FRAME_US
# How late a packet may be written before the frame writer drops it as stale.
_STALE_US = 100_000

Log = Callable[[str, dict[str, Any]], None]


@dataclass(frozen=True, slots=True)
class Utterance:
    """What the user said: mono s16 PCM at ``rate``, and where it lies on the user's broadcast timeline."""

    pcm: bytes
    rate: int
    from_us: int
    until_us: int


Respond = Callable[[Utterance], AsyncIterator[bytes]]
"""The bot's brain: given an utterance, yield the reply as mono s16 PCM at the utterance's rate, in chunks of any size.

Plug speech-to-text, a language model and text-to-speech in here. The bot speaks each chunk as it arrives and cancels
the iterator when the user talks over it.
"""


async def parrot(utterance: Utterance) -> AsyncIterator[bytes]:
    """Say the user's words back."""
    yield utterance.pcm


GateEvent = Literal["start", "end"]


class EnergyGate:
    """A voice activity detector good enough for an example: speech is audio louder than ``threshold_dbfs``.

    An utterance starts after ``start_ms`` of speech without a break, and ends after ``hangover_ms`` of quiet or at
    ``max_ms``. Feed it decoded audio a frame at a time.
    """

    def __init__(
        self,
        *,
        rate: int,
        threshold_dbfs: float = -45.0,
        start_ms: int = 100,
        hangover_ms: int = 700,
        max_ms: int = 15_000,
    ) -> None:
        self.rate = rate
        self.threshold_dbfs = threshold_dbfs
        self.start_ms = start_ms
        self.hangover_ms = hangover_ms
        self.max_ms = max_ms
        self.open = False
        self._loud_ms = 0.0
        self._quiet_ms = 0.0
        self._open_ms = 0.0

    @property
    def quiet_ms(self) -> float:
        """How long it has been quiet since the last speech, while open, and at the end of the last utterance."""
        return self._quiet_ms

    def reset(self) -> None:
        """Forget everything heard, for a discontinuity."""
        self.open = False
        self._loud_ms = self._quiet_ms = self._open_ms = 0.0

    def push(self, pcm: bytes) -> GateEvent | None:
        """Hear one frame; return ``"start"`` or ``"end"`` when the utterance does."""
        samples = np.frombuffer(pcm, dtype="<i2")
        if samples.size == 0:
            return None
        duration_ms = samples.size * 1000 / self.rate
        rms = math.sqrt(float(np.mean(np.square(samples.astype(np.float64)))))
        loud = rms > 0 and 20 * math.log10(rms / 32768) > self.threshold_dbfs
        if not self.open:
            self._loud_ms = self._loud_ms + duration_ms if loud else 0.0
            if self._loud_ms >= self.start_ms:
                self.open = True
                self._quiet_ms = 0.0
                self._open_ms = self._loud_ms
                return "start"
            return None
        self._open_ms += duration_ms
        self._quiet_ms = 0.0 if loud else self._quiet_ms + duration_ms
        if self._quiet_ms >= self.hangover_ms or self._open_ms >= self.max_ms:
            self.open = False
            self._loud_ms = 0.0
            return "end"
        return None


class Speaker:
    """Speaks replies on the bot's audio track in real time, and interrupts them.

    Each reply is one run of the Opus encoder, stamped from the broadcast clock when its first audio arrives and
    written as the clock reaches each packet, as a microphone would send it. So a listener holds little of a reply
    at any moment, and an interrupt has little to cut.
    """

    def __init__(
        self,
        writer: FrameWriter[OpusPacket],
        interrupts: RelayTrackWriter,
        clock: BroadcastClock,
        *,
        rate: int,
        log: Log,
    ) -> None:
        self._writer = writer
        self._interrupts = interrupts
        self._clock = clock
        self._encoder = OpusEncoder(input_rate=rate)
        self._log = log
        self._task: asyncio.Task[None] | None = None
        # The end of the last audio sent: where the next reply may start, and what an interrupt cuts to.
        self._sent_until_us = 0
        # The end of the audio the last interrupt cut: what was sent after it is not cut yet.
        self._cut_until_us = 0

    @property
    def speaking(self) -> bool:
        """Whether a reply is under way, or the end of one may still be playing at a listener."""
        return (self._task is not None and not self._task.done()) or self._uncut_audio_playing()

    def say(self, chunks: AsyncIterator[bytes]) -> None:
        """Start speaking a reply, in place of any under way."""
        if self._task is not None:
            self._task.cancel()
        self._task = asyncio.create_task(self._say(chunks))

    async def interrupt(self) -> None:
        """Stop the reply under way, and tell listeners to drop what they still hold of the audio sent."""
        task = self._task
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        elif not self._uncut_audio_playing():
            return
        if self._sent_until_us <= self._cut_until_us:
            return  # Nothing was sent since the last cut: the reply was still being thought up.
        # Everything sent so far goes; the next reply starts at or after its end, as the interrupt track requires.
        self._cut_until_us = self._sent_until_us
        if self._interrupts.has_demand:
            await self._interrupts.write(build_interrupt(until_us=self._cut_until_us))
        self._log("interrupted", {"untilUs": self._cut_until_us})

    async def close(self) -> None:
        """Stop speaking."""
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    def _uncut_audio_playing(self) -> bool:
        # A reply written to its end is still in listeners' jitter buffers and in flight for a while after.
        return self._sent_until_us > self._cut_until_us and self._clock.now_us() < self._sent_until_us + _HELD_US

    async def _say(self, chunks: AsyncIterator[bytes]) -> None:
        sent = 0
        try:
            async for chunk in chunks:
                self._drop_if_starved()
                if self._encoder.next_timestamp_us is None:
                    start_us = max(self._clock.now_us(), self._sent_until_us)
                    sent += await self._send(self._encoder.encode(chunk, timestamp_us=start_us))
                else:
                    sent += await self._send(self._encoder.encode(chunk))
            self._drop_if_starved()
            sent += await self._send(self._encoder.flush())
            self._log("replied", {"ms": sent * OPUS_FRAME_US // 1000})
        finally:
            # Cancelled mid-reply: the rest of the run is never sent.
            self._encoder.flush()
            aclose = getattr(chunks, "aclose", None)
            if aclose is not None:
                await aclose()

    def _drop_if_starved(self) -> None:
        # The reply came too slowly, as from a TTS that stalls: what the run holds would arrive late. Drop it, so the
        # next audio starts a new run at the clock.
        next_us = self._encoder.next_timestamp_us
        if next_us is not None and next_us < self._clock.now_us() - _STARVED_US:
            self._encoder.flush()

    async def _send(self, packets: Sequence[OpusPacket]) -> int:
        for packet in packets:
            delay_us = packet.timestamp_us - self._clock.now_us()
            if delay_us > 0:
                await asyncio.sleep(delay_us / 1_000_000)
            # Counted before the write: an interrupt that cancels it mid-way must cut the packet too.
            self._sent_until_us = packet.timestamp_us + packet.duration_us
            # A frame writer reports a failed frame rather than raising; the next packet tries again.
            await self._writer.write(packet)
        return len(packets)


class VoiceBot:
    """Listen to the user's audio track and answer on the bot's.

    Speech that starts while the bot is speaking is a barge-in: the bot interrupts itself, then listens.
    """

    def __init__(
        self,
        speaker: Speaker,
        *,
        respond: Respond,
        gate: EnergyGate,
        log: Log,
    ) -> None:
        self._speaker = speaker
        self._respond = respond
        self._gate = gate
        self._decoder = OpusDecoder(rate=gate.rate)
        self._log = log
        # Audio from just before the gate opened, so the utterance keeps its first syllable.
        self._preroll: deque[bytes] = deque(maxlen=max(1, math.ceil(gate.start_ms * 1000 / OPUS_FRAME_US)))
        self._heard: list[bytes] = []
        self._from_us = 0

    async def listen(self, frames: AsyncIterator[SequencedFrame]) -> None:
        """Run until ``frames`` ends."""
        async for frame in frames:
            if frame.discontinuity is not None:
                self._decoder.reset()
                if frame.discontinuity == "restart":
                    # A new publisher session: what was being said is gone.
                    self._gate.reset()
                    self._heard = []
                    self._preroll.clear()
            pcm = self._decoder.decode(frame.payload)
            event = self._gate.push(pcm)
            if event == "start":
                self._heard = [*self._preroll, pcm]
                self._from_us = frame.timestamp_us - len(self._preroll) * OPUS_FRAME_US
                self._log("heard speech", {"barge-in": self._speaker.speaking or None})
                if self._speaker.speaking:
                    await self._speaker.interrupt()
            elif self._gate.open or event == "end":
                self._heard.append(pcm)
            if event == "end":
                # The quiet that closed the gate is not part of what was said.
                quiet_frames = round(self._gate.quiet_ms * 1000 / OPUS_FRAME_US)
                spoken = self._heard[: max(1, len(self._heard) - quiet_frames)]
                until_us = self._from_us + len(spoken) * OPUS_FRAME_US
                utterance = Utterance(b"".join(spoken), self._gate.rate, self._from_us, until_us)
                self._heard = []
                self._log("utterance", {"ms": (utterance.until_us - utterance.from_us) // 1000})
                self._speaker.say(self._respond(utterance))
            self._preroll.append(pcm)


def reply_writer(track: RelayTrackWriter, clock: BroadcastClock) -> FrameWriter[OpusPacket]:
    """The bot's audio frame writer: one packet per group, and a packet more than 100 ms late is dropped as stale.

    A packet that a stalled write held back would be late at the listener too: dropping it adds no delay.
    """
    return FrameWriter(track, grouping="frame", stale=lambda packet: clock.now_us() - packet.timestamp_us > _STALE_US)


def bot_catalog(namespace: Sequence[str]) -> bytes:
    """The bot's catalog: mono Opus, and the interrupt track a listener cuts on."""
    return build_msf_catalog(
        namespace,
        audio=AudioTrackInfo(channel_config="1", bitrate=32_000),
        data=[DataTrackInfo(name=INTERRUPT_TRACK)],
    )


async def run_bot(
    publisher: RelayPublisher,
    subscriber: SupervisedSubscriber,
    *,
    user_namespace: Sequence[str],
    bot_namespace: Sequence[str],
    respond: Respond = parrot,
    gate: EnergyGate | None = None,
    log: Log,
) -> None:
    """Answer the user at ``user_namespace`` from ``publisher``, whose namespace is ``bot_namespace``, until cancelled.

    The subscriber keeps the user's audio track wanted across page reloads, so the bot can start first. The publisher
    is not supervised: when its connection closes or a track ends, this raises.
    """
    gate = gate or EnergyGate(rate=16_000)
    catalog_track = publisher.register_track(CATALOG_TRACK, track_properties=loc_track_properties())
    audio_track = publisher.register_track(
        AUDIO_TRACK, track_properties=[*loc_track_properties(), *media_track_properties("audio")]
    )
    interrupt_track = publisher.register_track(INTERRUPT_TRACK, track_properties=loc_track_properties())
    catalog = CatalogService(catalog_track, bot_catalog(bot_namespace))
    clock = BroadcastClock()
    writer = reply_writer(audio_track, clock)
    speaker = Speaker(writer, interrupt_track, clock, rate=gate.rate, log=log)
    bot = VoiceBot(speaker, respond=respond, gate=gate, log=log)

    # The bot is a listener with no player: nothing reports audio to the clock, so it stays idle and nothing is late.
    reader: FrameReader[SequencedFrame] = FrameReader(FrameSequencer(PlayoutClock(), grouping="frame"))

    def restarted(reason: RestartReason, namespace: tuple[str, ...]) -> None:
        log("user restarted", {"reason": reason})

    subscriber.want(
        "user",
        user_namespace,
        AUDIO_TRACK,
        reader.want_handlers(
            on_subscribed=lambda *_: log("user subscribed", {}),
            on_restarted=restarted,
        ),
    )
    serving = asyncio.ensure_future(catalog.serve())
    listening = asyncio.ensure_future(bot.listen(reader))
    closed = asyncio.ensure_future(publisher.closed)
    try:
        await asyncio.wait([serving, listening, closed], return_when=asyncio.FIRST_COMPLETED)
        if closed.done():
            raise closed.result().error or RuntimeError("The bot's publisher connection closed")
        if serving.done():
            serving.result()  # Raises the error that ended the catalog track, if a write saw one.
            raise RuntimeError("The bot's catalog track ended")
        listening.result()
    finally:
        reader.close()
        subscriber.unwant("user")
        for task in (serving, listening, closed):
            task.cancel()
        await speaker.close()
        await writer.close()
        await asyncio.gather(serving, listening, closed, return_exceptions=True)


# ---------------------------------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Config:
    url: str
    token: str
    namespace: list[str]
    threshold_dbfs: float


def read_token() -> str:
    token = os.environ.get("RELAY_TOKEN")
    if not token and sys.stdin.isatty():
        token = getpass("Relay token (input hidden): ").strip()
    elif not token:
        token = sys.stdin.readline().strip()
    return token or ""


def parse_args(argv: list[str]) -> Config:
    parser = argparse.ArgumentParser(description="A parrot voice bot: talk to it from the voice-bot page.")
    parser.add_argument("--url", default=os.environ.get("RELAY_URL", DEFAULT_ENDPOINT), help="Relay endpoint")
    parser.add_argument(
        "--namespace",
        help="The conversation's namespace, slash-separated, including the token's root prefix; the user publishes "
        "one field below it at user, the bot at bot (default RELAY_NAMESPACE_PREFIX or relay-sdk-example, "
        "plus voice-bot)",
    )
    parser.add_argument(
        "--threshold", type=float, default=-45.0, help="Speech is louder than this, in dBFS (default -45)"
    )
    args = parser.parse_args(argv)
    parts = urlsplit(args.url)
    if parts.scheme != "https":
        parser.error("--url must be an https URL")
    if "jwt=" in parts.query:
        parser.error("Supply the token via RELAY_TOKEN or the prompt, not inside --url")
    namespace_text = args.namespace or f"{os.environ.get('RELAY_NAMESPACE_PREFIX', 'relay-sdk-example')}/voice-bot"
    namespace = [field for field in namespace_text.split("/") if field]
    if not namespace:
        parser.error("--namespace must contain at least one field")
    token = read_token()
    if not token:
        parser.error("A Relay token is required (RELAY_TOKEN or the prompt)")
    return Config(url=args.url, token=token, namespace=namespace, threshold_dbfs=args.threshold)


async def run(config: Config, log: Log) -> None:
    user_namespace = [*config.namespace, "user"]
    bot_namespace = [*config.namespace, "bot"]
    publisher = await connect_relay_publisher(config.url, namespace=bot_namespace, get_token=lambda: config.token)
    try:
        subscriber = await connect_supervised_subscriber(config.url, get_token=lambda: config.token)
        try:
            log("listening", {"user": "/".join(user_namespace), "bot": "/".join(bot_namespace)})
            await run_bot(
                publisher,
                subscriber,
                user_namespace=user_namespace,
                bot_namespace=bot_namespace,
                gate=EnergyGate(rate=16_000, threshold_dbfs=config.threshold_dbfs),
                log=log,
            )
        finally:
            subscriber.close()
    finally:
        await publisher.close()


def main(argv: list[str] | None = None) -> int:
    config = parse_args(sys.argv[1:] if argv is None else argv)

    def log(event: str, fields: dict[str, Any]) -> None:
        rendered = " ".join(f"{key}={value}" for key, value in fields.items() if value is not None)
        print(f"{event:<16} {rendered}".rstrip().replace(config.token, "[redacted]"), flush=True)

    try:
        asyncio.run(run(config, log))
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        print(f"failed: {type(error).__name__}: {error}".replace(config.token, "[redacted]"), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
