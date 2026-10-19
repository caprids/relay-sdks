"""The parrot bot over the in-memory relay hub: a talker stands in for the browser page's microphone."""

import asyncio
import itertools
import math
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import Any, cast

import numpy as np
import pytest
from core_helpers.waiting import wait_for
from relay_helpers.fake_track_writer import FakeGroupWriter, FakeTrackWriter
from relay_helpers.relay_hub import create_relay_hub
from voice_bot.bot import EnergyGate, Speaker, Utterance, parrot, reply_writer, run_bot

from moq_core import KeyValuePair, MoqtObject
from moq_media import (
    AUDIO_TRACK,
    INTERRUPT_TRACK,
    OPUS_FRAME_US,
    TRANSCRIPT_TRACK,
    BroadcastClock,
    Interrupt,
    TranscriptSegment,
    loc_timestamp_us,
    loc_track_properties,
    parse_interrupt,
    parse_transcript_segment,
)
from moq_media.opus import OpusDecoder, OpusEncoder, OpusPacket
from mux_relay import (
    RelaySubscriptionHandlers,
    RelayTrackWriter,
    connect_relay_publisher,
    connect_relay_subscriber,
    connect_supervised_subscriber,
)
from mux_relay.media import FrameWriter

URL = "https://relay.invalid/"
USER = ["voice", "user"]
BOT = ["voice", "bot"]
FRAME_SAMPLES = 960  # 20 ms at 48 kHz

Cleanup = list[Callable[[], Awaitable[None] | None]]


@pytest.fixture
async def cleanup() -> AsyncIterator[Cleanup]:
    steps: Cleanup = []
    yield steps
    for dispose in reversed(steps):
        result = dispose()
        if result is not None:
            await result


def tone(samples: int, rate: int, *, start: int = 0, amplitude: float = 0.3) -> bytes:
    t = (np.arange(samples) + start) / rate
    return (np.sin(2 * np.pi * 440 * t) * amplitude * 32767).astype("<i2").tobytes()


def dbfs(pcm: bytes) -> float:
    samples = np.frombuffer(pcm, dtype="<i2").astype(np.float64)
    rms = math.sqrt(float(np.mean(np.square(samples)))) if samples.size else 0.0
    return -math.inf if rms == 0 else 20 * math.log10(rms / 32768)


class Talker:
    """A microphone that never stops: a 440 Hz tone while ``loud``, silence otherwise, in 20 ms Opus frames."""

    def __init__(self, track: RelayTrackWriter) -> None:
        self.loud = False
        self._writer: FrameWriter[OpusPacket] = FrameWriter(track, grouping="frame")
        self._encoder = OpusEncoder()
        self._clock = BroadcastClock()
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        start = self._clock.now_us()
        for frame in range(1_000_000):
            pcm = tone(FRAME_SAMPLES, 48_000, start=frame * FRAME_SAMPLES) if self.loud else bytes(FRAME_SAMPLES * 2)
            packets = self._encoder.encode(pcm, timestamp_us=start if frame == 0 else None)
            for packet in packets:
                await self._writer.write(packet)
            await asyncio.sleep(max(0, (start + (frame + 1) * OPUS_FRAME_US - self._clock.now_us()) / 1_000_000))

    async def close(self) -> None:
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        await self._writer.close()


class TestEnergyGate:
    def test_an_utterance_starts_after_enough_speech_and_ends_after_the_hangover(self) -> None:
        gate = EnergyGate(rate=16_000, start_ms=60, hangover_ms=100)
        loud, quiet = tone(320, 16_000), bytes(640)
        assert [gate.push(loud) for _ in range(3)] == [None, None, "start"]
        assert [gate.push(quiet) for _ in range(5)] == [None, None, None, None, "end"]
        assert not gate.open
        assert gate.quiet_ms == 100

    def test_a_short_noise_or_a_brief_pause_changes_nothing(self) -> None:
        gate = EnergyGate(rate=16_000, start_ms=60, hangover_ms=100)
        loud, quiet = tone(320, 16_000), bytes(640)
        assert [gate.push(frame) for frame in [loud, loud, quiet, loud, loud]] == [None] * 5
        assert gate.push(loud) == "start"
        assert [gate.push(frame) for frame in [quiet, quiet, loud, quiet]] == [None] * 4
        assert gate.open

    def test_a_long_utterance_ends_at_the_cap(self) -> None:
        gate = EnergyGate(rate=16_000, start_ms=20, hangover_ms=100, max_ms=100)
        loud = tone(320, 16_000)
        assert [gate.push(loud) for _ in range(5)] == ["start", None, None, None, "end"]
        assert gate.quiet_ms == 0

    def test_the_threshold_decides_what_is_speech(self) -> None:
        gate = EnergyGate(rate=16_000, start_ms=20, threshold_dbfs=-10)
        assert gate.push(tone(320, 16_000)) is None  # about -13 dBFS
        assert gate.push(tone(320, 16_000, amplitude=0.9)) == "start"


async def test_parrot_says_the_utterance_back_after_its_text() -> None:
    utterance = Utterance(b"\x01\x00\x02\x00", 16_000, 0, 1_500_000)
    assert [chunk async for chunk in parrot(utterance)] == ["(your words back: 1.5 s)", utterance.pcm]


class TimedTrackWriter(FakeTrackWriter):
    """A fake track that records how late, on the clock, each frame's group opened after its LOC timestamp."""

    def __init__(self, clock: BroadcastClock) -> None:
        super().__init__()
        self.clock = clock
        self.lateness_us: list[int] = []

    async def open_group(
        self, payload: bytes | None = None, *, properties: Sequence[KeyValuePair] | None = None
    ) -> FakeGroupWriter:
        self.lateness_us.append(self.clock.now_us() - (loc_timestamp_us(properties or []) or 0))
        return await super().open_group(payload, properties=properties)


class TestSpeaker:
    def speaker(self) -> tuple[Speaker, TimedTrackWriter, FakeTrackWriter, list[str]]:
        clock = BroadcastClock()
        audio, interrupts = TimedTrackWriter(clock), FakeTrackWriter()
        self.transcripts = FakeTrackWriter()
        events: list[str] = []
        writer: FrameWriter[OpusPacket] = FrameWriter(audio, grouping="frame")

        def log(event: str, _: dict[str, Any]) -> None:
            events.append(event)

        speaker = Speaker(writer, cast(Any, interrupts), cast(Any, self.transcripts), clock, rate=16_000, log=log)
        return speaker, audio, interrupts, events

    @staticmethod
    def stamps(audio: FakeTrackWriter) -> list[int]:
        return [loc_timestamp_us(group.objects[0][1]) or 0 for group in audio.groups]

    def segments(self) -> list[TranscriptSegment]:
        return [parse_transcript_segment(payload) for payload in self.transcripts.writes]

    async def test_each_part_of_a_reply_gets_its_text_before_its_audio_and_a_final_version_after(self) -> None:
        speaker, audio, _, events = self.speaker()

        async def reply(_: Utterance) -> AsyncIterator[bytes | str]:
            yield "Hello"
            yield tone(1600, 16_000)
            yield "there"
            yield tone(1600, 16_000)

        speaker.say(reply(Utterance(b"", 16_000, 0, 0)))
        await wait_for(lambda: "replied" in events, timeout=3)
        stamps = self.stamps(audio)
        hello, hello_final, there, there_final = self.segments()
        assert (hello.text, hello.from_us, hello.until_us, hello.final) == ("Hello", stamps[0], None, False)
        assert (hello_final.seq, hello_final.final) == (hello.seq, True)
        assert (there.seq, there.text, there.final) == (hello.seq + 1, "there", False)
        # Each part's segment ends where its own audio does, and the next starts there or, after a scheduling delay,
        # later.
        assert hello_final.until_us - OPUS_FRAME_US in stamps[4:]
        assert there.from_us in stamps[5:]
        assert hello_final.until_us <= there.from_us
        assert (there_final.until_us, there_final.final) == (stamps[-1] + OPUS_FRAME_US, True)

    async def test_text_after_audio_too_short_for_a_packet_starts_its_own_segment(self) -> None:
        speaker, _, _, events = self.speaker()

        async def reply(_: Utterance) -> AsyncIterator[bytes | str]:
            yield "First"
            yield tone(160, 16_000)  # 10 ms: the encoder holds it
            yield "Second"
            yield tone(1600, 16_000)

        speaker.say(reply(Utterance(b"", 16_000, 0, 0)))
        await wait_for(lambda: "replied" in events, timeout=3)
        texts = [(segment.text, segment.final) for segment in self.segments()]
        assert texts == [("First", False), ("First", True), ("Second", False), ("Second", True)]

    async def test_an_interrupted_segment_gets_no_final_version(self) -> None:
        speaker, audio, interrupts, _ = self.speaker()

        async def reply(_: Utterance) -> AsyncIterator[bytes | str]:
            yield "A long answer"
            yield tone(16_000, 16_000)
            yield "never said"

        speaker.say(reply(Utterance(b"", 16_000, 0, 0)))
        await wait_for(lambda: len(audio.groups) >= 3, timeout=3)
        await speaker.interrupt()
        (segment,) = self.segments()
        assert (segment.text, segment.final) == ("A long answer", False)
        assert parse_interrupt(interrupts.writes[0]).until_us > segment.from_us

    async def test_a_reply_that_stalls_is_restamped_rather_than_sent_late(self) -> None:
        speaker, audio, _, events = self.speaker()

        async def stalling(_: Utterance) -> AsyncIterator[bytes]:
            yield tone(3200, 16_000)
            await asyncio.sleep(0.5)
            yield tone(3200, 16_000)

        speaker.say(stalling(Utterance(b"", 16_000, 0, 0)))
        await wait_for(lambda: "replied" in events, timeout=3)
        stamps = self.stamps(audio)
        gaps = [later - earlier for earlier, later in itertools.pairwise(stamps)]
        # The run that starved was dropped, not sent 400 ms late: the second chunk starts its own run, later on.
        assert max(gaps) > 300_000
        assert all(gap > 0 for gap in gaps)
        assert max(audio.lateness_us) < 2 * OPUS_FRAME_US

    async def test_a_reply_that_stalls_before_it_ends_does_not_send_its_tail_late(self) -> None:
        speaker, audio, _, events = self.speaker()

        async def stalling(_: Utterance) -> AsyncIterator[bytes]:
            yield tone(3200, 16_000)
            await asyncio.sleep(0.5)

        speaker.say(stalling(Utterance(b"", 16_000, 0, 0)))
        await wait_for(lambda: "replied" in events, timeout=3)
        assert audio.groups
        assert max(audio.lateness_us) < 2 * OPUS_FRAME_US

    async def test_talking_over_the_end_of_a_reply_still_interrupts_it_once(self) -> None:
        speaker, audio, interrupts, events = self.speaker()
        speaker.say(parrot(Utterance(tone(1600, 16_000), 16_000, 0, 100_000)))
        await wait_for(lambda: "replied" in events, timeout=3)
        # Written to its end, but a listener still holds the last of it.
        assert speaker.speaking
        await speaker.interrupt()
        await speaker.interrupt()
        (payload,) = interrupts.writes
        assert parse_interrupt(payload).until_us == self.stamps(audio)[-1] + OPUS_FRAME_US
        assert not speaker.speaking

    async def test_a_reply_finished_long_ago_is_not_interrupted(self) -> None:
        speaker, _, interrupts, events = self.speaker()
        speaker.say(parrot(Utterance(tone(320, 16_000), 16_000, 0, 20_000)))
        await wait_for(lambda: "replied" in events, timeout=3)
        await asyncio.sleep(0.6)
        assert not speaker.speaking
        await speaker.interrupt()
        assert interrupts.writes == []

    async def test_talking_over_a_reply_still_being_thought_up_cancels_it_and_sends_nothing(self) -> None:
        speaker, audio, interrupts, _ = self.speaker()
        thinking = asyncio.Event()

        async def slow(_: Utterance) -> AsyncIterator[bytes]:
            thinking.set()
            await asyncio.sleep(10)
            yield b""

        speaker.say(slow(Utterance(b"", 16_000, 0, 0)))
        await thinking.wait()
        assert speaker.speaking
        await speaker.interrupt()
        assert not speaker.speaking
        assert audio.groups == []
        assert interrupts.writes == []


async def test_the_reply_writer_drops_a_packet_a_stalled_write_held_back() -> None:
    clock = BroadcastClock()
    writer = reply_writer(cast(Any, FakeTrackWriter()), clock)
    now = clock.now_us()
    assert (await writer.write(OpusPacket(now - 200_000, b"\xf8"))).kind == "stale"
    assert (await writer.write(OpusPacket(now - 10_000, b"\xf8"))).kind == "sent"


async def test_the_bot_stops_when_its_publisher_closes(cleanup: Cleanup) -> None:
    hub = create_relay_hub()
    cleanup.append(hub.destroy)
    publisher = await connect_relay_publisher(URL, namespace=BOT, create_transport=hub.connect_publisher)
    subscriber = await connect_supervised_subscriber(URL, create_transport=hub.connect_subscriber)
    cleanup.extend([subscriber.close, publisher.close])
    bot = asyncio.create_task(
        run_bot(publisher, subscriber, user_namespace=USER, bot_namespace=BOT, log=lambda *_: None)
    )
    await asyncio.sleep(0.05)
    assert not bot.done()
    await publisher.close()
    await wait_for(bot.done)
    assert bot.exception() is not None


async def test_the_bot_answers_after_a_pause_and_interrupts_itself_when_talked_over(cleanup: Cleanup) -> None:
    # The hub serves one broadcast, so each side's broadcast gets its own; a relay would serve both.
    user_hub, bot_hub = create_relay_hub(), create_relay_hub()
    cleanup.extend([user_hub.destroy, bot_hub.destroy])
    user = await connect_relay_publisher(URL, namespace=USER, create_transport=user_hub.connect_publisher)
    bot_publisher = await connect_relay_publisher(URL, namespace=BOT, create_transport=bot_hub.connect_publisher)
    bot_subscriber = await connect_supervised_subscriber(URL, create_transport=user_hub.connect_subscriber)
    listener = await connect_relay_subscriber(URL, create_transport=bot_hub.connect_subscriber)
    cleanup.extend([listener.close, bot_subscriber.close, bot_publisher.close, user.close])

    # The user's side: a microphone, and a listener on the bot's audio and interrupt tracks.
    microphone = user.register_track(AUDIO_TRACK, track_properties=loc_track_properties())
    heard: list[MoqtObject] = []
    interrupts: list[Interrupt] = []
    segments: list[TranscriptSegment] = []
    listener.subscribe(BOT, AUDIO_TRACK, RelaySubscriptionHandlers(on_object=heard.append))
    listener.subscribe(
        BOT,
        TRANSCRIPT_TRACK,
        RelaySubscriptionHandlers(on_object=lambda obj: segments.append(parse_transcript_segment(obj.payload))),
    )
    listener.subscribe(
        BOT,
        INTERRUPT_TRACK,
        RelaySubscriptionHandlers(on_object=lambda obj: interrupts.append(parse_interrupt(obj.payload))),
    )

    events: list[tuple[str, dict[str, Any]]] = []
    bot = asyncio.create_task(
        run_bot(
            bot_publisher,
            bot_subscriber,
            user_namespace=USER,
            bot_namespace=BOT,
            gate=EnergyGate(rate=16_000, hangover_ms=300),
            log=lambda event, fields: events.append((event, fields)),
        )
    )

    async def stop_bot() -> None:
        bot.cancel()
        await asyncio.gather(bot, return_exceptions=True)

    cleanup.append(stop_bot)
    await wait_for(lambda: microphone.has_demand)
    talker = Talker(microphone)
    cleanup.append(talker.close)

    def said(event: str) -> int:
        return sum(1 for name, _ in events if name == event)

    # One second of speech, then a pause: the bot says it back.
    talker.loud = True
    await asyncio.sleep(1.0)
    talker.loud = False
    await wait_for(lambda: len(heard) >= 10, timeout=3)
    assert said("utterance") == 1
    assert not interrupts

    # The user talks over the reply: the bot stops, interrupts, and answers the new utterance after its pause.
    talker.loud = True
    await wait_for(lambda: interrupts, timeout=3)
    await asyncio.sleep(0.3)
    talker.loud = False
    await wait_for(lambda: said("utterance") == 2 and said("replied") == 1, timeout=4)

    (interrupt,) = interrupts
    assert interrupt.track == AUDIO_TRACK
    assert interrupt.from_us is None
    stamped = [(obj, loc_timestamp_us(obj.properties) or 0) for obj in heard]
    assert all(stamp > 0 for _, stamp in stamped)
    first = [obj for obj, stamp in stamped if stamp < interrupt.until_us]
    second = [obj for obj, stamp in stamped if stamp >= interrupt.until_us]
    # The first reply was cut short of the second of speech it repeated; the cut covers all of it that was sent.
    assert 10 <= len(first) < 45
    last = loc_timestamp_us(first[-1].properties)
    assert last is not None
    assert interrupt.until_us - 2 * OPUS_FRAME_US <= last < interrupt.until_us
    assert second
    # What the bot said is the tone it heard: the parrot's reply decodes to loud audio.
    decoder = OpusDecoder(rate=16_000)
    middle = b"".join(decoder.decode(obj.payload) for obj in first[:10])[3200:]
    assert dbfs(middle) > -20

    # Each reply's text lies where its audio starts; the interrupted one never got a final version.
    first_text, second_text, second_final = segments
    assert first_text.text.startswith("(your words back: ")
    assert (first_text.from_us, first_text.final) == (loc_timestamp_us(first[0].properties), False)
    assert (second_text.seq, second_text.from_us) == (first_text.seq + 1, loc_timestamp_us(second[0].properties))
    assert (second_final.seq, second_final.final) == (second_text.seq, True)
    assert second_final.until_us == (loc_timestamp_us(second[-1].properties) or 0) + OPUS_FRAME_US
