"""Opus encoding and decoding on PyAV's libopus, for a server-side talker such as a voice bot.

Install with the ``opus`` extra (``mux-moq-media[opus]``). This is the one module in the package that
needs PyAV; ``import moq_media`` does not load it.
"""

from dataclasses import dataclass
from fractions import Fraction
from typing import Literal

try:
    import av
    from av.audio.resampler import AudioResampler
except ImportError as error:  # pragma: no cover - exercised only without the extra
    raise ImportError("moq_media.opus needs PyAV: install mux-moq-media[opus]") from error

from moq_core import KeyValuePair

from .audio import OPUS_FRAME_US, OPUS_SAMPLE_RATE
from .loc import MICROSECONDS_PER_SECOND, loc_properties

OpusApplication = Literal["voip", "audio", "lowdelay"]
"""libopus's tuning: ``voip`` favours speech intelligibility, ``audio`` fidelity, ``lowdelay`` the shortest delay."""

_FRAME_SAMPLES = OPUS_SAMPLE_RATE * OPUS_FRAME_US // MICROSECONDS_PER_SECOND
_SAMPLE_BYTES = 2  # s16
_LAYOUTS = {1: "mono", 2: "stereo"}

Pcm = bytes | bytearray | memoryview
"""Interleaved signed 16-bit little-endian samples."""


@dataclass(frozen=True, slots=True)
class OpusPacket:
    """One encoded Opus packet and where it starts on the broadcast timeline.

    It has the attributes ``mux_relay.media.FrameWriter`` takes, so it can be written as a frame as it is.
    """

    timestamp_us: int
    payload: bytes
    duration_us: int = OPUS_FRAME_US
    key: bool = True
    """Every Opus packet decodes on its own: one group per frame (LOC)."""
    discontinuity: bool = False

    @property
    def properties(self) -> list[KeyValuePair]:
        """The LOC object properties for the packet: its timestamp in microseconds."""
        return loc_properties(self.timestamp_us)


class OpusEncoder:
    """Encodes PCM into 20 ms Opus packets stamped on the broadcast timeline, with voice defaults.

    Input is a run of audio: the first :meth:`encode` call after construction or :meth:`flush` gives the
    timeline timestamp of its first sample, and every later call continues from where the samples so far
    end. :meth:`flush` ends the run, the end of an utterance, padding its last partial frame with silence.

    Packet *k* of a run is stamped at the run's timestamp plus 20 ms per packet before it, as WebCodecs stamps its
    output. Decoded audio trails those stamps by the encoder's 6.5 ms look-ahead, and a flushed run ends
    with one packet more than the frames it was given, which carries that look-ahead's tail.
    """

    def __init__(
        self,
        *,
        input_rate: int = OPUS_SAMPLE_RATE,
        input_channels: int = 1,
        channels: int = 1,
        bitrate: int | None = 32_000,
        application: OpusApplication = "voip",
    ) -> None:
        """Make an encoder for s16 PCM at ``input_rate`` with ``input_channels``, encoding ``channels``.

        ``bitrate`` is in bits per second, or ``None`` for libopus's own choice.
        """
        _integer(input_rate, "input_rate", positive=True)
        _channels(input_channels, "input_channels")
        _channels(channels, "channels")
        if bitrate is not None:
            _integer(bitrate, "bitrate", positive=True)
        if application not in ("voip", "audio", "lowdelay"):
            raise ValueError("application must be 'voip', 'audio' or 'lowdelay'")
        self.input_rate = input_rate
        self.input_channels = input_channels
        self.channels = channels
        self.bitrate = bitrate
        self.application: OpusApplication = application
        self._origin_us: int | None = None
        self._input_seconds = Fraction(0)
        self._pending = bytearray()
        self._open()

    @property
    def next_timestamp_us(self) -> int | None:
        """Where the next sample handed in would land on the timeline, or ``None`` between runs."""
        if self._origin_us is None:
            return None
        return self._origin_us + int(self._input_seconds * MICROSECONDS_PER_SECOND)

    def encode(self, pcm: Pcm | av.AudioFrame, *, timestamp_us: int | None = None) -> list[OpusPacket]:
        """Encode audio and return the packets it completed, which may be none.

        ``pcm`` is interleaved signed 16-bit little-endian samples at the encoder's input rate and channel
        count, or a PyAV ``AudioFrame`` of any format, rate and layout, the same throughout a run.
        ``timestamp_us`` is required to start a run and refused within one.
        """
        if self._origin_us is None:
            if timestamp_us is None:
                raise ValueError("timestamp_us is required to start a run")
            _integer(timestamp_us, "timestamp_us")
            self._origin_us = timestamp_us
        elif timestamp_us is not None:
            raise ValueError("timestamp_us starts a run; flush() the current one first")

        if isinstance(pcm, av.AudioFrame):
            frame = pcm
            self._input_seconds += Fraction(frame.samples, frame.sample_rate)
        else:
            data = memoryview(pcm).cast("B")
            stride = _SAMPLE_BYTES * self.input_channels
            if len(data) % stride:
                raise ValueError(f"pcm must hold whole samples of {stride} bytes")
            if not data:
                return []
            frame = _frame(bytes(data), self.input_channels, self.input_rate)
            self._input_seconds += Fraction(len(data) // stride, self.input_rate)
        for resampled in self._resampler.resample(frame):
            self._take(resampled)
        return self._encode_pending()

    def flush(self) -> list[OpusPacket]:
        """End the run: pad its last partial frame with silence and return every packet left.

        The next :meth:`encode` starts a new run and needs a timestamp again. Flushing between runs
        returns nothing.
        """
        if self._origin_us is None:
            return []
        for resampled in self._resampler.resample(None):
            self._take(resampled)
        frame_bytes = _FRAME_SAMPLES * _SAMPLE_BYTES * self.channels
        if self._pending:
            self._pending.extend(bytes(frame_bytes - len(self._pending)))
        packets = self._encode_pending()
        packets.extend(self._packet(packet) for packet in self._context.encode(None))
        self._origin_us = None
        self._input_seconds = Fraction(0)
        self._open()
        return packets

    def _open(self) -> None:
        context = av.CodecContext.create("libopus", "w")
        context.sample_rate = OPUS_SAMPLE_RATE
        context.layout = _LAYOUTS[self.channels]
        context.format = "s16"
        context.time_base = Fraction(1, OPUS_SAMPLE_RATE)
        if self.bitrate is not None:
            context.bit_rate = self.bitrate
        context.options = {"application": self.application}
        context.open()
        self._context = context
        self._resampler = AudioResampler(format="s16", layout=_LAYOUTS[self.channels], rate=OPUS_SAMPLE_RATE)
        self._pending.clear()
        self._frames_in = 0
        self._packets_out = 0

    def _take(self, frame: av.AudioFrame) -> None:
        # Packed s16 has one plane; its buffer can be longer than the samples it holds.
        self._pending.extend(bytes(frame.planes[0])[: frame.samples * _SAMPLE_BYTES * self.channels])

    def _encode_pending(self) -> list[OpusPacket]:
        frame_bytes = _FRAME_SAMPLES * _SAMPLE_BYTES * self.channels
        packets: list[OpusPacket] = []
        while len(self._pending) >= frame_bytes:
            frame = _frame(bytes(self._pending[:frame_bytes]), self.channels, OPUS_SAMPLE_RATE)
            del self._pending[:frame_bytes]
            frame.pts = self._frames_in * _FRAME_SAMPLES
            frame.time_base = Fraction(1, OPUS_SAMPLE_RATE)
            self._frames_in += 1
            packets.extend(self._packet(packet) for packet in self._context.encode(frame))
        return packets

    def _packet(self, packet: av.Packet) -> OpusPacket:
        assert self._origin_us is not None
        timestamp_us = self._origin_us + self._packets_out * OPUS_FRAME_US
        self._packets_out += 1
        return OpusPacket(timestamp_us, bytes(packet))


class OpusDecoder:
    """Decodes Opus packets into PCM at the rate and channel count the caller wants, such as 16 kHz mono for STT.

    The decoder keeps state from packet to packet. Call :meth:`reset` wherever the packets stop following
    one another: a gap, a resync, or a restart (a frame's ``discontinuity``).
    """

    def __init__(self, *, rate: int = OPUS_SAMPLE_RATE, channels: int = 1) -> None:
        """Make a decoder whose output is interleaved s16 PCM at ``rate`` with ``channels``."""
        _integer(rate, "rate", positive=True)
        _channels(channels, "channels")
        self.rate = rate
        self.channels = channels
        self._open()

    def decode(self, payload: bytes | bytearray | memoryview) -> bytes:
        """Decode one packet into interleaved signed 16-bit little-endian PCM.

        The output's length can vary from packet to packet while resampling, and the first packets after a
        reset can return less, or nothing, while the resampler fills.
        """
        data = bytes(memoryview(payload).cast("B"))
        if not data:
            raise ValueError("payload must not be empty")
        out = bytearray()
        for frame in self._context.decode(av.Packet(data)):
            frame.pts = None
            for resampled in self._resampler.resample(frame):
                out.extend(bytes(resampled.planes[0])[: resampled.samples * _SAMPLE_BYTES * self.channels])
        return bytes(out)

    def reset(self) -> None:
        """Forget the previous packets, for a discontinuity. Audio still in the resampler is dropped."""
        self._open()

    def _open(self) -> None:
        context = av.CodecContext.create("libopus", "r")
        context.sample_rate = OPUS_SAMPLE_RATE
        context.layout = _LAYOUTS[self.channels]
        self._context = context
        self._resampler = AudioResampler(format="s16", layout=_LAYOUTS[self.channels], rate=self.rate)


def _integer(value: int, name: str, *, positive: bool = False) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < int(positive):
        raise ValueError(f"{name} must be a {'positive' if positive else 'nonnegative'} integer")


def _channels(value: int, name: str) -> None:
    if isinstance(value, bool) or value not in _LAYOUTS:
        raise ValueError(f"{name} must be 1 or 2")


def _frame(data: bytes, channels: int, rate: int) -> av.AudioFrame:
    frame = av.AudioFrame(
        format="s16", layout=_LAYOUTS[channels], samples=len(data) // (_SAMPLE_BYTES * channels), align=1
    )
    frame.planes[0].update(data)
    frame.sample_rate = rate
    return frame
