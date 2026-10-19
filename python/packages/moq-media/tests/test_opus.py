"""The PyAV-backed Opus encoder and decoder, and the package root's independence from PyAV."""

import math
import struct
import subprocess
import sys

import av
import pytest

from moq_media import OPUS_FRAME_US, loc_properties, opus_packet_duration_us
from moq_media.opus import OpusDecoder, OpusEncoder

ORIGIN_US = 1_760_000_000_000_000


def tone(seconds: float, *, rate: int = 48_000, channels: int = 1, hz: float = 440.0) -> bytes:
    samples = round(seconds * rate)
    values = (round(8_000 * math.sin(2 * math.pi * hz * n / rate)) for n in range(samples))
    return b"".join(struct.pack("<" + "h" * channels, *([value] * channels)) for value in values)


def rms(pcm: bytes) -> float:
    values = struct.unpack(f"<{len(pcm) // 2}h", pcm)
    return math.sqrt(sum(value * value for value in values) / len(values)) if values else 0.0


class TestOpusEncoder:
    def test_stamps_twenty_millisecond_packets_from_the_run_start(self) -> None:
        encoder = OpusEncoder()

        packets = encoder.encode(tone(0.1), timestamp_us=ORIGIN_US)

        assert [packet.timestamp_us for packet in packets] == [ORIGIN_US + k * OPUS_FRAME_US for k in range(5)]
        assert all(opus_packet_duration_us(packet.payload) == OPUS_FRAME_US for packet in packets)
        assert all(packet.duration_us == OPUS_FRAME_US for packet in packets)
        assert encoder.next_timestamp_us == ORIGIN_US + 100_000

    def test_packets_carry_what_a_frame_writer_takes(self) -> None:
        packet = OpusEncoder().encode(tone(0.02), timestamp_us=ORIGIN_US)[0]

        assert packet.key is True
        assert packet.discontinuity is False
        assert packet.properties == loc_properties(ORIGIN_US)

    def test_holds_a_partial_frame_until_more_audio_or_a_flush(self) -> None:
        encoder = OpusEncoder()

        assert encoder.encode(tone(0.015), timestamp_us=ORIGIN_US) == []
        assert len(encoder.encode(tone(0.015))) == 1
        assert encoder.next_timestamp_us == ORIGIN_US + 30_000

        tail = encoder.flush()

        # The padded partial frame, then the look-ahead's tail.
        assert [packet.timestamp_us for packet in tail] == [ORIGIN_US + 20_000, ORIGIN_US + 40_000]
        assert encoder.next_timestamp_us is None

    def test_a_flush_starts_a_new_run_that_needs_a_timestamp(self) -> None:
        encoder = OpusEncoder()
        encoder.encode(tone(0.04), timestamp_us=ORIGIN_US)
        encoder.flush()

        with pytest.raises(ValueError, match="required"):
            encoder.encode(tone(0.02))
        packets = encoder.encode(tone(0.04), timestamp_us=ORIGIN_US + 1_000_000)

        assert [packet.timestamp_us for packet in packets] == [ORIGIN_US + 1_000_000, ORIGIN_US + 1_020_000]

    @pytest.mark.parametrize("pcm", [b"", b"\x00"], ids=["empty", "partial sample"])
    def test_a_call_that_takes_no_audio_starts_no_run(self, pcm: bytes) -> None:
        encoder = OpusEncoder()
        try:
            encoder.encode(pcm, timestamp_us=ORIGIN_US)
        except ValueError:
            pass

        assert encoder.next_timestamp_us is None
        packets = encoder.encode(tone(0.02), timestamp_us=ORIGIN_US + 20_000)
        assert [packet.timestamp_us for packet in packets] == [ORIGIN_US + 20_000]

    def test_refuses_a_timestamp_inside_a_run(self) -> None:
        encoder = OpusEncoder()
        encoder.encode(tone(0.02), timestamp_us=ORIGIN_US)

        with pytest.raises(ValueError, match="flush"):
            encoder.encode(tone(0.02), timestamp_us=ORIGIN_US + 20_000)

    def test_flushing_between_runs_returns_nothing(self) -> None:
        assert OpusEncoder().flush() == []

    def test_resamples_other_input_rates_and_channel_counts(self) -> None:
        encoder = OpusEncoder(input_rate=24_000, input_channels=2)

        packets = encoder.encode(tone(0.2, rate=24_000, channels=2), timestamp_us=ORIGIN_US)
        packets += encoder.flush()

        assert encoder.next_timestamp_us is None
        # 200 ms is ten frames; the resampler's delay can hold back part of one until the flush.
        assert len(packets) in (10, 11, 12)
        assert packets[-1].timestamp_us == ORIGIN_US + (len(packets) - 1) * OPUS_FRAME_US

    def test_a_flush_whose_drain_crosses_a_frame_boundary_pads_only_the_remainder(self) -> None:
        # 481 samples at 24 kHz: 930 at 48 kHz are out before the flush, and the drain brings the run to 962.
        encoder = OpusEncoder(input_rate=24_000)
        encoder.encode(tone(481 / 24_000, rate=24_000), timestamp_us=ORIGIN_US)

        packets = encoder.flush()

        # Two frames (one whole, one padded), then the look-ahead's tail.
        assert [packet.timestamp_us for packet in packets] == [ORIGIN_US + k * OPUS_FRAME_US for k in range(3)]

    def test_a_short_resampled_run_keeps_its_audio(self) -> None:
        encoder = OpusEncoder(input_rate=8_000)
        encoder.encode(tone(20 / 8_000, rate=8_000, hz=1_000), timestamp_us=ORIGIN_US)

        packets = encoder.flush()
        decoder = OpusDecoder()
        pcm = b"".join(decoder.decode(packet.payload) for packet in packets)

        assert len(packets) == 2
        assert rms(pcm) > 100

    def test_takes_pyav_frames(self) -> None:
        encoder = OpusEncoder(channels=2, bitrate=96_000, application="audio")
        data = tone(0.04, rate=44_100, channels=2)
        frame = av.AudioFrame(format="s16", layout="stereo", samples=len(data) // 4, align=1)
        frame.planes[0].update(data)
        frame.sample_rate = 44_100

        packets = encoder.encode(frame, timestamp_us=ORIGIN_US) + encoder.flush()

        assert encoder.next_timestamp_us is None
        assert len(packets) >= 2
        assert all(opus_packet_duration_us(packet.payload) == OPUS_FRAME_US for packet in packets)

    def test_refuses_partial_samples(self) -> None:
        with pytest.raises(ValueError, match="whole samples"):
            OpusEncoder(input_channels=2).encode(b"\x00\x00\x00", timestamp_us=ORIGIN_US)

    @pytest.mark.parametrize(
        "options",
        [
            {"input_rate": 0},
            {"input_channels": 3},
            {"channels": 0},
            {"channels": True},
            {"bitrate": -1},
            {"application": "music"},
        ],
    )
    def test_refuses_bad_options(self, options: dict[str, object]) -> None:
        with pytest.raises(ValueError):
            OpusEncoder(**options)  # ty: ignore[invalid-argument-type]

    def test_refuses_a_negative_timestamp(self) -> None:
        with pytest.raises(ValueError, match="timestamp_us"):
            OpusEncoder().encode(tone(0.02), timestamp_us=-1)


class TestOpusDecoder:
    def test_round_trips_a_tone(self) -> None:
        encoder = OpusEncoder()
        packets = encoder.encode(tone(0.5), timestamp_us=ORIGIN_US) + encoder.flush()
        decoder = OpusDecoder()

        pcm = b"".join(decoder.decode(packet.payload) for packet in packets)

        assert len(pcm) == len(packets) * 960 * 2
        # From 50 to 450 ms, past the encoder's start-up and before the run's end, the tone is at about its level
        # (8000 / √2).
        assert 4_500 < rms(pcm[4_800:43_200]) < 7_000

    def test_resamples_to_the_requested_rate_and_channels(self) -> None:
        encoder = OpusEncoder()
        packets = encoder.encode(tone(0.2), timestamp_us=ORIGIN_US) + encoder.flush()
        decoder = OpusDecoder(rate=16_000, channels=2)

        pcm = b"".join(decoder.decode(packet.payload) for packet in packets)

        samples = len(pcm) // 4
        # 220 ms at 16 kHz, less what the resampler still holds.
        assert 3_400 <= samples <= 3_520

    def test_reset_starts_from_a_clean_state(self) -> None:
        encoder = OpusEncoder()
        packets = encoder.encode(tone(0.1), timestamp_us=ORIGIN_US)
        decoder = OpusDecoder()
        decoder.decode(packets[0].payload)

        decoder.reset()

        assert len(decoder.decode(packets[3].payload)) == 960 * 2

    def test_refuses_an_empty_payload(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            OpusDecoder().decode(b"")


def test_the_package_root_imports_without_pyav() -> None:
    blocked = (
        "import sys; sys.modules['av'] = None; import moq_media; print(moq_media.opus_packet_duration_us(b'\\x08'))"
    )
    result = subprocess.run([sys.executable, "-c", blocked], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "20000"


def test_the_opus_module_names_the_extra_without_pyav() -> None:
    blocked = "import sys; sys.modules['av'] = None; import moq_media.opus"
    result = subprocess.run([sys.executable, "-c", blocked], capture_output=True, text=True, check=False)
    assert result.returncode != 0
    assert "mux-moq-media[opus]" in result.stderr
