"""Opus packet facts read from the bytes alone. No codec or transport dependencies."""

OPUS_SAMPLE_RATE = 48_000
"""The rate every Opus stream decodes at (RFC 7845 §5.1), whatever rate the encoder was fed."""

OPUS_FRAME_US = 20_000
"""The frame length :class:`moq_media.opus.OpusEncoder` emits, and the LOC mapping's one frame per group."""

# SILK: 10, 20, 40, 60 ms; hybrid: 10, 20 ms; CELT: 2.5, 5, 10, 20 ms (RFC 6716 §3.1, Table 2).
_SILK_US = (10_000, 20_000, 40_000, 60_000)
_HYBRID_US = (10_000, 20_000)
_CELT_US = (2_500, 5_000, 10_000, 20_000)


def opus_packet_duration_us(packet: bytes) -> int | None:
    """An Opus packet's duration in microseconds from its TOC byte (RFC 6716 §3.1), or ``None`` if truncated.

    Code 0 holds one frame, codes 1 and 2 two, and code 3 the count in the next byte's low six bits.
    """
    if not packet:
        return None
    toc = packet[0]
    config = toc >> 3
    if config < 12:
        frame_us = _SILK_US[config % 4]
    elif config < 16:
        frame_us = _HYBRID_US[config % 2]
    else:
        frame_us = _CELT_US[config % 4]
    code = toc & 3
    if code == 0:
        return frame_us
    if code != 3:
        return 2 * frame_us
    if len(packet) < 2:
        return None
    return (packet[1] & 0x3F) * frame_us
