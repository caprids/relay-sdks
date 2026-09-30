"""H.264 decoder configuration and Annex B access-unit conversion."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass

NAL_SLICE_IDR = 5
NAL_SEI = 6
NAL_SPS = 7
NAL_PPS = 8
NAL_AUD = 9

_PARAMETER_AND_DELIMITER_NALS = frozenset({NAL_SPS, NAL_PPS, NAL_AUD})


def iter_annexb_nal_units(data: bytes) -> Iterator[bytes]:
    """Split an Annex B byte stream (3- or 4-byte start codes) into NAL units without their start codes."""
    start: int | None = None
    index = data.find(b"\x00\x00\x01")
    while index != -1:
        if start is not None:
            nal = data[start:index].rstrip(b"\x00")
            if nal:
                yield nal
        start = index + 3
        index = data.find(b"\x00\x00\x01", start)

    if start is not None:
        nal = data[start:].rstrip(b"\x00")
        if nal:
            yield nal


def nal_unit_type(nal: bytes) -> int:
    """The ``nal_unit_type`` of a NAL unit without its start code: 7 for SPS, 8 for PPS, 5 for IDR."""
    return nal[0] & 0x1F


def annexb_to_avcc(data: bytes, *, drop_parameter_sets: bool = True) -> bytes:
    """Re-frame an Annex B access unit as 4-byte-length-prefixed NAL units.

    ``avc1`` promises parameter sets out of band, so SPS/PPS (and access-unit delimiters) are dropped
    by default; they travel in the avcC instead.
    """
    out = bytearray()

    for nal in iter_annexb_nal_units(data):
        if drop_parameter_sets and nal_unit_type(nal) in _PARAMETER_AND_DELIMITER_NALS:
            continue

        out += len(nal).to_bytes(4, "big")
        out += nal

    return bytes(out)


def extract_parameter_sets(data: bytes) -> tuple[list[bytes], list[bytes]]:
    """SPS and PPS NAL units found in an Annex B byte stream (an encoder's extradata or a keyframe)."""
    sps = [nal for nal in iter_annexb_nal_units(data) if nal_unit_type(nal) == NAL_SPS]
    pps = [nal for nal in iter_annexb_nal_units(data) if nal_unit_type(nal) == NAL_PPS]
    return sps, pps


def build_avcc(sps: Sequence[bytes], pps: Sequence[bytes]) -> bytes:
    """Build an AVCDecoderConfigurationRecord (avcC) with 4-byte NAL length prefixes."""
    if not sps or not pps:
        raise ValueError("an avcC needs at least one SPS and one PPS")

    if len(sps) > 31 or len(pps) > 255:
        raise ValueError("too many SPS or PPS NAL units")
    for units, kind in ((sps, NAL_SPS), (pps, NAL_PPS)):
        if any(not nal or len(nal) > 65535 or nal_unit_type(nal) != kind for nal in units):
            raise ValueError("invalid parameter-set type or length")

    first = sps[0]
    if len(first) < 4:
        raise ValueError("SPS is too short")

    # High-family profiles need SPS-derived chroma/bit-depth extension fields.
    # This stage supports construction for Baseline, Main, and Extended only.
    if any(len(nal) < 4 or nal[1] not in (66, 77, 88) for nal in sps):
        raise ValueError("avcC construction supports only Baseline, Main, and Extended profiles")

    record = bytearray()
    record.append(1)  # configurationVersion
    record += first[1:4]  # AVCProfileIndication, profile_compatibility, AVCLevelIndication
    record.append(0xFF)  # reserved (6 bits) + lengthSizeMinusOne = 3
    record.append(0xE0 | (len(sps) & 0x1F))  # reserved (3 bits) + numOfSequenceParameterSets

    for nal in sps:
        record += len(nal).to_bytes(2, "big")
        record += nal

    record.append(len(pps) & 0xFF)

    for nal in pps:
        record += len(nal).to_bytes(2, "big")
        record += nal

    return bytes(record)


def avc_codec_string(avcc: bytes) -> str:
    """The ``avc1.PPCCLL`` WebCodecs codec string an avcC declares (RFC 6381 §3.3)."""
    if len(avcc) < 4 or avcc[0] != 1:
        raise ValueError("not an AVCDecoderConfigurationRecord")

    return "avc1." + "".join(f"{byte:02X}" for byte in avcc[1:4])


@dataclass(frozen=True, slots=True)
class H264Config:
    """An avc1 decoder record and the codec string derived from it."""

    avcc: bytes

    @classmethod
    def from_annexb(cls, data: bytes) -> H264Config:
        """Build the record from the SPS and PPS in Annex B bytes (an encoder's extradata or a keyframe).

        Raises ``ValueError`` as :func:`build_avcc` does.
        """
        return cls(build_avcc(*extract_parameter_sets(data)))

    @property
    def codec(self) -> str:
        """The ``avc1.PPCCLL`` codec string the record declares."""
        return avc_codec_string(self.avcc)

    def to_avcc(self, data: bytes) -> bytes:
        """Re-frame an Annex B access unit for this configuration, dropping in-band SPS/PPS and AUDs."""
        return annexb_to_avcc(data)
