"""The media-format side of streaming to a Video.js MoQ player, kept free of encoders and networking.

Everything here mirrors what the Video.js v10 MoQ engine (``packages/spf/src/media/moq``) reads:

- **LOC packaging** (draft-ietf-moq-loc-04): one encoded frame per MOQT object, with the frame's
  Timestamp (0x10), Timescale (0x08), and, on keyframes, the codec's Video Config (0x0D) carried as
  object properties. A group is one GOP; object 0 is the keyframe.
- **MSF catalog** (draft-ietf-moq-msf-01 §5): the JSON document published on the ``catalog`` track,
  one independent catalog per group. Codec strings are WebCodecs registry strings and the H.264
  decoder configuration (avcC) rides inline as ``initDataList``.
- **MSF source URLs** (msf-01 §11.1): ``moqt://relay/path#msf:<namespace>--catalog&c4m=<token>``,
  the string a player takes as its source.
- **AVCC** (ISO/IEC 14496-15): the length-prefixed H.264 bitstream an ``avc1`` codec string promises,
  converted from the Annex B byte stream libx264 emits, plus the avcC record built from SPS/PPS.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from urllib.parse import quote, urlsplit, urlunsplit

from mux_relay import KeyValuePair

# ---------------------------------------------------------------------------------------------------
# LOC object properties
# ---------------------------------------------------------------------------------------------------

LOC_TIMESTAMP = 0x10
LOC_TIMESCALE = 0x08
LOC_VIDEO_CONFIG = 0x0D
LOC_AUDIO_CONFIG = 0x0F
MICROSECONDS_PER_SECOND = 1_000_000

CATALOG_TRACK = "catalog"
VIDEO_TRACK = "video"
AUDIO_TRACK = "audio"

TRACK_TIMESCALE_PROPERTIES = [KeyValuePair(LOC_TIMESCALE, MICROSECONDS_PER_SECOND)]
"""Track properties declared on every media track: timestamps are microseconds."""


def loc_properties(
    timestamp_us: int, *, video_config: bytes | None = None, audio_config: bytes | None = None
) -> list[KeyValuePair]:
    """Object properties to send for one LOC frame; pass the codec config only on keyframes.

    Relay 0.14.17 drops the object Timescale and Video Config properties. The example also
    declares the track timescale and carries decoder configuration in the MSF catalog.
    """
    properties = [KeyValuePair(LOC_TIMESTAMP, timestamp_us), KeyValuePair(LOC_TIMESCALE, MICROSECONDS_PER_SECOND)]

    if video_config is not None:
        properties.append(KeyValuePair(LOC_VIDEO_CONFIG, bytes(video_config)))

    if audio_config is not None:
        properties.append(KeyValuePair(LOC_AUDIO_CONFIG, bytes(audio_config)))

    return properties


# ---------------------------------------------------------------------------------------------------
# H.264: Annex B <-> AVCC
# ---------------------------------------------------------------------------------------------------

NAL_SLICE_IDR = 5
NAL_SEI = 6
NAL_SPS = 7
NAL_PPS = 8
NAL_AUD = 9

_PARAMETER_AND_DELIMITER_NALS = frozenset({NAL_SPS, NAL_PPS, NAL_AUD})


def iter_annexb_nal_units(data: bytes) -> Iterator[bytes]:
    """Split an Annex B byte stream (3- or 4-byte start codes) into NAL units without their start codes."""
    length = len(data)
    index = 0
    start: int | None = None

    while index + 2 < length:
        if data[index] == 0 and data[index + 1] == 0 and data[index + 2] == 1:
            if start is not None:
                end = index

                # A 4-byte start code's leading zero belongs to the code, not the previous NAL.
                if end > start and data[end - 1] == 0:
                    end -= 1

                if end > start:
                    yield data[start:end]

            index += 3
            start = index
            continue

        index += 1

    if start is not None and start < length:
        yield data[start:]


def nal_unit_type(nal: bytes) -> int:
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

    first = sps[0]
    if len(first) < 4:
        raise ValueError("SPS is too short")

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


# ---------------------------------------------------------------------------------------------------
# MSF catalog
# ---------------------------------------------------------------------------------------------------

MSF_CATALOG_VERSION = "draft-01"


@dataclass(frozen=True, slots=True, kw_only=True)
class VideoTrackInfo:
    name: str = VIDEO_TRACK
    codec: str
    """WebCodecs registry codec string, e.g. ``avc1.42E01F``."""
    width: int
    height: int
    framerate: float | None = None
    bitrate: int | None = None
    init_data: bytes | None = None
    """Decoder configuration (avcC for ``avc1``), emitted inline as ``initDataList``."""


@dataclass(frozen=True, slots=True, kw_only=True)
class AudioTrackInfo:
    name: str = AUDIO_TRACK
    codec: str = "opus"
    samplerate: int = 48_000
    channel_config: str = "2"
    bitrate: int | None = None
    init_data: bytes | None = None


def _prune(entry: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in entry.items() if value is not None}


def build_msf_catalog(
    namespace: Sequence[str],
    *,
    video: VideoTrackInfo | None = None,
    audio: AudioTrackInfo | None = None,
    generated_at_ms: int | None = None,
) -> bytes:
    """The independent MSF catalog JSON for a live LOC broadcast, byte-compatible with the Video.js parser."""
    namespace_text = "/".join(namespace)
    shared: dict[str, object] = {"namespace": namespace_text, "packaging": "loc", "isLive": True}
    init_data_list: list[dict[str, str]] = []
    tracks: list[dict[str, object]] = []

    def register_init_data(name: str, init_data: bytes | None) -> dict[str, object]:
        if init_data is None:
            return {}

        identifier = f"{name}-init"
        init_data_list.append({"id": identifier, "type": "inline", "data": base64.b64encode(init_data).decode()})
        return {"initRef": identifier}

    if video is not None:
        tracks.append(
            _prune(
                {
                    **shared,
                    **register_init_data(video.name, video.init_data),
                    "name": video.name,
                    "role": "video",
                    "codec": video.codec,
                    "width": video.width,
                    "height": video.height,
                    "framerate": video.framerate,
                    "bitrate": video.bitrate,
                }
            )
        )

    if audio is not None:
        tracks.append(
            _prune(
                {
                    **shared,
                    **register_init_data(audio.name, audio.init_data),
                    "name": audio.name,
                    "role": "audio",
                    "codec": audio.codec,
                    "samplerate": audio.samplerate,
                    "channelConfig": audio.channel_config,
                    "bitrate": audio.bitrate,
                }
            )
        )

    catalog = _prune(
        {
            "version": MSF_CATALOG_VERSION,
            "generatedAt": generated_at_ms,
            "isComplete": True,
            "tracks": tracks,
            "initDataList": init_data_list or None,
        }
    )
    return json.dumps(catalog, separators=(",", ":")).encode("utf-8")


# ---------------------------------------------------------------------------------------------------
# MSF source URLs (msf-01 §11.1)
# ---------------------------------------------------------------------------------------------------


def encode_name_component(value: str) -> str:
    """MSF namespace/name string form: ``[a-zA-Z0-9_]`` literal, every other UTF-8 byte as ``.xx`` lowercase hex."""
    out: list[str] = []

    for byte in value.encode("utf-8"):
        char = chr(byte)

        if char.isascii() and (char.isalnum() or char == "_"):
            out.append(char)
        else:
            out.append(f".{byte:02x}")

    return "".join(out)


def encode_namespace_name(namespace: Sequence[str], track_name: str) -> str:
    """The ``ns1-ns2--name`` identifier that follows ``#msf:`` in a source URL (msf-01 §11.1.2)."""
    return "-".join(encode_name_component(field) for field in namespace) + "--" + encode_name_component(track_name)


def compose_player_url(relay_url: str, namespace: Sequence[str], *, token: str | None = None) -> str:
    """Compose the ``moqt://`` MSF source URL a Video.js MoQ player takes, the inverse of its parser.

    The relay origin may be written as ``https://`` or ``moqt://``; an existing path and query
    survive. The token, when given, rides the ``c4m`` fragment parameter, which the player moves onto
    the connect URL's ``jwt`` query parameter.
    """
    parts = urlsplit(relay_url)

    if parts.scheme not in ("https", "moqt"):
        raise ValueError("relay URL must be https:// or moqt://")

    if parts.fragment:
        raise ValueError("relay URL must not carry a fragment")

    fields = [field for field in namespace if field]
    if not fields:
        raise ValueError("namespace is empty")

    base = urlunsplit(("moqt", parts.netloc, parts.path or "/", parts.query, ""))
    fragment = f"msf:{encode_namespace_name(fields, CATALOG_TRACK)}"

    if token:
        fragment += f"&c4m={quote(token, safe='')}"

    return f"{base}#{fragment}"


__all__ = [
    "AUDIO_TRACK",
    "CATALOG_TRACK",
    "LOC_AUDIO_CONFIG",
    "LOC_TIMESCALE",
    "LOC_TIMESTAMP",
    "LOC_VIDEO_CONFIG",
    "MICROSECONDS_PER_SECOND",
    "MSF_CATALOG_VERSION",
    "TRACK_TIMESCALE_PROPERTIES",
    "VIDEO_TRACK",
    "AudioTrackInfo",
    "VideoTrackInfo",
    "annexb_to_avcc",
    "avc_codec_string",
    "build_avcc",
    "build_msf_catalog",
    "compose_player_url",
    "encode_name_component",
    "encode_namespace_name",
    "extract_parameter_sets",
    "iter_annexb_nal_units",
    "loc_properties",
    "nal_unit_type",
]
