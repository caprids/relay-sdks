"""Independent MSF draft-01 catalog construction."""

from __future__ import annotations

import base64
import json
import math
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal

from .h264 import avc_codec_string
from .loc import AUDIO_TRACK, VIDEO_TRACK

MSF_CATALOG_VERSION = "draft-01"


@dataclass(frozen=True, slots=True, kw_only=True)
class VideoTrackInfo:
    name: str = VIDEO_TRACK
    codec: str
    """WebCodecs registry codec string, e.g. ``avc1.42E01F``."""
    width: int | None = None
    height: int | None = None
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


@dataclass(frozen=True, slots=True, kw_only=True)
class DataTrackInfo:
    name: str
    role: str | None = None


_MEDIA_ROLES = frozenset({"video", "audio", "caption", "subtitle", "audiodescription", "signlanguage"})


def _prune(entry: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in entry.items() if value is not None}


def build_msf_catalog(
    namespace: Sequence[str],
    *,
    video: VideoTrackInfo | None = None,
    audio: AudioTrackInfo | None = None,
    data: Sequence[DataTrackInfo] = (),
    generated_at_ms: int | None = None,
) -> bytes:
    """The independent MSF catalog JSON for a live LOC broadcast, byte-compatible with the Video.js parser."""
    if isinstance(namespace, str):
        raise ValueError("namespace must be a sequence of fields, not a string")
    if not namespace or any(not field or "/" in field for field in namespace):
        raise ValueError("catalog namespace fields must be non-empty and contain no slash")
    descriptions = [track for track in (video, audio, *data) if track is not None]
    names = [track.name for track in descriptions]
    if any(not name for name in names) or len(set(names)) != len(names):
        raise ValueError("catalog track names must be non-empty and unique")
    if any(track.role in _MEDIA_ROLES for track in data):
        raise ValueError("data tracks must not use media roles")
    namespace_text = "/".join(namespace)
    shared: dict[str, object] = {"namespace": namespace_text, "packaging": "loc", "isLive": True}
    init_data_list: list[dict[str, str]] = []
    tracks: list[dict[str, object]] = []

    def register_init_data(name: str, init_data: bytes | None) -> dict[str, object]:
        if not init_data:
            return {}

        identifier = f"{name}-init"
        init_data_list.append({"id": identifier, "type": "inline", "data": base64.b64encode(init_data).decode()})
        return {"initRef": identifier}

    h264 = video is not None and video.codec.split(".")[0] in {"avc1", "avc3"}
    if video is not None and (not h264 or video.init_data):
        codec = video.codec
        if h264 and video.init_data is not None:
            codec = video.codec.split(".")[0] + avc_codec_string(video.init_data)[4:]
        tracks.append(
            _prune(
                {
                    **shared,
                    **register_init_data(video.name, video.init_data),
                    "name": video.name,
                    "role": "video",
                    "codec": codec,
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
                    "samplerate": 48_000 if audio.codec == "opus" else audio.samplerate,
                    "channelConfig": audio.channel_config,
                    "bitrate": audio.bitrate,
                }
            )
        )

    for track in data:
        tracks.append(_prune({**shared, "name": track.name, "role": track.role}))

    catalog = _prune(
        {
            "version": MSF_CATALOG_VERSION,
            "generatedAt": generated_at_ms,
            "isComplete": True,
            "tracks": tracks,
            "initDataList": init_data_list or None,
        }
    )

    return _encode_json(catalog).encode("utf-8")


def _encode_json(value: object) -> str:
    """Match JSON.stringify's number spelling as well as its compact UTF-8 string encoding."""
    if isinstance(value, dict):
        return "{" + ",".join(f"{_encode_json(key)}:{_encode_json(item)}" for key, item in value.items()) + "}"
    if isinstance(value, list):
        return "[" + ",".join(_encode_json(item) for item in value) + "]"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError("catalog numbers must be finite")
        if isinstance(value, int) or value.is_integer():
            if abs(value) > 2**53 - 1:
                raise ValueError("catalog integers must be safe integers")
            return str(int(value))
        # Python switches to exponent notation at 1e-4; ECMAScript does so below 1e-6.
        text = repr(value)
        if "e" in text:
            mantissa, exponent = text.split("e")
            if abs(value) >= 1e-6:
                return format(Decimal(text), "f")
            return f"{mantissa}e{int(exponent)}"
        return text
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
