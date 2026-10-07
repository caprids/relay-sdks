"""Independent MSF draft-01 catalog construction and reading."""

from __future__ import annotations

import base64
import json
import math
import re
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal

from .h264 import avc_codec_string
from .loc import AUDIO_TRACK, VIDEO_TRACK

MSF_CATALOG_VERSION = "draft-01"


@dataclass(frozen=True, slots=True, kw_only=True)
class VideoTrackInfo:
    """The video track :func:`build_msf_catalog` describes.

    An ``avc1`` or ``avc3`` track needs ``init_data``: without it the track is left out of the
    catalog, and with it the codec string's profile and level come from the avcC. Fields left as
    ``None`` are omitted from the catalog.
    """

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
    """The audio track :func:`build_msf_catalog` describes. Fields left as ``None`` are omitted."""

    name: str = AUDIO_TRACK
    codec: str = "opus"
    samplerate: int = 48_000
    """Opus always declares 48000, the rate it decodes at, whatever is given here."""
    channel_config: str = "2"
    bitrate: int | None = None
    init_data: bytes | None = None
    """Decoder configuration, emitted inline as ``initDataList``."""


@dataclass(frozen=True, slots=True, kw_only=True)
class DataTrackInfo:
    """A non-media track :func:`build_msf_catalog` lists by name and role only."""

    name: str
    role: str | None = None
    """Must not be a media role (``video``, ``audio``, ``caption`` and the like)."""


_MEDIA_ROLES = frozenset({"video", "audio", "caption", "subtitle", "audiodescription", "signlanguage"})


def _prune(entry: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in entry.items() if value is not None}


MEDIA_RENDER_GROUP = 1
"""The render group (msf-01 §5.2.11) every video and audio entry joins: one broadcast timeline, played together.

The builder sets no target latency or buffer, which §5.2.8 and §5.2.9 require to be identical across a render group.
"""


def build_msf_catalog(
    namespace: Sequence[str],
    *,
    video: VideoTrackInfo | None = None,
    audio: AudioTrackInfo | None = None,
    data: Sequence[DataTrackInfo] = (),
    generated_at_ms: int | None = None,
) -> bytes:
    """The independent MSF catalog JSON for a live LOC broadcast, byte-compatible with the Video.js parser.

    The catalog leaves ``isComplete`` out: msf-01 §5.1.3 forbids it while false, and once present it can
    never be removed. End a broadcast with :func:`build_msf_end_catalog`.
    """
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
                    "renderGroup": MEDIA_RENDER_GROUP,
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
                    "renderGroup": MEDIA_RENDER_GROUP,
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
            "tracks": tracks,
            "initDataList": init_data_list or None,
        }
    )

    return _encode_json(catalog).encode("utf-8")


def build_msf_end_catalog(*, generated_at_ms: int | None = None) -> bytes:
    """The catalog that ends a broadcast (msf-01 §11.3): ``isComplete: true`` and no tracks.

    It commits that no track will be added and no content published, so send it only for a broadcast
    that ends on purpose, once every media track has ended (closing a track sends PUBLISH_DONE Track
    Ended), and never on a crash or lost connection.
    """
    catalog = _prune({"version": MSF_CATALOG_VERSION, "generatedAt": generated_at_ms, "isComplete": True, "tracks": []})
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


# ---------------------------------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------------------------------

# msf-01 §5.1.1: a subscriber MUST NOT parse a catalog version it does not understand. Versions follow
# the "draft-XX" convention; the bare "1" is a lenient alias deployed producers emit.
_SUPPORTED_MSF_VERSIONS = frozenset({"draft-01", "1"})


@dataclass(frozen=True, slots=True, kw_only=True)
class MsfCatalogTrack:
    """One track object of an independent MSF catalog (msf-01 §5.2), as ``parse_msf_catalog`` reads it."""

    namespace: tuple[str, ...]
    """The track's own slash-joined ``namespace`` split into fields, else the catalog track's."""
    name: str
    packaging: str
    """Payload encapsulation as declared (``loc``, or e.g. moq-rs ``cmaf``/``legacy``); empty when absent."""
    is_live: bool
    """``False`` when absent."""
    role: str | None = None
    codec: str | None = None
    width: int | None = None
    height: int | None = None
    framerate: float | None = None
    samplerate: int | None = None
    channel_config: str | None = None
    bitrate: int | None = None
    render_group: int | None = None
    """Render group (§5.2.11): tracks sharing one are meant to play together."""
    init_data: bytes | None = None
    """Decoder init data, resolved from ``initRef`` through the catalog's inline ``initDataList``."""


@dataclass(frozen=True, slots=True, kw_only=True)
class MsfCatalog:
    """An independent MSF catalog (msf-01 §5.1), as ``parse_msf_catalog`` reads it."""

    version: str
    tracks: tuple[MsfCatalogTrack, ...]
    generated_at_ms: int | None = None
    is_complete: bool = False
    """``True`` only when the catalog declares the broadcast complete (§5.1.3)."""


def parse_msf_catalog(payload: bytes, *, catalog_namespace: Sequence[str]) -> MsfCatalog:
    """Read an independent MSF catalog object's payload into its track descriptions.

    ``catalog_namespace`` is the namespace of the catalog track itself, which tracks that declare
    none inherit (§5.2.2). Raises ``ValueError`` on a payload that is not a UTF-8 JSON object, a
    missing or unsupported ``version``, a missing ``tracks`` array, or a delta update
    (``deltaUpdate``, not supported). Everything else is lenient, as msf-01 requires: unknown fields
    and fields of the wrong type are ignored, track entries without a string ``name`` are skipped,
    and packaging is returned as declared, not interpreted.
    """
    if isinstance(catalog_namespace, str):
        raise ValueError("catalog_namespace must be a sequence of fields, not a string")
    raw = json.loads(payload.decode("utf-8"), parse_constant=_reject_constant)
    if not isinstance(raw, dict):
        raise ValueError("MSF catalog is not a JSON object")
    if "deltaUpdate" in raw:
        raise ValueError("MSF delta updates are not supported")
    version = raw.get("version")
    if not isinstance(version, str):
        raise ValueError("MSF catalog is missing its version")
    if version not in _SUPPORTED_MSF_VERSIONS:
        raise ValueError(f"unsupported MSF catalog version {version}")
    entries = raw.get("tracks")
    if not isinstance(entries, list):
        raise ValueError("MSF catalog is missing its tracks array")

    init_data_list = _parse_init_data_list(raw.get("initDataList"))
    inherited = tuple(catalog_namespace)
    tracks = tuple(
        track
        for entry in entries
        if isinstance(entry, dict) and (track := _parse_track(entry, inherited, init_data_list)) is not None
    )

    return MsfCatalog(
        version=version,
        tracks=tracks,
        generated_at_ms=_integer(raw.get("generatedAt")),
        is_complete=raw.get("isComplete") is True,
    )


def _reject_constant(name: str) -> object:
    # JSON.parse rejects NaN and Infinity; so does this reader.
    raise ValueError(f"MSF catalog is not valid JSON: {name}")


def _parse_track(
    raw: dict[str, object], inherited: tuple[str, ...], init_data_list: dict[str, bytes]
) -> MsfCatalogTrack | None:
    name = raw.get("name")
    if not isinstance(name, str):
        return None

    # msf-01 leaves the tuple's spelling open; this dialect (Video.js, and build_msf_catalog) joins
    # the fields with "/", which the builder therefore rejects inside a field. Empty fields are dropped.
    namespace = raw.get("namespace")
    init_ref = _string(raw.get("initRef"))

    return MsfCatalogTrack(
        namespace=tuple(field for field in namespace.split("/") if field) if isinstance(namespace, str) else inherited,
        name=name,
        packaging=_string(raw.get("packaging")) or "",
        is_live=raw.get("isLive") is True,
        role=_string(raw.get("role")),
        codec=_string(raw.get("codec")),
        width=_integer(raw.get("width")),
        height=_integer(raw.get("height")),
        framerate=_number(raw.get("framerate")),
        samplerate=_integer(raw.get("samplerate")),
        channel_config=_string(raw.get("channelConfig")),
        bitrate=_integer(raw.get("bitrate")),
        render_group=_integer(raw.get("renderGroup")),
        init_data=None if init_ref is None else init_data_list.get(init_ref),
    )


def _parse_init_data_list(raw: object) -> dict[str, bytes]:
    init_data: dict[str, bytes] = {}
    if not isinstance(raw, list):
        return init_data
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        identifier, kind, data = entry.get("id"), entry.get("type", "inline"), entry.get("data")
        if isinstance(identifier, str) and isinstance(data, str) and kind == "inline":
            init_data[identifier] = _forgiving_base64(data)
    return init_data


_BASE64 = re.compile(r"[A-Za-z0-9+/]*")


def _forgiving_base64(text: str) -> bytes:
    """Decode base64 as the WHATWG ``atob`` does: ASCII whitespace ignored, padding optional."""
    data = re.sub(r"[\t\n\f\r ]", "", text)
    if len(data) % 4 == 0:
        data = data.removesuffix("==") if data.endswith("==") else data.removesuffix("=")
    if len(data) % 4 == 1 or not _BASE64.fullmatch(data):
        raise ValueError("MSF catalog init data is not base64")
    return base64.b64decode(data + "=" * (-len(data) % 4))


def _string(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _integer(value: object) -> int | None:
    """Counts (pixels, bits per second, hertz, milliseconds) are read only when integral."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float) and not value.is_integer():
        return None
    return int(value)
