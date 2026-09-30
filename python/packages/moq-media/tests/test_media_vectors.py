"""Both language implementations consume the same independent format expectations."""

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import moq_media as media

SPEC = Path(__file__).resolve().parents[4] / "spec"
VECTORS = json.loads((SPEC / "vectors/media.json").read_text())


def track_input(raw: dict[str, Any]) -> dict[str, Any]:
    names = {"initData": "init_data", "channelConfig": "channel_config"}
    result = {names.get(key, key): value for key, value in raw.items()}
    for key in ("width", "height", "samplerate", "bitrate"):
        if key in result:
            result[key] = int(result[key])
    if "framerate" in result:
        result["framerate"] = float(result["framerate"])
    if "init_data" in result:
        result["init_data"] = bytes.fromhex(result["init_data"])
    return result


def build(raw: dict[str, Any]) -> bytes:
    return media.build_msf_catalog(
        raw["namespace"],
        video=media.VideoTrackInfo(**track_input(raw["video"])) if "video" in raw else None,
        audio=media.AudioTrackInfo(**track_input(raw["audio"])) if "audio" in raw else None,
        data=[media.DataTrackInfo(**track) for track in raw.get("data", [])],
        generated_at_ms=int(raw["generatedAt"]) if "generatedAt" in raw else None,
    )


@pytest.mark.parametrize("vector", VECTORS["catalogs"], ids=lambda v: v["name"])
def test_catalog_bytes(vector: dict[str, Any]) -> None:
    assert build(vector["input"]) == vector["json"].encode("utf-8")


@pytest.mark.parametrize("vector", VECTORS["catalogErrors"], ids=lambda v: v["name"])
def test_catalog_errors(vector: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        build(vector["input"])


def readable(catalog: media.MsfCatalog) -> dict[str, Any]:
    """The vectors' ``parsed`` form: camelCase, lists for tuples, init data as hex, ``None`` omitted."""
    names = {"is_live": "isLive", "channel_config": "channelConfig", "init_data": "initData"}
    tracks = []
    for track in catalog.tracks:
        fields: dict[str, Any] = {}
        for key in track.__dataclass_fields__:
            value = getattr(track, key)
            if value is not None:
                fields[names.get(key, key)] = (
                    value.hex() if isinstance(value, bytes) else list(value) if isinstance(value, tuple) else value
                )
        tracks.append(fields)
    result: dict[str, Any] = {"version": catalog.version}
    if catalog.generated_at_ms is not None:
        result["generatedAt"] = catalog.generated_at_ms
    return result | {"isComplete": catalog.is_complete, "tracks": tracks}


@pytest.mark.parametrize("vector", VECTORS["catalogs"], ids=lambda v: v["name"])
def test_catalog_round_trip(vector: dict[str, Any]) -> None:
    parsed = media.parse_msf_catalog(build(vector["input"]), catalog_namespace=vector["input"]["namespace"])
    assert readable(parsed) == vector["parsed"]


@pytest.mark.parametrize("vector", VECTORS["catalogs"], ids=lambda v: v["name"])
def test_catalog_reads_back_its_inputs(vector: dict[str, Any]) -> None:
    raw = vector["input"]
    parsed = {
        track.name: track for track in media.parse_msf_catalog(build(raw), catalog_namespace=raw["namespace"]).tracks
    }
    video = media.VideoTrackInfo(**track_input(raw["video"])) if "video" in raw else None
    audio = media.AudioTrackInfo(**track_input(raw["audio"])) if "audio" in raw else None
    inputs = [track for track in (video, audio) if track is not None and track.name in parsed]
    for track in inputs:
        read = parsed[track.name]
        assert read.namespace == tuple(raw["namespace"])
        role = "video" if isinstance(track, media.VideoTrackInfo) else "audio"
        assert (read.packaging, read.is_live, read.role) == ("loc", True, role)
        assert read.bitrate == track.bitrate
        assert read.init_data == (track.init_data or None)
        if isinstance(track, media.VideoTrackInfo):
            assert (read.width, read.height, read.framerate) == (track.width, track.height, track.framerate)
            if not track.codec.startswith(("avc1", "avc3")):
                assert read.codec == track.codec
        else:
            assert (read.codec, read.channel_config) == (track.codec, track.channel_config)
            assert read.samplerate == (48_000 if track.codec == "opus" else track.samplerate)
    for track in raw.get("data", []):
        assert (parsed[track["name"]].role, parsed[track["name"]].codec) == (track.get("role"), None)


@pytest.mark.parametrize("vector", VECTORS["catalogReads"], ids=lambda v: v["name"])
def test_catalog_reads(vector: dict[str, Any]) -> None:
    parsed = media.parse_msf_catalog(vector["json"].encode(), catalog_namespace=vector["catalogNamespace"])
    assert readable(parsed) == vector["parsed"]


@pytest.mark.parametrize("vector", VECTORS["catalogReadErrors"], ids=lambda v: v["name"])
def test_catalog_read_errors(vector: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        media.parse_msf_catalog(vector["json"].encode(), catalog_namespace=vector["catalogNamespace"])


def test_catalog_read_rejects_non_utf8() -> None:
    with pytest.raises(ValueError):
        media.parse_msf_catalog(b'{"version":"draft-01","tracks":[],"x":"\xff"}', catalog_namespace=["room"])
    with pytest.raises(ValueError, match="sequence of fields"):
        media.parse_msf_catalog(b'{"version":"draft-01","tracks":[]}', catalog_namespace="room")


@pytest.mark.parametrize("vector", VECTORS["loc"], ids=lambda v: v["name"])
def test_loc(vector: dict[str, Any]) -> None:
    properties = media.loc_properties(
        int(vector["timestampUs"]),
        timescale=int(vector.get("timescale", "1000000")),
        video_config=bytes.fromhex(vector["videoConfig"]) if "videoConfig" in vector else None,
        audio_config=bytes.fromhex(vector["audioConfig"]) if "audioConfig" in vector else None,
    )
    assert [[p.type, p.value.hex() if isinstance(p.value, bytes) else str(p.value)] for p in properties] == vector[
        "properties"
    ]
    assert media.loc_track_properties(int(vector.get("timescale", "1000000"))) == [properties[1]]


@pytest.mark.parametrize("vector", VECTORS["urls"], ids=lambda v: v["name"])
def test_urls(vector: dict[str, Any]) -> None:
    assert (
        media.compose_player_url(
            vector["origin"],
            vector["namespace"],
            token=vector.get("token"),
            track_name=vector.get("trackName", "catalog"),
        )
        == vector["url"]
    )


@pytest.mark.parametrize("vector", VECTORS["urlErrors"])
def test_url_errors(vector: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        media.compose_player_url(vector["origin"], vector["namespace"])


@pytest.mark.parametrize("vector", VECTORS["h264"], ids=lambda v: v["name"])
def test_h264(vector: dict[str, Any]) -> None:
    data = bytes.fromhex(vector["annexb"])
    sps, pps = media.extract_parameter_sets(data)
    assert [nal.hex() for nal in sps] == vector["sps"]
    assert [nal.hex() for nal in pps] == vector["pps"]
    config = media.H264Config.from_annexb(data)
    assert config.avcc.hex() == vector["avcc"]
    assert config.codec == vector["codec"]


@pytest.mark.parametrize("vector", VECTORS["accessUnits"], ids=lambda v: v["name"])
def test_access_units(vector: dict[str, Any]) -> None:
    data = bytes.fromhex(vector["annexb"])
    assert media.annexb_to_avcc(data).hex() == vector["avcc"]
    assert media.annexb_to_avcc(data, drop_parameter_sets=False).hex() == vector["allAvcc"]
    assert media.H264Config(b"\x01\x42\xc0\x14").to_avcc(data).hex() == vector["avcc"]


@pytest.mark.parametrize(
    "timestamp,timescale", [(-1, 1000000), (0, 0), (0, -1), (2**53, 1000000), (2**53 - 1, 2**53 - 1)]
)
def test_rejects_invalid_loc_numbers(timestamp: int, timescale: int) -> None:
    with pytest.raises(ValueError):
        media.loc_properties(timestamp, timescale=timescale)


@pytest.mark.parametrize(
    "sps,pps",
    [
        ([], [b"\x68"]),
        ([b"\x67\x42\xc0\x14"], []),
        ([b"\x67\x42\xc0\x14"] * 32, [b"\x68"]),
        ([b"\x67\x42\xc0\x14"], [b"\x68"] * 256),
        ([b"\x68\x42\xc0\x14"], [b"\x68"]),
        ([b"\x67\x42\xc0\x14"], [b"\x68" * 65536]),
    ],
)
def test_parameter_set_limits(sps: list[bytes], pps: list[bytes]) -> None:
    with pytest.raises(ValueError):
        media.build_avcc(sps, pps)


def test_protocol_pins() -> None:
    pin = json.loads((SPEC / "protocol.json").read_text())["media"]
    assert pin == {
        "locDocument": media.LOC_DOCUMENT,
        "msfDocument": media.MSF_DOCUMENT,
        "catalogVersion": media.MSF_CATALOG_VERSION,
        "defaultTimescale": media.MICROSECONDS_PER_SECOND,
        "tracks": {"catalog": media.CATALOG_TRACK, "video": media.VIDEO_TRACK, "audio": media.AUDIO_TRACK},
        "properties": {
            "timestamp": media.LOC_TIMESTAMP,
            "timescale": media.LOC_TIMESCALE,
            "videoConfig": media.LOC_VIDEO_CONFIG,
            "audioConfig": media.LOC_AUDIO_CONFIG,
        },
    }


def test_import_needs_no_transport_or_product() -> None:
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, moq_media; assert not {'mux_relay', 'aioquic', 'av', 'numpy'} & sys.modules.keys()",
        ],
        check=True,
    )


@pytest.mark.parametrize("vector", VECTORS["h264Errors"], ids=lambda v: v["name"])
def test_unsupported_avcc_profiles(vector: dict[str, Any]) -> None:
    sps = [bytes.fromhex(nal) for nal in vector["sps"]]
    pps = [bytes.fromhex(nal) for nal in vector["pps"]]
    with pytest.raises(ValueError, match="profiles"):
        media.build_avcc(sps, pps)
    with pytest.raises(ValueError, match="profiles"):
        media.H264Config.from_annexb(b"".join(b"\x00\x00\x01" + nal for nal in (*sps, *pps)))


def test_namespace_must_not_be_a_bare_string() -> None:
    with pytest.raises(ValueError, match="sequence of fields"):
        media.build_msf_catalog("room", audio=media.AudioTrackInfo())
    with pytest.raises(ValueError, match="sequence of fields"):
        media.compose_player_url("https://relay.example", "room")
    with pytest.raises(ValueError, match="sequence of fields"):
        media.encode_namespace_name("room", "catalog")
