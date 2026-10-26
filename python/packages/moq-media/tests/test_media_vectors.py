"""Both language implementations consume the same independent format expectations."""

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import moq_media as media
from moq_core import KeyValuePair

SPEC = Path(__file__).resolve().parents[4] / "spec"
VECTORS = json.loads((SPEC / "vectors/media.json").read_text())


def track_input(raw: dict[str, Any]) -> dict[str, Any]:
    names = {
        "initData": "init_data",
        "channelConfig": "channel_config",
        "renderGroup": "render_group",
        "altGroup": "alt_group",
    }
    result = {names.get(key, key): value for key, value in raw.items() if key != "role"}
    for key in ("width", "height", "samplerate", "bitrate", "render_group", "alt_group"):
        if key in result:
            result[key] = int(result[key])
    if "framerate" in result:
        result["framerate"] = float(result["framerate"])
    if "init_data" in result:
        result["init_data"] = bytes.fromhex(result["init_data"])
    return result


def media_track(raw: dict[str, Any]) -> media.VideoTrackInfo | media.AudioTrackInfo:
    kind = media.VideoTrackInfo if raw["role"] == "video" else media.AudioTrackInfo
    return kind(**track_input(raw))


def build(raw: dict[str, Any]) -> bytes:
    return media.build_msf_catalog(
        raw["namespace"],
        video=media.VideoTrackInfo(**track_input(raw["video"])) if "video" in raw else None,
        audio=media.AudioTrackInfo(**track_input(raw["audio"])) if "audio" in raw else None,
        media=[media_track(track) for track in raw.get("media", [])],
        data=[media.DataTrackInfo(**track) for track in raw.get("data", [])],
        media_timeline=media.MediaTimelineTrackInfo(**raw["mediaTimeline"]) if "mediaTimeline" in raw else None,
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
    names = {
        "is_live": "isLive",
        "channel_config": "channelConfig",
        "init_data": "initData",
        "render_group": "renderGroup",
        "alt_group": "altGroup",
    }
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


def build_end(raw: dict[str, Any]) -> bytes:
    return media.build_msf_end_catalog(generated_at_ms=int(raw["generatedAt"]) if "generatedAt" in raw else None)


@pytest.mark.parametrize("vector", VECTORS["endCatalogs"], ids=lambda v: v["name"])
def test_end_catalog_bytes_and_round_trip(vector: dict[str, Any]) -> None:
    document = build_end(vector["input"])
    assert document == vector["json"].encode("utf-8")
    assert readable(media.parse_msf_catalog(document, catalog_namespace=["room"])) == vector["parsed"]


@pytest.mark.parametrize("vector", VECTORS["endCatalogErrors"], ids=lambda v: v["name"])
def test_end_catalog_errors(vector: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        build_end(vector["input"])


@pytest.mark.parametrize("vector", VECTORS["catalogs"], ids=lambda v: v["name"])
def test_catalog_reads_back_its_inputs(vector: dict[str, Any]) -> None:
    raw = vector["input"]
    parsed = {
        track.name: track for track in media.parse_msf_catalog(build(raw), catalog_namespace=raw["namespace"]).tracks
    }
    video = media.VideoTrackInfo(**track_input(raw["video"])) if "video" in raw else None
    audio = media.AudioTrackInfo(**track_input(raw["audio"])) if "audio" in raw else None
    listed = [video, audio, *(media_track(track) for track in raw.get("media", []))]
    inputs = [track for track in listed if track is not None and track.name in parsed]
    for track in inputs:
        read = parsed[track.name]
        assert read.namespace == tuple(raw["namespace"])
        role = "video" if isinstance(track, media.VideoTrackInfo) else "audio"
        assert (read.packaging, read.is_live, read.role) == ("loc", True, role)
        assert read.bitrate == track.bitrate
        assert read.init_data == (track.init_data or None)
        assert (read.render_group, read.alt_group) == (track.render_group, track.alt_group)
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
    if "groups" in vector:
        groups = [
            {"renderGroup": group.render_group, "altGroup": group.alt_group}
            for group in media.group_msf_tracks(parsed.tracks)
        ]
        expected = [{key: group.get(key) for key in ("renderGroup", "altGroup")} for group in vector["groups"]]
        assert groups == expected
        assert [[track.name for track in group.tracks] for group in media.group_msf_tracks(parsed.tracks)] == [
            group["tracks"] for group in vector["groups"]
        ]


@pytest.mark.parametrize("vector", VECTORS["catalogReadErrors"], ids=lambda v: v["name"])
def test_catalog_read_errors(vector: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        media.parse_msf_catalog(vector["json"].encode(), catalog_namespace=vector["catalogNamespace"])


def test_catalog_read_rejects_non_utf8() -> None:
    with pytest.raises(ValueError):
        media.parse_msf_catalog(b'{"version":"draft-01","tracks":[],"x":"\xff"}', catalog_namespace=["room"])
    with pytest.raises(ValueError, match="sequence of fields"):
        media.parse_msf_catalog(b'{"version":"draft-01","tracks":[]}', catalog_namespace="room")


def record(entry: media.MediaTimelineEntry) -> list[Any]:
    """The vectors' form of an entry: msf-01's ``[mediaTimeMs, [group, object], wallclockMs]``."""
    return [entry.media_time_ms, [entry.group, entry.object], entry.wallclock_ms]


def entry(raw: list[Any]) -> media.MediaTimelineEntry:
    return media.MediaTimelineEntry(media_time_ms=raw[0], group=raw[1][0], object=raw[1][1], wallclock_ms=raw[2])


def decimal(text: str) -> Any:
    """A vector's decimal string; fractional ones stay floats so the helpers can refuse them."""
    return float(text) if "." in text else int(text)


def timeline_entry(raw: dict[str, Any]) -> media.MediaTimelineEntry:
    return media.media_timeline_entry(
        decimal(raw["timestampUs"]),
        decimal(raw["group"]),
        object=decimal(raw.get("object", "0")),
        wallclock_us=decimal(raw["wallclockUs"]) if "wallclockUs" in raw else None,
    )


@pytest.mark.parametrize("vector", VECTORS["timelineEntries"], ids=lambda v: v["name"])
def test_timeline_entries(vector: dict[str, Any]) -> None:
    assert record(timeline_entry(vector)) == vector["entry"]


@pytest.mark.parametrize("vector", VECTORS["timelineEntryErrors"], ids=lambda v: v["name"])
def test_timeline_entry_errors(vector: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        timeline_entry(vector)


@pytest.mark.parametrize("vector", VECTORS["timelines"], ids=lambda v: v["name"])
def test_timeline_bytes_and_round_trip(vector: dict[str, Any]) -> None:
    document = media.build_media_timeline([entry(raw) for raw in vector["entries"]])
    assert document == vector["json"].encode("utf-8")
    assert [record(read) for read in media.parse_media_timeline(document)] == vector["entries"]


@pytest.mark.parametrize("vector", VECTORS["timelineErrors"], ids=lambda v: v["name"])
def test_timeline_errors(vector: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        media.build_media_timeline([entry(raw) for raw in vector["entries"]])


@pytest.mark.parametrize("vector", VECTORS["timelineReads"], ids=lambda v: v["name"])
def test_timeline_reads(vector: dict[str, Any]) -> None:
    read = media.parse_media_timeline(vector["json"].encode("utf-8"))
    assert [record(entry) for entry in read] == vector["entries"]
    assert all(isinstance(value, int) for entry in read for value in record(entry)[::2])


@pytest.mark.parametrize("vector", VECTORS["timelineReadErrors"], ids=lambda v: v["name"])
def test_timeline_read_errors(vector: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        media.parse_media_timeline(vector["json"].encode("utf-8"))


def test_timeline_read_rejects_non_utf8() -> None:
    with pytest.raises(ValueError):
        media.parse_media_timeline(b'[[0,[0,0],0],"\xff"]')


def media_timeline(raw: dict[str, Any]) -> media.MediaTimeline:
    return media.MediaTimeline(max_entries=decimal(raw["maxEntries"]))


@pytest.mark.parametrize("vector", VECTORS["timelineRetention"], ids=lambda v: v["name"])
def test_timeline_retention(vector: dict[str, Any]) -> None:
    timeline = media_timeline(vector)
    document = timeline.document
    for add in vector["adds"]:
        document = timeline.add(
            decimal(add["timestampUs"]),
            decimal(add["group"]),
            object=decimal(add.get("object", "0")),
            wallclock_us=decimal(add["wallclockUs"]) if "wallclockUs" in add else None,
        )
    assert document == timeline.document == vector["json"].encode("utf-8")
    assert [record(read) for read in timeline.entries] == json.loads(vector["json"])


@pytest.mark.parametrize("vector", VECTORS["timelineRetentionErrors"], ids=lambda v: v["name"])
def test_timeline_retention_errors(vector: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        media_timeline(vector)


def test_timeline_rejected_add_leaves_it_unchanged() -> None:
    timeline = media.MediaTimeline(max_entries=1)
    timeline.add(1_000, 0)
    with pytest.raises(ValueError):
        timeline.add(-1, 1)
    assert timeline.document == b"[[1,[0,0],1]]"


def test_a_new_timeline_starts_empty() -> None:
    # A supervised publisher's next generation is a new broadcast: it makes a new timeline, not a cleared one.
    media.MediaTimeline().add(1_000, 0)
    assert media.MediaTimeline().document == b"[]"


def test_timeline_entry_from_a_broadcast_clock() -> None:
    clock = media.BroadcastClock(now_us=lambda: 0, origin_us=0, origin_epoch_us=1_759_924_158_381_000)
    timeline = media.media_timeline_entry(clock.from_source_us(2_002_000), 1)
    assert record(timeline) == [1_759_924_160_383, [1, 0], 1_759_924_160_383]


def kv_pairs(raw: list[list[Any]]) -> list[KeyValuePair]:
    """The vectors' ``[type, decimal or hex]`` pairs: even types carry integers, odd types bytes."""
    return [KeyValuePair(t, bytes.fromhex(v) if t % 2 else int(v)) for t, v in raw]


@pytest.mark.parametrize("vector", VECTORS["locTimestamps"], ids=lambda v: v["name"])
def test_loc_timestamps(vector: dict[str, Any]) -> None:
    read = media.loc_timestamp_us(
        kv_pairs(vector["properties"]),
        track_properties=kv_pairs(vector.get("trackProperties", [])),
    )
    expected = vector["timestampUs"]
    assert read == (None if expected is None else int(expected))


def interrupt_input(raw: dict[str, Any]) -> bytes:
    return media.build_interrupt(
        until_us=decimal(raw["untilUs"]),
        **({"from_us": decimal(raw["fromUs"])} if "fromUs" in raw else {}),
        **({"track": raw["track"]} if "track" in raw else {}),
    )


@pytest.mark.parametrize("vector", VECTORS["interrupts"], ids=lambda v: v["name"])
def test_interrupt_bytes(vector: dict[str, Any]) -> None:
    payload = interrupt_input(vector["input"])
    assert payload == vector["json"].encode("utf-8")
    assert media.parse_interrupt(payload) == media.parse_interrupt(vector["json"].encode("utf-8"))


@pytest.mark.parametrize("vector", VECTORS["interruptErrors"], ids=lambda v: v["name"])
def test_interrupt_errors(vector: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        interrupt_input(vector["input"])


@pytest.mark.parametrize("vector", VECTORS["interruptReads"], ids=lambda v: v["name"])
def test_interrupt_reads(vector: dict[str, Any]) -> None:
    read = media.parse_interrupt(vector["json"].encode("utf-8"))
    parsed = vector["parsed"]
    assert read == media.Interrupt(
        track=parsed["track"],
        until_us=int(parsed["untilUs"]),
        from_us=int(parsed["fromUs"]) if "fromUs" in parsed else None,
    )


@pytest.mark.parametrize("vector", VECTORS["interruptReadErrors"], ids=lambda v: v["name"])
def test_interrupt_read_errors(vector: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        media.parse_interrupt(vector["json"].encode("utf-8"))


def transcript_input(raw: dict[str, Any]) -> bytes:
    return media.build_transcript_segment(
        seq=decimal(raw["seq"]),
        from_us=decimal(raw["fromUs"]),
        text=raw["text"],
        final=raw["final"],
        **({"until_us": decimal(raw["untilUs"])} if "untilUs" in raw else {}),
        **({"track": raw["track"]} if "track" in raw else {}),
    )


def transcript_segment(raw: dict[str, Any]) -> media.TranscriptSegment:
    return media.TranscriptSegment(
        seq=int(raw["seq"]),
        track=raw["track"],
        from_us=int(raw["fromUs"]),
        until_us=int(raw["untilUs"]) if "untilUs" in raw else None,
        text=raw["text"],
        final=raw["final"],
    )


@pytest.mark.parametrize("vector", VECTORS["transcriptSegments"], ids=lambda v: v["name"])
def test_transcript_segment_bytes(vector: dict[str, Any]) -> None:
    payload = transcript_input(vector["input"])
    assert payload == vector["json"].encode("utf-8")
    assert media.parse_transcript_segment(payload) == media.parse_transcript_segment(vector["json"].encode("utf-8"))


@pytest.mark.parametrize("vector", VECTORS["transcriptSegmentErrors"], ids=lambda v: v["name"])
def test_transcript_segment_errors(vector: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        transcript_input(vector["input"])


@pytest.mark.parametrize("vector", VECTORS["transcriptSegmentReads"], ids=lambda v: v["name"])
def test_transcript_segment_reads(vector: dict[str, Any]) -> None:
    read = media.parse_transcript_segment(vector["json"].encode("utf-8"))
    assert read == transcript_segment(vector["parsed"])


@pytest.mark.parametrize("vector", VECTORS["transcriptSegmentReadErrors"], ids=lambda v: v["name"])
def test_transcript_segment_read_errors(vector: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        media.parse_transcript_segment(vector["json"].encode("utf-8"))


def transcript_cut(transcript: media.Transcript, step: dict[str, Any]) -> None:
    transcript.cut(
        until_us=decimal(step["cut"]["untilUs"]),
        from_us=decimal(step["cut"]["fromUs"]) if "fromUs" in step["cut"] else None,
        position_us=float(step["positionUs"]) if "positionUs" in step else None,
    )


@pytest.mark.parametrize("vector", VECTORS["transcripts"], ids=lambda v: v["name"])
def test_transcripts(vector: dict[str, Any]) -> None:
    transcript = media.Transcript()
    for step in vector["steps"]:
        if "apply" in step:
            segment = media.parse_transcript_segment(transcript_input(step["apply"]))
            assert transcript.apply(segment) is step["applied"]
        else:
            transcript_cut(transcript, step)
    assert transcript.lines == [
        media.TranscriptLine(**vars_of(transcript_segment(line)), cut=line["cut"]) for line in vector["lines"]
    ]
    for shown in vector["linesAt"]:
        position = None if shown["positionUs"] is None else float(shown["positionUs"])
        assert [line.seq for line in transcript.lines_at(position)] == [int(seq) for seq in shown["seqs"]]


def vars_of(segment: media.TranscriptSegment) -> dict[str, Any]:
    return {name: getattr(segment, name) for name in segment.__dataclass_fields__}


@pytest.mark.parametrize("vector", VECTORS["transcriptCutErrors"], ids=lambda v: v["name"])
def test_transcript_cut_errors(vector: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        transcript_cut(media.Transcript(), vector)


@pytest.mark.parametrize("vector", VECTORS["opusPacketDurations"], ids=lambda v: v["name"])
def test_opus_packet_durations(vector: dict[str, Any]) -> None:
    expected = vector["durationUs"]
    assert media.opus_packet_duration_us(bytes.fromhex(vector["packet"])) == (
        None if expected is None else int(expected)
    )


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
        "tracks": {
            "catalog": media.CATALOG_TRACK,
            "video": media.VIDEO_TRACK,
            "audio": media.AUDIO_TRACK,
            "mediaTimeline": media.MEDIA_TIMELINE_TRACK,
            "interrupt": media.INTERRUPT_TRACK,
            "transcript": media.TRANSCRIPT_TRACK,
        },
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
            "import sys, moq_media; assert not {'caprid_relay', 'aioquic', 'av', 'numpy'} & sys.modules.keys()",
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
