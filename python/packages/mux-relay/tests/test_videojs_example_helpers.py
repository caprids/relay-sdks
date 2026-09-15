"""The Video.js streaming example's helpers: the media formats, and how it reads a publish failure."""

import asyncio
import base64
import json
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any, cast

import pytest
from videojs_stream.moq_media import (
    AudioTrackInfo,
    VideoTrackInfo,
    annexb_to_avcc,
    avc_codec_string,
    build_avcc,
    build_msf_catalog,
    compose_player_url,
    encode_name_component,
    encode_namespace_name,
    extract_parameter_sets,
    iter_annexb_nal_units,
    loc_properties,
)
from videojs_stream.stream import (
    EncodedSample,
    Stats,
    failure_ends_track,
    make_track_end_handler,
    publish_media,
    serve_catalog,
    summarize,
    track_has_ended,
    write_ended_track,
)

from mux_relay import RelayClosedError, RelayError, RelayTimeoutError


class TestAnnexB:
    def test_splits_three_and_four_byte_start_codes(self) -> None:
        stream = b"\x00\x00\x00\x01\x67\xaa" + b"\x00\x00\x01\x68\xbb\xcc" + b"\x00\x00\x00\x01\x65\x01\x02"

        assert list(iter_annexb_nal_units(stream)) == [b"\x67\xaa", b"\x68\xbb\xcc", b"\x65\x01\x02"]

    def test_converts_an_access_unit_to_length_prefixed_nals_without_parameter_sets(self) -> None:
        stream = b"\x00\x00\x00\x01\x67\xaa" + b"\x00\x00\x00\x01\x68\xbb" + b"\x00\x00\x00\x01\x09\xf0"
        stream += b"\x00\x00\x00\x01\x06\x05\xff" + b"\x00\x00\x01\x65\x88\x84"

        assert annexb_to_avcc(stream) == b"\x00\x00\x00\x03\x06\x05\xff" + b"\x00\x00\x00\x03\x65\x88\x84"
        assert annexb_to_avcc(stream, drop_parameter_sets=False).startswith(b"\x00\x00\x00\x02\x67\xaa")

    def test_builds_an_avcc_and_derives_the_codec_string(self) -> None:
        sps = bytes.fromhex("6742c014da05067e7840000003004000000f23c5")
        pps = bytes.fromhex("68ce3c80")
        found_sps, found_pps = extract_parameter_sets(b"\x00\x00\x00\x01" + sps + b"\x00\x00\x00\x01" + pps)

        assert found_sps == [sps]
        assert found_pps == [pps]

        avcc = build_avcc(found_sps, found_pps)

        assert avcc[0] == 1
        assert avcc[1:4] == sps[1:4]
        assert avcc[4] == 0xFF  # 4-byte NAL lengths
        assert avcc[5] == 0xE1  # one SPS
        assert avcc[6:8] == len(sps).to_bytes(2, "big")
        assert avcc[8 : 8 + len(sps)] == sps
        assert avcc[8 + len(sps)] == 1  # one PPS
        assert avcc.endswith(len(pps).to_bytes(2, "big") + pps)
        # Constrained Baseline, level 2.0: profile 0x42, constraints 0xC0, level 0x14.
        assert avc_codec_string(avcc) == "avc1.42C014"

        with pytest.raises(ValueError):
            build_avcc([], [pps])


class TestCatalog:
    def test_matches_the_video_js_builder_shape(self) -> None:
        avcc = bytes.fromhex("0142c014ffe1000a6742c014da05067e784001000468ce3c80")
        document = build_msf_catalog(
            ["mattypoo", "tv"],
            video=VideoTrackInfo(
                codec="avc1.42C014", width=640, height=360, framerate=30, bitrate=1_500_000, init_data=avcc
            ),
            audio=AudioTrackInfo(bitrate=96_000),
            generated_at_ms=1_700_000_000_000,
        )
        catalog = json.loads(document)

        assert catalog["version"] == "draft-01"
        assert catalog["isComplete"] is True
        assert catalog["generatedAt"] == 1_700_000_000_000
        video, audio = catalog["tracks"]
        assert video == {
            "namespace": "mattypoo/tv",
            "packaging": "loc",
            "isLive": True,
            "initRef": "video-init",
            "name": "video",
            "role": "video",
            "codec": "avc1.42C014",
            "width": 640,
            "height": 360,
            "framerate": 30,
            "bitrate": 1_500_000,
        }
        assert audio == {
            "namespace": "mattypoo/tv",
            "packaging": "loc",
            "isLive": True,
            "name": "audio",
            "role": "audio",
            "codec": "opus",
            "samplerate": 48_000,
            "channelConfig": "2",
            "bitrate": 96_000,
        }
        assert catalog["initDataList"] == [
            {"id": "video-init", "type": "inline", "data": base64.b64encode(avcc).decode()}
        ]

    def test_omits_absent_fields_and_the_init_data_list(self) -> None:
        catalog = json.loads(build_msf_catalog(["ns"], video=VideoTrackInfo(codec="vp8", width=320, height=180)))

        assert "generatedAt" not in catalog
        assert "initDataList" not in catalog
        assert "initRef" not in catalog["tracks"][0]
        assert "framerate" not in catalog["tracks"][0]


class TestLocProperties:
    def test_carries_timestamp_timescale_and_config_on_keyframes(self) -> None:
        keyframe = loc_properties(1_000_000, video_config=b"\x01\x42")
        delta = loc_properties(1_033_333)

        assert [(p.type, p.value) for p in keyframe] == [(0x10, 1_000_000), (0x08, 1_000_000), (0x0D, b"\x01\x42")]
        assert [(p.type, p.value) for p in delta] == [(0x10, 1_033_333), (0x08, 1_000_000)]


class TestPlayerUrl:
    def test_encodes_names_like_the_video_js_parser_expects(self) -> None:
        assert encode_name_component("relay_sdk") == "relay_sdk"
        assert encode_name_component("relay-sdk") == "relay.2dsdk"
        assert encode_name_component("a/b") == "a.2fb"
        assert encode_namespace_name(["mattypoo", "tv"], "catalog") == "mattypoo-tv--catalog"

    def test_composes_the_moqt_source_url_with_the_token_in_the_fragment(self) -> None:
        url = compose_player_url("https://sjc.relay.mux.global", ["mattypoo", "relay-sdk-example"], token="a b/c")

        assert url == "moqt://sjc.relay.mux.global/#msf:mattypoo-relay.2dsdk.2dexample--catalog&c4m=a%20b%2Fc"
        assert (
            compose_player_url("moqt://relay.example.com/moq?x=1", ["ns"])
            == "moqt://relay.example.com/moq?x=1#msf:ns--catalog"
        )

        with pytest.raises(ValueError):
            compose_player_url("http://relay.example.com", ["ns"])

        with pytest.raises(ValueError):
            compose_player_url("https://relay.example.com", [])


class TestFailureEndsTrack:
    """The example's read of the SDK contract: which publish failures retire the track."""

    def test_a_timeout_waiting_for_demand_is_retryable(self) -> None:
        timeout = RelayTimeoutError("Publish write timed out waiting for relay demand or transport")

        assert failure_ends_track(timeout, operation="write", bound=False) is False

    def test_a_timeout_on_a_bound_write_is_terminal(self) -> None:
        # One deadline covers the wait for demand and the transport write (#19), and the SDK retires
        # the track when it fires on a bound one. Until #31 exposes `closed`, the only way to see
        # that is the writer state `track_has_ended` reads, not the exception class.
        timeout = RelayTimeoutError("Publish write timed out waiting for relay demand or transport")

        assert failure_ends_track(timeout, operation="write", bound=True) is True

    def test_a_closed_track_is_always_terminal(self) -> None:
        closed = RelayClosedError("Published track is closed")

        assert failure_ends_track(closed, operation="write", bound=False) is True
        assert failure_ends_track(closed, operation="group", bound=True) is True

    def test_a_bare_relay_error_on_a_bound_write_is_terminal(self) -> None:
        # What a relay re-bind during a bound write raises today (#31); the dedicated subclass of
        # RelayClosedError that lands with it is covered by the isinstance check above.
        rebind = RelayError("Relay subscription changed during a write; the object may be incomplete")

        assert failure_ends_track(rebind, operation="write", bound=True) is True

    def test_a_bare_relay_error_on_an_unbound_write_is_retryable(self) -> None:
        vanished = RelayError("Relay subscription ended before the write started")

        assert failure_ends_track(vanished, operation="write", bound=False) is False

    def test_a_group_that_fails_part_way_keeps_its_track(self) -> None:
        # The SDK resets the group's stream and leaves the track open for the next keyframe.
        interrupted = RelayError("Relay subscription changed during the group; it may be incomplete")

        assert failure_ends_track(interrupted, operation="group", bound=True) is False


class FakeGroup:
    """Enough of RelayGroupWriter for the example's publish loop, with a scripted failing object."""

    def __init__(self, group_id: int, *, fail_on: int | None = None, error: RelayError | None = None) -> None:
        self.group_id = group_id
        self.objects = 0
        self._fail_on = fail_on
        self._error = error

    async def write(self, payload: bytes, *, properties: object = None) -> None:
        if self._fail_on is not None and self.objects == self._fail_on:
            assert self._error is not None
            self._fail_on = None
            raise self._error

        self.objects += 1

    async def close(self) -> None:
        return None


class FakeTrack:
    """A track writer whose nth operation fails the way a relay re-bind does, taking `bound` with it.

    Moving a relay subscription during a bound write is not reproducible on demand (it happened in
    one of three long campaign runs), so the terminal path is injected here rather than waited for.
    `bindings()` mirrors RelayTrackWriter: the current demand state first, and nothing at all once
    the track is retired.
    """

    def __init__(
        self,
        *,
        bound: bool = True,
        fail_on: int | None = None,
        error: RelayError | None = None,
        retires: bool = True,
        group_fail_on: int | None = None,
        group_error: RelayError | None = None,
    ) -> None:
        self.bound = bound
        self.ended = False
        self.writes = 0
        self.groups = 0
        self._fail_on = fail_on
        self._error = error
        self._retires = retires
        self._group_fail_on = group_fail_on
        self._group_error = group_error

    def _maybe_fail(self) -> None:
        if self._fail_on is not None and self.writes + self.groups == self._fail_on:
            assert self._error is not None
            self._fail_on = None
            self.bound = False
            self.ended = self._retires
            raise self._error

    async def write(self, payload: bytes, *, properties: object = None) -> None:
        self._maybe_fail()
        self.writes += 1

    async def open_group(self) -> FakeGroup:
        self._maybe_fail()
        group = FakeGroup(self.groups, fail_on=self._group_fail_on, error=self._group_error)
        self.groups += 1
        return group

    async def _bindings(self) -> AsyncIterator[bool]:
        if self.ended:
            return

        yield self.bound

    def bindings(self) -> AsyncIterator[bool]:
        return self._bindings()


class TestTrackHasEnded:
    async def test_reads_the_end_of_track_out_of_the_bindings_iterator(self) -> None:
        assert await track_has_ended(cast(Any, FakeTrack(bound=True))) is False
        assert await track_has_ended(cast(Any, FakeTrack(bound=False))) is False

        retired = FakeTrack()
        retired.ended = True

        assert await track_has_ended(cast(Any, retired)) is True

    async def test_prefers_the_closed_property_that_31_adds(self) -> None:
        # #31 gives RelayTrackWriter a `closed` property; the example uses it as soon as it exists
        # and falls back to `bindings()` against today's SDK.
        alive = SimpleNamespace(closed=False, bindings=lambda: pytest.fail("bindings() was consulted"))
        gone = SimpleNamespace(closed=True, bindings=lambda: pytest.fail("bindings() was consulted"))

        assert await track_has_ended(cast(Any, alive)) is False
        assert await track_has_ended(cast(Any, gone)) is True


class TestWriteEndedTrack:
    async def test_confirms_a_terminal_classification_against_the_writer(self) -> None:
        rebind = RelayError("Relay subscription changed during a write; the object may be incomplete")
        retired = FakeTrack()
        retired.ended = True

        assert await write_ended_track(cast(Any, retired), rebind, bound=True) is True
        assert await write_ended_track(cast(Any, FakeTrack()), rebind, bound=True) is False
        assert await write_ended_track(cast(Any, retired), rebind, bound=False) is False

    async def test_a_closed_track_needs_no_confirmation(self) -> None:
        # A connection that went away leaves the writer un-retired while every write still fails.
        closed = RelayClosedError("Published track is closed")

        assert await write_ended_track(cast(Any, FakeTrack()), closed, bound=True) is True


def audio_samples(count: int) -> list[EncodedSample]:
    return [EncodedSample(kind="audio", timestamp_us=i * 20_000, payload=b"\xfc\xff", key=True) for i in range(count)]


def video_samples(count: int) -> list[EncodedSample]:
    """One keyframe then delta frames, the shape a GOP arrives in."""
    return [EncodedSample(kind="video", timestamp_us=i * 33_333, payload=b"\x00", key=i == 0) for i in range(count)]


async def drive(
    samples: list[EncodedSample], *, video: FakeTrack, audio: FakeTrack | None
) -> tuple[Stats, list[tuple[str, RelayError]]]:
    """Run the example's publish loop over a scripted sample list and report what it counted."""
    queue: asyncio.Queue[EncodedSample | None] = asyncio.Queue()

    for sample in samples:
        queue.put_nowait(sample)

    queue.put_nowait(None)
    stats = Stats()
    ended: list[tuple[str, RelayError]] = []
    encoder = cast(Any, SimpleNamespace(avcc=b"\x01\x42"))
    await publish_media(
        queue,
        cast(Any, video),
        cast(Any, audio),
        encoder,
        stats,
        cast(Any, lambda event, fields: None),
        lambda track, error: ended.append((track, error)),
    )
    return stats, ended


class TestPublishMediaTrackEnd:
    async def test_a_bound_audio_write_failure_ends_the_track_once_and_counts_the_rest(self) -> None:
        # #32: a 30-minute run reported skippedAudio=0 while ~74,700 frames were never sent.
        rebind = RelayError("Relay subscription changed during a write; the object may be incomplete")
        audio = FakeTrack(fail_on=3, error=rebind)
        stats, ended = await drive(audio_samples(10), video=FakeTrack(bound=False), audio=audio)

        assert stats.audio_objects == 3
        assert stats.dropped_audio == 7
        assert stats.skipped_audio == 0
        assert [(track, str(error)) for track, error in ended] == [("audio", str(rebind))]
        assert audio.writes == 3
        assert summarize(stats)["droppedAudio"] == 7

    async def test_frames_with_no_subscription_stay_skipped_not_dropped(self) -> None:
        stats, ended = await drive(audio_samples(4), video=FakeTrack(bound=False), audio=FakeTrack(bound=False))

        assert (stats.audio_objects, stats.skipped_audio, stats.dropped_audio) == (0, 4, 0)
        assert ended == []
        assert summarize(stats)["endedTracks"] is None

    async def test_a_vanished_subscription_only_skips_the_frame(self) -> None:
        # `bound` was True when the write started but demand disappeared before it bound, so the
        # writer is still open; the example must confirm that instead of ending the run.
        vanished = RelayError("Relay subscription ended before the write started")
        audio = FakeTrack(fail_on=2, error=vanished, retires=False)
        stats, ended = await drive(audio_samples(3), video=FakeTrack(bound=False), audio=audio)

        # The third frame then finds `bound` False, the way it does while nobody is subscribed.
        assert (stats.audio_objects, stats.skipped_audio, stats.dropped_audio) == (2, 1, 0)
        assert ended == []

    async def test_a_closed_video_track_is_reported_and_its_frames_counted_as_dropped(self) -> None:
        closed = RelayClosedError("Published track is closed")
        video = FakeTrack(fail_on=0, error=closed)
        keyframes = [EncodedSample(kind="video", timestamp_us=i * 33_333, payload=b"\x00", key=True) for i in range(3)]
        stats, ended = await drive(keyframes, video=video, audio=None)

        assert (stats.video_groups, stats.video_objects, stats.dropped_video, stats.skipped_video) == (0, 0, 3, 0)
        assert [track for track, _ in ended] == ["video"]

    async def test_an_interrupted_group_counts_its_own_sample_as_skipped(self) -> None:
        # The group is reset and the track survives, so the sample that failed is a skip like the
        # ones that follow it before the next keyframe -- and it is in a counter either way.
        interrupted = RelayError("Relay subscription changed during the group; it may be incomplete")
        video = FakeTrack(group_fail_on=0, group_error=interrupted)
        stats, ended = await drive(video_samples(3), video=video, audio=None)

        assert (stats.video_groups, stats.video_objects, stats.skipped_video, stats.dropped_video) == (1, 0, 3, 0)
        assert ended == []

    async def test_a_terminal_group_write_failure_counts_its_own_sample_as_dropped(self) -> None:
        closed = RelayClosedError("Published track is closed")
        video = FakeTrack(group_fail_on=0, group_error=closed)
        stats, ended = await drive(video_samples(3), video=video, audio=None)

        assert (stats.video_groups, stats.video_objects, stats.skipped_video, stats.dropped_video) == (1, 0, 0, 3)
        assert [track for track, _ in ended] == ["video"]

    async def test_every_sample_lands_in_exactly_one_counter(self) -> None:
        # What `summarize` promises: sent + skipped + dropped accounts for the whole timeline.
        interrupted = RelayError("Relay subscription changed during the group; it may be incomplete")
        samples = video_samples(6) + audio_samples(4)
        stats, _ = await drive(
            samples, video=FakeTrack(group_fail_on=2, group_error=interrupted), audio=FakeTrack(bound=False)
        )

        video_total = stats.video_objects + stats.skipped_video + stats.dropped_video
        audio_total = stats.audio_objects + stats.skipped_audio + stats.dropped_audio

        assert (video_total, audio_total) == (6, 4)


class TestTrackEndHandler:
    def test_logs_once_records_the_track_and_stops_the_run(self) -> None:
        stats = Stats()
        logged: list[str] = []
        stopped: list[BaseException] = []
        handler = make_track_end_handler(
            "stop", stats, cast(Any, lambda event, fields: logged.append(event)), stopped.append
        )
        error = RelayError("Relay subscription changed during a write; the object may be incomplete")

        handler("audio", error)
        handler("audio", error)

        assert logged == ["audio track ended"]
        assert stats.ended_tracks == {"audio": f"RelayError: {error}"}
        assert [type(reason).__name__ for reason in stopped] == ["TrackEnded"]
        assert summarize(stats)["endedTracks"] == "audio"

    def test_continue_keeps_the_run_going(self) -> None:
        stats = Stats()
        stopped: list[BaseException] = []
        handler = make_track_end_handler("continue", stats, cast(Any, lambda event, fields: None), stopped.append)

        handler("audio", RelayError("gone"))

        assert stopped == []
        assert stats.ended_tracks == {"audio": "RelayError: gone"}


class TestServeCatalog:
    async def test_a_retired_catalog_track_stops_the_resend_loop(self) -> None:
        class FakeCatalog:
            """A catalog writer the relay subscribes to twice, whose write fails the terminal way."""

            def __init__(self) -> None:
                self.writes = 0

            async def _bindings(self) -> AsyncIterator[bool]:
                yield True
                yield True

            def bindings(self) -> AsyncIterator[bool]:
                return self._bindings()

            async def write(self, payload: bytes, *, properties: object = None) -> None:
                self.writes += 1
                raise RelayClosedError("Published track is closed")

        catalog = FakeCatalog()
        stats = Stats()
        ended: list[str] = []
        await serve_catalog(
            cast(Any, catalog),
            b"{}",
            stats,
            cast(Any, lambda event, fields: None),
            lambda track, error: ended.append(track),
        )

        assert (catalog.writes, stats.catalog_sends, stats.catalog_failures) == (1, 0, 0)
        assert ended == ["catalog"]
