"""``caprid_relay.media.ReplaceableTrack`` through its own interface, over a fake publisher whose tracks are the media
module's fake track writer, as the TypeScript SDK's ``replaceable-track.test.ts`` does.
"""

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import pytest
from core_helpers.waiting import settle, wait_for
from relay_helpers.fake_track_writer import FakeTrackWriter

from caprid_relay import RelayError
from caprid_relay.media import FrameOutcome, ReplaceableTrack, TrackReplacement
from moq_core import KeyValuePair


@dataclass(frozen=True, slots=True)
class Audio:
    """An independently decodable frame, the kind a replaceable track carries."""

    payload: bytes
    key: bool = True
    properties: Sequence[KeyValuePair] = ()
    discontinuity: bool = False


def frame(label: str) -> Audio:
    return Audio(label.encode())


class Publisher:
    """Registers fake tracks by name and remembers how it was asked to."""

    def __init__(self, *, demand: bool = True) -> None:
        self.demand = demand
        self.tracks: dict[str, FakeTrackWriter] = {}
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def register_track(self, track: str, **options: Any) -> FakeTrackWriter:
        self.calls.append((track, options))
        # The first track has demand, as a listener subscribed to it before; replacements wait for their listener.
        registered = FakeTrackWriter(demand=self.demand if self.tracks else True)
        self.tracks[track] = registered
        return registered


class Catalog:
    """A document service that remembers every document, and a builder that can fail."""

    def __init__(self) -> None:
        self.documents: list[bytes] = []
        self.fail_for: str | None = None

    def update(self, document: bytes) -> None:
        self.documents.append(document)

    def build(self, name: str) -> bytes:
        if name == self.fail_for:
            raise RuntimeError("catalog build failed")

        return name.encode()


def create(publisher: Publisher, catalog: Catalog, **options: Any) -> ReplaceableTrack[Audio]:
    return ReplaceableTrack(publisher, track="audio", catalog_service=catalog, build_catalog=catalog.build, **options)


def payloads(track: FakeTrackWriter) -> list[bytes]:
    return [payload for group in track.groups for payload in group.payloads]


class TestCreation:
    async def test_the_first_track_is_registered_with_its_options_and_listed_in_the_catalog(self) -> None:
        publisher, catalog = Publisher(), Catalog()
        properties = [KeyValuePair(0x06, 500)]

        reply = create(publisher, catalog, track_properties=properties, write_timeout=3.0)

        assert publisher.calls == [("audio", {"track_properties": properties, "write_timeout": 3.0})]
        assert catalog.documents == [b"audio"]
        assert (reply.track, reply.replacements, reply.closed) == ("audio", 0, False)

    async def test_unset_options_are_not_passed_on(self) -> None:
        publisher = Publisher()

        create(publisher, Catalog())

        assert publisher.calls == [("audio", {})]

    async def test_a_catalog_that_fails_at_creation_closes_the_track_and_raises(self) -> None:
        publisher, catalog = Publisher(), Catalog()
        catalog.fail_for = "audio"

        with pytest.raises(RuntimeError, match="catalog build failed"):
            create(publisher, catalog)

        assert publisher.tracks["audio"].closed

    async def test_write_sends_each_frame_as_its_own_group_on_the_current_track(self) -> None:
        publisher = Publisher()
        reply = create(publisher, Catalog())

        outcomes = [await reply.write(frame("a")), await reply.write(frame("b"))]

        assert [outcome.kind for outcome in outcomes] == ["sent", "sent"]
        assert [group.payloads for group in publisher.tracks["audio"].groups] == [[b"a"], [b"b"]]

    async def test_writer_options_reach_the_frame_writer(self) -> None:
        publisher = Publisher()
        reply = create(publisher, Catalog(), writer_options={"grouping": "keyframe"})

        await reply.write(Audio(b"k"))
        await reply.write(Audio(b"d", key=False))

        assert [group.payloads for group in publisher.tracks["audio"].groups] == [[b"k", b"d"]]

    def test_options_are_checked_as_arguments(self) -> None:
        publisher, catalog = Publisher(), Catalog()

        for options, error in [
            ({"track": ""}, TypeError),
            ({"track": 3}, TypeError),
            ({"name": "audio-2"}, TypeError),
            ({"catalog_service": object()}, TypeError),
            ({"build_catalog": None}, TypeError),
            ({"demand_wait": "soon"}, TypeError),
            ({"demand_wait": True}, TypeError),
            ({"demand_wait": -1}, ValueError),
            ({"demand_wait": float("nan")}, ValueError),
        ]:
            arguments: dict[str, Any] = {
                "track": "audio",
                "catalog_service": catalog,
                "build_catalog": catalog.build,
                **options,
            }

            with pytest.raises(error):
                ReplaceableTrack(publisher, **arguments)

        assert publisher.calls == []


class TestReplace:
    async def test_replace_registers_the_next_track_lists_it_and_closes_the_old_one(self) -> None:
        publisher, catalog = Publisher(), Catalog()
        reply = create(publisher, catalog, track_properties=[KeyValuePair(0x06, 500)], write_timeout=3.0)

        replacement = await reply.replace()

        assert replacement == TrackReplacement("audio", "audio-2", 1)
        assert publisher.calls[1] == ("audio-2", publisher.calls[0][1])
        assert catalog.documents == [b"audio", b"audio-2"]
        assert publisher.tracks["audio"].closed
        assert not publisher.tracks["audio-2"].closed
        assert (reply.track, reply.replacements) == ("audio-2", 1)

    async def test_later_writes_go_to_the_new_track_and_names_count_on(self) -> None:
        publisher, catalog = Publisher(), Catalog()
        reply = create(publisher, catalog)
        await reply.write(frame("old"))

        await reply.replace()
        publisher.tracks["audio-2"].bind(True)
        outcome = await reply.write(frame("new"))
        third = await reply.replace()

        assert outcome.kind == "sent"
        assert payloads(publisher.tracks["audio"]) == [b"old"]
        assert payloads(publisher.tracks["audio-2"]) == [b"new"]
        assert third == TrackReplacement("audio-2", "audio-3", 2)
        assert catalog.documents[-1] == b"audio-3"

    async def test_a_custom_name_function_names_the_replacements(self) -> None:
        publisher, catalog = Publisher(), Catalog()
        reply = create(publisher, catalog, name=lambda replacement: f"reply/{replacement}")

        replacement = await reply.replace()

        assert replacement == TrackReplacement("audio", "reply/1", 1)
        assert "reply/1" in publisher.tracks
        assert catalog.documents[-1] == b"reply/1"

    async def test_a_name_that_is_not_a_string_is_refused_and_nothing_changes(self) -> None:
        publisher, catalog = Publisher(), Catalog()
        reply = create(publisher, catalog, name=lambda replacement: "")

        with pytest.raises(TypeError):
            await reply.replace()

        assert list(publisher.tracks) == ["audio"]
        assert reply.track == "audio"

    async def test_a_catalog_that_fails_rejects_closes_the_new_track_and_keeps_the_current_one(self) -> None:
        publisher, catalog = Publisher(), Catalog()
        reply = create(publisher, catalog)
        catalog.fail_for = "audio-2"

        with pytest.raises(RuntimeError, match="catalog build failed"):
            await reply.replace()

        assert publisher.tracks["audio-2"].closed
        assert not publisher.tracks["audio"].closed
        assert (reply.track, reply.replacements) == ("audio", 0)
        assert catalog.documents == [b"audio"]
        assert (await reply.write(frame("still"))).kind == "sent"
        assert payloads(publisher.tracks["audio"]) == [b"still"]

    async def test_a_failed_attempt_uses_its_name_up_and_the_next_asks_for_the_next(self) -> None:
        publisher, catalog = Publisher(), Catalog()
        reply = create(publisher, catalog)
        register = publisher.register_track

        def unique(track: str, **options: Any) -> FakeTrackWriter:
            # Names are unique per connection, including after a track closes, as a real publisher holds them.
            if track in publisher.tracks:
                raise ValueError(f"{track} is already registered on this publisher connection")

            return register(track, **options)

        publisher.register_track = unique  # ty: ignore[invalid-assignment]
        catalog.fail_for = "audio-2"

        with pytest.raises(RuntimeError, match="catalog build failed"):
            await reply.replace()

        assert await reply.replace() == TrackReplacement("audio", "audio-3", 1)
        assert reply.track == "audio-3"

    async def test_a_registration_that_fails_rejects_and_keeps_the_current_track(self) -> None:
        publisher = Publisher()
        reply = create(publisher, Catalog())
        publisher.tracks["audio-2"] = FakeTrackWriter()

        def refuse(track: str, **options: Any) -> FakeTrackWriter:
            raise ValueError("Track name is already registered on this publisher connection")

        publisher.register_track = refuse  # ty: ignore[invalid-assignment]

        with pytest.raises(ValueError, match="already registered"):
            await reply.replace()

        assert not publisher.tracks["audio"].closed
        assert reply.replacements == 0


class TestDemandWait:
    async def test_frames_written_before_the_relay_subscribes_wait_and_go_out_in_order(self) -> None:
        publisher = Publisher(demand=False)
        reply = create(publisher, Catalog())
        await reply.replace()
        replacement = publisher.tracks["audio-2"]

        writes = [asyncio.ensure_future(reply.write(frame(label))) for label in ("one", "two", "three")]
        await settle()

        assert not any(write.done() for write in writes)
        assert replacement.groups == []

        replacement.bind(True)
        outcomes = await asyncio.gather(*writes)

        assert [outcome.kind for outcome in outcomes] == ["sent"] * 3
        assert [group.payloads for group in replacement.groups] == [[b"one"], [b"two"], [b"three"]]

    async def test_frames_written_while_the_waiting_frames_go_out_follow_them(self) -> None:
        publisher = Publisher(demand=False)
        reply = create(publisher, Catalog())
        await reply.replace()
        replacement = publisher.tracks["audio-2"]
        first = asyncio.ensure_future(reply.write(frame("one")))
        await settle()

        replacement.bind(True)
        second = asyncio.ensure_future(reply.write(frame("two")))
        third = asyncio.ensure_future(reply.write(frame("three")))
        outcomes = await asyncio.gather(first, second, third)

        assert [outcome.kind for outcome in outcomes] == ["sent"] * 3
        assert payloads(replacement) == [b"one", b"two", b"three"]

    async def test_frames_still_waiting_when_the_wait_runs_out_are_skipped(self) -> None:
        publisher = Publisher(demand=False)
        reply = create(publisher, Catalog(), demand_wait=0.02)
        await reply.replace()

        outcomes = await asyncio.gather(reply.write(frame("one")), reply.write(frame("two")))

        assert [outcome.kind for outcome in outcomes] == ["skipped", "skipped"]
        assert publisher.tracks["audio-2"].groups == []
        # The wait is over: a frame now goes straight to the writer, which sends it once the relay subscribes.
        publisher.tracks["audio-2"].bind(True)
        assert (await reply.write(frame("three"))).kind == "sent"

    async def test_a_zero_wait_skips_at_once_without_demand(self) -> None:
        publisher = Publisher(demand=False)
        reply = create(publisher, Catalog(), demand_wait=0)
        await reply.replace()

        assert (await asyncio.wait_for(reply.write(frame("one")), 1)).kind == "skipped"

    async def test_a_replacement_the_relay_already_subscribed_to_does_not_wait(self) -> None:
        publisher = Publisher(demand=True)
        reply = create(publisher, Catalog())
        await reply.replace()

        assert (await asyncio.wait_for(reply.write(frame("one")), 1)).kind == "sent"

    async def test_the_first_track_never_waits(self) -> None:
        publisher = Publisher()
        reply = create(publisher, Catalog())
        publisher.tracks["audio"].bind(False)

        assert (await asyncio.wait_for(reply.write(frame("one")), 1)).kind == "skipped"

    async def test_a_track_that_ends_while_frames_wait_releases_them_to_the_writer(self) -> None:
        publisher = Publisher(demand=False)
        reply = create(publisher, Catalog())
        await reply.replace()
        write = asyncio.ensure_future(reply.write(frame("one")))
        await settle()

        publisher.tracks["audio-2"].end()
        outcome = await asyncio.wait_for(write, 1)

        assert outcome.kind in ("skipped", "ended")

    async def test_a_replace_while_frames_wait_drops_them(self) -> None:
        publisher = Publisher(demand=False)
        reply = create(publisher, Catalog())
        await reply.replace()
        waiting = [asyncio.ensure_future(reply.write(frame(label))) for label in ("one", "two")]
        await settle()

        await reply.replace()

        assert await asyncio.gather(*waiting) == [FrameOutcome("dropped")] * 2
        assert publisher.tracks["audio-2"].closed
        assert publisher.tracks["audio-2"].groups == []
        assert reply.track == "audio-3"

    async def test_a_cancelled_waiting_write_leaves_the_others_alone(self) -> None:
        publisher = Publisher(demand=False)
        reply = create(publisher, Catalog())
        await reply.replace()
        replacement = publisher.tracks["audio-2"]
        cancelled = asyncio.ensure_future(reply.write(frame("gone")))
        kept = asyncio.ensure_future(reply.write(frame("kept")))
        await settle()

        cancelled.cancel()
        replacement.bind(True)

        assert (await kept).kind == "sent"
        assert payloads(replacement) == [b"kept"]


class TestReplaceDropsUnsentFrames:
    async def test_frames_waiting_in_the_old_writer_are_dropped_and_the_old_track_closed(self) -> None:
        publisher = Publisher()
        block = asyncio.Event()
        reply = create(publisher, Catalog())
        old = publisher.tracks["audio"]
        old.block = block
        # Frame grouping writes through open_group with the payload; hold it with the group write gate.
        in_flight = asyncio.ensure_future(reply.write(frame("flying")))
        queued = asyncio.ensure_future(reply.write(frame("queued")))
        await wait_for(lambda: old.blocked.is_set())

        replacing = asyncio.ensure_future(reply.replace())
        await wait_for(lambda: reply.track == "audio-2")

        assert (await queued).kind == "dropped"
        block.set()
        await replacing
        assert (await in_flight).kind in ("sent", "interrupted")
        assert old.closed
        assert b"queued" not in payloads(old)


class TestClose:
    async def test_close_closes_the_current_track_and_write_and_replace_raise_afterwards(self) -> None:
        publisher = Publisher()
        reply = create(publisher, Catalog())

        await reply.close()
        await reply.close()

        assert reply.closed
        assert publisher.tracks["audio"].closed

        with pytest.raises(RelayError) as written:
            await reply.write(frame("late"))

        assert written.value.code == "CLOSED"

        with pytest.raises(RelayError) as replaced:
            await reply.replace()

        assert replaced.value.code == "CLOSED"
        assert list(publisher.tracks) == ["audio"]

    async def test_close_after_a_replacement_closes_the_new_track_and_drops_what_waits(self) -> None:
        publisher = Publisher(demand=False)
        reply = create(publisher, Catalog())
        await reply.replace()
        waiting = asyncio.ensure_future(reply.write(frame("one")))
        await settle()

        await reply.close()

        assert (await waiting).kind == "dropped"
        assert all(track.closed for track in publisher.tracks.values())

    async def test_close_retires_the_current_track_at_once_even_while_an_old_one_is_closing(self) -> None:
        publisher = Publisher(demand=False)
        block = asyncio.Event()
        reply = create(publisher, Catalog())
        old = publisher.tracks["audio"]
        old.block = block
        in_flight = asyncio.ensure_future(reply.write(frame("old")))
        await wait_for(lambda: old.blocked.is_set())
        replacing = asyncio.ensure_future(reply.replace())
        await wait_for(lambda: reply.track == "audio-2")
        waiting = asyncio.ensure_future(reply.write(frame("new")))
        await settle()

        closing = asyncio.ensure_future(reply.close())
        await settle()
        # The relay subscribes to the replacement while the old track is still closing.
        publisher.tracks["audio-2"].bind(True)
        await settle()

        assert (await waiting).kind == "dropped"
        block.set()
        await closing
        await replacing
        await in_flight
        assert payloads(publisher.tracks["audio-2"]) == []
        assert all(track.closed for track in publisher.tracks.values())
