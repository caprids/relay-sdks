"""A publish session announcing several namespaces, ported from ``publish-namespaces.test.ts``."""

from __future__ import annotations

from core_helpers.raw_peer import raw_subscribe, solicit_namespace
from core_helpers.transport_pair import TransportPair, create_transport_pair, with_server_request_ids
from core_helpers.waiting import settle, wait_for

from moq_core.control_messages import Namespace, NamespaceDone
from moq_core.publish_session import (
    AnnounceEnded,
    MoqtPublishSession,
    MoqtPublishSessionCallbacks,
    MoqtPublishSessionConfig,
    RegisterTrackOptions,
    TrackBinding,
    create_moqt_publish_session,
)
from moq_core.session import MoqtSession, create_moqt_session

PERSON = ["room", "p", "alice"]
HOST = ["room", "host"]


class Harness:
    def __init__(self, callbacks: MoqtPublishSessionCallbacks | None = None) -> None:
        self.pair: TransportPair = create_transport_pair()
        self.subscriber: MoqtSession = create_moqt_session(with_server_request_ids(self.pair.server))
        self.session: MoqtPublishSession = create_moqt_publish_session(
            self.pair.client, MoqtPublishSessionConfig(callbacks=callbacks or MoqtPublishSessionCallbacks())
        )

    def register(self, namespace: list[str], track_name: str) -> None:
        self.session.register_track(RegisterTrackOptions(track_namespace=namespace, track_name=track_name))

    def announce_both(self) -> None:
        self.session.announce(PERSON)
        self.session.announce(HOST)
        self.register(PERSON, "video")
        self.register(HOST, "control")

    def done(self) -> None:
        self.session.destroy()
        self.subscriber.destroy()


async def test_announces_each_namespace_on_one_solicitation_and_reports_each_once() -> None:
    announced: list[list[str]] = []
    harness = Harness(MoqtPublishSessionCallbacks(on_announced=announced.append))

    await harness.session.ready
    harness.session.announce(PERSON)
    harness.session.announce(HOST)
    harness.session.announce(PERSON)
    harness.register(PERSON, "video")
    harness.register(HOST, "control")

    solicitation = await solicit_namespace(harness.pair.server, ["room"])

    await wait_for(lambda: len(solicitation.received) == 3)
    assert sorted(solicitation.received[1:], key=repr) == sorted(
        [Namespace(track_namespace_suffix=["p", "alice"]), Namespace(track_namespace_suffix=["host"])], key=repr
    )
    await wait_for(lambda: len(announced) == 2)
    assert sorted(announced) == sorted([PERSON, HOST])
    harness.done()


async def test_defers_each_namespaces_announce_until_a_track_in_it_is_registered() -> None:
    harness = Harness()

    await harness.session.ready
    harness.session.announce(PERSON)
    harness.session.announce(HOST)
    harness.register(PERSON, "video")

    solicitation = await solicit_namespace(harness.pair.server, [])

    await wait_for(lambda: solicitation.kinds() == ["request-ok", "namespace"])
    assert solicitation.received[1] == Namespace(track_namespace_suffix=PERSON)
    await settle()
    assert len(solicitation.received) == 2

    harness.register(HOST, "control")
    await wait_for(lambda: len(solicitation.received) == 3)
    assert solicitation.received[2] == Namespace(track_namespace_suffix=HOST)
    harness.done()


async def test_withdraws_one_namespace_with_namespace_done_and_keeps_the_other() -> None:
    announced: list[list[str]] = []
    endings: list[AnnounceEnded] = []
    harness = Harness(MoqtPublishSessionCallbacks(on_announced=announced.append, on_announce_ended=endings.append))

    await harness.session.ready
    harness.announce_both()
    solicitation = await solicit_namespace(harness.pair.server, [])
    await wait_for(lambda: len(solicitation.received) == 3)

    harness.session.withdraw(HOST)
    harness.session.withdraw(HOST)
    harness.session.withdraw(["room", "never"])
    await wait_for(lambda: len(solicitation.received) == 4)
    assert solicitation.received[3] == NamespaceDone(track_namespace_suffix=HOST)
    assert harness.session.is_solicited(PERSON) is True

    # Announcing it again writes a fresh NAMESPACE and reports it again.
    harness.session.announce(HOST)
    await wait_for(lambda: len(solicitation.received) == 5)
    assert solicitation.received[4] == Namespace(track_namespace_suffix=HOST)
    await wait_for(lambda: announced.count(HOST) == 2)
    assert endings == []
    harness.done()


async def test_reports_the_loss_of_each_namespace_a_dying_solicitation_carried_and_only_those() -> None:
    endings: list[list[str]] = []
    harness = Harness(MoqtPublishSessionCallbacks(on_announce_ended=lambda ended: endings.append(ended.namespace)))

    await harness.session.ready
    harness.announce_both()
    people = await solicit_namespace(harness.pair.server, ["room", "p"], 1)
    hosts = await solicit_namespace(harness.pair.server, ["room", "host"], 3)
    await wait_for(lambda: len(people.received) == 2 and len(hosts.received) == 2)

    hosts.reset()
    await wait_for(lambda: endings == [HOST])
    assert harness.session.is_solicited(HOST) is False
    assert harness.session.is_solicited(PERSON) is True
    assert harness.session.namespace_solicited is True
    harness.done()


async def test_retracts_every_namespace_at_close() -> None:
    harness = Harness()

    await harness.session.ready
    harness.announce_both()
    solicitation = await solicit_namespace(harness.pair.server, ["room"])
    await wait_for(lambda: len(solicitation.received) == 3)

    harness.session.close()
    await wait_for(
        lambda: (
            sorted(
                message.track_namespace_suffix
                for message in solicitation.received
                if isinstance(message, NamespaceDone)
            )
            == [["host"], ["p", "alice"]]
        )
    )
    harness.subscriber.destroy()


async def test_keeps_namespaces_apart_whose_fields_joined_with_a_slash_would_match() -> None:
    bindings: list[TrackBinding] = []
    harness = Harness(MoqtPublishSessionCallbacks(on_track_binding=bindings.append))

    await harness.session.ready
    harness.session.announce(["a/b"])
    harness.session.announce(["a", "b"])
    harness.register(["a/b"], "video")
    harness.register(["a", "b"], "video")
    await solicit_namespace(harness.pair.server, [], 101)

    first = await raw_subscribe(harness.pair.server, ["a/b"], "video", 103)
    await wait_for(lambda: first.kinds() == ["subscribe-ok"] and len(bindings) == 1)
    assert [(binding.track_namespace, binding.track_alias) for binding in bindings] == [(["a/b"], 103)]
    harness.done()


async def test_drops_a_carrier_whose_withdrawal_write_fails_and_reports_what_it_still_carried() -> None:
    endings: list[list[str]] = []
    harness = Harness(MoqtPublishSessionCallbacks(on_announce_ended=lambda ended: endings.append(ended.namespace)))

    await harness.session.ready
    harness.announce_both()
    solicitation = await solicit_namespace(harness.pair.server, ["room"], 1)
    await wait_for(lambda: len(solicitation.received) == 3)

    solicitation.abandon_reads()
    harness.session.withdraw(HOST)
    await wait_for(lambda: endings == [PERSON])
    assert harness.session.is_solicited(PERSON) is False

    # A replacement solicitation is a fresh start, not a PREFIX_OVERLAP against the dead carrier.
    replacement = await solicit_namespace(harness.pair.server, ["room"], 3)
    await wait_for(lambda: replacement.kinds() == ["request-ok", "namespace"])
    harness.done()


async def test_does_not_report_a_loss_for_a_namespace_an_earlier_loss_handler_withdrew() -> None:
    endings: list[list[str]] = []
    harness: Harness

    def on_announce_ended(ended: AnnounceEnded) -> None:
        endings.append(ended.namespace)
        harness.session.withdraw(HOST if ended.namespace == PERSON else PERSON)

    harness = Harness(MoqtPublishSessionCallbacks(on_announce_ended=on_announce_ended))

    await harness.session.ready
    harness.announce_both()
    solicitation = await solicit_namespace(harness.pair.server, ["room"], 1)
    await wait_for(lambda: len(solicitation.received) == 3)

    solicitation.reset()
    await wait_for(lambda: len(endings) == 1)
    await settle()
    assert len(endings) == 1
    harness.done()
