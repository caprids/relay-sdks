"""The public vocabulary in ``spec/vocabulary.md``, pinned so a rename in one SDK cannot land alone.

The counterpart of ``ts/scripts/check-boundaries.mjs``, which pins ``@mux/relay``'s runtime export
list: ``__all__`` is asserted in full, every name in it must appear in the spec, and the core
re-exports must match what ``ts/src/index.ts`` re-exports from ``@mux/moq-core``.
"""

import dataclasses
import importlib.util
import re
import types
from pathlib import Path

import moq_core
import mux_relay
import mux_relay.media
import mux_relay.quic
from mux_relay import RelayPublisher, RelaySubscriber, RelaySubscriptionHandlers, RelayTrackWriter

REPO = Path(__file__).resolve().parents[4]

PUBLIC_NAMES = [
    "DEFAULT_ENDPOINT",
    "CancellationToken",
    "Cancelled",
    "Closed",
    "ConnectingTransport",
    "Goaway",
    "GoawayEvent",
    "KeyValuePair",
    "LocationFilter",
    "MessageParameters",
    "MoqTrackSource",
    "MoqtObject",
    "MoqtTransport",
    "NamespaceEvent",
    "NamespaceEvents",
    "ObjectEvent",
    "PublishDone",
    "PublisherDoneEvent",
    "RelayError",
    "RelayErrorCode",
    "RelayErrorPhase",
    "RelayGroupWriter",
    "RelayPublisher",
    "RelaySubscriber",
    "RelaySubscription",
    "RelaySubscriptionHandlers",
    "RelayTrackWriter",
    "RelayTransportFactory",
    "RequestError",
    "StateNotifyEvent",
    "SubgroupEndEvent",
    "SubgroupResetEvent",
    "SubscribeOk",
    "SubscribedEvent",
    "Subscription",
    "RelayAwaitable",
    "SubscriptionConnectionClosedError",
    "SubscriptionEnd",
    "SubscriptionEndKind",
    "SubscriptionEvent",
    "SubscriptionEvents",
    "SubscriptionHandlers",
    "SubscriptionOverflowError",
    "SubscriptionRequestError",
    "UpdateOkEvent",
    "connect_relay_publisher",
    "connect_relay_subscriber",
    "read_json",
    "read_text",
]
"""The whole public interface. A name added here needs a row in ``spec/vocabulary.md`` first."""

MEDIA_NAMES = [
    "CatalogService",
    "Frame",
    "FrameOutcome",
    "FrameOutcomeKind",
    "FrameWriter",
]
"""The public interface of ``mux_relay.media``, the media publishing policy; the root exports none of it."""

QUIC_NAMES = ["connect_quic"]
"""The public interface of ``mux_relay.quic``, the aioquic native QUIC transport: its factory, kept out of the root."""


def test_all_is_exactly_the_pinned_list() -> None:
    assert sorted(mux_relay.__all__) == sorted(PUBLIC_NAMES)
    assert len(set(mux_relay.__all__)) == len(mux_relay.__all__)


def test_the_media_module_is_exactly_the_pinned_list_and_stays_out_of_the_root() -> None:
    assert sorted(mux_relay.media.__all__) == sorted(MEDIA_NAMES)
    assert not set(MEDIA_NAMES) & set(mux_relay.__all__)

    for name in MEDIA_NAMES:
        assert hasattr(mux_relay.media, name)


def test_the_package_has_no_webtransport_dialing_end() -> None:
    """Native QUIC is the only dialing end (ADR 0004); the WebTransport session lives in the test helpers (#232)."""
    assert importlib.util.find_spec("mux_relay.webtransport") is None
    assert not hasattr(mux_relay, "connect_webtransport")


def test_the_quic_package_binds_its_factory_and_nothing_else() -> None:
    """The session and protocol classes are reached through the private submodule, and the root has none of it."""
    assert sorted(mux_relay.quic.__all__) == sorted(QUIC_NAMES)
    bound = {
        name
        for name, value in vars(mux_relay.quic).items()
        if not name.startswith("_") and not isinstance(value, types.ModuleType)
    }
    assert bound == set(QUIC_NAMES)
    assert not set(QUIC_NAMES) & set(mux_relay.__all__)


def test_all_matches_the_names_the_package_binds() -> None:
    """Nothing is importable from the root without being pinned, and nothing pinned is missing."""
    bound = {
        name
        for name, value in vars(mux_relay).items()
        if not name.startswith("_") and not isinstance(value, types.ModuleType)
    }
    assert bound == set(mux_relay.__all__)


def test_every_export_appears_in_the_spec() -> None:
    """``spec/vocabulary.md``: a name that exists in one SDK without a row is a bug."""
    spec = (REPO / "spec" / "vocabulary.md").read_text() + (REPO / "spec" / "behavior.md").read_text()
    names = [*mux_relay.__all__, *mux_relay.media.__all__, *mux_relay.quic.__all__]
    missing = [name for name in names if re.search(rf"\b{re.escape(name)}\b", spec) is None]
    assert missing == []


def test_core_reexports_are_exactly_what_the_typescript_sdk_reexports_plus_closed() -> None:
    """``mux_relay`` re-exports what ``@mux/relay`` re-exports from the core, plus ``Closed``."""
    index = (REPO / "ts" / "src" / "index.ts").read_text()
    block = re.search(r"export type \{([^}]*)\} from '@mux/moq-core';", index)
    assert block is not None, "ts/src/index.ts no longer has a single core re-export block"
    typescript = {name.strip() for name in block.group(1).split(",") if name.strip()}
    python = {name for name in mux_relay.__all__ if getattr(moq_core, name, None) is getattr(mux_relay, name)}
    assert python == typescript | {"Closed"}


def test_exports_one_entry_point_per_connection_role_and_nothing_under_the_old_names() -> None:
    assert callable(mux_relay.connect_relay_subscriber)
    assert callable(mux_relay.connect_relay_publisher)

    for old in ("connect_relay", "RelayConnection"):
        assert not hasattr(mux_relay, old)
        assert old not in mux_relay.__all__


def test_spells_subscription_events_after_what_happened_not_after_the_protocol_message() -> None:
    fields = {field.name for field in dataclasses.fields(RelaySubscriptionHandlers)}

    assert fields == {
        "on_subscribed",
        "on_error",
        "on_object",
        "on_subgroup_end",
        "on_subgroup_reset",
        "on_publisher_done",
        "on_update_ok",
        "on_state_notify",
        "on_goaway",
    }


def test_names_the_subscriber_the_publisher_and_the_track_writer_by_role_and_by_demand() -> None:
    assert isinstance(RelaySubscriber.track_source, property)
    assert callable(RelaySubscriber.subscribe_events)
    assert callable(RelaySubscriber.namespace_events)
    assert callable(RelayPublisher.register_track)
    assert isinstance(RelayTrackWriter.has_demand, property)
    assert isinstance(RelayTrackWriter.closed, property)
    assert callable(RelayTrackWriter.demand_changes)

    for cls, old in (
        (RelaySubscriber, "tracks"),
        (RelayPublisher, "publish"),
        (RelayTrackWriter, "bound"),
        (RelayTrackWriter, "bindings"),
    ):
        assert not hasattr(cls, old), f"{cls.__name__}.{old} should not exist"
