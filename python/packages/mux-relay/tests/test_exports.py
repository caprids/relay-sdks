"""The public vocabulary in ``spec/vocabulary.md``, pinned so a rename in one SDK cannot land alone."""

import dataclasses

import mux_relay
from mux_relay import RelayPublisher, RelaySubscriber, RelaySubscriptionHandlers, RelayTrackWriter


def test_exports_one_entry_point_per_connection_role_and_nothing_under_the_old_names() -> None:
    assert callable(mux_relay.connect_relay_subscriber)
    assert callable(mux_relay.connect_relay_publisher)

    for old in ("connect_relay", "RelayConnection"):
        assert not hasattr(mux_relay, old)
        assert old not in mux_relay.__all__


def test_all_lists_the_product_types_and_the_borrowed_core_contract() -> None:
    for name in (
        "RelaySubscriber",
        "RelayPublisher",
        "RelaySubscriptionHandlers",
        "RelaySubscription",
        "SubscriptionAwaitable",
        "SubscriptionEnd",
        "SubscriptionEndKind",
        "SubscriptionEvents",
        "SubscriptionEvent",
        "SubscriptionOverflowError",
        "SubscriptionRequestError",
        "SubscriptionConnectionClosedError",
        "RelayTrackWriter",
        "RelayGroupWriter",
        "MoqTrackSource",
        "SubscriptionHandlers",
    ):
        assert name in mux_relay.__all__
        assert hasattr(mux_relay, name)


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
