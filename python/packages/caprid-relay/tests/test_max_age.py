"""Subscriber max age (the MoQ Timestamp extension) end to end: sent only to a relay whose SETUP declared it."""

import pytest
from core_helpers.waiting import wait_for
from relay_helpers.relay_hub import RelayHub

from caprid_relay import RelaySubscriptionHandlers, connect_relay_publisher, connect_relay_subscriber
from moq_core import MoqtSubscribeOptions
from moq_core.control_messages import SETUP_OPTION, KeyValuePair, MessageParameters

NAMESPACE = ["testing", "max-age"]
URL = "https://relay.example.com/moq"


@pytest.mark.parametrize("declared", [True, False])
async def test_a_max_age_reaches_only_a_relay_whose_setup_declared_it(declared: bool) -> None:
    hub = RelayHub()
    hub.subscriber_setup_options = [KeyValuePair(SETUP_OPTION.MAX_AGE, 1)] if declared else []
    publisher = await connect_relay_publisher(URL, namespace=NAMESPACE, create_transport=hub.connect_publisher)
    subscriber = await connect_relay_subscriber(URL, create_transport=hub.connect_subscriber)

    try:
        publisher.register_track("events")
        parameters = MessageParameters(subscriber_priority=7, max_age=500)
        subscribed: list[object] = []

        # The relay's SETUP arrived before the connection was returned.
        assert subscriber.max_age_supported is declared

        subscriber.subscribe(
            NAMESPACE, "events", RelaySubscriptionHandlers(on_subscribed=subscribed.append), parameters=parameters
        )
        subscriber.track_source.subscribe(
            MoqtSubscribeOptions(track_namespace=NAMESPACE, track_name="events", parameters=parameters)
        )
        await wait_for(lambda: len(subscribed) == 1 and len(hub.downstream_subscribes) == 2)

        sent = MessageParameters(subscriber_priority=7, max_age=500 if declared else None)
        assert [subscribe.parameters for subscribe in hub.downstream_subscribes] == [sent, sent]
        # The caller's parameters are not touched.
        assert parameters == MessageParameters(subscriber_priority=7, max_age=500)
    finally:
        subscriber.close()
        await publisher.close()
        hub.destroy()
