"""The negotiated MOQT version end to end: the SDK offers moqt-22, then moqt-20, and speaks what the relay selects."""

import pytest
from core_helpers.waiting import wait_for
from relay_helpers.relay_hub import RelayHub

from caprid_relay import RelaySubscriptionHandlers, connect_relay_publisher, connect_relay_subscriber
from moq_core import MoqtObject
from moq_core.control_messages import MessageParameters, MoqtProtocolId, NextObjectFilter, RelativeGroupFilter

NAMESPACE = ["testing", "negotiated"]
URL = "https://relay.example.com/moq"


@pytest.mark.parametrize(
    ("relay_protocols", "negotiated"),
    [(("moqt-22", "moqt-20"), "moqt-22"), (("moqt-20",), "moqt-20"), (("moqt-20", "moqt-22"), "moqt-20")],
)
async def test_a_subscription_filter_reaches_the_relay_in_the_negotiated_draft(
    relay_protocols: tuple[MoqtProtocolId, ...], negotiated: MoqtProtocolId
) -> None:
    hub = RelayHub(relay_protocols)
    publisher = await connect_relay_publisher(URL, namespace=NAMESPACE, create_transport=hub.connect_publisher)
    subscriber = await connect_relay_subscriber(URL, create_transport=hub.connect_subscriber)

    try:
        track = publisher.register_track("events")
        objects: list[MoqtObject] = []
        subscribed: list[object] = []
        parameters = MessageParameters(
            location_filter=NextObjectFilter(),
            fill_parameters=MessageParameters(location_filter=RelativeGroupFilter(groups_before_next=1)),
        )
        subscriber.subscribe(
            NAMESPACE,
            "events",
            RelaySubscriptionHandlers(on_subscribed=subscribed.append, on_object=objects.append),
            parameters=parameters,
        )
        await wait_for(lambda: len(subscribed) == 1)

        # The hub decodes each connection in the draft that connection negotiated, so the filter it
        # read is the one the subscriber sent only if both sides agreed on the framing.
        assert [subscribe.parameters for subscribe in hub.downstream_subscribes] == [parameters]
        assert hub.negotiated == [negotiated, negotiated]

        await track.write(b"after the filter")
        await wait_for(lambda: len(objects) == 1)
        assert objects[0].payload == b"after the filter"
    finally:
        subscriber.close()
        await publisher.close()
        hub.destroy()
