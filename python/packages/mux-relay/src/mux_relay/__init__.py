"""Relay Python SDK.

Owns the Relay product API and authentication convention, built on ``moq_core``. The default
transport is WebTransport over HTTP/3 through aioquic, which reaches a relay natively with no
browser; ``create_transport`` swaps in another implementation of the same seam.

The public names are the rows of ``spec/vocabulary.md``, pinned by ``tests/test_exports.py``. From
the core this package re-exports exactly what ``@mux/relay`` re-exports, plus ``Closed``; every
other protocol type (location filters, fetch, track status, the constants) is imported from
``moq_core``, a declared dependency.
"""

from moq_core import (
    Closed,
    Goaway,
    KeyValuePair,
    LocationFilter,
    MessageParameters,
    MoqtObject,
    MoqTrackSource,
    MoqtTransport,
    PublishDone,
    RequestError,
    SubscribeOk,
    Subscription,
    SubscriptionHandlers,
)

from ._subscription import RelaySubscription, SubscriptionAwaitable, SubscriptionEnd, SubscriptionEndKind
from .cancellation import CancellationToken, Cancelled
from .connect import RelaySubscriber, RelaySubscriptionHandlers, connect_relay_subscriber
from .connect_session import ConnectingTransport, RelayTransportFactory
from .errors import RelayError, RelayErrorCode, RelayErrorPhase
from .payload import read_json, read_text
from .publish import RelayGroupWriter, RelayPublisher, RelayTrackWriter, connect_relay_publisher
from .subscription_events import (
    GoawayEvent,
    ObjectEvent,
    PublisherDoneEvent,
    StateNotifyEvent,
    SubgroupEndEvent,
    SubgroupResetEvent,
    SubscribedEvent,
    SubscriptionConnectionClosedError,
    SubscriptionEvent,
    SubscriptionEvents,
    SubscriptionOverflowError,
    SubscriptionRequestError,
    UpdateOkEvent,
)
from .webtransport import connect_webtransport

DEFAULT_ENDPOINT = "https://sjc.relay.mux.global"
"""The default Relay endpoint pinned in ``spec/protocol.json``."""

__all__ = [
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
    "SubscriptionAwaitable",
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
    "connect_webtransport",
    "read_json",
    "read_text",
]
