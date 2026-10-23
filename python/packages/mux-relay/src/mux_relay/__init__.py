"""Relay Python SDK.

Owns the Relay product API and authentication convention, built on ``moq_core``. The default
transport is native QUIC through aioquic (``mux_relay.quic``), which reaches a relay natively with no
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

from ._awaitable import RelayAwaitable
from ._subscription import RelaySubscription, SubscriptionEnd, SubscriptionEndKind
from ._supervision import SupervisedStatus, SupervisionBackoff
from .cancellation import CancellationToken, Cancelled
from .connect import RelaySubscriber, RelaySubscriptionHandlers, connect_relay_subscriber
from .connect_session import ConnectingTransport, RelayCredential, RelayTransportFactory
from .errors import RelayError, RelayErrorCode, RelayErrorPhase
from .namespace_events import NamespaceEvent, NamespaceEvents
from .payload import read_json, read_text
from .publish import RelayGroupWriter, RelayPublisher, RelayTrackWriter, connect_relay_publisher
from .room_endpoint import room_endpoint
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
from .supervised_publisher import PublisherGeneration, SupervisedPublisher, connect_supervised_publisher
from .supervised_subscriber import (
    RestartReason,
    SupervisedSubscriber,
    WantHandlers,
    WantSwitchedInfo,
    connect_supervised_subscriber,
)
from .switchable import SwitchableHandlers, SwitchableSubscription, SwitchedInfo

DEFAULT_ENDPOINT = "https://relay.mux.global"
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
    "NamespaceEvent",
    "NamespaceEvents",
    "ObjectEvent",
    "PublishDone",
    "PublisherDoneEvent",
    "PublisherGeneration",
    "RelayAwaitable",
    "RelayCredential",
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
    "RestartReason",
    "StateNotifyEvent",
    "SubgroupEndEvent",
    "SubgroupResetEvent",
    "SubscribeOk",
    "SubscribedEvent",
    "Subscription",
    "SubscriptionConnectionClosedError",
    "SubscriptionEnd",
    "SubscriptionEndKind",
    "SubscriptionEvent",
    "SubscriptionEvents",
    "SubscriptionHandlers",
    "SubscriptionOverflowError",
    "SubscriptionRequestError",
    "SupervisedPublisher",
    "SupervisedStatus",
    "SupervisedSubscriber",
    "SupervisionBackoff",
    "SwitchableHandlers",
    "SwitchableSubscription",
    "SwitchedInfo",
    "UpdateOkEvent",
    "WantHandlers",
    "WantSwitchedInfo",
    "connect_relay_publisher",
    "connect_relay_subscriber",
    "connect_supervised_publisher",
    "connect_supervised_subscriber",
    "read_json",
    "read_text",
    "room_endpoint",
]
