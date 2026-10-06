#!/usr/bin/env python3
"""Opt-in live probe of which SUBSCRIBE parameters a Relay honors. Excluded from ``mise run python:check``.

Re-checks the parameters ``spec/relay-interop.md`` ("Subscription parameters") lists as ignored, so
the table there stays honest across relay upgrades. It compares what it observes with the documented
moq-relay 0.17.0 behavior and exits non-zero on any difference, which means the table needs
updating, not that the SDK is wrong.

1. A subscriber subscribes to a fresh track with ``group_order="ascending"``, ``new_group_request=0``
   and both delivery timeouts, on a track whose publisher declares DEFAULT_PUBLISHER_GROUP_ORDER
   (0x22) Ascending. The probe records the relay's upstream SUBSCRIBE as the publisher receives it,
   and the group order the relay states in its SUBSCRIBE_OK.
2. The publisher writes a burst of large groups the relay caches. A second subscriber asks for them
   from an absolute start with ``group_order="ascending"`` and ``new_group_request=0``; the probe
   records the order in which their first objects arrive, and whether the relay sends the publisher
   a REQUEST_UPDATE for the new-group request (draft-22 §9.20.19 says it MUST, for a value of 0 on an
   established subscription to a track with dynamic groups; SDK publishers do not declare
   DYNAMIC_GROUPS, so this records only whether the relay forwards anything).

Environment: as ``scripts/test_relay.py`` (``RELAY_TOKEN`` or a hidden prompt, ``RELAY_URL``,
``RELAY_NAMESPACE_PREFIX``, and ``RELAY_ANONYMOUS=1`` with ``RELAY_CA_FILE`` for a local relay).

    RELAY_URL="$RELAY_URL" RELAY_NAMESPACE_PREFIX="$RELAY_NAMESPACE" mise run python:probe:relay-parameters
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from typing import Any

from test_relay import read_endpoint, read_token, redact

import mux_relay.publish
from moq_core import AbsoluteLocationFilter, KeyValuePair, Location, RelativeGroupFilter
from moq_core.control_messages import Subscribe
from moq_core.publish_session import IncomingRequestUpdate, MoqtPublishSession, MoqtPublishSessionConfig
from moq_core.transport import MoqtTransport
from mux_relay import (
    DEFAULT_ENDPOINT,
    CancellationToken,
    MessageParameters,
    MoqtObject,
    RelayError,
    RelaySubscriptionHandlers,
    SubscribeOk,
    connect_relay_publisher,
    connect_relay_subscriber,
)

RUN_DEADLINE = 60.0
RECEIVE_DEADLINE = 10.0
UPDATE_GRACE = 1.0
BURST_GROUPS = 12
BURST_PAYLOAD = 96 * 1024
DEFAULT_PUBLISHER_GROUP_ORDER = 0x22
GROUP_ORDER_NAMES = {0x1: "ascending", 0x2: "descending"}

# What spec/relay-interop.md documents for moq-relay 0.17.0.
DOCUMENTED: dict[str, Any] = {
    "upstream group_order": "descending",
    "upstream new_group_request": None,
    "upstream object_delivery_timeout": None,
    "upstream subgroup_delivery_timeout": None,
    "SUBSCRIBE_OK group order": "descending",
    "backlog arrived ascending": False,
    "upstream REQUEST_UPDATEs": 0,
}


def observe_publish_session(subscribes: list[Subscribe], updates: list[IncomingRequestUpdate]) -> None:
    """Record every SUBSCRIBE and REQUEST_UPDATE the publisher's session receives.

    The SDK does not surface the relay's upstream requests, so the probe wraps the core session
    factory that ``connect_relay_publisher`` calls and sets two core callbacks the SDK leaves unset.
    """
    create = mux_relay.publish.create_moqt_publish_session

    def create_observed(transport: MoqtTransport, config: MoqtPublishSessionConfig | None = None) -> MoqtPublishSession:
        assert config is not None
        config.callbacks.on_subscribe = subscribes.append
        config.callbacks.on_request_update = updates.append
        return create(transport, config)

    # setattr: a module function attribute is not reassignable as far as the type checker knows.
    setattr(mux_relay.publish, "create_moqt_publish_session", create_observed)  # noqa: B010


def stated_group_order(ok: SubscribeOk) -> str | None:
    if ok.parameters.group_order is not None:
        return ok.parameters.group_order

    for pair in ok.track_properties:
        if pair.type == DEFAULT_PUBLISHER_GROUP_ORDER:
            return GROUP_ORDER_NAMES.get(pair.value, repr(pair.value))

    return None


async def run(endpoint: str, token: str | None, namespace: list[str], report: Any) -> dict[str, Any]:
    from aioquic.quic.configuration import QuicConfiguration

    from mux_relay.quic import connect_quic

    cafile = os.environ.get("RELAY_CA_FILE")

    def dial(url: str, protocols: Any) -> Any:
        return connect_quic(url, protocols, configuration=QuicConfiguration(is_client=True, cafile=cafile))

    # Without a trust root, the run takes the SDK's own default transport.
    create_transport = dial if cafile else None

    lifetime = CancellationToken()
    asyncio.get_running_loop().call_later(
        RUN_DEADLINE, lambda: lifetime.cancel(TimeoutError(f"Live relay probe exceeded {RUN_DEADLINE:.0f} seconds"))
    )
    subscribes: list[Subscribe] = []
    updates: list[IncomingRequestUpdate] = []
    errors: list[str] = []
    observe_publish_session(subscribes, updates)

    async def wait_for(predicate: Any, label: str) -> None:
        until = time.monotonic() + RECEIVE_DEADLINE

        while not predicate():
            if errors:
                raise RuntimeError("; ".join(errors))

            lifetime.throw_if_cancelled()

            if time.monotonic() >= until:
                raise TimeoutError(f"Timed out waiting for {label}")

            await asyncio.sleep(0.02)

    def connect_subscriber() -> Any:
        return connect_relay_subscriber(
            endpoint, get_token=lambda: token, cancellation=lifetime, create_transport=create_transport
        )

    publisher = await connect_relay_publisher(
        endpoint, namespace=namespace, get_token=lambda: token, cancellation=lifetime, create_transport=create_transport
    )
    subscriber = await connect_subscriber()
    late = await connect_subscriber()
    report({"phase": "connected", "namespace": namespace})

    try:
        # The announce goes out with the first registered track.
        track = publisher.register_track(
            "probe",
            track_properties=[KeyValuePair(type=DEFAULT_PUBLISHER_GROUP_ORDER, value=0x1)],
            write_timeout=20,
        )
        await publisher.announced
        oks: list[SubscribeOk] = []
        live: list[int] = []

        def on_live(obj: MoqtObject) -> None:
            if obj.status == "normal":
                live.append(obj.group_id)

        # A subscribe that races the announce is refused (spec/relay-interop.md); retry it.
        refused: list[Any] = []

        for _ in range(20):
            refused.clear()
            subscription = subscriber.subscribe(
                namespace,
                "probe",
                RelaySubscriptionHandlers(on_subscribed=oks.append, on_object=on_live, on_error=refused.append),
                parameters=MessageParameters(
                    # Not Next Object: moq-relay 0.17.0 closes the session on its draft-22 spelling
                    # (spec/relay-interop.md, "Draft-22 LOCATION_FILTER framing").
                    location_filter=RelativeGroupFilter(groups_before_next=0),
                    group_order="ascending",
                    new_group_request=0,
                    object_delivery_timeout=5000,
                    subgroup_delivery_timeout=5000,
                ),
            )
            await wait_for(lambda: bool(oks or refused), "SUBSCRIBE answer")

            if oks:
                break

            subscription.cancel()
            await asyncio.sleep(0.25)
        else:
            raise RuntimeError(f"subscription refused: {refused[0].error_code} {refused[0].reason}")

        # The relay's first upstream subscription can be a short-lived track-info probe whose objects
        # are lost (spec/relay-interop.md, #243): write until one reaches the subscriber.
        await wait_for(lambda: bool(subscribes), "the relay's upstream SUBSCRIBE")
        until = time.monotonic() + RECEIVE_DEADLINE

        while not live:
            if time.monotonic() >= until:
                raise TimeoutError("Timed out waiting for a warm-up object")

            try:
                await track.write(b"warm-up")
            except RelayError as error:
                if error.code != "SUBSCRIPTION_CHANGED":
                    raise

            await asyncio.sleep(0.25)

        upstream = subscribes[-1].parameters
        report(
            {
                "phase": "upstream SUBSCRIBE",
                "subscribes": len(subscribes),
                "subscriber_priority": upstream.subscriber_priority,
                "group_order": upstream.group_order,
                "new_group_request": upstream.new_group_request,
            }
        )

        # Groups the relay caches, each large enough that the order they are served in can show.
        burst_start = live[-1] + 1
        burst_end = burst_start + BURST_GROUPS - 1

        for _ in range(BURST_GROUPS):
            await track.write(bytes(BURST_PAYLOAD))

        await wait_for(lambda: live[-1] >= burst_end, "the burst on the first subscriber")
        updates_before = len(updates)
        late_oks: list[SubscribeOk] = []
        arrivals: list[int] = []

        def on_late(obj: MoqtObject) -> None:
            if obj.status == "normal" and obj.object_id == 0:
                arrivals.append(obj.group_id)

        late.subscribe(
            namespace,
            "probe",
            RelaySubscriptionHandlers(
                on_subscribed=late_oks.append,
                on_object=on_late,
                on_error=lambda error: errors.append(f"late: {error.error_code} {error.reason}"),
            ),
            parameters=MessageParameters(
                location_filter=AbsoluteLocationFilter(start=Location(group=burst_start, object=0)),
                group_order="ascending",
                new_group_request=0,
            ),
        )
        await wait_for(lambda: len(arrivals) >= BURST_GROUPS, f"{BURST_GROUPS} cached groups")
        await asyncio.sleep(UPDATE_GRACE)
        report(
            {"phase": "backlog", "arrivals": arrivals, "late SUBSCRIBE_OK group order": stated_group_order(late_oks[0])}
        )

        return {
            "upstream group_order": upstream.group_order,
            "upstream new_group_request": upstream.new_group_request,
            "upstream object_delivery_timeout": upstream.object_delivery_timeout,
            "upstream subgroup_delivery_timeout": upstream.subgroup_delivery_timeout,
            "SUBSCRIBE_OK group order": stated_group_order(oks[0]),
            "backlog arrived ascending": arrivals == sorted(arrivals),
            "upstream REQUEST_UPDATEs": len(updates) - updates_before,
        }
    finally:
        subscriber.close()
        late.close()
        await publisher.close()


def main() -> int:
    endpoint = read_endpoint("RELAY_URL", DEFAULT_ENDPOINT)
    token = read_token()
    prefix = [field for field in os.environ.get("RELAY_NAMESPACE_PREFIX", "relay-sdk-smoke").split("/") if field]
    namespace = [*prefix, str(uuid.uuid4())]

    def report(value: Any) -> None:
        print(redact(json.dumps(value) if not isinstance(value, str) else value, token), flush=True)

    try:
        observed = asyncio.run(run(endpoint, token, namespace, report))
    except BaseException as error:
        report({"result": "FAIL", "error": redact(str(error) or type(error).__name__, token)})
        return 1

    differs = {key: value for key, value in observed.items() if DOCUMENTED[key] != value}
    report({"result": "DIFFERS" if differs else "AS DOCUMENTED", "observed": observed, "differs": differs})
    return 1 if differs else 0


if __name__ == "__main__":
    sys.exit(main())
