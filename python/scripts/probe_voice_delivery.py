#!/usr/bin/env python3
"""Opt-in live probe of a Relay's voice delivery. Excluded from ``mise run python:check``.

Measures what a voice-style track (one object per group, every 20 ms, a LOC Timestamp on each
object) gets from a relay, and prints what it observes. It compares nothing with documented values,
because relay behavior changes between versions; read the lines and the final ``MEASURED`` summary.
It exits 0 on a completed run and 1 on a failure.

1. A publisher registers a track with track property Timescale (0x08) = 1000000 and writes one-object
   groups every 20 ms for ``VOICE_RUN_S`` seconds. Each object carries a LOC Timestamp (0x10) in
   microseconds and a ``VOICE_PAYLOAD``-byte payload whose first 8 bytes are the sequence number.
2. A subscriber, on ``RELAY_SUB_URL``, subscribes with ``RelativeGroupFilter(groups_before_next=0)``,
   retrying the announce race, with ``object_delivery_timeout`` and ``subgroup_delivery_timeout`` set to
   ``VOICE_SUB_TIMEOUT_MS`` and the group order from ``VOICE_GROUP_ORDER``.
3. At ``VOICE_FETCH_AT_S`` the subscriber sends standalone FETCHes through its track source: one
   single group at each age in ``VOICE_FETCH_AGES_MS`` behind the newest group it has received, and
   one two-group range. Each ``fetch`` line reports ok or the REQUEST_ERROR code and reason, how long
   the fetch took, how many objects it returned, and whether they carried properties (a LOC
   Timestamp in particular).
4. After publishing ends and a drain of ``VOICE_DRAIN_S`` seconds, the ``live`` line reports sent,
   received and missing objects, subgroup resets, lateness (arrival minus send, on this host's clock)
   as p50, p99 and max in milliseconds, how many received objects carried a Timestamp, and how many
   arrivals came before an older group (``out_of_order``).

Point ``RELAY_SUB_URL`` at a link impairer such as moq-shaper to make the subscriber's link slow.

Environment: as ``scripts/test_relay.py`` (``RELAY_TOKEN`` or a hidden prompt, ``RELAY_URL``,
``RELAY_SUB_URL`` for the subscriber's relay, defaulting to ``RELAY_URL``, ``RELAY_NAMESPACE_PREFIX``,
and ``RELAY_ANONYMOUS=1`` with ``RELAY_CA_FILE`` for a local relay), plus these knobs:

- ``VOICE_RUN_S``: seconds of publishing (default 14).
- ``VOICE_PAYLOAD``: payload bytes per object, at least 8 (default 160).
- ``VOICE_SUB_TIMEOUT_MS``: both delivery timeouts, in milliseconds; 0 omits them (default 100).
- ``VOICE_GROUP_ORDER``: ``ascending`` or ``descending``; unset omits it.
- ``VOICE_FETCH_AT_S``: seconds after the subscription at which the fetches start (default 10).
- ``VOICE_FETCH_AGES_MS``: comma-separated ages for the single-group fetches (default
  ``40,1000,3000,5000,8000``).
- ``VOICE_DRAIN_S``: seconds to keep receiving after publishing ends (default 2).

    RELAY_URL="$RELAY_URL" RELAY_NAMESPACE_PREFIX="$RELAY_NAMESPACE" mise run python:probe:voice-delivery
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from test_relay import read_endpoint, read_token, redact

from moq_core import AbsoluteLocationFilter, FetchOk, KeyValuePair, Location, RelativeGroupFilter
from moq_core.control_messages import RequestError
from moq_core.object_stream import FetchStreamEntry
from moq_core.session import FetchHandlers, FetchReset, MoqtSubscribeOptions
from mux_relay import (
    DEFAULT_ENDPOINT,
    CancellationToken,
    MessageParameters,
    MoqtObject,
    RelaySubscriptionHandlers,
    SubscribeOk,
    connect_relay_publisher,
    connect_relay_subscriber,
)

RUN_DEADLINE_SLACK = 60.0
ANSWER_DEADLINE = 2.0
FETCH_DEADLINE = 5.0
SUBSCRIBE_ATTEMPTS = 40
FRAME_S = 0.02
FRAME_MS = 20
TIMESCALE = 0x08
TIMESTAMP = 0x10
TRACK = "audio"


@dataclass(slots=True)
class Arrival:
    """One live object as the subscriber received it."""

    group_id: int
    at: float
    timestamp: int | None


@dataclass(slots=True)
class FetchResult:
    """What one FETCH did, filled in by its handlers."""

    ok: bool = False
    error: tuple[int, str] | None = None
    end: str | None = None
    objects: list[tuple[int, list[tuple[int, Any]]]] = field(default_factory=list)


def percentile(sorted_values: list[float], fraction: float) -> float | None:
    if not sorted_values:
        return None

    return sorted_values[min(len(sorted_values) - 1, max(0, math.ceil(fraction * len(sorted_values)) - 1))]


def count_out_of_order(arrivals_in_order: list[int]) -> int:
    """Count arrivals of a group that an older group's later arrival overtakes."""
    count = 0
    oldest_later: int | None = None

    for group_id in reversed(arrivals_in_order):
        if oldest_later is not None and oldest_later < group_id:
            count += 1

        oldest_later = group_id if oldest_later is None else min(oldest_later, group_id)

    return count


async def run(endpoint: str, sub_endpoint: str, token: str | None, namespace: list[str], report: Any) -> dict[str, Any]:
    from aioquic.quic.configuration import QuicConfiguration

    from mux_relay.quic import connect_quic

    run_s = float(os.environ.get("VOICE_RUN_S", "14"))
    payload_size = int(os.environ.get("VOICE_PAYLOAD", "160"))
    timeout_ms = int(os.environ.get("VOICE_SUB_TIMEOUT_MS", "100"))
    group_order = os.environ.get("VOICE_GROUP_ORDER") or None
    fetch_at = float(os.environ.get("VOICE_FETCH_AT_S", "10"))
    ages_ms = [int(age) for age in os.environ.get("VOICE_FETCH_AGES_MS", "40,1000,3000,5000,8000").split(",") if age]
    drain_s = float(os.environ.get("VOICE_DRAIN_S", "2"))

    if payload_size < 8:
        raise SystemExit("VOICE_PAYLOAD must be at least 8")

    if group_order not in (None, "ascending", "descending"):
        raise SystemExit("VOICE_GROUP_ORDER must be ascending or descending")

    cafile = os.environ.get("RELAY_CA_FILE")

    def dial(url: str, protocols: Any) -> Any:
        return connect_quic(url, protocols, configuration=QuicConfiguration(is_client=True, cafile=cafile))

    # Without a trust root, the run takes the SDK's own default transport.
    create_transport = dial if cafile else None

    deadline = max(run_s, fetch_at) + drain_s + RUN_DEADLINE_SLACK
    lifetime = CancellationToken()
    asyncio.get_running_loop().call_later(
        deadline, lambda: lifetime.cancel(TimeoutError(f"Live relay probe exceeded {deadline:.0f} seconds"))
    )

    publisher = await connect_relay_publisher(
        endpoint, namespace=namespace, get_token=lambda: token, cancellation=lifetime, create_transport=create_transport
    )
    subscriber = await connect_relay_subscriber(
        sub_endpoint, get_token=lambda: token, cancellation=lifetime, create_transport=create_transport
    )
    report({"phase": "connected", "namespace": namespace, "subscriber_on_publisher_relay": sub_endpoint == endpoint})

    sent: dict[int, float] = {}
    arrivals: dict[int, Arrival] = {}
    arrival_order: list[int] = []
    resets: list[int] = []
    publish_task: asyncio.Task[None] | None = None

    def on_object(obj: MoqtObject) -> None:
        if obj.status != "normal" or len(obj.payload) < 8:
            return

        seq = int.from_bytes(obj.payload[:8], "big")
        timestamp = next((pair.value for pair in obj.properties if pair.type == TIMESTAMP), None)

        if seq not in arrivals:
            arrival_order.append(obj.group_id)

        arrivals[seq] = Arrival(
            group_id=obj.group_id, at=time.monotonic(), timestamp=timestamp if isinstance(timestamp, int) else None
        )

    try:
        track = publisher.register_track(
            TRACK, track_properties=[KeyValuePair(type=TIMESCALE, value=1_000_000)], write_timeout=20
        )
        await publisher.announced

        # A subscribe that races the announce is refused (spec/relay-interop.md); retry it.
        oks: list[SubscribeOk] = []
        refused: list[Any] = []
        delivery_timeouts = timeout_ms or None

        for _ in range(SUBSCRIBE_ATTEMPTS):
            oks.clear()
            refused.clear()
            subscription = subscriber.subscribe(
                namespace,
                TRACK,
                RelaySubscriptionHandlers(
                    on_subscribed=oks.append,
                    on_object=on_object,
                    on_error=refused.append,
                    on_subgroup_reset=lambda info: resets.append(info.group_id),
                ),
                parameters=MessageParameters(
                    # Not Next Object: moq-relay 0.17.0 closes the session on its draft-22 spelling
                    # (spec/relay-interop.md, "Draft-22 LOCATION_FILTER framing").
                    location_filter=RelativeGroupFilter(groups_before_next=0),
                    object_delivery_timeout=delivery_timeouts,
                    subgroup_delivery_timeout=delivery_timeouts,
                    group_order=group_order,
                ),
            )
            until = time.monotonic() + ANSWER_DEADLINE

            while not (oks or refused) and time.monotonic() < until:
                lifetime.throw_if_cancelled()
                await asyncio.sleep(0.02)

            if oks:
                break

            subscription.cancel()
            await asyncio.sleep(0.1)
        else:
            detail = f"{refused[0].error_code} {refused[0].reason}" if refused else "no answer"
            raise RuntimeError(f"subscription not accepted after {SUBSCRIBE_ATTEMPTS} attempts: {detail}")

        report(
            {
                "phase": "subscribed",
                "object_delivery_timeout_ms": delivery_timeouts,
                "subgroup_delivery_timeout_ms": delivery_timeouts,
                "group_order": group_order,
                # The relay states it as track property 0x22 (Default Publisher Group Order): 1 ascending, 2 descending.
                "SUBSCRIBE_OK group order": oks[0].parameters.group_order
                or next((pair.value for pair in oks[0].track_properties if pair.type == 0x22), None),
            }
        )

        origin = time.monotonic()

        async def publish() -> None:
            start = time.monotonic()
            seq = 0

            while time.monotonic() - start < run_s:
                now = time.monotonic()
                sent[seq] = now
                await track.write(
                    seq.to_bytes(8, "big") + bytes(payload_size - 8),
                    properties=[KeyValuePair(type=TIMESTAMP, value=int((now - origin) * 1_000_000))],
                )
                seq += 1
                await asyncio.sleep(max(0.0, start + seq * FRAME_S - time.monotonic()))

        publish_task = asyncio.create_task(publish())
        await _sleep(lifetime, fetch_at, publish_task)

        if not arrivals:
            raise RuntimeError(f"no object had arrived {fetch_at:g} s after the subscription")

        newest = arrivals[max(arrivals)].group_id

        async def fetch(label: str, start_group: int, end_group_delta: int) -> dict[str, Any]:
            result = FetchResult()
            done = asyncio.Event()

            def on_ok(ok: FetchOk) -> None:
                result.ok = True

            def on_error(error: RequestError) -> None:
                result.error = (error.error_code, error.reason)
                done.set()

            def on_entry(entry: FetchStreamEntry) -> None:
                if entry.kind == "object":
                    result.objects.append((entry.group_id, [(pair.type, pair.value) for pair in entry.properties]))

            def on_end() -> None:
                result.end = "fin"
                done.set()

            def on_reset(reset: FetchReset) -> None:
                result.end = f"reset: {reset.error}"
                done.set()

            started = time.monotonic()
            subscriber.track_source.fetch(
                MoqtSubscribeOptions(
                    track_namespace=namespace,
                    track_name=TRACK,
                    parameters=MessageParameters(
                        location_filter=AbsoluteLocationFilter(
                            start=Location(group=start_group, object=0), end_group_delta=end_group_delta
                        )
                    ),
                ),
                FetchHandlers(on_ok=on_ok, on_error=on_error, on_entry=on_entry, on_end=on_end, on_reset=on_reset),
            )

            try:
                await asyncio.wait_for(done.wait(), FETCH_DEADLINE)
            except TimeoutError:
                result.end = result.end or "timeout"

            property_types = sorted({kind for _, props in result.objects for kind, _ in props})
            line: dict[str, Any] = {
                "phase": "fetch",
                "label": label,
                "start_group": start_group,
                "groups_behind_newest": newest - start_group,
                "ok": result.ok,
                "elapsed_ms": round((time.monotonic() - started) * 1000),
                "objects": len(result.objects),
                "groups_returned": sorted({group_id for group_id, _ in result.objects}),
                "objects_with_properties": sum(1 for _, props in result.objects if props),
                "objects_with_timestamp": sum(
                    1 for _, props in result.objects if any(k == TIMESTAMP for k, _ in props)
                ),
                "property_types": property_types,
                "end": result.end,
            }

            if result.error is not None:
                line["error_code"], line["error_reason"] = result.error

            report(line)
            return line

        fetches: list[dict[str, Any]] = []

        for age in ages_ms:
            if newest - age // FRAME_MS < 0:
                # The track has no group that old yet (group IDs start near 0): shorten the run's
                # fetch age or lengthen VOICE_FETCH_AT_S.
                report({"phase": "fetch", "label": f"age {age} ms", "skipped": "older than the track's first group"})
                continue

            fetches.append(await fetch(f"age {age} ms", newest - age // FRAME_MS, 0))

        fetches.append(await fetch("two-group range", max(0, newest - 10), 1))

        await publish_task
        await _sleep(lifetime, drain_s)

        lates = sorted((arrivals[seq].at - sent[seq]) * 1000 for seq in arrivals if seq in sent)
        missing = [seq for seq in sent if seq not in arrivals]
        live: dict[str, Any] = {
            "sent": len(sent),
            "received": len(arrivals),
            "missing": len(missing),
            "subgroup_resets": len(resets),
            "late_p50_ms": _round(percentile(lates, 0.50)),
            "late_p99_ms": _round(percentile(lates, 0.99)),
            "late_max_ms": _round(lates[-1] if lates else None),
            "with_timestamp": sum(1 for arrival in arrivals.values() if arrival.timestamp is not None),
            "out_of_order": count_out_of_order(arrival_order),
        }
        report({"phase": "live", **live})

        return {
            "subscriber_timeout_ms": delivery_timeouts,
            "group_order": group_order,
            "payload": payload_size,
            "live": live,
            "fetches": {
                str(line["label"]): "ok" if line["ok"] else f"error {line.get('error_code')} {line.get('error_reason')}"
                for line in fetches
            },
        }
    finally:
        lifetime.cancel()

        if publish_task is not None and not publish_task.done():
            publish_task.cancel()
            await asyncio.gather(publish_task, return_exceptions=True)

        subscriber.close()
        await publisher.close()


def _round(value: float | None) -> float | None:
    return None if value is None else round(value, 1)


async def _sleep(lifetime: CancellationToken, seconds: float, publish_task: asyncio.Task[None] | None = None) -> None:
    """Sleep ``seconds``, failing early if the run was cancelled or the publisher task failed."""
    until = time.monotonic() + seconds

    while time.monotonic() < until:
        lifetime.throw_if_cancelled()

        if publish_task is not None and publish_task.done():
            publish_task.result()

        await asyncio.sleep(min(0.02, max(0.0, until - time.monotonic())))


def main() -> int:
    endpoint = read_endpoint("RELAY_URL", DEFAULT_ENDPOINT)
    sub_endpoint = read_endpoint("RELAY_SUB_URL", endpoint)
    token = read_token()
    prefix = [field for field in os.environ.get("RELAY_NAMESPACE_PREFIX", "relay-sdk-smoke").split("/") if field]
    namespace = [*prefix, str(uuid.uuid4())]

    def report(value: Any) -> None:
        print(redact(json.dumps(value) if not isinstance(value, str) else value, token), flush=True)

    try:
        measured = asyncio.run(run(endpoint, sub_endpoint, token, namespace, report))
    except BaseException as error:
        report({"result": "FAIL", "error": redact(str(error) or type(error).__name__, token)})
        return 1

    report({"result": "MEASURED", **measured})
    return 0


if __name__ == "__main__":
    sys.exit(main())
