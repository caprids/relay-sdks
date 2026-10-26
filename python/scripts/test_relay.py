#!/usr/bin/env python3
"""Opt-in live smoke test against a real Relay. Excluded from ``mise run python:check``.

Publishes and subscribes on the same random namespace over the SDK's default transport (native QUIC
on aioquic), then asserts the list in ``spec/behavior.md``:

1. A publisher connection and a subscriber connection both open with the supplied token.
2. Once the publisher's ``announced`` settles, subscriptions to two tracks on the namespace are
   accepted, retrying the fresh-namespace rejection while the announce propagates: a supervised
   subscriber's wants make them and never resubscribe during the run.
3. Binary payloads, an empty payload, and a text payload all arrive intact, each written once after
   a warm-up marker has arrived on its track; the report records the warm-up writes.
4. Closing one track writer does not affect delivery on another track.
5. A late subscriber decodes LARGEST_OBJECT and receives the next live object.
6. The subscriber's discovery on the namespace's parent prefix reports the namespace ``announced``,
   then ``unannounced`` after the publisher's clean ``close()``; the report records how long that took.
7. A want following a supervised publisher's root receives the first generation's objects; when a
   second supervised publisher starts on the same root while the first is still up, the want moves to
   the newer generation make-before-break (issue #282): both generations beat one shared counter, the
   want reports one ``on_switched`` and no ``on_restarted``, and no beat number is missing across the
   move. The report records how long the move took.
8. A switchable subscription moves from one layer to another make-before-break with no gap.
9. A room-rooted endpoint (issue #266), whose path is the run's namespace (built with
   ``room_endpoint``, issue #348), takes namespaces relative to that path: a publisher and a
   subscriber on each form reach each other's track, discovery on each form names both publishers,
   and the relay refuses an absolute namespace on a rooted connection without delivering the room's
   objects.
10. All connections close cleanly once the test closes them.

Environment (shared by every SDK's tooling): ``RELAY_TOKEN`` (or a hidden prompt), ``RELAY_URL``
(https, without the token), ``RELAY_SUB_URL`` (the subscribers' relay for a cross-relay run, defaulting
to ``RELAY_URL``), ``RELAY_NAMESPACE_PREFIX`` (a random field is appended). The token is
never written to disk and is redacted from output. For anonymous local relays, set
``RELAY_ANONYMOUS=1``; ``RELAY_CA_FILE`` supplies a local certificate trust root.
See ``spec/relay-interop.md`` for the local moq-relay 0.17.0 reproduction.

    # After the README's endpoint/namespace setup; enter the token at the hidden prompt.
    RELAY_URL="$RELAY_URL" RELAY_NAMESPACE_PREFIX="$RELAY_NAMESPACE" mise run python:test:relay
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from getpass import getpass
from typing import Any
from urllib.parse import urlsplit

from caprid_relay import (
    DEFAULT_ENDPOINT,
    CancellationToken,
    Closed,
    MessageParameters,
    MoqtObject,
    RelayAwaitable,
    RelayError,
    RelayPublisher,
    RelaySubscriber,
    RelaySubscriptionHandlers,
    SupervisedPublisher,
    SupervisedSubscriber,
    SwitchableHandlers,
    WantHandlers,
    WantSwitchedInfo,
    connect_relay_publisher,
    connect_relay_subscriber,
    connect_supervised_publisher,
    connect_supervised_subscriber,
    room_endpoint,
)
from moq_core import RelativeGroupFilter

RUN_DEADLINE = 60.0
RECEIVE_DEADLINE = 10.0
WARM_UP = b"relay-sdk-smoke warm-up"
WARM_UP_INTERVAL = 0.25


def read_token() -> str | None:
    if os.environ.get("RELAY_ANONYMOUS") == "1":
        return os.environ.get("RELAY_TOKEN") or None

    token = os.environ.get("RELAY_TOKEN")

    if not token and sys.stdin.isatty():
        token = getpass("Relay token (input hidden): ").strip()
    elif not token:
        token = sys.stdin.readline().strip()

    if not token:
        raise SystemExit("A Relay token is required (RELAY_TOKEN or the prompt)")

    return token


def redact(value: str, token: str | None) -> str:
    return value.replace(token, "[redacted]") if token else value


def read_endpoint(name: str, default: str) -> str:
    endpoint = os.environ.get(name, default)
    parts = urlsplit(endpoint)

    if parts.scheme != "https":
        raise SystemExit(f"{name} must be an https URL")

    if "jwt=" in parts.query:
        raise SystemExit(f"Supply the token via stdin or RELAY_TOKEN, not {name}")

    return endpoint


async def run(endpoint: str, sub_endpoint: str, token: str | None, namespace: list[str], report: Any) -> dict[str, Any]:
    from aioquic.quic.configuration import QuicConfiguration

    from caprid_relay.quic import connect_quic

    cafile = os.environ.get("RELAY_CA_FILE")

    def dial(url: str, protocols: Any) -> Any:
        return connect_quic(url, protocols, configuration=QuicConfiguration(is_client=True, cafile=cafile))

    # Without a trust root, the run takes the SDK's own default transport.
    create_transport = dial if cafile else None

    lifetime = CancellationToken()
    loop = asyncio.get_running_loop()
    deadline = loop.call_later(
        RUN_DEADLINE, lambda: lifetime.cancel(TimeoutError(f"Live relay test exceeded {RUN_DEADLINE:.0f} seconds"))
    )
    received: dict[str, list[bytes]] = {"events": [], "status": [], "lateStatus": []}
    errors: list[str] = []
    expected_events = [bytes([0, 255, 128, 1]), b"hello from Relay SDK", b""]
    closing = False
    publisher: RelayPublisher | None = None
    subscriber: RelaySubscriber | None = None
    late_subscriber: RelaySubscriber | None = None
    watcher: SupervisedSubscriber | None = None
    supervised_publishers: list[SupervisedPublisher] = []
    late_largest_object = None
    discovered: list[str] = []
    announced_namespaces: set[tuple[str, ...]] = set()
    layer_publisher: RelayPublisher | None = None
    layer_switch: dict[str, Any] | None = None
    discovery_task: asyncio.Task[None] | None = None
    closed_on_purpose: set[str] = set()
    room_publishers: list[RelayPublisher] = []
    room_subscriber: RelaySubscriber | None = None
    room_pair: dict[str, Any] | None = None

    def observe_closed(label: str, closed: RelayAwaitable[Closed]) -> None:
        def done(future: asyncio.Future[Closed]) -> None:
            if not closing and label not in closed_on_purpose:
                errors.append(f"{label} closed: {future.result().error or 'peer closure'}")

        closed.add_done_callback(done)

    async def wait_for(predicate: Any, label: str) -> None:
        until = time.monotonic() + RECEIVE_DEADLINE

        while not predicate():
            if errors:
                raise RuntimeError("; ".join(errors))

            lifetime.throw_if_cancelled()

            if time.monotonic() >= until:
                raise TimeoutError(f"Timed out waiting for {label}")

            await asyncio.sleep(0.02)

    async def wait_for_count(track: str, count: int) -> None:
        await wait_for(lambda: len(received[track]) >= count, f"{track}: {count} objects")

    warmed: set[str] = set()

    async def warm_up(track: str, writer: Any) -> int:
        # SUBSCRIBE_OK says the subscriber's relay took the request, not that the path to the publisher
        # is ready. A relay on another hop first subscribes only to learn the track's info and cancels
        # that within about 50 ms (spec/relay-interop.md), so the writes it takes are lost. Repeat a
        # marker the handlers drop until one arrives; the assertion payloads are then written once.
        until = time.monotonic() + RECEIVE_DEADLINE
        writes = 0

        while track not in warmed:
            if errors:
                raise RuntimeError("; ".join(errors))

            if time.monotonic() >= until:
                raise TimeoutError(f"Timed out waiting for {track}: warm-up object")

            try:
                await writer.write(WARM_UP)
            except RelayError as error:
                if error.code != "SUBSCRIPTION_CHANGED":
                    raise

            writes += 1
            sent = time.monotonic()

            while track not in warmed and time.monotonic() - sent < WARM_UP_INTERVAL:
                lifetime.throw_if_cancelled()
                await asyncio.sleep(0.02)

        return writes

    try:
        publisher = await connect_relay_publisher(
            endpoint,
            namespace=namespace,
            get_token=lambda: token,
            cancellation=lifetime,
            create_transport=create_transport,
        )
        observe_closed("publisher", publisher.closed)
        report({"phase": "publisher connected", "namespace": namespace})
        events = publisher.register_track("events", write_timeout=20)
        status = publisher.register_track("status", write_timeout=20)
        subscriber = await connect_relay_subscriber(
            sub_endpoint, get_token=lambda: token, cancellation=lifetime, create_transport=create_transport
        )
        observe_closed("subscriber", subscriber.closed)
        report({"phase": "subscriber connected"})

        # Discovery: the parent prefix may hold other runs' namespaces, so only this run's counts.
        prefix = namespace[:-1] if len(namespace) > 1 else namespace
        discovery = subscriber.namespace_events(prefix, cancellation=lifetime)

        async def discover() -> None:
            try:
                async with discovery:
                    async for event in discovery:
                        if event.type == "announced":
                            announced_namespaces.add(tuple(event.namespace))
                        if list(event.namespace) == namespace:
                            discovered.append(event.type)
                if not closing:
                    errors.append("discovery ended early")
            except Exception as error:
                if not closing:
                    errors.append(f"discovery: {error!r}")

        discovery_task = asyncio.create_task(discover())

        # Assertion 2. The announce left with the first registered track; the relay routes subscribes to
        # the namespace about a round trip later. The supervised subscriber's wants retry the rejection it
        # may still answer, and report only what they do not retry.
        await publisher.announced
        report({"phase": "announced"})
        await wait_for(lambda: "announced" in discovered, "discovery: announced")
        report({"phase": "discovered"})

        def on_status(state: str, error: BaseException | None) -> None:
            if state == "reconnecting" and not closing:
                errors.append(f"supervised subscriber reconnecting: {error!r}")

        watcher = await connect_supervised_subscriber(
            sub_endpoint,
            get_token=lambda: token,
            cancellation=lifetime,
            create_transport=create_transport,
            on_status=on_status,
        )
        report({"phase": "supervised subscriber connected"})
        subscribed: set[str] = set()

        for track in ("events", "status"):

            def on_object(obj: MoqtObject, track: str = track) -> None:
                if obj.status != "normal":
                    return
                if obj.payload == WARM_UP:
                    warmed.add(track)
                else:
                    received[track].append(obj.payload)

            def on_subscribed(_ok: Any, _namespace: tuple[str, ...], track: str = track) -> None:
                subscribed.add(track)
                report({"phase": "subscribed", "track": track})

            def on_restarted(reason: str, _namespace: tuple[str, ...], track: str = track) -> None:
                if not closing:
                    errors.append(f"{track} resubscribed unexpectedly: {reason}")

            watcher.want(
                track,
                namespace,
                track,
                WantHandlers(
                    on_subscribed=on_subscribed,
                    on_object=on_object,
                    on_error=lambda error, track=track: errors.append(f"{track}: {error!r}"),
                    on_subgroup_reset=lambda reset, track=track: errors.append(f"{track} reset: {reset.error}"),
                    on_restarted=on_restarted,
                ),
            )

        await wait_for(lambda: len(subscribed) == 2, "both wants subscribed")

        warm_up_writes = {"events": await warm_up("events", events), "status": await warm_up("status", status)}
        report({"phase": "warmed up", "writes": warm_up_writes})

        for payload in expected_events:
            await events.write(payload)

        await status.write(bytes([42]))
        await wait_for_count("events", 3)
        await wait_for_count("status", 1)
        report(
            {"phase": "received initial payloads", "events": len(received["events"]), "status": len(received["status"])}
        )
        # Make the relay send LARGEST_OBJECT by joining after content has arrived.
        late_subscriber = await connect_relay_subscriber(
            sub_endpoint, get_token=lambda: token, cancellation=lifetime, create_transport=create_transport
        )
        observe_closed("late subscriber", late_subscriber.closed)

        def on_late_ok(ok: Any) -> None:
            nonlocal late_largest_object
            late_largest_object = ok.parameters.largest_object
            if late_largest_object is None:
                errors.append("late SUBSCRIBE_OK omitted LARGEST_OBJECT")

        def on_late_object(obj: MoqtObject) -> None:
            if obj.status == "normal" and obj.payload != WARM_UP:
                received["lateStatus"].append(obj.payload)

        # Start at the next group, the next live object. Not Next Object: moq-relay 0.17.0 frames
        # LOCATION_FILTER the draft-20 way on moqt-22 too, and closes the session on draft-22's Next
        # Object spelling (spec/relay-interop.md, "Draft-22 LOCATION_FILTER framing"). A relative start
        # of 0 is the same bytes in both drafts.
        late_subscriber.subscribe(
            namespace,
            "status",
            RelaySubscriptionHandlers(
                on_subscribed=on_late_ok,
                on_object=on_late_object,
                on_error=lambda error: errors.append(f"late status: {error.error_code} {error.reason}"),
            ),
            parameters=MessageParameters(location_filter=RelativeGroupFilter(groups_before_next=0)),
        )
        await wait_for(lambda: late_largest_object is not None, "late SUBSCRIBE_OK with LARGEST_OBJECT")
        # The track's end would make its want resubscribe and replay the cached group (spec/publisher-recovery.md).
        watcher.unwant("events")
        events.close()
        await status.write(bytes([43]))
        await wait_for_count("status", 2)
        await wait_for_count("lateStatus", 1)
        if received["lateStatus"] != [bytes([43])]:
            raise RuntimeError("Late subscriber payload mismatch")

        if sorted(received["events"]) != sorted(expected_events) or received["status"] != [bytes([42]), bytes([43])]:
            raise RuntimeError(f"Payload mismatch: {received!r}")

        if errors:
            raise RuntimeError("; ".join(errors))

        # A clean publisher close withdraws the announce; the subscriber's discovery hears it. The want on
        # its last track goes first, so the publisher's done does not start a resubscription.
        watcher.unwant("status")
        closed_at = time.monotonic()
        closed_on_purpose.add("publisher")
        await publisher.close()
        await wait_for(lambda: "unannounced" in discovered, "discovery: unannounced")
        withdrawn_after_ms = round((time.monotonic() - closed_at) * 1000)
        report({"phase": "undiscovered", "afterMs": withdrawn_after_ms})
        if discovered != ["announced", "unannounced"]:
            raise RuntimeError(f"Discovery mismatch: {discovered!r}")

        # Assertion 7. A want following a supervised publisher's root moves to a newer generation that
        # announces while the old one is still up, the way a restarted publisher replaces a crashed one
        # whose announce the relay still holds, or a publisher rotates its connection. The move is made
        # before the break (issue #282): both generations beat one shared counter every 50 ms, the want
        # cuts over once the new generation's beat is within a few of the latest delivered (each
        # generation numbers its own groups, so the counter is the timeline), the old one drains for a
        # second, and no beat number is missing across the move.
        root = [*namespace, "supervised"]
        beats: list[tuple[str, int]] = []
        restarts: list[str] = []
        generation_switches: list[WantSwitchedInfo] = []

        def beat_of(obj: MoqtObject) -> tuple[str, int]:
            label, n = obj.payload.decode().split(":")
            return label, int(n)

        def on_beat(obj: MoqtObject) -> None:
            if obj.status == "normal":
                beats.append(beat_of(obj))

        def caught_up(obj: MoqtObject) -> bool:
            if not beats:
                return True
            latest = max(n for _, n in beats)
            return latest - 5 <= beat_of(obj)[1] <= latest + 1

        watcher.want(
            "beat",
            root,
            "beat",
            WantHandlers(
                on_object=on_beat,
                on_switched=generation_switches.append,
                on_restarted=lambda reason, _namespace: restarts.append(reason),
                on_error=lambda error: errors.append(f"beat: {error!r}"),
            ),
            generations=True,
            until=caught_up,
            drain=1.0,
        )

        generations: list[tuple[str, Any]] = []
        beating = True

        async def write_beat(label: str, writer: Any, n: int) -> None:
            try:
                await writer.write(f"{label}:{n}".encode())
            except RelayError as error:
                if error.code not in ("SUBSCRIPTION_CHANGED", "WRITE_TIMEOUT", "RECONNECTING"):
                    raise

        # One counter for every generation, written only where the relay wants it, so a generation nobody
        # watches yet never holds up the one being watched.
        async def beat_loop() -> None:
            n = 0
            while beating:
                await asyncio.gather(
                    *(write_beat(label, writer, n) for label, writer in list(generations) if writer.has_demand)
                )
                n += 1
                await asyncio.sleep(0.05)

        async def start_generation(label: str) -> SupervisedPublisher:
            supervised = await connect_supervised_publisher(
                endpoint,
                namespace=root,
                get_token=lambda: token,
                cancellation=lifetime,
                create_transport=create_transport,
            )
            supervised_publishers.append(supervised)
            generations.append((label, supervised.register_track("beat", write_timeout=2)))
            return supervised

        generation_switch: dict[str, Any] = {}
        beater = asyncio.ensure_future(beat_loop())
        try:
            first = await start_generation("first")
            assert first.generation is not None
            report({"phase": "first generation", "namespace": list(first.generation.namespace)})
            await wait_for(lambda: sum(1 for label, _ in beats if label == "first") >= 10, "first generation's beats")
            second = await start_generation("second")
            switch_started = time.monotonic()
            await wait_for(lambda: bool(generation_switches), "move to the second generation")
            switched_after_ms = round((time.monotonic() - switch_started) * 1000)
            at_cutover = len(beats)
            await wait_for(lambda: len(beats) >= at_cutover + 10, "second generation's beats")
            beating = False
            await beater
            if errors:
                raise RuntimeError("; ".join(errors))
            assert second.generation is not None
            move = generation_switches[0]
            if move.to_namespace != second.generation.namespace:
                raise RuntimeError(f"Want moved to {move.to_namespace!r}")
            numbers = sorted({n for _, n in beats})
            missing = [n for n in range(numbers[0], numbers[-1] + 1) if n not in numbers]
            labels = list(dict.fromkeys(label for label, _ in beats))
            if missing or len(generation_switches) != 1 or restarts or labels != ["first", "second"]:
                raise RuntimeError(
                    f"Generation move left a gap: missing {missing}, {len(generation_switches)} switches, "
                    f"restarts {restarts}, {beats}"
                )
            first_new = next(i for i, (label, _) in enumerate(beats) if label == "second")
            generation_switch = {
                "afterMs": switched_after_ms,
                "lastOld": beats[first_new - 1][1],
                "firstNew": beats[first_new][1],
                "beats": len(beats),
            }
            report({"phase": "followed newer generation", **generation_switch})
        finally:
            beating = False
            if not beater.done():
                beater.cancel()
            watcher.unwant("beat")
        if errors:
            raise RuntimeError("; ".join(errors))

        # Assertion 8. A switchable subscription moves from one layer to another make-before-break: the
        # layers carry the same frame numbers, one group per frame, and the frames the handlers receive
        # leave no gap across the cutover (issue #264). The frame number stands in for a media timestamp:
        # the switch waits for a new-layer frame within a few of the latest the old layer delivered, as a
        # player compares timestamps. The first object can be stale: the relay caches the layer's latest
        # group, and the publisher skips a layer nobody watches. A subscriber on another relay can also
        # lose the first object (issue #243). The drain delivers older frames still in flight on the old
        # layer: across relays groups arrive out of order, and cancelling at the cutover can cut one off.
        layer_namespace = [*namespace, "layers"]
        layer_publisher = await connect_relay_publisher(
            endpoint,
            namespace=layer_namespace,
            get_token=lambda: token,
            cancellation=lifetime,
            create_transport=create_transport,
        )
        observe_closed("layer publisher", layer_publisher.closed)
        layers = {track: layer_publisher.register_track(track, write_timeout=2) for track in ("video-lo", "video-hi")}
        # Subscribe once discovery reports the announce, so the relay cannot refuse it as unroutable.
        await layer_publisher.announced
        await wait_for(lambda: tuple(layer_namespace) in announced_namespaces, "discovery: layers announced")
        layer_errors: list[str] = []
        frames: list[tuple[str, int]] = []
        switches = 0
        writing_layers = True

        async def write_layer(writer: Any, payload: bytes) -> None:
            try:
                await writer.write(payload)
            except RelayError as error:
                if error.code not in ("SUBSCRIPTION_CHANGED", "WRITE_TIMEOUT"):
                    raise

        async def write_layers() -> None:
            frame_number = 0
            try:
                while writing_layers:
                    payload = str(frame_number).encode()
                    frame_number += 1
                    await asyncio.gather(
                        *(write_layer(writer, payload) for writer in layers.values() if writer.has_demand)
                    )
                    await asyncio.sleep(0.05)
            except Exception as error:
                layer_errors.append(f"layer write: {error!r}")

        def frame_of(obj: Any) -> int:
            return int(obj.payload.decode() or -1)

        def on_layer_object(obj: Any, track: str) -> None:
            if obj.status == "normal":
                frames.append((track, frame_of(obj)))

        def on_switched(_info: Any) -> None:
            nonlocal switches
            switches += 1

        layer_writes = asyncio.create_task(write_layers())
        switchable = subscriber.subscribe_switchable(
            layer_namespace,
            "video-lo",
            SwitchableHandlers(
                on_object=on_layer_object,
                on_switched=on_switched,
                on_error=lambda error, track: layer_errors.append(f"{track}: {error.error_code} {error.reason}"),
                on_subgroup_reset=lambda info, track: layer_errors.append(f"{track} reset group {info.group_id}"),
            ),
        )
        try:
            await switchable.subscribed
            await wait_for(lambda: len(frames) >= 10, "video-lo: 10 frames")

            def caught_up(obj: Any) -> bool:
                latest = max(n for _, n in frames)
                return latest - 5 <= frame_of(obj) <= latest + 1

            switch_started = time.monotonic()
            switched = await switchable.switch_to("video-hi", until=caught_up, drain=1.0)
            cutover_ms = round((time.monotonic() - switch_started) * 1000)
            at_cutover = len(frames)
            await wait_for(lambda: len(frames) >= at_cutover + 10, "video-hi: 10 frames")
            writing_layers = False
            await layer_writes
            if layer_errors:
                raise RuntimeError("; ".join(layer_errors))
            numbers = sorted({n for _, n in frames})
            missing = [n for n in range(numbers[0], numbers[-1] + 1) if n not in numbers]
            tracks = list(dict.fromkeys(track for track, _ in frames))
            if missing or switches != 1 or tracks != ["video-lo", "video-hi"]:
                raise RuntimeError(f"Layer switch left a gap: missing {missing}, {switches} switches, {frames}")
            first_hi = next(i for i, (track, _) in enumerate(frames) if track == "video-hi")
            layer_switch = {
                "cutoverMs": cutover_ms,
                "from": switched.from_track,
                "to": switched.to_track,
                "lastOld": frames[first_hi - 1][1],
                "firstNew": frames[first_hi][1],
                "frames": len(frames),
            }
            report({"phase": "switched layers", **layer_switch})
        finally:
            writing_layers = False
            switchable.cancel()
            if not layer_writes.done():
                layer_writes.cancel()

        # Assertion 9. A room-rooted endpoint (issue #266), built with room_endpoint (issue #348): the
        # endpoint's path is the run's namespace and its connections name namespaces relative to it. A publisher
        # on each form, a subscriber on each form subscribing to the other's track, and discovery on each form
        # must agree: the relay resolves `<endpoint>/<room>` + `['pair', 'rooted']` to what `<endpoint>` +
        # `[*room, 'pair', 'rooted']` addresses. Each publisher writes a counter while it has demand, so an
        # object the relay's first hop drops (issue #243) is followed by another. An absolute namespace on a
        # rooted connection addresses a namespace below the room, not the run's own, so the relay refuses its
        # subscription as unroutable and it never gets the bare publisher's objects.
        room = namespace

        def rooted(url: str) -> str:
            return room_endpoint(url, room)

        room_publisher = await connect_relay_publisher(
            rooted(endpoint),
            namespace=["pair", "rooted"],
            get_token=lambda: token,
            cancellation=lifetime,
            create_transport=create_transport,
        )
        room_publishers.append(room_publisher)
        observe_closed("room publisher", room_publisher.closed)
        bare_pair_publisher = await connect_relay_publisher(
            endpoint,
            namespace=[*room, "pair", "bare"],
            get_token=lambda: token,
            cancellation=lifetime,
            create_transport=create_transport,
        )
        room_publishers.append(bare_pair_publisher)
        observe_closed("bare pair publisher", bare_pair_publisher.closed)
        room_subscriber = await connect_relay_subscriber(
            rooted(sub_endpoint), get_token=lambda: token, cancellation=lifetime, create_transport=create_transport
        )
        observe_closed("room subscriber", room_subscriber.closed)
        report({"phase": "room-rooted connections", "publisher": rooted(endpoint), "subscriber": rooted(sub_endpoint)})
        room_discovered: set[tuple[str, ...]] = set()
        room_discovery = room_subscriber.namespace_events(["pair"], cancellation=lifetime)

        async def discover_room() -> None:
            try:
                async with room_discovery:
                    async for event in room_discovery:
                        if event.type == "announced":
                            room_discovered.add(tuple(event.namespace))
            except Exception as error:
                if not closing:
                    errors.append(f"room discovery: {error!r}")

        room_discovery_task = asyncio.create_task(discover_room())
        pair_writers = [publisher.register_track("pair", write_timeout=2) for publisher in room_publishers]
        writing_pairs = True

        async def write_pairs() -> None:
            n = 0
            try:
                while writing_pairs:
                    await asyncio.gather(
                        *(write_layer(writer, str(n).encode()) for writer in pair_writers if writer.has_demand)
                    )
                    n += 1
                    await asyncio.sleep(0.1)
            except Exception as error:
                errors.append(f"pair write: {error!r}")

        pair_writes = asyncio.create_task(write_pairs())
        pair_started = time.monotonic()
        try:
            await room_publisher.announced
            await bare_pair_publisher.announced
            # Each form's discovery names both publishers, the room's relative to its path.
            await wait_for(
                lambda: {(*room, "pair", "rooted"), (*room, "pair", "bare")} <= announced_namespaces,
                "bare discovery: both pair publishers",
            )
            await wait_for(
                lambda: {("pair", "rooted"), ("pair", "bare")} <= room_discovered,
                "room discovery: both pair publishers",
            )
            discovered_after_ms = round((time.monotonic() - pair_started) * 1000)
            report({"phase": "room-rooted discovery", "afterMs": discovered_after_ms})
            pair_objects = {"roomFromBare": 0, "bareFromRoom": 0, "escape": 0}
            escape_refused: str | None = None

            def count_pair(label: str) -> Any:
                def on_object(obj: MoqtObject) -> None:
                    if obj.status == "normal":
                        pair_objects[label] += 1

                return on_object

            def on_escape_error(error: Any) -> None:
                nonlocal escape_refused
                escape_refused = f"{error.error_code} {error.reason}"

            pair_subscriptions = [
                room_subscriber.subscribe(
                    ["pair", "bare"],
                    "pair",
                    RelaySubscriptionHandlers(
                        on_object=count_pair("roomFromBare"),
                        on_error=lambda error: errors.append(f"room subscriber: {error.error_code} {error.reason}"),
                    ),
                ),
                subscriber.subscribe(
                    [*room, "pair", "rooted"],
                    "pair",
                    RelaySubscriptionHandlers(
                        on_object=count_pair("bareFromRoom"),
                        on_error=lambda error: errors.append(f"bare subscriber: {error.error_code} {error.reason}"),
                    ),
                ),
                room_subscriber.subscribe(
                    [*room, "pair", "bare"],
                    "pair",
                    RelaySubscriptionHandlers(on_object=count_pair("escape"), on_error=on_escape_error),
                ),
            ]
            try:
                await wait_for(
                    lambda: pair_objects["roomFromBare"] >= 3 and pair_objects["bareFromRoom"] >= 3,
                    "pair objects across the endpoint forms",
                )
                # The absolute namespace's request must have been answered, not just outrun by the others.
                await wait_for(
                    lambda: escape_refused is not None, "refusal of the rooted connection's absolute namespace"
                )
            finally:
                for subscription in pair_subscriptions:
                    subscription.cancel()
            if pair_objects["escape"]:
                raise RuntimeError(
                    f"A rooted connection's absolute namespace reached the room: {pair_objects['escape']} objects"
                )
            room_pair = {
                "roomPath": urlsplit(rooted(endpoint)).path,
                "discoveredAfterMs": discovered_after_ms,
                "roomFromBare": pair_objects["roomFromBare"],
                "bareFromRoom": pair_objects["bareFromRoom"],
                "absoluteOnRooted": escape_refused,
            }
            report({"phase": "room-rooted pair", **room_pair})
        finally:
            writing_pairs = False
            await pair_writes
            room_discovery_task.cancel()
        if errors:
            raise RuntimeError("; ".join(errors))

        return {
            "endpoint": endpoint,
            "subscriberEndpoint": sub_endpoint,
            "namespace": namespace,
            "events": len(received["events"]),
            "status": len(received["status"]),
            "warmUpWrites": warm_up_writes,
            "binaryAndEmptyPayloads": "passed",
            "independentTrackClose": "passed",
            "lateSubscriberLargestObject": {
                "group": late_largest_object.group,
                "object": late_largest_object.object,
            }
            if late_largest_object is not None
            else None,
            "discovery": {"announced": "passed", "withdrawnAfterMs": withdrawn_after_ms},
            "generationSwitch": generation_switch,
            "layerSwitch": layer_switch,
            "roomPair": room_pair,
        }
    finally:
        closing = True
        deadline.cancel()
        if discovery_task is not None:
            discovery_task.cancel()

        if watcher is not None:
            watcher.close()

        try:
            closing_publishers = [supervised.close() for supervised in supervised_publishers]
            if publisher is not None:
                closing_publishers.append(publisher.close())
            if layer_publisher is not None:
                closing_publishers.append(layer_publisher.close())
            closing_publishers.extend(room_publisher.close() for room_publisher in room_publishers)
            await asyncio.gather(*closing_publishers)
        finally:
            if subscriber is not None:
                subscriber.close()
            if late_subscriber is not None:
                late_subscriber.close()
            if room_subscriber is not None:
                room_subscriber.close()


def main() -> int:
    endpoint = read_endpoint("RELAY_URL", DEFAULT_ENDPOINT)
    sub_endpoint = read_endpoint("RELAY_SUB_URL", endpoint)
    token = read_token()
    prefix = [field for field in os.environ.get("RELAY_NAMESPACE_PREFIX", "relay-sdk-smoke").split("/") if field]
    namespace = [*prefix, str(uuid.uuid4())]

    def report(value: Any) -> None:
        print(redact(json.dumps(value) if not isinstance(value, str) else value, token), flush=True)

    try:
        result = asyncio.run(run(endpoint, sub_endpoint, token, namespace, report))
    except BaseException as error:
        report({"result": "FAIL", "error": redact(str(error) or type(error).__name__, token)})
        return 1

    report({"result": "PASS", **result, "connections": "closed"})
    return 0


if __name__ == "__main__":
    sys.exit(main())
