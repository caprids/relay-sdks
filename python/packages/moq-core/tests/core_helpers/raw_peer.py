"""Raw peer-initiated request streams for publish-side tests.

The relay's half of the announce-and-serve flow, which the subscribe driver does not initiate.
:func:`open_raw_request` writes one request message and then records every control frame the
publisher sends back, plus whether the publisher FINed its side; :func:`solicit_namespace` and
:func:`raw_subscribe` wrap it with the two messages moq-lite-rs sends (SUBSCRIBE_NAMESPACE right
after SETUP, SUBSCRIBE per pulled track).
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass, field

from moq_core.control_messages import (
    ControlMessage,
    ControlMessageDeframer,
    MessageParameters,
    NextObjectFilter,
    RelativeGroupFilter,
    Subscribe,
    SubscribeNamespace,
    decode_control_message,
    encode_subscribe,
    encode_subscribe_namespace,
)
from moq_core.transport import MoqtTransport, ReceiveStream, SendStream


@dataclass(slots=True)
class RawRequest:
    received: list[ControlMessage] = field(default_factory=list)
    """Control frames the publisher wrote back, in order."""
    raw: bytearray = field(default_factory=bytearray)
    """The exact bytes the publisher wrote back, before its FIN."""
    _ended: bool = False
    _failure: BaseException | None = None
    _writer: SendStream | None = None
    _reader: ReceiveStream | None = None

    def ended(self) -> bool:
        """True once the publisher FINed (or reset) its side."""
        return self._ended

    def failure(self) -> BaseException | None:
        """The decode/stream error that ended the read loop, if any.

        The helper exists for byte-precise assertions, so a malformed frame must surface here instead
        of masquerading as a clean FIN.
        """
        return self._failure

    def kinds(self) -> list[str]:
        return [message.kind for message in self.received]

    async def send(self, data: bytes) -> None:
        """Write a follow-up frame on the request stream (e.g. a GOAWAY)."""
        assert self._writer is not None
        try:
            await self._writer.write(data)
        except Exception:
            pass

    def abandon_reads(self) -> None:
        """Reset only the response direction (stop reading) while keeping the request half open."""
        assert self._reader is not None
        self._reader.cancel()

    async def fin(self) -> None:
        """FIN the peer's side: half-closure, NOT a withdrawal (§3.3.2)."""
        assert self._writer is not None
        try:
            await self._writer.close()
        except Exception:
            pass

    def reset(self, reason: object = None) -> None:
        """Reset the peer's side: the actual withdrawal signal. An exception ``reason`` is what the reader raises."""
        assert self._writer is not None
        self._writer.abort(reason)


async def open_raw_request(server: MoqtTransport, message: bytes) -> RawRequest:
    stream = await server.create_bidirectional_stream()
    await stream.writable.write(message)
    request = RawRequest(_writer=stream.writable, _reader=stream.readable)

    async def read_loop() -> None:
        deframer = ControlMessageDeframer()

        try:
            while True:
                chunk = await stream.readable.read()
                if chunk is None:
                    break

                request.raw.extend(chunk)
                for frame in deframer.push(chunk):
                    request.received.append(decode_control_message(frame))
        except Exception as error:
            request._failure = error
        finally:
            request._ended = True

    asyncio.get_running_loop().create_task(read_loop())
    return request


def solicit_namespace(server: MoqtTransport, prefix: Sequence[str], request_id: int = 1) -> asyncio.Future[RawRequest]:
    """The relay's opening move: solicit announces for a namespace prefix."""
    return asyncio.ensure_future(
        open_raw_request(
            server,
            encode_subscribe_namespace(SubscribeNamespace(request_id=request_id, track_namespace_prefix=list(prefix))),
        )
    )


def raw_subscribe(
    server: MoqtTransport, track_namespace: Sequence[str], track_name: str, request_id: int
) -> asyncio.Future[RawRequest]:
    """The relay's per-track pull, with the parameters moq-relay 0.14.17 and 0.15.8 send on draft-20.

    Next Object plus a current-group fill; our publisher advertises no cached content.
    """
    return asyncio.ensure_future(
        open_raw_request(
            server,
            encode_subscribe(
                Subscribe(
                    request_id=request_id,
                    track_namespace=list(track_namespace),
                    track_name=track_name,
                    parameters=MessageParameters(
                        forward=1,
                        subscriber_priority=0,
                        location_filter=NextObjectFilter(),
                        fill_parameters=MessageParameters(location_filter=RelativeGroupFilter(groups_before_next=1)),
                        group_order="descending",
                    ),
                )
            ),
        )
    )
