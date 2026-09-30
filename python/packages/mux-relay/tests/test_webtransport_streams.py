"""The stream halves in process: the ``moq_core.transport`` stream contract over both adapters, then the halves alone.

The contract tests run twice, over ``core_helpers.streams.Pipe`` (the in-memory adapter the core and product
tests run on) and over a ``_SendHalf`` connected to a ``_ReceiveHalf`` through ``relay_helpers.stream_link``,
so both adapters are held to the same expectations. What differs between them is tested on the halves alone:
a write settles when the link transmits, not when the peer reads; the transmit waiter is shared by concurrent
writers and shielded from their cancellation; an abort after a transmitted FIN is a no-op; and the link's own
failure reaches every half.
"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import pytest
from core_helpers.streams import Pipe, StreamClosedError
from relay_helpers.stream_link import StreamLink

from moq_core.transport import ReceiveStream, SendStream
from mux_relay.webtransport import WebTransportError
from mux_relay.webtransport._errors import StreamStopped
from mux_relay.webtransport._streams import _StreamQueue

STREAM_ID = 4


@dataclass
class Pair:
    writer: SendStream
    reader: ReceiveStream
    closed_error: type[Exception]
    hold: Callable[[], None]
    """Stop deliveries, so a write stays pending."""
    settle: Callable[[], Awaitable[None]]
    """Let whatever is queued cross to the other side."""


def pipe_pair() -> Pair:
    pipe = Pipe()

    async def settle() -> None:
        await asyncio.sleep(0)

    return Pair(writer=pipe, reader=pipe, closed_error=StreamClosedError, hold=lambda: None, settle=settle)


def halves_pair() -> Pair:
    link = StreamLink(auto=True)
    send, receive = link.pair(STREAM_ID)

    def hold() -> None:
        link.auto = False

    async def settle() -> None:
        link.transmit()
        await asyncio.sleep(0)

    return Pair(writer=send, reader=receive, closed_error=WebTransportError, hold=hold, settle=settle)


@pytest.fixture(params=[pipe_pair, halves_pair], ids=["pipe", "halves"])
def pair(request: pytest.FixtureRequest) -> Pair:
    return request.param()


async def read_all(reader: ReceiveStream) -> list[bytes]:
    chunks: list[bytes] = []

    while (chunk := await reader.read()) is not None:
        chunks.append(chunk)

    return chunks


class TestStreamContract:
    """What ``moq_core.transport`` promises of every adapter, checked on both."""

    async def test_chunks_arrive_in_order_and_writes_settle_once_accepted(self, pair: Pair) -> None:
        async def produce() -> None:
            await pair.writer.write(b"one")
            await pair.writer.write(b"two")
            await pair.writer.close()

        _, chunks = await asyncio.wait_for(asyncio.gather(produce(), read_all(pair.reader)), 2)
        assert chunks == [b"one", b"two"]

    async def test_read_keeps_returning_none_after_the_fin(self, pair: Pair) -> None:
        closing = asyncio.ensure_future(pair.writer.close())
        assert await asyncio.wait_for(pair.reader.read(), 2) is None
        assert await asyncio.wait_for(pair.reader.read(), 2) is None
        await closing

    async def test_an_empty_write_carries_nothing(self, pair: Pair) -> None:
        async def produce() -> None:
            await pair.writer.write(b"")
            await pair.writer.write(b"x")
            await pair.writer.close()

        _, chunks = await asyncio.wait_for(asyncio.gather(produce(), read_all(pair.reader)), 2)
        assert chunks == [b"x"]

    async def test_write_and_close_raise_once_closed(self, pair: Pair) -> None:
        await asyncio.wait_for(pair.writer.close(), 2)

        with pytest.raises(pair.closed_error):
            await pair.writer.write(b"x")

        with pytest.raises(pair.closed_error):
            await pair.writer.close()

    async def test_abort_fails_the_reader_and_refuses_later_writes(self, pair: Pair) -> None:
        pair.writer.abort()
        await pair.settle()

        with pytest.raises(pair.closed_error):
            await asyncio.wait_for(pair.reader.read(), 2)

        with pytest.raises(pair.closed_error):
            await pair.writer.write(b"x")

    async def test_a_pending_write_fails_when_the_writer_aborts(self, pair: Pair) -> None:
        pair.hold()
        pending = asyncio.ensure_future(pair.writer.write(b"\0" * 1024))
        await asyncio.sleep(0)
        assert not pending.done()

        pair.writer.abort()

        with pytest.raises(pair.closed_error):
            await asyncio.wait_for(pending, 2)

    async def test_cancel_ends_reads_and_stops_the_writer(self, pair: Pair) -> None:
        pair.reader.cancel()
        assert await asyncio.wait_for(pair.reader.read(), 2) is None
        await pair.settle()

        with pytest.raises(pair.closed_error):
            await pair.writer.write(b"x")

    async def test_abort_after_the_fin_is_harmless(self, pair: Pair) -> None:
        await asyncio.wait_for(pair.writer.close(), 2)
        await pair.settle()
        pair.writer.abort()
        assert await asyncio.wait_for(pair.reader.read(), 2) is None


class TestSendHalf:
    async def test_a_write_settles_only_when_the_link_transmits(self) -> None:
        link = StreamLink()
        send, receive = link.pair(STREAM_ID)
        pending = asyncio.ensure_future(send.write(b"abc"))
        await asyncio.sleep(0)
        assert not pending.done()
        assert send.pending

        link.transmit()
        await asyncio.wait_for(pending, 1)
        assert not send.pending
        assert await receive.read() == b"abc"

    async def test_concurrent_writers_share_the_transmit_and_a_cancelled_one_does_not_poison_it(self) -> None:
        # The in-process form of the loopback test of the same name: giving up on a blocked write must not
        # cancel the future that concurrent and later writers on the stream share.
        link = StreamLink()
        send, receive = link.pair(STREAM_ID)
        victim = asyncio.ensure_future(asyncio.wait_for(send.write(b"\1" * 8), 0.01))
        await asyncio.sleep(0)
        second = asyncio.ensure_future(send.write(b"tail"))

        with pytest.raises(TimeoutError):
            await victim

        await asyncio.sleep(0)
        assert not second.done()

        link.transmit()
        await asyncio.wait_for(second, 1)
        closing = asyncio.ensure_future(send.close())
        await asyncio.sleep(0)
        link.transmit()
        await asyncio.wait_for(closing, 1)
        assert await read_all(receive) == [b"\1" * 8, b"tail"]

    async def test_abort_after_the_fin_went_out_sends_no_reset(self) -> None:
        link = StreamLink()
        send, receive = link.pair(STREAM_ID)
        closing = asyncio.ensure_future(send.close())
        await asyncio.sleep(0)
        link.transmit()
        await closing

        send.abort(WebTransportError("too late", code=9))
        assert link.resets == []
        assert await receive.read() is None

    async def test_abort_during_a_pending_fin_fails_the_close_with_the_reason_and_resets(self) -> None:
        link = StreamLink()
        send, receive = link.pair(STREAM_ID)
        closing = asyncio.ensure_future(send.close())
        await asyncio.sleep(0)
        reason = WebTransportError("giving up", code=7)

        send.abort(reason)

        with pytest.raises(WebTransportError) as failure:
            await closing

        assert failure.value is reason
        assert link.resets == [(STREAM_ID, 7)]
        link.transmit()

        with pytest.raises(WebTransportError) as peer:
            await receive.read()

        assert peer.value.code == 7

    async def test_abort_with_a_plain_reason_fails_the_pending_write_generically_and_maps_the_code(self) -> None:
        link = StreamLink()
        send, _ = link.pair(STREAM_ID)
        pending = asyncio.ensure_future(send.write(b"x"))
        await asyncio.sleep(0)

        send.abort(12)

        with pytest.raises(WebTransportError, match="aborted"):
            await pending

        assert link.resets == [(STREAM_ID, 12)]

    async def test_abort_with_no_usable_reason_resets_with_code_zero(self) -> None:
        link = StreamLink()
        send, _ = link.pair(STREAM_ID)
        send.abort(object())
        assert link.resets == [(STREAM_ID, 0)]

    async def test_a_stopped_stream_fails_the_pending_write_and_refuses_later_ones_with_the_peers_code(self) -> None:
        link = StreamLink()
        send, _ = link.pair(STREAM_ID)
        pending = asyncio.ensure_future(send.write(b"x"))
        await asyncio.sleep(0)

        send.stopped(9)

        with pytest.raises(StreamStopped) as failure:
            await pending

        assert failure.value.code == 9

        with pytest.raises(StreamStopped) as refused:
            await send.write(b"y")

        assert refused.value.code == 9

    async def test_the_peer_cancelling_reaches_the_send_half_as_a_stop(self) -> None:
        link = StreamLink()
        send, receive = link.pair(STREAM_ID)
        pending = asyncio.ensure_future(send.write(b"x"))
        await asyncio.sleep(0)

        receive.cancel(WebTransportError("enough", code=3))
        assert link.stops == [(STREAM_ID, 3)]
        link.transmit()

        with pytest.raises(WebTransportError) as failure:
            await pending

        assert failure.value.code == 3

    async def test_a_dead_link_fails_the_pending_write_and_every_later_operation(self) -> None:
        link = StreamLink()
        send, receive = link.pair(STREAM_ID)
        pending = asyncio.ensure_future(send.write(b"x"))
        await asyncio.sleep(0)
        error = WebTransportError("QUIC connection terminated", code=1)

        link.end(error)

        with pytest.raises(WebTransportError) as failure:
            await pending

        assert failure.value is error

        with pytest.raises(WebTransportError) as later:
            await send.write(b"y")

        assert later.value is error

        with pytest.raises(WebTransportError) as reading:
            await receive.read()

        assert reading.value is error

    async def test_a_link_failure_still_fails_a_pending_fin(self) -> None:
        link = StreamLink()
        send, _ = link.pair(STREAM_ID)
        closing = asyncio.ensure_future(send.close())
        await asyncio.sleep(0)
        error = WebTransportError("gone")

        link.end(error)

        with pytest.raises(WebTransportError) as failure:
            await closing

        assert failure.value is error
        assert send.closed

    async def test_a_closing_session_refuses_new_writes_before_touching_the_stream(self) -> None:
        link = StreamLink()
        send, _ = link.pair(STREAM_ID)
        link.error = WebTransportError("WebTransport session closed")

        with pytest.raises(WebTransportError, match="session closed"):
            await send.write(b"x")

        assert link.transmits == 0


class TestReceiveHalf:
    async def test_buffered_chunks_are_read_before_a_reset_is_reported(self) -> None:
        link = StreamLink()
        send, receive = link.pair(STREAM_ID)
        writing = asyncio.ensure_future(send.write(b"head"))
        await asyncio.sleep(0)
        link.transmit()
        await writing

        receive.fail(WebTransportError("peer reset the stream (code 5)", code=5))
        assert receive.ended
        assert await receive.read() == b"head"

        with pytest.raises(WebTransportError) as failure:
            await receive.read()

        assert failure.value.code == 5

    async def test_a_retransmitted_fin_is_ignored(self) -> None:
        link = StreamLink()
        _, receive = link.pair(STREAM_ID)
        receive.feed(b"a", True)
        receive.feed(b"", True)
        receive.feed(b"late", True)
        assert receive.ended
        assert await read_all(receive) == [b"a"]
        assert await receive.read() is None

    async def test_nothing_arrives_after_cancel(self) -> None:
        link = StreamLink()
        _, receive = link.pair(STREAM_ID)
        receive.feed(b"a", False)

        receive.cancel()
        receive.feed(b"b", False)

        assert receive.cancelled
        assert not receive.ended
        assert await receive.read() is None
        assert link.stops == [(STREAM_ID, 0)]

    async def test_a_failure_after_the_fin_is_ignored(self) -> None:
        link = StreamLink()
        _, receive = link.pair(STREAM_ID)
        receive.feed(b"", True)
        receive.fail(WebTransportError("late"))
        assert await receive.read() is None

    async def test_a_waiting_read_wakes_when_data_arrives(self) -> None:
        link = StreamLink()
        _, receive = link.pair(STREAM_ID)
        reading = asyncio.ensure_future(receive.read())
        await asyncio.sleep(0)
        assert not reading.done()

        receive.feed(b"late", False)
        assert await asyncio.wait_for(reading, 1) == b"late"


class TestStreamQueue:
    async def test_yields_pushed_items_in_order_and_ends_on_close(self) -> None:
        queue: _StreamQueue = _StreamQueue()
        queue.push(1)
        queue.push(2)

        async def drain() -> list[int]:
            return [item async for item in queue]

        draining = asyncio.ensure_future(drain())
        await asyncio.sleep(0)
        queue.push(3)
        queue.close()
        assert await asyncio.wait_for(draining, 1) == [1, 2, 3]

    async def test_a_push_after_close_is_dropped(self) -> None:
        queue = _StreamQueue()
        queue.close()
        queue.push(1)
        assert [item async for item in queue] == []
