"""The dropped-FIN retry driven without a network: real aioquic senders in each state the sweep must tell apart."""

import asyncio

from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection
from aioquic.quic.packet_builder import QuicDeliveryState
from aioquic.quic.stream import QuicStream
from relay_helpers.quic_pair import QuicPair

from caprid_relay.quic._aioquic import fin_unsent, live_stream
from caprid_relay.quic._fin_retry import DroppedFinRetry


class Harness:
    """A client connection that never sends a packet, so every stream stays exactly where the test puts it."""

    def __init__(self, interval: float = 0.01) -> None:
        self.quic = QuicConnection(configuration=QuicConfiguration(is_client=True))
        self.transmits = 0
        self.retry = DroppedFinRetry(asyncio.get_running_loop(), self.quic, self.transmit, interval=interval)

    def transmit(self) -> None:
        self.transmits += 1

    def fin_only_stream(self) -> int:
        """A unidirectional stream whose FIN is queued and not yet consumed."""
        stream_id = self.quic.get_next_available_stream_id(is_unidirectional=True)
        self.quic.send_stream_data(stream_id, b"", end_stream=True)
        return stream_id

    def stream(self, stream_id: int) -> QuicStream:
        stream = live_stream(self.quic, stream_id)
        assert stream is not None
        return stream

    def consume(self, stream_id: int) -> None:
        """aioquic takes the FIN frame for a packet, whether or not that packet then had room for it."""
        frame = self.stream(stream_id).sender.get_frame(1200)
        assert frame is not None and frame.fin

    def acknowledge_fin(self, stream_id: int) -> None:
        self.stream(stream_id).sender.on_data_delivery(QuicDeliveryState.ACKED, 0, 0, True)


class TestDroppedFinRetry:
    async def test_a_consumed_fin_is_re_armed_and_a_transmit_follows(self) -> None:
        harness = Harness()
        stream_id = harness.fin_only_stream()
        harness.retry.watch(stream_id)
        harness.consume(stream_id)
        assert fin_unsent(harness.stream(stream_id), set())

        harness.retry.sweep()

        assert not fin_unsent(harness.stream(stream_id), set())  # queued again
        assert harness.stream(stream_id).sender._pending_eof is True
        assert harness.transmits == 1
        assert not harness.retry.idle  # until the FIN is acknowledged
        harness.retry.stop()

    async def test_a_fin_still_queued_is_left_alone(self) -> None:
        harness = Harness()
        stream_id = harness.fin_only_stream()
        harness.retry.watch(stream_id)

        harness.retry.sweep()

        assert harness.transmits == 0
        assert harness.stream(stream_id).sender._pending_eof is True
        assert not harness.retry.idle
        harness.retry.stop()

    async def test_an_acknowledged_fin_is_forgotten_and_the_sweep_goes_idle(self) -> None:
        harness = Harness()
        stream_id = harness.fin_only_stream()
        harness.retry.watch(stream_id)
        harness.consume(stream_id)
        harness.acknowledge_fin(stream_id)

        harness.retry.sweep()

        assert harness.transmits == 0
        assert harness.retry.idle

    async def test_a_reset_stream_is_forgotten(self) -> None:
        harness = Harness()
        stream_id = harness.fin_only_stream()
        harness.retry.watch(stream_id)
        harness.consume(stream_id)
        harness.stream(stream_id).sender.reset(0)

        harness.retry.sweep()

        assert harness.transmits == 0
        assert harness.stream(stream_id).sender._pending_eof is False
        assert harness.retry.idle

    async def test_a_stream_aioquic_discarded_is_forgotten(self) -> None:
        harness = Harness()
        harness.retry.watch(102)  # never created: what a discarded stream looks like from here

        harness.retry.sweep()

        assert harness.transmits == 0
        assert harness.retry.idle

    async def test_one_sweep_re_arms_every_dropped_fin_with_one_transmit(self) -> None:
        harness = Harness()
        first, second = harness.fin_only_stream(), harness.fin_only_stream()

        for stream_id in (first, second):
            harness.retry.watch(stream_id)
            harness.consume(stream_id)

        harness.retry.sweep()

        assert harness.transmits == 1
        assert not fin_unsent(harness.stream(first), set()) and not fin_unsent(harness.stream(second), set())
        harness.retry.stop()

    async def test_a_fin_in_a_packet_in_flight_is_left_to_loss_recovery(self) -> None:
        # A dead link (#160): the FIN left in a packet that is never acknowledged. Re-arming it would put
        # FIN-only frames into every probe packet and starve the data of the write waiting behind them.
        pair = QuicPair()
        transmits: list[None] = []
        retry = DroppedFinRetry(asyncio.get_running_loop(), pair.client, lambda: transmits.append(None))
        [in_flight] = pair.open_streams(1, 100, fin=True)
        retry.watch(in_flight)
        pair.send_unpaced()  # sent, never delivered
        # One FIN aioquic consumed without a packet, the bug the sweep exists for.
        [dropped] = pair.open_streams(1, 0, fin=True)
        retry.watch(dropped)
        frame = pair.client._streams[dropped].sender.get_frame(1200)
        assert frame is not None and frame.fin

        retry.sweep()

        assert pair.client._streams[in_flight].sender._pending_eof is False  # left alone
        assert pair.client._streams[dropped].sender._pending_eof is True  # re-armed
        assert len(transmits) == 1
        retry.stop()

    async def test_stop_cancels_the_sweep_and_ignores_later_watches(self) -> None:
        harness = Harness()
        stream_id = harness.fin_only_stream()
        harness.retry.watch(stream_id)
        harness.consume(stream_id)

        harness.retry.stop()
        assert harness.retry.idle
        harness.retry.watch(stream_id)
        assert harness.retry.idle
        harness.retry.sweep()

        assert harness.transmits == 0
        assert fin_unsent(harness.stream(stream_id), set())  # nothing re-armed after stop

    async def test_the_timer_runs_the_sweep_at_the_interval(self) -> None:
        harness = Harness(interval=0.01)
        stream_id = harness.fin_only_stream()
        harness.retry.watch(stream_id)
        harness.consume(stream_id)

        await asyncio.sleep(0.05)

        assert harness.transmits >= 1
        assert harness.stream(stream_id).sender._pending_eof is True
        harness.retry.stop()
        assert harness.retry.idle
