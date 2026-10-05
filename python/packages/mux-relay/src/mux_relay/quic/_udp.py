"""The dialing end's UDP socket: bound unconnected, and hearing an ICMP port unreachable for the handshake only (#163).

A connected UDP socket would report a refusal, but it also pins the local source address, so a
client whose own address changes (a network switch, a DHCP renew) could no longer send from the new
one and let the relay migrate. The client socket therefore stays unconnected. On Linux,
``IP_RECVERR`` (and ``IPV6_RECVERR`` for an IPv6 peer) makes the kernel record ICMP errors on an
unconnected socket too: the next receive raises ``ConnectionRefusedError``, which asyncio hands to
``QuicProtocol.error_received``. Each report also waits on the socket's error queue, and a
non-empty queue keeps the socket polling as errored, so the event loop's reader would spin until it
is read: the protocol drains it whenever it hears an error, and turns the option off (which discards
the queue) on every way out of the handshake. Python names neither constant, and other platforms
(macOS) have no equivalent, so there a closed port still waits out the connect deadline.
"""

from __future__ import annotations

import socket
import sys
from collections.abc import Sequence
from typing import Any

from ._errors import _session_error

_MSG_ERRQUEUE = getattr(socket, "MSG_ERRQUEUE", 0x2000)

_LINUX_RECVERR = ((socket.IPPROTO_IP, 11), (socket.IPPROTO_IPV6, 25))
"""``(level, IP_RECVERR)`` and ``(level, IPV6_RECVERR)`` from Linux ``<linux/in.h>`` / ``<linux/in6.h>``.

An IPv4-mapped peer on a dual-stack socket reports through the IPv4 option, a native IPv6 peer through
the IPv6 one.
"""


def _options(sock: Any) -> tuple[tuple[int, int], ...]:
    if not sys.platform.startswith("linux"):
        return ()

    return _LINUX_RECVERR if sock.family == socket.AF_INET6 else _LINUX_RECVERR[:1]


def report_refusals(sock: Any, on: bool) -> bool:
    """Turn the kernel's ICMP error reports on this UDP socket on or off; ``False`` where it cannot."""
    options = _options(sock)

    for level, option in options:
        try:
            sock.setsockopt(level, option, int(on))
        except OSError:
            return False

    return bool(options)


def drain_errors(sock: Any) -> None:
    """Read every queued ICMP error report, so the socket stops polling as errored."""
    if not _options(sock):
        return

    # asyncio's socket wrapper has no recvmsg; borrow the descriptor without taking it over.
    raw = socket.socket(sock.family, sock.type, fileno=sock.fileno())

    try:
        while True:
            try:
                raw.recvmsg(1, 1024, _MSG_ERRQUEUE | socket.MSG_DONTWAIT)
            except OSError:  # BlockingIOError once the queue is empty
                return
    finally:
        raw.detach()


def reports_refusals(sock: Any) -> bool:
    """Whether the socket currently has the kernel's ICMP error reports on."""
    return any(sock.getsockopt(level, option) for level, option in _options(sock))


def open_socket(addresses: Sequence[tuple[Any, ...]]) -> tuple[socket.socket, tuple[Any, ...]]:
    """Bind a UDP socket and pick the resolved address it can reach.

    Like aioquic's client: a dual-stack IPv6 socket when the host offers one (IPv4 addresses are
    mapped onto it), otherwise an IPv4 socket with the first IPv4 address the resolver returned.
    The socket stays unconnected, so the kernel picks the source address per packet and the relay can
    migrate the connection when the client's address changes. Until the handshake completes it
    reports ICMP errors (see above), so a closed port fails the connect at once.
    """
    if not addresses:
        raise _session_error("no addresses to connect to")

    try:
        sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        try:
            sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
            sock.bind(("::", 0, 0, 0))
        except OSError:
            sock.close()
            raise
    except OSError:
        ipv4 = [address for address in addresses if len(address) == 2]

        if not ipv4:
            raise _session_error("the host resolves only to IPv6 addresses, but IPv6 sockets are unavailable") from None

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("0.0.0.0", 0))
        report_refusals(sock, True)
        return sock, ipv4[0]

    address = addresses[0]

    if len(address) == 2:
        address = ("::ffff:" + address[0], address[1], 0, 0)

    report_refusals(sock, True)
    return sock, address
