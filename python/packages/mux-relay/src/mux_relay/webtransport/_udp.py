"""Hearing an ICMP port unreachable on an unconnected UDP socket, for the handshake only (#163).

A connected UDP socket would report a refusal, but it also pins the local source address, so a
client whose own address changes (a network switch, a DHCP renew) could no longer send from the new
one and let the relay migrate. The client socket therefore stays unconnected. On Linux,
``IP_RECVERR`` (and ``IPV6_RECVERR`` for an IPv6 peer) makes the kernel record ICMP errors on an
unconnected socket too: the next receive raises ``ConnectionRefusedError``, which asyncio hands to
``WebTransportProtocol.error_received``. Each report also waits on the socket's error queue, and a
non-empty queue keeps the socket polling as errored, so the event loop's reader would spin until it
is read: the protocol drains it whenever it hears an error, and turns the option off (which discards
the queue) on every way out of the handshake. Python names neither constant, and other platforms
(macOS) have no equivalent, so there a closed port still waits out the connect deadline.
"""

from __future__ import annotations

import socket
import sys
from typing import Any

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
