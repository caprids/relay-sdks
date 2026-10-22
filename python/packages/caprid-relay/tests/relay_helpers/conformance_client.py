"""The conformance peer (``conformance_peer.py``) out of process, driven over its JSON-lines control protocol.

This is the Python counterpart of ``ts/scripts/conformance-peer.js`` and ``ts/conformance/targets/remote-peer.js``:
:class:`ConformancePeerProcess` starts the accepting end and sends it one request per line, and :class:`RemotePeer`
spells the suite's peer methods (``ts/conformance/README.md``, "The peer") as those requests. It speaks the protocol
the TypeScript suite speaks; it adds none of its own.

The process outlives any one event loop: pytest-asyncio gives every test a loop of its own, so a thread reads the
replies and settles each request's future on the loop that sent it.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

TESTS = Path(__file__).resolve().parent.parent
"""The directory that holds ``relay_helpers``: what the peer process needs on ``PYTHONPATH``."""

CALL_TIMEOUT = 45.0
"""Seconds a request may take: longer than any case waits, so a case's own bound reports first."""


class PeerError(RuntimeError):
    """The accepting end answered a request with an error, or exited."""


class ConformancePeerProcess:
    """The accepting end for ``framing`` (``webtransport`` or ``native-quic``), running until :meth:`stop`."""

    def __init__(self, framing: str = "webtransport") -> None:
        self.framing = framing
        env = {**os.environ, "PYTHONPATH": os.pathsep.join(filter(None, [str(TESTS), os.environ.get("PYTHONPATH")]))}
        self._process = subprocess.Popen(
            [sys.executable, "-m", "relay_helpers.conformance_peer", "--framing", framing],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            env=env,
            text=True,
            bufsize=1,
        )
        assert self._process.stdin is not None and self._process.stdout is not None
        self._stdin = self._process.stdin
        self._stdout = self._process.stdout
        line = self._stdout.readline()
        if not line:
            raise PeerError(f"the conformance peer exited before it listened (code {self._process.wait()})")
        listening = json.loads(line)
        self.port: int = listening["port"]
        self.certificate_hash: str = listening["certificateHash"]
        self.certificate_pem: str = listening["certificatePem"]
        self._lock = threading.Lock()
        self._next_id = 0
        self._pending: dict[int, tuple[asyncio.AbstractEventLoop, asyncio.Future[Any]]] = {}
        self._reader = threading.Thread(target=self._read_replies, name=f"conformance-peer-{framing}", daemon=True)
        self._reader.start()

    async def call(self, method: str, **params: Any) -> Any:
        """Send one request and return its result; raises :class:`PeerError` for an error reply."""
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        request = {key: value for key, value in params.items() if value is not None}
        with self._lock:
            request_id = self._next_id
            self._next_id += 1
            self._pending[request_id] = (loop, future)
            self._stdin.write(json.dumps({"id": request_id, "method": method, "params": request}) + "\n")
            self._stdin.flush()
        try:
            return await asyncio.wait_for(future, CALL_TIMEOUT)
        finally:
            with self._lock:
                self._pending.pop(request_id, None)

    def stop(self) -> None:
        """Close the peer's stdin, which ends it, and wait for it to exit."""
        with contextlib.suppress(OSError):
            self._stdin.close()
        try:
            self._process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait()
        self._reader.join(timeout=5)

    def _read_replies(self) -> None:
        for line in self._stdout:
            message = json.loads(line)
            with self._lock:
                waiter = self._pending.pop(message.get("id"), None)
            if waiter is not None:
                self._settle(waiter, message)
        exited = {"error": f"the conformance peer exited (code {self._process.poll()})"}
        with self._lock:
            waiters, self._pending = list(self._pending.values()), {}
        for waiter in waiters:
            self._settle(waiter, exited)

    @staticmethod
    def _settle(waiter: tuple[asyncio.AbstractEventLoop, asyncio.Future[Any]], message: dict[str, Any]) -> None:
        loop, future = waiter

        def settle() -> None:
            if future.done():
                return
            if "error" in message:
                future.set_exception(PeerError(f"peer: {message['error']}"))
            else:
                future.set_result(message.get("result"))

        with contextlib.suppress(RuntimeError):  # the loop that sent it has closed
            loop.call_soon_threadsafe(settle)


class RemotePeer:
    """The suite's peer methods as requests to the accepting end. Stream ids are opaque; bytes cross as lists."""

    def __init__(self, process: ConformancePeerProcess) -> None:
        self._call = process.call

    async def accept_uni(self) -> int:
        return await self._call("acceptUni")

    async def accept_bi(self) -> int:
        return await self._call("acceptBi")

    async def open_uni(self) -> int:
        return await self._call("openUni")

    async def open_bi(self) -> int:
        return await self._call("openBi")

    async def write(self, stream: int, data: bytes) -> None:
        await self._call("write", stream=stream, data=list(data))

    async def finish(self, stream: int) -> None:
        await self._call("finish", stream=stream)

    async def reset(self, stream: int, code: int | str, *, wire: bool = False) -> None:
        """Reset with ``code``; with ``wire``, put ``code`` on native QUIC's wire as it is, unencoded (TA-27)."""
        await self._call("reset", stream=stream, code=code, wire=wire or None)

    async def stop(self, stream: int, code: int | str, *, wire: bool = False) -> None:
        """Stop with ``code``; with ``wire``, as :meth:`reset`."""
        await self._call("stop", stream=stream, code=code, wire=wire or None)

    async def read(self, stream: int, *, count: int | None = None, discard: bool = False) -> dict[str, Any]:
        """``{bytes, byteLength, fin, resetCode}``: read until the FIN, a reset, or ``count`` bytes."""
        return await self._call("read", stream=stream, count=count, discard=discard or None)

    async def stopped(self, stream: int) -> int:
        return await self._call("stopped", stream=stream)

    async def wire_code(self, stream: int) -> int | None:
        """The code the dialer's last reset or stop of ``stream`` carried on native QUIC's wire (TA-27)."""
        code = await self._call("wireCode", stream=stream)
        return None if code is None else int(code)

    async def close(self, code: int, reason: str) -> None:
        await self._call("close", code=code, reason=reason)

    async def closed(self) -> dict[str, Any]:
        """``{closeCode, reason}`` once the dialer closed the session."""
        return await self._call("closed")

    async def quic_close(self) -> None:
        await self._call("quicClose")

    async def lose_connection(self) -> None:
        await self._call("loseConnection")

    async def pause(self) -> None:
        await self._call("pause")

    async def resume(self) -> None:
        await self._call("resume")

    async def release_handshake(self) -> None:
        await self._call("releaseHandshake")

    async def observed(self) -> dict[str, Any] | None:
        """What the peer saw: ``{path, protocols}`` on WebTransport, ``{serverName, protocols}`` on native QUIC."""
        return await self._call("observedDial")
