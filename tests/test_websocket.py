import asyncio

import pytest

from _util import StdServer, free_port

websockets = pytest.importorskip("websockets")


async def _echo_text(port: int) -> None:
    async with websockets.connect(f"ws://127.0.0.1:{port}/ws") as ws:
        await ws.send("hello")
        assert await ws.recv() == "hello"
        await ws.send("world")
        assert await ws.recv() == "world"
        await ws.send("close")
        # server closes; recv() raises ConnectionClosedOK
        with pytest.raises(websockets.exceptions.ConnectionClosed):
            await ws.recv()


async def _echo_bytes(port: int) -> None:
    async with websockets.connect(f"ws://127.0.0.1:{port}/ws-bytes") as ws:
        await ws.send(b"\x00\x01\x02binary")
        assert await ws.recv() == b"\x00\x01\x02binary"


async def _ping_pong(port: int) -> None:
    async with websockets.connect(f"ws://127.0.0.1:{port}/ws") as ws:
        pong = await ws.ping()
        await pong  # server-level PONG handled in Rust, app never sees it
        await ws.send("close")
        with pytest.raises(websockets.exceptions.ConnectionClosed):
            await ws.recv()


def _run(coro):
    return asyncio.run(coro)


def test_websocket_text_echo_and_close():
    port = free_port()
    srv = StdServer("examples.websocket_app:app", port)
    try:
        _run(_echo_text(port))
    finally:
        srv.stop_and_assert_clean()


def test_websocket_binary_echo():
    port = free_port()
    srv = StdServer("examples.websocket_app:app", port)
    try:
        _run(_echo_bytes(port))
    finally:
        srv.stop_and_assert_clean()


def test_websocket_ping_pong_handled_by_server():
    port = free_port()
    srv = StdServer("examples.websocket_app:app", port)
    try:
        _run(_ping_pong(port))
    finally:
        srv.stop_and_assert_clean()
