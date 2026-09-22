"""Gunicorn + RustWASGIWorker integration tests (the production path)."""

import asyncio
import json
import os
import signal
import socket
import time

import pytest

from _util import GunicornServer, child_pids, free_port, wait_for_socket, wait_for_workers

websockets = pytest.importorskip("websockets")


def _pids(port: int, n: int = 10) -> set[int]:
    import http.client

    out: set[int] = set()
    for _ in range(n):
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        conn.request("GET", "/pid")
        resp = conn.getresponse()
        assert resp.status == 200, resp.status
        out.add(json.loads(resp.read().decode())["pid"])
        conn.close()
    return out


def test_gunicorn_basic_fastapi():
    port = free_port()
    srv = GunicornServer("examples.fastapi_app:app", port, workers=2)
    try:
        status, _, data = srv.request("GET", "/")
        assert status == 200, data
        assert json.loads(data.decode()) == {"message": "hello from rustwasgi"}

        status, _, data = srv.request("GET", "/async")
        assert status == 200, data

        body = json.dumps({"hello": "world"}).encode()
        status, _, data = srv.request(
            "POST", "/echo", body=body,
            headers={"Content-Type": "application/json"},
        )
        assert status == 200, data
        assert json.loads(data.decode()) == {"hello": "world"}
    finally:
        srv.stop()


def test_gunicorn_multi_worker_distributes():
    port = free_port()
    srv = GunicornServer("examples.fastapi_app:app", port, workers=4)
    try:
        workers = wait_for_workers(srv.master_pid, 4)
        seen = _pids(port, 40)
        assert len(seen) >= 2, f"expected >=2 workers, saw {seen}"
        assert seen <= workers, (seen, workers)
    finally:
        srv.stop()


def test_gunicorn_worker_restart():
    port = free_port()
    srv = GunicornServer("examples.fastapi_app:app", port, workers=2)
    try:
        before = wait_for_workers(srv.master_pid, 2)
        victim = sorted(before)[0]
        os.kill(victim, signal.SIGTERM)
        deadline = time.time() + 30
        while time.time() < deadline:
            after = set(child_pids(srv.master_pid))
            if len(after) == 2 and victim not in after:
                break
            time.sleep(0.5)
        else:
            raise AssertionError(f"worker not replaced: {after}")
        status, _, data = srv.request("GET", "/")
        assert status == 200, data
    finally:
        srv.stop()


def test_gunicorn_hup_reload():
    port = free_port()
    srv = GunicornServer("examples.fastapi_app:app", port, workers=2)
    try:
        before = set(child_pids(srv.master_pid))
        os.kill(srv.master_pid, signal.SIGHUP)
        deadline = time.time() + 45
        changed = False
        while time.time() < deadline:
            # requests must keep succeeding during reload
            status, _, data = srv.request("GET", "/")
            assert status == 200, data
            after = set(child_pids(srv.master_pid))
            if len(after) == 2 and after != before:
                changed = True
                break
            time.sleep(1)
        assert changed, f"workers not reloaded: {after}"
    finally:
        srv.stop()


def test_gunicorn_max_requests_recycles():
    port = free_port()
    srv = GunicornServer(
        "examples.fastapi_app:app", port, workers=2,
        extra=("--max-requests", "10", "--max-requests-jitter", "0"),
    )
    try:
        seen = _pids(port, 60)
        assert len(seen) > 2, f"expected recycling, saw {seen}"
    finally:
        srv.stop()


def test_gunicorn_preload():
    port = free_port()
    srv = GunicornServer(
        "examples.fastapi_app:app", port, workers=2, extra=("--preload",),
    )
    try:
        status, _, data = srv.request("GET", "/")
        assert status == 200, data
    finally:
        srv.stop()


def test_gunicorn_lifespan_startup_and_shutdown(tmp_path):
    marker = tmp_path / "lifespan.log"
    os.environ["RUSTWASGI_TEST_LIFESPAN_FILE"] = str(marker)
    port = free_port()
    srv = GunicornServer("examples.fastapi_lifespan:app", port, workers=1)
    try:
        # The master listens before workers finish startup: poll.
        deadline = time.time() + 30
        while time.time() < deadline:
            if marker.exists() and "startup" in marker.read_text():
                break
            time.sleep(0.5)
        else:
            raise AssertionError("lifespan startup never ran")
        status, _, data = srv.request("GET", "/state")
        assert status == 200, data
    finally:
        srv.stop()
        os.environ.pop("RUSTWASGI_TEST_LIFESPAN_FILE", None)
    assert "shutdown" in marker.read_text()


def test_gunicorn_heartbeat_keeps_long_websocket_alive():
    """WS held open past gunicorn timeout=5: heartbeat must prevent murder."""
    port = free_port()
    srv = GunicornServer(
        "examples.websocket_app:app", port, workers=1, timeout=5,
    )
    try:
        async def hold():
            async with websockets.connect(f"ws://127.0.0.1:{port}/ws") as ws:
                await asyncio.sleep(8)  # exceeds timeout=5
                await ws.send("still-here")
                assert await ws.recv() == "still-here"
                await ws.send("close")

        asyncio.run(hold())
    finally:
        srv.stop()


def test_gunicorn_unix_socket(tmp_path):
    import subprocess
    import sys

    from _util import ROOT, GUNICORN

    path = str(tmp_path / "app.sock")
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.Popen(
        [GUNICORN, "-k", "rustwasgi.gunicorn.RustWASGIWorker",
         "-w", "1", "-b", f"unix:{path}",
         "--access-logfile", "-", "--error-logfile", "-",
         "examples.fastapi_app:app"],
        cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, env=env,
    )
    try:
        wait_for_socket(path)
        s = socket.socket(socket.AF_UNIX)
        try:
            s.settimeout(10)
            s.connect(path)
            s.sendall(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            raw = b""
            while True:
                chunk = s.recv(65536)
                if not chunk:
                    break
                raw += chunk
        finally:
            s.close()
        head, _, body = raw.partition(b"\r\n\r\n")
        assert "200" in head.split(b"\r\n")[0].decode(), head[:200]
        assert json.loads(body.decode()) == {"message": "hello from rustwasgi"}
    finally:
        proc.terminate()
        try:
            proc.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate(timeout=10)
