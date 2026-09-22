"""Shared server fixtures (stdlib only, plus websockets for WS tests)."""

from __future__ import annotations

import http.client
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GUNICORN = str(ROOT / ".venv" / "bin" / "gunicorn")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_port(port: int, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"server did not listen on 127.0.0.1:{port}")


def wait_for_socket(path: str, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.socket(socket.AF_UNIX) as s:
                s.settimeout(1)
                s.connect(path)
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"server did not listen on {path}")


def child_pids(pid: int) -> list[int]:
    """Direct child PIDs via /proc (no psutil dependency)."""
    out = []
    try:
        for entry in os.listdir("/proc"):
            if not entry.isdigit():
                continue
            try:
                with open(f"/proc/{entry}/stat") as f:
                    parts = f.read().rsplit(")", 1)[1].split()
                    ppid = int(parts[1])
            except (OSError, ValueError, IndexError):
                continue
            if ppid == pid:
                out.append(int(entry))
    except FileNotFoundError:
        pass
    return out


def wait_for_workers(master_pid: int, n: int, timeout: float = 60.0) -> set[int]:
    """Poll until the master has exactly n worker children (boot race)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        pids = set(child_pids(master_pid))
        if len(pids) == n:
            return pids
        time.sleep(0.5)
    raise TimeoutError(
        f"master {master_pid} has {sorted(child_pids(master_pid))}, wanted {n}"
    )


class StdServer:
    """Standalone `python -m rustwasgi` server."""

    def __init__(self, app: str, port: int, extra: tuple = ()):
        env = dict(os.environ)
        env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "rustwasgi", app,
             "--host", "127.0.0.1", "--port", str(port), *extra],
            cwd=str(ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )
        wait_for_port(port)
        self.port = port
        self.is_tls = any("--tls-cert" in str(x) for x in extra)


class TlsServer(StdServer):
    """Standalone TLS server (HTTPS)"""

    def __init__(self, app: str, port: int, cert: str, key: str, extra: tuple = ()):
        super().__init__(app, port, extra=("--tls-cert", cert, "--tls-key", key, *extra))
        self.is_tls = True

    def request(self, method, path, body=None, headers=None):
        # Override to use HTTPS - explicit debug to catch BadStatusLine flake
        import ssl
        ctx = ssl._create_unverified_context()
        # Use same code path as manual successful test: create new context each time
        conn = http.client.HTTPSConnection("127.0.0.1", self.port, context=ctx, timeout=15)
        try:
            conn.request(method, path, body=body or b"", headers=headers or {})
            resp = conn.getresponse()
            data = resp.read()
            out = (resp.status, resp.getheaders(), data)
            conn.close()
            return out
        except Exception as e:
            # Include port/context debug before re-raising
            import traceback
            print(f"DEBUG TlsServer.request failed port={self.port} err={e}", flush=True)
            traceback.print_exc()
            try:
                conn.close()
            except Exception:
                pass
            raise

    def get_cert_der(self) -> bytes:
        import ssl, socket
        ctx = ssl._create_unverified_context()
        with socket.create_connection(("127.0.0.1", self.port), timeout=5) as sock:
            with ctx.wrap_socket(sock, server_hostname="localhost") as ssock:
                der = ssock.getpeercert(binary_form=True)
                return der if der else b""

    def stop(self) -> str:
        self.proc.terminate()
        try:
            out, _ = self.proc.communicate(timeout=20)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            out, _ = self.proc.communicate(timeout=10)
        return out or ""

    def stop_and_assert_clean(self) -> str:
        out = self.stop()
        assert self.proc.returncode is not None, "server still running after SIGTERM"
        assert "Traceback" not in out, f"traceback in server output:\n{out}"
        return out


# Backwards-compatible alias used by the first-generation tests.
Server = StdServer


class GunicornServer:
    """Gunicorn master + RustWASGIWorker workers."""

    def __init__(self, app: str, port: int, workers: int = 2,
                 extra: tuple = (), timeout: int = 30):
        env = dict(os.environ)
        env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
        self.proc = subprocess.Popen(
            [GUNICORN, "-k", "rustwasgi.gunicorn.RustWASGIWorker",
             "-w", str(workers), "-b", f"127.0.0.1:{port}",
             "--timeout", str(timeout),
             "--access-logfile", "-", "--error-logfile", "-",
             *extra, app],
            cwd=str(ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )
        wait_for_port(port, timeout=60)
        self.port = port

    @property
    def master_pid(self) -> int:
        return self.proc.pid

    def request(self, method, path, body=None, headers=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=15)
        conn.request(method, path, body=body or b"", headers=headers or {})
        resp = conn.getresponse()
        data = resp.read()
        out = (resp.status, resp.getheaders(), data)
        conn.close()
        return out

    def stop(self, sig=signal.SIGTERM) -> str:
        try:
            os.kill(self.master_pid, sig)
        except ProcessLookupError:
            pass
        try:
            out, _ = self.proc.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            out, _ = self.proc.communicate(timeout=10)
        return out or ""
