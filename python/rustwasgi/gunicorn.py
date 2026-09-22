"""Gunicorn custom worker: `rustwasgi.gunicorn.RustWASGIWorker`.

Production deployment path::

    gunicorn -k rustwasgi.gunicorn.RustWASGIWorker -w 4 app:app

Division of labour (see README "Why Gunicorn"):

* Gunicorn master: bind/listen, fork workers, supervise/restart, signals,
  graceful reload (HUP), timeout murder, max-requests policy (count), config.
* This worker: load the ASGI app (Gunicorn resolves ``module:attr`` and
  ``module:factory()``), dup the inherited listener FDs, run the Rust
  (hyper/Tokio) protocol server with heartbeat + access-log integration.

Fork safety: nothing Rust/Tokio/asyncio related is created at import time or
in the master. ``run()`` executes post-fork and builds the Tokio runtime,
the asyncio loop thread, and lifespan state fresh in each worker. This holds
for ``--preload`` too (the *application object* may predate the fork, but all
worker-specific runtimes are created after it).
"""

from __future__ import annotations

import os
import socket as pysocket
from datetime import timedelta

from gunicorn.workers import base

import rustwasgi


def _family_name(sock: pysocket.socket) -> str:
    fam = sock.family
    if fam == pysocket.AF_INET:
        return "tcp4"
    if fam == pysocket.AF_INET6:
        return "tcp6"
    if hasattr(pysocket, "AF_UNIX") and fam == pysocket.AF_UNIX:
        return "unix"
    raise RuntimeError(f"unsupported socket family {fam}")


class _AccessShim:
    """Minimal objects satisfying gunicorn's Logger.atoms()."""

    class Resp:
        def __init__(self, status, headers, sent):
            self.status = f"{status} {_reason(status)}"
            self.headers = list(headers)
            self.response_length = sent
            self.sent = sent

    class Req:
        def __init__(self, headers):
            self.headers = list(headers)


def _reason(status: int) -> str:
    try:
        import http as _http

        return _http.HTTPStatus(status).phrase
    except Exception:
        return "Unknown"


class RustWASGIWorker(base.Worker):
    """Gunicorn worker serving ASGI apps through the Rust/hyper core."""

    @classmethod
    def check_config(cls, cfg, log):
        if getattr(cfg, "is_ssl", False):
            raise RuntimeError(
                "rustwasgi worker does not terminate TLS; "
                "terminate TLS at a reverse proxy (nginx/HAProxy/Caddy)."
            )

    def load_wsgi(self):
        # Gunicorn resolves 'module:attr' AND 'module:factory()'.
        # The result is the ASGI callable; Rust receives the object itself.
        try:
            self.asgi_app = self.app.wsgi()
        except SyntaxError:
            if not self.cfg.reload:
                raise
            raise

    def init_process(self):
        # base inits env/signals/reloader, loads the app, then calls run().
        super().init_process()

    def run(self):
        cfg = self.cfg
        listeners = []
        for wrapper in self.sockets:
            sock = wrapper.sock
            family = _family_name(sock)
            # dup(): Rust takes ownership of the duplicate and closes it on
            # shutdown; Gunicorn keeps (and later closes) the original.
            fd = os.dup(sock.fileno())
            try:
                local = sock.getsockname()
            except OSError:
                local = None
            listeners.append((family, fd, f"{wrapper} local={local}"))

        app_spec = getattr(getattr(self.app, "app_uri", None), "__str__", lambda: "?")()
        if callable(app_spec):
            try:
                app_spec = app_spec()
            except Exception:
                app_spec = "?"

        lifespan = os.environ.get("RUSTWASGI_LIFESPAN", "auto")
        root_path = os.environ.get("RUSTWASGI_ROOT_PATH", "")
        access_enabled = bool(cfg.accesslog and cfg.accesslog != "none")

        heartbeat_interval = max(1, min(int(getattr(self, "timeout", 30) // 2 or 1), 30))

        self.log.info("RustWASGI worker serving %s (pid=%s)", app_spec, os.getpid())

        try:
            rustwasgi.run_worker(
                self.asgi_app,
                listeners,
                app_spec=str(app_spec),
                root_path=root_path,
                lifespan=lifespan,
                max_requests=int(getattr(self, "max_requests", 0) or 0),
                graceful_timeout=int(getattr(cfg, "graceful_timeout", 30) or 30),
                heartbeat_interval=heartbeat_interval,
                access_log=False,  # Gunicorn path uses the access hook instead.
                keep_alive=int(getattr(cfg, "keepalive", 0) or 0),
                notify=self.notify,
                is_alive=lambda: self.alive,
                access=self._make_access() if access_enabled else None,
            )
        finally:
            # run_worker owns the dup'd FDs and closed them; nothing to do.
            # Returning lets Gunicorn recycle the worker (e.g. max_requests).
            self.log.info("RustWASGI worker exiting (pid=%s)", os.getpid())

    # -- access log integration -------------------------------------------

    def _make_access(self):
        log = self.log

        def access(method, path, query, status, size, duration_ms, client):
            raw_uri = path + (f"?{query}" if query else "")
            environ = {
                "REMOTE_ADDR": client or "-",
                "REQUEST_METHOD": method,
                "RAW_URI": raw_uri,
                "SERVER_PROTOCOL": "HTTP/1.1",
                "PATH_INFO": path,
                "QUERY_STRING": query,
            }
            req = _AccessShim.Req([])
            resp = _AccessShim.Resp(status, [], size)
            request_time = timedelta(milliseconds=duration_ms)
            try:
                log.access(resp, req, environ, request_time)
            except Exception:
                log.exception("access log failed")

        return access
