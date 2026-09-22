"""CLI: `rustwasgi app:app [--host ..] [--port ..] ...` (standalone dev server).

Production multi-worker deployment uses Gunicorn instead:
`gunicorn -k rustwasgi.gunicorn.RustWASGIWorker -w 4 app:app`.
"""

from __future__ import annotations

import argparse
import os
import sys

import rustwasgi


def env(name: str, default: str) -> str:
    return os.environ.get(f"RUSTWASGI_{name}", default)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="rustwasgi", description="Rust (hyper) ASGI server"
    )
    p.add_argument("app", help="ASGI app, e.g. 'app:app'")
    p.add_argument("--host", default=env("HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(env("PORT", "8000")))
    p.add_argument("--workers", type=int, default=int(env("WORKERS", "1")))
    p.add_argument("--log-level", default=env("LOG_LEVEL", "info"))
    p.add_argument("--root-path", default=env("ROOT_PATH", ""))
    p.add_argument(
        "--lifespan",
        default=env("LIFESPAN", "auto"),
        choices=["auto", "on", "off"],
    )
    p.add_argument(
        "--access-log",
        action=argparse.BooleanOptionalAction,
        default=env("ACCESS_LOG", "0") == "1",
    )
    p.add_argument(
        "--keep-alive", type=int, default=int(env("KEEP_ALIVE", "0")),
        help="idle keep-alive header wait in seconds (0 = hyper default 30s)",
    )
    p.add_argument("--tls-cert", default=env("TLS_CERT", "") or None, help="TLS certificate file (PEM)")
    p.add_argument("--tls-key", default=env("TLS_KEY", "") or None, help="TLS private key file (PEM)")
    p.add_argument("--redirect-http-to-https", action=argparse.BooleanOptionalAction, default=env("REDIRECT_HTTP_TO_HTTPS", "0") == "1", help="Redirect HTTP to HTTPS (except ACME challenges)")
    p.add_argument("--acme-directory", default=env("ACME_DIRECTORY", "") or None, help="ACME directory URL (e.g. https://acme-v02.api.letsencrypt.org/directory)")
    p.add_argument("--acme-email", default=env("ACME_EMAIL", "") or None, help="ACME contact email")
    p.add_argument("--acme-domain", dest="acme_domains", action="append", default=None, help="ACME domain (repeatable, SAN list)")
    p.add_argument("--acme-dir", default=env("ACME_DIR", "") or None, help="ACME storage directory")
    return p


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    # ACME domains from env if not given via CLI
    acme_domains = args.acme_domains
    if acme_domains is None:
        env_domains = env("ACME_DOMAINS", "")
        acme_domains = [d.strip() for d in env_domains.split(",") if d.strip()] if env_domains else []
    try:
        rustwasgi.run(
            args.app,
            host=args.host,
            port=args.port,
            workers=args.workers,
            log_level=args.log_level,
            root_path=args.root_path,
            lifespan=args.lifespan,
            access_log=args.access_log,
            keep_alive=args.keep_alive,
            tls_cert=args.tls_cert,
            tls_key=args.tls_key,
            redirect_http_to_https=args.redirect_http_to_https,
            acme_directory=args.acme_directory,
            acme_email=args.acme_email,
            acme_domains=acme_domains,
            acme_dir=args.acme_dir,
        )
    except KeyboardInterrupt:
        # SIGINT is already handled gracefully by the Rust runtime (quick
        # shutdown via its own signal watcher); the interpreter may still
        # deliver KeyboardInterrupt to this thread while it re-acquires the
        # GIL during teardown. Swallow it so Ctrl-C exits silently with 0
        # instead of dumping a traceback for a shutdown that already happened.
        pass


if __name__ == "__main__":
    sys.exit(main())
