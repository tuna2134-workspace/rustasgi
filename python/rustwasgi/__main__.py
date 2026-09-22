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
    return p


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
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
    )


if __name__ == "__main__":
    sys.exit(main())
