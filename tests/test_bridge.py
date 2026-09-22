"""Regression tests: the request path must not use thread-synchronous
bridges (§32). Our own src/ may not synchronously wait for Python.

Allowed:
- runtime.rs: fire-and-forget `call_soon_threadsafe` ONLY for loop stop
  (shutdown path, never waited on) and `handle.join()` (shutdown teardown,
  GIL released).
- Comments/docs mentioning the forbidden mechanisms in prose.

Everything else (asgi.rs, ws.rs, server.rs, lifespan.rs, lib.rs request
flow) must use pyo3-async-runtimes waker channels only.
"""

from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"

# token -> files where it may appear (with justification)
ALLOW = {
    "call_soon_threadsafe": {"runtime.rs"},  # loop.stop(), shutdown only
    "handle.join": {"lib.rs"},  # loop-thread join, GIL released
    "join_handle": set(),  # name fragment, always fine (checked separately)
}

FORBIDDEN = [
    "spawn_blocking",
    "run_coroutine_threadsafe",
    "blocking_send",
    "blocking_recv",
    'call_method1("result"',
    "call_method0(\"result\"",
    'call_method1("result",',
    "std::thread::sleep",
    "thread::sleep(",
    "Condvar",
]


def _code_lines(path: Path):
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if stripped.startswith("//"):
            continue
        yield line


def test_no_thread_sync_bridges():
    violations = []
    for path in sorted(SRC.glob("*.rs")):
        allowed_here = {
            tok for tok, files in ALLOW.items() if path.name in files
        }
        for i, line in enumerate(_code_lines(path), 1):
            for tok in FORBIDDEN:
                if tok in line and tok not in allowed_here:
                    violations.append(f"{path.name}:{i}: {tok}: {line.strip()}")
            if "call_soon_threadsafe" in line and path.name not in ALLOW["call_soon_threadsafe"]:
                violations.append(
                    f"{path.name}:{i}: call_soon_threadsafe: {line.strip()}"
                )
    assert not violations, "thread-synchronous bridge use:\n" + "\n".join(violations)


def test_no_blocking_wait_helpers_remain():
    # The old bridge's named helpers must be gone (no silent resurrection).
    names = ["await_app_completion", "feed_request_message_sync", "Bridge::submit"]
    hits = []
    for path in sorted(SRC.glob("*.rs")):
        for i, line in enumerate(_code_lines(path), 1):
            for name in names:
                if name in line:
                    hits.append(f"{path.name}:{i}: {line.strip()}")
    assert not hits, "removed bridge helpers reappeared:\n" + "\n".join(hits)
