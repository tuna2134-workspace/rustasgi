# RustWASGI — Rust ASGI server as a Gunicorn custom worker

RustWASGI runs unmodified FastAPI (or any ASGI) applications behind a real
Rust HTTP/WebSocket stack, deployed the boring, reliable way: as a
**Gunicorn custom worker**.

```bash
gunicorn -k rustwasgi.gunicorn.RustWASGIWorker -w 4 app:app
```

## What is ASGI?

ASGI (Asynchronous Server Gateway Interface,
[spec](https://asgi.readthedocs.io/en/latest/)) is the standard contract
between Python async web apps and servers: the server calls
`await app(scope, receive, send)` once per connection scope, with `scope`
describing the connection, `receive()` yielding inbound events, and `send()`
delivering outbound events. Versions implemented here: **ASGI 3.0**,
**HTTP/WebSocket spec 2.5**, **Lifespan**, plus legacy **ASGI 2**
double-callable compatibility.

## Architecture

```text
Gunicorn master (bind, fork, supervise, signals, reload, timeouts)
   |  inherited listener FDs (dup'd per worker)
   v
RustWASGIWorker (one Python interpreter per worker process)
   |-- FastAPI / Starlette / user ASGI app
   |-- PyO3 bridge (scope/receive/send, asyncio loop thread)
   |-- Tokio runtime + hyper (sockets, HTTP/1.1, keep-alive, streaming)
   +-- WebSocket driver (tungstenite over hyper upgrade)
```

Ownership split:

| Concern | Owner |
|---|---|
| master process, worker fork/supervise/restart, signals, HUP reload, timeout murder, max-requests policy, socket bind/listen, config | Gunicorn |
| HTTP parsing, keep-alive, request/response streaming, ASGI scope/receive/send, WebSocket framing + ping/pong, lifespan, app invocation, backpressure | RustWASGI (Rust) |
| app loading (`module:attr`, `module:factory()`), access-log formatting, heartbeat file | Gunicorn worker shim (`python/rustwasgi/gunicorn.py`) |

There is intentionally **no Rust master/worker system**: Gunicorn already is
one. There is intentionally **no Python HTTP server**: hyper owns every
socket byte.

## Why Gunicorn / Rust / PyO3 / hyper

- **Gunicorn = process manager.** Pre-fork supervision, graceful HUP reload,
  timeout enforcement, max-requests recycling, socket inheritance — a decade
  of production behavior we reuse instead of reimplementing.
- **Rust = protocol server.** hyper gives correct HTTP/1.1 (keep-alive,
  chunked, upgrades) with Tokio concurrency; Rust owns framing so a Python
  app bug can never corrupt the wire.
- **PyO3 = bridge.** Zero-copy-ish bytes/dict/list/tuple construction,
  precise GIL discipline (see below).
- **hyper = HTTP implementation.** `http1::Builder` + `service_fn` per
  connection; `hyper::upgrade::on()` for WebSocket takeover.
- **Tokio = Rust async runtime**, one multi-thread runtime per worker,
  created post-fork.
- **FastAPI = example ASGI application**, never modified by the server.

## Installation

Requires Rust (1.97+) and Python 3.10+.

```bash
uv venv && source .venv/bin/activate
uv pip install maturin "rustwasgi[gunicorn]"   # or: pip install .
maturin develop            # local editable build
```

Standalone dev server: `python -m rustwasgi app:app`.
Production: `gunicorn -k rustwasgi.gunicorn.RustWASGIWorker -w 4 app:app`.

## FastAPI deployment (production)

```bash
gunicorn \
    --worker-class rustwasgi.gunicorn.RustWASGIWorker \
    --workers 4 \
    --bind 0.0.0.0:8000 \
    --timeout 120 \
    --graceful-timeout 30 \
    --keep-alive 5 \
    --max-requests 10000 \
    --max-requests-jitter 1000 \
    app:app
```

Tune workers/timeouts for your workload; the values above are a starting
point, not universal optima. Health endpoint example:

```python
@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
```

`/healthz` checks application health (routing + app stack), not just TCP.
Readiness = worker booted **and** `lifespan.startup.complete` received
before the worker serves.

## Gunicorn configuration

Honored settings: `--workers`, `--bind` (IPv4/IPv6/Unix sockets),
`--backlog` (socket pre-bound by master), `--timeout` (heartbeat is
`timeout//2`, so long requests/WebSockets/streams don't look dead),
`--graceful-timeout` (drain bound), `--keep-alive` (idle header wait),
`--max-requests`/`--max-requests-jitter` (Rust counts completions, then the
worker exits and the master reforks), `--preload` (safe: runtimes init
post-fork in `run()`), `--worker-tmp-dir`, `--access-logfile`
(`Logger.atoms` integration), `--error-logfile`, `--log-level`, config file,
`--reload` (alive-flag driven).

Extra worker knobs via environment: `RUSTWASGI_LIFESPAN=auto|on|off`,
`RUSTWASGI_ROOT_PATH=/prefix`. TLS is **not** terminated by workers
(`is_ssl` raises with a clear error) — terminate at nginx/HAProxy/Caddy.

## Worker configuration

Each worker = one OS process = one Python interpreter = one Tokio runtime +
one asyncio loop thread + one lifespan instance. Never share asyncio objects
between workers; DB pools/clients/locks initialize in lifespan (post-fork).

## PyO3 integration

- GIL is held only for: importing the app, building scope dicts, constructing
  channel objects, submitting coroutines, parsing one `send()` message.
- GIL is **released** while: hyper does network I/O, Tokio waits, blocking
  threads wait on `concurrent.futures` results, `SendSink` waits for channel
  capacity, the loop thread is joined.
- Shutdown deadlock rule (learned the hard way): never `join()` the loop
  thread while holding the GIL — it needs the GIL to run `loop.stop()`.

## asyncio integration

One dedicated OS thread per worker runs `loop.run_forever()`. Hyper request
tasks never `block_on` Python: they submit `app(scope, receive, send)` via
`asyncio.run_coroutine_threadsafe` and wait on the concurrent future from
`spawn_blocking` (GIL released in the wait). `await asyncio.sleep(...)` and
arbitrary async I/O work; the loop thread is the only place coroutines run.

## HTTP implementation

hyper `http1` + `with_upgrades`, one task per connection, `service_fn` per
request. Scope: `type/http_version/method/scheme/path(raw+decoded)/
query_string/headers(list of byte pairs, duplicates + order preserved)/
client/server/root_path/state/extensions`. `Transfer-Encoding: chunked` is
decoded by hyper; the app sees plain bytes. App `transfer-encoding` response
headers are stripped (server owns framing). Status/headers strictly validated
(lowercase bytes, no pseudo-headers); unknown `send()` types fail loudly;
extra keys ignored.

## Streaming

- Request: hyper body chunks → bounded `asyncio.Queue(maxsize=64)` via
  blocking `put` → `await receive()` yields `http.request` with
  `more_body=True/False`. Backpressure is end-to-end (slow app stalls the
  feeder stalls hyper stalls TCP). Chunked uploads verified.
- Response: `send()` → `SendSink` → bounded Tokio mpsc (16) → hyper
  `Channel` body, chunk-by-chunk; `StreamingResponse` verified incremental.
  Full channel => `send()` waits with GIL released (bounded memory).
- HEAD suppresses the wire body; headers preserved.

## WebSocket implementation

Upgrade detected in `service_fn`; 101 (+`Sec-WebSocket-Accept`) returned by
hyper; `hyper::upgrade::on()` stream adapted to Tokio I/O and driven by
tungstenite (server role). ASGI events: `connect/accept/receive(text+bytes)/
send(text|bytes exclusive)/close(code+reason)/disconnect`. Fragmentation
reassembled by tungstenite; PING→PONG in the protocol layer; `send()` after
close raises `OSError`. Limitation: the 101 precedes the app's accept/deny
(hyper upgrade API), so deny = 101 followed by a close frame; subprotocol
negotiation is ignored.

## Lifespan implementation

Per worker, pre-serve: `lifespan.startup` → `complete` (captures optional
`state` → HTTP/WS scopes) / `failed` (fatal) / silence+exit (unsupported →
continue in `auto`, fatal in `on`, skipped in `off`). Post-serve:
`lifespan.shutdown` (bounded wait), then loop stop/join. Modes:
`--lifespan` (standalone) / `RUSTWASGI_LIFESPAN` (gunicorn).

## ASGI 2 compatibility

`asyncio.iscoroutinefunction` → ASGI 3; plain `def app(scope)` → legacy
double-callable; other callables → ASGI 3 with `TypeError` fallback to ASGI 2
(equivalent in spirit to `asgiref.compatibility`).

## Graceful shutdown / Signals

Order: stop accepting → drain in-flight (bounded by graceful-timeout) →
`lifespan.shutdown` → stop loop → exit. `SIGTERM` = graceful, `SIGQUIT`/`SIGINT`
= quick; Rust watches signals itself because the main thread is parked in the
runtime (Python-level handlers still fire on return — cooperation, not
replacement). Orphan detection via ppid change. HUP reload, worker restart,
and recycling all verified against a live master.

## Configuration

Gunicorn flags (above) + env (`RUSTWASGI_*`). Standalone CLI mirrors the
subset: `--host --port --workers(1) --log-level --root-path --lifespan
--access-log --keep-alive`, each overridable via `RUSTWASGI_*`.

## Testing

```bash
cargo check && cargo clippy --all-targets -- -D warnings && cargo fmt --check
maturin develop
pytest -v            # 26 tests: ASGI/HTTP/WS/lifespan/Gunicorn/FastAPI
```

Covers methods/paths/query/HEAD/chunked/large-body/keep-alive/100-concurrent,
streaming incrementality, WS text+bytes+ping/pong, lifespan startup/state/
shutdown/unsupported, gunicorn basic/4-worker/restart/HUP/max-requests/
preload/lifespan/timeout-heartbeat/unix-socket, factories, ASGI 2.

## Benchmarking

No wrk/hey/ab in this environment; method: threaded keep-alive loader
(500-request warmup, then 3 rounds), same FastAPI/pure-ASGI apps, same box
(8 cores), **release** build (`maturin develop --release`).

Single worker, `GET /`, concurrency 20, 5000 requests x 3 rounds:

| server | rps (avg/min/max) | avg | p50 | p95 | p99 | RSS |
|---|---|---|---|---|---|---|
| rustwasgi + FastAPI | 1045 / 1012 / 1071 | 18.8ms | 18.3ms | 29.2ms | 35.4ms | ~50 MiB |
| uvicorn + FastAPI | 1914 / 1822 / 2015 | 10.5ms | 10.2ms | 13.0ms | 14.1ms | ~48 MiB |
| rustwasgi + pure ASGI | 994 / 960 / 1027 | 19.9ms | 19.5ms | 29.0ms | 33.6ms | ~28 MiB |
| uvicorn + pure ASGI | 2353 / 2271 / 2479 | 8.4ms | 8.3ms | 10.8ms | 11.9ms | ~31 MiB |

Single connection latency (concurrency 1, pure ASGI): rustwasgi 1.4ms avg.

Production path (Gunicorn master, 4 workers), concurrency 50, 10000 x 3:

| server | rps (avg) | avg | p50 | p95 | p99 |
|---|---|---|---|---|---|
| gunicorn + RustWASGIWorker | 2087 | ~19.8ms | ~19ms | ~33.8ms | ~41.3ms |
| gunicorn + UvicornWorker | 3876 | ~12.0ms | ~10.5ms | ~25.7ms | ~35.8ms |

Findings, reported without spin:

- Pure-ASGI ≈ FastAPI on rustwasgi ⇒ cost is bridge overhead, not the
  framework. Per-request true service is ~1.4ms (vs ~0.4ms on uvicorn);
  everything funnels through one asyncio loop thread plus GIL/thread hops
  (`run_coroutine_threadsafe` + blocking waits per request).
- A 42ms floor in early runs turned out to be Nagle/delayed-ACK (no
  `TCP_NODELAY` on the standalone socket) — fixed, 30x latency win, and a
  reminder to distrust first numbers.
- A benchmark-triggered `IncompleteRead` exposed a real drain bug
  (in-flight counter dropped at response headers while the body still
  streamed); fixed with an RAII guard covering pump/WS-driver lifetime.
- No superiority claimed. Next profiling targets: fewer threadsafe
  round-trips per request (combined init+app submit, fire-and-forget
  terminal body put), release LTO.

## Security

No tracebacks to clients (500 + logged); bounded channels everywhere except
the request queue depth×chunk product (documented); no proxy-header trust
(`X-Forwarded-*` ignored); no shell-out app loading; strict header/status
validation; TLS termination left to the reverse proxy.

## Reverse proxy / Docker / systemd

Run behind nginx/HAProxy/Caddy (plain HTTP or unix socket). Multi-stage
Dockerfile builds the wheel in a Rust stage and ships only
Python+Gunicorn+wheel+app:

```dockerfile
# builder
FROM rust:1.97 AS builder
RUN pip install maturin
COPY . /src
WORKDIR /src
RUN maturin build --release -o /dist
# runtime
FROM python:3.13-slim
RUN pip install gunicorn fastapi "rustwasgi[gunicorn]" --no-index --find-links /dist ...
```

See `deploy/rustwasgi.service` for a systemd unit (simple type; no fake
`sd_notify`).

## Observability

Gunicorn access log (combined format incl. timing/size), error log for Rust
lines (`INFO/WARN/ERROR rustwasgi:`), StatsD via Gunicorn's own
instrumentation (no custom metrics protocol required).

## Known limitations

- Debug-build performance (see Benchmarking); per-request bridge hops.
- HTTP/1.x only (no HTTP/2); WS subprotocols ignored; deny-after-101.
- No response trailers (`trailers: True` rejected loudly).
- Request queue bounded (64 chunks) but chunk-size unbounded per chunk.
- App-supplied `Content-Length` trusted (mismatch aborts the stream).
- Rust logs go to stderr, not through Gunicorn's error-log formatter.
- No sd_notify; worker stats (active conns) not yet exposed.
