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
`RUSTWASGI_ROOT_PATH=/prefix`, `RUSTWASGI_THREADS=N` (Tokio workers per
process; default CPU count — lower it to avoid oversubscription when running
many workers, e.g. 2 with `-w 4` on 8 cores), `RUSTWASGI_PROFILE=1` (prints
a request-path aggregate profile at shutdown; off by default).
TLS is **not** terminated by workers
(`is_ssl` raises with a clear error) — terminate at nginx/HAProxy/Caddy.

## Worker configuration

Each worker = one OS process = one Python interpreter = one Tokio runtime +
one asyncio loop thread + one lifespan instance. Never share asyncio objects
between workers; DB pools/clients/locks initialize in lifespan (post-fork).

## PyO3 integration

Via `pyo3-async-runtimes` (Tokio backend), not hand-rolled threads:

- Python awaitable → Rust future: `into_future_with_locals` schedules the
  coroutine onto our explicit loop (fire-and-forget) and completes through a
  `oneshot` channel. Awaiting it parks the Tokio task — no blocked threads,
  no condition variables.
- Rust future → Python awaitable: `future_into_py_with_locals` (used only
  for backpressured `send()` waits), spawned onto the worker's own Tokio
  runtime via `init_with_runtime` — no second runtime exists.
- GIL is held only for microsecond-scale construction/scheduling; **released**
  across every `.await` (network I/O, channel waits, app execution, loop join).
- Shutdown deadlock rule (learned the hard way): never `join()` the loop
  thread while holding the GIL — it needs the GIL to run `loop.stop()`.

## asyncio integration

One dedicated OS thread per worker runs `loop.run_forever()`; one shared
Tokio runtime drives hyper. Request tasks `await` the app future directly —
no `spawn_blocking`, no `Future.result()`, zero threads per request.
`await asyncio.sleep(...)` and arbitrary async I/O work; the loop thread is
the only place coroutines run. Task-local `TaskLocals` pins every conversion
to the worker's loop from any thread.

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
  `into_future` puts awaited by a Tokio feeder task (no threads).
  `await receive()` yields `http.request` with `more_body=True/False`.
  Backpressure is end-to-end (slow app stalls the feeder stalls hyper stalls
  TCP). Bodyless requests skip the feeder entirely (pre-seeded terminal).
  Chunked uploads verified.
- Response: `send()` → `SendSink.try_send` → bounded Tokio mpsc (16) →
  hyper `Channel` body, chunk-by-chunk; `StreamingResponse` verified
  incremental. Full channel => `send()` awaits a Rust future with GIL
  released (bounded memory, zero threads).
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

Reproducible harness: `bench/bench.py` (stdlib only; threaded keep-alive
loader, warmup, N rounds with median, RPS/latency/CPU/RSS/context-switch/
thread/fd accounting, JSON output). No wrk/hey/ab in this environment.
Same app (`examples/bench_app.py`, `{"ok": true}`), same box (8 cores),
**release** build (`maturin develop --release`).

Run: `.venv/bin/python bench/bench.py --servers standalone,uvicorn
--levels 1,2,4,8,16,20,32,64,128,256 --rounds 3 --json out.json`

Single worker, `GET /` (median of 3 rounds):

| conc | rustwasgi rps | uvicorn rps | ratio | rw CPU | uv CPU |
|---|---|---|---|---|---|
| 1 | 1287 | 1290 | **100%** | 69% | 77% |
| 2 | 1732 | 1749 | **99%** | 104% | 94% |
| 4 | 2044 | 1938 | **105%** | 122% | 95% |
| 8 | 2386 | 1858 | **128%** | 129% | 95% |
| 16 | 2595 | 1882 | **138%** | 131% | 94% |
| 20 | 2819 | 1937 | **146%** | 140% | 95% |
| 32 | 2859 | 1821 | **157%** | 138% | 94% |
| 64 | 2916 | 1911 | **153%** | 137% | 94% |
| 128 | 2840 | 1826 | **156%** | 138% | 94% |
| 256 | 2873 | 1906 | **151%** | 136% | 94% |

CPU per request at conc 20 is identical (0.50ms vs 0.49ms); RSS on par
(~47 MiB). Uvicorn saturates one core (94-95%, 1 thread, ~10 ctx switches
total); rustwasgi spreads ~140% over 10 threads with more wakeups — same
energy, higher ceiling on multi-core.

Production path (Gunicorn master, 4 workers, `--reuse-port`):

| load | rustwasgi | UvicornWorker | ratio |
|---|---|---|---|
| 50 conc, 1 loader | 3578 | 3792 | 94% |
| 50 conc, 2 loaders | 5710 | 5270 | **108%** |
| 128 conc, 4 loaders | 6925 | 4283 | **162%** |

Uvicorn's single-threaded workers saturate their loops under high
concurrency while rustwasgi workers keep scaling. Single-loader numbers
above ~3500 rps are loader-GIL-limited, not server-limited — always verify
high-RPS claims with parallel loader processes.

Bridge microbenchmark (`_bench_bridge`: into_future round-trip ms/op at
1/4/8/16/20/32/64 concurrency): **0.150 / 0.145 / 0.124 / 0.091 / 0.095 /
0.086 / 0.080** — flat-to-improving, vs the old bridge's 0.083 idle →
1.77 contended (21x blowup, gone).

Profiling (`RUSTWASGI_PROFILE=1`, aggregate at shutdown): per bodyless GET —
1 into_future, 2 Tokio spawns, 1 GIL attach, 2 bounded channel sends, 0
feeder chunks; phases establish ~1.3ms (of which ~1.0ms is GIL acquisition
wait, ~0.27ms holding), app_wait ~3.0ms (loop queueing + app), pump ~0.06ms.

Findings, reported without spin:

- Pure-ASGI ≈ FastAPI on rustwasgi ⇒ cost is bridge overhead, not the
  framework (~0.1ms of 0.8ms service).
- A 42ms floor in early runs turned out to be Nagle/delayed-ACK (no
  `TCP_NODELAY` on the standalone socket) — fixed, 30x latency win, and a
  reminder to distrust first numbers. (Uvicorn's `--workers 4` supervisor
  mode shows the same 42ms signature — its own socket setup, not ours.)
- A benchmark-triggered `IncompleteRead` exposed a real drain bug
  (in-flight counter dropped at response headers while the body still
  streamed); fixed with an RAII guard covering pump/WS-driver lifetime.
- Merging 6 per-request GIL acquisitions into 1 cut establish latency
  ~1.9ms → ~1.3ms and lifted throughput ~40% (profile-guided, §24 table in
  the performance report).
- 4-worker scaling without `--reuse-port` pins up to 70% of keep-alive
  requests on one worker (kernel accept distribution); `--reuse-port`
  balances it. Unevenness is environmental, not a code bottleneck.
- Access logging costs ~8% throughput when enabled (per-request formatting);
  both workers pay it equally.
- `RUSTWASGI_THREADS` (default: CPU count) caps Tokio workers per process;
  2 threads match 8-thread throughput on this workload — useful against
  oversubscription on small boxes, no behavior change otherwise.

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
