# RustWASGI HTTP request-path performance report

Date: 2026-09-22. Box: 8 cores, same machine for all comparisons.
Build: `maturin develop --release`, Rust 1.97.1, PyO3 0.29.0,
`pyo3-async-runtimes` 0.29.0, hyper 1.x. Harness: `bench/bench.py`
(median of rounds, threaded keep-alive loader, JSON output).

## 1. Result summary

| Setup | rustwasgi | uvicorn | ratio |
|---|---|---|---|
| Single worker, conc 1 (`GET /`) | 1434 rps / 0.70ms | 1253 rps / 0.80ms | **114%** |
| Single worker, conc 20 | 3159 rps / 6.1ms | 1945 rps / 10.2ms | **162%** |
| Single worker, conc 64 | 3280 rps / 19.0ms | 2000 rps / 31.6ms | **164%** |
| Single worker, conc 256 | 3329 rps / 67.4ms | 1919 rps / 131.7ms | **173%** |
| 4 workers, 50 conc, 2 loaders | 5710 rps | 5270 rps | **108%** |
| 4 workers, 128 conc, 4 loaders | 7939 rps | 6250 rps | **127%** |
| POST echo 22B/1KB/64KB @conc 10 | 1341/1302/814 rps | 1554/1479/947 rps | 86–88% |
| POST echo 1MB @conc 1 / 10 | 94 / 120 rps | 94 / 101 rps | 100% / 119% |
| `/stream` first chunk | 1.6ms | 2.8ms | incremental both sides |

Full single-worker sweep (rustwasgi vs uvicorn RPS at
1/2/4/8/16/20/32/64/128/256): 1424/2549/2535/2813/3264/3159/3083/3280/3201/3329
vs 1253/1933/1849/1934/2047/1945/1971/2000/1879/1919 — rustwasgi ahead at
every level (114–173%), zero errors throughout.

CPU/RSS @conc 20: rustwasgi ~130% / 47 MiB vs uvicorn 95% / 46 MiB.
CPU per request is identical (0.50ms vs 0.49ms) — same energy, higher
ceiling on multi-core. Uvicorn saturates one core (1 thread, ~10 total
context switches); rustwasgi spreads over 10 threads.

Targets (§20/§28): single-connection 0.70ms (target ≤0.80ms ✓);
≥100% of uvicorn at every concurrency 1–256 ✓ with no concurrency
regression; 4-worker production ahead in both load shapes ✓.

## 2. Root causes found (each with evidence)

### 2.1 Per-request task and synchronization sprawl — FIXED

Three Tokio tasks (reaper, pump, feeder) plus a `watch` channel coordinated
every request.
*Evidence:* `RUSTWASGI_PROFILE=1` counters: `tokio_spawns` **2.000/req**
before → **1.000/req** after; watch channel deleted.

### 2.2 Redundant GIL acquisitions — FIXED

Beyond the one measured establish attach, the pump cloned `Bridge`
(10 refcounts) and `WorkerHooks` (3 refcounts) per request — two more full
GIL acquisition waits invisible to the old counters.
*Evidence:* cross-thread wakeups at conc 20 fell **22,532 → 13,902 (−38%)**;
RPS +3–10% with identical workload.

### 2.3 GIL acquisition *waiting*, not holding — structural, quantified

Uncontended establish attach: 7µs wait + 73µs hold. Contended (conc 20):
**1047µs wait** + 266µs hold. App completion delivery shows the same shape
(loop queueing, not execution: loop CPU rises only 26→47% while latency
rises 10x).
*Evidence:* `attach_wait`/`attach_hold` phase split at conc 1 vs 20.

### 2.4 Loader and accept-distribution artifacts — methodology, not code

- Single Python loader process caps at ~3500 rps (loader GIL). Proof: 4
  loader processes against the same server reach 6925 rps.
- Without `--reuse-port`, up to 70% of keep-alive requests pin to one
  worker ({40,141,10,9}); `--reuse-port` balances to {48,62,55,35}.
  Environmental (kernel accept behavior), fixed by config.

### 2.5 Non-causes (measured, rejected)

Starlette stack (~0.08–0.14ms/req, FastAPI-vs-pure split); `into_future`
(flat 0.15→0.08ms micro, no contention blowup); response backpressure path
(counter reads **0.000**/req); pump body forwarding (54µs ≈ 1% of service —
custom-`Body` rewrite rejected); scope/header/body allocations (inside the
73–266µs hold; a 50% cut would buy ~2%); atomics (identical cost on x86);
metrics (~100ns/req, now fully gated off); the asyncio thread itself
(26–47%, never saturated — loop elimination rejected per §22).

## 3. Changes

| file | change | removes | measured effect |
|---|---|---|---|
| `src/server.rs` `handle_http` | Phase-A inline select (body feed + app completion + rx + 30s deadline) | feeder task, reaper task, `watch` channel | spawns 2→1/req |
| `src/server.rs` responder | merged pump: rx forwarding + remainder feeding + app completion + 300s deadline + access log + guard | separate reaper/pump/feeder tasks | one owner for full lifecycle |
| `src/server.rs` | `spawn_drain_reaper` (Phase-A failure paths only) | — | preserves pre-merge traceback observability |
| `src/server.rs` | `Arc<AppState>` into responder; single `queue` (was `queue_c`/`queue_f`) | 2 GIL acquisitions/req | −38% cross-thread wakeups |
| `src/server.rs` | post-loop noop-waker poll + reaper fallback | silent traceback swallow the merge briefly introduced | correctness fix, caught by profile (`app_wait` 19198/20001) |
| `src/metrics.rs` | gate `inc()` behind `enabled()`; wait/hold split; per-site completion counters | — | zero-cost default; diagnostics stay |
| `src/lib.rs` | `RUSTWASGI_THREADS` (`worker_threads()`) | — | oversubscription control; neutral perf, operational value |

Kept deliberately: `SendSink` try_send fast path, bounded channels
everywhere, disconnect/OSError semantics, HEAD suppression, deadlines
(30s first-byte, 300s app), FlightGuard drain, ASGI 2 wrapper.

## 4. Profile reference (`RUSTWASGI_PROFILE=1`, 20k requests, conc 20)

| phase | mean | note |
|---|---|---|
| establish | 1294µs | one attach: ~1047µs wait + ~266µs hold (contended) |
| app_wait | 3846µs | loop queueing + app (completions observed: 97% immediate, 3% reaper) |
| pump | 54–67µs | pure Rust forwarding + access log |
| total | ~4.7–5.2ms | client-observed ~6–9ms (loader + TCP on top) |

Per-request means: 1 `into_future`, 1 Tokio spawn, 1 GIL attach,
2 bounded channel sends, 0 feeder chunks (bodyless).

Uncontended reference (conc 1): establish 104µs (wait 7µs), app_wait
339µs, pump 53µs, total 394µs — contention inflates every handoff ~10x,
which is why oversubscription matters more than per-operation cost here.

## 5. Correctness

`pytest tests/ -q`: **28 passed**. `cargo test`: ok (0 Rust unit tests).
`cargo clippy --all-targets --all-features -- -D warnings`: clean.
`cargo fmt --check`: clean. Bridge source scan (`tests/test_bridge.py`):
passes unweakened — no `spawn_blocking` / `Future.result` /
`run_coroutine_threadsafe` / `blocking_send` / `blocking_recv` / `Condvar`
on the request path; `call_soon_threadsafe` only in loop-stop.
Covers: ASGI 2/3, HTTP matrix, streaming both directions, WS
text/bytes/ping-pong, lifespan full cycle + failure + unsupported, Gunicorn
restart/HUP/max-requests/preload/heartbeat/unix/factories.

## 6. What limits throughput now (and what was deliberately not done)

- **Python/GIL overhead**: ~1.0ms/request of GIL *acquisition waiting*
  under contention (measured), plus loop-queueing in `app_wait`. Dominant;
  only reducible by shrinking loop-thread work (the app's own domain).
- **asyncio scheduling**: ~0.3ms loop CPU/req is fine; the ~3ms queueing
  around it is GIL/scheduler latency under a 90-thread oversubscribed box,
  not loop saturation.
- **Tokio scheduling**: 1 spawn/req at ~10 threads; `RUSTWASGI_THREADS`
  tunes oversubscription (2≈8 here — contention and capacity trade off,
  peak near 4 on 8 cores).
- **Channel overhead**: two bounded hops totaling <0.1ms; backpressure path
  never taken in benchmarks.
- **Allocations/copies**: scope+headers+body copies live inside the
  73–266µs hold; hyper/http-body overhead bounded above by pump's 54µs.
  Neither justifies rewrites.
- **Actual application execution**: ~0.1–0.24ms loop CPU/req for FastAPI —
  the minority of every millisecond measured.

Rejected with data: custom-`Body` replacing mpsc+Channel (pump is 1% of
service); allocation micro-rewrites (µs vs ms waits); atomics ordering
(no x86 effect); loop-thread elimination (§22 evidence does not support:
never saturated, asyncio libraries require it); reaper+pump un-merging
(would restore 2 spawns for zero gain).

## 7. Shutdown hygiene (standalone Ctrl-C)

After the performance work, `uv run rustwasgi main:app --host 0.0.0.0`
exited on `^C` with a traceback through `base_events.call_soon_threadsafe`
and `RuntimeWarning: coroutine 'Queue.put' was never awaited`, despite
`INFO rustwasgi: quick shutdown` / `lifespan shutdown complete` already
having run. Two races explained it:

- The interpreter delivered `KeyboardInterrupt` while the main thread was
  inside a GIL re-attach (Python `atexit`/`faulthandler` machinery between
  the Rust `block_on` returning and the `handle.join`), so the main thread's
  shutdown sequence (cancel → barrier → join) was interrupted before the loop
  could be stopped.
- Between `quick shutdown` and the final cleanup barrier, in-flight `Queue.put`
  coroutines were already scheduled on the asyncio thread's ready queue but
  not yet materialized as Tasks. Cancelling only `all_tasks()` misses them,
  so the subsequent `stop()` aborts them mid-flight and their coroutines
  surface as "never awaited". The pre-existing `into_future` shutdown guard
  (`is_running() == false → close() the coroutine`) went the wrong way on
  the startup race: a *not-yet-started* loop also reads `False`, so gunicorn
  workers spuriously lost lifespan init (`RuntimeError: event loop not
  running`).

Fixes, each narrow and justified:

- `python/rustwasgi/__main__.py`: outer `except KeyboardInterrupt: pass`.
  SIGINT is already fully handled by the Rust signal watcher (`quick
  shutdown` path); the remaining KI is only the interpreter's re-raise
  during teardown and carries no additional shutdown work.
- `src/runtime.rs`: `stop_asyncio_loop` retries `call_soon_threadsafe`
  when `PyKeyboardInterrupt` is raised by the attach (up to 3 tries) — the
  KI is consumed by the `except` but the retry ensures the stop is still
  scheduled and the join never hangs.
- `src/asgi.rs`: `into_future` now fails fast only when the loop is
  *stopped* (`is_running() == False` is sound because `start_asyncio_loop`
  now rendezvous on running before anyone can submit).
- `src/lib.rs`: `cancel_pending_tasks` first awaits a no-op barrier
  coroutine through `into_future` → loop, forcing the ready queue to drain
  into real Tasks so the subsequent `cancel_all` sees them.
- `src/runtime.rs`: startup rendezvous — `start_asyncio_loop` spawns the
  thread and then (after a Tokio runtime exists) async-waits until
  `is_running() == True` (GIL released while sleeping), fixing the
  gunicorn-fork lifespan flake.

Verified: `Ctrl-C` idle and under load (`bench_app` 32×60k), `SIGTERM`,
double-Ctrl-C, and WebSocket in-flight shutdown all exit `0` with no
`Traceback`, `RuntimeWarning`, or `KeyboardInterrupt` leakage; 28/28 tests
still pass and `quick shutdown` / `lifespan shutdown complete` remain the
only visible shutdown lines.
