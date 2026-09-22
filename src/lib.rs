//! `rustwasgi._rustwasgi` native extension.
//!
//! Two serving paths share one core:
//!
//! - `run(app_spec, ...)` — standalone dev server: imports the app from a
//!   `"module:attr"` string and binds its own socket.
//! - `run_worker(app, listeners, ...)` — Gunicorn worker path: receives the
//!   already-loaded ASGI callable plus already-bound (dup'd) listener FDs.
//!
//! # Runtime ownership (one per worker process, post-fork only)
//!
//! ```text
//! run()/run_worker()  (worker process, never the Gunicorn master)
//!   1. import app / install bridge (GIL)
//!   2. create asyncio loop object + TaskLocals; spawn its driver thread
//!   3. build the Tokio multi-thread runtime; share it with
//!      pyo3-async-runtimes via `init_with_runtime` (one runtime total)
//!   4. lifespan.startup -> serve -> lifespan.shutdown (async, GIL-free waits)
//!   5. cancel leftover tasks, stop loop, join thread, return
//! ```
//!
//! Nothing Tokio/asyncio is created at import time or pre-fork, so
//! `--preload` is safe: only the (user) application object may predate the
//! fork; every runtime is born after it.

mod acme;
mod asgi;
mod cli;
mod lifespan;
mod metrics;
mod runtime;
mod server;
mod socket;
mod tls;
mod ws;

use std::time::Duration;

use pyo3::prelude::*;
use pyo3_async_runtimes::TaskLocals;

use crate::cli::ServerConfig;
use crate::lifespan::{LifespanError, LifespanManager, LifespanMode};
use crate::server::{ServeConfig, WorkerHooks};
use crate::socket::{BoundListener, InheritedSocket};

/// Run the standalone ASGI server (blocking until SIGINT/SIGTERM).
#[pyfunction]
#[pyo3(signature = (app, *, host="127.0.0.1", port=8000, workers=1, log_level="info", root_path="", lifespan="auto", access_log=false, keep_alive=0, tls_cert=None, tls_key=None, redirect_http_to_https=false, acme_directory=None, acme_email=None, acme_domains=Vec::new(), acme_dir=None))]
#[allow(clippy::too_many_arguments)]
fn run(
    py: Python<'_>,
    app: String,
    host: &str,
    port: u16,
    workers: usize,
    log_level: &str,
    root_path: &str,
    lifespan: &str,
    access_log: bool,
    keep_alive: u64,
    tls_cert: Option<String>,
    tls_key: Option<String>,
    redirect_http_to_https: bool,
    acme_directory: Option<String>,
    acme_email: Option<String>,
    acme_domains: Vec<String>,
    acme_dir: Option<String>,
) -> PyResult<()> {
    // Env fallback for ACME domains if not passed (Gunicorn path may use env)
    let acme_domains = if acme_domains.is_empty() {
        std::env::var("RUSTWASGI_ACME_DOMAINS")
            .ok()
            .map(|s| s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect())
            .unwrap_or_default()
    } else {
        acme_domains
    };
    let tls_cert = tls_cert.or_else(|| std::env::var("RUSTWASGI_TLS_CERT").ok().filter(|s| !s.is_empty()));
    let tls_key = tls_key.or_else(|| std::env::var("RUSTWASGI_TLS_KEY").ok().filter(|s| !s.is_empty()));
    let config = ServerConfig::new(
        app.clone(),
        host.to_string(),
        port,
        workers,
        log_level.to_string(),
        root_path.to_string(),
        lifespan.to_string(),
        access_log,
        tls_cert,
        tls_key,
        redirect_http_to_https || std::env::var("RUSTWASGI_REDIRECT_HTTP_TO_HTTPS").as_deref() == Ok("1"),
        acme_directory.or_else(|| std::env::var("RUSTWASGI_ACME_DIRECTORY").ok()),
        acme_email.or_else(|| std::env::var("RUSTWASGI_ACME_EMAIL").ok()),
        acme_domains,
        acme_dir.or_else(|| std::env::var("RUSTWASGI_ACME_DIR").ok()),
    );
    if workers > 1 {
        config.log_warn(&format!(
            "workers={workers} requested but standalone mode runs a single worker; \
             use Gunicorn (rustwasgi.gunicorn.RustWASGIWorker) for multi-worker"
        ));
    }

    // No-op hooks: no Gunicorn supervisor in standalone mode.
    let notify: Py<PyAny> = py.eval(c"lambda: None", None, None)?.unbind();
    let is_alive: Py<PyAny> = py.eval(c"lambda: True", None, None)?.unbind();

    let (app_obj, bridge) = setup_python(py, &app)?;
    config.log_info(&format!("loaded ASGI application {app}"));
    // Build the serving runtime first: standalone sockets must be bound on
    // the same runtime that serves them (Tokio I/O is runtime-bound), and
    // the loop-startup rendezvous needs its timer.
    let rt_owned = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(tokio_worker_threads())
        .thread_name("rustwasgi-worker")
        .build()
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("failed to start tokio runtime: {e}"))
        })?;
    let rt: &'static tokio::runtime::Runtime = Box::leak(Box::new(rt_owned));
    share_runtime(rt);
    let loop_handle = runtime::start_asyncio_loop(py)?;
    runtime::wait_loop_running(py, rt, &loop_handle.loop_obj)?;
    let listeners = py
        .detach(|| rt.block_on(socket::bind_standalone(host, port)))
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    serve_process_on(
        py,
        rt,
        app_obj,
        loop_handle,
        bridge,
        listeners,
        app,
        &config,
        WorkerHooks {
            notify,
            is_alive,
            access: None,
        },
        0,
        30,
        3600,
        keep_alive,
    )
}

/// Run one Gunicorn worker (blocking until shutdown).
///
/// `app` is the already-loaded ASGI callable (Gunicorn resolved
/// `module:attr` or `module:factory()` itself). `listeners` is a list of
/// `(family, fd, description)` where `fd` is an `os.dup()`'d listener FD and
/// `family` is `"tcp4"`, `"tcp6"` or `"unix"`. `notify`/`is_alive` wire the
/// Gunicorn heartbeat; `access` receives access-log tuples (or `None`).
#[pyfunction]
#[pyo3(signature = (app, listeners, *, app_spec="gunicorn", root_path="", lifespan="auto", max_requests=0, graceful_timeout=30, heartbeat_interval=1, access_log=false, keep_alive=0, tls_cert=None, tls_key=None, redirect_http_to_https=false, acme_directory=None, acme_email=None, acme_domains=Vec::new(), acme_dir=None, notify, is_alive, access=None))]
#[allow(clippy::too_many_arguments)]
fn run_worker(
    py: Python<'_>,
    app: Bound<'_, PyAny>,
    listeners: Vec<(String, i32, String)>,
    app_spec: &str,
    root_path: &str,
    lifespan: &str,
    max_requests: usize,
    graceful_timeout: u64,
    heartbeat_interval: u64,
    access_log: bool,
    keep_alive: u64,
    tls_cert: Option<String>,
    tls_key: Option<String>,
    redirect_http_to_https: bool,
    acme_directory: Option<String>,
    acme_email: Option<String>,
    acme_domains: Vec<String>,
    acme_dir: Option<String>,
    notify: Py<PyAny>,
    is_alive: Py<PyAny>,
    access: Option<Py<PyAny>>,
) -> PyResult<()> {
    let mut inherited = Vec::with_capacity(listeners.len());
    for (family, fd, description) in listeners {
        // SAFETY: Gunicorn worker passed dup'd FDs (see socket.rs + gunicorn.py).
        inherited.push(unsafe { InheritedSocket::from_raw(family, fd, description) });
    }

    let acme_domains = if acme_domains.is_empty() {
        std::env::var("RUSTWASGI_ACME_DOMAINS")
            .ok()
            .map(|s| s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect())
            .unwrap_or_default()
    } else { acme_domains };
    let tls_cert = tls_cert.or_else(|| std::env::var("RUSTWASGI_TLS_CERT").ok().filter(|s| !s.is_empty()));
    let tls_key = tls_key.or_else(|| std::env::var("RUSTWASGI_TLS_KEY").ok().filter(|s| !s.is_empty()));
    let config = ServerConfig::new(
        app_spec.to_string(),
        String::new(),
        0,
        1,
        "info".to_string(),
        root_path.to_string(),
        lifespan.to_string(),
        access_log,
        tls_cert,
        tls_key,
        redirect_http_to_https || std::env::var("RUSTWASGI_REDIRECT_HTTP_TO_HTTPS").as_deref() == Ok("1"),
        acme_directory.or_else(|| std::env::var("RUSTWASGI_ACME_DIRECTORY").ok()),
        acme_email.or_else(|| std::env::var("RUSTWASGI_ACME_EMAIL").ok()),
        acme_domains,
        acme_dir.or_else(|| std::env::var("RUSTWASGI_ACME_DIR").ok()),
    );
    let app_obj = app.clone().unbind();
    let bridge = asgi::Bridge::install(py)?;
    // Serving runtime for this (post-fork) worker; the loop rendezvous below
    // needs its timer, and inherited FDs become Tokio listeners *inside* its
    // context (Tokio I/O is runtime-bound).
    let rt_owned = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(tokio_worker_threads())
        .thread_name("rustwasgi-worker")
        .build()
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("failed to start tokio runtime: {e}"))
        })?;
    let rt: &'static tokio::runtime::Runtime = Box::leak(Box::new(rt_owned));
    share_runtime(rt);
    let loop_handle = runtime::start_asyncio_loop(py)?;
    runtime::wait_loop_running(py, rt, &loop_handle.loop_obj)?;
    let bound = {
        let _guard = rt.enter();
        socket::bind_inherited(inherited).map_err(pyo3::exceptions::PyRuntimeError::new_err)?
    };
    serve_process_on(
        py,
        rt,
        app_obj,
        loop_handle,
        bridge,
        bound,
        app_spec.to_string(),
        &config,
        WorkerHooks {
            notify,
            is_alive,
            access,
        },
        max_requests,
        graceful_timeout,
        heartbeat_interval,
        keep_alive,
    )?;
    Ok(())
}

/// Tokio worker thread count for this worker process.
///
/// `RUSTWASGI_THREADS` overrides; otherwise one thread per CPU. Under
/// Gunicorn with W workers on C cores, W*C Tokio threads oversubscribe the
/// machine — set e.g. `RUSTWASGI_THREADS=2` so total threads ≈ cores.
/// Gunicorn may also pass an explicit per-worker count in future.
fn tokio_worker_threads() -> usize {
    if let Ok(v) = std::env::var("RUSTWASGI_THREADS")
        && let Ok(n) = v.parse::<usize>()
        && n >= 1
    {
        return n;
    }
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

/// Share our runtime with pyo3-async-runtimes so `future_into_py` send-waits
/// spawn onto it instead of a second runtime. Once per process (post-fork);
/// later calls are harmless no-ops.
fn share_runtime(rt: &'static tokio::runtime::Runtime) {
    let _ = pyo3_async_runtimes::tokio::init_with_runtime(rt);
}

/// Import app + install bridge (standalone path). Loop startup happens after
/// the Tokio runtime exists so start/stop can rendezvous on it.
fn setup_python(py: Python<'_>, app_spec: &str) -> PyResult<(Py<PyAny>, asgi::Bridge)> {
    let app_obj = runtime::import_app(py, app_spec)?;
    let bridge = asgi::Bridge::install(py)?;
    Ok((app_obj, bridge))
}

/// Shared orchestration on the worker runtime: lifespan startup -> serve ->
/// lifespan shutdown -> task cleanup -> loop stop/join. All waits are async
/// with the GIL released; no thread is ever blocked on Python.
#[allow(clippy::too_many_arguments)]
fn serve_process_on(
    py: Python<'_>,
    rt: &'static tokio::runtime::Runtime,
    app_obj: Py<PyAny>,
    loop_handle: runtime::LoopHandle,
    bridge: asgi::Bridge,
    listeners: Vec<BoundListener>,
    app_spec: String,
    config: &ServerConfig,
    hooks: WorkerHooks,
    max_requests: usize,
    graceful_timeout: u64,
    heartbeat_interval: u64,
    keep_alive: u64,
) -> PyResult<()> {
    let runtime::LoopHandle {
        loop_obj,
        locals,
        join_handle,
    } = loop_handle;
    // TLS setup — outside HTTP path, validated at startup
    let tls_state = if let (Some(cert), Some(key)) = (&config.tls_cert, &config.tls_key) {
        match crate::tls::TlsState::from_pem_files(std::path::Path::new(cert), std::path::Path::new(key)) {
            Ok(s) => {
                eprintln!("INFO rustwasgi: TLS enabled cert={cert} key={key}");
                Some(std::sync::Arc::new(s))
            }
            Err(e) => {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "failed to load TLS cert/key: {e}"
                )))
            }
        }
    } else if config.tls_cert.is_some() || config.tls_key.is_some() {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "both --tls-cert and --tls-key must be provided",
        ));
    } else {
        None
    };
    let challenge_store = crate::tls::ChallengeStore::new();
    // Log ACME config if present (ACME manager will be started inside serve)
    if config.is_acme_enabled() {
        eprintln!(
            "INFO rustwasgi: ACME enabled domains={:?} dir={:?}",
            config.acme_domains, config.acme_dir
        );
    }
    let serve_config = ServeConfig {
        app_spec,
        root_path: config.root_path.clone(),
        max_requests,
        graceful_timeout: Duration::from_secs(graceful_timeout.max(1)),
        heartbeat_interval: Duration::from_secs(heartbeat_interval.max(1)),
        access_log: config.access_log,
        keep_alive_secs: keep_alive,
        tls_state: tls_state.clone(),
        challenge_store: challenge_store.clone(),
        redirect_http_to_https: config.redirect_http_to_https,
    };
    // Keep one loop reference for the post-serve stop.
    let loop_for_stop = loop_obj.clone_ref(py);

    // ACME renewal task handle (lifecycle managed, outside GIL)
    let acme_handle: Option<crate::acme::renewal::RenewalTask> = None;
    py.detach(|| {
        rt.block_on(async {
            // --- ACME setup (outside HTTP path, before serve) --------------
            let _acme_task = if let Some(acme_cfg) =
                crate::acme::renewal::AcmeConfig::from_server_config(config)
            {
                if tls_state.is_none() {
                    eprintln!("WARN rustwasgi: ACME enabled but TLS not configured; ACME renewal disabled");
                    None
                } else {
                    let acme_tls = tls_state.clone().unwrap();
                    eprintln!(
                        "INFO rustwasgi: ACME renewal task starting for {:?}",
                        acme_cfg.domains
                    );
                    Some(crate::acme::renewal::RenewalTask::spawn(
                        acme_cfg,
                        acme_tls,
                        challenge_store.clone(),
                    ))
                }
            } else {
                None
            };
            // --- lifespan startup (before accepting) ----------------------
            let mode = LifespanMode::parse(&config.lifespan);
            let lifespan_state = match mode {
                LifespanMode::Off => None,
                LifespanMode::Auto | LifespanMode::On => {
                    match LifespanManager::startup(&bridge, &locals, &app_obj).await {
                        Ok(mgr) => {
                            eprintln!("INFO rustwasgi: lifespan startup complete");
                            let state =
                                Python::attach(|py| crate::runtime::clone_opt_py(py, &mgr.state));
                            store_lifespan_manager(mgr);
                            state
                        }
                        Err(LifespanError::Unsupported(msg)) if mode == LifespanMode::Auto => {
                            eprintln!("INFO rustwasgi: lifespan unsupported, continuing ({msg})");
                            None
                        }
                        Err(e) => {
                            return Err(format!("lifespan startup failed: {e}"));
                        }
                    }
                }
            };

            // --- serve ----------------------------------------------------
            server::serve(
                listeners,
                app_obj,
                bridge,
                locals.clone(),
                lifespan_state,
                serve_config,
                hooks,
            )
            .await?;

            // --- lifespan shutdown ----------------------------------------
            if let Some(mgr) = take_lifespan_manager() {
                mgr.shutdown(std::time::Duration::from_secs(10)).await;
                eprintln!("INFO rustwasgi: lifespan shutdown complete");
            }

            // --- cancel leftover loop tasks (forced-abort hygiene) --------
            cancel_pending_tasks(&locals).await;
            Ok::<(), String>(())
        })
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)
    })?;

    // --- stop + join the loop thread ---------------------------------------
    // Schedule stop with the GIL, then join WITHOUT it: the loop thread
    // needs the GIL to run the stop callback (joining while holding the
    // GIL deadlocks).
    runtime::stop_asyncio_loop(&loop_for_stop);
    if let Some(handle) = join_handle {
        py.detach(move || {
            let _ = handle.join();
        });
    }
    Ok(())
}

/// Cancel all pending tasks on the loop so shutdown never logs
/// "Task was destroyed but it is pending". Best effort, bounded.
async fn cancel_pending_tasks(locals: &TaskLocals) {
    let res: Result<(), String> = async {
        // Barrier first: a no-op round-trip forces the loop to run one full
        // pass, converting scheduled-but-unrun callbacks (e.g. in-flight
        // `into_future` submissions) into real Tasks — otherwise they sit in
        // the loop's ready queue past stop() and their coroutines die with
        // "was never awaited" warnings at teardown.
        let barrier = Python::attach(|py| {
            let code = c"async def _barrier():\n    return None\n";
            let module = pyo3::types::PyModule::from_code(
                py,
                code,
                c"rustwasgi_barrier.py",
                c"rustwasgi_barrier",
            )
            .map_err(|e| format!("{e}"))?;
            let coro = module
                .getattr("_barrier")
                .map_err(|e| format!("{e}"))?
                .call0()
                .map_err(|e| format!("{e}"))?;
            crate::asgi::into_future(locals, coro).map_err(|e| format!("{e}"))
        })?;
        let _ = tokio::time::timeout(Duration::from_secs(5), barrier).await;
        let coro = Python::attach(|py| {
            let asyncio = py.import("asyncio").map_err(|e| format!("{e}"))?;
            // all_tasks() must run ON the loop: wrap in a coroutine.
            let code = c"async def _cancel_all():\n    import asyncio\n    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]\n    for t in tasks:\n        t.cancel()\n    if tasks:\n        await asyncio.gather(*tasks, return_exceptions=True)\n    return len(tasks)\n";
            let module = pyo3::types::PyModule::from_code(
                py,
                code,
                c"rustwasgi_cleanup.py",
                c"rustwasgi_cleanup",
            )
            .map_err(|e| format!("{e}"))?;
            let coro = module
                .getattr("_cancel_all")
                .map_err(|e| format!("{e}"))?
                .call0()
                .map_err(|e| format!("{e}"))?;
            let _ = asyncio;
            crate::asgi::into_future(locals, coro).map_err(|e| format!("{e}"))
        })?;
        tokio::time::timeout(Duration::from_secs(5), coro)
            .await
            .map_err(|_| "cleanup timed out".to_string())?
            .map(|_| ())
            .map_err(|e| {
                Python::attach(|py| e.print(py));
                "cleanup failed".to_string()
            })
    }
    .await;
    if let Err(e) = res {
        eprintln!("WARN rustwasgi: loop cleanup: {e}");
    }
}

static LIFESPAN_MANAGER: std::sync::OnceLock<std::sync::Mutex<Option<LifespanManager>>> =
    std::sync::OnceLock::new();

fn store_lifespan_manager(mgr: LifespanManager) {
    let cell = LIFESPAN_MANAGER.get_or_init(|| std::sync::Mutex::new(None));
    *cell.lock().expect("lifespan lock") = Some(mgr);
}

fn take_lifespan_manager() -> Option<LifespanManager> {
    LIFESPAN_MANAGER
        .get()
        .and_then(|cell| cell.lock().expect("lifespan lock").take())
}

/// Diagnostic microbenchmark for §27: measures the raw `into_future`
/// round-trip (submit a no-op coroutine, await completion) at several
/// concurrency levels. Headless: builds its own loop thread + runtime, so it
/// never touches worker globals.
///
/// Returns `(idle_ms, per_level_json)` where levels cover 4..=64.
#[pyfunction]
fn _bench_bridge(py: Python<'_>, concurrency: usize, iters: usize) -> PyResult<String> {
    use std::time::Instant;

    // Headless loop thread (same construction as workers, no server).
    let asyncio = py.import("asyncio")?;
    let loop_obj: Py<PyAny> = asyncio.call_method0("new_event_loop")?.unbind();
    let locals = TaskLocals::new(loop_obj.bind(py).clone());
    let thread_loop = loop_obj.clone_ref(py);
    let handle = std::thread::Builder::new()
        .name("rustwasgi-bench-loop".to_string())
        .spawn(move || {
            Python::attach(|py| {
                let _ = py
                    .import("asyncio")
                    .and_then(|m| m.call_method1("set_event_loop", (thread_loop.bind(py),)))
                    .and_then(|_| thread_loop.bind(py).call_method0("run_forever"));
            });
        })
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("bench loop thread: {e}"))
        })?;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("bench runtime: {e}")))?;

    // A no-op coroutine factory, compiled once.
    let noop_fn: Py<PyAny> = Python::attach(|py| {
        let code = c"async def _noop():\n    return None\n";
        let module =
            pyo3::types::PyModule::from_code(py, code, c"rustwasgi_bench.py", c"rustwasgi_bench")?;
        Ok::<Py<PyAny>, PyErr>(module.getattr("_noop")?.unbind())
    })?;
    let levels = [1usize, 4, 8, 16, 20, 32, 64];
    let mut out = String::from("{");
    // block_on WITHOUT the GIL: the loop thread needs it to run the probes.
    py.detach(|| {
        rt.block_on(async {
            for (li, level) in levels.iter().enumerate() {
                let level = (*level).min(concurrency.max(1));
                let per_task = (iters / level).max(1);
                let start = Instant::now();
                let mut tasks = Vec::with_capacity(level);
                for _ in 0..level {
                    let locals_c = locals.clone();
                    let noop_c = Python::attach(|py| noop_fn.clone_ref(py));
                    tasks.push(tokio::spawn(async move {
                        for _ in 0..per_task {
                            let fut = Python::attach(|py| {
                                let coro = noop_c.bind(py).call0()?;
                                crate::asgi::into_future(&locals_c, coro)
                            });
                            let fut = match fut {
                                Ok(f) => f,
                                Err(_) => break,
                            };
                            if fut.await.is_err() {
                                break;
                            }
                        }
                    }));
                }
                for t in tasks {
                    let _ = t.await;
                }
                let total_ms = start.elapsed().as_secs_f64() * 1000.0;
                let total_ops = (per_task * level) as f64;
                if li > 0 {
                    out.push_str(", ");
                }
                out.push_str(&format!("\"{level}\": {:.3}", total_ms / total_ops));
                if level >= concurrency {
                    break;
                }
            }
        })
    });
    out.push('}');

    // Stop + join (GIL released for join).
    runtime::stop_asyncio_loop(&loop_obj);
    py.detach(move || {
        let _ = handle.join();
    });
    Ok(out)
}

/// Native module. The maturin `module-name` maps this to
/// `rustwasgi._rustwasgi`; the pure-Python `rustwasgi/__init__.py` re-exports.
#[pymodule]
fn _rustwasgi(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run, m)?)?;
    m.add_function(wrap_pyfunction!(run_worker, m)?)?;
    m.add_function(wrap_pyfunction!(_bench_bridge, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
