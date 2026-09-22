//! `rustwasgi._rustwasgi` native extension.
//!
//! Two serving paths share one core:
//!
//! - `run(app_spec, ...)` — standalone dev server: imports the app from a
//!   `"module:attr"` string and binds its own socket.
//! - `run_worker(app, listeners, ...)` — Gunicorn worker path: receives the
//!   already-loaded ASGI callable plus already-bound (dup'd) listener FDs.
//!
//! Both release the GIL while Tokio drives hyper; per-request Python work
//! re-acquires the GIL on blocking-pool threads and executes coroutines on
//! the dedicated asyncio loop thread (see `runtime.rs`).

mod asgi;
mod cli;
mod lifespan;
mod runtime;
mod server;
mod socket;
mod ws;

use std::time::Duration;

use pyo3::prelude::*;

use crate::cli::ServerConfig;
use crate::lifespan::{LifespanError, LifespanManager, LifespanMode};
use crate::server::{ServeConfig, WorkerHooks};
use crate::socket::{BoundListener, InheritedSocket};

/// Run the standalone ASGI server (blocking until SIGINT/SIGTERM).
#[pyfunction]
#[pyo3(signature = (app, *, host="127.0.0.1", port=8000, workers=1, log_level="info", root_path="", lifespan="auto", access_log=false, keep_alive=0))]
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
) -> PyResult<()> {
    let config = ServerConfig::new(
        app.clone(),
        host.to_string(),
        port,
        workers,
        log_level.to_string(),
        root_path.to_string(),
        lifespan.to_string(),
        access_log,
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

    let (app_obj, loop_obj, bridge) = setup_python(py, &app, &config)?;
    config.log_info(&format!("loaded ASGI application {app}"));
    // Build the serving runtime first: standalone sockets must be bound on
    // the same runtime that serves them (Tokio I/O is runtime-bound).
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("rustwasgi-worker")
        .build()
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("failed to start tokio runtime: {e}"))
        })?;
    let listeners = py
        .detach(|| rt.block_on(socket::bind_standalone(host, port)))
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    serve_process_on(
        py,
        rt,
        app_obj,
        loop_obj,
        bridge,
        listeners,
        app,
        &config,
        WorkerHooks {
            notify,
            is_alive,
            access: None,
        },
        take_loop_handle(),
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
#[pyo3(signature = (app, listeners, *, app_spec="gunicorn", root_path="", lifespan="auto", max_requests=0, graceful_timeout=30, heartbeat_interval=1, access_log=false, keep_alive=0, notify, is_alive, access=None))]
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
    notify: Py<PyAny>,
    is_alive: Py<PyAny>,
    access: Option<Py<PyAny>>,
) -> PyResult<()> {
    let mut inherited = Vec::with_capacity(listeners.len());
    for (family, fd, description) in listeners {
        // SAFETY: Gunicorn worker passed dup'd FDs (see socket.rs + gunicorn.py).
        inherited.push(unsafe { InheritedSocket::from_raw(family, fd, description) });
    }

    let config = ServerConfig::new(
        app_spec.to_string(),
        String::new(),
        0,
        1,
        "info".to_string(),
        root_path.to_string(),
        lifespan.to_string(),
        access_log,
    );
    // Clone the app object out of the bound reference while holding the GIL.
    let app_obj = app.clone().unbind();
    let bridge = asgi::Bridge::install(py)?;
    let (loop_obj, handle) = runtime::start_asyncio_loop(py)?;
    // Build the serving runtime in this (post-fork) worker, then convert the
    // inherited FDs into Tokio listeners *inside* its context (Tokio I/O is
    // runtime-bound).
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("rustwasgi-worker")
        .build()
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("failed to start tokio runtime: {e}"))
        })?;
    let bound = {
        let _guard = rt.enter();
        socket::bind_inherited(inherited).map_err(pyo3::exceptions::PyRuntimeError::new_err)?
    };
    // The loop thread is joined at the end of serve_process_on.
    serve_process_on(
        py,
        rt,
        app_obj,
        loop_obj,
        bridge,
        bound,
        app_spec.to_string(),
        &config,
        WorkerHooks {
            notify,
            is_alive,
            access,
        },
        Some(handle),
        max_requests,
        graceful_timeout,
        heartbeat_interval,
        keep_alive,
    )?;
    Ok(())
}

/// Import app + install bridge + start loop (standalone path).
fn setup_python(
    py: Python<'_>,
    app_spec: &str,
    _config: &ServerConfig,
) -> PyResult<(Py<PyAny>, Py<PyAny>, asgi::Bridge)> {
    let app_obj = runtime::import_app(py, app_spec)?;
    let bridge = asgi::Bridge::install(py)?;
    let (loop_obj, handle) = runtime::start_asyncio_loop(py)?;
    // Stash the handle where serve_process can join it: leak a box into a
    // module-global. (Only one server per process in standalone mode.)
    store_loop_handle(handle);
    Ok((app_obj, loop_obj, bridge))
}

static LOOP_HANDLE: std::sync::OnceLock<std::sync::Mutex<Option<std::thread::JoinHandle<()>>>> =
    std::sync::OnceLock::new();

fn store_loop_handle(handle: std::thread::JoinHandle<()>) {
    let cell = LOOP_HANDLE.get_or_init(|| std::sync::Mutex::new(None));
    *cell.lock().expect("loop handle lock") = Some(handle);
}

fn take_loop_handle() -> Option<std::thread::JoinHandle<()>> {
    LOOP_HANDLE
        .get()
        .and_then(|cell| cell.lock().expect("loop handle lock").take())
}

/// Shared orchestration on an already-built runtime: lifespan startup ->
/// serve -> lifespan shutdown -> loop stop/join.
#[allow(clippy::too_many_arguments)]
fn serve_process_on(
    py: Python<'_>,
    rt: tokio::runtime::Runtime,
    app_obj: Py<PyAny>,
    loop_obj: Py<PyAny>,
    bridge: asgi::Bridge,
    listeners: Vec<BoundListener>,
    app_spec: String,
    config: &ServerConfig,
    hooks: WorkerHooks,
    loop_handle: Option<std::thread::JoinHandle<()>>,
    max_requests: usize,
    graceful_timeout: u64,
    heartbeat_interval: u64,
    keep_alive: u64,
) -> PyResult<()> {
    // --- lifespan startup (before accepting) ------------------------------
    let mode = LifespanMode::parse(&config.lifespan);
    let lifespan_state: Option<Py<PyAny>> = match mode {
        LifespanMode::Off => None,
        LifespanMode::Auto | LifespanMode::On => {
            match LifespanManager::startup(py, &bridge, &loop_obj, &app_obj) {
                Ok(mgr) => {
                    eprintln!("INFO rustwasgi: lifespan startup complete");
                    let state = crate::runtime::clone_opt_py(py, &mgr.state);
                    store_lifespan_manager(mgr);
                    state
                }
                Err(LifespanError::Unsupported(msg)) if mode == LifespanMode::Auto => {
                    eprintln!("INFO rustwasgi: lifespan unsupported, continuing ({msg})");
                    None
                }
                Err(e) => {
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "lifespan startup failed: {e}"
                    )));
                }
            }
        }
    };

    // --- serve (GIL released) ----------------------------------------------
    let serve_config = ServeConfig {
        app_spec,
        root_path: config.root_path.clone(),
        max_requests,
        graceful_timeout: Duration::from_secs(graceful_timeout.max(1)),
        heartbeat_interval: Duration::from_secs(heartbeat_interval.max(1)),
        access_log: config.access_log,
        keep_alive_secs: keep_alive,
    };
    // Keep one loop reference for the post-serve stop; the other moves into
    // the serving closure.
    let loop_for_stop = loop_obj.clone_ref(py);
    py.detach(move || {
        let res = rt.block_on(server::serve(
            listeners,
            app_obj,
            loop_obj,
            bridge,
            lifespan_state,
            serve_config,
            hooks,
        ));
        // Bounded teardown: lingering keep-alive/idle tasks must not stall
        // worker exit (Gunicorn enforces graceful-timeout externally too).
        rt.shutdown_timeout(Duration::from_secs(5));
        res.map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("server error: {e}")))
    })?;

    // --- lifespan shutdown ---------------------------------------------------
    if let Some(mgr) = take_lifespan_manager() {
        mgr.shutdown(py, 10.0);
        eprintln!("INFO rustwasgi: lifespan shutdown complete");
    }

    // --- stop + join the loop thread -------------------------------------------
    // Schedule stop with the GIL, then join WITHOUT it: the loop thread
    // needs the GIL to run the stop callback (joining while holding the
    // GIL deadlocks).
    runtime::stop_asyncio_loop(&loop_for_stop);
    if let Some(handle) = loop_handle {
        // run_forever() returns after loop.stop(); the GIL is released
        // while joining so shutdown always makes progress.
        py.detach(move || {
            let _ = handle.join();
        });
    }
    Ok(())
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

/// Native module. The maturin `module-name` maps this to
/// `rustwasgi._rustwasgi`; the pure-Python `rustwasgi/__init__.py` re-exports.
#[pymodule]
fn _rustwasgi(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run, m)?)?;
    m.add_function(wrap_pyfunction!(run_worker, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
