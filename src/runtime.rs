//! Python asyncio integration without thread rendezvous.
//!
//! # Model (see also `asgi.rs`)
//!
//! Each worker process owns exactly one asyncio event loop, created on the
//! calling thread with `asyncio.new_event_loop()` (construction needs no
//! running loop) and driven by one dedicated OS thread running
//! `loop.run_forever()`. Startup synchronizes on the loop actually running
//! (poll rendezvous, GIL released while waiting) so the first bridge calls
//! never observe a not-yet-started loop.
//!
//! Cross-thread interaction uses ONLY `pyo3-async-runtimes`:
//!
//! - Python awaitable -> Rust future: `into_future_with_locals(&locals, coro)`
//!   schedules the coroutine onto our explicit loop (fire-and-forget signal)
//!   and returns a future completed through a `oneshot` channel. Awaiting it
//!   from Tokio never blocks an OS thread and never touches a condition
//!   variable.
//! - Rust future -> Python awaitable: `tokio::future_into_py_with_locals`
//!   (used for backpressured `send()` waits). These spawn onto the worker's
//!   own Tokio runtime via `init_with_runtime` — no second runtime exists.
//!
//! GIL discipline: attach only to create objects / schedule work (microsecond
//! scale), never across `.await`.

use std::thread::{self, JoinHandle};

use pyo3::prelude::*;
use pyo3_async_runtimes::TaskLocals;

/// Handles for one worker's asyncio loop. `locals` pins
/// `into_future_with_locals` to this loop from any thread.
pub struct LoopHandle {
    pub loop_obj: Py<PyAny>,
    pub locals: TaskLocals,
    pub join_handle: Option<JoinHandle<()>>,
}

/// Import the ASGI callable from a `"module:attr"` specifier.
///
/// Supports dotted module paths (`"pkg.mod:app"`), dotted attribute paths,
/// and no-arg factory calls (`"pkg:create_app()"`). Uses Python's import
/// machinery through PyO3; ensures CWD is importable. Must be called with
/// the GIL held.
pub fn import_app(py: Python<'_>, spec: &str) -> PyResult<Py<PyAny>> {
    // Make sure CWD is importable (maturin develop / CLI UX).
    let sys = py.import("sys")?;
    let path = sys.getattr("path")?;
    let os = py.import("os")?;
    let cwd: String = os.call_method0("getcwd")?.extract()?;
    let contains: bool = path
        .call_method1("__contains__", (&cwd,))?
        .extract()
        .unwrap_or(true);
    if !contains {
        path.call_method1("insert", (0, &cwd))?;
    }

    let (module_name, attr) = spec.split_once(':').ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err(
            "app must be in the form 'module:attr', e.g. 'app:app'",
        )
    })?;
    if module_name.is_empty() || attr.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "app must be in the form 'module:attr', e.g. 'app:app'",
        ));
    }
    let importlib = py.import("importlib")?;
    let mut current = importlib.call_method1("import_module", (module_name,))?;
    // Dotted attribute path, with optional no-arg factory call `attr()`.
    let call_factory = attr.ends_with("()");
    let attr_path = attr.strip_suffix("()").unwrap_or(attr);
    for part in attr_path.split('.') {
        if part.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid empty attribute in app spec",
            ));
        }
        current = current.getattr(part)?;
    }
    if call_factory {
        current = current.call0()?;
    }
    Ok(current.unbind())
}

/// Start the asyncio loop thread.
///
/// The loop object is created HERE (no running loop needed for construction)
/// and `TaskLocals` is pinned to it, so `into_future_with_locals` works from
/// any thread. The spawned thread sets the loop for itself and drives it;
/// it exits when the loop is stopped.
///
/// This returns as soon as the thread is spawned — use
/// [`wait_loop_running`] (once a Tokio runtime exists) to rendezvous on the
/// loop actually running before the first bridge submission. Without that,
/// early `into_future` calls would observe `is_running() == false` and
/// wrongly conclude the loop is *stopping* — flaky under fork/load, where
/// the child thread is slow to start.
pub fn start_asyncio_loop(py: Python<'_>) -> PyResult<LoopHandle> {
    let asyncio = py.import("asyncio")?;
    let loop_obj: Py<PyAny> = asyncio.call_method0("new_event_loop")?.unbind();
    let locals = TaskLocals::new(loop_obj.bind(py).clone());

    let thread_loop = Python::attach(|py| loop_obj.clone_ref(py));
    let handle = thread::Builder::new()
        .name("rustwasgi-asyncio".to_string())
        .spawn(move || {
            Python::attach(|py| {
                // Register the loop for this thread, then drive it. Holds
                // the GIL while running Python; asyncio releases it in waits.
                let asyncio = match py.import("asyncio") {
                    Ok(m) => m,
                    Err(e) => {
                        e.print(py);
                        return;
                    }
                };
                if let Err(e) = asyncio.call_method1("set_event_loop", (thread_loop.bind(py),)) {
                    e.print(py);
                    return;
                }
                match thread_loop.bind(py).call_method0("run_forever") {
                    Ok(_) => {}
                    Err(e) => e.print(py),
                }
            });
        })
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "failed to spawn asyncio thread: {e}"
            ))
        })?;

    Ok(LoopHandle {
        loop_obj,
        locals,
        join_handle: Some(handle),
    })
}

/// Wait until the loop thread has actually entered `run_forever`.
///
/// Must be called after a Tokio runtime exists (uses its timer) and before
/// the first bridge submission. Each poll holds the GIL only for one
/// attribute call; the sleeps are async and hold no GIL, and this itself
/// runs with the GIL released — so the child thread is never starved by
/// this wait, and no OS thread is ever blocked on Python.
pub fn wait_loop_running(
    py: Python<'_>,
    rt: &tokio::runtime::Runtime,
    loop_obj: &Py<PyAny>,
) -> PyResult<()> {
    let loop_c = loop_obj.clone_ref(py);
    py.detach(|| {
        rt.block_on(async {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
            loop {
                let running = Python::attach(|py| {
                    loop_c
                        .bind(py)
                        .call_method0("is_running")
                        .and_then(|v| v.extract::<bool>())
                        .unwrap_or(false)
                });
                if running {
                    return Ok(());
                }
                if tokio::time::Instant::now() > deadline {
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(
                        "asyncio loop thread did not start within 10s",
                    ));
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
    })
}

/// Ask a running loop (from another thread) to stop.
///
/// Fire-and-forget `call_soon_threadsafe` with NO wait — the only direct
/// threadsafe call left in our code, used solely for shutdown (never on the
/// request path). Callers joining the loop thread MUST release the GIL while
/// joining (the thread needs it to run the stop callback).
///
/// KeyboardInterrupt transparency: our own signal watcher already drives
/// shutdown, so a KI raised by the interpreter inside this attach must not
/// be swallowed (that would lose the stop and hang the join) nor abort the
/// shutdown — retry the schedule instead. A single ^C raises exactly once,
/// so the retry proceeds normally.
pub fn stop_asyncio_loop(loop_obj: &Py<PyAny>) {
    Python::attach(|py| {
        let running: bool = loop_obj
            .bind(py)
            .call_method0("is_running")
            .and_then(|v| v.extract())
            .unwrap_or(false);
        if !running {
            return;
        }
        let Ok(stop) = loop_obj.getattr(py, "stop") else {
            return;
        };
        for _ in 0..3 {
            let stop_ref = stop.clone_ref(py);
            match loop_obj.call_method1(py, "call_soon_threadsafe", (stop_ref,)) {
                Ok(_) => return,
                Err(e) if e.is_instance_of::<pyo3::exceptions::PyKeyboardInterrupt>(py) => {
                    // Signal consumed by the raise; shutdown is already in
                    // progress via the Rust watcher — retry the schedule.
                    continue;
                }
                Err(_) => return,
            }
        }
    });
}

/// Clone an optional `Py` handle.
pub fn clone_opt_py(py: Python<'_>, obj: &Option<Py<PyAny>>) -> Option<Py<PyAny>> {
    obj.as_ref().map(|o| o.clone_ref(py))
}

/// RAII in-flight request guard: increments on creation, decrements on drop.
/// Move into the task that owns the request's full lifetime (including
/// background body pumps and WebSocket drivers) so graceful drain cannot
/// end while bytes are still streaming.
pub struct FlightGuard {
    counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl FlightGuard {
    pub fn new(counter: &std::sync::Arc<std::sync::atomic::AtomicUsize>) -> Self {
        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self {
            counter: counter.clone(),
        }
    }
}

impl Drop for FlightGuard {
    fn drop(&mut self) {
        self.counter
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}
