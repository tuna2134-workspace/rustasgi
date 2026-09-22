//! Python asyncio integration.
//!
//! # Chosen approach
//!
//! A Python coroutine cannot be synchronously awaited from Rust. Instead of
//! inventing a new executor, we integrate with the real `asyncio` event loop:
//!
//! 1. At server startup, a dedicated OS thread creates a fresh
//!    `asyncio.new_event_loop()` and runs `loop.run_forever()` there. That
//!    thread is the *only* place where Python coroutines execute.
//! 2. Each hyper request (a Tokio task) collects the HTTP request, then hands
//!    the ASGI call to `tokio::task::spawn_blocking` so the Tokio async
//!    runtime is never blocked. Inside the blocking thread we acquire the GIL,
//!    call `app(scope, receive, send)` to get a coroutine, and submit it with
//!    `asyncio.run_coroutine_threadsafe(coro, loop)`.
//! 3. We wait on the returned `concurrent.futures.Future` via
//!    `future.result(timeout)`. CPython releases the GIL while waiting on the
//!    underlying condition variable, so the loop thread can run the coroutine
//!    concurrently. GIL is therefore held only for short object
//!    construction/inspection, never across network I/O.
//!
//! This gives correct execution of FastAPI `async def` endpoints (including
//! `await asyncio.sleep(...)`) without blocking hyper.

use pyo3::prelude::*;
use std::thread::{self, JoinHandle};

/// Import the ASGI callable from a `"module:attr"` specifier.
///
/// Supports dotted module paths (`"pkg.mod:app"`). Ensures `os.getcwd()` is on
/// `sys.path` so `app:app` next to the invocation directory works. Must be
/// called with the GIL held.
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

/// Start the dedicated asyncio loop thread.
///
/// Takes the GIL token so it can release the GIL while waiting for the loop
/// thread to report readiness (the child needs the GIL to create the loop;
/// blocking on the channel while holding the GIL would deadlock).
///
/// Returns the loop object (for `run_coroutine_threadsafe`) and the thread's
/// join handle. The thread runs `loop.run_forever()` until the process exits;
/// on server shutdown we stop the loop from the calling thread via
/// `loop.call_soon_threadsafe(loop.stop)`.
pub fn start_asyncio_loop(py: Python<'_>) -> PyResult<(Py<PyAny>, JoinHandle<()>)> {
    let (tx, rx) = std::sync::mpsc::channel::<PyResult<Py<PyAny>>>();
    let handle = thread::Builder::new()
        .name("rustwasgi-asyncio".to_string())
        .spawn(move || {
            let created = Python::attach(|py| -> PyResult<Py<PyAny>> {
                let asyncio = py.import("asyncio")?;
                let new_loop = asyncio.call_method0("new_event_loop")?;
                asyncio.call_method1("set_event_loop", (&new_loop,))?;
                Ok(new_loop.unbind())
            });
            match created {
                Ok(loop_obj) => {
                    // Hand one clone to the parent; keep one for run_forever.
                    let for_parent = Python::attach(|py| loop_obj.clone_ref(py));
                    let _ = tx.send(Ok(for_parent));
                    // Drive the loop. Holds the GIL while running Python
                    // code; asyncio releases it during I/O waits.
                    Python::attach(|py| match loop_obj.bind(py).call_method0("run_forever") {
                        Ok(_) => {}
                        Err(e) => e.print(py),
                    });
                }
                Err(e) => {
                    Python::attach(|py| e.print(py));
                    let _ = tx.send(Err(pyo3::exceptions::PyRuntimeError::new_err(
                        "failed to create asyncio event loop",
                    )));
                }
            }
        })
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "failed to spawn asyncio thread: {e}"
            ))
        })?;
    // Release the GIL while waiting: the child needs it to create the loop.
    // (`move` the receiver in — it is Send but not Sync.)
    let loop_obj = py.detach(move || rx.recv()).map_err(|_| {
        pyo3::exceptions::PyRuntimeError::new_err("asyncio thread died on startup")
    })??;
    Ok((loop_obj, handle))
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

/// Ask a running loop (from another thread) to stop.
///
/// Holds the GIL only for scheduling `loop.stop()`; callers that then join
/// the loop thread MUST release the GIL while joining (the thread needs it
/// to run the stop callback — joining with the GIL held deadlocks).
pub fn stop_asyncio_loop(loop_obj: &Py<PyAny>) {
    Python::attach(|py| {
        let running: bool = loop_obj
            .bind(py)
            .call_method0("is_running")
            .and_then(|v| v.extract())
            .unwrap_or(false);
        if running && let Ok(stop) = loop_obj.getattr(py, "stop") {
            let _ = loop_obj.call_method1(py, "call_soon_threadsafe", (stop,));
        }
    });
}
