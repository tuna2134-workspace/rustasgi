//! ASGI Lifespan protocol (startup/shutdown + state propagation).
//!
//! Runs once per worker process, before accepting connections:
//!
//! ```text
//! submit _call_app(app, {"type": "lifespan", ...}) to the asyncio loop
//! put {"type": "lifespan.startup"} into the app's queue
//! wait for {"type": "lifespan.startup.complete" | "lifespan.startup.failed"}
//!   complete -> capture optional "state" for HTTP/WebSocket scopes
//!   failed   -> fatal startup error
//!   app raised / no reply -> "unsupported", server may continue in auto mode
//! ... serve ...
//! put {"type": "lifespan.shutdown"}; wait for complete/failed (best effort)
//! ```
//!
//! All queue operations go through `asyncio.run_coroutine_threadsafe` + a
//! blocking wait (GIL released while waiting), so no Tokio worker is ever
//! blocked and the loop thread is never starved.

use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyDict};

use crate::asgi::Bridge;

/// How to handle lifespan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifespanMode {
    Off,
    Auto,
    On,
}

impl LifespanMode {
    pub fn parse(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "off" => LifespanMode::Off,
            "on" => LifespanMode::On,
            _ => LifespanMode::Auto,
        }
    }
}

/// Lifespan failure kinds.
#[derive(Debug)]
pub enum LifespanError {
    /// App does not implement lifespan (raised, or stayed silent).
    /// Fatal only when mode is `on`.
    Unsupported(String),
    /// App explicitly sent `lifespan.startup.failed`. Always fatal.
    Failed(String),
}

impl std::fmt::Display for LifespanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LifespanError::Unsupported(m) => write!(f, "lifespan unsupported: {m}"),
            LifespanError::Failed(m) => write!(f, "lifespan startup failed: {m}"),
        }
    }
}

const HANDSHAKE_TIMEOUT: f64 = 30.0;

/// Manager bound to one worker's app + loop. Created before serving.
pub struct LifespanManager {
    loop_obj: Py<PyAny>,
    to_app: Option<Py<PyAny>>,
    from_app: Option<Py<PyAny>>,
    /// The application task; kept alive across the serving lifetime so the
    /// app's lifespan context (e.g. FastAPI `@asynccontextmanager`) stays
    /// open until shutdown.
    _app_future: Option<Py<PyAny>>,
    /// State captured from `lifespan.startup.complete`, shared with scopes.
    pub state: Option<Py<PyAny>>,
}

impl LifespanManager {
    /// Run the startup handshake (blocking; GIL released while waiting).
    /// `bridge`, `loop_obj` and `app` are only borrowed via the GIL token.
    pub fn startup(
        py: Python<'_>,
        bridge: &Bridge,
        loop_obj: &Py<PyAny>,
        app: &Py<PyAny>,
    ) -> Result<Self, LifespanError> {
        let loop_bound = loop_obj.bind(py);

        // 1. Create queues + receive/send wrappers on the loop thread.
        let init_coro = bridge.init_lifespan_fn(py).call0().map_err(|e| {
            e.print(py);
            LifespanError::Unsupported("could not create lifespan channel".to_string())
        })?;
        let init_future = Bridge::submit(py, &loop_bound.clone(), init_coro).map_err(|e| {
            e.print(py);
            LifespanError::Unsupported("could not submit lifespan init".to_string())
        })?;
        let channel: Bound<'_, PyAny> = init_future
            .bind(py)
            .call_method1("result", (10.0,))
            .map_err(|e| {
                e.print(py);
                LifespanError::Unsupported("lifespan init timed out".to_string())
            })?;
        let (to_app, from_app, receive, send) = channel
            .extract::<(
                Bound<'_, PyAny>,
                Bound<'_, PyAny>,
                Bound<'_, PyAny>,
                Bound<'_, PyAny>,
            )>()
            .map_err(|e| {
                e.print(py);
                LifespanError::Unsupported("bad lifespan channel".to_string())
            })?;
        let (to_app, from_app) = (to_app.unbind(), from_app.unbind());

        // 2. Submit the application with a lifespan scope.
        let scope = PyDict::new(py);
        scope
            .set_item("type", "lifespan")
            .and_then(|_| {
                scope.set_item(
                    "asgi",
                    [("version", "3.0"), ("spec_version", "2.5")].into_py_dict(py)?,
                )
            })
            .and_then(|_| scope.set_item("state", PyDict::new(py)))
            .map_err(|e| {
                e.print(py);
                LifespanError::Unsupported("could not build lifespan scope".to_string())
            })?;
        let app_coro = bridge
            .call_app_fn(py)
            .call1((app.bind(py), scope, receive, send))
            .map_err(|e| {
                e.print(py);
                LifespanError::Unsupported("could not call lifespan app".to_string())
            })?;
        let app_future = Bridge::submit(py, loop_bound, app_coro).map_err(|e| {
            e.print(py);
            LifespanError::Unsupported("could not submit lifespan app".to_string())
        })?;

        // 3. Send lifespan.startup, wait for the reply.
        let mut mgr = Self {
            loop_obj: loop_obj.clone_ref(py),
            to_app: Some(to_app.clone_ref(py)),
            from_app: Some(from_app.clone_ref(py)),
            _app_future: Some(app_future.clone_ref(py)),
            state: None,
        };
        // (app_future clone above keeps the task referenced from Rust too.)
        let _ = app_future;
        if let Err(msg) = Self::queue_put(py, &mgr.loop_obj, &to_app, "lifespan.startup") {
            return Err(LifespanError::Unsupported(msg));
        }
        // Wait for the reply, but fail fast when the app task already exited
        // (raised/returned without answering): that means "no lifespan
        // support" and must not cost the full handshake timeout.
        match Self::wait_startup_reply(py, &mgr.loop_obj, &from_app, &app_future) {
            Ok(msg) => {
                let msg_type: String = msg
                    .bind(py)
                    .get_item("type")
                    .ok()
                    .and_then(|v| v.extract().ok())
                    .unwrap_or_default();
                match msg_type.as_str() {
                    "lifespan.startup.complete" => {
                        let state: Option<Py<PyAny>> =
                            msg.bind(py).get_item("state").ok().map(|v| v.unbind());
                        mgr.state = crate::runtime::clone_opt_py(py, &state);
                        Ok(mgr)
                    }
                    "lifespan.startup.failed" => {
                        let text: String = msg
                            .bind(py)
                            .get_item("message")
                            .ok()
                            .and_then(|v| v.extract().ok())
                            .unwrap_or_else(|| "unknown".to_string());
                        Err(LifespanError::Failed(text))
                    }
                    other => Err(LifespanError::Unsupported(format!(
                        "unexpected lifespan reply {other:?}"
                    ))),
                }
            }
            Err(msg) => {
                let _ = app_future.bind(py).call_method1("cancel", ());
                Err(LifespanError::Unsupported(msg))
            }
        }
    }

    /// Run shutdown handshake (best effort; logs but never raises).
    pub fn shutdown(&self, py: Python<'_>, timeout_secs: f64) {
        let (to_app, from_app) = match (&self.to_app, &self.from_app) {
            (Some(t), Some(f)) => (t.clone_ref(py), f.clone_ref(py)),
            _ => return, // startup never completed; nothing to shut down.
        };
        if Self::queue_put(py, &self.loop_obj, &to_app, "lifespan.shutdown").is_err() {
            return;
        }
        match Self::queue_get(py, &self.loop_obj, &from_app, timeout_secs) {
            Ok(msg) => {
                let msg_type: String = msg
                    .bind(py)
                    .get_item("type")
                    .ok()
                    .and_then(|v| v.extract().ok())
                    .unwrap_or_default();
                if msg_type == "lifespan.shutdown.failed" {
                    let text: String = msg
                        .bind(py)
                        .get_item("message")
                        .ok()
                        .and_then(|v| v.extract().ok())
                        .unwrap_or_else(|| "unknown".to_string());
                    eprintln!("ERROR rustwasgi: lifespan.shutdown.failed: {text}");
                }
            }
            Err(msg) => {
                eprintln!("WARN rustwasgi: lifespan shutdown: {msg}");
            }
        }
        if let Some(fut) = &self._app_future {
            let _ = fut.bind(py).call_method1("cancel", ());
        }
    }

    /// Submit `queue.put({"type": kind})` and wait (GIL released in wait).
    fn queue_put(
        py: Python<'_>,
        loop_obj: &Py<PyAny>,
        queue: &Py<PyAny>,
        kind: &str,
    ) -> Result<(), String> {
        let loop_bound = loop_obj.bind(py);
        let msg = PyDict::new(py);
        msg.set_item("type", kind)
            .map_err(|e| format!("dict: {e}"))?;
        let put_coro = queue
            .bind(py)
            .call_method1("put", (msg,))
            .map_err(|e| format!("queue.put: {e}"))?;
        let fut = Bridge::submit(py, loop_bound, put_coro).map_err(|e| format!("submit: {e}"))?;
        fut.bind(py)
            .call_method1("result", (10.0,))
            .map(|_| ())
            .map_err(|e| {
                e.print(py);
                "queue.put failed".to_string()
            })
    }

    /// Wait for the startup reply: submit ONE `queue.get()` and poll it in
    /// short slices so an already-dead app task (no lifespan support) is
    /// detected in ~0.5s instead of after the full handshake timeout.
    /// Submitting once (not once per slice) avoids stranding orphaned `get`
    /// tasks on the loop; the future is cancelled on give-up.
    fn wait_startup_reply(
        py: Python<'_>,
        loop_obj: &Py<PyAny>,
        from_app: &Py<PyAny>,
        app_future: &Py<PyAny>,
    ) -> Result<Py<PyAny>, String> {
        let loop_bound = loop_obj.bind(py);
        let get_coro = from_app
            .bind(py)
            .call_method0("get")
            .map_err(|e| format!("queue.get: {e}"))?;
        let fut = Bridge::submit(py, loop_bound, get_coro).map_err(|e| format!("submit: {e}"))?;
        let fut_ref = fut.bind(py);
        let start = std::time::Instant::now();
        loop {
            match fut_ref.call_method1("result", (0.5,)) {
                Ok(msg) => return Ok(msg.unbind()),
                Err(e) if is_timeout_error(py, &e) => {
                    let done: bool = app_future
                        .bind(py)
                        .call_method0("done")
                        .and_then(|v| v.extract())
                        .unwrap_or(false);
                    if done {
                        let _ = fut_ref.call_method0("cancel");
                        return Err("app exited without a lifespan reply (unsupported)".to_string());
                    }
                    if start.elapsed().as_secs_f64() >= HANDSHAKE_TIMEOUT {
                        let _ = fut_ref.call_method0("cancel");
                        return Err(
                            "lifespan reply timed out (app may not support lifespan)".to_string()
                        );
                    }
                }
                Err(e) => {
                    let _ = fut_ref.call_method0("cancel");
                    e.print(py);
                    return Err("lifespan reply failed".to_string());
                }
            }
        }
    }

    /// Submit `queue.get()` and wait up to `timeout` (GIL released in wait).
    /// The future is cancelled on failure so no orphaned `get` task lingers
    /// on the loop (a destroyed-pending task logs noise at loop close).
    fn queue_get(
        py: Python<'_>,
        loop_obj: &Py<PyAny>,
        queue: &Py<PyAny>,
        timeout: f64,
    ) -> Result<Py<PyAny>, String> {
        let loop_bound = loop_obj.bind(py);
        let get_coro = queue
            .bind(py)
            .call_method0("get")
            .map_err(|e| format!("queue.get: {e}"))?;
        let fut = Bridge::submit(py, loop_bound, get_coro).map_err(|e| format!("submit: {e}"))?;
        let fut_ref = fut.bind(py);
        match fut_ref.call_method1("result", (timeout,)) {
            Ok(msg) => Ok(msg.unbind()),
            Err(e) => {
                let _ = fut_ref.call_method0("cancel");
                // Timeouts just mean "no lifespan support"; don't dump traces.
                if is_timeout_error(py, &e) {
                    Err("lifespan reply timed out (app may not support lifespan)".to_string())
                } else {
                    e.print(py);
                    Err("lifespan reply failed".to_string())
                }
            }
        }
    }
}

/// True for `concurrent.futures.TimeoutError` (and `asyncio.TimeoutError`,
/// which is an alias in 3.11+).
fn is_timeout_error(py: Python<'_>, e: &PyErr) -> bool {
    e.get_type(py)
        .name()
        .map(|n| {
            let n = n.to_string_lossy();
            n.contains("Timeout")
        })
        .unwrap_or(false)
}
