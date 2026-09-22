//! ASGI Lifespan protocol (startup/shutdown + state propagation).
//!
//! Runs once per worker process, before accepting connections, on the SAME
//! loop and runtime as request processing:
//!
//! ```text
//! into_future(app(scope=lifespan)) --> app task on the loop
//! into_future(to_app.put(startup)) --> delivered
//! race: from_app.get() vs app completion vs timeout
//!   complete -> capture optional "state" for HTTP/WebSocket scopes
//!   failed   -> fatal startup error
//!   app exited first / timeout -> unsupported (auto continues, on is fatal)
//! ... serve ...
//! into_future(to_app.put(shutdown)) / race get vs timeout (best effort)
//! ```
//!
//! No threads, no blocking waits: every wait parks the calling task.

use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyDict};
use pyo3_async_runtimes::TaskLocals;

use crate::asgi::{Bridge, into_future};

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
    /// App does not implement lifespan (raised, exited silently, or timed out).
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

const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Manager bound to one worker's app + loop.
pub struct LifespanManager {
    locals: TaskLocals,
    to_app: Option<Py<PyAny>>,
    from_app: Option<Py<PyAny>>,
    /// State captured from `lifespan.startup.complete`, shared with scopes.
    pub state: Option<Py<PyAny>>,
}

impl LifespanManager {
    /// Run the startup handshake (async; GIL only for construction steps).
    pub async fn startup(
        bridge: &Bridge,
        locals: &TaskLocals,
        app: &Py<PyAny>,
    ) -> Result<Self, LifespanError> {
        // 1. Queues + receive/send wrappers, created on the loop.
        let init_fut = Python::attach(|py| -> Result<_, LifespanError> {
            let init_coro = bridge.init_lifespan_fn(py).call0().map_err(|e| {
                e.print(py);
                LifespanError::Unsupported("could not create lifespan channel".to_string())
            })?;
            into_future(locals, init_coro).map_err(|e| {
                e.print(py);
                LifespanError::Unsupported("could not submit lifespan init".to_string())
            })
        })?;
        let channel = init_fut.await.map_err(|e| {
            Python::attach(|py| e.print(py));
            LifespanError::Unsupported("lifespan init failed".to_string())
        })?;
        let (to_app, from_app, receive, send): (Py<PyAny>, Py<PyAny>, Py<PyAny>, Py<PyAny>) =
            Python::attach(|py| -> Result<_, LifespanError> {
                channel
                    .bind(py)
                    .extract::<(Py<PyAny>, Py<PyAny>, Py<PyAny>, Py<PyAny>)>()
                    .map_err(|e| {
                        e.print(py);
                        LifespanError::Unsupported("bad lifespan channel".to_string())
                    })
            })?;

        // 2. Submit the application with a lifespan scope.
        let app_fut = Python::attach(|py| -> Result<_, LifespanError> {
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
                .call1((app.bind(py), scope, receive.bind(py), send.bind(py)))
                .map_err(|e| {
                    e.print(py);
                    LifespanError::Unsupported("could not call lifespan app".to_string())
                })?;
            into_future(locals, app_coro).map_err(|e| {
                e.print(py);
                LifespanError::Unsupported("could not submit lifespan app".to_string())
            })
        })?;
        let mut app_fut = app_fut;

        // 3. Deliver lifespan.startup.
        Self::queue_put(locals, &to_app, "lifespan.startup")
            .await
            .map_err(LifespanError::Unsupported)?;

        // 4. Race the reply against early app exit and the timeout. An app
        // that raises/returns without answering is "unsupported", detected
        // as soon as its task ends — no full-timeout penalty.
        let get_fut = Self::queue_get(locals, &from_app);
        tokio::pin!(get_fut);
        let reply = tokio::select! {
            biased;
            r = &mut get_fut => Some(r),
            // The app exiting without answering MEANS "no lifespan support"
            // (e.g. a scope-type assert in a pure-HTTP app). That is routine,
            // not an error: one line, no traceback.
            r = &mut app_fut => {
                match r {
                    Ok(_) => eprintln!(
                        "INFO rustwasgi: lifespan app exited without replying (unsupported)"
                    ),
                    Err(e) => {
                        let name = Python::attach(|py| {
                            e.get_type(py)
                                .name()
                                .map(|n| n.to_string_lossy().into_owned())
                                .unwrap_or_else(|_| "?".to_string())
                        });
                        eprintln!(
                            "INFO rustwasgi: lifespan app raised {name} without replying (unsupported)"
                        );
                    }
                }
                None
            }
            _ = tokio::time::sleep(HANDSHAKE_TIMEOUT) => None,
        };
        match reply {
            Some(Ok(msg)) => {
                let msg_type: String = Python::attach(|py| {
                    msg.bind(py)
                        .get_item("type")
                        .ok()
                        .and_then(|v| v.extract().ok())
                        .unwrap_or_default()
                });
                match msg_type.as_str() {
                    "lifespan.startup.complete" => {
                        let state: Option<Py<PyAny>> = Python::attach(|py| {
                            msg.bind(py).get_item("state").ok().map(|v| v.unbind())
                        });
                        Ok(Self {
                            locals: locals.clone(),
                            to_app: Some(Python::attach(|py| to_app.clone_ref(py))),
                            from_app: Some(Python::attach(|py| from_app.clone_ref(py))),
                            state,
                        })
                    }
                    "lifespan.startup.failed" => {
                        let text: String = Python::attach(|py| {
                            msg.bind(py)
                                .get_item("message")
                                .ok()
                                .and_then(|v| v.extract().ok())
                                .unwrap_or_else(|| "unknown".to_string())
                        });
                        Err(LifespanError::Failed(text))
                    }
                    other => Err(LifespanError::Unsupported(format!(
                        "unexpected lifespan reply {other:?}"
                    ))),
                }
            }
            Some(Err(e)) => {
                Python::attach(|py| e.print(py));
                Err(LifespanError::Unsupported(
                    "lifespan reply failed".to_string(),
                ))
            }
            None => Err(LifespanError::Unsupported(
                "app exited without a lifespan reply (or timed out)".to_string(),
            )),
        }
    }

    /// Run shutdown handshake (best effort; logs but never raises).
    pub async fn shutdown(&self, timeout: std::time::Duration) {
        let (to_app, from_app) = match (&self.to_app, &self.from_app) {
            (Some(t), Some(f)) => (
                Python::attach(|py| t.clone_ref(py)),
                Python::attach(|py| f.clone_ref(py)),
            ),
            _ => return,
        };
        if Self::queue_put(&self.locals, &to_app, "lifespan.shutdown")
            .await
            .is_err()
        {
            return;
        }
        let get_fut = Self::queue_get(&self.locals, &from_app);
        tokio::pin!(get_fut);
        let reply = tokio::select! {
            biased;
            r = &mut get_fut => Some(r),
            _ = tokio::time::sleep(timeout) => None,
        };
        match reply {
            Some(Ok(msg)) => {
                let msg_type: String = Python::attach(|py| {
                    msg.bind(py)
                        .get_item("type")
                        .ok()
                        .and_then(|v| v.extract().ok())
                        .unwrap_or_default()
                });
                if msg_type == "lifespan.shutdown.failed" {
                    let text: String = Python::attach(|py| {
                        msg.bind(py)
                            .get_item("message")
                            .ok()
                            .and_then(|v| v.extract().ok())
                            .unwrap_or_else(|| "unknown".to_string())
                    });
                    eprintln!("ERROR rustwasgi: lifespan.shutdown.failed: {text}");
                }
            }
            Some(Err(e)) => {
                Python::attach(|py| e.print(py));
                eprintln!("WARN rustwasgi: lifespan shutdown reply failed");
            }
            None => {
                eprintln!("WARN rustwasgi: lifespan shutdown timed out");
            }
        }
    }

    /// Deliver `{"type": kind}` asynchronously (GIL only for construction).
    async fn queue_put(locals: &TaskLocals, queue: &Py<PyAny>, kind: &str) -> Result<(), String> {
        let put_fut = Python::attach(|py| {
            let msg = PyDict::new(py);
            msg.set_item("type", kind)
                .map_err(|e| format!("dict: {e}"))?;
            let put_coro = queue
                .bind(py)
                .call_method1("put", (msg,))
                .map_err(|e| format!("queue.put: {e}"))?;
            into_future(locals, put_coro).map_err(|e| {
                e.print(py);
                "lifespan submit failed".to_string()
            })
        })?;
        tokio::time::timeout(std::time::Duration::from_secs(10), put_fut)
            .await
            .map_err(|_| "lifespan put timed out".to_string())?
            .map(|_| ())
            .map_err(|e| {
                Python::attach(|py| e.print(py));
                "lifespan put failed".to_string()
            })
    }

    /// Await one `queue.get()` asynchronously (GIL only for construction).
    async fn queue_get(locals: &TaskLocals, queue: &Py<PyAny>) -> PyResult<Py<PyAny>> {
        let get_fut = Python::attach(|py| {
            let get_coro = queue.bind(py).call_method0("get")?;
            into_future(locals, get_coro)
        })?;
        get_fut.await
    }
}
