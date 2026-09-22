//! ASGI <-> hyper adapter with no thread hops on the request path.
//!
//! # Integration model (`pyo3-async-runtimes`, Tokio backend)
//!
//! - Python awaitable -> Rust future: `into_future_with_locals(&locals, coro)`
//!   schedules the coroutine onto our explicit asyncio loop (fire-and-forget
//!   signal) and returns a future completed through a `oneshot` channel. The
//!   awaiting Tokio task parks — no OS thread is blocked, no condition
//!   variable exists, the GIL is not held while waiting.
//! - Rust future -> Python awaitable: `tokio::future_into_py_with_locals`
//!   (backpressured `send()` waits only). Spawned onto the worker's own Tokio
//!   runtime via `init_with_runtime` — no second runtime exists.
//! - `receive()` is a plain Python coroutine awaiting an `asyncio.Queue`
//!   (bodyless requests get a pre-seeded terminal message, no queue traffic).
//! - `send()` is a plain Python coroutine: `SendSink._push` does a non-blocking
//!   `try_send`; only a FULL channel returns an awaitable (genuine bounded
//!   backpressure, GIL-free wait).
//!
//! Per `GET /` request: scope build (GIL, µs) + 2 loop wakeups (init, app) +
//! completion wakeup. No `spawn_blocking`, no `Future.result`, no threads.
//!
//! ```text
//! hyper body chunks --Tokio feeder--> into_future(queue.put) --> asyncio.Queue
//!                                                                          | await
//!                                                              Python await receive()
//! Python await send(msg) --try_send--> tokio mpsc (bound 16)
//!       \--on Full--> future_into_py(send) --> capacity (GIL-free)
//!                                                  | rx.recv().await
//! hyper Channel body <--pump task--------------+--> TCP socket
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use pyo3::exceptions::{PyOSError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes, PyDict, PyList, PySequence, PyTuple};
use pyo3_async_runtimes::{TaskLocals, tokio as par_tokio};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

/// Bound of the application -> server response channel.
///
/// Genuine backpressure buffer: when full, `send()` asynchronously waits for
/// the pump task (GIL released). No unbounded buffering anywhere.
pub const RESPONSE_CHANNEL_BOUND: usize = 16;

/// Bound of the server -> application request queue (`asyncio.Queue(maxsize)`).
///
/// The Tokio feeder awaits `queue.put` through `into_future` (no threads), so
/// a slow application stalls the feeder, which stalls hyper, which applies
/// TCP backpressure. Worst-case memory per request is roughly
/// `MAXSIZE x max-chunk-size`.
pub const REQUEST_QUEUE_MAXSIZE: usize = 64;

/// Per-put timeout for request-body queue puts (fail-safe, not a hot path).
pub const REQUEST_PUT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Awaitable Rust future resolving to the Python app's return (usually None)
/// or its exception (traceback preserved in `PyErr`).
pub type AppFuture = Pin<Box<dyn Future<Output = PyResult<Py<PyAny>>> + Send>>;

/// Events flowing from the Python application to the hyper response pump.
#[derive(Debug)]
pub enum ResponseEvent {
    Start {
        status: u16,
        headers: Vec<(Vec<u8>, Vec<u8>)>,
    },
    Body {
        data: Vec<u8>,
        more_body: bool,
    },
}

/// Rust end of `send()`, exposed to Python.
///
/// Created per request; wrapped by the Python `_Sender` class. `_push` never
/// blocks: it validates, `try_send`s, and only on a FULL channel builds a
/// `future_into_py` awaitable whose GIL-free wait provides backpressure.
/// Closed channel / lost connection -> `OSError` (ASGI 2.4+ send-after-
/// disconnect semantics).
#[pyclass]
pub struct SendSink {
    tx: mpsc::Sender<ResponseEvent>,
    disconnected: Arc<AtomicBool>,
    locals: TaskLocals,
}

#[pymethods]
impl SendSink {
    /// Returns `None` when the message was accepted immediately, or a Python
    /// awaitable to wait on when the channel is full.
    fn _push<'py>(
        &self,
        py: Python<'py>,
        message: Bound<'py, PyAny>,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        if self.disconnected.load(Ordering::SeqCst) {
            return Err(PyOSError::new_err(
                "ASGI send(): connection closed (send after disconnect)",
            ));
        }
        let event = parse_send_message(&message)?;
        match self.tx.try_send(event) {
            Ok(()) => {
                crate::metrics::inc(crate::metrics::C_SEND_FAST);
                Ok(None)
            }
            Err(TrySendError::Full(event)) => {
                crate::metrics::inc(crate::metrics::C_SEND_FULL);
                let tx = self.tx.clone();
                let fut = async move {
                    tx.send(event)
                        .await
                        .map_err(|_| PyOSError::new_err("ASGI send(): connection closed"))?;
                    Ok(())
                };
                Ok(Some(par_tokio::future_into_py_with_locals(
                    py,
                    self.locals.clone(),
                    fut,
                )?))
            }
            Err(TrySendError::Closed(_)) => {
                Err(PyOSError::new_err("ASGI send(): connection closed"))
            }
        }
    }
}

impl SendSink {
    pub fn new(
        tx: mpsc::Sender<ResponseEvent>,
        disconnected: Arc<AtomicBool>,
        locals: TaskLocals,
    ) -> Self {
        Self {
            tx,
            disconnected,
            locals,
        }
    }
}

/// Validate one `send()` message per the ASGI HTTP spec.
///
/// - unknown `type` -> error; missing/invalid `status` or non-bytes headers
///   are rejected; header names must be lowercase bytes, no pseudo-headers;
/// - an app-supplied `transfer-encoding` header is stripped (server owns
///   framing); `trailers: True` / `http.response.trailers` are rejected with
///   a clear error (hyper has no stable HTTP/1 trailer support);
/// - extra unknown keys are ignored (ASGI error-handling rules).
fn parse_send_message(message: &Bound<'_, PyAny>) -> PyResult<ResponseEvent> {
    let msg_type: String = message
        .get_item("type")
        .map_err(|_| PyValueError::new_err("send(): message missing 'type'"))?
        .extract()
        .map_err(|_| PyValueError::new_err("send(): message 'type' must be str"))?;

    match msg_type.as_str() {
        "http.response.start" => {
            let status: u16 = message
                .get_item("status")
                .map_err(|_| PyValueError::new_err("send(): http.response.start missing 'status'"))?
                .extract()
                .map_err(|_| PyValueError::new_err("send(): 'status' must be an integer"))?;
            if !(100..=599).contains(&status) {
                return Err(PyValueError::new_err(format!(
                    "send(): invalid HTTP status {status}"
                )));
            }
            if let Ok(trailers) = message.get_item("trailers") {
                let wanted: bool = trailers.extract().unwrap_or(false);
                if wanted {
                    return Err(PyValueError::new_err(
                        "send(): HTTP trailers are not supported by this server",
                    ));
                }
            }
            let mut headers = Vec::new();
            if let Ok(raw) = message.get_item("headers") {
                for item in raw
                    .try_iter()
                    .map_err(|_| PyValueError::new_err("send(): 'headers' must be iterable"))?
                {
                    let pair = item?;
                    let (name, value) = extract_header_pair(&pair)?;
                    if name.iter().any(|b| b.is_ascii_uppercase()) {
                        return Err(PyValueError::new_err(
                            "send(): response header names must be lowercase",
                        ));
                    }
                    if name.first() == Some(&b':') {
                        return Err(PyValueError::new_err(
                            "send(): pseudo-headers are not allowed",
                        ));
                    }
                    if name == b"transfer-encoding" {
                        // Server owns framing; ignore per ASGI spec.
                        continue;
                    }
                    headers.push((name, value));
                }
            }
            Ok(ResponseEvent::Start { status, headers })
        }
        "http.response.body" => {
            let data = match message.get_item("body") {
                Ok(b) => as_bytes(&b)
                    .map_err(|_| PyValueError::new_err("send(): 'body' must be bytes"))?,
                Err(_) => Vec::new(),
            };
            let more_body: bool = message
                .get_item("more_body")
                .ok()
                .and_then(|v| v.extract::<bool>().ok())
                .unwrap_or(false);
            Ok(ResponseEvent::Body { data, more_body })
        }
        "http.response.trailers" => Err(PyValueError::new_err(
            "send(): HTTP trailers are not supported by this server",
        )),
        other => Err(PyValueError::new_err(format!(
            "send(): unsupported message type {other:?}"
        ))),
    }
}

/// Extract one `(name, value)` header pair; accepts tuples or lists of bytes.
/// `str` values are rejected (ASGI requires byte strings).
fn extract_header_pair(pair: &Bound<'_, PyAny>) -> PyResult<(Vec<u8>, Vec<u8>)> {
    let seq = pair.cast::<PySequence>().map_err(|_| {
        PyValueError::new_err("send(): headers must be 2-item (name, value) sequences")
    })?;
    if seq.len().unwrap_or(0) != 2 {
        return Err(PyValueError::new_err(
            "send(): headers must be 2-item (name, value) sequences",
        ));
    }
    let name = as_bytes(&seq.get_item(0)?)
        .map_err(|_| PyValueError::new_err("send(): header names must be bytes"))?;
    let value = as_bytes(&seq.get_item(1)?)
        .map_err(|_| PyValueError::new_err("send(): header values must be bytes"))?;
    Ok((name, value))
}

/// Strict bytes extraction: `bytes` / `bytearray` accepted, `str` rejected.
fn as_bytes(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(b) = obj.cast::<PyBytes>() {
        return Ok(b.as_bytes().to_vec());
    }
    if let Ok(b) = obj.cast::<PyByteArray>() {
        return Ok(unsafe { b.as_bytes().to_vec() });
    }
    if obj.is_none() {
        return Ok(Vec::new());
    }
    Err(PyValueError::new_err("expected bytes"))
}

/// Python channel objects plus the ASGI 2/3 compatibility wrapper.
///
/// `receive` awaits an `asyncio.Queue` (or a pre-seeded terminal message for
/// bodyless requests — no queue traffic at all); `send` forwards into the
/// Rust channel via `SendSink` (awaiting only under real backpressure).
const BRIDGE_CODE: &str = r#"
import asyncio
import inspect

_TERMINAL = {"type": "http.request", "body": b"", "more_body": False}
_STREAMING = object()


class _Receive:
    """await receive() -> next http.request / http.disconnect message."""

    def __init__(self, queue, first=_STREAMING):
        self._queue = queue
        self._first = first

    async def __call__(self):
        if self._first is not _STREAMING:
            message, self._first = self._first, _STREAMING
            return message
        return await self._queue.get()


class _Sender:
    """await send(msg) -> validated + forwarded to Rust (may raise OSError)."""

    def __init__(self, sink):
        self._sink = sink

    async def __call__(self, message):
        pending = self._sink._push(message)
        if pending is not None:
            await pending


async def _init_http_channel(receive_cls, send_cls, sink, maxsize, terminal):
    queue = asyncio.Queue(maxsize=maxsize)
    first = _TERMINAL if terminal else _STREAMING
    return queue, receive_cls(queue, first), send_cls(sink)


async def _init_lifespan_channel():
    to_app = asyncio.Queue()
    from_app = asyncio.Queue()

    async def receive():
        return await to_app.get()

    async def send(message):
        await from_app.put(message)

    return to_app, from_app, receive, send


async def _init_ws_channel(send_cls, sink, maxsize):
    to_app = asyncio.Queue(maxsize=maxsize)
    return to_app, _Receive(to_app), send_cls(sink)


async def _put_nowait(queue, message):
    queue.put_nowait(message)


async def _call_app(app, scope, receive, send):
    """Invoke an ASGI 2 or ASGI 3 application (cf. asgiref.compatibility)."""
    if asyncio.iscoroutinefunction(app):
        await app(scope, receive, send)
        return
    if inspect.isfunction(app) or inspect.ismethod(app) or inspect.isbuiltin(app):
        # Plain `def app(scope)` -> legacy ASGI 2 double-callable.
        instance = app(scope)
        result = instance(receive, send)
        if inspect.isawaitable(result):
            await result
        else:
            raise TypeError("ASGI 2 application instance did not return an awaitable")
        return
    # Callable object / class (e.g. Starlette/FastAPI instance): ASGI 3,
    # with a TypeError fallback to the ASGI 2 double-callable shape.
    try:
        result = app(scope, receive, send)
    except TypeError:
        instance = app(scope)
        result = instance(receive, send)
    if inspect.isawaitable(result):
        await result
    else:
        raise TypeError("ASGI application did not return an awaitable")
"#;

/// Installed bridge callables (unbound, GIL-independent handles).
#[derive(Debug)]
pub struct Bridge {
    recv_cls: Py<PyAny>,
    send_cls: Py<PyAny>,
    init_http_fn: Py<PyAny>,
    init_lifespan_fn: Py<PyAny>,
    init_ws_fn: Py<PyAny>,
    put_nowait_fn: Py<PyAny>,
    call_app_fn: Py<PyAny>,
    terminal_msg: Py<PyAny>,
    streaming_sentinel: Py<PyAny>,
    /// `asyncio.Queue()` constructor is loop-free since Python 3.10, allowing
    /// single-submit channel setup. Older interpreters use the init coroutine.
    pub direct_queue: bool,
}

impl Clone for Bridge {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            recv_cls: self.recv_cls.clone_ref(py),
            send_cls: self.send_cls.clone_ref(py),
            init_http_fn: self.init_http_fn.clone_ref(py),
            init_lifespan_fn: self.init_lifespan_fn.clone_ref(py),
            init_ws_fn: self.init_ws_fn.clone_ref(py),
            put_nowait_fn: self.put_nowait_fn.clone_ref(py),
            call_app_fn: self.call_app_fn.clone_ref(py),
            terminal_msg: self.terminal_msg.clone_ref(py),
            streaming_sentinel: self.streaming_sentinel.clone_ref(py),
            direct_queue: self.direct_queue,
        })
    }
}

impl Bridge {
    /// Execute [`BRIDGE_CODE`] and keep the channel/app-invocation callables.
    pub fn install(py: Python<'_>) -> PyResult<Self> {
        let code = std::ffi::CString::new(BRIDGE_CODE).expect("bridge code");
        let module = PyModule::from_code(py, &code, c"rustwasgi_bridge.py", c"rustwasgi_bridge")?;
        let sys = py.import("sys")?;
        let version: (u8, u8) = (|| {
            let info = sys.getattr("version_info")?;
            let major: u8 = info.get_item(0)?.extract()?;
            let minor: u8 = info.get_item(1)?.extract()?;
            PyResult::Ok((major, minor))
        })()
        .unwrap_or((3, 8));
        Ok(Self {
            recv_cls: module.getattr("_Receive")?.unbind(),
            send_cls: module.getattr("_Sender")?.unbind(),
            init_http_fn: module.getattr("_init_http_channel")?.unbind(),
            init_lifespan_fn: module.getattr("_init_lifespan_channel")?.unbind(),
            init_ws_fn: module.getattr("_init_ws_channel")?.unbind(),
            put_nowait_fn: module.getattr("_put_nowait")?.unbind(),
            call_app_fn: module.getattr("_call_app")?.unbind(),
            terminal_msg: module.getattr("_TERMINAL")?.unbind(),
            streaming_sentinel: module.getattr("_STREAMING")?.unbind(),
            direct_queue: version >= (3, 10),
        })
    }

    pub fn init_lifespan_fn<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.init_lifespan_fn.bind(py).clone()
    }

    pub fn init_ws_fn<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.init_ws_fn.bind(py).clone()
    }

    pub fn put_nowait_fn<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.put_nowait_fn.bind(py).clone()
    }

    /// The generic `_Sender` wrapper also fronts WebSocket sinks: it only
    /// calls `sink._push(message)`, and parsing is sink-specific.
    pub fn ws_send_cls<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.send_cls.bind(py).clone()
    }

    pub fn call_app_fn<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.call_app_fn.bind(py).clone()
    }
}

/// Convert a Python awaitable into an awaitable Rust future bound to our
/// loop. The GIL is needed only to build/schedule; the returned future
/// parks the Tokio task (oneshot/waker) with no threads and no GIL.
pub fn into_future(locals: &TaskLocals, awaitable: Bound<'_, PyAny>) -> PyResult<AppFuture> {
    crate::metrics::inc(crate::metrics::C_INTO_FUTURE);
    let fut = pyo3_async_runtimes::into_future_with_locals(locals, awaitable)?;
    Ok(Box::pin(fut))
}

/// Flat HTTP request metadata extracted from hyper (cheap to move).
/// The body itself streams separately; `has_body == false` selects the
/// zero-hop terminal fast path (no feeder task at all).
#[derive(Debug, Clone)]
pub struct RequestData {
    pub method: String,
    pub http_version: String,
    pub scheme: String,
    pub path: String,
    pub raw_path: Vec<u8>,
    pub query_string: Vec<u8>,
    pub root_path: String,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub has_body: bool,
    pub client_host: String,
    pub client_port: u16,
    pub server_host: String,
    pub server_port: u16,
}
/// Handles for one in-flight request: the queue the (inline or pump-task)
/// feeder fills and the application future awaited directly (no monitor
/// thread — `Err` carries the traceback in `PyErr`).
pub struct EstablishedCall {
    pub queue: Py<PyAny>,
    pub app_fut: AppFuture,
}

/// Build scope, create the queue-backed channel, and submit the application.
///
/// The entire construction (sink, queue, receive, send, scope, app coroutine,
/// into_future scheduling) happens under a SINGLE GIL acquisition: under
/// contention each acquisition can wait on the loop thread, so one wait
/// replaces six. Every wait after that is a waker-parked `into_future`
/// await. No threads involved.
pub async fn establish_http_call(
    bridge: &Bridge,
    locals: &TaskLocals,
    app: &Py<PyAny>,
    lifespan_state: Option<&Py<PyAny>>,
    req: &RequestData,
    tx: mpsc::Sender<ResponseEvent>,
    disconnected: Arc<AtomicBool>,
) -> Result<EstablishedCall, String> {
    if bridge.direct_queue {
        // Direct path (Python 3.10+): everything in one attach, then a
        // single into_future for the app. Bodyless requests pre-seed the
        // terminal message: no feeder, no queue traffic at all.
        struct Ready {
            queue: Py<PyAny>,
            app_fut: AppFuture,
        }
        let ready = crate::metrics::attach_measured(|py| {
            let sink: Py<SendSink> =
                pyo3::Py::new(py, SendSink::new(tx, disconnected, locals.clone()))?;
            let asyncio = py.import("asyncio")?;
            let queue = asyncio.call_method1("Queue", (REQUEST_QUEUE_MAXSIZE,))?;
            let first = if req.has_body {
                bridge.streaming_sentinel.bind(py).clone()
            } else {
                bridge.terminal_msg.bind(py).clone()
            };
            let receive = bridge.recv_cls.bind(py).call1((queue.clone(), first))?;
            let send = bridge.send_cls.bind(py).call1((sink,))?;
            let scope = build_http_scope(py, lifespan_state, req)?;
            let app_coro = bridge
                .call_app_fn(py)
                .call1((app.bind(py), scope, receive, send))?;
            let app_fut = into_future(locals, app_coro)?;
            Ok::<_, PyErr>(Ready {
                queue: queue.unbind(),
                app_fut,
            })
        })
        .map_err(|e| {
            Python::attach(|py| e.print(py));
            "ASGI channel init failed".to_string()
        })?;
        crate::metrics::inc(crate::metrics::C_ATTACH);
        Ok(EstablishedCall {
            queue: ready.queue,
            app_fut: ready.app_fut,
        })
    } else {
        // Legacy path (<3.10): queue must be born on the loop thread.
        let init_fut = Python::attach(|py| {
            let sink: Py<SendSink> =
                pyo3::Py::new(py, SendSink::new(tx, disconnected, locals.clone()))?;
            let init_coro = bridge.init_http_fn.bind(py).call1((
                bridge.recv_cls.bind(py),
                bridge.send_cls.bind(py),
                sink,
                REQUEST_QUEUE_MAXSIZE,
                !req.has_body, // `terminal`
            ))?;
            into_future(locals, init_coro)
        })
        .map_err(|e| {
            Python::attach(|py| e.print(py));
            "ASGI channel init failed".to_string()
        })?;
        crate::metrics::inc(crate::metrics::C_ATTACH);
        let channel = init_fut.await.map_err(|e| {
            Python::attach(|py| e.print(py));
            "ASGI channel init failed".to_string()
        })?;
        let (queue, receive, send): (Py<PyAny>, Py<PyAny>, Py<PyAny>) = Python::attach(|py| {
            let (queue, receive, send): (Bound<'_, PyAny>, Bound<'_, PyAny>, Bound<'_, PyAny>) =
                channel.bind(py).extract()?;
            Ok::<_, PyErr>((queue.unbind(), receive.unbind(), send.unbind()))
        })
        .map_err(|e| {
            Python::attach(|py| e.print(py));
            "ASGI channel init failed".to_string()
        })?;
        crate::metrics::inc(crate::metrics::C_ATTACH);
        // 2. Build the ASGI HTTP scope (spec_version 2.5), constructed once.
        // 3. Submit the application (ASGI 2/3 compatible wrapper).
        let app_fut: AppFuture = Python::attach(|py| {
            let scope = build_http_scope(py, lifespan_state, req)?;
            let receive_b = receive.bind(py);
            let send_b = send.bind(py);
            let app_coro =
                bridge
                    .call_app_fn(py)
                    .call1((app.bind(py), scope, receive_b, send_b))?;
            into_future(locals, app_coro)
        })
        .map_err(|e| {
            Python::attach(|py| e.print(py));
            "ASGI submit failed".to_string()
        })?;
        crate::metrics::inc(crate::metrics::C_ATTACH);
        Ok(EstablishedCall { queue, app_fut })
    }
}

fn build_http_scope<'py>(
    py: Python<'py>,
    lifespan_state: Option<&Py<PyAny>>,
    req: &RequestData,
) -> PyResult<Bound<'py, PyDict>> {
    use pyo3::types::IntoPyDict;

    let scope = PyDict::new(py);
    scope.set_item("type", "http")?;
    scope.set_item(
        "asgi",
        [("version", "3.0"), ("spec_version", "2.5")].into_py_dict(py)?,
    )?;
    scope.set_item("http_version", &req.http_version)?;
    scope.set_item("method", &req.method)?;
    scope.set_item("scheme", &req.scheme)?;
    scope.set_item("path", &req.path)?;
    scope.set_item("raw_path", PyBytes::new(py, &req.raw_path))?;
    scope.set_item("query_string", PyBytes::new(py, &req.query_string))?;
    scope.set_item("root_path", &req.root_path)?;
    let headers = PyList::empty(py);
    for (k, v) in &req.headers {
        headers.append((PyBytes::new(py, k), PyBytes::new(py, v)))?;
    }
    scope.set_item("headers", headers)?;
    {
        let h: Bound<'_, PyAny> = req.client_host.clone().into_pyobject(py)?.into_any();
        let p: Bound<'_, PyAny> = req.client_port.into_pyobject(py)?.into_any();
        scope.set_item("client", PyTuple::new(py, [h, p])?)?;
    }
    {
        let h: Bound<'_, PyAny> = req.server_host.clone().into_pyobject(py)?.into_any();
        let p: Bound<'_, PyAny> = req.server_port.into_pyobject(py)?.into_any();
        scope.set_item("server", PyTuple::new(py, [h, p])?)?;
    }
    scope.set_item("extensions", PyDict::new(py))?;
    match lifespan_state {
        Some(state) => scope.set_item("state", state.bind(py))?,
        None => scope.set_item("state", PyDict::new(py))?,
    }
    Ok(scope)
}

/// Enqueue one message with genuine async backpressure (bounded queue).
/// GIL held only to build the `put` coroutine; the wait parks the Tokio task.
pub async fn feed_message(
    locals: &TaskLocals,
    queue: &Py<PyAny>,
    build: impl FnOnce(Python<'_>) -> PyResult<Py<PyAny>>,
    timeout: std::time::Duration,
) -> Result<(), String> {
    crate::metrics::inc(crate::metrics::C_ATTACH);
    let put_fut = Python::attach(|py| {
        let msg = build(py)?;
        let put_coro = queue.bind(py).call_method1("put", (msg,))?;
        into_future(locals, put_coro)
    })
    .map_err(|e| {
        Python::attach(|py| e.print(py));
        "request queue submit failed".to_string()
    })?;
    tokio::time::timeout(timeout, put_fut)
        .await
        .map_err(|_| "request queue put timed out".to_string())?
        .map(|_| ())
        .map_err(|e| {
            Python::attach(|py| e.print(py));
            "request queue put failed".to_string()
        })
}

/// Deliver one `http.disconnect` (client went away). Fire-and-forget waiter:
/// schedules a tiny `put_nowait` wrapper and waits at most 5s so shutdown
/// paths can never hang on it. No threads involved.
pub async fn feed_disconnect(bridge: &Bridge, locals: &TaskLocals, queue: &Py<PyAny>) {
    let res: Result<(), String> = async {
        let coro = Python::attach(|py| {
            let msg = PyDict::new(py);
            msg.set_item("type", "http.disconnect")
                .map_err(|e| format!("dict: {e}"))?;
            bridge
                .put_nowait_fn(py)
                .call1((queue.bind(py), msg))
                .map_err(|e| {
                    e.print(py);
                    "disconnect submit failed".to_string()
                })
                .and_then(|c| {
                    into_future(locals, c).map_err(|e| {
                        e.print(py);
                        "disconnect submit failed".to_string()
                    })
                })
        })?;
        tokio::time::timeout(std::time::Duration::from_secs(5), coro)
            .await
            .map_err(|_| "disconnect wait timed out".to_string())?
            .map(|_| ())
            .map_err(|e| {
                Python::attach(|py| e.print(py));
                "disconnect wait failed".to_string()
            })
    }
    .await;
    if let Err(e) = res {
        eprintln!("WARN rustwasgi: {e}");
    }
}

/// Minimal percent-decoder for the ASGI `path` (decoded) vs `raw_path`.
/// Invalid sequences are passed through literally; lossy UTF-8 fallback.
pub fn percent_decode(input: &str) -> String {
    let mut out = Vec::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(h), Some(l)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2]))
        {
            out.push(h * 16 + l);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
