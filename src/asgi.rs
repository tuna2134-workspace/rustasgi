//! ASGI <-> hyper adapter.
//!
//! Responsibilities:
//! - build the ASGI `scope` dict from a hyper request (+ socket addrs),
//! - provide per-request `receive` / `send` callables backed by async
//!   channels (never buffered end-to-end),
//! - invoke `await app(scope, receive, send)` on the dedicated asyncio loop
//!   thread and translate the captured response start into a hyper response.
//!
//! # Streaming design
//!
//! ```text
//! hyper body stream (Tokio task)
//!        |  per chunk, via loop.call_soon_threadsafe(queue.put_nowait, msg)
//!        v
//! asyncio.Queue  (unbounded; see known limitations)
//!        |  await queue.get()
//!        v
//! Python `await receive()`  -> {"type": "http.request", ...}
//!
//! Python `await send(msg)`
//!        |  SendSink._push (releases GIL while waiting for capacity)
//!        v
//! tokio::sync::mpsc (BOUND = RESPONSE_CHANNEL_BOUND, real backpressure)
//!        |  rx.recv().await
//!        v
//! hyper response Body channel -> TCP socket
//! ```
//!
//! `receive()` is a real async operation on an `asyncio.Queue` fed
//! incrementally: the application coroutine is submitted *before* the request
//! body has arrived, so large uploads stream through `http.request` messages
//! with `more_body=True/False`. hyper decodes `Transfer-Encoding: chunked`
//! itself, so the app only ever sees plain body bytes.
//!
//! `send()` forwards each message into a **bounded** Tokio mpsc channel
//! ([`RESPONSE_CHANNEL_BOUND`]). When the client is slow and the channel is
//! full, the loop thread parks (GIL released) until capacity frees up —
//! genuine backpressure instead of unbounded buffering. Response bodies are
//! forwarded chunk-by-chunk into hyper's streaming body; nothing is
//! concatenated in memory.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use pyo3::exceptions::{PyOSError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyBool, PyByteArray, PyBytes, PyDict, PyList, PySequence, PyTuple};
use tokio::sync::mpsc;

/// Bound of the application -> server response channel.
///
/// This is the documented backpressure buffer: at most this many response
/// messages may be in flight between the Python app and the hyper response
/// pump. When full, `send()` waits (GIL released) for the network task to
/// catch up.
pub const RESPONSE_CHANNEL_BOUND: usize = 16;

/// Bound of the server -> application request queue (`asyncio.Queue(maxsize)`).
///
/// The feeder uses a blocking `queue.put` (via `run_coroutine_threadsafe`),
/// so a slow application stalls the feeder, which stalls hyper, which applies
/// TCP backpressure to the client. Worst-case memory per request is roughly
/// `MAXSIZE x max-chunk-size`.
pub const REQUEST_QUEUE_MAXSIZE: usize = 64;

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
/// Created per request in Rust, wrapped by the Python `_Sender` class.
/// `_push` is called on the asyncio loop thread (never inside a Tokio runtime
/// worker), so `blocking_send` is safe; the GIL is released while waiting for
/// channel capacity so one slow client cannot stall other requests' Python
/// execution.
#[pyclass]
pub struct SendSink {
    tx: mpsc::Sender<ResponseEvent>,
    disconnected: Arc<AtomicBool>,
}

#[pymethods]
impl SendSink {
    fn _push(&self, py: Python<'_>, message: Bound<'_, PyAny>) -> PyResult<()> {
        if self.disconnected.load(Ordering::SeqCst) {
            return Err(PyOSError::new_err(
                "ASGI send(): connection closed (send after disconnect)",
            ));
        }
        let event = parse_send_message(&message)?;
        // Release the GIL while applying backpressure.
        py.detach(|| self.tx.blocking_send(event))
            .map_err(|_| PyOSError::new_err("ASGI send(): connection closed"))?;
        Ok(())
    }
}

impl SendSink {
    pub fn new(tx: mpsc::Sender<ResponseEvent>, disconnected: Arc<AtomicBool>) -> Self {
        Self { tx, disconnected }
    }
}

/// Validate one `send()` message per the ASGI HTTP spec.
///
/// - unknown `type` -> error; missing keys with defaults are tolerated, but
///   missing/invalid `status` or non-bytes headers are rejected;
/// - header names must be lowercase bytes, must not be pseudo-headers;
/// - an app-supplied `transfer-encoding` header is stripped: the server owns
///   HTTP framing (ASGI spec);
/// - `trailers: True` / `http.response.trailers` are rejected with a clear
///   error (documented limitation: hyper's server API has no stable trailer
///   support for HTTP/1.1 responses);
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
        // SAFETY: copy under the GIL; no Python code runs during the copy.
        return Ok(unsafe { b.as_bytes().to_vec() });
    }
    if obj.is_none() {
        return Ok(Vec::new());
    }
    Err(PyValueError::new_err("expected bytes"))
}

/// Python source for the per-request channel objects plus the ASGI 2/3
/// compatibility wrapper. `receive` awaits an `asyncio.Queue` fed by Rust;
/// `send` forwards into the Rust channel via `SendSink`.
const BRIDGE_CODE: &str = r#"
import asyncio
import inspect


class _Receive:
    """await receive() -> next fed http.request / http.disconnect message."""

    def __init__(self, queue):
        self._queue = queue

    async def __call__(self):
        return await self._queue.get()


class _Sender:
    """await send(msg) -> validated + forwarded to Rust (may raise OSError)."""

    def __init__(self, sink):
        self._sink = sink

    async def __call__(self, message):
        self._sink._push(message)


async def _init_http_channel(receive_cls, send_cls, sink, maxsize):
    queue = asyncio.Queue(maxsize=maxsize)
    return queue, receive_cls(queue), send_cls(sink)


async def _init_lifespan_channel():
    to_app = asyncio.Queue()
    from_app = asyncio.Queue()

    async def receive():
        return await to_app.get()

    async def send(message):
        await from_app.put(message)

    return to_app, from_app, receive, send


async def _init_ws_channel(send_cls, sink):
    to_app = asyncio.Queue()
    return to_app, _Receive(to_app), send_cls(sink)


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
    call_app_fn: Py<PyAny>,
}

impl Clone for Bridge {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            recv_cls: self.recv_cls.clone_ref(py),
            send_cls: self.send_cls.clone_ref(py),
            init_http_fn: self.init_http_fn.clone_ref(py),
            init_lifespan_fn: self.init_lifespan_fn.clone_ref(py),
            init_ws_fn: self.init_ws_fn.clone_ref(py),
            call_app_fn: self.call_app_fn.clone_ref(py),
        })
    }
}

impl Bridge {
    /// Execute [`BRIDGE_CODE`] and keep the channel/app-invocation callables.
    pub fn install(py: Python<'_>) -> PyResult<Self> {
        let code = std::ffi::CString::new(BRIDGE_CODE).expect("bridge code");
        let module = PyModule::from_code(py, &code, c"rustwasgi_bridge.py", c"rustwasgi_bridge")?;
        Ok(Self {
            recv_cls: module.getattr("_Receive")?.unbind(),
            send_cls: module.getattr("_Sender")?.unbind(),
            init_http_fn: module.getattr("_init_http_channel")?.unbind(),
            init_lifespan_fn: module.getattr("_init_lifespan_channel")?.unbind(),
            init_ws_fn: module.getattr("_init_ws_channel")?.unbind(),
            call_app_fn: module.getattr("_call_app")?.unbind(),
        })
    }

    pub fn init_lifespan_fn<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.init_lifespan_fn.bind(py).clone()
    }

    pub fn init_ws_fn<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.init_ws_fn.bind(py).clone()
    }

    /// The generic `_Sender` wrapper also fronts WebSocket sinks: it only
    /// calls `sink._push(message)`, and parsing is sink-specific.
    pub fn ws_send_cls<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.send_cls.bind(py).clone()
    }

    pub fn call_app_fn<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.call_app_fn.bind(py).clone()
    }

    /// Submit `coro` to the loop from any thread; returns the
    /// `concurrent.futures.Future`. Caller must hold the GIL only for the
    /// submit itself.
    pub fn submit(
        py: Python<'_>,
        loop_obj: &Bound<'_, PyAny>,
        coro: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let asyncio = py.import("asyncio")?;
        let future = asyncio.call_method1("run_coroutine_threadsafe", (coro, loop_obj))?;
        Ok(future.unbind())
    }
}

/// Flat HTTP request metadata extracted from hyper (cheap to move into
/// `spawn_blocking`). The body itself streams separately.
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
    pub client_host: String,
    pub client_port: u16,
    pub server_host: String,
    pub server_port: u16,
}

/// Handles for one in-flight request after channel setup on the loop thread:
/// - `app_future`: `concurrent.futures.Future` for the running application;
/// - `queue`: the `asyncio.Queue` the feeder pushes `http.request` messages to.
pub struct EstablishedCall {
    pub app_future: Py<PyAny>,
    pub queue: Py<PyAny>,
}

/// Build scope, create the queue-backed channel, and submit the application.
/// Blocks the calling (blocking-pool) thread while waiting for the tiny init
/// coroutine; the GIL is released during those waits.
pub fn establish_http_call(
    bridge: &Bridge,
    loop_obj: &Py<PyAny>,
    app: &Py<PyAny>,
    lifespan_state: Option<&Py<PyAny>>,
    req: &RequestData,
    sink: Py<SendSink>,
) -> Result<EstablishedCall, String> {
    Python::attach(|py| {
        establish_inner(py, bridge, loop_obj, app, lifespan_state, req, sink).map_err(|e| {
            e.print(py);
            format!("ASGI setup failed: {e}")
        })
    })
}

fn establish_inner(
    py: Python<'_>,
    bridge: &Bridge,
    loop_obj: &Py<PyAny>,
    app: &Py<PyAny>,
    lifespan_state: Option<&Py<PyAny>>,
    req: &RequestData,
    sink: Py<SendSink>,
) -> PyResult<EstablishedCall> {
    let loop_bound = loop_obj.bind(py);

    // 1. Create the queue + receive/send pair on the loop thread.
    let init_coro = bridge.init_http_fn.bind(py).call1((
        bridge.recv_cls.bind(py),
        bridge.send_cls.bind(py),
        sink,
        REQUEST_QUEUE_MAXSIZE,
    ))?;
    // `submit` borrows py; drop the future's GIL need during the wait by
    // scoping: future.result() releases the GIL internally while waiting.
    let init_future = Bridge::submit(py, &loop_bound.clone(), init_coro)?;
    let channel: Bound<'_, PyAny> = init_future.bind(py).call_method1("result", (10.0,))?;
    let (queue, receive, send): (Bound<'_, PyAny>, Bound<'_, PyAny>, Bound<'_, PyAny>) =
        channel.extract()?;

    // 2. Build the ASGI HTTP scope (spec_version 2.5).
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

    // 3. Submit the application (ASGI 2/3 compatible wrapper).
    let app_coro = bridge
        .call_app_fn
        .bind(py)
        .call1((app.bind(py), scope, receive, send))?;
    let app_future = Bridge::submit(py, loop_bound, app_coro)?;
    Ok(EstablishedCall {
        app_future,
        queue: queue.unbind(),
    })
}

/// Enqueue one `http.request` message, waiting for queue capacity.
///
/// This is the request-side backpressure path: the queue is bounded
/// ([`REQUEST_QUEUE_MAXSIZE`]), so a slow application stalls the feeder,
/// which stalls hyper, which applies TCP backpressure. Blocks the calling
/// (blocking-pool) thread; the GIL is released while waiting.
pub fn feed_request_message_sync(
    loop_obj: &Py<PyAny>,
    queue: &Py<PyAny>,
    body: &[u8],
    more_body: bool,
) -> Result<(), String> {
    Python::attach(|py| {
        let loop_bound = loop_obj.bind(py);
        let msg = PyDict::new(py);
        msg.set_item("type", "http.request")
            .and_then(|_| msg.set_item("body", PyBytes::new(py, body)))
            .and_then(|_| msg.set_item("more_body", PyBool::new(py, more_body)))
            .map_err(|e| format!("dict: {e}"))?;
        let put_coro = queue
            .bind(py)
            .call_method1("put", (msg,))
            .map_err(|e| format!("queue.put: {e}"))?;
        let fut = Bridge::submit(py, loop_bound, put_coro).map_err(|e| format!("submit: {e}"))?;
        let fut_ref = fut.bind(py);
        fut_ref
            .call_method1("result", (60.0,))
            .map(|_| ())
            .map_err(|e| {
                // Don't strand the put task on the loop.
                let _ = fut_ref.call_method0("cancel");
                e.print(py);
                "request queue put failed".to_string()
            })
    })
}

/// Enqueue an `http.disconnect` message (client went away mid-request).
pub fn feed_disconnect(loop_obj: &Py<PyAny>, queue: &Py<PyAny>) {
    Python::attach(|py| {
        let _ = (|| -> PyResult<()> {
            let q = queue.bind(py);
            let put = q.getattr("put_nowait")?;
            let msg = PyDict::new(py);
            msg.set_item("type", "http.disconnect")?;
            loop_obj
                .bind(py)
                .call_method1("call_soon_threadsafe", (put, msg))?;
            Ok(())
        })();
    });
}

/// Wait for the application future and log any exception (traceback) without
/// crashing the server. Runs on a blocking-pool thread.
pub fn await_app_completion(app_future: Py<PyAny>, started: Arc<AtomicBool>, timeout_secs: f64) {
    Python::attach(|py| {
        let res = app_future.bind(py).call_method1("result", (timeout_secs,));
        match res {
            Ok(_) => {}
            Err(e) => {
                e.print(py);
                if started.load(Ordering::SeqCst) {
                    eprintln!(
                        "ERROR rustwasgi: ASGI application raised after response start; connection aborted"
                    );
                } else {
                    eprintln!("ERROR rustwasgi: ASGI application raised; returned 500");
                }
            }
        }
    });
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
