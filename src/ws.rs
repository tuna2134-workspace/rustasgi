//! WebSocket support (ASGI WebSocket spec 2.5).
//!
//! The Rust server owns the full WebSocket transport:
//!
//! ```text
//! hyper (with_upgrades) -> 101 response -> hyper::upgrade::on()
//!   -> Upgraded I/O -> tokio-tungstenite WebSocketStream (server role)
//!     -> ASGI websocket.* events via asyncio.Queue (in) / WsSink (out)
//! ```
//!
//! Protocol duties handled in Rust, never exposed to ASGI:
//! - HTTP Upgrade handshake (Sec-WebSocket-Accept via tungstenite),
//! - frame fragmentation reassembly (tungstenite yields whole messages),
//! - PING -> automatic PONG reply; PONG frames are dropped.
//!
//! Deviations (documented): the 101 response is sent optimistically before
//! the application accepts, because hyper's upgrade API requires the 101 to
//! come from the request handler. If the app denies the connection (sends
//! `websocket.close` instead of `websocket.accept`), the server sends a
//! WebSocket close frame immediately after the 101. `subprotocol` and extra
//! accept headers are therefore ignored. `send()` after disconnect raises
//! `OSError`, per ASGI 2.4+.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use futures_util::{SinkExt, StreamExt};
use pyo3::exceptions::{PyOSError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes, PyDict};
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

use crate::asgi::Bridge;

/// Bound of the application -> server WebSocket channel (backpressure).
pub const WS_CHANNEL_BOUND: usize = 16;

/// Events flowing from the Python application to the WS driver.
#[derive(Debug)]
pub enum WsEvent {
    Accept,
    SendText(String),
    SendBytes(Vec<u8>),
    Close { code: u16, reason: String },
}

/// Rust end of WS `send()`, exposed to Python (see `SendSink` for the HTTP
/// equivalent). `_push` runs on the asyncio loop thread; the GIL is released
/// while waiting for channel capacity.
#[pyclass]
pub struct WsSink {
    tx: mpsc::Sender<WsEvent>,
    disconnected: Arc<AtomicBool>,
}

#[pymethods]
impl WsSink {
    fn _push(&self, py: Python<'_>, message: Bound<'_, PyAny>) -> PyResult<()> {
        if self.disconnected.load(Ordering::SeqCst) {
            return Err(PyOSError::new_err(
                "ASGI send(): websocket closed (send after disconnect)",
            ));
        }
        let event = parse_ws_message(&message)?;
        py.detach(|| self.tx.blocking_send(event))
            .map_err(|_| PyOSError::new_err("ASGI send(): websocket closed"))?;
        Ok(())
    }
}

impl WsSink {
    pub fn new(tx: mpsc::Sender<WsEvent>, disconnected: Arc<AtomicBool>) -> Self {
        Self { tx, disconnected }
    }
}

/// Validate one WS `send()` message. `text` and `bytes` are mutually
/// exclusive; close defaults to code 1000.
fn parse_ws_message(message: &Bound<'_, PyAny>) -> PyResult<WsEvent> {
    let msg_type: String = message
        .get_item("type")
        .map_err(|_| PyValueError::new_err("send(): message missing 'type'"))?
        .extract()
        .map_err(|_| PyValueError::new_err("send(): message 'type' must be str"))?;
    match msg_type.as_str() {
        "websocket.accept" => Ok(WsEvent::Accept),
        "websocket.send" => {
            let text = message.get_item("text").ok();
            let bytes = message.get_item("bytes").ok();
            match (text, bytes) {
                (Some(t), None) => {
                    let s: String = t
                        .extract()
                        .map_err(|_| PyValueError::new_err("send(): 'text' must be str"))?;
                    Ok(WsEvent::SendText(s))
                }
                (None, Some(b)) => {
                    let data = ws_bytes(&b)
                        .map_err(|_| PyValueError::new_err("send(): 'bytes' must be bytes"))?;
                    Ok(WsEvent::SendBytes(data))
                }
                (Some(_), Some(_)) => Err(PyValueError::new_err(
                    "send(): 'text' and 'bytes' are mutually exclusive",
                )),
                (None, None) => Err(PyValueError::new_err(
                    "send(): websocket.send needs 'text' or 'bytes'",
                )),
            }
        }
        "websocket.close" => {
            let code: u16 = message
                .get_item("code")
                .ok()
                .and_then(|v| v.extract().ok())
                .unwrap_or(1000);
            let reason: String = message
                .get_item("reason")
                .ok()
                .and_then(|v| v.extract().ok())
                .unwrap_or_default();
            Ok(WsEvent::Close { code, reason })
        }
        other => Err(PyValueError::new_err(format!(
            "send(): unsupported message type {other:?}"
        ))),
    }
}

fn ws_bytes(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(b) = obj.cast::<PyBytes>() {
        return Ok(b.as_bytes().to_vec());
    }
    if let Ok(b) = obj.cast::<PyByteArray>() {
        return Ok(unsafe { b.as_bytes().to_vec() });
    }
    Err(PyValueError::new_err("expected bytes"))
}

/// True when the request is a WebSocket upgrade handshake.
pub fn is_websocket_upgrade(req: &hyper::Request<hyper::body::Incoming>) -> bool {
    let headers = req.headers();
    let upgrade_ok = headers
        .get(http::header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("websocket"))
        .unwrap_or(false);
    let connection_ok = headers
        .get(http::header::CONNECTION)
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            v.split(',')
                .any(|t| t.trim().eq_ignore_ascii_case("upgrade"))
        })
        .unwrap_or(false);
    upgrade_ok && connection_ok && *req.method() == http::Method::GET
}

/// Compute `Sec-WebSocket-Accept` for the 101 response.
pub fn accept_key(key: &str) -> String {
    tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes())
}

/// Metadata needed for the WS scope + handshake, extracted before the
/// request is consumed by `hyper::upgrade::on()`.
#[derive(Debug, Clone)]
pub struct WsHandshake {
    pub path: String,
    pub raw_path: Vec<u8>,
    pub query_string: Vec<u8>,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub scheme: String,
    pub http_version: String,
}

/// Byte stream of a completed upgrade, adapted from hyper's I/O traits to
/// Tokio's so tungstenite can drive it. Pure SYSCALL-level shim: no
/// buffering, no protocol logic.
struct TokioUpgraded(hyper::upgrade::Upgraded);

impl tokio::io::AsyncRead for TokioUpgraded {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let unfilled = buf.initialize_unfilled();
        let mut hbuf = hyper::rt::ReadBuf::new(unfilled);
        let cursor = hbuf.unfilled();
        match hyper::rt::Read::poll_read(std::pin::Pin::new(&mut self.0), cx, cursor) {
            std::task::Poll::Ready(Ok(())) => {
                let n = hbuf.filled().len();
                buf.advance(n);
                std::task::Poll::Ready(Ok(()))
            }
            std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Err(e)),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl tokio::io::AsyncWrite for TokioUpgraded {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        hyper::rt::Write::poll_write(std::pin::Pin::new(&mut self.0), cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        hyper::rt::Write::poll_flush(std::pin::Pin::new(&mut self.0), cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        hyper::rt::Write::poll_shutdown(std::pin::Pin::new(&mut self.0), cx)
    }
}

/// Run one WebSocket connection to completion: ASGI handshake, message pump,
/// close semantics. Takes the raw post-101 byte stream.
pub async fn run_websocket_connection(upgraded: hyper::upgrade::Upgraded, ctx: WsContext) {
    let stream = tokio_tungstenite::WebSocketStream::from_raw_socket(
        TokioUpgraded(upgraded),
        tokio_tungstenite::tungstenite::protocol::Role::Server,
        None,
    )
    .await;
    drive_websocket(stream, ctx).await;
}

#[derive(Debug)]
pub struct WsContext {
    pub bridge: Bridge,
    pub loop_obj: Py<PyAny>,
    pub app: Py<PyAny>,
    pub handshake: WsHandshake,
    pub lifespan_state: Option<Py<PyAny>>,
    pub client_host: String,
    pub client_port: u16,
    pub server_host: String,
    pub server_port: u16,
}

impl Clone for WsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            bridge: self.bridge.clone(),
            loop_obj: self.loop_obj.clone_ref(py),
            app: self.app.clone_ref(py),
            handshake: self.handshake.clone(),
            lifespan_state: self.lifespan_state.as_ref().map(|s| s.clone_ref(py)),
            client_host: self.client_host.clone(),
            client_port: self.client_port,
            server_host: self.server_host.clone(),
            server_port: self.server_port,
        })
    }
}

type ServerWsStream = WebSocketStream<TokioUpgraded>;

async fn drive_websocket(mut ws: ServerWsStream, ctx: WsContext) {
    let disconnected = Arc::new(AtomicBool::new(false));
    let (tx, mut rx) = mpsc::channel::<WsEvent>(WS_CHANNEL_BOUND);

    // Setup on a blocking thread: sink, queue-backed channel, scope, submit.
    let setup: Result<(Py<PyAny>, Py<PyAny>), String> = {
        let bridge = ctx.bridge.clone();
        let loop_obj = Python::attach(|py| ctx.loop_obj.clone_ref(py));
        let app = Python::attach(|py| ctx.app.clone_ref(py));
        let state = ctx
            .lifespan_state
            .as_ref()
            .map(|s| Python::attach(|py| s.clone_ref(py)));
        let hs = ctx.handshake.clone();
        let (ch, cp, sh, sp) = (
            ctx.client_host.clone(),
            ctx.client_port,
            ctx.server_host.clone(),
            ctx.server_port,
        );
        let disc = disconnected.clone();
        tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                setup_ws_call(
                    py,
                    &bridge,
                    &loop_obj,
                    &app,
                    state.as_ref(),
                    &hs,
                    &ch,
                    cp,
                    &sh,
                    sp,
                    tx,
                    disc,
                )
                .map_err(|e| {
                    e.print(py);
                    "websocket setup failed".to_string()
                })
            })
        })
        .await
        .unwrap_or(Err("setup task failed".to_string()))
    };
    let (queue, app_future) = match setup {
        Ok(v) => v,
        Err(e) => {
            eprintln!("ERROR rustwasgi: {e}");
            let _ = ws
                .send(Message::Close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                    code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Error,
                    reason: "internal error".into(),
                })))
                .await;
            return;
        }
    };

    // Monitor the app task for exceptions (tracebacks to the log).
    let monitor_loop = Python::attach(|py| ctx.loop_obj.clone_ref(py));
    let monitor_fut = Python::attach(|py| app_future.clone_ref(py));
    tokio::task::spawn_blocking(move || {
        Python::attach(|py| {
            if let Err(e) = monitor_fut.bind(py).call_method1("result", (300.0,)) {
                // CancelledError after clean close is normal; don't log noise.
                let name = e
                    .get_type(py)
                    .name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default();
                if !name.contains("Cancelled") {
                    e.print(py);
                    eprintln!("ERROR rustwasgi: websocket application raised");
                }
            }
        });
        let _ = monitor_loop;
    });

    // Feed websocket.connect.
    feed_ws(&ctx.loop_obj, &queue, WsInbound::Connect);

    // 1. First app event must be Accept (or Close = deny).
    let accepted = match tokio::time::timeout(std::time::Duration::from_secs(30), rx.recv()).await {
        Ok(Some(WsEvent::Accept)) => true,
        Ok(Some(WsEvent::Close { code, reason })) => {
            send_close(&mut ws, code, &reason).await;
            finish_ws(&ctx.loop_obj, &queue, &app_future).await;
            return;
        }
        Ok(Some(_)) => {
            eprintln!("ERROR rustwasgi: app must accept websocket before sending");
            send_close(&mut ws, 1011, "expected accept").await;
            finish_ws(&ctx.loop_obj, &queue, &app_future).await;
            return;
        }
        Ok(None) => {
            send_close(&mut ws, 1011, "app failed").await;
            return;
        }
        Err(_) => {
            eprintln!("ERROR rustwasgi: websocket accept timed out");
            send_close(&mut ws, 1011, "accept timeout").await;
            finish_ws(&ctx.loop_obj, &queue, &app_future).await;
            return;
        }
    };
    debug_assert!(accepted);

    // 2. Bidirectional pump.
    let mut app_closed = false;
    loop {
        tokio::select! {
            event = rx.recv(), if !app_closed => {
                match event {
                    Some(WsEvent::SendText(s)) => {
                        if ws.send(Message::Text(s.into())).await.is_err() {
                            break;
                        }
                    }
                    Some(WsEvent::SendBytes(b)) => {
                        if ws.send(Message::Binary(b.into())).await.is_err() {
                            break;
                        }
                    }
                    Some(WsEvent::Close { code, reason }) => {
                        send_close(&mut ws, code, &reason).await;
                        app_closed = true;
                        // Keep reading client frames until close echo / EOF.
                    }
                    Some(WsEvent::Accept) => {} // duplicate accept: ignore
                    None => {
                        // App returned: clean close if still open.
                        if !app_closed {
                            send_close(&mut ws, 1000, "").await;
                        }
                        break;
                    }
                }
            }
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Text(s))) => {
                        feed_ws(&ctx.loop_obj, &queue, WsInbound::Text(s.to_string()));
                    }
                    Some(Ok(Message::Binary(b))) => {
                        feed_ws(&ctx.loop_obj, &queue, WsInbound::Bytes(b.to_vec()));
                    }
                    Some(Ok(Message::Ping(p))) => {
                        // Protocol layer: reply PONG, never expose to ASGI.
                        if ws.send(Message::Pong(p)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        let (code, reason) = frame
                            .map(|f| (u16::from(f.code), f.reason.to_string()))
                            .unwrap_or((1006, String::new()));
                        // Echo close if the app hasn't closed.
                        if !app_closed {
                            send_close(&mut ws, code, "").await;
                        }
                        feed_ws(&ctx.loop_obj, &queue, WsInbound::Disconnect { code, reason });
                        break;
                    }
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Err(_)) | None => {
                        feed_ws(&ctx.loop_obj, &queue, WsInbound::Disconnect { code: 1006, reason: String::new() });
                        break;
                    }
                }
            }
        }
    }

    disconnected.store(true, Ordering::SeqCst);
    // Drop the receiver so pending/future send() calls fail fast with OSError.
    drop(rx);
    finish_ws(&ctx.loop_obj, &queue, &app_future).await;
}

/// Wait for the app task to finish (bounded), so ``asyncio`` resources and
/// lifespan-adjacent cleanup complete before the connection task ends.
async fn finish_ws(loop_obj: &Py<PyAny>, _queue: &Py<PyAny>, app_future: &Py<PyAny>) {
    let loop_c = Python::attach(|py| loop_obj.clone_ref(py));
    let fut_c = Python::attach(|py| app_future.clone_ref(py));
    let _ = tokio::task::spawn_blocking(move || {
        Python::attach(|py| {
            let _ = fut_c.bind(py).call_method1("result", (15.0,));
        });
    })
    .await;
    let _ = loop_c;
}

async fn send_close(ws: &mut ServerWsStream, code: u16, reason: &str) {
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
    let frame = CloseFrame {
        code: CloseCode::from(code),
        reason: reason.into(),
    };
    let _ = ws.send(Message::Close(Some(frame))).await;
}

enum WsInbound {
    Connect,
    Text(String),
    Bytes(Vec<u8>),
    Disconnect { code: u16, reason: String },
}

fn feed_ws(loop_obj: &Py<PyAny>, queue: &Py<PyAny>, inbound: WsInbound) {
    Python::attach(|py| {
        let _ = (|| -> PyResult<()> {
            let q = queue.bind(py);
            // Bounded queue + blocking put would need a blocking thread;
            // WS messages are small — put_nowait, drop on full (documented).
            let put = q.getattr("put_nowait")?;
            let msg = PyDict::new(py);
            match inbound {
                WsInbound::Connect => {
                    msg.set_item("type", "websocket.connect")?;
                }
                WsInbound::Text(s) => {
                    msg.set_item("type", "websocket.receive")?;
                    msg.set_item("text", s)?;
                }
                WsInbound::Bytes(b) => {
                    msg.set_item("type", "websocket.receive")?;
                    msg.set_item("body", PyBytes::new(py, &b))?;
                    msg.set_item("bytes", PyBytes::new(py, &b))?;
                }
                WsInbound::Disconnect { code, reason } => {
                    msg.set_item("type", "websocket.disconnect")?;
                    msg.set_item("code", code)?;
                    msg.set_item("reason", reason)?;
                }
            }
            // put_nowait may raise QueueFull under extreme load; then the
            // message is dropped (documented) rather than deadlocking.
            let _ = loop_obj
                .bind(py)
                .call_method1("call_soon_threadsafe", (put.clone(), msg));
            let _ = put;
            Ok(())
        })();
    });
}

#[allow(clippy::too_many_arguments)]
fn setup_ws_call(
    py: Python<'_>,
    bridge: &Bridge,
    loop_obj: &Py<PyAny>,
    app: &Py<PyAny>,
    lifespan_state: Option<&Py<PyAny>>,
    hs: &WsHandshake,
    client_host: &str,
    client_port: u16,
    server_host: &str,
    server_port: u16,
    tx: mpsc::Sender<WsEvent>,
    disconnected: Arc<AtomicBool>,
) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
    use pyo3::types::{IntoPyDict, PyList, PyTuple};

    let loop_bound = loop_obj.bind(py);
    let sink = pyo3::Py::new(py, WsSink::new(tx, disconnected))?;
    let init_coro = bridge
        .init_ws_fn(py)
        .call1((bridge.ws_send_cls(py), sink))?;
    let init_future = Bridge::submit(py, &loop_bound.clone(), init_coro)?;
    let channel: Bound<'_, PyAny> = init_future.bind(py).call_method1("result", (10.0,))?;
    let (queue, receive, send): (Bound<'_, PyAny>, Bound<'_, PyAny>, Bound<'_, PyAny>) =
        channel.extract()?;

    let scope = PyDict::new(py);
    scope.set_item("type", "websocket")?;
    scope.set_item(
        "asgi",
        [("version", "3.0"), ("spec_version", "2.5")].into_py_dict(py)?,
    )?;
    scope.set_item("http_version", &hs.http_version)?;
    scope.set_item("scheme", &hs.scheme)?;
    scope.set_item("path", crate::asgi::percent_decode(&hs.path))?;
    scope.set_item("raw_path", PyBytes::new(py, &hs.raw_path))?;
    scope.set_item("query_string", PyBytes::new(py, &hs.query_string))?;
    scope.set_item("root_path", "")?;
    let headers = PyList::empty(py);
    for (k, v) in &hs.headers {
        headers.append((PyBytes::new(py, k), PyBytes::new(py, v)))?;
    }
    scope.set_item("headers", headers)?;
    {
        let h: Bound<'_, PyAny> = client_host.to_owned().into_pyobject(py)?.into_any();
        let p: Bound<'_, PyAny> = client_port.into_pyobject(py)?.into_any();
        scope.set_item("client", PyTuple::new(py, [h, p])?)?;
    }
    {
        let h: Bound<'_, PyAny> = server_host.to_owned().into_pyobject(py)?.into_any();
        let p: Bound<'_, PyAny> = server_port.into_pyobject(py)?.into_any();
        scope.set_item("server", PyTuple::new(py, [h, p])?)?;
    }
    scope.set_item("extensions", PyDict::new(py))?;
    match lifespan_state {
        Some(state) => scope.set_item("state", state.bind(py))?,
        None => scope.set_item("state", PyDict::new(py))?,
    }
    // Subprotocol negotiation happens in the 101 (already sent); expose none.
    scope.set_item("subprotocols", PyList::empty(py))?;

    let app_coro = bridge
        .call_app_fn(py)
        .call1((app.bind(py), scope, receive, send))?;
    let app_future = Bridge::submit(py, loop_bound, app_coro)?;
    Ok((queue.unbind(), app_future))
}
