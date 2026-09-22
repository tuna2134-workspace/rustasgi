//! WebSocket support (ASGI WebSocket spec 2.5), zero-thread async bridge.
//!
//! The Rust server owns the full WebSocket transport:
//!
//! ```text
//! hyper (with_upgrades) -> 101 response -> hyper::upgrade::on()
//!   -> Upgraded I/O -> Tokio adapter -> tungstenite WebSocketStream
//!     -> ASGI websocket.* events via into_future puts (in) / WsSink (out)
//! ```
//!
//! Protocol duties handled in Rust, never exposed to ASGI:
//! - HTTP Upgrade handshake (Sec-WebSocket-Accept via tungstenite),
//! - frame fragmentation reassembly (tungstenite yields whole messages),
//! - PING -> automatic PONG reply; PONG frames are dropped.
//!
//! App execution, message feeding, and completion waiting are all
//! `into_future` awaits on Tokio tasks — no monitor threads, no blocking
//! waits. `send()` uses `try_send` with a `future_into_py` wait only under
//! real backpressure.
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
use pyo3::types::{PyByteArray, PyBytes, PyDict, PyList, PyTuple};
use pyo3_async_runtimes::{TaskLocals, tokio as par_tokio};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

use crate::asgi::Bridge;

/// Bound of the application -> server WebSocket channel (backpressure).
pub const WS_CHANNEL_BOUND: usize = 16;

/// Bound of the server -> application WebSocket queue.
pub const WS_QUEUE_MAXSIZE: usize = 64;

/// Events flowing from the Python application to the WS driver.
#[derive(Debug)]
pub enum WsEvent {
    Accept,
    SendText(String),
    SendBytes(Vec<u8>),
    Close { code: u16, reason: String },
}

/// Rust end of WS `send()`. Never blocks: `try_send`, with a
/// `future_into_py` awaitable only when the channel is genuinely full.
#[pyclass]
pub struct WsSink {
    tx: mpsc::Sender<WsEvent>,
    disconnected: Arc<AtomicBool>,
    locals: TaskLocals,
}

#[pymethods]
impl WsSink {
    fn _push<'py>(
        &self,
        py: Python<'py>,
        message: Bound<'py, PyAny>,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        if self.disconnected.load(Ordering::SeqCst) {
            return Err(PyOSError::new_err(
                "ASGI send(): websocket closed (send after disconnect)",
            ));
        }
        let event = parse_ws_message(&message)?;
        match self.tx.try_send(event) {
            Ok(()) => Ok(None),
            Err(TrySendError::Full(event)) => {
                let tx = self.tx.clone();
                let fut = async move {
                    tx.send(event)
                        .await
                        .map_err(|_| PyOSError::new_err("ASGI send(): websocket closed"))?;
                    Ok(())
                };
                Ok(Some(par_tokio::future_into_py_with_locals(
                    py,
                    self.locals.clone(),
                    fut,
                )?))
            }
            Err(TrySendError::Closed(_)) => {
                Err(PyOSError::new_err("ASGI send(): websocket closed"))
            }
        }
    }
}

impl WsSink {
    pub fn new(
        tx: mpsc::Sender<WsEvent>,
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

type ServerWsStream = WebSocketStream<TokioUpgraded>;

/// Run one WebSocket connection to completion: ASGI handshake, message pump,
/// close semantics. Takes the raw post-101 byte stream.
pub async fn run_websocket_connection(upgraded: hyper::upgrade::Upgraded, ctx: WsContext) {
    let stream = WebSocketStream::from_raw_socket(
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
    pub locals: TaskLocals,
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
            locals: self.locals.clone(),
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

async fn drive_websocket(mut ws: ServerWsStream, ctx: WsContext) {
    let disconnected = Arc::new(AtomicBool::new(false));
    let (tx, mut rx) = mpsc::channel::<WsEvent>(WS_CHANNEL_BOUND);

    // Setup without threads: sink, queue-backed channel, scope, submit.
    let setup: Result<(Py<PyAny>, crate::asgi::AppFuture), String> = {
        let bridge = ctx.bridge.clone();
        let locals = ctx.locals.clone();
        let app = Python::attach(|py| ctx.app.clone_ref(py));
        let state = Python::attach(|py| crate::runtime::clone_opt_py(py, &ctx.lifespan_state));
        let hs = ctx.handshake.clone();
        let (ch, cp, sh, sp) = (
            ctx.client_host.clone(),
            ctx.client_port,
            ctx.server_host.clone(),
            ctx.server_port,
        );
        let disc = disconnected.clone();
        setup_ws_call(
            &bridge,
            &locals,
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
        .await
    };
    let (queue, app_fut) = match setup {
        Ok(v) => v,
        Err(e) => {
            eprintln!("ERROR rustwasgi: {e}");
            send_close(&mut ws, 1011, "internal error").await;
            return;
        }
    };

    // Feed websocket.connect (async, bounded).
    if feed_ws(&ctx.locals, &queue, WsInbound::Connect)
        .await
        .is_err()
    {
        send_close(&mut ws, 1011, "setup failed").await;
        return;
    }

    // 1. First app event must be Accept (or Close = deny).
    match tokio::time::timeout(std::time::Duration::from_secs(30), rx.recv()).await {
        Ok(Some(WsEvent::Accept)) => {}
        Ok(Some(WsEvent::Close { code, reason })) => {
            send_close(&mut ws, code, &reason).await;
            finish_ws(app_fut).await;
            return;
        }
        Ok(Some(_)) => {
            eprintln!("ERROR rustwasgi: app must accept websocket before sending");
            send_close(&mut ws, 1011, "expected accept").await;
            finish_ws(app_fut).await;
            return;
        }
        Ok(None) => {
            send_close(&mut ws, 1011, "app failed").await;
            return;
        }
        Err(_) => {
            eprintln!("ERROR rustwasgi: websocket accept timed out");
            send_close(&mut ws, 1011, "accept timeout").await;
            finish_ws(app_fut).await;
            return;
        }
    };

    // App completion is awaited directly (no monitor thread); the driver
    // fuses it so a returning app ends the connection promptly.
    let mut app_fut = app_fut;
    let mut app_done: Option<PyResult<Py<PyAny>>> = None;

    // 2. Bidirectional pump.
    let mut app_closed = false;
    loop {
        tokio::select! {
            biased;
            res = &mut app_fut, if app_done.is_none() => {
                app_done = Some(res);
                match &app_done {
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        let msg = format!("{e}");
                        Python::attach(|py| e.print(py));
                        eprintln!("ERROR rustwasgi: websocket application raised: {msg}");
                    }
                    None => unreachable!(),
                }
                if !app_closed {
                    send_close(&mut ws, 1000, "").await;
                }
                break;
            }
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
                        // App returned without closing (handled above too).
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
                        if feed_ws(&ctx.locals, &queue, WsInbound::Text(s.to_string())).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Binary(b))) => {
                        if feed_ws(&ctx.locals, &queue, WsInbound::Bytes(b.to_vec())).await.is_err() {
                            break;
                        }
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
                        if !app_closed {
                            send_close(&mut ws, code, "").await;
                        }
                        let _ = feed_ws(&ctx.locals, &queue, WsInbound::Disconnect { code, reason }).await;
                        break;
                    }
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Err(_)) | None => {
                        let _ = feed_ws(&ctx.locals, &queue, WsInbound::Disconnect { code: 1006, reason: String::new() }).await;
                        break;
                    }
                }
            }
        }
    }

    disconnected.store(true, Ordering::SeqCst);
    drop(rx); // future send() calls fail fast with OSError.
    if app_done.is_none() {
        // Driver ended first (client gone): bound the app's remaining time.
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            finish_ws_take(&mut app_fut),
        )
        .await;
    }
}

async fn finish_ws(mut app_fut: crate::asgi::AppFuture) {
    finish_ws_take(&mut app_fut).await;
}

async fn finish_ws_take(app_fut: &mut crate::asgi::AppFuture) {
    let res = tokio::time::timeout(std::time::Duration::from_secs(15), app_fut.as_mut()).await;
    match res {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            Python::attach(|py| e.print(py));
            eprintln!("ERROR rustwasgi: websocket application raised");
        }
        Err(_) => {
            eprintln!("WARN rustwasgi: websocket app did not finish; dropping");
        }
    }
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

/// Feed one inbound WS message through the bounded queue (async, no threads).
async fn feed_ws(locals: &TaskLocals, queue: &Py<PyAny>, inbound: WsInbound) -> Result<(), String> {
    let put_fut = Python::attach(|py| -> PyResult<_> {
        let q = queue.bind(py);
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
                msg.set_item("bytes", PyBytes::new(py, &b))?;
            }
            WsInbound::Disconnect { code, reason } => {
                msg.set_item("type", "websocket.disconnect")?;
                msg.set_item("code", code)?;
                msg.set_item("reason", reason)?;
            }
        }
        let put_coro = q.call_method1("put", (msg,))?;
        crate::asgi::into_future(locals, put_coro)
    })
    .map_err(|e| {
        Python::attach(|py| e.print(py));
        "ws queue submit failed".to_string()
    })?;
    tokio::time::timeout(std::time::Duration::from_secs(30), put_fut)
        .await
        .map_err(|_| "ws queue put timed out".to_string())?
        .map(|_| ())
        .map_err(|e| {
            Python::attach(|py| e.print(py));
            "ws queue put failed".to_string()
        })
}

#[allow(clippy::too_many_arguments)]
async fn setup_ws_call(
    bridge: &Bridge,
    locals: &TaskLocals,
    app: &Py<PyAny>,
    lifespan_state: Option<&Py<PyAny>>,
    hs: &WsHandshake,
    client_host: &str,
    client_port: u16,
    server_host: &str,
    server_port: u16,
    tx: mpsc::Sender<WsEvent>,
    disconnected: Arc<AtomicBool>,
) -> Result<(Py<PyAny>, crate::asgi::AppFuture), String> {
    use pyo3::types::IntoPyDict;

    let sink: Py<WsSink> =
        Python::attach(|py| pyo3::Py::new(py, WsSink::new(tx, disconnected, locals.clone())))
            .map_err(|e| {
                Python::attach(|py| e.print(py));
                "ws sink failed".to_string()
            })?;

    let (queue, receive, send): (Py<PyAny>, Py<PyAny>, Py<PyAny>) = {
        let init_fut = Python::attach(|py| {
            let init_coro =
                bridge
                    .init_ws_fn(py)
                    .call1((bridge.ws_send_cls(py), sink, WS_QUEUE_MAXSIZE))?;
            crate::asgi::into_future(locals, init_coro)
        })
        .map_err(|e| {
            Python::attach(|py| e.print(py));
            "ws channel init failed".to_string()
        })?;
        let channel = init_fut.await.map_err(|e| {
            Python::attach(|py| e.print(py));
            "ws channel init failed".to_string()
        })?;
        Python::attach(|py| {
            channel
                .bind(py)
                .extract::<(Py<PyAny>, Py<PyAny>, Py<PyAny>)>()
        })
        .map_err(|e| {
            Python::attach(|py| e.print(py));
            "ws channel init failed".to_string()
        })?
    };

    let app_fut = Python::attach(|py| -> PyResult<crate::asgi::AppFuture> {
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
        scope.set_item("subprotocols", PyList::empty(py))?;

        let app_coro =
            bridge
                .call_app_fn(py)
                .call1((app.bind(py), scope, receive.bind(py), send.bind(py)))?;
        crate::asgi::into_future(locals, app_coro)
    })
    .map_err(|e| {
        Python::attach(|py| e.print(py));
        "ws submit failed".to_string()
    })?;
    Ok((queue, app_fut))
}
