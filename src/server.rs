//! hyper HTTP server owning the network sockets.
//!
//! hyper owns every accept loop and all HTTP/1 parsing (keep-alive included).
//! Each request is translated to ASGI in `asgi.rs`, executed on the asyncio
//! loop thread, and streamed back through hyper's `Channel` body.
//!
//! Per-request flow (all network I/O stays on Tokio; Python runs only on the
//! loop thread or short blocking-pool hops with the GIL released in waits):
//!
//! ```text
//! hyper Incoming body --chunks--> feeder task --blocking queue.put--> asyncio.Queue
//!                                                                          | await
//!                                                              Python await receive()
//! Python await send(msg) --SendSink._push--> tokio mpsc (bound 16)
//!                                                  | rx.recv().await
//! hyper Channel body <--pump task--------------+--> TCP socket
//! ```
//!
//! Gunicorn cooperation: heartbeat (`notify`), liveness (`is_alive`),
//! request counting for `max_requests` recycling, and access-log callbacks
//! are driven from Rust tasks that briefly acquire the GIL.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::{HeaderName, HeaderValue, StatusCode};
use http_body_util::channel::{Channel, Sender as BodySender};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use pyo3::prelude::*;
use tokio::task::JoinSet;

use crate::asgi::{self, Bridge, RESPONSE_CHANNEL_BOUND, RequestData, ResponseEvent, SendSink};
use crate::socket::BoundListener;
use crate::ws;

type RespBody = BoxBody<Bytes, hyper::Error>;

const FIRST_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
const APP_COMPLETION_TIMEOUT: f64 = 300.0;

/// Runtime knobs for one worker process.
#[derive(Clone)]
pub struct ServeConfig {
    pub app_spec: String,
    pub root_path: String,
    pub max_requests: usize,
    pub graceful_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub access_log: bool,
    /// Idle keep-alive header wait (0 = hyper default of 30s). Maps
    /// Gunicorn's `keepalive` setting.
    pub keep_alive_secs: u64,
}

/// Callbacks into the Python (Gunicorn) worker. All are thread-safe to invoke
/// via `Python::attach` from any thread; each call holds the GIL briefly.
pub struct WorkerHooks {
    /// `worker.notify()` heartbeat.
    pub notify: Py<PyAny>,
    /// `() -> bool` liveness (mirrors `worker.alive`).
    pub is_alive: Py<PyAny>,
    /// `(method, path, query, status, size, duration_ms, client) -> None`,
    /// or `None` to log a fixed line to stderr.
    pub access: Option<Py<PyAny>>,
}

impl Clone for WorkerHooks {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            notify: self.notify.clone_ref(py),
            is_alive: self.is_alive.clone_ref(py),
            access: crate::runtime::clone_opt_py(py, &self.access),
        })
    }
}

impl WorkerHooks {
    fn clone_for_task(&self) -> Self {
        Python::attach(|py| Self {
            notify: self.notify.clone_ref(py),
            is_alive: self.is_alive.clone_ref(py),
            access: self.access.as_ref().map(|a| a.clone_ref(py)),
        })
    }
}

struct AppState {
    app: Py<PyAny>,
    loop_obj: Py<PyAny>,
    bridge: Bridge,
    lifespan_state: Option<Py<PyAny>>,
    config: ServeConfig,
    hooks: WorkerHooks,
}

/// How shutdown was triggered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShutdownKind {
    Graceful,
    Quick,
}

struct Shutdown {
    flag: AtomicBool,
    quick: AtomicBool,
    notify: tokio::sync::Notify,
}

impl Shutdown {
    fn new() -> Self {
        Self {
            flag: AtomicBool::new(false),
            quick: AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn trigger(&self, kind: ShutdownKind) {
        if kind == ShutdownKind::Quick {
            self.quick.store(true, Ordering::SeqCst);
        }
        if !self.flag.swap(true, Ordering::SeqCst) {
            self.notify.notify_waiters();
        }
    }

    fn is_set(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    fn kind(&self) -> ShutdownKind {
        if self.quick.load(Ordering::SeqCst) {
            ShutdownKind::Quick
        } else {
            ShutdownKind::Graceful
        }
    }

    async fn wait(&self) {
        self.notify.notified().await;
    }
}

/// Serve all listeners until shutdown. Returns when drained.
pub async fn serve(
    listeners: Vec<BoundListener>,
    app: Py<PyAny>,
    loop_obj: Py<PyAny>,
    bridge: Bridge,
    lifespan_state: Option<Py<PyAny>>,
    config: ServeConfig,
    hooks: WorkerHooks,
) -> Result<(), String> {
    for l in &listeners {
        eprintln!("INFO rustwasgi: listening on {}", l.description());
    }
    eprintln!(
        "INFO rustwasgi: worker serving {} (pid={})",
        config.app_spec,
        std::process::id()
    );

    let state = Arc::new(AppState {
        app,
        loop_obj,
        bridge,
        lifespan_state,
        config,
        hooks,
    });
    let shutdown = Arc::new(Shutdown::new());
    let in_flight = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));

    // Signal watcher: SIGTERM -> graceful, SIGQUIT/SIGINT -> quick.
    // (Cooperates with Gunicorn: it sends these; Python-level handlers fire
    // once control returns to the interpreter after run_worker() returns.)
    // The watcher exits when `serve_done` fires so runtime shutdown never
    // hangs on it.
    let (serve_done_tx, serve_done_rx) = tokio::sync::watch::channel(false);
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            watch_signals(shutdown, serve_done_rx).await;
        });
    }
    // Heartbeat + liveness + orphan detection.
    {
        let state = state.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            heartbeat_loop(state, shutdown).await;
        });
    }

    // One accept task per listener (TCP + Unix alike).
    let mut acceptors: JoinSet<()> = JoinSet::new();
    for listener in listeners {
        let state = state.clone();
        let shutdown = shutdown.clone();
        let in_flight = in_flight.clone();
        let completed = completed.clone();
        acceptors.spawn(async move {
            match listener {
                BoundListener::Tcp {
                    listener,
                    local,
                    description,
                } => {
                    let server_host = local.ip().to_string();
                    let server_port = local.port();
                    loop {
                        if shutdown.is_set() {
                            break;
                        }
                        let accepted = tokio::select! {
                            _ = shutdown.wait() => break,
                            res = listener.accept() => res,
                        };
                        let (stream, client) = match accepted {
                            Ok(v) => v,
                            Err(e) => {
                                eprintln!("WARN rustwasgi: accept error on {description}: {e}");
                                continue;
                            }
                        };
                        // Disable Nagle: ASGI responses are small writes that
                        // would otherwise wait ~40ms for delayed ACKs.
                        // (Gunicorn pre-sets this on inherited sockets; the
                        // standalone path must do it here.)
                        if let Err(e) = stream.set_nodelay(true) {
                            eprintln!("WARN rustwasgi: set_nodelay failed: {e}");
                        }
                        let io = TokioIo::new(stream);
                        spawn_connection(
                            io,
                            client.to_string(),
                            client.port(),
                            server_host.clone(),
                            server_port,
                            &state,
                            &shutdown,
                            &in_flight,
                            &completed,
                        );
                    }
                }
                BoundListener::Unix {
                    listener,
                    path,
                    description,
                } => {
                    let server_host = path.clone();
                    loop {
                        if shutdown.is_set() {
                            break;
                        }
                        let accepted = tokio::select! {
                            _ = shutdown.wait() => break,
                            res = listener.accept() => res,
                        };
                        let (stream, peer) = match accepted {
                            Ok(v) => v,
                            Err(e) => {
                                eprintln!("WARN rustwasgi: accept error on {description}: {e}");
                                continue;
                            }
                        };
                        let (client_host, client_port) = match peer.as_pathname() {
                            Some(p) => (p.display().to_string(), 0),
                            None => (String::from(""), 0),
                        };
                        let io = TokioIo::new(stream);
                        spawn_connection(
                            io,
                            client_host,
                            client_port,
                            server_host.clone(),
                            0,
                            &state,
                            &shutdown,
                            &in_flight,
                            &completed,
                        );
                    }
                }
            }
        });
    }

    // Wait for the shutdown trigger, then stop accepting.
    shutdown.wait().await;
    let kind = shutdown.kind();
    match kind {
        ShutdownKind::Graceful => eprintln!("INFO rustwasgi: shutting down gracefully"),
        ShutdownKind::Quick => eprintln!("INFO rustwasgi: quick shutdown"),
    }
    // Let acceptor tasks observe the flag and exit.
    while acceptors.join_next().await.is_some() {}

    // Drain in-flight requests (graceful only, bounded by graceful_timeout).
    if kind == ShutdownKind::Graceful {
        let waited = state.config.graceful_timeout;
        let start = Instant::now();
        while in_flight.load(Ordering::SeqCst) > 0 && start.elapsed() < waited {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let remaining = in_flight.load(Ordering::SeqCst);
        if remaining > 0 {
            eprintln!(
                "WARN rustwasgi: shutdown with {remaining} in-flight request(s) after {waited:?}"
            );
        } else {
            eprintln!("INFO rustwasgi: shutdown complete");
        }
    }
    // Release the signal watcher so runtime shutdown cannot hang on it.
    let _ = serve_done_tx.send(true);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn spawn_connection<I>(
    io: I,
    client_host: String,
    client_port: u16,
    server_host: String,
    server_port: u16,
    state: &Arc<AppState>,
    shutdown: &Arc<Shutdown>,
    in_flight: &Arc<AtomicUsize>,
    completed: &Arc<AtomicUsize>,
) where
    I: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
{
    let state = state.clone();
    let shutdown = shutdown.clone();
    let in_flight = in_flight.clone();
    let completed = completed.clone();
    let keep_alive_secs = state.config.keep_alive_secs;
    tokio::spawn(async move {
        let service = service_fn(move |req: Request<Incoming>| {
            let state = state.clone();
            let shutdown = shutdown.clone();
            let in_flight = in_flight.clone();
            let completed = completed.clone();
            let (ch, sh) = (client_host.clone(), server_host.clone());
            async move {
                if shutdown.is_set() {
                    // Drain: refuse new work on old connections with 503.
                    return Ok::<_, std::convert::Infallible>(status_response(
                        503,
                        "Server shutting down",
                    ));
                }
                // The guard lives until the response body (or WS driver)
                // finishes: handle_request moves it into the background pump.
                let guard = crate::runtime::FlightGuard::new(&in_flight);
                let resp =
                    handle_request(req, &state, &ch, client_port, &sh, server_port, guard).await;
                // max_requests recycling: once the quota is hit, stop
                // accepting so the worker can exit and be replaced.
                let done = completed.fetch_add(1, Ordering::SeqCst) + 1;
                if state.config.max_requests > 0 && done >= state.config.max_requests {
                    eprintln!("INFO rustwasgi: max_requests reached ({done}); recycling worker");
                    shutdown.trigger(ShutdownKind::Graceful);
                }
                Ok::<_, std::convert::Infallible>(resp)
            }
        });
        if let Err(e) = connection_builder(keep_alive_secs)
            .serve_connection(io, service)
            .with_upgrades()
            .await
        {
            eprintln!("WARN rustwasgi: connection error: {e}");
        }
    });
}

/// Build per-connection hyper options from the worker config.
fn connection_builder(keep_alive_secs: u64) -> http1::Builder {
    let mut builder = http1::Builder::new();
    if keep_alive_secs > 0 {
        builder.timer(hyper_util::rt::TokioTimer::new());
        builder.header_read_timeout(Some(Duration::from_secs(keep_alive_secs)));
    }
    builder
}

async fn watch_signals(shutdown: Arc<Shutdown>, mut done: tokio::sync::watch::Receiver<bool>) {
    #[cfg(unix)]
    {
        let mut term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
        let mut quit = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::quit()).ok();
        let mut int =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).ok();
        // First signal triggers shutdown; afterwards only listen for an
        // escalating quick signal, and exit as soon as serve() is done.
        let mut triggered = false;
        loop {
            tokio::select! {
                _ = async { match &mut term { Some(s) => { s.recv().await; } None => std::future::pending().await } }, if !triggered => {
                    shutdown.trigger(ShutdownKind::Graceful);
                    triggered = true;
                }
                _ = async { match &mut quit { Some(s) => { s.recv().await; } None => std::future::pending().await } } => {
                    shutdown.trigger(ShutdownKind::Quick);
                    if triggered { break; }
                    triggered = true;
                }
                _ = async { match &mut int { Some(s) => { s.recv().await; } None => std::future::pending().await } } => {
                    shutdown.trigger(ShutdownKind::Quick);
                    if triggered { break; }
                    triggered = true;
                }
                _ = done.changed() => break,
            }
            if done.borrow().to_owned() {
                break;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        shutdown.trigger(ShutdownKind::Quick);
        let _ = done;
    }
}

async fn heartbeat_loop(state: Arc<AppState>, shutdown: Arc<Shutdown>) {
    let interval = state.config.heartbeat_interval;
    #[cfg(unix)]
    let start_ppid = std::os::unix::process::parent_id();
    #[cfg(not(unix))]
    let start_ppid = 0u32;
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        if shutdown.is_set() {
            break;
        }
        // Orphan detection: Gunicorn master died -> exit promptly.
        #[cfg(unix)]
        if std::os::unix::process::parent_id() != start_ppid {
            eprintln!("WARN rustwasgi: parent changed; shutting down");
            shutdown.trigger(ShutdownKind::Quick);
            break;
        }
        let hooks = state.hooks.clone_for_task();
        let alive: bool = tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                if let Err(e) = hooks.notify.bind(py).call0() {
                    e.print(py);
                    eprintln!("WARN rustwasgi: heartbeat notify() failed");
                }
                hooks
                    .is_alive
                    .bind(py)
                    .call0()
                    .and_then(|v| v.extract::<bool>())
                    .unwrap_or(true)
            })
        })
        .await
        .unwrap_or(true);
        if !alive {
            eprintln!("INFO rustwasgi: worker marked not alive; shutting down");
            shutdown.trigger(ShutdownKind::Graceful);
            break;
        }
    }
}

/// Per-request dispatch: WebSocket upgrade vs. plain HTTP.
///
/// The `guard` is moved into whichever background task owns the request's
/// full lifetime (response pump or WS driver); early returns drop it.
#[allow(clippy::too_many_arguments)]
async fn handle_request(
    req: Request<Incoming>,
    state: &AppState,
    client_host: &str,
    client_port: u16,
    server_host: &str,
    server_port: u16,
    guard: crate::runtime::FlightGuard,
) -> Response<RespBody> {
    if ws::is_websocket_upgrade(&req) {
        return handle_websocket_upgrade(
            req,
            state,
            client_host,
            client_port,
            server_host,
            server_port,
            guard,
        )
        .await;
    }
    handle_http(
        req,
        state,
        client_host,
        client_port,
        server_host,
        server_port,
        guard,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn handle_websocket_upgrade(
    req: Request<Incoming>,
    state: &AppState,
    client_host: &str,
    client_port: u16,
    server_host: &str,
    server_port: u16,
    guard: crate::runtime::FlightGuard,
) -> Response<RespBody> {
    // Extract handshake data before hyper takes the request.
    let key = req
        .headers()
        .get(http::header::SEC_WEBSOCKET_KEY)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    if key.is_empty() {
        return status_response(400, "Missing Sec-WebSocket-Key");
    }
    let version_str = match req.version() {
        http::Version::HTTP_10 => "1.0",
        http::Version::HTTP_11 => "1.1",
        http::Version::HTTP_2 => "2",
        _ => "1.1",
    }
    .to_string();
    let uri = req.uri().clone();
    let path_raw = uri.path().to_string();
    let mut headers = Vec::new();
    for (name, value) in req.headers().iter() {
        headers.push((name.as_str().as_bytes().to_vec(), value.as_bytes().to_vec()));
    }
    let handshake = ws::WsHandshake {
        path: path_raw.clone(),
        raw_path: path_raw.as_bytes().to_vec(),
        query_string: uri.query().unwrap_or("").as_bytes().to_vec(),
        headers,
        scheme: "ws".to_string(),
        http_version: version_str,
    };
    let on_upgrade = hyper::upgrade::on(req);

    // 101 must come from the handler (hyper upgrade API); the ASGI accept/deny
    // decision is enforced by the driver task afterwards (see ws.rs).
    let response = Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(http::header::UPGRADE, "websocket")
        .header(http::header::CONNECTION, "Upgrade")
        .header(http::header::SEC_WEBSOCKET_ACCEPT, ws::accept_key(&key))
        .body(empty_body())
        .expect("101 builds");

    let ctx = ws::WsContext {
        bridge: state.bridge.clone(),
        loop_obj: Python::attach(|py| state.loop_obj.clone_ref(py)),
        app: Python::attach(|py| state.app.clone_ref(py)),
        handshake,
        lifespan_state: Python::attach(|py| {
            crate::runtime::clone_opt_py(py, &state.lifespan_state)
        }),
        client_host: client_host.to_string(),
        client_port,
        server_host: server_host.to_string(),
        server_port,
    };
    tokio::spawn(async move {
        match on_upgrade.await {
            Ok(upgraded) => {
                ws::run_websocket_connection(upgraded, ctx).await;
            }
            Err(e) => {
                eprintln!("WARN rustwasgi: upgrade failed: {e}");
            }
        }
        // Full WS lifetime ends here (101 return does not end it).
        drop(guard);
    });
    response
}

#[allow(clippy::too_many_arguments)]
async fn handle_http(
    req: Request<Incoming>,
    state: &AppState,
    client_host: &str,
    client_port: u16,
    server_host: &str,
    server_port: u16,
    guard: crate::runtime::FlightGuard,
) -> Response<RespBody> {
    let started_at = Instant::now();
    let (parts, body) = req.into_parts();
    let method = parts.method.as_str().to_string();
    let is_head = parts.method == http::Method::HEAD;
    let version_str = match parts.version {
        http::Version::HTTP_10 => "1.0",
        http::Version::HTTP_11 => "1.1",
        http::Version::HTTP_2 => "2",
        http::Version::HTTP_3 => "3",
        _ => "1.1",
    }
    .to_string();
    let uri = parts.uri.clone();
    let path_raw = uri.path().to_string();
    let mut headers = Vec::new();
    for (name, value) in parts.headers.iter() {
        headers.push((name.as_str().as_bytes().to_vec(), value.as_bytes().to_vec()));
    }
    let data = RequestData {
        method: method.clone(),
        http_version: version_str,
        scheme: uri.scheme_str().unwrap_or("http").to_string(),
        path: asgi::percent_decode(&path_raw),
        raw_path: path_raw.as_bytes().to_vec(),
        query_string: uri.query().unwrap_or("").as_bytes().to_vec(),
        root_path: state.config.root_path.clone(),
        headers,
        client_host: client_host.to_string(),
        client_port,
        server_host: server_host.to_string(),
        server_port,
    };
    let path_for_log = data.path.clone();
    let query_for_log = String::from_utf8_lossy(&data.query_string).into_owned();

    // Response channel + sink; the app task is established on a
    // blocking-pool thread (GIL only for setup + released in waits).
    let disconnected = Arc::new(AtomicBool::new(false));
    let (tx, mut rx) = tokio::sync::mpsc::channel::<ResponseEvent>(RESPONSE_CHANNEL_BOUND);
    let established = {
        let bridge = state.bridge.clone();
        let loop_c = Python::attach(|py| state.loop_obj.clone_ref(py));
        let app_c = Python::attach(|py| state.app.clone_ref(py));
        let st_c = Python::attach(|py| crate::runtime::clone_opt_py(py, &state.lifespan_state));
        let sink = Python::attach(|py| {
            SendSink::new(tx, disconnected.clone())
                .into_pyobject(py)
                .map(|b| b.unbind())
        });
        let sink = match sink {
            Ok(s) => s,
            Err(e) => {
                Python::attach(|py| e.print(py));
                return status_response(500, "Internal Server Error");
            }
        };
        tokio::task::spawn_blocking(move || {
            asgi::establish_http_call(&bridge, &loop_c, &app_c, st_c.as_ref(), &data, sink)
        })
        .await
        .unwrap_or_else(|e| Err(format!("establish task failed: {e}")))
    };
    let (queue, app_future) = match established {
        Ok(call) => (call.queue, call.app_future),
        Err(e) => {
            eprintln!("ERROR rustwasgi: {e}");
            log_access(
                state,
                &method,
                &path_for_log,
                &query_for_log,
                500,
                0,
                started_at.elapsed(),
                client_host,
            )
            .await;
            return status_response(500, "Internal Server Error");
        }
    };

    // App completion monitor: logs tracebacks, marks done for the feeder.
    let app_done = Arc::new(AtomicBool::new(false));
    let started_flag = Arc::new(AtomicBool::new(false));
    {
        let fut_c = Python::attach(|py| app_future.clone_ref(py));
        let done = app_done.clone();
        let started = started_flag.clone();
        tokio::task::spawn_blocking(move || {
            asgi::await_app_completion(fut_c, started, APP_COMPLETION_TIMEOUT);
            done.store(true, Ordering::SeqCst);
        });
    }

    // Request body feeder: streams hyper chunks into the asyncio queue with
    // bounded puts (backpressure). hyper already decoded chunked framing.
    {
        let loop_c = Python::attach(|py| state.loop_obj.clone_ref(py));
        let queue_c = Python::attach(|py| queue.clone_ref(py));
        let done = app_done.clone();
        tokio::spawn(async move {
            feed_body(body, &loop_c, &queue_c, &done).await;
        });
    }

    // Wait for response start (or app failure before start -> 500).
    let first = tokio::time::timeout(FIRST_RESPONSE_TIMEOUT, rx.recv()).await;
    let (status, resp_headers) = match first {
        Ok(Some(ResponseEvent::Start { status, headers })) => (status, headers),
        Ok(Some(ResponseEvent::Body { .. })) => {
            eprintln!("ERROR rustwasgi: app sent body before response.start");
            app_done.store(true, Ordering::SeqCst);
            asgi::feed_disconnect(
                &Python::attach(|py| state.loop_obj.clone_ref(py)),
                &Python::attach(|py| queue.clone_ref(py)),
            );
            return status_response(500, "Internal Server Error");
        }
        Ok(None) => {
            // App raised before responding; traceback logged by monitor.
            log_access(
                state,
                &method,
                &path_for_log,
                &query_for_log,
                500,
                0,
                started_at.elapsed(),
                client_host,
            )
            .await;
            return status_response(500, "Internal Server Error");
        }
        Err(_) => {
            eprintln!("ERROR rustwasgi: app did not respond in time; returning 500");
            log_access(
                state,
                &method,
                &path_for_log,
                &query_for_log,
                500,
                0,
                started_at.elapsed(),
                client_host,
            )
            .await;
            return status_response(500, "Internal Server Error");
        }
    };
    started_flag.store(true, Ordering::SeqCst);

    let (body_sender, channel_body): (
        BodySender<Bytes, hyper::Error>,
        Channel<Bytes, hyper::Error>,
    ) = Channel::new(RESPONSE_CHANNEL_BOUND);
    let mut builder = Response::builder()
        .status(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR));
    for (k, v) in &resp_headers {
        match (HeaderName::from_bytes(k), HeaderValue::from_bytes(v)) {
            (Ok(name), Ok(value)) => {
                builder = builder.header(name, value);
            }
            _ => {
                eprintln!(
                    "WARN rustwasgi: skipping invalid response header {:?}",
                    String::from_utf8_lossy(k)
                );
            }
        }
    }
    let response = builder
        .body(channel_body.boxed())
        .unwrap_or_else(|_| status_response(500, "Internal Server Error"));

    // Response pump: forwards chunks; HEAD suppresses the wire body.
    // It owns `guard`, so graceful drain waits for the full body.
    {
        let loop_c = Python::attach(|py| state.loop_obj.clone_ref(py));
        let queue_c = Python::attach(|py| queue.clone_ref(py));
        let disc = disconnected.clone();
        let client_label = client_host.to_string();
        let state_c = state_for_task(state);
        tokio::spawn(async move {
            let mut sender = body_sender;
            let mut bytes_sent: u64 = 0;
            while let Some(event) = rx.recv().await {
                match event {
                    ResponseEvent::Start { .. } => {
                        eprintln!("WARN rustwasgi: duplicate response.start ignored");
                    }
                    ResponseEvent::Body { data, more_body } => {
                        if !is_head && !data.is_empty() {
                            bytes_sent += data.len() as u64;
                            if sender.send_data(Bytes::from(data)).await.is_err() {
                                // Client gone: fail fast future sends (OSError
                                // per ASGI 2.4+) and wake a pending receive().
                                disc.store(true, Ordering::SeqCst);
                                asgi::feed_disconnect(&loop_c, &queue_c);
                                break;
                            }
                        } else if is_head {
                            bytes_sent += data.len() as u64;
                        }
                        if !more_body {
                            break;
                        }
                    }
                }
            }
            drop(sender); // EOS (or abort if the client already went away).
            log_access_task(
                &state_c,
                &method,
                &path_for_log,
                &query_for_log,
                status,
                bytes_sent,
                started_at.elapsed(),
                &client_label,
            )
            .await;
            // Full body delivered: release the drain guard.
            drop(guard);
        });
    }
    response
}

// AppState is not Clone (Py fields need the GIL); re-clone cheaply per task.
fn state_for_task(state: &AppState) -> TaskState {
    Python::attach(|py| TaskState {
        loop_obj: state.loop_obj.clone_ref(py),
        hooks: state.hooks.clone_for_task(),
        access_log: state.config.access_log,
    })
}

struct TaskState {
    loop_obj: Py<PyAny>,
    hooks: WorkerHooks,
    access_log: bool,
}

/// Stream the hyper request body into the app queue, chunk by chunk.
/// Stops early if the app finished (e.g. it replied without reading).
///
/// Ordering is critical: every message (data and the terminal EOF) goes
/// through the same blocking `queue.put`, so the app can never observe EOF
/// before pending data.
async fn feed_body(
    body: Incoming,
    loop_obj: &Py<PyAny>,
    queue: &Py<PyAny>,
    app_done: &Arc<AtomicBool>,
) {
    let mut body = body;
    let mut ok = true;
    loop {
        if app_done.load(Ordering::SeqCst) {
            ok = false;
            break;
        }
        let frame = match body.frame().await {
            Some(Ok(f)) => f,
            Some(Err(_)) | None => break,
        };
        if let Some(data) = frame.data_ref() {
            if data.is_empty() {
                continue;
            }
            let chunk = data.to_vec();
            if put_body_chunk(loop_obj, queue, &chunk, true).await.is_err() {
                ok = false;
                break;
            }
        }
        // Trailers/metadata frames are ignored (no trailer support).
    }
    // Terminal EOF through the same ordered path (unless the app is gone).
    if ok && !app_done.load(Ordering::SeqCst) {
        let _ = put_body_chunk(loop_obj, queue, b"", false).await;
    }
}

/// One ordered blocking `queue.put` hop for a body message.
async fn put_body_chunk(
    loop_obj: &Py<PyAny>,
    queue: &Py<PyAny>,
    chunk: &[u8],
    more_body: bool,
) -> Result<(), ()> {
    let loop_c = Python::attach(|py| loop_obj.clone_ref(py));
    let queue_c = Python::attach(|py| queue.clone_ref(py));
    // Bounded put: backpressure propagates to hyper/TCP.
    let owned = chunk.to_vec();
    tokio::task::spawn_blocking(move || {
        asgi::feed_request_message_sync(&loop_c, &queue_c, &owned, more_body)
    })
    .await
    .map_err(|_| ())?
    .map_err(|_| ())
}

/// Emit one access-log entry via the Python hook (Gunicorn atoms) or stderr.
#[allow(clippy::too_many_arguments)]
async fn log_access(
    state: &AppState,
    method: &str,
    path: &str,
    query: &str,
    status: u16,
    size: u64,
    duration: Duration,
    client: &str,
) {
    let hook = Python::attach(|py| crate::runtime::clone_opt_py(py, &state.hooks.access));
    let (method, path, query, client) = (
        method.to_string(),
        path.to_string(),
        query.to_string(),
        client.to_string(),
    );
    let duration_ms = duration.as_secs_f64() * 1000.0;
    let enabled = state.config.access_log;
    if let Some(access) = hook {
        let status_u = status as u64;
        tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                if let Err(e) = access.bind(py).call1((
                    method,
                    path,
                    query,
                    status_u,
                    size,
                    duration_ms,
                    client,
                )) {
                    e.print(py);
                }
            });
        })
        .await
        .ok();
    } else if enabled {
        eprintln!(
            "INFO rustwasgi: {client} \"{method} {path}\" {status} {size} {duration_ms:.1}ms"
        );
    }
}

#[allow(clippy::too_many_arguments)]
async fn log_access_task(
    state: &TaskState,
    method: &str,
    path: &str,
    query: &str,
    status: u16,
    size: u64,
    duration: Duration,
    client: &str,
) {
    let hook = Python::attach(|py| crate::runtime::clone_opt_py(py, &state.hooks.access));
    let (method, path, query, client) = (
        method.to_string(),
        path.to_string(),
        query.to_string(),
        client.to_string(),
    );
    let duration_ms = duration.as_secs_f64() * 1000.0;
    let enabled = state.access_log;
    if let Some(access) = hook {
        let status_u = status as u64;
        tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                if let Err(e) = access.bind(py).call1((
                    method,
                    path,
                    query,
                    status_u,
                    size,
                    duration_ms,
                    client,
                )) {
                    e.print(py);
                }
            });
        })
        .await
        .ok();
    } else if enabled {
        eprintln!(
            "INFO rustwasgi: {client} \"{method} {path}\" {status} {size} {duration_ms:.1}ms"
        );
    }
    let _ = &state.loop_obj;
}

fn empty_body() -> RespBody {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}

fn full_body(data: Vec<u8>) -> RespBody {
    http_body_util::Full::new(Bytes::from(data))
        .map_err(|never| match never {})
        .boxed()
}

fn status_response(status: u16, text: &str) -> Response<RespBody> {
    let body = format!("{text}\n");
    Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .header(http::header::CONTENT_LENGTH, body.len().to_string())
        .body(full_body(body.into_bytes()))
        .expect("static response builds")
}
