//! hyper HTTP server owning the network sockets — zero-thread request path.
//!
//! hyper owns every accept loop and all HTTP/1 parsing (keep-alive included).
//! Each request is translated to ASGI in `asgi.rs`, executed on the asyncio
//! loop, and streamed back through hyper's `Channel` body. At no point does a
//! Tokio task block an OS thread waiting for Python, and no Python execution
//! blocks waiting for Rust: the two sides meet only through
//! `pyo3-async-runtimes` waker channels.
//!
//! Per-request flow (bodyless `GET /`, the hot path):
//!
//! ```text
//! hyper request (Tokio task, GIL-free)
//!   | attach GIL (µs): build scope, init coroutine, into_future
//!   v
//! loop thread runs init (queue + pre-seeded terminal receive + send)
//!   | attach GIL (µs): build app coroutine, into_future
//!   v
//! loop thread runs FastAPI <--- no feeder task (no body) ---
//!   | app send()s stream via try_send into mpsc(16)
//!   v
//! pump task: rx.recv().await --> hyper Channel --> TCP
//! app completion: into_future awaited directly (traceback on Err)
//! ```
//!
//! With a body, one extra Tokio feeder task awaits `queue.put` through
//! `into_future` (async backpressure, no threads).
//!
//! Gunicorn cooperation: heartbeat (`notify`), liveness (`is_alive`),
//! request counting for `max_requests` recycling, and access-log callbacks
//! are driven from plain Tokio tasks that attach the GIL briefly.

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
use pyo3_async_runtimes::TaskLocals;
use tokio::task::JoinSet;

use crate::asgi::{self, Bridge, RequestData, ResponseEvent};
use crate::asgi::{REQUEST_PUT_TIMEOUT, RESPONSE_CHANNEL_BOUND};
use crate::socket::BoundListener;
use crate::ws;

type RespBody = BoxBody<Bytes, hyper::Error>;

const FIRST_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
const APP_COMPLETION_TIMEOUT: Duration = Duration::from_secs(300);

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

/// Callbacks into the Python (Gunicorn) worker. Invoked via brief
/// `Python::attach` from Tokio tasks — never waited on, never blocking.
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
    bridge: Bridge,
    locals: TaskLocals,
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

/// Serve all listeners until shutdown. Returns when drained; connection
/// tasks are aborted so a leaked runtime is never needed for teardown.
pub async fn serve(
    listeners: Vec<BoundListener>,
    app: Py<PyAny>,
    bridge: Bridge,
    locals: TaskLocals,
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
        bridge,
        locals,
        lifespan_state,
        config,
        hooks,
    });
    let shutdown = Arc::new(Shutdown::new());
    let in_flight = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    // All connection tasks: aborted after the drain so nothing outlives serve.
    let connections: Arc<tokio::sync::Mutex<JoinSet<()>>> =
        Arc::new(tokio::sync::Mutex::new(JoinSet::new()));

    // Signal watcher: SIGTERM -> graceful, SIGQUIT/SIGINT -> quick.
    // (Cooperates with Gunicorn: it sends these; Python-level handlers fire
    // once control returns to the interpreter after run_worker() returns.)
    let (serve_done_tx, serve_done_rx) = tokio::sync::watch::channel(false);
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            watch_signals(shutdown, serve_done_rx).await;
        });
    }
    // Heartbeat + liveness + orphan detection (plain async task, no threads).
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
        let connections = connections.clone();
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
                        // Disable Nagle: small ASGI responses would otherwise
                        // wait ~40ms for delayed ACKs.
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
                            &connections,
                        )
                        .await;
                    }
                }
                #[cfg(unix)]
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
                            &connections,
                        )
                        .await;
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
    if crate::metrics::enabled() {
        eprintln!("{}", crate::metrics::report());
    }
    // Abort leftover (idle keep-alive) connections, release the watcher.
    connections.lock().await.abort_all();
    while connections.lock().await.join_next().await.is_some() {}
    let _ = serve_done_tx.send(true);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn spawn_connection<I>(
    io: I,
    client_host: String,
    client_port: u16,
    server_host: String,
    server_port: u16,
    state: &Arc<AppState>,
    shutdown: &Arc<Shutdown>,
    in_flight: &Arc<AtomicUsize>,
    completed: &Arc<AtomicUsize>,
    connections: &Arc<tokio::sync::Mutex<JoinSet<()>>>,
) where
    I: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
{
    let state = state.clone();
    let shutdown = shutdown.clone();
    let in_flight = in_flight.clone();
    let completed = completed.clone();
    let keep_alive_secs = state.config.keep_alive_secs;
    let task = async move {
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
    };
    connections.lock().await.spawn(task);
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
            if *done.borrow() {
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
        // Brief GIL attach: two quick calls, never waited on.
        let hooks = state.hooks.clone_for_task();
        let alive: bool = Python::attach(|py| {
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
        });
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
    state: &Arc<AppState>,
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
    state: &Arc<AppState>,
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
        locals: state.locals.clone(),
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
    state: &Arc<AppState>,
    client_host: &str,
    client_port: u16,
    server_host: &str,
    server_port: u16,
    guard: crate::runtime::FlightGuard,
) -> Response<RespBody> {
    crate::metrics::inc(crate::metrics::C_REQUESTS);
    let t_total = crate::metrics::now();
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
    // Bodyless fast-path detection: chunked framing or a positive
    // Content-Length means a body may stream; anything else (GET/HEAD/...) is
    // declared bodyless and gets a zero-hop pre-seeded terminal message.
    // (Malformed lengths take the streaming path; hyper arbitrates.)
    let mut has_body = false;
    for (name, value) in parts.headers.iter() {
        headers.push((name.as_str().as_bytes().to_vec(), value.as_bytes().to_vec()));
        if name == http::header::TRANSFER_ENCODING {
            has_body = true;
        } else if name == http::header::CONTENT_LENGTH {
            let positive = value
                .to_str()
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .map(|n| n > 0)
                .unwrap_or(true);
            if positive {
                has_body = true;
            }
        }
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
        has_body,
        client_host: client_host.to_string(),
        client_port,
        server_host: server_host.to_string(),
        server_port,
    };
    let path_for_log = data.path.clone();
    let query_for_log = String::from_utf8_lossy(&data.query_string).into_owned();

    // Response channel; the app task is established with pure async
    // awaits (one GIL acquisition for all construction — see establish).
    let disconnected = Arc::new(AtomicBool::new(false));
    let (tx, mut rx) = tokio::sync::mpsc::channel::<ResponseEvent>(RESPONSE_CHANNEL_BOUND);
    let established = asgi::establish_http_call(
        &state.bridge,
        &state.locals,
        &state.app,
        state.lifespan_state.as_ref(),
        &data,
        tx,
        disconnected.clone(),
    )
    .await;
    crate::metrics::phase(crate::metrics::P_ESTABLISH, t_total);
    let (queue, mut app_fut) = match established {
        Ok(call) => (call.queue, call.app_fut),
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

    // Phase A (inline, no spawn): drive body feeding while awaiting response
    // Start. One task observes body frames, app completion, and rx events
    // together — no feeder task, no reaper task, no watch channel.
    let app_logged = Arc::new(AtomicBool::new(false));
    let t_app = crate::metrics::now();
    let mut app_done = false;
    let mut feeding = has_body;
    let mut body_opt = has_body.then_some(body);
    let start_deadline = tokio::time::sleep(FIRST_RESPONSE_TIMEOUT);
    tokio::pin!(start_deadline);
    let (status, resp_headers) = loop {
        tokio::select! {
            biased;
            event = rx.recv() => {
                match event {
                    Some(ResponseEvent::Start { status, headers }) => break (status, headers),
                    Some(ResponseEvent::Body { .. }) => {
                        eprintln!("ERROR rustwasgi: app sent body before response.start");
                        asgi::feed_disconnect(&state.bridge, &state.locals, &queue).await;
                        spawn_drain_reaper(app_fut, t_app, app_logged.clone());
                        return status_response(500, "Internal Server Error");
                    }
                    None => {
                        // App raised before responding; traceback logged by
                        // whoever observes completion first.
                        log_access(
                            state, &method, &path_for_log, &query_for_log, 500, 0,
                            started_at.elapsed(), client_host,
                        )
                        .await;
                        spawn_drain_reaper(app_fut, t_app, app_logged.clone());
                        return status_response(500, "Internal Server Error");
                    }
                }
            }
            res = &mut app_fut, if !app_done => {
                app_done = true;
                crate::metrics::inc(crate::metrics::C_APPDONE_A);
                if !app_logged.swap(true, Ordering::SeqCst) {
                    crate::metrics::phase(crate::metrics::P_APP_WAIT, t_app);
                    match res {
                        Ok(_) => {}
                        Err(e) => {
                            Python::attach(|py| e.print(py));
                            eprintln!("ERROR rustwasgi: ASGI application raised");
                        }
                    }
                }
            }
            fed = async {
                match body_opt.as_mut() {
                    Some(b) if feeding => {
                        feed_step(b, &state.bridge, &state.locals, &queue).await
                    }
                    _ => std::future::pending().await,
                }
            } => {
                feeding = fed;
                if !fed {
                    // EOF/error already delivered; drop the stream so the
                    // responder cannot deliver a duplicate terminal.
                    body_opt = None;
                }
            }
            _ = &mut start_deadline => {
                eprintln!("ERROR rustwasgi: app did not respond in time; returning 500");
                log_access(
                    state, &method, &path_for_log, &query_for_log, 500, 0,
                    started_at.elapsed(), client_host,
                )
                .await;
                spawn_drain_reaper(app_fut, t_app, app_logged.clone());
                return status_response(500, "Internal Server Error");
            }
        }
    };

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

    // Responder: one task owns response forwarding, remaining body feeding,
    // and app completion (reaper merged in). It holds an Arc<AppState> (no
    // GIL refcounting) and owns `guard`, so graceful drain waits for the
    // full body.
    {
        let state_c = Arc::clone(state);
        let disc = disconnected.clone();
        let client_label = client_host.to_string();
        let app_logged_c = app_logged.clone();
        crate::metrics::inc(crate::metrics::C_SPAWNS);
        let t_pump = crate::metrics::now();
        tokio::spawn(async move {
            let app_deadline = tokio::time::sleep(APP_COMPLETION_TIMEOUT);
            tokio::pin!(app_deadline);
            let mut sender = body_sender;
            let mut bytes_sent: u64 = 0;
            let mut feeding = has_body && body_opt.is_some();
            let mut body_opt = body_opt;
            let mut app_done_local = app_done;
            let mut app_fut_opt = Some(app_fut);
            loop {
                tokio::select! {
                    biased;
                    event = rx.recv() => {
                        match event {
                            Some(ResponseEvent::Start { .. }) => {
                                eprintln!("WARN rustwasgi: duplicate response.start ignored");
                            }
                            Some(ResponseEvent::Body { data, more_body }) => {
                                if !is_head && !data.is_empty() {
                                    bytes_sent += data.len() as u64;
                            if sender.send_data(Bytes::from(data)).await.is_err() {
                                // Client gone: fail fast future sends (OSError
                                // per ASGI 2.4+) and wake a pending receive().
                                disc.store(true, Ordering::SeqCst);
                                asgi::feed_disconnect(
                                    &state_c.bridge,
                                    &state_c.locals,
                                    &queue,
                                )
                                .await;
                                break;
                            }
                                } else if is_head {
                                    bytes_sent += data.len() as u64;
                                }
                                if !more_body {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                    res = async {
                        match app_fut_opt.as_mut() {
                            Some(f) if !app_done_local => f.await,
                            _ => std::future::pending().await,
                        }
                    } => {
                        app_done_local = true;
                        crate::metrics::inc(crate::metrics::C_APPDONE_PUMP);
                        if !app_logged_c.swap(true, Ordering::SeqCst) {
                            crate::metrics::phase(crate::metrics::P_APP_WAIT, t_app);
                            match res {
                                Ok(_) => {}
                                Err(e) => {
                                    Python::attach(|py| e.print(py));
                                    eprintln!("ERROR rustwasgi: ASGI application raised");
                                }
                            }
                        }
                    }
                    fed = async {
                        match body_opt.as_mut() {
                            Some(b) if feeding => {
                                feed_step(b, &state_c.bridge, &state_c.locals, &queue).await
                            }
                            _ => std::future::pending().await,
                        }
                    } => {
                        feeding = fed;
                        if !fed {
                            body_opt = None;
                        }
                    }
                    _ = &mut app_deadline => {
                        crate::metrics::inc(crate::metrics::C_APPDONE_DEADLINE);
                        eprintln!("ERROR rustwasgi: ASGI application timed out; dropping");
                        break;
                    }
                }
            }
            drop(sender); // EOS (or abort if the client already went away).
            // The loop may exit (final body / rx close) without having
            // observed app completion (e.g. app returned right after its
            // final send). Observe it now: rx-close implies the sink dropped,
            // which implies the app frame is gone, so this resolves
            // immediately in the common case. Fall back to a drain reaper
            // rather than silently swallowing a late traceback.
            if !app_done_local && let Some(mut f) = app_fut_opt.take() {
                // Poll once with a noop waker: rx-close implies the sink
                // dropped, which implies the app frame is gone, so this
                // resolves immediately in the common case. Pending
                // (genuinely still running) falls back to a drain reaper
                // rather than swallowing a traceback.
                use std::task::{Context, Poll};
                let waker = std::task::Waker::noop();
                let mut cx = Context::from_waker(waker);
                match std::pin::Pin::new(&mut f).poll(&mut cx) {
                    Poll::Ready(res) => {
                        crate::metrics::inc(crate::metrics::C_APPDONE_NOWNOVER);
                        if !app_logged_c.swap(true, Ordering::SeqCst) {
                            crate::metrics::phase(crate::metrics::P_APP_WAIT, t_app);
                            if let Err(e) = res {
                                Python::attach(|py| e.print(py));
                                eprintln!("ERROR rustwasgi: ASGI application raised");
                            }
                        }
                    }
                    Poll::Pending => {
                        spawn_drain_reaper(f, t_app, app_logged_c);
                    }
                }
            }
            log_access_task(
                &state_c.hooks,
                state_c.config.access_log,
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
            crate::metrics::phase(crate::metrics::P_PUMP, t_pump);
            drop(guard);
        });
    }
    crate::metrics::phase(crate::metrics::P_TOTAL, t_total);
    response
}

/// Observe a detached app future to completion for logging (Phase-A failure
/// paths only, where no responder exists to reap). Preserves the old
/// reaper's observability without a per-request task on the hot path.
fn spawn_drain_reaper(
    app_fut: crate::asgi::AppFuture,
    t_app: std::time::Instant,
    app_logged: Arc<AtomicBool>,
) {
    crate::metrics::inc(crate::metrics::C_SPAWNS);
    tokio::spawn(async move {
        let res = tokio::time::timeout(APP_COMPLETION_TIMEOUT, app_fut).await;
        crate::metrics::inc(crate::metrics::C_APPDONE_REAPER);
        if !app_logged.swap(true, Ordering::SeqCst) {
            crate::metrics::phase(crate::metrics::P_APP_WAIT, t_app);
            match res {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    Python::attach(|py| e.print(py));
                    eprintln!("ERROR rustwasgi: ASGI application raised");
                }
                Err(_) => {
                    eprintln!("ERROR rustwasgi: ASGI application timed out; dropping");
                }
            }
        }
    });
}

/// Feed a single body step inline (shared by Phase A and the responder).
/// Returns false when feeding is finished (EOF sent, error, or app gone).
async fn feed_step(
    body: &mut Incoming,
    bridge: &Bridge,
    locals: &TaskLocals,
    queue: &Py<PyAny>,
) -> bool {
    match body.frame().await {
        Some(Ok(f)) => match f.into_data() {
            Ok(data) if !data.is_empty() => {
                let chunk = data.to_vec();
                crate::metrics::inc(crate::metrics::C_FEED_CHUNKS);
                if asgi::feed_message(
                    locals,
                    queue,
                    |py| {
                        use pyo3::types::{PyBool, PyBytes, PyDict};
                        let msg = PyDict::new(py);
                        msg.set_item("type", "http.request")?;
                        msg.set_item("body", PyBytes::new(py, &chunk))?;
                        msg.set_item("more_body", PyBool::new(py, true))?;
                        Ok(msg.into_any().unbind())
                    },
                    REQUEST_PUT_TIMEOUT,
                )
                .await
                .is_err()
                {
                    asgi::feed_disconnect(bridge, locals, queue).await;
                    return false;
                }
                true
            }
            _ => true, // metadata frame: no trailer support, skip.
        },
        _ => {
            // EOF or error: terminal message through the same ordered path.
            let _ = asgi::feed_message(
                locals,
                queue,
                |py| {
                    use pyo3::types::{PyBool, PyBytes, PyDict};
                    let msg = PyDict::new(py);
                    msg.set_item("type", "http.request")?;
                    msg.set_item("body", PyBytes::new(py, b""))?;
                    msg.set_item("more_body", PyBool::new(py, false))?;
                    Ok(msg.into_any().unbind())
                },
                REQUEST_PUT_TIMEOUT,
            )
            .await;
            false
        }
    }
}

/// Emit one access-log entry via the Python hook (Gunicorn atoms) or stderr.
/// Brief GIL attach for a fast call; nothing waited on.
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
    // Fast path: no hook and no stderr log means zero GIL interaction.
    if state.hooks.access.is_none() && !state.config.access_log {
        return;
    }
    crate::metrics::inc(crate::metrics::C_ATTACH);
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
        Python::attach(|py| {
            if let Err(e) =
                access
                    .bind(py)
                    .call1((method, path, query, status_u, size, duration_ms, client))
            {
                e.print(py);
            }
        });
    } else if enabled {
        eprintln!(
            "INFO rustwasgi: {client} \"{method} {path}\" {status} {size} {duration_ms:.1}ms"
        );
    }
}

#[allow(clippy::too_many_arguments)]
async fn log_access_task(
    hooks: &WorkerHooks,
    access_log: bool,
    method: &str,
    path: &str,
    query: &str,
    status: u16,
    size: u64,
    duration: Duration,
    client: &str,
) {
    // Fast path: no hook and no stderr log means zero GIL interaction.
    if hooks.access.is_none() && !access_log {
        return;
    }
    let hook = Python::attach(|py| crate::runtime::clone_opt_py(py, &hooks.access));
    let (method, path, query, client) = (
        method.to_string(),
        path.to_string(),
        query.to_string(),
        client.to_string(),
    );
    let duration_ms = duration.as_secs_f64() * 1000.0;
    let enabled = access_log;
    if let Some(access) = hook {
        let status_u = status as u64;
        Python::attach(|py| {
            if let Err(e) =
                access
                    .bind(py)
                    .call1((method, path, query, status_u, size, duration_ms, client))
            {
                e.print(py);
            }
        });
    } else if enabled {
        eprintln!(
            "INFO rustwasgi: {client} \"{method} {path}\" {status} {size} {duration_ms:.1}ms"
        );
    }
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
