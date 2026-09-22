//! CLI / server configuration shared by the Rust core.
//!
//! Actual argument parsing lives in `python/rustwasgi/__main__.py`
//! (standalone) and in Gunicorn itself (worker path). This struct is the
//! Rust-side contract for the standalone path.

/// Server configuration passed from Python.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ServerConfig {
    /// `"module:attr"` ASGI application specifier, e.g. `"app:app"`.
    pub app_spec: String,
    /// Bind host (standalone path only).
    pub host: String,
    /// Bind port (standalone path only).
    pub port: u16,
    /// Worker count (standalone runs 1; multi-worker is Gunicorn's job).
    pub workers: usize,
    /// Log level string (`debug`, `info`, `warning`, `error`, `quiet`).
    pub log_level: String,
    /// ASGI `root_path` mounted scope value.
    pub root_path: String,
    /// Lifespan mode: `auto` (default), `on`, `off`.
    pub lifespan: String,
    /// Emit per-request access lines to stderr (standalone only; under
    /// Gunicorn the `access` hook feeds Gunicorn's access log instead).
    pub access_log: bool,
}

impl ServerConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        app_spec: String,
        host: String,
        port: u16,
        workers: usize,
        log_level: String,
        root_path: String,
        lifespan: String,
        access_log: bool,
    ) -> Self {
        Self {
            app_spec,
            host,
            port,
            workers,
            log_level,
            root_path,
            lifespan,
            access_log,
        }
    }

    pub fn log_info(&self, msg: &str) {
        let level = self.log_level.to_ascii_lowercase();
        if level != "quiet" && level != "silent" {
            eprintln!("INFO rustwasgi: {msg}");
        }
    }

    pub fn log_warn(&self, msg: &str) {
        let level = self.log_level.to_ascii_lowercase();
        if level != "quiet" && level != "silent" {
            eprintln!("WARN rustwasgi: {msg}");
        }
    }
}
