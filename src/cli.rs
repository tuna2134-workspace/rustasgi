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
    /// TLS certificate file (PEM)
    pub tls_cert: Option<String>,
    /// TLS private key file (PEM)
    pub tls_key: Option<String>,
    /// Redirect HTTP to HTTPS (301/308) except ACME challenges
    pub redirect_http_to_https: bool,
    /// ACME directory URL (e.g. Let's Encrypt)
    pub acme_directory: Option<String>,
    /// ACME contact email
    pub acme_email: Option<String>,
    /// ACME domains (SAN list)
    pub acme_domains: Vec<String>,
    /// ACME storage directory
    pub acme_dir: Option<String>,
    /// SNI entries as `domain:cert:key` strings (for multiple certs)
    pub tls_sni: Vec<String>,
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
        tls_cert: Option<String>,
        tls_key: Option<String>,
        redirect_http_to_https: bool,
        acme_directory: Option<String>,
        acme_email: Option<String>,
        acme_domains: Vec<String>,
        acme_dir: Option<String>,
        tls_sni: Vec<String>,
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
            tls_cert,
            tls_key,
            redirect_http_to_https,
            acme_directory,
            acme_email,
            acme_domains,
            acme_dir,
            tls_sni,
        }
    }

    #[allow(dead_code)]
    pub fn is_tls_enabled(&self) -> bool {
        (self.tls_cert.is_some() && self.tls_key.is_some()) || !self.tls_sni.is_empty()
    }

    pub fn is_acme_enabled(&self) -> bool {
        !self.acme_domains.is_empty() && self.acme_email.is_some()
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
