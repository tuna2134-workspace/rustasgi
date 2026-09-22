//! Listening sockets owned by someone else (Gunicorn master).
//!
//! # Ownership rules
//!
//! ```text
//! Gunicorn master: bind() + listen()          <- owns the original FD
//!        | fork / pass to worker
//! Gunicorn worker (Python): os.dup(fd)        <- duplicate, owned by Python
//!        | passed as (family, fd) to run_worker()
//! Rust: OwnedFd::from_raw_fd(dup_fd)          <- owns the duplicate
//!        | TcpListener::from_std / UnixListener::from_std
//! Rust: closes the duplicate on shutdown      <- drop
//! ```
//!
//! The original FD always stays with Gunicorn (which closes it on worker
//! exit). Rust never touches the original: Python duplicates first with
//! `os.dup()`, so there is no double-close and no leak. `dup()` (rather than
//! borrowing) is required because the Tokio listener takes full ownership of
//! its FD and because the Python `socket` object may be closed by Gunicorn
//! while Rust is still serving.
//!
//! Supported families: IPv4 TCP, IPv6 TCP, Unix domain sockets (unix only).

use std::net::SocketAddr;

#[cfg(unix)]
use std::os::fd::{FromRawFd, OwnedFd};
#[cfg(windows)]
use std::os::windows::io::{FromRawSocket, OwnedSocket};
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;

/// One inherited listening socket, as passed from Python.
#[derive(Debug)]
pub struct InheritedSocket {
    /// `"tcp4"`, `"tcp6"` or `"unix"`.
    pub family: String,
    #[cfg(unix)]
    /// A **duplicated** FD owned by this process (see module docs).
    pub fd: OwnedFd,
    #[cfg(windows)]
    /// A **duplicated** socket owned by this process (Windows).
    pub fd: OwnedSocket,
    #[cfg(not(any(unix, windows)))]
    /// Fallback: raw fd stored, not owned.
    pub fd: i32,
    /// Human description for logs (e.g. `"http://127.0.0.1:8000"`).
    pub description: String,
}

impl InheritedSocket {
    /// Build from a raw FD number. Takes ownership (closes on drop).
    ///
    /// # Safety
    /// The caller must guarantee `fd` is a valid, uniquely-owned (dup'd)
    /// socket FD of the stated family. Misuse (wrong family, double ownership)
    /// is memory-safe but will produce errors or close the wrong FD.
    #[cfg(unix)]
    pub unsafe fn from_raw(family: String, fd: i32, description: String) -> Self {
        // SAFETY: upheld by the contract above; the FD comes from os.dup().
        let owned = unsafe { OwnedFd::from_raw_fd(fd) };
        Self {
            family,
            fd: owned,
            description,
        }
    }

    #[cfg(windows)]
    pub unsafe fn from_raw(family: String, fd: i32, description: String) -> Self {
        use std::os::windows::io::RawSocket;
        // SAFETY: upheld by the contract above; the FD comes from os.dup().
        // On Windows, sockets are RawSocket (usize), but Python's os.dup for sockets
        // duplicates the underlying SOCKET handle which fits in RawSocket.
        let owned = unsafe { OwnedSocket::from_raw_socket(fd as RawSocket) };
        Self {
            family,
            fd: owned,
            description,
        }
    }

    #[cfg(not(any(unix, windows)))]
    pub unsafe fn from_raw(family: String, fd: i32, description: String) -> Self {
        Self {
            family,
            fd,
            description,
        }
    }
}

/// A bound Tokio listener plus a stable id for logs/scopes.
pub enum BoundListener {
    Tcp {
        listener: TcpListener,
        local: SocketAddr,
        description: String,
    },
    #[cfg(unix)]
    Unix {
        listener: UnixListener,
        path: String,
        description: String,
    },
}

impl BoundListener {
    pub fn description(&self) -> &str {
        match self {
            BoundListener::Tcp { description, .. } => description,
            #[cfg(unix)]
            BoundListener::Unix { description, .. } => description,
        }
    }
}

/// Convert inherited sockets into Tokio listeners (non-blocking mode set).
pub fn bind_inherited(sockets: Vec<InheritedSocket>) -> Result<Vec<BoundListener>, String> {
    let mut out = Vec::with_capacity(sockets.len());
    for sock in sockets {
        match sock.family.as_str() {
            "tcp4" | "tcp6" | "tcp" => {
                #[cfg(unix)]
                {
                    use std::os::fd::IntoRawFd;
                    let raw = sock.fd.into_raw_fd();
                    // SAFETY: we just took ownership back from OwnedFd; the FD is
                    // a valid listening TCP socket (dup'd by the parent).
                    let std_listener: std::net::TcpListener =
                        unsafe { std::os::fd::FromRawFd::from_raw_fd(raw) };
                    std_listener
                        .set_nonblocking(true)
                        .map_err(|e| format!("failed to set nonblocking: {e}"))?;
                    let local = std_listener
                        .local_addr()
                        .map_err(|e| format!("getsockname failed: {e}"))?;
                    let listener = TcpListener::from_std(std_listener)
                        .map_err(|e| format!("tokio TcpListener failed: {e}"))?;
                    out.push(BoundListener::Tcp {
                        listener,
                        local,
                        description: sock.description,
                    });
                }
                #[cfg(windows)]
                {
                    use std::os::windows::io::IntoRawSocket;
                    let raw = sock.fd.into_raw_socket();
                    // SAFETY: we just took ownership back from OwnedSocket; the FD is
                    // a valid listening TCP socket (dup'd by the parent).
                    let std_listener: std::net::TcpListener =
                        unsafe { std::os::windows::io::FromRawSocket::from_raw_socket(raw) };
                    std_listener
                        .set_nonblocking(true)
                        .map_err(|e| format!("failed to set nonblocking: {e}"))?;
                    let local = std_listener
                        .local_addr()
                        .map_err(|e| format!("getsockname failed: {e}"))?;
                    let listener = TcpListener::from_std(std_listener)
                        .map_err(|e| format!("tokio TcpListener failed: {e}"))?;
                    out.push(BoundListener::Tcp {
                        listener,
                        local,
                        description: sock.description,
                    });
                }
                #[cfg(not(any(unix, windows)))]
                {
                    return Err(format!(
                        "unsupported platform for tcp sockets: {}",
                        sock.description
                    ));
                }
            }
            "unix" => {
                #[cfg(unix)]
                {
                    use std::os::fd::IntoRawFd;
                    let raw = sock.fd.into_raw_fd();
                    // SAFETY: same contract as above, for a Unix socket.
                    let std_listener: std::os::unix::net::UnixListener =
                        unsafe { std::os::fd::FromRawFd::from_raw_fd(raw) };
                    std_listener
                        .set_nonblocking(true)
                        .map_err(|e| format!("failed to set nonblocking: {e}"))?;
                    let path = std_listener
                        .local_addr()
                        .ok()
                        .and_then(|a| a.as_pathname().map(|p| p.display().to_string()))
                        .unwrap_or_else(|| "<unnamed unix socket>".to_string());
                    let listener = UnixListener::from_std(std_listener)
                        .map_err(|e| format!("tokio UnixListener failed: {e}"))?;
                    out.push(BoundListener::Unix {
                        listener,
                        path,
                        description: sock.description,
                    });
                }
                #[cfg(not(unix))]
                {
                    return Err(format!(
                        "unsupported socket family unix on this platform: {}",
                        sock.description
                    ));
                }
            }
            other => {
                return Err(format!(
                    "unsupported socket family {other:?} (want tcp4/tcp6/unix)"
                ));
            }
        }
    }
    if out.is_empty() {
        return Err("no listening sockets provided".to_string());
    }
    Ok(out)
}

/// Bind fresh TCP sockets (standalone dev-server path; Rust owns bind()).
pub async fn bind_standalone(host: &str, port: u16) -> Result<Vec<BoundListener>, String> {
    let addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .map_err(|e| format!("invalid bind address {host}:{port}: {e}"))?;
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| format!("failed to bind {addr}: {e}"))?;
    let local = listener
        .local_addr()
        .map_err(|e| format!("getsockname failed: {e}"))?;
    Ok(vec![BoundListener::Tcp {
        listener,
        local,
        description: format!("http://{local}"),
    }])
}
