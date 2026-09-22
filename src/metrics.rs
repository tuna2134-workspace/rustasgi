//! Gated request-path instrumentation (§6, §16).
//!
//! Always-compiled atomic counters and phase timers. Counting is unconditional
//! (a fetch_add is ~1ns); latency recording and the shutdown report are gated
//! on `RUSTWASGI_PROFILE=1` (checked once) so benchmarks stay clean by
//! default. With the env var set, `serve()` prints an aggregate table:
//!
//! ```text
//! requests: 100000
//! tokio_spawns/request: X
//! python_attaches/request: X
//! into_future/request: X
//! attach_mean_us: X
//! ```
//!
//! Phases (indices): 0 = establish, 1 = app_wait, 2 = pump, 3 = total.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

pub const N_PHASES: usize = 6;
pub const P_ESTABLISH: usize = 0;
pub const P_APP_WAIT: usize = 1;
pub const P_PUMP: usize = 2;
pub const P_TOTAL: usize = 3;
pub const P_ATTACH_WAIT: usize = 4;
pub const P_ATTACH_HOLD: usize = 5;

pub const PHASE_NAMES: [&str; N_PHASES] = [
    "establish",
    "app_wait",
    "pump",
    "total",
    "attach_wait",
    "attach_hold",
];

// Counters: requests, tokio spawns, into_future calls, send fast/full,
// GIL attaches (hot path), feeder chunks, TLS, ACME.
pub const N_COUNTERS: usize = 18;
pub const C_REQUESTS: usize = 0;
pub const C_SPAWNS: usize = 1;
pub const C_INTO_FUTURE: usize = 2;
pub const C_SEND_FAST: usize = 3;
pub const C_SEND_FULL: usize = 4;
pub const C_ATTACH: usize = 5;
pub const C_FEED_CHUNKS: usize = 6;
pub const C_APPDONE_A: usize = 7;
pub const C_APPDONE_PUMP: usize = 8;
pub const C_APPDONE_NOWNOVER: usize = 9;
pub const C_APPDONE_REAPER: usize = 10;
pub const C_APPDONE_DEADLINE: usize = 11;
pub const C_TLS_HANDSHAKES: usize = 12;
pub const C_TLS_HANDSHAKE_ERRORS: usize = 13;
pub const C_ACME_ORDERS: usize = 14;
pub const C_ACME_RENEWALS: usize = 15;
pub const C_ACME_RENEWAL_FAILURES: usize = 16;
pub const C_ACME_CHALLENGE_HITS: usize = 17;

pub const COUNTER_NAMES: [&str; N_COUNTERS] = [
    "requests",
    "tokio_spawns",
    "into_future",
    "send_try_ok",
    "send_full_wait",
    "gil_attach",
    "feed_chunks",
    "appdone_phase_a",
    "appdone_pump",
    "appdone_now_or_never",
    "appdone_reaper",
    "appdone_deadline",
    "tls_handshakes",
    "tls_handshake_errors",
    "acme_orders",
    "acme_renewals",
    "acme_renewal_failures",
    "acme_challenge_hits",
];

static ENABLED: OnceLock<bool> = OnceLock::new();
static PHASE_NS: [AtomicU64; N_PHASES] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];
static PHASE_N: [AtomicU64; N_PHASES] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];
static COUNTERS: [AtomicU64; N_COUNTERS] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];

#[inline]
pub fn inc_tls_handshake() {
    inc(C_TLS_HANDSHAKES);
}

#[inline]
pub fn inc_tls_handshake_error() {
    inc(C_TLS_HANDSHAKE_ERRORS);
}

#[inline]
pub fn inc_acme_challenge_hit() {
    inc(C_ACME_CHALLENGE_HITS);
}

/// True when `RUSTWASGI_PROFILE=1` (report enabled).
#[inline]
pub fn enabled() -> bool {
    *ENABLED.get_or_init(|| std::env::var("RUSTWASGI_PROFILE").as_deref() == Ok("1"))
}

#[inline]
pub fn inc(counter: usize) {
    // Gated so unprofiled runs pay nothing (a single predictable branch);
    // profiled runs get complete counts. This makes "metrics disabled" the
    // default benchmark configuration with no feature-flag rebuild needed.
    if enabled() {
        COUNTERS[counter].fetch_add(1, Ordering::Relaxed);
    }
}

/// Record elapsed nanoseconds for a phase (no-op unless profiling).
#[inline]
pub fn phase(phase: usize, start: Instant) {
    if enabled() {
        PHASE_NS[phase].fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        PHASE_N[phase].fetch_add(1, Ordering::Relaxed);
    }
}

/// Cheap wall-time stamp for phase measurement.
#[inline]
pub fn now() -> Instant {
    Instant::now()
}

/// Attach that records GIL wait vs hold separately (gated).
/// wait = time to acquire the GIL; hold = time executing the closure.
pub fn attach_measured<T>(f: impl FnOnce(pyo3::Python<'_>) -> T) -> T {
    use std::sync::atomic::Ordering;
    if !enabled() {
        return pyo3::Python::attach(f);
    }
    let t0 = Instant::now();
    pyo3::Python::attach(|py| {
        let t1 = Instant::now();
        let out = f(py);
        let t2 = Instant::now();
        PHASE_NS[P_ATTACH_WAIT]
            .fetch_add(t1.duration_since(t0).as_nanos() as u64, Ordering::Relaxed);
        PHASE_N[P_ATTACH_WAIT].fetch_add(1, Ordering::Relaxed);
        PHASE_NS[P_ATTACH_HOLD]
            .fetch_add(t2.duration_since(t1).as_nanos() as u64, Ordering::Relaxed);
        PHASE_N[P_ATTACH_HOLD].fetch_add(1, Ordering::Relaxed);
        out
    })
}

/// Aggregate table (empty unless profiling).
pub fn report() -> String {
    if !enabled() {
        return String::new();
    }
    let mut s = String::from("---- request-path profile ----\n");
    for i in 0..N_PHASES {
        let n = PHASE_N[i].load(Ordering::Relaxed);
        let ns = PHASE_NS[i].load(Ordering::Relaxed);
        let mean_us = if n > 0 {
            ns as f64 / n as f64 / 1000.0
        } else {
            0.0
        };
        s.push_str(&format!(
            "phase {:12} n={:8} mean={:9.3}us\n",
            PHASE_NAMES[i], n, mean_us
        ));
    }
    let reqs = COUNTERS[C_REQUESTS].load(Ordering::Relaxed).max(1) as f64;
    for i in 0..N_COUNTERS {
        if i == C_REQUESTS {
            s.push_str(&format!(
                "cnt   {:12} {}\n",
                COUNTER_NAMES[i],
                COUNTERS[i].load(Ordering::Relaxed)
            ));
            continue;
        }
        s.push_str(&format!(
            "cnt   {:12} {:.3}/req\n",
            COUNTER_NAMES[i],
            COUNTERS[i].load(Ordering::Relaxed) as f64 / reqs
        ));
    }
    s
}
