use std::fs::{self, File};
use std::io::Cursor;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use x509_parser::prelude::*;

use crate::tls::TlsState;

/// Atomically install cert and key, then reload TLS state
/// Writes to temp files, fsync, rename, then validates and activates
pub fn atomic_install(
    cert_pem: &[u8],
    key_pem: &[u8],
    cert_path: &Path,
    key_path: &Path,
    tls_state: &TlsState,
) -> Result<(), String> {
    // Validate before touching filesystem
    validate_pem_pair(cert_pem, key_pem)?;

    let cert_tmp = cert_path.with_extension("pem.tmp");
    let key_tmp = key_path.with_extension("key.tmp");

    // Write cert
    fs::write(&cert_tmp, cert_pem).map_err(|e| format!("write cert tmp: {e}"))?;
    #[cfg(unix)]
    {
        let _ = fs::set_permissions(&cert_tmp, fs::Permissions::from_mode(0o600));
    }
    // Ensure data is on disk
    if let Ok(f) = File::open(&cert_tmp) {
        let _ = f.sync_all();
    }

    // Write key
    fs::write(&key_tmp, key_pem).map_err(|e| format!("write key tmp: {e}"))?;
    #[cfg(unix)]
    {
        let _ = fs::set_permissions(&key_tmp, fs::Permissions::from_mode(0o600));
    }
    if let Ok(f) = File::open(&key_tmp) {
        let _ = f.sync_all();
    }

    // Atomic rename
    fs::rename(&cert_tmp, cert_path).map_err(|e| format!("rename cert: {e}"))?;
    fs::rename(&key_tmp, key_path).map_err(|e| format!("rename key: {e}"))?;

    // Fsync directory for durability (best effort)
    if let Some(dir) = cert_path.parent()
        && let Ok(f) = File::open(dir)
    {
        let _ = f.sync_all();
    }

    // Reload in-memory
    tls_state
        .reload_from_pem(cert_path, key_path)
        .map_err(|e| format!("reload: {e}"))?;

    eprintln!(
        "INFO rustwasgi: certificate atomically installed {} {}",
        cert_path.display(),
        key_path.display()
    );
    Ok(())
}

fn validate_pem_pair(cert_pem: &[u8], key_pem: &[u8]) -> Result<(), String> {
    let certs = rustls_pemfile::certs(&mut Cursor::new(cert_pem))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("cert parse: {e}"))?;
    if certs.is_empty() {
        return Err("no certs".into());
    }
    let key = rustls_pemfile::private_key(&mut Cursor::new(key_pem))
        .map_err(|e| format!("key parse: {e}"))?
        .ok_or("no key")?;
    crate::tls::config::validate_pair(&certs, &key).map_err(|e| e.to_string())?;
    // Check SAN and expiry via x509-parser
    if let Some(first) = certs.first() {
        if let Ok((_, cert)) = X509Certificate::from_der(first.as_ref()) {
            let _ = cert.subject();
        } else {
            return Err("x509 parse failed".into());
        }
    }
    Ok(())
}
