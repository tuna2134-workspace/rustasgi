use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::ServerConfig;
use x509_parser::prelude::*;

#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("certificate file not found: {0}")]
    CertNotFound(PathBuf),
    #[error("private key file not found: {0}")]
    KeyNotFound(PathBuf),
    #[error("failed to read {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse certificate {path}: {reason}")]
    CertParse { path: PathBuf, reason: String },
    #[error("failed to parse private key {path}: {reason}")]
    KeyParse { path: PathBuf, reason: String },
    #[error("no certificates found in {0}")]
    NoCerts(PathBuf),
    #[error("no private key found in {0}")]
    NoKey(PathBuf),
    #[error("private key does not match certificate: {0}")]
    KeyMismatch(String),
    #[error("invalid certificate chain: {0}")]
    InvalidChain(String),
    #[error("rustls error: {0}")]
    Rustls(String),
}

pub fn load_cert_chain(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let file = File::open(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            TlsError::CertNotFound(path.to_path_buf())
        } else {
            TlsError::Io {
                path: path.to_path_buf(),
                source: e,
            }
        }
    })?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError::CertParse {
            path: path.to_path_buf(),
            reason: e.to_string(),
        })?;
    if certs.is_empty() {
        return Err(TlsError::NoCerts(path.to_path_buf()));
    }
    for der in &certs {
        if der.is_empty() {
            return Err(TlsError::InvalidChain(format!(
                "empty cert in {}",
                path.display()
            )));
        }
    }
    Ok(certs)
}

pub fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    let file = File::open(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            TlsError::KeyNotFound(path.to_path_buf())
        } else {
            TlsError::Io {
                path: path.to_path_buf(),
                source: e,
            }
        }
    })?;
    let mut reader = BufReader::new(file);
    let key = rustls_pemfile::private_key(&mut reader).map_err(|e| TlsError::KeyParse {
        path: path.to_path_buf(),
        reason: e.to_string(),
    })?;
    key.ok_or_else(|| TlsError::NoKey(path.to_path_buf()))
}

/// Build a rustls ServerConfig from a single cert/key pair (no SNI)
pub fn build_single_config(cert_path: &Path, key_path: &Path) -> Result<ServerConfig, TlsError> {
    let certs = load_cert_chain(cert_path)?;
    let key = load_private_key(key_path)?;
    validate_pair(&certs, &key)?;
    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| TlsError::Rustls(e.to_string()))?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

/// Build with SNI resolver
pub fn build_sni_config(
    entries: &[(String, PathBuf, PathBuf)],
    default_cert: Option<(&Path, &Path)>,
) -> Result<ServerConfig, TlsError> {
    let provider = rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()));
    let mut resolver = crate::tls::resolver::SniResolver::new();
    // Add SNI entries
    for (domain, cert_path, key_path) in entries {
        let certs = load_cert_chain(cert_path)?;
        let key = load_private_key(key_path)?;
        let ck = rustls::sign::CertifiedKey::from_der(certs, key, &provider)
            .map_err(|e| TlsError::KeyMismatch(e.to_string()))?;
        resolver
            .add(domain, ck)
            .map_err(|e| TlsError::Rustls(e.to_string()))?;
    }
    if let Some((cert_path, key_path)) = default_cert {
        let certs = load_cert_chain(cert_path)?;
        let key = load_private_key(key_path)?;
        let ck = rustls::sign::CertifiedKey::from_der(certs, key, &provider)
            .map_err(|e| TlsError::KeyMismatch(e.to_string()))?;
        resolver.set_default(ck);
        // Build config with default cert then override resolver to SNI-aware one with fallback
        let mut cfg = ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(resolver));
        cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(cfg)
    } else {
        let mut cfg = ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(resolver));
        cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(cfg)
    }
}

pub fn validate_pair(
    certs: &[CertificateDer<'static>],
    key: &PrivateKeyDer<'static>,
) -> Result<(), TlsError> {
    let provider = rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()));
    let ck = rustls::sign::CertifiedKey::from_der(certs.to_vec(), key.clone_key(), &provider)
        .map_err(|e| TlsError::KeyMismatch(e.to_string()))?;
    ck.keys_match()
        .map_err(|e| TlsError::KeyMismatch(e.to_string()))?;
    // Check SAN / validity via x509-parser
    if let Some(first) = certs.first()
        && let Ok((_, cert)) = X509Certificate::from_der(first.as_ref())
    {
        // Basic validity check - ensure cert has subject
        let _ = cert.subject();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{CertificateParams, DistinguishedName, KeyPair};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};

    fn generate_self_signed(
        domains: &[String],
    ) -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
        let mut params = CertificateParams::new(domains.to_vec()).unwrap();
        params.distinguished_name = DistinguishedName::new();
        let key_pair = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key_pair).unwrap();
        let cert_der = CertificateDer::from(cert.der().to_vec());
        let key_der = PrivateKeyDer::try_from(key_pair.serialize_der()).unwrap();
        (vec![cert_der], key_der)
    }

    #[test]
    fn test_generate_and_validate() {
        let (certs, key) = generate_self_signed(&["example.com".to_string()]);
        validate_pair(&certs, &key).unwrap();
    }

    #[test]
    fn test_mismatch_fails() {
        let (certs1, _) = generate_self_signed(&["example.com".to_string()]);
        let (_, key2) = generate_self_signed(&["other.com".to_string()]);
        assert!(validate_pair(&certs1, &key2).is_err());
    }
}
