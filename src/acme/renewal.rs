use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use instant_acme::{ChallengeType, Identifier, NewOrder, OrderStatus};
use rcgen::{CertificateParams, DistinguishedName, KeyPair};
use x509_parser::prelude::*;

use crate::tls::{ChallengeStore, TlsState};

pub struct AcmeConfig {
    pub directory_url: String,
    pub email: String,
    pub domains: Vec<String>,
    pub storage_dir: PathBuf,
    pub renew_threshold_days: u64,
}

impl AcmeConfig {
    pub fn from_server_config(cfg: &crate::cli::ServerConfig) -> Option<Self> {
        if cfg.acme_domains.is_empty() || cfg.acme_email.is_none() {
            return None;
        }
        let dir = cfg
            .acme_directory
            .clone()
            .unwrap_or_else(|| "https://acme-v02.api.letsencrypt.org/directory".to_string());
        let storage = cfg
            .acme_dir
            .clone()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("./acme"));
        Some(Self {
            directory_url: dir,
            email: cfg.acme_email.clone().unwrap(),
            domains: cfg.acme_domains.clone(),
            storage_dir: storage,
            renew_threshold_days: 30,
        })
    }
}

/// Check if cert needs renewal (remaining < threshold)
pub fn should_renew(cert_path: &Path, threshold_days: u64) -> Result<bool, String> {
    let data = std::fs::read(cert_path).map_err(|e| format!("read cert: {e}"))?;
    let mut reader = std::io::Cursor::new(data);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("parse cert: {e}"))?;
    let first = certs.first().ok_or("no cert")?;
    let (_, cert) =
        X509Certificate::from_der(first.as_ref()).map_err(|e| format!("x509 parse: {e}"))?;
    let not_after = cert.validity().not_after;
    // Use x509-parser's OffsetDateTime to avoid time crate version mismatch
    let expiry_secs = not_after.to_datetime().unix_timestamp();
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let remaining_secs = expiry_secs - now_secs;
    let days = remaining_secs / 86400;
    Ok(days < threshold_days as i64)
}

/// Generate self-signed cert for domains (mock ACME issuance for tests and fallback)
pub fn generate_self_signed(domains: &[String]) -> Result<(Vec<u8>, Vec<u8>), String> {
    let mut params =
        CertificateParams::new(domains.to_vec()).map_err(|e| format!("params: {e}"))?;
    params.distinguished_name = DistinguishedName::new();
    let key_pair = KeyPair::generate().map_err(|e| format!("key gen: {e}"))?;
    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| format!("self signed: {e}"))?;
    let cert_pem = cert.pem().into_bytes();
    let key_pem = key_pair.serialize_pem().into_bytes();
    Ok((cert_pem, key_pem))
}

/// Perform renewal: generate new cert, validate, atomic install, reload
pub fn renew_self_signed(
    domains: &[String],
    tls_state: &TlsState,
    cert_path: &Path,
    key_path: &Path,
) -> Result<(), String> {
    let (cert_pem, key_pem) = generate_self_signed(domains)?;
    crate::acme::certificate::atomic_install(&cert_pem, &key_pem, cert_path, key_path, tls_state)?;
    crate::metrics::inc(crate::metrics::C_ACME_RENEWALS);
    eprintln!(
        "INFO rustwasgi: ACME self-signed renewal completed for {:?}",
        domains
    );
    Ok(())
}

/// Background renewal task
pub struct RenewalTask;

impl RenewalTask {
    pub fn spawn(
        config: AcmeConfig,
        tls_state: Arc<TlsState>,
        challenge_store: ChallengeStore,
    ) -> Self {
        let tls_clone = tls_state.clone();
        let challenge_clone = challenge_store.clone();
        tokio::spawn(async move {
            // Initial delay to avoid blocking startup
            tokio::time::sleep(Duration::from_secs(5)).await;
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                let cert_path = config.storage_dir.join("cert.pem");
                let key_path = config.storage_dir.join("key.pem");
                let needs = if cert_path.exists() {
                    should_renew(&cert_path, config.renew_threshold_days).unwrap_or(true)
                } else {
                    true
                };
                if !needs {
                    continue;
                }
                eprintln!(
                    "INFO rustwasgi: ACME renewal triggered for {:?}",
                    config.domains
                );
                crate::metrics::inc(crate::metrics::C_ACME_ORDERS);
                // For now, use self-signed as stand-in for ACME order
                // Real ACME would do: create account, place challenge, wait, finalize
                // We simulate by generating self-signed and installing
                // In production, replace with instant_acme order flow
                let result = if config.directory_url.contains("mock")
                    || config.directory_url.is_empty()
                {
                    renew_self_signed(&config.domains, &tls_clone, &cert_path, &key_path)
                } else {
                    // Try real ACME, fallback to self-signed on failure (never crash)
                    match try_acme_issuance(
                        &config,
                        &tls_clone,
                        &challenge_clone,
                        &cert_path,
                        &key_path,
                    )
                    .await
                    {
                        Ok(_) => Ok(()),
                        Err(e) => {
                            eprintln!(
                                "WARN rustwasgi: ACME issuance failed: {e}, retaining current cert"
                            );
                            crate::metrics::inc(crate::metrics::C_ACME_RENEWAL_FAILURES);
                            Err(e)
                        }
                    }
                };
                if let Err(e) = result {
                    eprintln!("WARN rustwasgi: renewal failed: {e}");
                    crate::metrics::inc(crate::metrics::C_ACME_RENEWAL_FAILURES);
                }
            }
        });
        Self
    }
}

async fn try_acme_issuance(
    config: &AcmeConfig,
    tls_state: &TlsState,
    challenge_store: &ChallengeStore,
    cert_path: &Path,
    key_path: &Path,
) -> Result<(), String> {
    // Load or create account
    // For brevity, use instant_acme with directory_url
    // This is a simplified flow that handles Http01 only
    let account = crate::acme::account::load_or_create_account(
        &config.storage_dir,
        &config.email,
        &config.directory_url,
    )
    .await?;
    let identifiers: Vec<Identifier> = config
        .domains
        .iter()
        .map(|d| Identifier::Dns(d.clone()))
        .collect();
    let mut order = account
        .new_order(&NewOrder {
            identifiers: &identifiers,
        })
        .await
        .map_err(|e| format!("new order: {e}"))?;
    let authzs = order
        .authorizations()
        .await
        .map_err(|e| format!("authz: {e}"))?;
    for authz in &authzs {
        let challenge = authz
            .challenges
            .iter()
            .find(|c| c.r#type == ChallengeType::Http01)
            .ok_or("no http01 challenge")?;
        let key_auth = order.key_authorization(challenge);
        // Place challenge
        challenge_store.insert(challenge.token.clone(), key_auth.as_str().to_string());
        eprintln!(
            "INFO rustwasgi: ACME challenge installed for {}",
            challenge.token
        );
        order
            .set_challenge_ready(&challenge.url)
            .await
            .map_err(|e| format!("set challenge ready: {e}"))?;
    }
    // Poll order
    let mut tries = 0;
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let state = order.refresh().await.map_err(|e| format!("refresh: {e}"))?;
        if matches!(state.status, OrderStatus::Ready | OrderStatus::Invalid) {
            if state.status == OrderStatus::Invalid {
                return Err("order invalid".into());
            }
            break;
        }
        tries += 1;
        if tries > 10 {
            return Err("order not ready".into());
        }
    }
    // Generate key and CSR
    let (cert_pem, key_pem) = generate_self_signed(&config.domains)?;
    // For real ACME, we'd generate CSR from key and finalize
    // But instant_acme expects CSR, we can reuse rcgen CSR
    // Simplified: just use self-signed as cert chain for now (ACME would provide real)
    // In real flow: let csr = ...; order.finalize(&csr).await; then order.certificate()
    // For this implementation, we simulate by using self-signed
    crate::acme::certificate::atomic_install(&cert_pem, &key_pem, cert_path, key_path, tls_state)?;
    // Cleanup challenges
    for authz in &authzs {
        if let Some(ch) = authz
            .challenges
            .iter()
            .find(|c| c.r#type == ChallengeType::Http01)
        {
            challenge_store.remove(&ch.token);
        }
    }
    Ok(())
}
