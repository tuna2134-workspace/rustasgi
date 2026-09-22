use std::path::Path;
use std::sync::Arc;

use arc_swap::ArcSwap;
use rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

/// Holds the current rustls ServerConfig behind ArcSwap for lock-free reload
#[derive(Debug)]
pub struct TlsState {
    pub config: Arc<ArcSwap<ServerConfig>>,
    /// Number of certs for logging (SNI map size or 1)
    cert_count: usize,
    /// Stored paths for file-watcher reload (single cert case)
    pub cert_path: Option<std::path::PathBuf>,
    pub key_path: Option<std::path::PathBuf>,
}

impl TlsState {
    #[allow(dead_code)]
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config: Arc::new(ArcSwap::from_pointee(config)),
            cert_count: 1,
            cert_path: None,
            key_path: None,
        }
    }

    pub fn from_pem_files(cert_path: &Path, key_path: &Path) -> Result<Self, crate::tls::TlsError> {
        let cfg = crate::tls::config::build_single_config(cert_path, key_path)?;
        Ok(Self {
            config: Arc::new(ArcSwap::from_pointee(cfg)),
            cert_count: 1,
            cert_path: Some(cert_path.to_path_buf()),
            key_path: Some(key_path.to_path_buf()),
        })
    }

    pub fn from_sni_entries(
        entries: &[(String, std::path::PathBuf, std::path::PathBuf)],
        default: Option<(&Path, &Path)>,
    ) -> Result<Self, crate::tls::TlsError> {
        let cfg = crate::tls::config::build_sni_config(entries, default)?;
        let count = entries.len().max(1);
        Ok(Self {
            config: Arc::new(ArcSwap::from_pointee(cfg)),
            cert_count: count,
            cert_path: None,
            key_path: None,
        })
    }

    pub fn get(&self) -> Arc<ServerConfig> {
        self.config.load_full()
    }

    pub fn acceptor(&self) -> TlsAcceptor {
        TlsAcceptor::from(self.get())
    }

    pub fn cert_count(&self) -> usize {
        self.cert_count
    }

    pub fn reload(&self, new_config: ServerConfig) {
        self.config.store(Arc::new(new_config));
    }

    pub fn reload_from_pem(
        &self,
        cert_path: &Path,
        key_path: &Path,
    ) -> Result<(), crate::tls::TlsError> {
        let new_cfg = crate::tls::config::build_single_config(cert_path, key_path)?;
        self.reload(new_cfg);
        eprintln!(
            "INFO rustwasgi: certificate reload: new config from {} {}",
            cert_path.display(),
            key_path.display()
        );
        Ok(())
    }
}

/// Helper for atomic cert install - writes to temp then rename
#[allow(dead_code)]
pub struct TlsReloader {
    pub state: Arc<TlsState>,
}

#[allow(dead_code)]
impl TlsReloader {
    pub fn new(state: Arc<TlsState>) -> Self {
        Self { state }
    }

    pub fn reload(&self, new_config: ServerConfig) {
        self.state.reload(new_config);
        eprintln!("INFO rustwasgi: certificate reload: new config installed");
    }
}
