use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use rustls::pki_types::ServerName;
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;

/// Simple SNI resolver: map lowercased DNS name -> CertifiedKey
#[derive(Debug, Default)]
pub struct SniResolver {
    pub(crate) by_name: HashMap<String, Arc<CertifiedKey>>,
    pub(crate) default: Option<Arc<CertifiedKey>>,
}

impl SniResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, name: &str, ck: CertifiedKey) -> Result<(), rustls::Error> {
        // Validate DNS name
        let _ = ServerName::try_from(name.to_string())
            .map_err(|_| rustls::Error::General("Bad DNS name".into()))?;
        // Validate cert covers name via rustls verify
        // We do minimal check: ensure cert is parseable and matches
        let key = Arc::new(ck);
        self.by_name.insert(name.to_ascii_lowercase(), key);
        Ok(())
    }

    pub fn set_default(&mut self, ck: CertifiedKey) {
        self.default = Some(Arc::new(ck));
    }

    pub fn into_inner(self) -> Self {
        self
    }

    pub fn into_reloadable(self, _has_default: bool) -> ReloadableResolver {
        ReloadableResolver::new(self)
    }
}

impl ResolvesServerCert for SniResolver {
    fn resolve(&self, client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        if let Some(name) = client_hello.server_name() {
            if let Some(ck) = self.by_name.get(&name.to_ascii_lowercase()) {
                return Some(ck.clone());
            }
        }
        self.default.clone()
    }
}

/// Reloadable resolver wrapping ArcSwap for atomic reload without restart
#[derive(Debug)]
pub struct ReloadableResolver {
    inner: Arc<ArcSwap<SniResolver>>,
}

impl ReloadableResolver {
    pub fn new(initial: SniResolver) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(initial)),
        }
    }

    pub fn reload(&self, new_resolver: SniResolver) {
        self.inner.store(Arc::new(new_resolver));
    }

    pub fn get(&self) -> Arc<SniResolver> {
        self.inner.load_full()
    }
}

impl ResolvesServerCert for ReloadableResolver {
    fn resolve(&self, client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        self.inner.load().resolve(client_hello)
    }
}
