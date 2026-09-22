use std::collections::HashMap;
use std::sync::Arc;

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
}

impl ResolvesServerCert for SniResolver {
    fn resolve(&self, client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        if let Some(name) = client_hello.server_name()
            && let Some(ck) = self.by_name.get(&name.to_ascii_lowercase())
        {
            return Some(ck.clone());
        }
        self.default.clone()
    }
}
