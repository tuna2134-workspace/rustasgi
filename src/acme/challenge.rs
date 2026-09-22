use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Default)]
pub struct ChallengeStore {
    inner: Arc<RwLock<HashMap<String, String>>>,
}

impl ChallengeStore {
    pub fn new() -> Self { Self { inner: Arc::new(RwLock::new(HashMap::new())) } }
    pub fn insert(&self, token: String, auth: String) { self.inner.write().unwrap().insert(token, auth); }
    pub fn get(&self, token: &str) -> Option<String> { self.inner.read().unwrap().get(token).cloned() }
    pub fn remove(&self, token: &str) { self.inner.write().unwrap().remove(token); }
}

pub fn is_valid_token(token: &str) -> bool {
    if token.is_empty() || token.len() > 128 { return false; }
    if token.contains('/') || token.contains('\\') || token.contains("..") { return false; }
    token.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
}
