use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Default)]
pub struct ChallengeStore {
    inner: Arc<RwLock<HashMap<String, String>>>,
}

impl ChallengeStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn insert(&self, token: String, auth: String) {
        if !is_valid_token(&token) {
            eprintln!("WARN rustwasgi: rejecting invalid challenge token {token:?}");
            return;
        }
        self.inner.write().unwrap().insert(token, auth);
        eprintln!("INFO rustwasgi: ACME challenge installed for token");
    }

    pub fn get(&self, token: &str) -> Option<String> {
        if !is_valid_token(token) {
            return None;
        }
        self.inner.read().unwrap().get(token).cloned()
    }

    pub fn remove(&self, token: &str) {
        self.inner.write().unwrap().remove(token);
        eprintln!("INFO rustwasgi: ACME challenge removed");
    }

    pub fn clear(&self) {
        self.inner.write().unwrap().clear();
    }
}

pub fn is_valid_token(token: &str) -> bool {
    if token.is_empty() || token.len() > 128 {
        return false;
    }
    if token.contains('/') || token.contains('\\') || token.contains("..") {
        return false;
    }
    // ACME token is base64url (alphanum + - _), no dot, no slash
    token
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid() {
        assert!(is_valid_token("abc123-_9"));
        assert!(is_valid_token("abc123-_-XYZ_123"));
    }

    #[test]
    fn invalid_traversal() {
        assert!(!is_valid_token("../etc/passwd"));
        assert!(!is_valid_token("a/b"));
        assert!(!is_valid_token("a\\b"));
    }

    #[test]
    fn invalid_chars() {
        assert!(!is_valid_token("abc!"));
    }
}
