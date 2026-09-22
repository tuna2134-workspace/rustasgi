use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct AccountData {
    credentials: String, // JSON serialized AccountCredentials
    email: String,
    directory: String,
}

/// Load or create ACME account, persist with 0600
pub async fn load_or_create_account(
    acme_dir: &Path,
    email: &str,
    directory_url: &str,
) -> Result<instant_acme::Account, String> {
    let account_path = acme_dir.join("account.json");
    if account_path.exists() {
        let data = fs::read_to_string(&account_path)
            .map_err(|e| format!("failed to read account file: {e}"))?;
        let acc_data: AccountData = serde_json::from_str(&data)
            .map_err(|e| format!("failed to parse account file: {e}"))?;
        let creds: instant_acme::AccountCredentials =
            serde_json::from_str(&acc_data.credentials)
                .map_err(|e| format!("failed to parse credentials: {e}"))?;
        let account = instant_acme::Account::from_credentials(creds)
            .await
            .map_err(|e| format!("failed to restore account: {e}"))?;
        eprintln!(
            "INFO rustwasgi: ACME account loaded from {}",
            account_path.display()
        );
        return Ok(account);
    }

    // Create new account
    let (account, credentials) = instant_acme::Account::create(
        &instant_acme::NewAccount {
            contact: &[&format!("mailto:{email}")],
            terms_of_service_agreed: true,
            only_return_existing: false,
        },
        directory_url,
        None,
    )
    .await
    .map_err(|e| format!("failed to create ACME account: {e}"))?;

    // Persist with 0600
    fs::create_dir_all(acme_dir).map_err(|e| format!("failed to create acme dir: {e}"))?;
    #[cfg(unix)]
    {
        let _ = fs::set_permissions(acme_dir, fs::Permissions::from_mode(0o700));
    }
    let cred_str =
        serde_json::to_string(&credentials).map_err(|e| format!("serialize creds: {e}"))?;
    let data = AccountData {
        credentials: cred_str,
        email: email.to_string(),
        directory: directory_url.to_string(),
    };
    let json = serde_json::to_string_pretty(&data).unwrap();
    let tmp = acme_dir.join("account.json.tmp");
    fs::write(&tmp, json).map_err(|e| format!("write tmp account: {e}"))?;
    #[cfg(unix)]
    {
        let _ = fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600));
    }
    fs::rename(&tmp, &account_path).map_err(|e| format!("rename account: {e}"))?;
    eprintln!(
        "INFO rustwasgi: ACME account created and saved to {}",
        account_path.display()
    );
    Ok(account)
}
