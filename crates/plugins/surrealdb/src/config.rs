use std::collections::HashMap;

use anyhow::{Context as _, bail};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::{
    Database as DatabaseCredentials, Namespace as NamespaceCredentials, Root,
};

#[derive(Hash, Eq, PartialEq, Clone, Copy, Debug)]
pub enum CredentialLevel {
    Root,
    Namespace,
    Database,
}

impl CredentialLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Root => "root",
            Self::Namespace => "namespace",
            Self::Database => "database",
        }
    }

    fn from_config(config: &HashMap<String, String>) -> anyhow::Result<Self> {
        match config
            .get("level")
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
        {
            None => Ok(Self::Root),
            Some(level) if level.eq_ignore_ascii_case("root") => Ok(Self::Root),
            Some(level) if level.eq_ignore_ascii_case("namespace") => Ok(Self::Namespace),
            Some(level) if level.eq_ignore_ascii_case("database") => Ok(Self::Database),
            Some(level) => bail!(
                "seamlezz:surrealdb 'level' must be one of root, namespace, database, got '{level}'"
            ),
        }
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct ConnectionKey {
    pub url: String,
    pub namespace: String,
    pub database: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub level: CredentialLevel,
}

impl ConnectionKey {
    pub fn url_for_logging(&self) -> String {
        redact_url_credentials(&self.url)
    }

    pub fn from_config(config: &HashMap<String, String>) -> anyhow::Result<Self> {
        let url = config
            .get("url")
            .context("seamlezz:surrealdb requires 'url' in host_interfaces config")?
            .clone();
        let namespace = config
            .get("namespace")
            .context("seamlezz:surrealdb requires 'namespace' in host_interfaces config")?
            .clone();
        let database = config
            .get("database")
            .context("seamlezz:surrealdb requires 'database' in host_interfaces config")?
            .clone();

        if url.is_empty() {
            bail!("seamlezz:surrealdb 'url' must not be empty");
        }
        if namespace.is_empty() {
            bail!("seamlezz:surrealdb 'namespace' must not be empty");
        }
        if database.is_empty() {
            bail!("seamlezz:surrealdb 'database' must not be empty");
        }

        let username = config.get("username").cloned().filter(|v| !v.is_empty());
        let password = config.get("password").cloned().filter(|v| !v.is_empty());
        let level = CredentialLevel::from_config(config)?;

        if username.is_some() && password.is_none() {
            bail!("seamlezz:surrealdb 'password' is required when 'username' is set");
        }

        Ok(Self {
            url,
            namespace,
            database,
            username,
            password,
            level,
        })
    }
}

pub async fn connect(key: &ConnectionKey) -> anyhow::Result<Surreal<Any>> {
    let db: Surreal<Any> = Surreal::init();
    db.connect(&key.url).await?;

    if let Some(username) = &key.username {
        let password = key
            .password
            .clone()
            .context("username set but password missing")?;
        match key.level {
            CredentialLevel::Root => {
                db.signin(Root {
                    username: username.clone(),
                    password,
                })
                .await?;
            }
            CredentialLevel::Namespace => {
                db.signin(NamespaceCredentials {
                    namespace: key.namespace.clone(),
                    username: username.clone(),
                    password,
                })
                .await?;
            }
            CredentialLevel::Database => {
                db.signin(DatabaseCredentials {
                    namespace: key.namespace.clone(),
                    database: key.database.clone(),
                    username: username.clone(),
                    password,
                })
                .await?;
            }
        }
    }

    db.use_ns(&key.namespace).use_db(&key.database).await?;

    Ok(db)
}

fn redact_url_credentials(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };

    let (scheme, rest) = url.split_at(scheme_end + 3);
    let Some(at_pos) = rest.rfind('@') else {
        return url.to_string();
    };

    let auth = &rest[..at_pos];
    if auth.is_empty() || !auth.contains(':') {
        return url.to_string();
    }

    format!("{scheme}{}", &rest[at_pos + 1..])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn valid_minimal_config() {
        let cfg = config(&[("url", "memory"), ("namespace", "ns"), ("database", "db")]);
        let key = ConnectionKey::from_config(&cfg).unwrap();
        assert_eq!(key.url, "memory");
        assert_eq!(key.namespace, "ns");
        assert_eq!(key.database, "db");
        assert!(key.username.is_none());
        assert!(key.password.is_none());
        assert_eq!(key.level, CredentialLevel::Root);
    }

    #[test]
    fn valid_with_credentials() {
        let cfg = config(&[
            ("url", "memory"),
            ("namespace", "ns"),
            ("database", "db"),
            ("username", "admin"),
            ("password", "secret"),
        ]);
        let key = ConnectionKey::from_config(&cfg).unwrap();
        assert_eq!(key.username.as_deref(), Some("admin"));
        assert_eq!(key.password.as_deref(), Some("secret"));
        assert_eq!(key.level, CredentialLevel::Root);
    }

    #[test]
    fn valid_with_database_credentials() {
        let cfg = config(&[
            ("url", "memory"),
            ("namespace", "ns"),
            ("database", "db"),
            ("username", "admin"),
            ("password", "secret"),
            ("level", "database"),
        ]);
        let key = ConnectionKey::from_config(&cfg).unwrap();
        assert_eq!(key.level, CredentialLevel::Database);
    }

    #[test]
    fn valid_with_namespace_credentials() {
        let cfg = config(&[
            ("url", "memory"),
            ("namespace", "ns"),
            ("database", "db"),
            ("username", "admin"),
            ("password", "secret"),
            ("level", "namespace"),
        ]);
        let key = ConnectionKey::from_config(&cfg).unwrap();
        assert_eq!(key.level, CredentialLevel::Namespace);
    }

    #[test]
    fn invalid_level() {
        let cfg = config(&[
            ("url", "memory"),
            ("namespace", "ns"),
            ("database", "db"),
            ("level", "record"),
        ]);
        let err = ConnectionKey::from_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("level"));
    }

    #[test]
    fn missing_url() {
        let cfg = config(&[("namespace", "ns"), ("database", "db")]);
        let err = ConnectionKey::from_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("url"));
    }

    #[test]
    fn missing_namespace() {
        let cfg = config(&[("url", "memory"), ("database", "db")]);
        let err = ConnectionKey::from_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("namespace"));
    }

    #[test]
    fn missing_database() {
        let cfg = config(&[("url", "memory"), ("namespace", "ns")]);
        let err = ConnectionKey::from_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("database"));
    }

    #[test]
    fn empty_url() {
        let cfg = config(&[("url", ""), ("namespace", "ns"), ("database", "db")]);
        let err = ConnectionKey::from_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("url"));
    }

    #[test]
    fn empty_namespace() {
        let cfg = config(&[("url", "memory"), ("namespace", ""), ("database", "db")]);
        let err = ConnectionKey::from_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("namespace"));
    }

    #[test]
    fn empty_database() {
        let cfg = config(&[("url", "memory"), ("namespace", "ns"), ("database", "")]);
        let err = ConnectionKey::from_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("database"));
    }

    #[test]
    fn username_without_password() {
        let cfg = config(&[
            ("url", "memory"),
            ("namespace", "ns"),
            ("database", "db"),
            ("username", "admin"),
        ]);
        let err = ConnectionKey::from_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("password"));
    }

    #[test]
    fn empty_username_ignored() {
        let cfg = config(&[
            ("url", "memory"),
            ("namespace", "ns"),
            ("database", "db"),
            ("username", ""),
        ]);
        let key = ConnectionKey::from_config(&cfg).unwrap();
        assert!(key.username.is_none());
    }

    #[test]
    fn empty_password_ignored() {
        let cfg = config(&[
            ("url", "memory"),
            ("namespace", "ns"),
            ("database", "db"),
            ("password", ""),
        ]);
        let key = ConnectionKey::from_config(&cfg).unwrap();
        assert!(key.password.is_none());
    }

    #[test]
    fn url_for_logging_redacts_credentials() {
        let key = ConnectionKey {
            url: "ws://admin:secret@127.0.0.1:8000".to_string(),
            namespace: "ns".to_string(),
            database: "db".to_string(),
            username: None,
            password: None,
            level: CredentialLevel::Root,
        };
        assert_eq!(key.url_for_logging(), "ws://127.0.0.1:8000");
    }

    #[test]
    fn url_for_logging_preserves_url_without_credentials() {
        let key = ConnectionKey {
            url: "memory".to_string(),
            namespace: "ns".to_string(),
            database: "db".to_string(),
            username: None,
            password: None,
            level: CredentialLevel::Root,
        };
        assert_eq!(key.url_for_logging(), "memory");
    }
}
