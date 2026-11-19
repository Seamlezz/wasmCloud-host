use anyhow::{Context as _, bail};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::{
    Database as DatabaseCredentials, Namespace as NamespaceCredentials, Root,
};
use wash_runtime::wit::{Config, ConfigStr};

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

    fn from_config(config: &Config) -> anyhow::Result<Self> {
        match config
            .value("level")
            .map(ConfigStr::trim)
            .filter(|v| !v.is_empty())
        {
            None => Ok(Self::Root),
            Some(level) if level.expose().eq_ignore_ascii_case("root") => Ok(Self::Root),
            Some(level) if level.expose().eq_ignore_ascii_case("namespace") => Ok(Self::Namespace),
            Some(level) if level.expose().eq_ignore_ascii_case("database") => Ok(Self::Database),
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
    /// Whether `url` came from a plugin secret, so logging never shows it.
    url_secret: bool,
}

impl ConnectionKey {
    pub fn url_for_logging(&self) -> String {
        if self.url_secret {
            return "<redacted>".to_string();
        }
        redact_url_credentials(&self.url)
    }

    /// Reads the interface config. Every key may be plain text or a plugin
    /// secret resolved from a `secrets:` entry.
    pub fn from_config(config: &Config) -> anyhow::Result<Self> {
        let url = config
            .value("url")
            .context("seamlezz:surrealdb requires 'url' in host_interfaces config")?;
        let url_secret = url.is_secret();
        let url = url.expose().to_string();
        let namespace = config
            .expose("namespace")
            .context("seamlezz:surrealdb requires 'namespace' in host_interfaces config")?
            .to_string();
        let database = config
            .expose("database")
            .context("seamlezz:surrealdb requires 'database' in host_interfaces config")?
            .to_string();

        if url.is_empty() {
            bail!("seamlezz:surrealdb 'url' must not be empty");
        }
        if namespace.is_empty() {
            bail!("seamlezz:surrealdb 'namespace' must not be empty");
        }
        if database.is_empty() {
            bail!("seamlezz:surrealdb 'database' must not be empty");
        }

        let username = config
            .expose("username")
            .filter(|v| !v.is_empty())
            .map(str::to_string);
        let password = config
            .expose("password")
            .filter(|v| !v.is_empty())
            .map(str::to_string);
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
            url_secret,
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

    use wash_runtime::plugin::SecretValue;

    fn config(pairs: &[(&str, &str)]) -> Config {
        pairs.iter().map(|&(k, v)| (k, v)).collect()
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
            url_secret: false,
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
            url_secret: false,
        };
        assert_eq!(key.url_for_logging(), "memory");
    }

    #[test]
    fn credentials_from_plugin_secrets() {
        let mut cfg = config(&[("url", "memory"), ("namespace", "ns"), ("database", "db")]);
        cfg.insert("username", SecretValue::new("admin"));
        cfg.insert("password", SecretValue::new("secret"));
        let key = ConnectionKey::from_config(&cfg).unwrap();
        assert_eq!(key.username.as_deref(), Some("admin"));
        assert_eq!(key.password.as_deref(), Some("secret"));
    }

    #[test]
    fn url_from_plugin_secret_is_never_logged() {
        let mut cfg = config(&[("namespace", "ns"), ("database", "db")]);
        cfg.insert("url", SecretValue::new("ws://127.0.0.1:8000"));
        let key = ConnectionKey::from_config(&cfg).unwrap();
        assert_eq!(key.url, "ws://127.0.0.1:8000");
        assert_eq!(key.url_for_logging(), "<redacted>");
    }

    #[test]
    fn invalid_secret_level_is_not_quoted() {
        let mut cfg = config(&[("url", "memory"), ("namespace", "ns"), ("database", "db")]);
        cfg.insert("level", SecretValue::new("record"));
        let err = ConnectionKey::from_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("<redacted>"));
        assert!(!err.to_string().contains("record"));
    }
}
