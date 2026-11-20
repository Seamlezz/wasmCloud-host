use anyhow::{Context as _, anyhow, bail};
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
        match optional(config, "level")?
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

#[derive(Hash, Eq, PartialEq, Clone)]
pub struct ConnectionKey {
    pub url: String,
    pub namespace: String,
    pub database: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub level: CredentialLevel,
    /// Which values came from a plugin secret, so logs and errors never show them.
    secrets: Secrets,
}

#[derive(Hash, Eq, PartialEq, Clone, Copy, Default)]
struct Secrets {
    url: bool,
    namespace: bool,
    database: bool,
    username: bool,
}

const REDACTED: &str = "<redacted>";

fn logged(value: &str, secret: bool) -> &str {
    if secret { REDACTED } else { value }
}

impl ConnectionKey {
    pub fn url_for_logging(&self) -> String {
        if self.secrets.url {
            return REDACTED.to_string();
        }
        redact_url_credentials(&self.url)
    }

    pub fn namespace_for_logging(&self) -> &str {
        logged(&self.namespace, self.secrets.namespace)
    }

    pub fn database_for_logging(&self) -> &str {
        logged(&self.database, self.secrets.database)
    }

    /// Reads the interface config. Every key may be plain text or a plugin
    /// secret resolved from a `secrets:` entry.
    pub fn from_config(config: &Config) -> anyhow::Result<Self> {
        let url = required(config, "url")?;
        let namespace = required(config, "namespace")?;
        let database = required(config, "database")?;
        let username = optional(config, "username")?;
        let password = optional(config, "password")?;
        let level = CredentialLevel::from_config(config)?;

        if username.is_some() && password.is_none() {
            bail!("seamlezz:surrealdb 'password' is required when 'username' is set");
        }

        Ok(Self {
            secrets: Secrets {
                url: url.is_secret(),
                namespace: namespace.is_secret(),
                database: database.is_secret(),
                username: username.is_some_and(ConfigStr::is_secret),
            },
            url: url.expose().to_string(),
            namespace: namespace.expose().to_string(),
            database: database.expose().to_string(),
            username: username.map(|v| v.expose().to_string()),
            password: password.map(|v| v.expose().to_string()),
            level,
        })
    }

    /// `error` from connecting with this key, without any secret value it may
    /// quote and without the credentials of a plain url.
    fn connect_error(&self, error: surrealdb::Error) -> anyhow::Error {
        let Secrets {
            url,
            namespace,
            database,
            username,
        } = self.secrets;
        let secret_keys = [
            ("url", url),
            ("namespace", namespace),
            ("database", database),
            ("username", username),
        ]
        .into_iter()
        .filter_map(|(key, secret)| secret.then_some(key))
        .collect::<Vec<_>>();
        if !secret_keys.is_empty() {
            return anyhow!(
                "connecting to SurrealDB failed; the reason is withheld because it may quote the \
                 secret in {}",
                secret_keys.join(", ")
            );
        }
        anyhow!(
            "connecting to SurrealDB failed: {}",
            error
                .to_string()
                .replace(&self.url, &self.url_for_logging())
        )
    }
}

impl std::fmt::Debug for ConnectionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionKey")
            .field("url", &self.url_for_logging())
            .field("namespace", &self.namespace_for_logging())
            .field("database", &self.database_for_logging())
            .field(
                "username",
                &self
                    .username
                    .as_deref()
                    .map(|v| logged(v, self.secrets.username)),
            )
            .field("password", &self.password.as_ref().map(|_| REDACTED))
            .field("level", &self.level)
            .finish()
    }
}

/// The value at `key`, or `None` when it is unset or empty. A secret that is
/// not UTF-8 is an error rather than unset.
fn optional<'a>(config: &'a Config, key: &str) -> anyhow::Result<Option<ConfigStr<'a>>> {
    match (config.get_value(key), config.value(key)) {
        (Some(_), None) => bail!("seamlezz:surrealdb '{key}' must be text"),
        (_, value) => Ok(value.filter(|v| !v.is_empty())),
    }
}

fn required<'a>(config: &'a Config, key: &str) -> anyhow::Result<ConfigStr<'a>> {
    if !config.contains_key(key) {
        bail!("seamlezz:surrealdb requires '{key}' in host_interfaces config");
    }
    optional(config, key)?.with_context(|| format!("seamlezz:surrealdb '{key}' must not be empty"))
}

pub async fn connect(key: &ConnectionKey) -> anyhow::Result<Surreal<Any>> {
    let db: Surreal<Any> = Surreal::init();
    db.connect(&key.url)
        .await
        .map_err(|error| key.connect_error(error))?;

    if let Some(username) = &key.username {
        let password = key
            .password
            .clone()
            .context("username set but password missing")?;
        let username = username.clone();
        let signin = match key.level {
            CredentialLevel::Root => db.signin(Root { username, password }).await,
            CredentialLevel::Namespace => {
                db.signin(NamespaceCredentials {
                    namespace: key.namespace.clone(),
                    username,
                    password,
                })
                .await
            }
            CredentialLevel::Database => {
                db.signin(DatabaseCredentials {
                    namespace: key.namespace.clone(),
                    database: key.database.clone(),
                    username,
                    password,
                })
                .await
            }
        };
        signin.map_err(|error| key.connect_error(error))?;
    }

    db.use_ns(&key.namespace)
        .use_db(&key.database)
        .await
        .map_err(|error| key.connect_error(error))?;

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
            secrets: Secrets::default(),
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
            secrets: Secrets::default(),
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

    fn secret_config(secret: &[(&str, &[u8])], text: &[(&str, &str)]) -> Config {
        let mut cfg = config(text);
        for &(key, value) in secret {
            cfg.insert(key, SecretValue::from_bytes(value.to_vec()));
        }
        cfg
    }

    #[test]
    fn secret_namespace_and_database_are_never_logged() {
        let cfg = secret_config(
            &[("namespace", b"tenant-ns"), ("database", b"tenant-db")],
            &[("url", "memory")],
        );
        let key = ConnectionKey::from_config(&cfg).unwrap();
        assert_eq!(key.namespace, "tenant-ns");
        assert_eq!(key.database, "tenant-db");
        assert_eq!(key.namespace_for_logging(), "<redacted>");
        assert_eq!(key.database_for_logging(), "<redacted>");
    }

    #[test]
    fn debug_never_shows_password_or_secrets() {
        let cfg = secret_config(
            &[
                ("url", b"ws://secret-host:8000"),
                ("namespace", b"tenant-ns"),
            ],
            &[
                ("database", "db"),
                ("username", "admin"),
                ("password", "hunter2"),
            ],
        );
        let printed = format!("{:?}", ConnectionKey::from_config(&cfg).unwrap());
        for hidden in ["hunter2", "secret-host", "tenant-ns"] {
            assert!(!printed.contains(hidden), "{printed}");
        }
        assert!(printed.contains("admin"), "{printed}");
    }

    #[test]
    fn non_text_secret_is_reported_as_not_text() {
        let cfg = secret_config(
            &[("url", &[0xff, 0xfe])],
            &[("namespace", "ns"), ("database", "db")],
        );
        let err = ConnectionKey::from_config(&cfg).unwrap_err().to_string();
        assert!(err.contains("'url' must be text"), "{err}");
    }

    #[test]
    fn non_text_secret_level_is_rejected() {
        let cfg = secret_config(
            &[("level", &[0xff])],
            &[("url", "memory"), ("namespace", "ns"), ("database", "db")],
        );
        let err = ConnectionKey::from_config(&cfg).unwrap_err().to_string();
        assert!(err.contains("'level' must be text"), "{err}");
    }

    #[tokio::test]
    async fn connect_error_never_quotes_a_secret_url() {
        let cfg = secret_config(
            &[("url", b"http://secret-host:notaport")],
            &[("namespace", "ns"), ("database", "db")],
        );
        let key = ConnectionKey::from_config(&cfg).unwrap();
        let err = format!("{:#}", connect(&key).await.expect_err("an invalid url"));
        assert!(!err.contains("secret-host"), "{err}");
        assert!(err.contains("url"), "{err}");
    }

    #[tokio::test]
    async fn connect_error_drops_plain_url_credentials() {
        let cfg = config(&[
            ("url", "http://admin:hunter2@db-host:notaport"),
            ("namespace", "ns"),
            ("database", "db"),
        ]);
        let key = ConnectionKey::from_config(&cfg).unwrap();
        let err = format!("{:#}", connect(&key).await.expect_err("an invalid url"));
        assert!(!err.contains("hunter2"), "{err}");
    }
}
