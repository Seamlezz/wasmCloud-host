use tracing::Span;

use crate::config::ConnectionKey;

pub fn record_error(span: &Span, slug: &'static str, message: impl AsRef<str>) {
    span.record("error", true);
    span.record("exception.slug", slug);
    span.record("exception.message", message.as_ref());
}

pub fn record_connection_key(span: &Span, key: &ConnectionKey) {
    span.record("surrealdb.url", key.url_for_logging().as_str());
    span.record("surrealdb.namespace", key.namespace.as_str());
    span.record("surrealdb.database", key.database.as_str());
    span.record("surrealdb.credential.level", key.level.as_str());
    span.record("surrealdb.auth.configured", key.username.is_some());
}

pub fn record_connection_pool(span: &Span, reused: bool, size: usize) {
    span.record("surrealdb.connection.reused", reused);
    span.record("surrealdb.connection.pool.size", size);
}
