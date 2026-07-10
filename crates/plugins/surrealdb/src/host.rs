use std::sync::Arc;

use futures_util::StreamExt;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use tokio::sync::{RwLock, mpsc, oneshot};
use tracing::{Instrument, Span};
use wasmtime::component::{Accessor, StreamReader};

use surrealdb_host_adapter::{QueryError, SubscribeError, SubscriptionTask};
use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx};

use super::WasmcloudSurrealdb;
use super::config::ConnectionKey;
use super::observability;
use super::streams::{LiveEventProducer, to_binding_live_event};
use super::{PLUGIN_SURREALDB_ID, bindings};

type BindingLiveEvent = bindings::seamlezz::surrealdb::call::LiveEvent;

impl bindings::seamlezz::surrealdb::call::Host for ActiveCtx<'_> {}

fn record_span_error(slug: &'static str, message: impl AsRef<str>) {
    observability::record_error(&Span::current(), slug, message);
}

fn record_connection_key(key: &ConnectionKey) {
    observability::record_connection_key(&Span::current(), key);
}

fn record_query_result(rows: &[Result<Vec<u8>, String>]) {
    let span = Span::current();
    let failure_count = rows.iter().filter(|row| row.is_err()).count();
    span.record("surrealdb.result.rows", rows.len());
    span.record("surrealdb.query.failed", failure_count > 0);
    span.record("surrealdb.query.failure.count", failure_count);
    if let Some(message) = rows.iter().find_map(|row| row.as_ref().err()) {
        span.record("surrealdb.query.failure.message", message.as_str());
    }
}

async fn resolve_db(
    plugin: Arc<WasmcloudSurrealdb>,
    component_id: &str,
) -> anyhow::Result<(ConnectionKey, Arc<RwLock<Surreal<Any>>>)> {
    let key = plugin
        .tracker
        .read()
        .await
        .get_component_data(component_id)
        .map(|binding| binding.connection.clone())
        .ok_or_else(|| anyhow::anyhow!("component {component_id} not bound to surrealdb"))?;
    let db = plugin.get_or_create_connection(&key).await?;
    Ok((key, db))
}

fn map_resolve_error(error: anyhow::Error) -> wasmtime::Error {
    let message = error.to_string();
    if message.contains("not bound") {
        record_span_error("surrealdb-component-not-bound", &message);
    } else {
        record_span_error("surrealdb-connection-failed", &message);
    }
    wasmtime::Error::msg(message)
}

fn plugin_and_component_id(
    ctx: &ActiveCtx<'_>,
) -> anyhow::Result<(Arc<WasmcloudSurrealdb>, String)> {
    let plugin = ctx
        .try_get_plugin::<WasmcloudSurrealdb>(PLUGIN_SURREALDB_ID)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok((plugin, ctx.component_id.to_string()))
}

fn map_query_error(error: QueryError) -> wasmtime::Error {
    match error {
        QueryError::ParamDecode { key, source } => {
            let message = format!("param decode {key}: {source}");
            record_span_error("surrealdb-param-decode", &message);
            wasmtime::Error::msg(message)
        }
        QueryError::QueryExecution(source) => {
            record_span_error("surrealdb-query-failed", source.to_string());
            wasmtime::Error::new(source)
        }
    }
}

fn map_subscribe_error(error: SubscribeError) -> wasmtime::Error {
    match error {
        SubscribeError::ParamDecode { key, source } => {
            let message = format!("param decode {key}: {source}");
            record_span_error("surrealdb-param-decode", &message);
            wasmtime::Error::msg(message)
        }
        SubscribeError::QueryExecution(source) | SubscribeError::StreamOpen(source) => {
            record_span_error("surrealdb-subscribe-failed", source.to_string());
            wasmtime::Error::new(source)
        }
        SubscribeError::Serialize(source) => {
            let message = source.to_string();
            record_span_error("surrealdb-subscribe-failed", &message);
            wasmtime::Error::msg(message)
        }
    }
}

#[tracing::instrument(
    skip_all,
    fields(
        component_id = %component_id,
        plugin.id = PLUGIN_SURREALDB_ID,
        db.system = "surrealdb",
        db.operation = "query",
        surrealdb.url = tracing::field::Empty,
        surrealdb.namespace = tracing::field::Empty,
        surrealdb.database = tracing::field::Empty,
        surrealdb.credential.level = tracing::field::Empty,
        surrealdb.auth.configured = tracing::field::Empty,
        db.query.text = %query.trim(),
        surrealdb.query.length = query.len(),
        surrealdb.params.count = params.len(),
        surrealdb.result.rows = tracing::field::Empty,
        surrealdb.query.failed = tracing::field::Empty,
        surrealdb.query.failure.count = tracing::field::Empty,
        surrealdb.query.failure.message = tracing::field::Empty,
        error = tracing::field::Empty,
        exception.slug = tracing::field::Empty,
        exception.message = tracing::field::Empty,
    )
)]
async fn execute_query(
    plugin: Arc<WasmcloudSurrealdb>,
    component_id: String,
    query: String,
    params: Vec<(String, Vec<u8>)>,
) -> wasmtime::Result<Vec<Result<Vec<u8>, String>>> {
    let (key, db) = resolve_db(plugin, &component_id)
        .await
        .map_err(map_resolve_error)?;
    record_connection_key(&key);

    let guard = db.read().await;
    let result = surrealdb_host_adapter::query(&guard, query, params)
        .await
        .map_err(map_query_error);
    if let Ok(rows) = &result {
        record_query_result(rows);
    }
    result
}

#[tracing::instrument(
    skip_all,
    fields(
        component_id = %component_id,
        plugin.id = PLUGIN_SURREALDB_ID,
        db.system = "surrealdb",
        db.operation = "subscribe",
        surrealdb.url = tracing::field::Empty,
        surrealdb.namespace = tracing::field::Empty,
        surrealdb.database = tracing::field::Empty,
        surrealdb.credential.level = tracing::field::Empty,
        surrealdb.auth.configured = tracing::field::Empty,
        db.query.text = %query.trim(),
        surrealdb.query.length = query.len(),
        surrealdb.params.count = params.len(),
        surrealdb.subscription_id = tracing::field::Empty,
        error = tracing::field::Empty,
        exception.slug = tracing::field::Empty,
        exception.message = tracing::field::Empty,
    )
)]
async fn execute_subscribe(
    plugin: Arc<WasmcloudSurrealdb>,
    query: String,
    params: Vec<(String, Vec<u8>)>,
    component_id: String,
) -> wasmtime::Result<(u64, mpsc::UnboundedReceiver<BindingLiveEvent>)> {
    let subscriptions = Arc::clone(&plugin.subscription_manager);
    let (key, db) = resolve_db(Arc::clone(&plugin), &component_id)
        .await
        .map_err(map_resolve_error)?;
    record_connection_key(&key);

    let subscription_id = subscriptions.allocate_id();
    Span::current().record("surrealdb.subscription_id", subscription_id);

    let stream = {
        let guard = db.read().await;
        surrealdb_host_adapter::subscribe(&guard, query, params)
            .await
            .map_err(map_subscribe_error)?
    };

    let (sender, receiver) = mpsc::unbounded_channel();
    let (stop_tx, mut stop_rx) = oneshot::channel();
    let mut stream = Box::pin(stream);
    let task_subscriptions = Arc::clone(&subscriptions);
    let track_plugin = Arc::clone(&plugin);
    let track_component_id = component_id.clone();

    let stream_span = tracing::info_span!(
        "surrealdb.subscription.stream",
        component_id = %track_component_id,
        plugin.id = PLUGIN_SURREALDB_ID,
        db.system = "surrealdb",
        db.operation = "subscribe.stream",
        surrealdb.subscription_id = subscription_id,
        surrealdb.events.sent = tracing::field::Empty,
        surrealdb.stream.end_reason = tracing::field::Empty,
        error = tracing::field::Empty,
        exception.slug = tracing::field::Empty,
        exception.message = tracing::field::Empty,
    );

    let handle = tokio::spawn(
        async move {
            let span = Span::current();
            let mut events_sent = 0_u64;
            let end_reason;
            loop {
                tokio::select! {
                    _ = &mut stop_rx => {
                        end_reason = "cancelled";
                        break;
                    },
                    notification = stream.next() => {
                        let Some(notification) = notification else {
                            end_reason = "stream-ended";
                            break;
                        };

                        let notification = match notification {
                            Ok(notification) => notification,
                            Err(error) => {
                                observability::record_error(
                                    &span,
                                    "surrealdb-live-stream-failed",
                                    error.to_string(),
                                );
                                end_reason = "stream-error";
                                break;
                            }
                        };

                        let event = match surrealdb_host_adapter::notification_to_live_event(
                            subscription_id,
                            notification,
                        ) {
                            Ok(event) => event,
                            Err(error) => {
                                observability::record_error(
                                    &span,
                                    "surrealdb-live-event-conversion-failed",
                                    error.to_string(),
                                );
                                end_reason = "event-conversion-failed";
                                break;
                            }
                        };

                        if sender.send(to_binding_live_event(event)).is_err() {
                            end_reason = "receiver-dropped";
                            break;
                        }
                        events_sent += 1;
                    }
                }
            }

            span.record("surrealdb.events.sent", events_sent);
            span.record("surrealdb.stream.end_reason", end_reason);
            task_subscriptions.complete(subscription_id).await;
            track_plugin
                .untrack_subscription(&track_component_id, subscription_id)
                .await;
        }
        .instrument(stream_span),
    );

    subscriptions
        .register(subscription_id, SubscriptionTask::new(stop_tx, handle))
        .await;

    plugin
        .track_subscription(&component_id, subscription_id)
        .await;

    Ok((subscription_id, receiver))
}

#[tracing::instrument(
    skip_all,
    fields(
        component_id = %component_id,
        plugin.id = PLUGIN_SURREALDB_ID,
        db.system = "surrealdb",
        db.operation = "cancel",
        surrealdb.subscription_id = subscription_id,
        error = tracing::field::Empty,
        exception.slug = tracing::field::Empty,
        exception.message = tracing::field::Empty,
    )
)]
async fn execute_cancel(
    subscriptions: Arc<surrealdb_host_adapter::SubscriptionManager>,
    component_id: String,
    subscription_id: u64,
) -> wasmtime::Result<Result<(), String>> {
    let _ = component_id;
    if subscriptions.cancel(subscription_id).await {
        Ok(Ok(()))
    } else {
        let message = format!("subscription {subscription_id} not found");
        record_span_error("surrealdb-subscription-not-found", &message);
        Ok(Err(message))
    }
}

impl<T: Send + 'static> bindings::seamlezz::surrealdb::call::HostWithStore<T> for SharedCtx {
    async fn query(
        accessor: &Accessor<T, Self>,
        query: String,
        params: Vec<(String, Vec<u8>)>,
    ) -> wasmtime::Result<Vec<Result<Vec<u8>, String>>> {
        let (plugin, component_id) = accessor.with(|mut view| {
            plugin_and_component_id(&view.get()).map_err(|e| wasmtime::Error::msg(e.to_string()))
        })?;

        execute_query(plugin, component_id, query, params).await
    }

    async fn subscribe(
        accessor: &Accessor<T, Self>,
        query: String,
        params: Vec<(String, Vec<u8>)>,
    ) -> wasmtime::Result<(u64, StreamReader<BindingLiveEvent>)> {
        let (plugin, component_id) = accessor.with(|mut view| {
            plugin_and_component_id(&view.get()).map_err(|e| wasmtime::Error::msg(e.to_string()))
        })?;

        let (subscription_id, receiver) =
            execute_subscribe(plugin, query, params, component_id).await?;

        let reader =
            accessor.with(|store| StreamReader::new(store, LiveEventProducer::new(receiver)))?;

        Ok((subscription_id, reader))
    }

    async fn cancel(
        accessor: &Accessor<T, Self>,
        subscription_id: u64,
    ) -> wasmtime::Result<Result<(), String>> {
        let (subscriptions, component_id) = accessor.with(|mut view| {
            let (plugin, component_id) = plugin_and_component_id(&view.get())
                .map_err(|e| wasmtime::Error::msg(e.to_string()))?;
            Ok::<_, wasmtime::Error>((Arc::clone(&plugin.subscription_manager), component_id))
        })?;

        execute_cancel(subscriptions, component_id, subscription_id).await
    }
}
