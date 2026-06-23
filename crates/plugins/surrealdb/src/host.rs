use std::sync::Arc;
use std::time::Instant;

use futures_util::StreamExt;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use tokio::sync::{RwLock, mpsc, oneshot};
use tracing::Span;
use wasmtime::component::{Accessor, StreamReader};

use surrealdb_host_adapter::{QueryError, SubscribeError, SubscriptionTask};
use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx};

use super::WasmcloudSurrealdb;
use super::config::ConnectionKey;
use super::streams::{LiveEventProducer, to_binding_live_event};
use super::{PLUGIN_SURREALDB_ID, bindings};

type BindingLiveEvent = bindings::seamlezz::surrealdb::call::LiveEvent;

impl bindings::seamlezz::surrealdb::call::Host for ActiveCtx<'_> {}

fn record_span_error(slug: &'static str) {
    Span::current().record("error", true);
    Span::current().record("exception.slug", slug);
}

fn record_duration(start: Instant) {
    Span::current().record("surrealdb.duration_ms", start.elapsed().as_millis() as u64);
}

fn record_connection_key(key: &ConnectionKey) {
    let span = Span::current();
    span.record("surrealdb.url", key.url_for_logging().as_str());
    span.record("surrealdb.namespace", key.namespace.as_str());
    span.record("surrealdb.database", key.database.as_str());
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
    if error.to_string().contains("not bound") {
        record_span_error("surrealdb-component-not-bound");
    } else {
        record_span_error("surrealdb-connection-failed");
    }
    wasmtime::Error::msg(error.to_string())
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
            record_span_error("surrealdb-param-decode");
            wasmtime::Error::msg(format!("param decode {key}: {source}"))
        }
        QueryError::QueryExecution(source) => {
            record_span_error("surrealdb-query-failed");
            wasmtime::Error::new(source)
        }
    }
}

fn map_subscribe_error(error: SubscribeError) -> wasmtime::Error {
    match error {
        SubscribeError::ParamDecode { key, source } => {
            record_span_error("surrealdb-param-decode");
            wasmtime::Error::msg(format!("param decode {key}: {source}"))
        }
        SubscribeError::QueryExecution(source) | SubscribeError::StreamOpen(source) => {
            record_span_error("surrealdb-subscribe-failed");
            wasmtime::Error::new(source)
        }
        SubscribeError::Serialize(source) => {
            record_span_error("surrealdb-subscribe-failed");
            wasmtime::Error::msg(source.to_string())
        }
    }
}

#[tracing::instrument(
    skip_all,
    fields(
        main = true,
        component_id = %component_id,
        db.system = "surrealdb",
        db.operation = "query",
        surrealdb.url = tracing::field::Empty,
        surrealdb.namespace = tracing::field::Empty,
        surrealdb.database = tracing::field::Empty,
        surrealdb.query.length = query.len(),
        surrealdb.params.count = params.len(),
        surrealdb.result.rows = tracing::field::Empty,
        surrealdb.duration_ms = tracing::field::Empty,
        error = tracing::field::Empty,
        exception.slug = tracing::field::Empty,
    )
)]
async fn execute_query(
    plugin: Arc<WasmcloudSurrealdb>,
    component_id: String,
    query: String,
    params: Vec<(String, Vec<u8>)>,
) -> wasmtime::Result<Vec<Result<Vec<u8>, String>>> {
    let start = Instant::now();
    let (key, db) = resolve_db(plugin, &component_id)
        .await
        .map_err(map_resolve_error)?;
    record_connection_key(&key);

    let guard = db.read().await;
    let result = surrealdb_host_adapter::query(&guard, query, params)
        .await
        .map_err(map_query_error);
    record_duration(start);
    if let Ok(rows) = &result {
        Span::current().record("surrealdb.result.rows", rows.len());
    }
    result
}

#[tracing::instrument(
    skip_all,
    fields(
        main = true,
        component_id = %component_id,
        db.system = "surrealdb",
        db.operation = "subscribe",
        surrealdb.url = tracing::field::Empty,
        surrealdb.namespace = tracing::field::Empty,
        surrealdb.database = tracing::field::Empty,
        surrealdb.query.length = query.len(),
        surrealdb.params.count = params.len(),
        surrealdb.subscription_id = tracing::field::Empty,
        surrealdb.duration_ms = tracing::field::Empty,
        error = tracing::field::Empty,
        exception.slug = tracing::field::Empty,
    )
)]
async fn execute_subscribe(
    plugin: Arc<WasmcloudSurrealdb>,
    query: String,
    params: Vec<(String, Vec<u8>)>,
    component_id: String,
) -> wasmtime::Result<(u64, mpsc::UnboundedReceiver<BindingLiveEvent>)> {
    let start = Instant::now();
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

    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                notification = stream.next() => {
                    let Some(Ok(notification)) = notification else {
                        break;
                    };

                    let Ok(event) = surrealdb_host_adapter::notification_to_live_event(
                        subscription_id,
                        notification,
                    ) else {
                        break;
                    };

                    if sender.send(to_binding_live_event(event)).is_err() {
                        break;
                    }
                }
            }
        }

        task_subscriptions.complete(subscription_id).await;
        track_plugin
            .untrack_subscription(&track_component_id, subscription_id)
            .await;
    });

    subscriptions
        .register(subscription_id, SubscriptionTask::new(stop_tx, handle))
        .await;

    plugin
        .track_subscription(&component_id, subscription_id)
        .await;

    record_duration(start);
    Ok((subscription_id, receiver))
}

#[tracing::instrument(
    skip_all,
    fields(
        main = true,
        component_id = %component_id,
        db.system = "surrealdb",
        db.operation = "cancel",
        surrealdb.subscription_id = subscription_id,
        surrealdb.duration_ms = tracing::field::Empty,
        error = tracing::field::Empty,
        exception.slug = tracing::field::Empty,
    )
)]
async fn execute_cancel(
    subscriptions: Arc<surrealdb_host_adapter::SubscriptionManager>,
    component_id: String,
    subscription_id: u64,
) -> wasmtime::Result<Result<(), String>> {
    let _ = component_id;
    let start = Instant::now();
    let result = if subscriptions.cancel(subscription_id).await {
        Ok(Ok(()))
    } else {
        record_span_error("surrealdb-subscription-not-found");
        Ok(Err(format!("subscription {subscription_id} not found")))
    };
    record_duration(start);
    result
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
