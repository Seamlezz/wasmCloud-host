use std::sync::Arc;

use futures_util::StreamExt;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use tokio::sync::{RwLock, mpsc, oneshot};
use wasmtime::component::{Accessor, StreamReader};

use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx};
use surrealdb_host_adapter::{QueryError, SubscribeError, SubscriptionTask};

use super::WasmcloudSurrealdb;
use super::streams::{LiveEventProducer, to_binding_live_event};
use super::{PLUGIN_SURREALDB_ID, bindings};

type BindingLiveEvent = bindings::seamlezz::surrealdb::call::LiveEvent;

impl bindings::seamlezz::surrealdb::call::Host for ActiveCtx<'_> {}

async fn resolve_db(
    plugin: Arc<WasmcloudSurrealdb>,
    component_id: &str,
) -> anyhow::Result<Arc<RwLock<Surreal<Any>>>> {
    let key = plugin
        .tracker
        .read()
        .await
        .get_component_data(component_id)
        .map(|binding| binding.connection.clone())
        .ok_or_else(|| anyhow::anyhow!("component {component_id} not bound to surrealdb"))?;
    plugin.get_or_create_connection(&key).await
}

fn plugin_and_component_id(
    ctx: &ActiveCtx<'_>,
) -> anyhow::Result<(Arc<WasmcloudSurrealdb>, String)> {
    let plugin = ctx
        .get_plugin::<WasmcloudSurrealdb>(PLUGIN_SURREALDB_ID)
        .ok_or_else(|| anyhow::anyhow!("surrealdb plugin not registered"))?;
    Ok((plugin, ctx.component_id.to_string()))
}

fn map_query_error(error: QueryError) -> wasmtime::Error {
    match error {
        QueryError::ParamDecode { key, source } => {
            wasmtime::Error::msg(format!("param decode {key}: {source}"))
        }
        QueryError::QueryExecution(source) => wasmtime::Error::new(source),
    }
}

fn map_subscribe_error(error: SubscribeError) -> wasmtime::Error {
    match error {
        SubscribeError::ParamDecode { key, source } => {
            wasmtime::Error::msg(format!("param decode {key}: {source}"))
        }
        SubscribeError::QueryExecution(source) | SubscribeError::StreamOpen(source) => {
            wasmtime::Error::new(source)
        }
        SubscribeError::Serialize(source) => wasmtime::Error::msg(source.to_string()),
    }
}

impl bindings::seamlezz::surrealdb::call::HostWithStore for SharedCtx {
    async fn query<T: Send>(
        accessor: &Accessor<T, Self>,
        query: String,
        params: Vec<(String, Vec<u8>)>,
    ) -> wasmtime::Result<Vec<Result<Vec<u8>, String>>> {
        let (plugin, component_id) = accessor.with(|mut view| {
            plugin_and_component_id(&view.get())
                .map_err(|e| wasmtime::Error::msg(e.to_string()))
        })?;

        let db = resolve_db(plugin, &component_id)
            .await
            .map_err(|e| wasmtime::Error::msg(e.to_string()))?;

        let guard = db.read().await;
        surrealdb_host_adapter::query(&guard, query, params)
            .await
            .map_err(map_query_error)
    }

    async fn subscribe<T: Send>(
        accessor: &Accessor<T, Self>,
        query: String,
        params: Vec<(String, Vec<u8>)>,
    ) -> wasmtime::Result<(u64, StreamReader<BindingLiveEvent>)> {
        let (plugin, component_id, subscriptions) = accessor.with(|mut view| {
            let (plugin, component_id) = plugin_and_component_id(&view.get())
                .map_err(|e| wasmtime::Error::msg(e.to_string()))?;
            let subscriptions = Arc::clone(&plugin.subscription_manager);
            Ok::<_, wasmtime::Error>((plugin, component_id, subscriptions))
        })?;

        let db = resolve_db(Arc::clone(&plugin), &component_id)
            .await
            .map_err(|e| wasmtime::Error::msg(e.to_string()))?;

        let subscription_id = subscriptions.allocate_id();
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

        let reader =
            accessor.with(|store| StreamReader::new(store, LiveEventProducer::new(receiver)))?;

        Ok((subscription_id, reader))
    }

    async fn cancel<T: Send>(
        accessor: &Accessor<T, Self>,
        subscription_id: u64,
    ) -> wasmtime::Result<Result<(), String>> {
        let subscriptions = accessor.with(|mut view| {
            let (plugin, _) = plugin_and_component_id(&view.get())
                .map_err(|e| wasmtime::Error::msg(e.to_string()))?;
            Ok::<_, wasmtime::Error>(Arc::clone(&plugin.subscription_manager))
        })?;

        if subscriptions.cancel(subscription_id).await {
            Ok(Ok(()))
        } else {
            Ok(Err(format!("subscription {subscription_id} not found")))
        }
    }
}
