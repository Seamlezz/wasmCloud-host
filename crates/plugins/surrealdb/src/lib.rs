mod config;
mod host;
mod observability;
mod streams;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb_host_adapter::SubscriptionManager;
use tokio::sync::RwLock;
use tracing::{Span, instrument};
use wash_runtime::engine::ctx::{SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::WorkloadItem;
use wash_runtime::plugin::{HostPlugin, WitInterfaces, WorkloadTracker};
use wash_runtime::wit::{WitInterface, WitWorld};

pub use config::{ConnectionKey, CredentialLevel};

pub(crate) mod bindings {
    wasmtime::component::bindgen!({
        path: "wit",
        world: "surrealdb",
        imports: { default: async | store | trappable | tracing },
    });
}

pub const PLUGIN_SURREALDB_ID: &str = "wasmcloud-surrealdb";

type SurrealConnection = Arc<RwLock<Surreal<Any>>>;
type ConnectionPool = Arc<RwLock<HashMap<ConnectionKey, SurrealConnection>>>;

#[derive(Clone)]
pub(crate) struct ComponentBinding {
    pub connection: ConnectionKey,
    pub subscription_ids: HashSet<u64>,
}

impl ComponentBinding {
    fn new(connection: ConnectionKey) -> Self {
        Self {
            connection,
            subscription_ids: HashSet::new(),
        }
    }
}

#[derive(Clone)]
pub struct WasmcloudSurrealdb {
    pub(crate) connections: ConnectionPool,
    pub(crate) tracker: Arc<RwLock<WorkloadTracker<(), ComponentBinding>>>,
    pub(crate) subscription_manager: Arc<SubscriptionManager>,
}

impl WasmcloudSurrealdb {
    pub fn new() -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            tracker: Arc::new(RwLock::new(WorkloadTracker::default())),
            subscription_manager: Arc::new(SubscriptionManager::new()),
        }
    }

    pub async fn get_or_create_connection(
        &self,
        key: &ConnectionKey,
    ) -> anyhow::Result<Arc<RwLock<Surreal<Any>>>> {
        let mut connections = self.connections.write().await;
        if let Some(existing) = connections.get(key).cloned() {
            return Ok(existing);
        }

        let db = config::connect(key).await?;
        let wrapped = Arc::new(RwLock::new(db));
        connections.insert(key.clone(), Arc::clone(&wrapped));
        Ok(wrapped)
    }

    pub async fn track_subscription(&self, component_id: &str, subscription_id: u64) {
        if let Some(binding) = self
            .tracker
            .write()
            .await
            .get_component_data_mut(component_id)
        {
            binding.subscription_ids.insert(subscription_id);
        }
    }

    pub async fn untrack_subscription(&self, component_id: &str, subscription_id: u64) {
        if let Some(binding) = self
            .tracker
            .write()
            .await
            .get_component_data_mut(component_id)
        {
            binding.subscription_ids.remove(&subscription_id);
        }
    }

    async fn evict_unused_connections(&self) {
        let in_use: HashSet<ConnectionKey> = self
            .tracker
            .read()
            .await
            .workloads
            .values()
            .flat_map(|item| item.components.values().map(|b| b.connection.clone()))
            .collect();

        self.connections
            .write()
            .await
            .retain(|key, _| in_use.contains(key));
    }
}

impl Default for WasmcloudSurrealdb {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl HostPlugin for WasmcloudSurrealdb {
    fn id(&self) -> &'static str {
        PLUGIN_SURREALDB_ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            imports: HashSet::from([WitInterface::from("seamlezz:surrealdb/call@0.2.0")]),
            ..Default::default()
        }
    }

    #[instrument(
        skip_all,
        fields(
            main = true,
            workload_id = item.workload_id(),
            component_id = item.id(),
            plugin.id = PLUGIN_SURREALDB_ID,
            wasmcloud.interface = "seamlezz:surrealdb/call@0.2.0",
            db.system = "surrealdb",
            db.operation = "bind",
            surrealdb.interface.present = tracing::field::Empty,
            surrealdb.url = tracing::field::Empty,
            surrealdb.namespace = tracing::field::Empty,
            surrealdb.database = tracing::field::Empty,
            surrealdb.credential.level = tracing::field::Empty,
            surrealdb.auth.configured = tracing::field::Empty,
            surrealdb.connection.reused = tracing::field::Empty,
            surrealdb.connection.pool.size = tracing::field::Empty,
            error = tracing::field::Empty,
            exception.slug = tracing::field::Empty,
            exception.message = tracing::field::Empty,
        )
    )]
    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut WorkloadItem<'a>,
        interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        let span = Span::current();
        async {
            let Some(iface) = interfaces.get("seamlezz", "surrealdb", &[]) else {
                span.record("surrealdb.interface.present", false);
                return Ok(());
            };
            span.record("surrealdb.interface.present", true);

            let WorkloadItem::Component(component) = item else {
                return Ok(());
            };

            let key = ConnectionKey::from_config(&iface.config).inspect_err(|error| {
                observability::record_error(&span, "surrealdb-config-invalid", error.to_string());
            })?;
            observability::record_connection_key(&span, &key);

            let reused = self.connections.read().await.contains_key(&key);
            self.get_or_create_connection(&key)
                .await
                .inspect_err(|error| {
                    observability::record_error(
                        &span,
                        "surrealdb-connection-failed",
                        error.to_string(),
                    );
                })?;
            let pool_size = self.connections.read().await.len();
            observability::record_connection_pool(&span, reused, pool_size);

            self.tracker
                .write()
                .await
                .add_component(component, ComponentBinding::new(key));

            bindings::seamlezz::surrealdb::call::add_to_linker::<_, SharedCtx>(
                component.linker(),
                extract_active_ctx,
            )
            .inspect_err(|error| {
                observability::record_error(&span, "surrealdb-linker-failed", error.to_string());
            })?;

            Ok(())
        }
        .await
    }

    #[instrument(
        skip_all,
        fields(
            main = true,
            workload_id = %workload_id,
            plugin.id = PLUGIN_SURREALDB_ID,
            db.system = "surrealdb",
            db.operation = "unbind",
            surrealdb.subscriptions.cancelled = tracing::field::Empty,
            surrealdb.connections.evicted = tracing::field::Empty,
            error = tracing::field::Empty,
            exception.slug = tracing::field::Empty,
            exception.message = tracing::field::Empty,
        )
    )]
    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        let span = Span::current();
        let subscriptions_cancelled = self
            .tracker
            .read()
            .await
            .workloads
            .get(workload_id)
            .map(|item| {
                item.components
                    .values()
                    .map(|binding| binding.subscription_ids.len())
                    .sum::<usize>()
            })
            .unwrap_or_default();
        let connection_count = self.connections.read().await.len();
        let manager = Arc::clone(&self.subscription_manager);

        self.tracker
            .write()
            .await
            .remove_workload_with_cleanup(
                workload_id,
                |_| async {},
                move |binding: ComponentBinding| {
                    let manager = Arc::clone(&manager);
                    async move {
                        for id in binding.subscription_ids {
                            manager.cancel(id).await;
                        }
                    }
                },
            )
            .await;

        self.evict_unused_connections().await;
        let remaining_connection_count = self.connections.read().await.len();
        span.record("surrealdb.subscriptions.cancelled", subscriptions_cancelled);
        span.record(
            "surrealdb.connections.evicted",
            connection_count.saturating_sub(remaining_connection_count),
        );
        Ok(())
    }

    #[instrument(
        skip_all,
        fields(
            main = true,
            plugin.id = PLUGIN_SURREALDB_ID,
            db.system = "surrealdb",
            db.operation = "stop",
        )
    )]
    async fn stop(&self) -> anyhow::Result<()> {
        self.subscription_manager.shutdown().await;
        Ok(())
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use std::collections::HashSet;
    use wash_runtime::plugin::{HostPlugin, WitInterfaces, WorkloadTrackerItem};

    fn memory_connection_key() -> ConnectionKey {
        ConnectionKey::from_config(&std::collections::HashMap::from([
            ("url".to_string(), "memory".to_string()),
            ("namespace".to_string(), "test".to_string()),
            ("database".to_string(), "test".to_string()),
        ]))
        .expect("valid memory config")
    }

    #[tokio::test]
    async fn tracker_unbind_removes_workload_components() {
        let plugin = WasmcloudSurrealdb::new();
        let workload_id = "wl-abc";
        let component_id = "550e8400-e29b-41d4-a716-446655440000";
        let key = memory_connection_key();

        {
            let mut tracker = plugin.tracker.write().await;
            tracker
                .workloads
                .entry(workload_id.to_string())
                .or_insert_with(|| WorkloadTrackerItem {
                    workload_data: None,
                    components: HashMap::new(),
                })
                .components
                .insert(component_id.to_string(), ComponentBinding::new(key.clone()));
            tracker
                .components
                .insert(component_id.to_string(), workload_id.to_string());
        }

        let empty_interfaces = HashSet::new();
        plugin
            .on_workload_unbind(workload_id, WitInterfaces::new(&empty_interfaces))
            .await
            .expect("unbind should succeed");

        assert!(
            plugin
                .tracker
                .read()
                .await
                .get_component_data(component_id)
                .is_none()
        );
    }

    #[tokio::test]
    async fn evict_unused_connections_drops_unreferenced_pool_entry() {
        let plugin = WasmcloudSurrealdb::new();
        let key = memory_connection_key();

        plugin
            .get_or_create_connection(&key)
            .await
            .expect("failed to create connection");
        assert!(plugin.connections.read().await.contains_key(&key));

        plugin.evict_unused_connections().await;

        assert!(!plugin.connections.read().await.contains_key(&key));
    }

    #[tokio::test]
    async fn evict_unused_connections_keeps_referenced_pool_entry() {
        let plugin = WasmcloudSurrealdb::new();
        let workload_id = "wl-abc";
        let component_id = "550e8400-e29b-41d4-a716-446655440000";
        let key = memory_connection_key();

        plugin
            .get_or_create_connection(&key)
            .await
            .expect("failed to create connection");

        {
            let mut tracker = plugin.tracker.write().await;
            tracker
                .workloads
                .entry(workload_id.to_string())
                .or_insert_with(|| WorkloadTrackerItem {
                    workload_data: None,
                    components: HashMap::new(),
                })
                .components
                .insert(component_id.to_string(), ComponentBinding::new(key.clone()));
            tracker
                .components
                .insert(component_id.to_string(), workload_id.to_string());
        }

        plugin.evict_unused_connections().await;

        assert!(plugin.connections.read().await.contains_key(&key));
    }
}
