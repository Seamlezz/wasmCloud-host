#![allow(clippy::unwrap_used)]

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use futures_util::StreamExt;
use surrealdb_host_adapter::{
    LiveAction, SubscribeError, notification_to_live_event, query, subscribe,
};
use wasmcloud_plugin_surrealdb::{ConnectionKey, WasmcloudSurrealdb, runtime_builder};

fn memory_connection_key() -> ConnectionKey {
    ConnectionKey::from_config(&wash_runtime::wit::Config::from([
        ("url", "memory"),
        ("namespace", "test"),
        ("database", "test"),
    ]))
    .expect("valid memory config")
}

/// Runs `test` on the runtime the host binary uses, so SurrealDB executes on
/// the worker threads a production host gives it.
fn on_host_runtime<F: Future<Output = ()>>(test: F) {
    runtime_builder()
        .build()
        .expect("host runtime")
        .block_on(test);
}

#[test]
fn host_runtime_runs_schema_definitions() {
    on_host_runtime(async {
        let plugin = WasmcloudSurrealdb::new();
        let db = plugin
            .get_or_create_connection(&memory_connection_key())
            .await
            .unwrap();

        let results = query(
            Arc::clone(&db),
            "DEFINE TABLE person SCHEMALESS".to_string(),
            Vec::new(),
        )
        .await
        .context("define table")
        .unwrap();

        assert!(results.iter().all(Result::is_ok), "{results:?}");
    });
}

#[test]
fn plugin_memory_query() {
    on_host_runtime(async {
        let plugin = WasmcloudSurrealdb::new();
        let db = plugin
            .get_or_create_connection(&memory_connection_key())
            .await
            .context("failed to connect in-memory SurrealDB")
            .unwrap();

        query(
            Arc::clone(&db),
            "DEFINE TABLE person SCHEMALESS".to_string(),
            Vec::new(),
        )
        .await
        .context("define table")
        .unwrap();

        query(
            Arc::clone(&db),
            "CREATE person:demo CONTENT { name: 'demo', age: 42 }".to_string(),
            Vec::new(),
        )
        .await
        .context("create person")
        .unwrap();

        let results = query(
            Arc::clone(&db),
            "SELECT * FROM person".to_string(),
            Vec::new(),
        )
        .await
        .context("select person")
        .unwrap();

        let ok_rows = results.iter().filter(|r| r.is_ok()).count();
        assert!(ok_rows > 0, "expected at least one ok result row");
    });
}

#[test]
fn live_select_reports_created_records() {
    on_host_runtime(async {
        let plugin = WasmcloudSurrealdb::new();
        let db = plugin
            .get_or_create_connection(&memory_connection_key())
            .await
            .context("failed to connect")
            .unwrap();

        query(
            Arc::clone(&db),
            "DEFINE TABLE person SCHEMALESS".into(),
            vec![],
        )
        .await
        .context("define table")
        .unwrap();

        let stream = subscribe(Arc::clone(&db), "LIVE SELECT * FROM person".into(), vec![])
            .await
            .context("subscribe")
            .unwrap();
        let mut stream = Box::pin(stream);

        query(
            Arc::clone(&db),
            "CREATE person:demo SET name = 'demo'".into(),
            vec![],
        )
        .await
        .context("create person")
        .unwrap();

        let notification = tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .expect("a live notification within 3 seconds")
            .expect("an open live stream")
            .expect("a live notification");
        let event = notification_to_live_event(7, notification).unwrap();
        assert_eq!(event.subscription_id, 7);
        assert!(matches!(event.action, LiveAction::Create));
    });
}

#[test]
fn live_select_on_undefined_table_fails() {
    on_host_runtime(async {
        let plugin = WasmcloudSurrealdb::new();
        let db = plugin
            .get_or_create_connection(&memory_connection_key())
            .await
            .unwrap();

        let result = subscribe(Arc::clone(&db), "LIVE SELECT * FROM missing".into(), vec![]).await;

        assert!(
            matches!(result, Err(SubscribeError::QueryExecution(_))),
            "expected a query execution error"
        );
    });
}
