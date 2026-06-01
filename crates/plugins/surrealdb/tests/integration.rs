#![allow(clippy::unwrap_used)]

use std::collections::HashMap;

use anyhow::Context as _;
use surrealdb_host_adapter::{query, subscribe};
use wasmcloud_plugin_surrealdb::{ConnectionKey, WasmcloudSurrealdb};

fn memory_connection_key() -> ConnectionKey {
    ConnectionKey::from_config(&HashMap::from([
        ("url".to_string(), "memory".to_string()),
        ("namespace".to_string(), "test".to_string()),
        ("database".to_string(), "test".to_string()),
    ]))
    .expect("valid memory config")
}

#[tokio::test]
async fn plugin_memory_query() {
    let plugin = WasmcloudSurrealdb::new();
    let key = memory_connection_key();
    let db = plugin
        .get_or_create_connection(&key)
        .await
        .context("failed to connect in-memory SurrealDB")
        .unwrap();

    let guard = db.read().await;

    query(
        &guard,
        "DEFINE TABLE person SCHEMALESS".to_string(),
        Vec::new(),
    )
    .await
    .context("define table")
    .unwrap();

    query(
        &guard,
        "CREATE person:demo CONTENT { name: 'demo', age: 42 }".to_string(),
        Vec::new(),
    )
    .await
    .context("create person")
    .unwrap();

    let results = query(&guard, "SELECT * FROM person".to_string(), Vec::new())
        .await
        .context("select person")
        .unwrap();

    let ok_rows = results.iter().filter(|r| r.is_ok()).count();
    assert!(ok_rows > 0, "expected at least one ok result row");
}

#[tokio::test]
async fn plugin_subscribe_stream_lifecycle() {
    use std::time::Duration;

    use futures_util::StreamExt;

    let plugin = WasmcloudSurrealdb::new();
    let key = memory_connection_key();
    let db = plugin
        .get_or_create_connection(&key)
        .await
        .context("failed to connect")
        .unwrap();

    let guard = db.read().await;
    query(&guard, "DEFINE TABLE person SCHEMALESS".into(), vec![])
        .await
        .context("define table")
        .unwrap();

    let stream = subscribe(&guard, "LIVE SELECT * FROM person".into(), vec![])
        .await
        .context("subscribe")
        .unwrap();
    let mut stream = Box::pin(stream);

    let first = tokio::time::timeout(Duration::from_secs(3), stream.next()).await;
    if let Ok(Some(Err(e))) = first {
        panic!("subscribe stream error: {e:?}");
    }
}
