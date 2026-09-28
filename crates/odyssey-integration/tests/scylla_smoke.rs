//! Integration tests against a real Scylla container (testcontainers).
//!
//! Run with Docker available:
//!   cargo test -p odyssey-integration -- --ignored --nocapture

use std::time::Duration;

use odyssey_core::{ConsistencyLevel, SourceConfig};
use odyssey_cql::{connect_source, describe_cluster, discover_table};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn scylla_describe_and_schema_roundtrip() {
    let image = GenericImage::new("scylladb/scylla", "5.4")
        .with_exposed_port(9042.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Starting listening for CQL"))
        .with_cmd([
            "--smp",
            "1",
            "--memory",
            "750M",
            "--overprovisioned",
            "1",
            "--api-address",
            "0.0.0.0",
        ]);

    let container = image.start().await.expect("start scylla");
    let host = container.get_host().await.expect("host");
    let port = container
        .get_host_port_ipv4(9042)
        .await
        .expect("mapped 9042");
    let contact = format!("{host}:{port}");

    tokio::time::sleep(Duration::from_secs(8)).await;

    let source = SourceConfig {
        kind: "cql".into(),
        contact_points: vec![contact],
        datacenter: Some("datacenter1".into()),
        username: None,
        password: None,
        consistency: ConsistencyLevel::One,
        contact_points_only: true,
        address_translations: vec![],
    };

    let session = connect_source(&source).await.expect("connect scylla");
    let info = describe_cluster(&session).await.expect("describe");
    assert!(!info.cluster_name.is_empty());

    session
        .inner()
        .query_unpaged(
            "CREATE KEYSPACE IF NOT EXISTS odyssey_it WITH replication = \
             {'class': 'SimpleStrategy', 'replication_factor': 1}",
            &[],
        )
        .await
        .expect("create ks");
    session
        .inner()
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS odyssey_it.t (pk text PRIMARY KEY, v text)",
            &[],
        )
        .await
        .expect("create table");

    let schema = discover_table(&session, "odyssey_it", "t")
        .await
        .expect("discover");
    assert_eq!(schema.partition_key_columns().len(), 1);
}
