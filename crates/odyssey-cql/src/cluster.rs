use std::net::IpAddr;

use scylla::DeserializeRow;
use uuid::Uuid;

use crate::{CqlError, CqlSession};

/// High-level facts about a CQL cluster, read from `system.local`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterInfo {
    pub cluster_name: String,
    pub partitioner: String,
    pub data_center: String,
    pub rack: String,
    pub host_id: String,
    pub native_address: String,
}

#[derive(Debug, DeserializeRow)]
struct LocalRow {
    cluster_name: String,
    partitioner: String,
    data_center: String,
    rack: String,
    host_id: Uuid,
    #[scylla(rename = "rpc_address")]
    native_address: IpAddr,
}

/// Reads identifying metadata from the connected cluster.
pub async fn describe_cluster(session: &CqlSession) -> Result<ClusterInfo, CqlError> {
    tracing::debug!("querying system.local for cluster metadata");

    let result = session
        .inner()
        .query_unpaged(
            "SELECT cluster_name, partitioner, data_center, rack, host_id, rpc_address \
             FROM system.local",
            &[],
        )
        .await
        .map_err(|err| CqlError::Query(err.to_string()))?;

    let rows = result
        .into_rows_result()
        .map_err(|err| CqlError::Query(err.to_string()))?;

    let mut iter = rows
        .rows::<LocalRow>()
        .map_err(|err| CqlError::Query(err.to_string()))?;

    let row = iter
        .next()
        .transpose()
        .map_err(|err| CqlError::Query(err.to_string()))?
        .ok_or_else(|| CqlError::Query("system.local returned no rows".into()))?;

    let info = ClusterInfo {
        cluster_name: row.cluster_name,
        partitioner: row.partitioner,
        data_center: row.data_center,
        rack: row.rack,
        host_id: row.host_id.to_string(),
        native_address: row.native_address.to_string(),
    };

    tracing::info!(
        cluster_name = %info.cluster_name,
        partitioner = %info.partitioner,
        data_center = %info.data_center,
        "connected to CQL cluster"
    );

    Ok(info)
}
