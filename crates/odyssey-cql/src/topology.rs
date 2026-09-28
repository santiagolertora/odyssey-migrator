//! Token-ring / vnode discovery for topology-aware planning.

use scylla::DeserializeRow;

use crate::{CqlError, CqlSession};
use odyssey_types::TokenRange;

#[derive(Debug, DeserializeRow)]
struct TokensRow {
    tokens: Option<std::collections::HashSet<String>>,
}

/// Discover sorted vnode boundary tokens from `system.local` + `system.peers`.
pub async fn discover_vnode_tokens(session: &CqlSession) -> Result<Vec<i64>, CqlError> {
    let mut tokens: Vec<i64> = Vec::new();
    collect_tokens(session, "SELECT tokens FROM system.local", &mut tokens).await?;
    // peers may be empty on single-node / contact_points_only demos.
    let _ = collect_tokens(session, "SELECT tokens FROM system.peers", &mut tokens).await;

    tokens.sort_unstable();
    tokens.dedup();
    if tokens.is_empty() {
        return Err(CqlError::Query(
            "no vnode tokens found in system.local/peers".into(),
        ));
    }
    Ok(tokens)
}

async fn collect_tokens(
    session: &CqlSession,
    cql: &str,
    out: &mut Vec<i64>,
) -> Result<(), CqlError> {
    let result = session
        .inner()
        .query_unpaged(cql, &[])
        .await
        .map_err(|err| CqlError::Query(err.to_string()))?;
    let rows = result
        .into_rows_result()
        .map_err(|err| CqlError::Query(err.to_string()))?;
    let iter = rows
        .rows::<TokensRow>()
        .map_err(|err| CqlError::Query(err.to_string()))?;
    for row in iter {
        let row = row.map_err(|err| CqlError::Query(err.to_string()))?;
        if let Some(set) = row.tokens {
            for raw in set {
                let t: i64 = raw.parse().map_err(|err| {
                    CqlError::Query(format!("parse vnode token `{raw}`: {err}"))
                })?;
                out.push(t);
            }
        }
    }
    Ok(())
}

/// Build contiguous parent ranges covering the Murmur3 ring from sorted vnode tokens.
///
/// Each token `t` owns `(prev, t]` (Cassandra/Scylla convention). The wrap-around
/// range from the last token to [`i64::MIN`]… first token is represented as two
/// half-ranges when needed via [`TokenRange::murmur3_full`] splitting helpers.
pub fn vnode_parent_ranges(sorted_tokens: &[i64]) -> Result<Vec<TokenRange>, CqlError> {
    if sorted_tokens.is_empty() {
        return Err(CqlError::Query("empty vnode token list".into()));
    }
    let mut ranges = Vec::with_capacity(sorted_tokens.len());
    let first = sorted_tokens[0];
    let last = *sorted_tokens.last().unwrap();

    // Wrap: (last, +inf] U (-inf, first] → two ranges when last != i64::MAX.
    if last < i64::MAX {
        ranges.push(
            TokenRange::new(last, i64::MAX)
                .map_err(|e| CqlError::Query(e.to_string()))?,
        );
    }
    if first > i64::MIN {
        ranges.push(
            TokenRange::new(i64::MIN, first)
                .map_err(|e| CqlError::Query(e.to_string()))?,
        );
    }

    for w in sorted_tokens.windows(2) {
        let (a, b) = (w[0], w[1]);
        if a < b {
            ranges.push(TokenRange::new(a, b).map_err(|e| CqlError::Query(e.to_string()))?);
        }
    }
    Ok(ranges)
}
