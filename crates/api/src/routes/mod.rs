pub mod address;
pub mod blocks;
pub mod fees;
pub mod labels;
pub mod network;
pub mod pools;
pub mod price;
pub mod protocols;
pub mod rankings;
pub mod search;
pub mod status;
pub mod supply;
pub mod tx;
pub mod upgrades;

use serde::Deserialize;
use utoipa::IntoParams;

#[derive(Debug, Deserialize, IntoParams)]
pub struct Page {
    /// Max items (default 25, max 100).
    pub limit: Option<i64>,
    /// Zero-based page number.
    pub page: Option<i64>,
}

impl Page {
    pub fn limit(&self) -> i64 {
        self.limit.unwrap_or(25).clamp(1, 100)
    }
    pub fn offset(&self) -> i64 {
        self.page.unwrap_or(0).clamp(0, 10_000) * self.limit()
    }
}

/// Current tip height known to the index.
pub async fn tip(db: &zscan_db::Db) -> anyhow::Result<Option<i32>> {
    Ok(sqlx::query_scalar("SELECT high FROM sync_state")
        .fetch_one(&db.pool)
        .await?)
}
