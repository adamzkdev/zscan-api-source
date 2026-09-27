pub mod address;
pub mod blocks;
pub mod bridges;
pub mod compact;
pub mod fees;
pub mod inscriptions;
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

/// Current tip height known to the index.
pub async fn tip(db: &zscan_db::Db) -> anyhow::Result<Option<i32>> {
    Ok(sqlx::query_scalar("SELECT high FROM sync_state")
        .fetch_one(&db.pool)
        .await?)
}
