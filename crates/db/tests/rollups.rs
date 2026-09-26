//! Integration tests against a real Postgres. `#[sqlx::test]` creates a throwaway database per
//! test on the server in `DATABASE_URL` (required) and applies the migrations. Locally:
//! `DATABASE_URL=postgres://zscan:…@127.0.0.1:5433/zscan cargo test -p zscan-db`.

use sqlx::PgPool;
use zscan_db::Db;

/// Inserts a block at `height`, `day`s after 2026-09-01, adding `d_orchard` zatoshis to Orchard
/// and issuing a fixed subsidy.
async fn block(pool: &PgPool, height: i32, day: i32, d_orchard: i64) {
    sqlx::query(
        "INSERT INTO blocks (height, hash, prev_hash, time, tx_count, size, fees, d_transparent,
                             d_orchard, orchard_tree_size, subsidy, funding_paid, deferred, burned)
         VALUES ($1, 'h' || $1, 'h' || ($1 - 1), '2026-09-01'::timestamptz + make_interval(days => $2, secs => $1),
                 1, 1000, 10, 0, $3, $1, 156250000, 12500000, 18750000, 0)",
    )
    .bind(height)
    .bind(day)
    .bind(d_orchard)
    .execute(pool)
    .await
    .unwrap();
}

async fn snapshot(pool: &PgPool, height: i32, orchard: i64, lockbox: i64, supply: i64) {
    sqlx::query(
        "INSERT INTO pool_snapshots (height, transparent, sprout, sapling, orchard, ironwood, lockbox, supply)
         VALUES ($1, 0, 0, 0, $2, 0, $3, $4)",
    )
    .bind(height)
    .bind(orchard)
    .bind(lockbox)
    .bind(supply)
    .execute(pool)
    .await
    .unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn daily_rollup_matches_blocks(pool: PgPool) {
    let db = Db { pool: pool.clone() };
    for h in 1..=4 {
        block(&pool, h, if h <= 2 { 0 } else { 1 }, 100).await;
    }
    db.refresh_daily(None).await.unwrap();
    let rows: Vec<(i32, i32, i64, i64, i64, Option<i64>)> = sqlx::query_as(
        "SELECT blocks, econ_blocks, issued, fees, d_orchard, orchard_tree_size FROM daily_blocks ORDER BY day",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        rows,
        vec![
            (2, 2, 312_500_000, 20, 200, Some(2)),
            (2, 2, 312_500_000, 20, 200, Some(4)),
        ]
    );

    // A partial refresh drops days whose blocks were rolled back.
    sqlx::query("DELETE FROM blocks WHERE height > 2")
        .execute(&pool)
        .await
        .unwrap();
    let day1: chrono::DateTime<chrono::Utc> = "2026-09-02T00:00:00Z".parse().unwrap();
    db.refresh_daily(Some(day1)).await.unwrap();
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM daily_blocks")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(n, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn audit_passes_and_catches_mismatch(pool: PgPool) {
    let db = Db { pool: pool.clone() };
    for h in 1..=4 {
        block(&pool, h, 0, 100).await;
    }
    sqlx::query("UPDATE sync_state SET low = 1, high = 4")
        .execute(&pool)
        .await
        .unwrap();
    let net = 156_250_000; // subsidy - burned per block
    snapshot(&pool, 1, 100, 18_750_000, net).await;
    snapshot(&pool, 3, 300, 3 * 18_750_000, 3 * net).await; // consistent with blocks 2..=3
    snapshot(&pool, 4, 999, 4 * 18_750_000, 4 * net).await; // node claims 599 extra in Orchard
    assert_eq!(db.run_audits().await.unwrap(), 2);

    let checks: Vec<(i32, bool, i64)> =
        sqlx::query_as("SELECT height, ok, diff_orchard FROM audit_checks ORDER BY height")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(checks, vec![(3, true, 0), (4, false, 599)]);
    // Already-audited ranges are not re-checked.
    assert_eq!(db.run_audits().await.unwrap(), 0);
}
