//! Inscription state survives a flush and reload (the indexer restarts from what's stored),
//! using real mainnet transactions. Needs `DATABASE_URL`, like the other integration tests.

use sqlx::PgPool;
use zscan_chain::{inscriptions::Engine, parse_tx, ParsedTx};
use zscan_db::Db;

fn tx(hex_str: &str) -> ParsedTx {
    parse_tx(&hex::decode(hex_str.trim()).unwrap()).unwrap()
}

const DEPLOY: &str = include_str!("../../chain/testdata/zrc20_deploy_3133112.hex");
const MINT: &str = include_str!("../../chain/testdata/zrc20_mint_3133115.hex");
const PNG_CHAIN: &str = include_str!("../../chain/testdata/inscription_png_chain_3495676.hex");

#[sqlx::test(migrations = "./migrations")]
async fn state_round_trips_through_postgres(pool: PgPool) {
    let db = Db { pool };
    let mut e = Engine::new();
    e.apply_tx(3_133_112, 1_763_012_657, 1, &tx(DEPLOY));
    e.apply_tx(3_133_115, 1_763_012_800, 1, &tx(MINT));
    // Half of a four-transaction reveal chain: waits on the second reveal's output 0.
    let png: Vec<ParsedTx> = PNG_CHAIN.lines().map(tx).collect();
    e.apply_tx(3_495_676, 1_790_000_000, 1, &png[0]);
    e.apply_tx(3_495_676, 1_790_000_000, 2, &png[1]);
    db.flush_inscriptions(&mut e, 3_495_676, 61_200)
        .await
        .unwrap();

    let mut r = db.load_inscription_engine().await.unwrap();
    assert_eq!(db.inscription_cursor().await.unwrap(), 3_495_676);
    assert_eq!(r.next_number, 3);
    assert_eq!(r.tokens, e.tokens);
    assert_eq!(r.balances, e.balances);
    assert_eq!(r.pending.len(), 1);
    assert_eq!(r.locations.len(), e.locations.len());

    // The reloaded engine finishes the chain.
    r.apply_tx(3_495_676, 1_790_000_000, 3, &png[2]);
    r.apply_tx(3_495_676, 1_790_000_000, 4, &png[3]);
    db.flush_inscriptions(&mut r, 3_495_677, 1_000)
        .await
        .unwrap();

    let (number, len, stored, sha): (i64, i32, Option<Vec<u8>>, String) = sqlx::query_as(
        "SELECT number, content_len, content, sha256 FROM inscriptions
         WHERE id = 'efd3caaf2caa4632a3251ed86842b21794f4c1dcf07cc1fbb0b0ad1a1675d725i0'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!((number, len), (3, 2936));
    // Above INSCRIPTION_CONTENT_MAX: rebuilt from the chain on request instead.
    assert!(stored.is_none());
    assert_eq!(
        sha,
        "18dfd6c1fbbc5405b3e674a490b31ac0c1bc574212be53617dc2c44add34fb3a"
    );
    let pending: i64 = sqlx::query_scalar("SELECT count(*) FROM inscription_pending")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(pending, 0);

    let (minted, holders): (String, i32) =
        sqlx::query_as("SELECT minted::text, holders FROM zrc20_tokens WHERE tick = 'zero'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(minted, format!("1000{}", "0".repeat(18)));
    assert_eq!(holders, 1);
}
