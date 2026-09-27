//! Inscription state survives a flush and reload (the indexer restarts from what's stored),
//! using real mainnet transactions. Needs `DATABASE_URL`, like the other integration tests.

use sqlx::PgPool;
use zscan_chain::{inscriptions::Engine, parse_tx, ParsedTx};
use zscan_db::Db;

fn tx(hex_str: &str) -> ParsedTx {
    parse_tx(&hex::decode(hex_str.trim()).unwrap()).unwrap()
}

const ZRC20_DEPLOY: &str = include_str!("../../chain/testdata/zrc20_deploy_3133112.hex");
const ZGODS_MINT: &str = include_str!("../../chain/testdata/zrc721_mint_zgods.hex");
const PNG_CHAIN: &str = include_str!("../../chain/testdata/inscription_png_chain_3495676.hex");

#[sqlx::test(migrations = "./migrations")]
async fn state_round_trips_through_postgres(pool: PgPool) {
    let db = Db { pool };
    let mut e = Engine::new();
    // A token document takes number 1 without being stored.
    e.apply_tx(3_133_112, 1_763_012_657, 1, &tx(ZRC20_DEPLOY));
    e.apply_tx(3_143_217, 1_763_800_000, 1, &tx(ZGODS_MINT));
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
    assert_eq!(r.pending.len(), 1);
    assert_eq!(r.locations.len(), e.locations.len());

    // The reloaded engine finishes the chain.
    r.apply_tx(3_495_676, 1_790_000_000, 3, &png[2]);
    r.apply_tx(3_495_676, 1_790_000_000, 4, &png[3]);
    db.flush_inscriptions(&mut r, 3_495_677, 1_000)
        .await
        .unwrap();

    let rows: Vec<(i64, String, i32, bool, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT number, media, content_len, content IS NULL, collection, item
         FROM inscriptions ORDER BY number",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(
        rows,
        vec![
            (
                2,
                "collection".into(),
                60,
                false,
                Some("ZGODS".into()),
                Some("9963".into())
            ),
            // Above INSCRIPTION_CONTENT_MAX: rebuilt from the chain on request instead.
            (3, "image".into(), 2936, true, None, None),
        ]
    );
    let pending: i64 = sqlx::query_scalar("SELECT count(*) FROM inscription_pending")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(pending, 0);
}
