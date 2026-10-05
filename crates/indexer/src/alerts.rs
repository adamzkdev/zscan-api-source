//! Watchlist alerts. Watchlists live in Supabase (the ZScan console); after each block, every
//! watched transparent address that received or sent ZEC gets one alert per transaction, stored
//! in Supabase and sent by email (Resend) and Telegram. The Telegram bot's `/start <code>` links a
//! chat to an account.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use zscan_chain::notify::{html_escape, Mailer, Telegram};
use zscan_chain::supabase::Supabase;
use zscan_db::{sqlx, Db};

/// Blocks re-scanned behind the cursor: inputs resolved late and small reorgs still alert, and
/// an alert is never stored twice (one per watched address and transaction).
const RESCAN: i64 = 10;

#[derive(Debug, Clone, Deserialize)]
struct Watch {
    id: i64,
    user_id: String,
    address: String,
    label: Option<String>,
    notify_in: bool,
    notify_out: bool,
    min_zat: i64,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct NewAlert<'a> {
    watch_id: i64,
    user_id: &'a str,
    address: &'a str,
    txid: String,
    height: i32,
    direction: &'static str,
    amount: i64,
}

#[derive(Debug, Deserialize)]
struct AlertRow {
    id: i64,
    watch_id: i64,
    user_id: String,
    address: String,
    txid: String,
    height: i32,
    direction: String,
    amount: i64,
}

#[derive(Debug, Deserialize)]
struct Profile {
    id: String,
    email: Option<String>,
    email_alerts: bool,
    telegram_chat_id: Option<i64>,
}

fn zec(zat: i64) -> String {
    let s = format!("{:.8}", zat.abs() as f64 / 1e8);
    s.trim_end_matches('0').trim_end_matches('.').to_string()
}

fn short(a: &str) -> String {
    if a.len() > 16 {
        format!("{}…{}", &a[..8], &a[a.len() - 6..])
    } else {
        a.to_string()
    }
}

struct Ctx {
    db: Db,
    sb: Supabase,
    http: reqwest::Client,
    mailer: Option<Mailer>,
    telegram: Option<Telegram>,
    explorer: String,
    console: String,
}

pub async fn run(db: Db) -> Result<()> {
    let http = zscan_chain::http_client_builder()
        .timeout(Duration::from_secs(20))
        .user_agent("zscan/0.1")
        .build()?;
    let Some(sb) = Supabase::from_env(http.clone()) else {
        info!("alerts off: set SUPABASE_URL and SUPABASE_SECRET_KEY");
        return Ok(());
    };
    let env = |k: &str, d: &str| {
        std::env::var(k)
            .unwrap_or_else(|_| d.into())
            .trim_end_matches('/')
            .to_string()
    };
    let ctx = Ctx {
        db,
        sb,
        http,
        mailer: Mailer::from_env(),
        telegram: Telegram::from_env(),
        explorer: env("EXPLORER_URL", "https://zscan.cash"),
        console: env("CONSOLE_URL", "https://console.zscan.cash"),
    };
    if ctx.mailer.is_none() && ctx.telegram.is_none() {
        info!("alert delivery off: set RESEND_API_KEY and/or TELEGRAM_BOT_TOKEN");
    }
    if let Some(tg) = ctx.telegram.clone() {
        tokio::spawn(bot(
            ctx.db.clone(),
            ctx.sb.clone(),
            ctx.http.clone(),
            tg,
            ctx.console.clone(),
        ));
    }
    loop {
        if let Err(e) = scan(&ctx).await {
            warn!("alert scan: {e:#}");
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

/// Stores alerts for blocks since the cursor and delivers the new ones. The first run starts at
/// the tip: no alerts for history.
async fn scan(ctx: &Ctx) -> Result<()> {
    let Some(tip): Option<i32> = sqlx::query_scalar("SELECT max(height) FROM blocks")
        .fetch_one(&ctx.db.pool)
        .await?
    else {
        return Ok(());
    };
    let cursor: Option<i64> =
        sqlx::query_scalar("SELECT value FROM job_cursors WHERE job = 'alerts'")
            .fetch_optional(&ctx.db.pool)
            .await?;
    // Nothing new since the last pass: skip the Supabase round-trip.
    if cursor == Some(i64::from(tip)) {
        return Ok(());
    }
    let from = cursor.map_or(i64::from(tip), |c| c - RESCAN);

    let watches: Vec<Watch> = ctx
        .sb
        .select(
            "watchlist",
            "select=id,user_id,address,label,notify_in,notify_out,min_zat,created_at&limit=100000",
        )
        .await?;
    // TEX addresses pay the ordinary P2PKH script, which is what the chain index holds.
    let mut by_address: HashMap<String, Vec<&Watch>> = HashMap::new();
    for w in &watches {
        let chain =
            zscan_chain::parse::tex_to_p2pkh(&w.address).unwrap_or_else(|| w.address.clone());
        by_address.entry(chain).or_default().push(w);
    }
    let mut new = Vec::new();
    if !by_address.is_empty() {
        let addresses: Vec<String> = by_address.keys().cloned().collect();
        let moves: Vec<(String, String, i32, DateTime<Utc>, i64)> = sqlx::query_as(
            "SELECT address, txid, height, time, received - sent FROM address_txs
             WHERE address = ANY($1) AND height > $2 AND height <= $3",
        )
        .bind(&addresses)
        .bind(from as i32)
        .bind(tip)
        .fetch_all(&ctx.db.pool)
        .await?;
        for (address, txid, height, time, delta) in moves {
            for w in by_address.get(&address).into_iter().flatten() {
                let wanted = (delta > 0 && w.notify_in) || (delta < 0 && w.notify_out);
                // Nothing from before the address was added (the re-scan reaches back).
                let before = time < w.created_at - chrono::Duration::minutes(2);
                if !wanted || before || delta.abs() < w.min_zat {
                    continue;
                }
                new.push(NewAlert {
                    watch_id: w.id,
                    user_id: &w.user_id,
                    address: &w.address,
                    txid: txid.clone(),
                    height,
                    direction: if delta > 0 { "in" } else { "out" },
                    amount: delta,
                });
            }
        }
    }
    let inserted: Vec<AlertRow> = ctx.sb.insert_new("alerts", "watch_id,txid", &new).await?;
    sqlx::query(
        "INSERT INTO job_cursors (job, value) VALUES ('alerts', $1)
         ON CONFLICT (job) DO UPDATE SET value = GREATEST(job_cursors.value, $1)",
    )
    .bind(i64::from(tip))
    .execute(&ctx.db.pool)
    .await?;
    if !inserted.is_empty() {
        info!(alerts = inserted.len(), tip, "watchlist alerts");
        let labels: HashMap<i64, Option<String>> =
            watches.iter().map(|w| (w.id, w.label.clone())).collect();
        deliver(ctx, &inserted, &labels).await?;
    }
    Ok(())
}

/// Sends each alert on the channels its owner has on (one try per channel) and records the
/// outcome on the alert.
async fn deliver(
    ctx: &Ctx,
    alerts: &[AlertRow],
    labels: &HashMap<i64, Option<String>>,
) -> Result<()> {
    let mut ids: Vec<&str> = alerts.iter().map(|a| a.user_id.as_str()).collect();
    ids.sort_unstable();
    ids.dedup();
    let profiles: Vec<Profile> = ctx
        .sb
        .select(
            "profiles",
            &format!(
                "select=id,email,email_alerts,telegram_chat_id&id=in.({})",
                ids.join(",")
            ),
        )
        .await?;
    let profiles: HashMap<&str, &Profile> = profiles.iter().map(|p| (p.id.as_str(), p)).collect();

    for a in alerts {
        let Some(p) = profiles.get(a.user_id.as_str()) else {
            continue;
        };
        let name = labels
            .get(&a.watch_id)
            .cloned()
            .flatten()
            .unwrap_or_else(|| short(&a.address));
        let verb = if a.direction == "in" {
            "received"
        } else {
            "sent"
        };
        let tx_url = format!("{}/tx/{}", ctx.explorer, a.txid);
        let mut errors = Vec::new();
        let (mut email_ok, mut tg_ok) = (false, false);

        if let (Some(m), true, Some(to)) = (&ctx.mailer, p.email_alerts, &p.email) {
            let subject = format!("{name} {verb} {} ZEC", zec(a.amount));
            let text = format!(
                "{name} ({}) {verb} {} ZEC in block {}.\n\nTransaction: {tx_url}\n\nManage alerts: {}/watchlist",
                a.address,
                zec(a.amount),
                a.height,
                ctx.console
            );
            let html = format!(
                r#"<div style="font-family:system-ui,sans-serif;max-width:520px;margin:auto;padding:24px;color:#121a2b">
<p style="font-size:18px;font-weight:600;margin:0 0 8px">{n} {v} {amt} ZEC</p>
<p style="color:#5b6780;margin:0 0 16px;word-break:break-all">{addr} · block {h}</p>
<p><a href="{tx}" style="display:inline-block;background:#f4b728;color:#1a1405;padding:10px 18px;border-radius:8px;text-decoration:none;font-weight:600">View transaction</a></p>
<p style="color:#8a95aa;font-size:12px">You get this because the address is on your ZScan watchlist. <a href="{c}/watchlist" style="color:#0b6bcb">Manage alerts</a></p></div>"#,
                n = html_escape(&name),
                v = verb,
                amt = zec(a.amount),
                addr = html_escape(&a.address),
                h = a.height,
                tx = html_escape(&tx_url),
                c = html_escape(&ctx.console),
            );
            match m.send(&ctx.http, to, &subject, &text, &html).await {
                Ok(()) => email_ok = true,
                Err(e) => errors.push(format!("email: {e:#}")),
            }
        }
        if let (Some(tg), Some(chat)) = (&ctx.telegram, p.telegram_chat_id) {
            let msg = format!(
                "{} <b>{}</b> {verb} <b>{} ZEC</b>\n<code>{}</code> · block {}\n<a href=\"{}\">View transaction</a>",
                if a.direction == "in" { "⬇️" } else { "⬆️" },
                html_escape(&name),
                zec(a.amount),
                html_escape(&a.address),
                a.height,
                html_escape(&tx_url)
            );
            match tg.send(&ctx.http, chat, &msg).await {
                Ok(()) => tg_ok = true,
                Err(e) => errors.push(format!("telegram: {e:#}")),
            }
        }
        let now = Utc::now();
        let error = match (email_ok || tg_ok, errors.is_empty()) {
            (false, true) => Some("no channel on".to_string()),
            (_, false) => Some(errors.join("; ")),
            (true, true) => None,
        };
        ctx.sb
            .update(
                "alerts",
                &format!("id=eq.{}", a.id),
                &serde_json::json!({
                    "email_sent_at": email_ok.then_some(now),
                    "telegram_sent_at": tg_ok.then_some(now),
                    "error": error,
                }),
            )
            .await?;
    }
    Ok(())
}

/// The Telegram bot: `/start <code>` links the chat to the account that made the code, `/stop`
/// unlinks it.
async fn bot(db: Db, sb: Supabase, http: reqwest::Client, tg: Telegram, console: String) {
    let mut offset: i64 =
        sqlx::query_scalar("SELECT value FROM job_cursors WHERE job = 'telegram'")
            .fetch_optional(&db.pool)
            .await
            .ok()
            .flatten()
            .unwrap_or(0);
    loop {
        let updates = match tg.updates(&http, offset).await {
            Ok(u) => u,
            Err(e) => {
                warn!("telegram updates: {e:#}");
                tokio::time::sleep(Duration::from_secs(10)).await;
                continue;
            }
        };
        for u in updates {
            offset = offset.max(u.update_id + 1);
            let Some(m) = u.message else { continue };
            let text = m.text.unwrap_or_default();
            let chat = m.chat.id;
            let username = m.from.and_then(|f| f.username);
            let reply = if let Some(code) = text.strip_prefix("/start ").map(str::trim) {
                let linked: Result<Option<String>> = sb
                    .rpc(
                        "telegram_link",
                        &serde_json::json!({"link_code": code, "chat": chat, "username": username}),
                    )
                    .await;
                match linked {
                    Ok(Some(email)) => format!(
                        "✅ Connected to the ZScan account <b>{}</b>. Alerts for your watchlist will arrive here.",
                        html_escape(&email)
                    ),
                    Ok(None) => "This link has expired. Open the ZScan console and press “Connect Telegram” again.".into(),
                    Err(e) => {
                        warn!("telegram link: {e:#}");
                        "Something went wrong, try again in a minute.".into()
                    }
                }
            } else if text.starts_with("/stop") {
                let _ = sb
                    .update(
                        "profiles",
                        &format!("telegram_chat_id=eq.{chat}"),
                        &serde_json::json!({"telegram_chat_id": null, "telegram_username": null}),
                    )
                    .await;
                "Disconnected: no more ZScan alerts in this chat.".into()
            } else {
                format!(
                    "I send alerts for your ZScan watchlist. Connect this chat from the console: {console}/settings"
                )
            };
            if let Err(e) = tg.send(&http, chat, &reply).await {
                warn!("telegram reply: {e:#}");
            }
        }
        let _ = sqlx::query(
            "INSERT INTO job_cursors (job, value) VALUES ('telegram', $1)
             ON CONFLICT (job) DO UPDATE SET value = $1",
        )
        .bind(offset)
        .execute(&db.pool)
        .await;
    }
}
