//! Watchlist alerts: after each block, every watched transparent address that received or sent
//! ZEC gets one alert per transaction, by email (Resend) and Telegram. The Telegram bot's
//! `/start <code>` links a chat to an account.

use std::time::Duration;

use anyhow::Result;
use tracing::{info, warn};
use zscan_chain::notify::{html_escape, Mailer, Telegram};
use zscan_db::{sqlx, Db};

/// Blocks re-scanned behind the cursor: inputs resolved late and small reorgs still alert, and
/// an alert is never sent twice (one per watched address and transaction).
const RESCAN: i64 = 10;

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

struct Pending {
    id: i64,
    address: String,
    label: Option<String>,
    txid: String,
    height: i32,
    direction: String,
    amount: i64,
    email: String,
    email_alerts: bool,
    chat: Option<i64>,
}

pub async fn run(db: Db) -> Result<()> {
    let mailer = Mailer::from_env();
    let telegram = Telegram::from_env();
    if mailer.is_none() && telegram.is_none() {
        info!("alerts off: set RESEND_API_KEY and/or TELEGRAM_BOT_TOKEN");
    }
    let explorer = std::env::var("EXPLORER_URL")
        .unwrap_or_else(|_| "https://zscan.cash".into())
        .trim_end_matches('/')
        .to_string();
    let console = std::env::var("CONSOLE_URL")
        .unwrap_or_else(|_| "https://console.zscan.cash".into())
        .trim_end_matches('/')
        .to_string();
    let http = zscan_chain::http_client_builder()
        .timeout(Duration::from_secs(20))
        .user_agent("zscan/0.1")
        .build()?;
    if let Some(tg) = telegram.clone() {
        tokio::spawn(bot(db.clone(), http.clone(), tg));
    }

    loop {
        if let Err(e) = scan(&db).await {
            warn!("alert scan: {e:#}");
        }
        if let Err(e) = deliver(
            &db,
            &http,
            mailer.as_ref(),
            telegram.as_ref(),
            &explorer,
            &console,
        )
        .await
        {
            warn!("alert delivery: {e:#}");
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

/// Records alerts for blocks since the cursor. The first run starts at the tip: no alerts for
/// history.
async fn scan(db: &Db) -> Result<()> {
    let Some(tip): Option<i32> = sqlx::query_scalar("SELECT max(height) FROM blocks")
        .fetch_one(&db.pool)
        .await?
    else {
        return Ok(());
    };
    let cursor: Option<i64> =
        sqlx::query_scalar("SELECT value FROM job_cursors WHERE job = 'alerts'")
            .fetch_optional(&db.pool)
            .await?;
    let from = match cursor {
        Some(c) => c - RESCAN,
        None => i64::from(tip),
    };
    let inserted = sqlx::query(
        "INSERT INTO alerts (watch_id, user_id, address, txid, height, direction, amount)
         SELECT w.id, w.user_id, w.address, x.txid, x.height,
                CASE WHEN x.delta >= 0 THEN 'in' ELSE 'out' END, x.delta
         FROM watchlist w
         JOIN LATERAL (
             SELECT m.txid, t.height, t.time, sum(m.delta)::bigint AS delta FROM (
                 SELECT txid, value AS delta FROM tx_outputs WHERE address = w.address
                 UNION ALL
                 SELECT txid, -value FROM tx_inputs WHERE address = w.address AND value IS NOT NULL
             ) m JOIN transactions t ON t.txid = m.txid
             WHERE t.height > $1 AND t.height <= $2
             GROUP BY m.txid, t.height, t.time
         ) x ON true
         WHERE x.delta <> 0 AND abs(x.delta) >= w.min_zat
           AND ((x.delta > 0 AND w.notify_in) OR (x.delta < 0 AND w.notify_out))
           -- Nothing from before the address was added (the re-scan window reaches back).
           AND x.time > w.created_at - interval '2 minutes'
         ON CONFLICT (watch_id, txid) DO NOTHING",
    )
    .bind(from as i32)
    .bind(tip)
    .execute(&db.pool)
    .await?
    .rows_affected();
    sqlx::query(
        "INSERT INTO job_cursors (job, value) VALUES ('alerts', $1)
         ON CONFLICT (job) DO UPDATE SET value = GREATEST(job_cursors.value, $1)",
    )
    .bind(i64::from(tip))
    .execute(&db.pool)
    .await?;
    if inserted > 0 {
        info!(inserted, tip, "watchlist alerts");
    }
    Ok(())
}

/// Sends alerts not delivered yet on each channel the user has on (one try per channel; a
/// failure is recorded and not retried, so a bad address never loops).
async fn deliver(
    db: &Db,
    http: &reqwest::Client,
    mailer: Option<&Mailer>,
    telegram: Option<&Telegram>,
    explorer: &str,
    console: &str,
) -> Result<()> {
    let rows: Vec<(
        i64,
        String,
        Option<String>,
        String,
        i32,
        String,
        i64,
        String,
        bool,
        Option<i64>,
    )> = sqlx::query_as(
        "SELECT a.id, a.address, w.label, a.txid, a.height, a.direction, a.amount,
                    u.email, u.email_alerts, u.telegram_chat_id
             FROM alerts a JOIN watchlist w ON w.id = a.watch_id JOIN users u ON u.id = a.user_id
             WHERE a.email_sent_at IS NULL AND a.telegram_sent_at IS NULL AND a.error IS NULL
               AND a.created_at > now() - interval '1 hour'
             ORDER BY a.id LIMIT 100",
    )
    .fetch_all(&db.pool)
    .await?;
    for r in rows {
        let p = Pending {
            id: r.0,
            address: r.1,
            label: r.2,
            txid: r.3,
            height: r.4,
            direction: r.5,
            amount: r.6,
            email: r.7,
            email_alerts: r.8,
            chat: r.9,
        };
        let name = p.label.clone().unwrap_or_else(|| short(&p.address));
        let verb = if p.direction == "in" {
            "received"
        } else {
            "sent"
        };
        let tx_url = format!("{explorer}/tx/{}", p.txid);
        let addr_url = format!("{explorer}/address/{}", p.address);
        let mut errors = Vec::new();
        let mut email_ok = false;
        let mut tg_ok = false;

        if let (Some(m), true) = (mailer, p.email_alerts) {
            let subject = format!("{name} {verb} {} ZEC", zec(p.amount));
            let text = format!(
                "{name} ({}) {verb} {} ZEC in block {}.\n\nTransaction: {tx_url}\nAddress: {addr_url}\n\nManage alerts: {console}/watchlist",
                p.address,
                zec(p.amount),
                p.height
            );
            let html = format!(
                r#"<div style="font-family:system-ui,sans-serif;max-width:520px;margin:auto;padding:24px;color:#121a2b">
<p style="font-size:18px;font-weight:600;margin:0 0 8px">{n} {v} {a} ZEC</p>
<p style="color:#5b6780;margin:0 0 16px;word-break:break-all">{addr} · block {h}</p>
<p><a href="{tx}" style="display:inline-block;background:#f4b728;color:#1a1405;padding:10px 18px;border-radius:8px;text-decoration:none;font-weight:600">View transaction</a></p>
<p style="color:#8a95aa;font-size:12px">You get this because the address is on your ZScan watchlist. <a href="{c}/watchlist" style="color:#0b6bcb">Manage alerts</a></p></div>"#,
                n = html_escape(&name),
                v = verb,
                a = zec(p.amount),
                addr = html_escape(&p.address),
                h = p.height,
                tx = html_escape(&tx_url),
                c = html_escape(console),
            );
            match m.send(http, &p.email, &subject, &text, &html).await {
                Ok(()) => email_ok = true,
                Err(e) => errors.push(format!("email: {e:#}")),
            }
        }
        if let (Some(tg), Some(chat)) = (telegram, p.chat) {
            let msg = format!(
                "{} <b>{}</b> {verb} <b>{} ZEC</b>\n<code>{}</code> · block {}\n<a href=\"{}\">View transaction</a>",
                if p.direction == "in" { "⬇️" } else { "⬆️" },
                html_escape(&name),
                zec(p.amount),
                html_escape(&p.address),
                p.height,
                html_escape(&tx_url)
            );
            match tg.send(http, chat, &msg).await {
                Ok(()) => tg_ok = true,
                Err(e) => errors.push(format!("telegram: {e:#}")),
            }
        }
        let error = if !email_ok && !tg_ok && errors.is_empty() {
            Some("no channel on".to_string())
        } else if errors.is_empty() {
            None
        } else {
            Some(errors.join("; "))
        };
        sqlx::query(
            "UPDATE alerts SET email_sent_at = CASE WHEN $2 THEN now() END,
                telegram_sent_at = CASE WHEN $3 THEN now() END, error = $4 WHERE id = $1",
        )
        .bind(p.id)
        .bind(email_ok)
        .bind(tg_ok)
        .bind(error)
        .execute(&db.pool)
        .await?;
    }
    Ok(())
}

/// The Telegram bot: `/start <code>` links the chat to the account that made the code, `/stop`
/// unlinks it.
async fn bot(db: Db, http: reqwest::Client, tg: Telegram) {
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
                match link(&db, code, chat, username.as_deref()).await {
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
                let _ = sqlx::query(
                    "UPDATE users SET telegram_chat_id = NULL, telegram_username = NULL WHERE telegram_chat_id = $1",
                )
                .bind(chat)
                .execute(&db.pool)
                .await;
                "Disconnected: no more ZScan alerts in this chat.".into()
            } else {
                "I send alerts for your ZScan watchlist. Connect this chat from the console: https://console.zscan.cash/settings".into()
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

async fn link(db: &Db, code: &str, chat: i64, username: Option<&str>) -> Result<Option<String>> {
    let user: Option<i64> = sqlx::query_scalar(
        "DELETE FROM telegram_links WHERE code = $1 AND expires_at > now() RETURNING user_id",
    )
    .bind(code)
    .fetch_optional(&db.pool)
    .await?;
    let Some(user) = user else { return Ok(None) };
    // One account per chat: a chat linked elsewhere moves here.
    sqlx::query("UPDATE users SET telegram_chat_id = NULL, telegram_username = NULL WHERE telegram_chat_id = $1")
        .bind(chat)
        .execute(&db.pool)
        .await?;
    let email: String = sqlx::query_scalar(
        "UPDATE users SET telegram_chat_id = $2, telegram_username = $3 WHERE id = $1 RETURNING email",
    )
    .bind(user)
    .bind(chat)
    .bind(username)
    .fetch_one(&db.pool)
    .await?;
    Ok(Some(email))
}
