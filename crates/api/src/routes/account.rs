//! Console endpoints (console.zscan.cash): sign-in by email link, the user's API keys, watchlist,
//! Telegram link and alert history. Everything but sign-in needs `Authorization: Bearer zss_…`.

use axum::{
    extract::{Path, Query, State},
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use zscan_chain::notify::html_escape;
use zscan_chain::parse::{address_kind, tex_to_p2pkh, AddressKind};

use crate::accounts::{hash, new_token, User, KEY_PREFIX, SESSION_PREFIX};
use crate::error::{ApiError, ApiResult};
use crate::AppState;

pub const MAX_KEYS: i64 = 5;
pub const MAX_WATCH: i64 = 50;
const SESSION_DAYS: i32 = 30;

#[derive(Serialize)]
pub struct Done {
    ok: bool,
}
const OK: Done = Done { ok: true };

fn valid_email(e: &str) -> bool {
    let Some((user, domain)) = e.split_once('@') else {
        return false;
    };
    !user.is_empty()
        && domain.contains('.')
        && !domain.starts_with('.')
        && !domain.ends_with('.')
        && e.len() <= 254
        && !e.chars().any(|c| c.is_whitespace() || c == '<' || c == '>')
}

#[derive(Deserialize)]
pub struct LoginRequest {
    email: String,
}

/// Emails a one-time sign-in link. Answers the same whether or not the account exists.
pub async fn login(State(s): State<AppState>, Json(req): Json<LoginRequest>) -> ApiResult<Done> {
    let email = req.email.trim().to_lowercase();
    if !valid_email(&email) {
        return Err(ApiError::BadRequest(
            "invalid_email",
            "Enter a valid email address".into(),
        ));
    }
    let Some(mailer) = &s.mailer else {
        return Err(ApiError::BadRequest(
            "mail_disabled",
            "Sign-in by email is not configured".into(),
        ));
    };
    let recent: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM login_tokens WHERE email = $1 AND created_at > now() - interval '1 hour'",
    )
    .bind(&email)
    .fetch_one(&s.db.pool)
    .await?;
    if recent >= 5 {
        return Err(ApiError::TooManyRequests(
            "Too many sign-in emails for this address, try again in an hour".into(),
        ));
    }
    let token = new_token("");
    sqlx::query(
        "INSERT INTO login_tokens (token_hash, email, expires_at) VALUES ($1, $2, now() + interval '15 minutes')",
    )
    .bind(hash(&token))
    .bind(&email)
    .execute(&s.db.pool)
    .await?;
    let link = format!("{}/auth/verify?token={token}", s.console_url);
    let text = format!(
        "Sign in to ZScan: {link}\n\nThe link works once, for 15 minutes. If you didn't ask for it, ignore this email."
    );
    let html = format!(
        r#"<div style="font-family:system-ui,sans-serif;max-width:480px;margin:auto;padding:24px;color:#121a2b">
<p style="font-size:18px;font-weight:600;margin:0 0 16px">Sign in to ZScan</p>
<p><a href="{l}" style="display:inline-block;background:#f4b728;color:#1a1405;padding:10px 18px;border-radius:8px;text-decoration:none;font-weight:600">Sign in</a></p>
<p style="color:#5b6780;font-size:13px">The link works once, for 15 minutes. If you didn't ask for it, ignore this email.</p>
<p style="color:#8a95aa;font-size:12px;word-break:break-all">{l}</p></div>"#,
        l = html_escape(&link)
    );
    mailer
        .send(&s.http, &email, "Your ZScan sign-in link", &text, &html)
        .await
        .map_err(|e| {
            tracing::warn!("login email: {e:#}");
            ApiError::BadRequest(
                "mail_failed",
                "Could not send the email, try again later".into(),
            )
        })?;
    Ok(Json(OK))
}

#[derive(Deserialize)]
pub struct VerifyRequest {
    token: String,
}

#[derive(Serialize)]
pub struct SessionResponse {
    session: String,
    expires_at: DateTime<Utc>,
    email: String,
}

/// Trades a sign-in link for a session (creating the account on first sign-in).
pub async fn verify(
    State(s): State<AppState>,
    Json(req): Json<VerifyRequest>,
) -> ApiResult<SessionResponse> {
    let mut tx = s.db.pool.begin().await?;
    let email: Option<String> = sqlx::query_scalar(
        "UPDATE login_tokens SET used_at = now()
         WHERE token_hash = $1 AND used_at IS NULL AND expires_at > now() RETURNING email",
    )
    .bind(hash(req.token.trim()))
    .fetch_optional(&mut *tx)
    .await?;
    let email = email.ok_or_else(|| {
        ApiError::BadRequest(
            "invalid_link",
            "This sign-in link is invalid or has expired".into(),
        )
    })?;
    let user_id: i64 = sqlx::query_scalar(
        "INSERT INTO users (email, last_login_at) VALUES ($1, now())
         ON CONFLICT (email) DO UPDATE SET last_login_at = now() RETURNING id",
    )
    .bind(&email)
    .fetch_one(&mut *tx)
    .await?;
    let session = new_token(SESSION_PREFIX);
    let expires_at: DateTime<Utc> = sqlx::query_scalar(
        "INSERT INTO sessions (token_hash, user_id, expires_at)
         VALUES ($1, $2, now() + make_interval(days => $3)) RETURNING expires_at",
    )
    .bind(hash(&session))
    .bind(user_id)
    .bind(SESSION_DAYS)
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query("DELETE FROM login_tokens WHERE expires_at < now() - interval '1 day'")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Json(SessionResponse {
        session,
        expires_at,
        email,
    }))
}

pub async fn logout(
    State(s): State<AppState>,
    headers: axum::http::HeaderMap,
    _u: User,
) -> ApiResult<Done> {
    if let Some(t) = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    {
        sqlx::query("DELETE FROM sessions WHERE token_hash = $1")
            .bind(hash(t.trim()))
            .execute(&s.db.pool)
            .await?;
    }
    Ok(Json(OK))
}

#[derive(Serialize)]
pub struct Me {
    email: String,
    created_at: DateTime<Utc>,
    email_alerts: bool,
    telegram_connected: bool,
    telegram_username: Option<String>,
    /// The bot users talk to, when Telegram alerts are set up on this server.
    telegram_bot: Option<String>,
    watchlist: i64,
    keys: i64,
    alerts_30d: i64,
    max_watchlist: i64,
    max_keys: i64,
}

pub async fn me(State(s): State<AppState>, u: User) -> ApiResult<Me> {
    let (created_at, email_alerts, chat, username): (DateTime<Utc>, bool, Option<i64>, Option<String>) =
        sqlx::query_as(
            "SELECT created_at, email_alerts, telegram_chat_id, telegram_username FROM users WHERE id = $1",
        )
        .bind(u.id)
        .fetch_one(&s.db.pool)
        .await?;
    let (watchlist, keys, alerts_30d): (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM watchlist WHERE user_id = $1),
                (SELECT count(*) FROM api_keys WHERE user_id = $1 AND revoked_at IS NULL),
                (SELECT count(*) FROM alerts WHERE user_id = $1 AND created_at > now() - interval '30 days')",
    )
    .bind(u.id)
    .fetch_one(&s.db.pool)
    .await?;
    Ok(Json(Me {
        email: u.email,
        created_at,
        email_alerts,
        telegram_connected: chat.is_some(),
        telegram_username: username,
        telegram_bot: s.telegram_bot.clone(),
        watchlist,
        keys,
        alerts_30d,
        max_watchlist: MAX_WATCH,
        max_keys: MAX_KEYS,
    }))
}

#[derive(Deserialize)]
pub struct MePatch {
    email_alerts: Option<bool>,
}

pub async fn update_me(
    State(s): State<AppState>,
    u: User,
    Json(p): Json<MePatch>,
) -> ApiResult<Done> {
    if let Some(v) = p.email_alerts {
        sqlx::query("UPDATE users SET email_alerts = $2 WHERE id = $1")
            .bind(u.id)
            .bind(v)
            .execute(&s.db.pool)
            .await?;
    }
    Ok(Json(OK))
}

/// Deletes the account with its keys, watchlist and alerts.
pub async fn delete_me(State(s): State<AppState>, u: User) -> ApiResult<Done> {
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(u.id)
        .execute(&s.db.pool)
        .await?;
    Ok(Json(OK))
}

/* ---------------------------------------------------------------- API keys */

#[derive(Serialize, sqlx::FromRow)]
pub struct ApiKey {
    id: i64,
    name: String,
    prefix: String,
    created_at: DateTime<Utc>,
    last_used_at: Option<DateTime<Utc>>,
    requests: i64,
}

pub async fn keys(State(s): State<AppState>, u: User) -> ApiResult<Vec<ApiKey>> {
    Ok(Json(
        sqlx::query_as(
            "SELECT id, name, prefix, created_at, last_used_at, requests FROM api_keys
             WHERE user_id = $1 AND revoked_at IS NULL ORDER BY created_at",
        )
        .bind(u.id)
        .fetch_all(&s.db.pool)
        .await?,
    ))
}

#[derive(Deserialize)]
pub struct NewKey {
    name: String,
}

#[derive(Serialize)]
pub struct CreatedKey {
    /// Shown once: only its hash is stored.
    key: String,
    #[serde(flatten)]
    info: ApiKey,
}

pub async fn create_key(
    State(s): State<AppState>,
    u: User,
    Json(req): Json<NewKey>,
) -> ApiResult<CreatedKey> {
    let name = req.name.trim();
    let name = if name.is_empty() { "Default" } else { name };
    if name.chars().count() > 60 {
        return Err(ApiError::BadRequest(
            "invalid_name",
            "Name is too long".into(),
        ));
    }
    let active: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM api_keys WHERE user_id = $1 AND revoked_at IS NULL",
    )
    .bind(u.id)
    .fetch_one(&s.db.pool)
    .await?;
    if active >= MAX_KEYS {
        return Err(ApiError::BadRequest(
            "key_limit",
            format!("Up to {MAX_KEYS} keys: revoke one first"),
        ));
    }
    let key = new_token(KEY_PREFIX);
    let prefix: String = key.chars().take(KEY_PREFIX.len() + 6).collect();
    let info: ApiKey = sqlx::query_as(
        "INSERT INTO api_keys (user_id, name, prefix, key_hash) VALUES ($1, $2, $3, $4)
         RETURNING id, name, prefix, created_at, last_used_at, requests",
    )
    .bind(u.id)
    .bind(name)
    .bind(&prefix)
    .bind(hash(&key))
    .fetch_one(&s.db.pool)
    .await?;
    Ok(Json(CreatedKey { key, info }))
}

pub async fn revoke_key(
    State(s): State<AppState>,
    u: User,
    Path(id): Path<i64>,
) -> ApiResult<Done> {
    let n = sqlx::query(
        "UPDATE api_keys SET revoked_at = now() WHERE id = $1 AND user_id = $2 AND revoked_at IS NULL",
    )
    .bind(id)
    .bind(u.id)
    .execute(&s.db.pool)
    .await?
    .rows_affected();
    if n == 0 {
        return Err(ApiError::NotFound("No such key".into()));
    }
    Ok(Json(OK))
}

/* --------------------------------------------------------------- Watchlist */

#[derive(Serialize, sqlx::FromRow)]
pub struct Watch {
    id: i64,
    address: String,
    label: Option<String>,
    notify_in: bool,
    notify_out: bool,
    min_zat: i64,
    created_at: DateTime<Utc>,
    /// Latest alert for this address.
    last_alert_at: Option<DateTime<Utc>>,
}

pub async fn watchlist(State(s): State<AppState>, u: User) -> ApiResult<Vec<Watch>> {
    Ok(Json(
        sqlx::query_as(
            "SELECT w.id, w.address, w.label, w.notify_in, w.notify_out, w.min_zat, w.created_at,
                    (SELECT max(created_at) FROM alerts a WHERE a.watch_id = w.id) AS last_alert_at
             FROM watchlist w WHERE w.user_id = $1 ORDER BY w.created_at",
        )
        .bind(u.id)
        .fetch_all(&s.db.pool)
        .await?,
    ))
}

#[derive(Deserialize)]
pub struct WatchInput {
    address: Option<String>,
    label: Option<String>,
    notify_in: Option<bool>,
    notify_out: Option<bool>,
    /// Minimum movement that alerts, in ZEC.
    min_zec: Option<f64>,
}

fn min_zat(min_zec: Option<f64>) -> Result<Option<i64>, ApiError> {
    match min_zec {
        None => Ok(None),
        Some(z) if z.is_finite() && (0.0..=21_000_000.0).contains(&z) => {
            Ok(Some((z * 1e8).round() as i64))
        }
        Some(_) => Err(ApiError::BadRequest(
            "invalid_amount",
            "Invalid minimum amount".into(),
        )),
    }
}

fn clean_label(l: Option<String>) -> Result<Option<String>, ApiError> {
    let l = l.map(|l| l.trim().to_string()).filter(|l| !l.is_empty());
    if l.as_ref().is_some_and(|l| l.chars().count() > 60) {
        return Err(ApiError::BadRequest(
            "invalid_label",
            "Label is too long".into(),
        ));
    }
    Ok(l)
}

pub async fn add_watch(
    State(s): State<AppState>,
    u: User,
    Json(w): Json<WatchInput>,
) -> ApiResult<Watch> {
    let raw = w.address.unwrap_or_default().trim().to_string();
    let address =
        match address_kind(&raw) {
            Some(AddressKind::Transparent) => tex_to_p2pkh(&raw).unwrap_or(raw),
            Some(AddressKind::Shielded) => return Err(ApiError::BadRequest(
                "shielded_address",
                "Shielded addresses are private: their payments can't be watched from the chain"
                    .into(),
            )),
            None => {
                return Err(ApiError::BadRequest(
                    "invalid_address",
                    "Not a valid Zcash address".into(),
                ))
            }
        };
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM watchlist WHERE user_id = $1")
        .bind(u.id)
        .fetch_one(&s.db.pool)
        .await?;
    if count >= MAX_WATCH {
        return Err(ApiError::BadRequest(
            "watch_limit",
            format!("Up to {MAX_WATCH} addresses per account"),
        ));
    }
    let row: Option<Watch> = sqlx::query_as(
        "INSERT INTO watchlist (user_id, address, label, notify_in, notify_out, min_zat)
         VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (user_id, address) DO NOTHING
         RETURNING id, address, label, notify_in, notify_out, min_zat, created_at, NULL::timestamptz AS last_alert_at",
    )
    .bind(u.id)
    .bind(&address)
    .bind(clean_label(w.label)?)
    .bind(w.notify_in.unwrap_or(true))
    .bind(w.notify_out.unwrap_or(true))
    .bind(min_zat(w.min_zec)?.unwrap_or(0))
    .fetch_optional(&s.db.pool)
    .await?;
    row.map(Json).ok_or_else(|| {
        ApiError::BadRequest(
            "already_watched",
            "This address is already on your watchlist".into(),
        )
    })
}

pub async fn update_watch(
    State(s): State<AppState>,
    u: User,
    Path(id): Path<i64>,
    Json(w): Json<WatchInput>,
) -> ApiResult<Done> {
    let label_set = w.label.is_some();
    let n = sqlx::query(
        "UPDATE watchlist SET
            label = CASE WHEN $3 THEN $4 ELSE label END,
            notify_in = COALESCE($5, notify_in),
            notify_out = COALESCE($6, notify_out),
            min_zat = COALESCE($7, min_zat)
         WHERE id = $1 AND user_id = $2",
    )
    .bind(id)
    .bind(u.id)
    .bind(label_set)
    .bind(clean_label(w.label)?)
    .bind(w.notify_in)
    .bind(w.notify_out)
    .bind(min_zat(w.min_zec)?)
    .execute(&s.db.pool)
    .await?
    .rows_affected();
    if n == 0 {
        return Err(ApiError::NotFound(
            "No such address on your watchlist".into(),
        ));
    }
    Ok(Json(OK))
}

pub async fn remove_watch(
    State(s): State<AppState>,
    u: User,
    Path(id): Path<i64>,
) -> ApiResult<Done> {
    sqlx::query("DELETE FROM watchlist WHERE id = $1 AND user_id = $2")
        .bind(id)
        .bind(u.id)
        .execute(&s.db.pool)
        .await?;
    Ok(Json(OK))
}

/* ------------------------------------------------------------ Telegram, alerts */

#[derive(Serialize)]
pub struct TelegramLink {
    /// Opens the bot with a one-time code; pressing Start links this chat to the account.
    url: String,
}

pub async fn telegram_link(State(s): State<AppState>, u: User) -> ApiResult<TelegramLink> {
    let bot = s.telegram_bot.as_deref().ok_or_else(|| {
        ApiError::BadRequest(
            "telegram_disabled",
            "Telegram alerts are not set up yet".into(),
        )
    })?;
    // Telegram start parameters allow [A-Za-z0-9_-], up to 64 characters.
    let code = new_token("").chars().take(32).collect::<String>();
    sqlx::query("DELETE FROM telegram_links WHERE user_id = $1 OR expires_at < now()")
        .bind(u.id)
        .execute(&s.db.pool)
        .await?;
    sqlx::query(
        "INSERT INTO telegram_links (code, user_id, expires_at) VALUES ($1, $2, now() + interval '30 minutes')",
    )
    .bind(&code)
    .bind(u.id)
    .execute(&s.db.pool)
    .await?;
    Ok(Json(TelegramLink {
        url: format!("https://t.me/{bot}?start={code}"),
    }))
}

pub async fn telegram_unlink(State(s): State<AppState>, u: User) -> ApiResult<Done> {
    sqlx::query("UPDATE users SET telegram_chat_id = NULL, telegram_username = NULL WHERE id = $1")
        .bind(u.id)
        .execute(&s.db.pool)
        .await?;
    Ok(Json(OK))
}

#[derive(Serialize, sqlx::FromRow)]
pub struct Alert {
    id: i64,
    address: String,
    label: Option<String>,
    txid: String,
    height: i32,
    direction: String,
    amount: i64,
    created_at: DateTime<Utc>,
    email_sent_at: Option<DateTime<Utc>>,
    telegram_sent_at: Option<DateTime<Utc>>,
    error: Option<String>,
}

#[derive(Deserialize)]
pub struct AlertsQuery {
    limit: Option<i64>,
}

pub async fn alerts(
    State(s): State<AppState>,
    u: User,
    Query(q): Query<AlertsQuery>,
) -> ApiResult<Vec<Alert>> {
    Ok(Json(
        sqlx::query_as(
            "SELECT a.id, a.address, w.label, a.txid, a.height, a.direction, a.amount, a.created_at,
                    a.email_sent_at, a.telegram_sent_at, a.error
             FROM alerts a JOIN watchlist w ON w.id = a.watch_id
             WHERE a.user_id = $1 ORDER BY a.created_at DESC, a.id DESC LIMIT $2",
        )
        .bind(u.id)
        .bind(q.limit.unwrap_or(50).clamp(1, 200))
        .fetch_all(&s.db.pool)
        .await?,
    ))
}

#[cfg(test)]
mod tests {
    #[test]
    fn emails() {
        assert!(super::valid_email("a@b.co"));
        assert!(!super::valid_email("a@b"));
        assert!(!super::valid_email("a b@c.io"));
        assert!(!super::valid_email("@c.io"));
    }
}
