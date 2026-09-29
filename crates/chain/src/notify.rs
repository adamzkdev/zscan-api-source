//! Outgoing notifications: email through Resend and messages through a Telegram bot.

use anyhow::{bail, Context, Result};
use serde::Deserialize;

/// Mail settings from the environment: `RESEND_API_KEY` and `MAIL_FROM`
/// (default `ZScan <noreply@zscan.cash>`; before the domain is verified at Resend, only
/// `onboarding@resend.dev` can send, and only to the Resend account's own address).
#[derive(Clone)]
pub struct Mailer {
    key: String,
    from: String,
}

impl Mailer {
    pub fn from_env() -> Option<Self> {
        let key = std::env::var("RESEND_API_KEY")
            .ok()
            .filter(|k| !k.is_empty())?;
        let from = std::env::var("MAIL_FROM")
            .ok()
            .filter(|f| !f.is_empty())
            .unwrap_or_else(|| "ZScan <noreply@zscan.cash>".into());
        Some(Self { key, from })
    }

    pub async fn send(
        &self,
        http: &reqwest::Client,
        to: &str,
        subject: &str,
        text: &str,
        html: &str,
    ) -> Result<()> {
        let res = http
            .post("https://api.resend.com/emails")
            .bearer_auth(&self.key)
            .json(&serde_json::json!({
                "from": self.from, "to": [to], "subject": subject, "text": text, "html": html,
            }))
            .send()
            .await
            .context("resend")?;
        if !res.status().is_success() {
            bail!(
                "resend {}: {}",
                res.status(),
                res.text().await.unwrap_or_default()
            );
        }
        Ok(())
    }
}

/// A Telegram bot, from `TELEGRAM_BOT_TOKEN`.
#[derive(Clone)]
pub struct Telegram {
    token: String,
}

#[derive(Debug, Deserialize)]
struct TgResponse<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TgUpdate {
    pub update_id: i64,
    pub message: Option<TgMessage>,
}

#[derive(Debug, Deserialize)]
pub struct TgMessage {
    pub chat: TgChat,
    pub text: Option<String>,
    pub from: Option<TgUser>,
}

#[derive(Debug, Deserialize)]
pub struct TgChat {
    pub id: i64,
}

#[derive(Debug, Deserialize)]
pub struct TgUser {
    pub username: Option<String>,
}

impl Telegram {
    pub fn from_env() -> Option<Self> {
        let token = std::env::var("TELEGRAM_BOT_TOKEN")
            .ok()
            .filter(|t| !t.is_empty())?;
        Some(Self { token })
    }

    async fn call<T: serde::de::DeserializeOwned>(
        &self,
        http: &reqwest::Client,
        method: &str,
        body: serde_json::Value,
        timeout: std::time::Duration,
    ) -> Result<T> {
        let r: TgResponse<T> = http
            .post(format!(
                "https://api.telegram.org/bot{}/{method}",
                self.token
            ))
            .json(&body)
            .timeout(timeout)
            .send()
            .await
            .with_context(|| format!("telegram {method}"))?
            .json()
            .await?;
        match (r.ok, r.result) {
            (true, Some(v)) => Ok(v),
            _ => bail!("telegram {method}: {}", r.description.unwrap_or_default()),
        }
    }

    /// Sends an HTML-formatted message (links allowed, previews off).
    pub async fn send(&self, http: &reqwest::Client, chat_id: i64, html: &str) -> Result<()> {
        let _: serde_json::Value = self
            .call(
                http,
                "sendMessage",
                serde_json::json!({
                    "chat_id": chat_id, "text": html, "parse_mode": "HTML",
                    "link_preview_options": {"is_disabled": true},
                }),
                std::time::Duration::from_secs(15),
            )
            .await?;
        Ok(())
    }

    /// Long-polls for new messages to the bot (up to 25 s).
    pub async fn updates(&self, http: &reqwest::Client, offset: i64) -> Result<Vec<TgUpdate>> {
        self.call(
            http,
            "getUpdates",
            serde_json::json!({"offset": offset, "timeout": 25, "allowed_updates": ["message"]}),
            std::time::Duration::from_secs(35),
        )
        .await
    }
}

/// Escapes text for Telegram's HTML parse mode and for email HTML.
pub fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
