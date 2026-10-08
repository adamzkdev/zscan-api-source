//! The language model behind ZStreet's voices (chat, gossip, the daily paper, the mayor, character
//! memory): one OpenRouter call with the server's key. Callers always have a template to fall
//! back on, so every failure here is just `None`.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{json, Value};

const DEFAULT_MODEL: &str = "deepseek/deepseek-v4.1-flash";

/// The OpenRouter key, when talking is switched on.
pub fn key() -> Option<String> {
    std::env::var("OPENROUTER_API_KEY")
        .ok()
        .filter(|k| !k.is_empty())
}

/// One answer to `messages` (OpenAI chat format), trimmed; `None` on any failure.
pub async fn chat(
    http: &reqwest::Client,
    messages: Vec<Value>,
    max_tokens: u32,
    temperature: f32,
) -> Option<String> {
    let key = key()?;
    let model = std::env::var("STREET_CHAT_MODEL").unwrap_or_else(|_| DEFAULT_MODEL.into());
    let res = http
        .post("https://openrouter.ai/api/v1/chat/completions")
        .bearer_auth(key)
        .header("HTTP-Referer", "https://zstreet.cash")
        .header("X-Title", "ZStreet")
        .timeout(Duration::from_secs(45))
        .json(&json!({ "model": model, "messages": messages, "max_tokens": max_tokens, "temperature": temperature, "reasoning": { "enabled": false } }))
        .send()
        .await;
    let body: Value = match res {
        Ok(r) => r.json().await.unwrap_or_default(),
        Err(e) => {
            tracing::warn!("street llm: {e}");
            return None;
        }
    };
    let text = body["choices"][0]["message"]["content"]
        .as_str()
        .map(str::trim)
        .filter(|t| !t.is_empty());
    if text.is_none() {
        tracing::warn!("street llm: no answer: {}", body["error"]);
    }
    text.map(str::to_string)
}

/// Generated content (gossip, paper, decrees, memories) answered today (UTC), reset at midnight.
static DAY: AtomicU64 = AtomicU64::new(0);
static TODAY: AtomicU32 = AtomicU32::new(0);

fn under_budget() -> bool {
    let cap: u32 = std::env::var("STREET_LLM_DAILY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3000);
    let day = (chrono::Utc::now().timestamp() / 86_400) as u64;
    if DAY.swap(day, Ordering::Relaxed) != day {
        TODAY.store(0, Ordering::Relaxed);
    }
    TODAY.fetch_add(1, Ordering::Relaxed) < cap
}

/// A system prompt and one user message, within the daily budget for generated content.
pub async fn complete(
    http: &reqwest::Client,
    system: &str,
    user: &str,
    max_tokens: u32,
) -> Option<String> {
    if key().is_none() || !under_budget() {
        return None;
    }
    let messages = vec![
        json!({ "role": "system", "content": system }),
        json!({ "role": "user", "content": user }),
    ];
    chat(http, messages, max_tokens, 0.9).await
}

/// Like [`complete`], for prompts that ask for strict JSON: the first `{…}` object in the answer
/// (models like to wrap it in code fences or a sentence).
pub async fn complete_json(
    http: &reqwest::Client,
    system: &str,
    user: &str,
    max_tokens: u32,
) -> Option<Value> {
    let text = complete(http, system, user, max_tokens).await?;
    let v = parse_json(&text);
    if v.is_none() {
        tracing::warn!("street llm: not JSON: {}", clip(&text, 200));
    }
    v
}

pub fn parse_json(text: &str) -> Option<Value> {
    let start = text.find('{')?;
    let end = text.rfind('}')?;
    serde_json::from_str::<Value>(text.get(start..=end)?)
        .ok()
        .filter(Value::is_object)
}

pub fn clip(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

/// Shortens to at most `n` characters, at a word boundary when there is one.
pub fn fit(s: &str, n: usize) -> String {
    let s = s.split_whitespace().collect::<Vec<_>>().join(" ");
    if s.chars().count() <= n {
        return s;
    }
    let cut = clip(&s, n.saturating_sub(1));
    let cut = match cut.rfind(' ') {
        Some(i) if i > n / 2 => cut[..i].to_string(),
        _ => cut,
    };
    format!("{}…", cut.trim_end_matches([',', ';', ':', ' ', '.']))
}

/// Whether every number in `text` also appears in `facts`: the model may only repeat figures it
/// was given.
pub fn numbers_from(text: &str, facts: &str) -> bool {
    let mut ok = true;
    let mut cur = String::new();
    for c in text.chars().chain([' ']) {
        if c.is_ascii_digit() || (!cur.is_empty() && (c == '.' || c == ',')) {
            cur.push(c);
        } else if !cur.is_empty() {
            let n = cur.trim_end_matches(['.', ',']);
            ok &= facts.contains(n) || facts.contains(&n.replace(',', ""));
            cur.clear();
        }
    }
    ok
}

/// A string field of a model's JSON answer: trimmed, single-line, at most `n` characters, using
/// only numbers from `facts`.
pub fn field(v: &Value, n: usize, facts: &str) -> Option<String> {
    let s = v.as_str()?.trim().trim_matches('"');
    let s = fit(s, n);
    (!s.is_empty() && numbers_from(&s, facts)).then_some(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_must_come_from_facts() {
        let facts = "block 3101240 carried 12 transactions, fees 0.0012 ZEC";
        assert!(numbers_from("12 of us rode block 3,101,240!", facts));
        assert!(numbers_from("fees: 0.0012 ZEC.", facts));
        assert!(!numbers_from("13 of us rode", facts));
        assert!(numbers_from("no numbers here", facts));
    }

    #[test]
    fn json_inside_fences() {
        let v = parse_json("```json\n{\"a\": 1}\n```").unwrap();
        assert_eq!(v["a"], 1);
        assert!(parse_json("nope").is_none());
    }

    #[test]
    fn fit_cuts_at_words() {
        assert_eq!(fit("short", 10), "short");
        let f = fit("the quick brown fox jumps over", 16);
        assert!(f.chars().count() <= 16, "{f}");
        assert!(f.ends_with('…'));
    }
}
