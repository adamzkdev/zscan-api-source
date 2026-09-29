//! ZScan accounts live in Supabase (auth, profiles, watchlist, API keys, alerts, with row-level
//! security). The API and indexer reach them through PostgREST with the project's secret key,
//! which bypasses RLS: `SUPABASE_URL` and `SUPABASE_SECRET_KEY`.

use anyhow::{bail, Context, Result};
use serde::{de::DeserializeOwned, Serialize};

#[derive(Clone)]
pub struct Supabase {
    url: String,
    key: String,
    http: reqwest::Client,
}

impl Supabase {
    pub fn from_env(http: reqwest::Client) -> Option<Self> {
        let url = std::env::var("SUPABASE_URL")
            .ok()
            .filter(|v| !v.is_empty())?;
        let key = std::env::var("SUPABASE_SECRET_KEY")
            .ok()
            .filter(|v| !v.is_empty())?;
        Some(Self {
            url: url.trim_end_matches('/').to_string(),
            key,
            http,
        })
    }

    fn req(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.http
            .request(method, format!("{}/rest/v1/{path}", self.url))
            .header("apikey", &self.key)
            .timeout(std::time::Duration::from_secs(15))
    }

    async fn read<T: DeserializeOwned>(res: reqwest::Response, what: &str) -> Result<T> {
        let status = res.status();
        if !status.is_success() {
            bail!(
                "supabase {what} {status}: {}",
                res.text().await.unwrap_or_default()
            );
        }
        // Void functions answer 204 with no body.
        let body = res.text().await?;
        serde_json::from_str(if body.is_empty() { "null" } else { &body })
            .with_context(|| format!("supabase {what}"))
    }

    /// `GET /rest/v1/<table>?<query>`.
    pub async fn select<T: DeserializeOwned>(&self, table: &str, query: &str) -> Result<Vec<T>> {
        let res = self
            .req(reqwest::Method::GET, &format!("{table}?{query}"))
            .send()
            .await?;
        Self::read(res, table).await
    }

    /// Inserts rows, skipping those that clash with `on_conflict`; returns the rows inserted.
    pub async fn insert_new<T: Serialize, R: DeserializeOwned>(
        &self,
        table: &str,
        on_conflict: &str,
        rows: &[T],
    ) -> Result<Vec<R>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let res = self
            .req(
                reqwest::Method::POST,
                &format!("{table}?on_conflict={on_conflict}"),
            )
            .header(
                "Prefer",
                "resolution=ignore-duplicates,return=representation",
            )
            .json(rows)
            .send()
            .await?;
        Self::read(res, table).await
    }

    /// `PATCH /rest/v1/<table>?<filter>`.
    pub async fn update<T: Serialize>(&self, table: &str, filter: &str, patch: &T) -> Result<()> {
        let res = self
            .req(reqwest::Method::PATCH, &format!("{table}?{filter}"))
            .header("Prefer", "return=minimal")
            .json(patch)
            .send()
            .await?;
        if !res.status().is_success() {
            bail!(
                "supabase update {table} {}: {}",
                res.status(),
                res.text().await.unwrap_or_default()
            );
        }
        Ok(())
    }

    /// Calls a database function (`POST /rest/v1/rpc/<name>`).
    pub async fn rpc<A: Serialize, R: DeserializeOwned>(&self, name: &str, args: &A) -> Result<R> {
        let res = self
            .req(reqwest::Method::POST, &format!("rpc/{name}"))
            .json(args)
            .send()
            .await?;
        Self::read(res, name).await
    }
}
