use axum::extract::{Query, State};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::error::ApiResult;
use crate::news;
use crate::AppState;

#[derive(Debug, Deserialize, IntoParams)]
pub struct NewsQuery {
    /// development, grants, community, releases or network (ZScan's own events).
    pub category: Option<String>,
    /// A source id from /v1/news/sources.
    pub source: Option<String>,
    /// Items published before this time (pagination: pass the last item's `published_at`).
    pub before: Option<DateTime<Utc>>,
    /// Default 50, max 200.
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct NewsItem {
    pub id: String,
    pub source: String,
    #[sqlx(skip)]
    pub source_name: String,
    pub category: String,
    pub title: String,
    /// The story itself: ZScan stores the title and a short excerpt only.
    pub url: String,
    pub summary: Option<String>,
    pub published_at: DateTime<Utc>,
    /// Network events: kind (record / spike), metric, day, value and its baseline.
    #[schema(value_type = Option<Object>)]
    pub data: Option<serde_json::Value>,
}

/// Zcash news, newest first: official blogs, the community forum, grants, ecosystem digests,
/// software releases, and network events ZScan detects in its own data (daily records and
/// activity spikes).
#[utoipa::path(get, path = "/v1/news", params(NewsQuery),
    responses((status = 200, body = [NewsItem])), tag = "news")]
pub async fn list(
    State(s): State<AppState>,
    Query(q): Query<NewsQuery>,
) -> ApiResult<Vec<NewsItem>> {
    let mut rows = sqlx::query_as::<_, NewsItem>(
        "SELECT id, source, category, title, url, summary, published_at, data FROM news_items
         WHERE ($1::text IS NULL OR category = $1) AND ($2::text IS NULL OR source = $2)
           AND ($3::timestamptz IS NULL OR published_at < $3)
           AND published_at <= now() + interval '1 hour'
         ORDER BY published_at DESC, id LIMIT $4",
    )
    .bind(q.category.as_deref())
    .bind(q.source.as_deref())
    .bind(q.before)
    .bind(q.limit.unwrap_or(50).clamp(1, 200))
    .fetch_all(&s.db.pool)
    .await?;
    for r in &mut rows {
        r.source_name = news::source_name(&r.source).to_string();
    }
    Ok(Json(rows))
}

#[derive(Debug, Serialize, ToSchema)]
pub struct NewsSource {
    pub id: String,
    pub name: String,
    pub category: String,
    pub homepage: String,
    pub feed: String,
    pub items: i64,
    pub latest: Option<DateTime<Utc>>,
}

/// The aggregated sources (labels/news_sources.json), with item counts.
#[utoipa::path(get, path = "/v1/news/sources",
    responses((status = 200, body = [NewsSource])), tag = "news")]
pub async fn sources(State(s): State<AppState>) -> ApiResult<Vec<NewsSource>> {
    let counts: Vec<(String, i64, Option<DateTime<Utc>>)> =
        sqlx::query_as("SELECT source, count(*), max(published_at) FROM news_items GROUP BY 1")
            .fetch_all(&s.db.pool)
            .await?;
    let get = |id: &str| {
        counts
            .iter()
            .find(|c| c.0 == id)
            .map_or((0, None), |c| (c.1, c.2))
    };
    let mut out: Vec<NewsSource> = news::sources()
        .iter()
        .map(|x| {
            let (items, latest) = get(&x.id);
            NewsSource {
                id: x.id.clone(),
                name: x.name.clone(),
                category: x.category.clone(),
                homepage: x.homepage.clone(),
                feed: x.feed.clone(),
                items,
                latest,
            }
        })
        .collect();
    let (items, latest) = get("zscan");
    out.push(NewsSource {
        id: "zscan".into(),
        name: "ZScan".into(),
        category: "network".into(),
        homepage: "https://stats.zscan.cash".into(),
        feed: "https://api.zscan.cash/v1/news?category=network".into(),
        items,
        latest,
    });
    Ok(Json(out))
}
