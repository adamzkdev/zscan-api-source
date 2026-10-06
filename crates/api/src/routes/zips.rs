use axum::extract::{Path, Query};
use axum::Json;
use serde::Deserialize;
use utoipa::IntoParams;

use crate::error::{ApiError, ApiResult};
use crate::zips::{self, Zip};

#[derive(Debug, Deserialize, IntoParams)]
pub struct ZipQuery {
    /// Draft, Proposed, Active, Final, Obsolete, Withdrawn, Rejected, Reserved.
    pub status: Option<String>,
    /// A category, e.g. Consensus, Standards, Wallet, Process, Informational.
    pub category: Option<String>,
}

/// Zcash Improvement Proposals from github.com/zcash/zips, by number (drafts without a number
/// last): title, status, categories, owners, a short summary from the Abstract, and the ZScan page
/// where the ZIP can be watched at work. Refreshed every six hours.
#[utoipa::path(get, path = "/v1/zips", params(ZipQuery),
    responses((status = 200, body = [Zip])), tag = "zips")]
pub async fn list(Query(q): Query<ZipQuery>) -> ApiResult<Vec<Zip>> {
    let eq = |a: &str, b: &str| a.eq_ignore_ascii_case(b);
    Ok(Json(
        zips::all()
            .iter()
            .filter(|z| q.status.as_deref().is_none_or(|s| eq(&z.status, s)))
            .filter(|z| {
                q.category
                    .as_deref()
                    .is_none_or(|c| z.categories.iter().any(|x| eq(x, c)))
            })
            .cloned()
            .collect(),
    ))
}

/// One ZIP, by number (`317`) or file name (`zip-0317`, `draft-arya-deploy-nu7`).
#[utoipa::path(get, path = "/v1/zips/{id}", params(("id" = String, Path, description = "Number or slug")),
    responses((status = 200, body = Zip), (status = 404)), tag = "zips")]
pub async fn detail(Path(id): Path<String>) -> ApiResult<Zip> {
    let n = id.parse::<u32>().ok();
    zips::all()
        .iter()
        .find(|z| z.slug == id || (n.is_some() && z.number == n))
        .cloned()
        .map(Json)
        .ok_or_else(|| ApiError::NotFound(format!("no ZIP {id}")))
}
