//! Share cards for ZStreet: `GET /v1/street/card/tx/{txid}.png`, the 1200×630 picture X,
//! Discord and Telegram show for a `zstreet.cash/tx/<txid>` link. Drawn on the street's own
//! night shot with the ZScan fonts (embedded, OFL, see `assets/fonts`), from what the chain
//! makes public: kind, pools, the transparent-side amount, fee and block.
//!
//! Rendering costs ~tens of ms of one core, so it runs on the blocking pool, two at a time, and
//! the PNGs are kept in a small LRU: a mined transaction's card never changes, a mempool one is
//! redrawn after a short while (it may have been mined since).

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use ab_glyph::{Font, FontRef, PxScale, ScaleFont};
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::header,
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Utc};
use image::{imageops, RgbImage, RgbaImage};
use lru::LruCache;
use tokio::sync::Semaphore;

use crate::error::ApiError;
use crate::routes::tx::TxDetail;
use crate::AppState;

const W: u32 = 1200;
const H: u32 = 630;

static ROBOTO_BOLD: &[u8] = include_bytes!("../assets/fonts/Roboto-Bold.ttf");
static ROBOTO_MEDIUM: &[u8] = include_bytes!("../assets/fonts/Roboto-Medium.ttf");
static MONO: &[u8] = include_bytes!("../assets/fonts/JetBrainsMono-Medium.ttf");
static BACKDROP: &[u8] = include_bytes!("../assets/street/og.jpg");
static MARK: &[u8] = include_bytes!("../assets/street/mark.png");

const GOLD: [u8; 3] = [0xf4, 0xb7, 0x28];
const INK: [u8; 3] = [0xf2, 0xf2, 0xf2];
const DIM: [u8; 3] = [0xa3, 0xa3, 0xa3];
const FAINT: [u8; 3] = [0x7a, 0x7a, 0x7a];
const GREEN: [u8; 3] = [0x1b, 0xaf, 0x7a];

/// Mined cards kept in memory (each ~0.3–0.6 MB).
const KEEP: usize = 48;
/// How long a mempool card is served before it is drawn again.
const MEMPOOL_TTL: Duration = Duration::from_secs(30);
/// Below this many confirmations a card is not yet treated as final (reorgs).
const FINAL_CONFS: i64 = 10;

/// What a card shows, independent of where it came from (tests build it by hand).
#[derive(Debug, Clone)]
pub struct Card {
    pub txid: String,
    pub kind: String,
    pub height: Option<i64>,
    pub time: Option<DateTime<Utc>>,
    pub fee: Option<i64>,
    pub transparent_in: i64,
    pub transparent_out: i64,
    pub t_inputs: usize,
    pub t_outputs: usize,
    /// Value balances: sprout, sapling, orchard, ironwood (positive = leaves the pool).
    pub vb: [i64; 4],
    /// Whether each shielded pool has any spend, output or action in the transaction.
    pub active: [bool; 4],
}

const POOLS: [&str; 4] = ["SPROUT", "SAPLING", "ORCHARD", "IRONWOOD"];

impl Card {
    pub fn from_detail(d: &TxDetail) -> Self {
        let vb = &d.value_balances;
        Card {
            txid: d.txid.clone(),
            kind: d.kind.clone(),
            height: d.height,
            time: d.time,
            fee: d.fee,
            transparent_in: d.transparent_in,
            transparent_out: d.transparent_out,
            t_inputs: d.inputs.iter().filter(|i| i.coinbase.is_none()).count(),
            t_outputs: d.outputs.iter().filter(|o| o.value > 0).count(),
            vb: [vb.sprout, vb.sapling, vb.orchard, vb.ironwood],
            active: [
                d.joinsplits > 0,
                !d.sapling_spends.is_empty() || !d.sapling_outputs.is_empty(),
                !d.orchard_actions.is_empty(),
                !d.ironwood_actions.is_empty(),
            ],
        }
    }

    fn title(&self) -> &'static str {
        match self.kind.as_str() {
            "coinbase" => "Coinbase",
            "transparent" => "Transparent",
            "shielding" => "Shielding",
            "deshielding" => "Deshielding",
            "shielded" => "Fully shielded",
            "migration" => "Pool migration",
            _ => "Mixed",
        }
    }

    /// Where value comes from and goes to: `TRANSPARENT → ORCHARD`.
    fn flow(&self) -> (Vec<&'static str>, Vec<&'static str>) {
        let mut from = Vec::new();
        let mut to = Vec::new();
        if self.kind == "coinbase" {
            from.push("NEW COINS");
        } else if self.t_inputs > 0 {
            from.push("TRANSPARENT");
        }
        for (p, name) in POOLS.iter().enumerate() {
            if self.active[p] && self.vb[p] >= 0 {
                from.push(name);
            }
        }
        if self.t_outputs > 0 {
            to.push("TRANSPARENT");
        }
        for (p, name) in POOLS.iter().enumerate() {
            if self.active[p] && self.vb[p] < 0 {
                to.push(name);
            }
        }
        // Value that stays inside a pool (an internal shielded transfer, change notes).
        if to.is_empty() {
            to = from.iter().copied().filter(|n| *n != "NEW COINS").collect();
        }
        if from.is_empty() {
            from = to.clone();
        }
        (from, to)
    }

    /// The amount the chain shows in the clear, with its label; `None` when it is private.
    fn amount(&self) -> (&'static str, Option<i64>) {
        let into: i64 = self.vb.iter().filter(|v| **v < 0).map(|v| -v).sum();
        let out_of: i64 = self.vb.iter().filter(|v| **v > 0).sum();
        match self.kind.as_str() {
            "shielded" => ("AMOUNT", None),
            "shielding" => ("INTO THE POOL", Some(into)),
            "deshielding" => {
                let seen = self.transparent_out - self.transparent_in;
                (
                    "OUT OF THE POOL",
                    Some(if seen > 0 { seen } else { out_of }),
                )
            }
            "migration" => ("MIGRATED", Some(into)),
            "coinbase" => (
                "NEW COINS",
                Some(if self.transparent_out > 0 {
                    self.transparent_out
                } else {
                    into
                }),
            ),
            _ => ("AMOUNT", Some(self.transparent_out)),
        }
    }
}

fn group(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// Zatoshis as ZEC for a headline: grouped thousands, fewer decimals as the amount grows.
fn zec(zat: i64) -> String {
    let zat = zat.unsigned_abs();
    let whole = zat / 100_000_000;
    let frac = zat % 100_000_000;
    let digits = match whole {
        0 => 8,
        1..=999 => 4,
        _ => 2,
    };
    let frac = format!("{frac:08}");
    let frac = frac[..digits].trim_end_matches('0');
    if frac.is_empty() {
        group(whole)
    } else {
        format!("{}.{frac}", group(whole))
    }
}

struct Assets {
    bold: FontRef<'static>,
    medium: FontRef<'static>,
    mono: FontRef<'static>,
    base: RgbImage,
    mark: RgbaImage,
}

fn assets() -> &'static Assets {
    static A: OnceLock<Assets> = OnceLock::new();
    A.get_or_init(|| {
        let font = |b: &'static [u8]| FontRef::try_from_slice(b).expect("embedded font");
        let mut base = image::load_from_memory(BACKDROP)
            .expect("embedded backdrop")
            .to_rgb8();
        if base.dimensions() != (W, H) {
            base = imageops::resize(&base, W, H, imageops::FilterType::Triangle);
        }
        // Softened a touch: the rain noise is what makes a photo PNG heavy.
        let mut base = imageops::blur(&base, 1.1);
        // The night street, pushed far back: darkest behind the text on the left, a little of
        // the city's light left on the right.
        for (x, y, p) in base.enumerate_pixels_mut() {
            let t = x as f32 / W as f32;
            let v = y as f32 / H as f32;
            let k = 0.13 + 0.17 * t * t - 0.04 * v;
            for c in p.0.iter_mut() {
                // Snapped to even levels: invisible this dark, and the PNG is much smaller.
                let v = *c as f32 * k + 13.0 * (1.0 - k);
                *c = ((v / 2.0).round() * 2.0) as u8;
            }
        }
        Assets {
            bold: font(ROBOTO_BOLD),
            medium: font(ROBOTO_MEDIUM),
            mono: font(MONO),
            base,
            mark: imageops::resize(
                &image::load_from_memory(MARK)
                    .expect("embedded mark")
                    .to_rgba8(),
                84,
                84,
                imageops::FilterType::Triangle,
            ),
        }
    })
}

fn blend(img: &mut RgbImage, x: i32, y: i32, color: [u8; 3], a: f32) {
    if x < 0 || y < 0 || x >= W as i32 || y >= H as i32 || a <= 0.0 {
        return;
    }
    let a = a.min(1.0);
    let p = img.get_pixel_mut(x as u32, y as u32);
    for (c, k) in p.0.iter_mut().zip(color) {
        *c = (*c as f32 * (1.0 - a) + k as f32 * a).round() as u8;
    }
}

fn rect(img: &mut RgbImage, x: i32, y: i32, w: i32, h: i32, color: [u8; 3], a: f32) {
    for yy in y..y + h {
        for xx in x..x + w {
            blend(img, xx, yy, color, a);
        }
    }
}

/// A font at a CSS-like pixel size (the em square, not the line height).
fn scale(font: &FontRef<'_>, px: f32) -> PxScale {
    let upem = font.units_per_em().unwrap_or(1000.0);
    PxScale::from(px * font.height_unscaled() / upem)
}

fn measure(font: &FontRef<'_>, px: f32, tracking: f32, text: &str) -> f32 {
    let sf = font.as_scaled(scale(font, px));
    let mut w = 0.0;
    let mut prev = None;
    for c in text.chars() {
        let id = sf.glyph_id(c);
        if let Some(p) = prev {
            w += sf.kern(p, id) + tracking;
        }
        w += sf.h_advance(id);
        prev = Some(id);
    }
    w
}

/// Draws `text` with its baseline at `y`; returns the advance width.
#[allow(clippy::too_many_arguments)]
fn text(
    img: &mut RgbImage,
    font: &FontRef<'_>,
    px: f32,
    tracking: f32,
    x: f32,
    y: f32,
    color: [u8; 3],
    s: &str,
) -> f32 {
    let sc = scale(font, px);
    let sf = font.as_scaled(sc);
    let mut cx = x;
    let mut prev = None;
    for c in s.chars() {
        let id = sf.glyph_id(c);
        if let Some(p) = prev {
            cx += sf.kern(p, id) + tracking;
        }
        let g = id.with_scale_and_position(sc, ab_glyph::point(cx, y));
        if let Some(o) = font.outline_glyph(g) {
            let b = o.px_bounds();
            o.draw(|gx, gy, cov| {
                blend(
                    img,
                    b.min.x as i32 + gx as i32,
                    b.min.y as i32 + gy as i32,
                    color,
                    cov,
                )
            });
        }
        cx += sf.h_advance(id);
        prev = Some(id);
    }
    cx - x
}

/// The four gold corner ticks of the ZScan frame.
fn ticks(img: &mut RgbImage, x0: i32, y0: i32, x1: i32, y1: i32) {
    let (len, th) = (26, 3);
    for (cx, cy, dx, dy) in [
        (x0, y0, 1, 1),
        (x1, y0, -1, 1),
        (x0, y1, 1, -1),
        (x1, y1, -1, -1),
    ] {
        let hx = if dx > 0 { cx } else { cx - len + 1 };
        let vy = if dy > 0 { cy } else { cy - len + 1 };
        let hy = if dy > 0 { cy } else { cy - th + 1 };
        let vx = if dx > 0 { cx } else { cx - th + 1 };
        rect(img, hx, hy, len, th, GOLD, 1.0);
        rect(img, vx, vy, th, len, GOLD, 1.0);
    }
}

/// Draws the card. Pure CPU, no I/O.
pub fn render(card: &Card) -> RgbImage {
    let a = assets();
    let mut img = a.base.clone();

    // Frame: a hairline and the gold ticks.
    let (fx0, fy0, fx1, fy1) = (32, 32, W as i32 - 33, H as i32 - 33);
    rect(&mut img, fx0, fy0, fx1 - fx0 + 1, 1, INK, 0.16);
    rect(&mut img, fx0, fy1, fx1 - fx0 + 1, 1, INK, 0.16);
    rect(&mut img, fx0, fy0, 1, fy1 - fy0 + 1, INK, 0.16);
    rect(&mut img, fx1, fy0, 1, fy1 - fy0 + 1, INK, 0.16);
    ticks(&mut img, fx0, fy0, fx1, fy1);

    let left = 84.0;
    let right = W as f32 - 84.0;

    // Header: the shield and the wordmark; the address on the right.
    for (mx, my, p) in a.mark.enumerate_pixels() {
        let [r, g, b, al] = p.0;
        blend(
            &mut img,
            left as i32 - 22 + mx as i32,
            50 + my as i32,
            [r, g, b],
            al as f32 / 255.0,
        );
    }
    let wx = left + 56.0;
    let w = text(&mut img, &a.bold, 30.0, 3.0, wx, 104.0, INK, "Z");
    text(
        &mut img,
        &a.bold,
        30.0,
        3.0,
        wx + w + 3.0,
        104.0,
        GOLD,
        "STREET",
    );
    let site = "zstreet.cash";
    let sw = measure(&a.mono, 17.0, 0.5, site);
    text(&mut img, &a.mono, 17.0, 0.5, right - sw, 100.0, DIM, site);

    // The transaction.
    let mempool = card.height.is_none();
    text(
        &mut img,
        &a.mono,
        16.0,
        3.5,
        left,
        196.0,
        GOLD,
        "TRANSACTION",
    );
    let short = format!(
        "{}…{}",
        &card.txid[..10],
        &card.txid[card.txid.len() - 10..]
    );
    text(&mut img, &a.mono, 46.0, 0.0, left, 254.0, INK, &short);

    // Kind and the pools it touches.
    text(
        &mut img,
        &a.bold,
        64.0,
        -0.5,
        left,
        340.0,
        INK,
        card.title(),
    );
    let (from, to) = card.flow();
    let mut x = left;
    let fy = 384.0;
    for (i, side) in [from, to].iter().enumerate() {
        if i == 1 {
            x += 12.0;
            x += text(&mut img, &a.mono, 20.0, 0.0, x, fy, GOLD, "→");
            x += 12.0;
        }
        for (j, name) in side.iter().enumerate() {
            if j > 0 {
                x += text(&mut img, &a.mono, 20.0, 1.0, x, fy, FAINT, " + ");
            }
            x += text(&mut img, &a.mono, 20.0, 2.0, x, fy, DIM, name);
        }
    }

    // Three cells on hairlines: amount, fee, block.
    let top = 430;
    rect(
        &mut img,
        left as i32,
        top,
        (right - left) as i32,
        1,
        INK,
        0.22,
    );
    rect(
        &mut img,
        left as i32,
        top + 104,
        (right - left) as i32,
        1,
        INK,
        0.22,
    );
    let cells = [left, left + 452.0, left + 452.0 + 262.0];
    for c in &cells[1..] {
        rect(&mut img, *c as i32 - 26, top + 1, 1, 103, INK, 0.22);
    }
    let (label, amount) = card.amount();
    let ly = top as f32 + 32.0;
    let vy = top as f32 + 80.0;
    text(&mut img, &a.mono, 14.0, 2.5, cells[0], ly, FAINT, label);
    match amount {
        Some(z) => {
            let w = text(&mut img, &a.bold, 40.0, -0.3, cells[0], vy, INK, &zec(z));
            text(
                &mut img,
                &a.medium,
                22.0,
                1.0,
                cells[0] + w + 10.0,
                vy,
                GOLD,
                "ZEC",
            );
        }
        None => {
            text(
                &mut img, &a.bold, 40.0, -0.3, cells[0], vy, GOLD, "Shielded",
            );
        }
    }
    text(&mut img, &a.mono, 14.0, 2.5, cells[1], ly, FAINT, "FEE");
    match card.fee {
        Some(f) => {
            let w = text(&mut img, &a.bold, 40.0, -0.3, cells[1], vy, INK, &zec(f));
            text(
                &mut img,
                &a.medium,
                22.0,
                1.0,
                cells[1] + w + 10.0,
                vy,
                GOLD,
                "ZEC",
            );
        }
        None => {
            text(&mut img, &a.bold, 40.0, 0.0, cells[1], vy, DIM, "—");
        }
    }
    match card.height {
        Some(h) => {
            text(&mut img, &a.mono, 14.0, 2.5, cells[2], ly, FAINT, "BLOCK");
            text(
                &mut img,
                &a.bold,
                40.0,
                -0.3,
                cells[2],
                vy,
                INK,
                &group(h as u64),
            );
            if let Some(t) = card.time {
                let d = t.format("%Y-%m-%d %H:%M UTC").to_string();
                let dw = measure(&a.mono, 14.0, 0.5, &d);
                text(&mut img, &a.mono, 14.0, 0.5, right - dw, ly, FAINT, &d);
            }
        }
        None => {
            text(&mut img, &a.mono, 14.0, 2.5, cells[2], ly, FAINT, "STATUS");
            rect(
                &mut img,
                cells[2] as i32,
                vy as i32 - 22,
                10,
                10,
                GREEN,
                1.0,
            );
            text(
                &mut img,
                &a.bold,
                34.0,
                -0.3,
                cells[2] + 22.0,
                vy - 2.0,
                INK,
                "In the mempool",
            );
        }
    }

    // The invitation.
    let by = 584.0;
    let call = if mempool {
        "Watch it board the next block on ZStreet"
    } else {
        "Watch it cross ZStreet"
    };
    let w = text(&mut img, &a.medium, 26.0, 0.2, left, by, GOLD, call);
    text(
        &mut img,
        &a.mono,
        26.0,
        0.0,
        left + w + 12.0,
        by - 1.0,
        GOLD,
        "→",
    );
    let path = format!("zstreet.cash/tx/{}…", &card.txid[..8]);
    let pw = measure(&a.mono, 16.0, 0.3, &path);
    text(
        &mut img,
        &a.mono,
        16.0,
        0.3,
        right - pw,
        by - 2.0,
        FAINT,
        &path,
    );
    img
}

pub fn encode_png(img: &RgbImage) -> anyhow::Result<Vec<u8>> {
    use image::ImageEncoder;
    let mut out = Vec::with_capacity(512 * 1024);
    image::codecs::png::PngEncoder::new_with_quality(
        &mut out,
        image::codecs::png::CompressionType::Default,
        image::codecs::png::FilterType::Adaptive,
    )
    .write_image(img.as_raw(), W, H, image::ExtendedColorType::Rgb8)?;
    Ok(out)
}

struct Cached {
    png: Bytes,
    /// `None` for a final card; otherwise when it should be drawn again.
    until: Option<Instant>,
    cache_control: &'static str,
}

fn cache() -> &'static Mutex<LruCache<String, Arc<Cached>>> {
    static C: OnceLock<Mutex<LruCache<String, Arc<Cached>>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(LruCache::new(NonZeroUsize::new(KEEP).unwrap())))
}

fn cached(txid: &str) -> Option<Arc<Cached>> {
    let mut c = cache().lock().unwrap();
    let hit = c.get(txid)?.clone();
    match hit.until {
        Some(t) if t <= Instant::now() => {
            c.pop(txid);
            None
        }
        _ => Some(hit),
    }
}

fn respond(c: &Cached) -> Response {
    (
        [
            (header::CONTENT_TYPE, "image/png"),
            (header::CACHE_CONTROL, c.cache_control),
        ],
        c.png.clone(),
    )
        .into_response()
}

/// The ZStreet share card for a transaction (mined or in the mempool), as a 1200×630 PNG.
pub async fn tx(State(s): State<AppState>, Path(file): Path<String>) -> Result<Response, ApiError> {
    let txid = file
        .strip_suffix(".png")
        .map(str::to_lowercase)
        .filter(|t| zscan_chain::hash::is_hash(t))
        .ok_or_else(|| ApiError::NotFound("Expected /card/tx/<64-hex txid>.png".into()))?;
    if let Some(c) = cached(&txid) {
        return Ok(respond(&c));
    }
    // Two renders at a time; whoever waited finds the card the first one drew.
    static SLOTS: Semaphore = Semaphore::const_new(2);
    let _slot = SLOTS.acquire().await.map_err(anyhow::Error::from)?;
    if let Some(c) = cached(&txid) {
        return Ok(respond(&c));
    }
    let detail = crate::routes::tx::detail(State(s), Path(txid.clone()))
        .await?
        .0;
    let card = Card::from_detail(&detail);
    let png = tokio::task::spawn_blocking(move || encode_png(&render(&card)))
        .await
        .map_err(anyhow::Error::from)??;
    let confs = detail.confirmations.unwrap_or(0);
    let (until, cache_control) = match detail.height {
        None => (Some(Instant::now() + MEMPOOL_TTL), "public, max-age=30"),
        Some(_) if confs < FINAL_CONFS => (
            Some(Instant::now() + Duration::from_secs(600)),
            "public, max-age=600",
        ),
        Some(_) => (None, "public, max-age=31536000, immutable"),
    };
    let c = Arc::new(Cached {
        png: Bytes::from(png),
        until,
        cache_control,
    });
    cache().lock().unwrap().put(txid, c.clone());
    Ok(respond(&c))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn card(kind: &str) -> Card {
        Card {
            txid: "3f2a9c41d07be55e8a91c2f04d6b3e7718aa0c95f1e2d3b4c5a6978812e0b71e".into(),
            kind: kind.into(),
            height: Some(3_112_457),
            time: Utc.with_ymd_and_hms(2026, 10, 16, 21, 4, 0).single(),
            fee: Some(15_000),
            transparent_in: 0,
            transparent_out: 0,
            t_inputs: 0,
            t_outputs: 0,
            vb: [0; 4],
            active: [false; 4],
        }
    }

    fn samples() -> Vec<(&'static str, Card)> {
        let mut shielded = card("shielded");
        shielded.vb = [0, 0, 15_000, 0];
        shielded.active = [false, false, true, false];

        let mut transparent = card("transparent");
        transparent.txid =
            "a07c55e19b3d2f8e4c6a1b0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f".into();
        transparent.t_inputs = 3;
        transparent.t_outputs = 2;
        transparent.transparent_in = 1_284_517_350_000 + 20_000;
        transparent.transparent_out = 1_284_517_350_000;
        transparent.fee = Some(20_000);

        let mut mempool = card("shielding");
        mempool.txid = "e4b1f0a2c3d4e5f60718293a4b5c6d7e8f9012a3b4c5d6e7f8091a2b3c4d5e6f".into();
        mempool.height = None;
        mempool.time = None;
        mempool.t_inputs = 1;
        mempool.transparent_in = 2_505_000_000 + 10_000;
        mempool.vb = [0, 0, -2_505_000_000, 0];
        mempool.active = [false, false, true, false];
        mempool.fee = Some(10_000);

        let mut migration = card("migration");
        migration.txid = "77c0ffee00112233445566778899aabbccddeeff00112233445566778899aabb".into();
        migration.vb = [0, 0, 4_200_015_000, -4_200_000_000];
        migration.active = [false, false, true, true];
        vec![
            ("shielded", shielded),
            ("transparent", transparent),
            ("mempool", mempool),
            ("migration", migration),
        ]
    }

    #[test]
    fn amounts() {
        assert_eq!(zec(1_284_517_350_000), "12,845.17");
        assert_eq!(zec(15_000), "0.00015");
        assert_eq!(zec(2_505_000_000), "25.05");
        assert_eq!(zec(100_000_000_000), "1,000");
        let s = samples();
        assert_eq!(s[0].1.amount().1, None);
        assert_eq!(s[2].1.amount(), ("INTO THE POOL", Some(2_505_000_000)));
        assert_eq!(s[3].1.flow(), (vec!["ORCHARD"], vec!["IRONWOOD"]));
        assert_eq!(s[2].1.flow(), (vec!["TRANSPARENT"], vec!["ORCHARD"]));
        assert_eq!(s[0].1.flow(), (vec!["ORCHARD"], vec!["ORCHARD"]));
    }

    /// Draws the sample cards; with `STREET_CARD_OUT=<dir>` they are written there to look at.
    #[test]
    fn renders() {
        let out = std::env::var("STREET_CARD_OUT").ok();
        for (name, c) in samples() {
            let t = Instant::now();
            let img = render(&c);
            let png = encode_png(&img).unwrap();
            assert_eq!(img.dimensions(), (W, H));
            eprintln!("{name}: {} KB in {:?}", png.len() / 1024, t.elapsed());
            if let Some(dir) = &out {
                std::fs::create_dir_all(dir).unwrap();
                std::fs::write(format!("{dir}/{name}.png"), png).unwrap();
            }
        }
    }
}
