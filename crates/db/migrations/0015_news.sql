-- Zcash news: items from the curated feeds (labels/news_sources.json) and network events that
-- ZScan detects in its own data (daily records, activity spikes). Title and a short excerpt
-- only; the link is the story.
CREATE TABLE news_items (
    id           TEXT PRIMARY KEY,
    source       TEXT NOT NULL,
    category     TEXT NOT NULL,
    title        TEXT NOT NULL,
    url          TEXT NOT NULL,
    summary      TEXT,
    published_at TIMESTAMPTZ NOT NULL,
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Network events: the metric and figures behind the headline.
    data         JSONB
);
CREATE UNIQUE INDEX news_items_url_idx ON news_items (url);
CREATE INDEX news_items_published_idx ON news_items (published_at DESC);
CREATE INDEX news_items_category_idx ON news_items (category, published_at DESC);
