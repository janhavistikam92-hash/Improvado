-- =============================================================
-- Senior Marketing Analyst Assignment — Neon (Postgres) setup
-- =============================================================
-- Run order:
--   1. Section A — create the 3 raw tables
--   2. Load the CSVs (see "LOADING THE CSVs" comment block below)
--   3. Section B — create the unified table
--   4. Section C — sanity-check queries (optional)
--
-- WARNING: Section A drops & recreates the raw tables. If you re-run this
-- file after CSVs are loaded, you'll wipe them. To rebuild only the unified
-- table after the raw data is in place, run Section B by itself.
-- =============================================================


-- =============================================================
-- SECTION A — Raw tables (one per platform)
-- =============================================================

DROP TABLE IF EXISTS facebook_ads_raw;
CREATE TABLE facebook_ads_raw (
    date              DATE,
    campaign_id       TEXT,
    campaign_name     TEXT,
    ad_set_id         TEXT,
    ad_set_name       TEXT,
    impressions       BIGINT,
    clicks            BIGINT,
    spend             NUMERIC(12,2),
    conversions       INTEGER,
    video_views       BIGINT,
    engagement_rate   NUMERIC(8,5),
    reach             BIGINT,
    frequency         NUMERIC(8,3)
);

DROP TABLE IF EXISTS google_ads_raw;
CREATE TABLE google_ads_raw (
    date                       DATE,
    campaign_id                TEXT,
    campaign_name              TEXT,
    ad_group_id                TEXT,
    ad_group_name              TEXT,
    impressions                BIGINT,
    clicks                     BIGINT,
    cost                       NUMERIC(12,2),
    conversions                INTEGER,
    conversion_value           NUMERIC(12,2),
    ctr                        NUMERIC(8,5),
    avg_cpc                    NUMERIC(8,3),
    quality_score              INTEGER,
    search_impression_share    NUMERIC(8,4)
);

DROP TABLE IF EXISTS tiktok_ads_raw;
CREATE TABLE tiktok_ads_raw (
    date              DATE,
    campaign_id       TEXT,
    campaign_name     TEXT,
    adgroup_id        TEXT,
    adgroup_name      TEXT,
    impressions       BIGINT,
    clicks            BIGINT,
    cost              NUMERIC(12,2),
    conversions       INTEGER,
    video_views       BIGINT,
    video_watch_25    BIGINT,
    video_watch_50    BIGINT,
    video_watch_75    BIGINT,
    video_watch_100   BIGINT,
    likes             BIGINT,
    shares            BIGINT,
    comments          BIGINT
);


-- =============================================================
-- LOADING THE CSVs (run from psql, NOT in the SQL editor)
-- =============================================================
-- Neon is remote, so use \copy (client-side), not COPY.
-- Open a psql terminal connected to your Neon DB and run:
--
--   \copy facebook_ads_raw FROM '01_facebook_ads.csv' WITH (FORMAT csv, HEADER true);
--   \copy google_ads_raw   FROM '02_google_ads.csv'   WITH (FORMAT csv, HEADER true);
--   \copy tiktok_ads_raw   FROM '03_tiktok_ads.csv'   WITH (FORMAT csv, HEADER true);
--
-- Alternative: in DBeaver, right-click each table -> Import Data -> CSV.
-- =============================================================


-- =============================================================
-- SECTION B — Unified table
-- =============================================================
-- Design notes:
--   * One row = one (channel, date, campaign, ad_group) combination.
--   * Common metrics are normalized: impressions, clicks, spend, conversions.
--   * Platform-specific columns (video_views, revenue, engagement_rate, etc.)
--     are kept as nullable so no information is lost.
--   * Derived metrics (CTR, CPC, CPM, CPA, ROAS) are computed once here so
--     the BI layer (Power BI) doesn't have to redo the math per visual.
-- =============================================================

DROP TABLE IF EXISTS ads_unified;
CREATE TABLE ads_unified AS
SELECT
    'Facebook'::TEXT             AS channel,
    date,
    campaign_id,
    campaign_name,
    ad_set_id                    AS ad_group_id,
    ad_set_name                  AS ad_group_name,
    impressions,
    clicks,
    spend                        AS spend,
    conversions,
    video_views,
    NULL::NUMERIC                AS revenue,
    engagement_rate,
    reach,
    frequency,
    NULL::INTEGER                AS quality_score,
    NULL::NUMERIC                AS search_impression_share,
    NULL::BIGINT                 AS likes,
    NULL::BIGINT                 AS shares,
    NULL::BIGINT                 AS comments
FROM facebook_ads_raw

UNION ALL

SELECT
    'Google'::TEXT,
    date,
    campaign_id,
    campaign_name,
    ad_group_id,
    ad_group_name,
    impressions,
    clicks,
    cost,
    conversions,
    NULL::BIGINT                 AS video_views,
    conversion_value             AS revenue,
    NULL::NUMERIC                AS engagement_rate,
    NULL::BIGINT                 AS reach,
    NULL::NUMERIC                AS frequency,
    quality_score,
    search_impression_share,
    NULL::BIGINT, NULL::BIGINT, NULL::BIGINT
FROM google_ads_raw

UNION ALL

SELECT
    'TikTok'::TEXT,
    date,
    campaign_id,
    campaign_name,
    adgroup_id,
    adgroup_name,
    impressions,
    clicks,
    cost,
    conversions,
    video_views,
    NULL::NUMERIC                AS revenue,
    NULL::NUMERIC                AS engagement_rate,
    NULL::BIGINT                 AS reach,
    NULL::NUMERIC                AS frequency,
    NULL::INTEGER                AS quality_score,
    NULL::NUMERIC                AS search_impression_share,
    likes,
    shares,
    comments
FROM tiktok_ads_raw;

-- Add derived KPI columns (CTR, CPC, CPM, CPA, ROAS)
ALTER TABLE ads_unified
    ADD COLUMN ctr   NUMERIC(10,5),
    ADD COLUMN cpc   NUMERIC(10,3),
    ADD COLUMN cpm   NUMERIC(10,3),
    ADD COLUMN cpa   NUMERIC(10,3),
    ADD COLUMN roas  NUMERIC(10,3);

UPDATE ads_unified
SET
    ctr  = CASE WHEN impressions > 0 THEN clicks::NUMERIC      / impressions END,
    cpc  = CASE WHEN clicks      > 0 THEN spend::NUMERIC       / clicks      END,
    cpm  = CASE WHEN impressions > 0 THEN spend::NUMERIC * 1000 / impressions END,
    cpa  = CASE WHEN conversions > 0 THEN spend::NUMERIC       / conversions END,
    roas = CASE WHEN spend       > 0 AND revenue IS NOT NULL
                THEN revenue / spend END;

-- Helpful indexes for the BI layer
CREATE INDEX idx_ads_unified_date    ON ads_unified (date);
CREATE INDEX idx_ads_unified_channel ON ads_unified (channel);


-- =============================================================
-- SECTION C — Sanity checks (optional, run after loading)
-- =============================================================

-- Row counts per channel
-- SELECT channel, COUNT(*) AS rows FROM ads_unified GROUP BY channel;

-- Channel-level KPI roll-up
-- SELECT
--     channel,
--     SUM(impressions)                          AS impressions,
--     SUM(clicks)                               AS clicks,
--     SUM(spend)                                AS spend,
--     SUM(conversions)                          AS conversions,
--     SUM(revenue)                              AS revenue,
--     ROUND(SUM(clicks)::NUMERIC / NULLIF(SUM(impressions),0), 4) AS ctr,
--     ROUND(SUM(spend)::NUMERIC  / NULLIF(SUM(clicks),0),      3) AS cpc,
--     ROUND(SUM(spend)::NUMERIC  / NULLIF(SUM(conversions),0), 2) AS cpa
-- FROM ads_unified
-- GROUP BY channel
-- ORDER BY spend DESC;

-- Daily trend
-- SELECT date, channel, SUM(spend) AS spend, SUM(conversions) AS conversions
-- FROM ads_unified GROUP BY date, channel ORDER BY date, channel;
