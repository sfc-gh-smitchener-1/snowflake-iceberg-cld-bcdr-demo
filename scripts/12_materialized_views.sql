/*******************************************************************************
 * ICEBERG CLD BCDR DEMO
 * Script: 12_materialized_views.sql
 * Purpose: Create materialized views over the external Iceberg tables
 *
 * Materialized Views provide:
 * - Improved query performance through pre-computed results
 * - Automatic refresh when underlying data changes
 * - Reduced compute costs for repeated queries
 *
 * These MVs are created over the External Tables (ICEBERG_DEMO_EXT)
 * and can be replicated via failover groups (unlike CLDs).
 *
 * Prerequisites:
 *   - External tables created (script 10)
 *   - ICEBERG_ENGINEER role or higher
 *
 * Run as: ICEBERG_ENGINEER or ICEBERG_ADMIN
 ******************************************************************************/

-- ============================================================================
-- SETUP
-- ============================================================================

USE ROLE ICEBERG_ENGINEER;
USE WAREHOUSE ICEBERG_DEMO_WH;
USE DATABASE ICEBERG_DEMO_EXT;
USE SCHEMA ADVERTISING;

-- ============================================================================
-- SECTION 1: Campaigns Materialized View
-- ============================================================================

/*
 * MV_CAMPAIGNS: Pre-aggregated campaign summary with budget utilization
 */

CREATE OR REPLACE MATERIALIZED VIEW MV_CAMPAIGNS_SUMMARY
    COMMENT = 'Campaign summary with budget metrics'
AS
SELECT 
    campaign_id,
    campaign_name,
    channel,
    ad_format,
    target_region,
    status,
    budget_usd,
    daily_budget_usd,
    target_cpa_usd,
    start_date,
    end_date,
    advertiser_name,
    -- Calculated fields
    DATEDIFF('day', start_date::DATE, end_date::DATE) AS campaign_duration_days,
    CASE 
        WHEN status = 'active' THEN 'Running'
        WHEN status = 'paused' THEN 'Paused'
        WHEN status = 'completed' THEN 'Finished'
        ELSE 'Draft'
    END AS status_label,
    created_at,
    updated_at
FROM EXT_CAMPAIGNS;

-- Verify MV created
SHOW MATERIALIZED VIEWS LIKE 'MV_CAMPAIGNS%';

-- ============================================================================
-- SECTION 2: Impressions Materialized View
-- ============================================================================

/*
 * MV_IMPRESSIONS_DAILY: Daily aggregated impression metrics
 */

CREATE OR REPLACE MATERIALIZED VIEW MV_IMPRESSIONS_DAILY
    COMMENT = 'Daily impression aggregates by campaign and region'
AS
SELECT 
    campaign_id,
    date_key,
    geo_region,
    device_type,
    COUNT(*) AS impression_count,
    SUM(CASE WHEN viewable THEN 1 ELSE 0 END) AS viewable_impressions,
    SUM(cost_usd) AS total_cost_usd,
    AVG(cost_usd) AS avg_cost_per_impression,
    COUNT(DISTINCT publisher_id) AS unique_publishers
FROM EXT_IMPRESSIONS
GROUP BY 
    campaign_id,
    date_key,
    geo_region,
    device_type;

-- Verify
SHOW MATERIALIZED VIEWS LIKE 'MV_IMPRESSIONS%';

-- ============================================================================
-- SECTION 3: Clicks Materialized View
-- ============================================================================

/*
 * MV_CLICKS_DAILY: Daily aggregated click metrics with engagement stats
 */

CREATE OR REPLACE MATERIALIZED VIEW MV_CLICKS_DAILY
    COMMENT = 'Daily click aggregates with engagement metrics'
AS
SELECT 
    campaign_id,
    date_key,
    geo_region,
    device_type,
    COUNT(*) AS click_count,
    SUM(cost_usd) AS total_click_cost_usd,
    AVG(cost_usd) AS avg_cost_per_click,
    AVG(time_on_site_seconds) AS avg_time_on_site_seconds,
    AVG(pages_viewed) AS avg_pages_viewed,
    SUM(CASE WHEN bounce THEN 1 ELSE 0 END) AS bounce_count,
    ROUND(SUM(CASE WHEN bounce THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS bounce_rate_pct
FROM EXT_CLICKS
GROUP BY 
    campaign_id,
    date_key,
    geo_region,
    device_type;

-- Verify
SHOW MATERIALIZED VIEWS LIKE 'MV_CLICKS%';

-- ============================================================================
-- SECTION 4: Conversions Materialized View
-- ============================================================================

/*
 * MV_CONVERSIONS_DAILY: Daily aggregated conversion metrics with revenue
 */

CREATE OR REPLACE MATERIALIZED VIEW MV_CONVERSIONS_DAILY
    COMMENT = 'Daily conversion aggregates with revenue metrics'
AS
SELECT 
    campaign_id,
    date_key,
    conversion_type,
    geo_region,
    device_type,
    attribution_model,
    COUNT(*) AS conversion_count,
    SUM(revenue_usd) AS total_revenue_usd,
    AVG(revenue_usd) AS avg_revenue_per_conversion,
    SUM(quantity) AS total_quantity,
    SUM(CASE WHEN new_customer THEN 1 ELSE 0 END) AS new_customer_conversions,
    ROUND(SUM(CASE WHEN new_customer THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS new_customer_rate_pct
FROM EXT_CONVERSIONS
GROUP BY 
    campaign_id,
    date_key,
    conversion_type,
    geo_region,
    device_type,
    attribution_model;

-- Verify
SHOW MATERIALIZED VIEWS LIKE 'MV_CONVERSIONS%';

-- ============================================================================
-- SECTION 5: Campaign Performance Materialized View (Joined)
-- ============================================================================

/*
 * MV_CAMPAIGN_PERFORMANCE: Combined funnel metrics per campaign
 * Joins impressions, clicks, and conversions for full funnel view
 */

CREATE OR REPLACE MATERIALIZED VIEW MV_CAMPAIGN_PERFORMANCE
    COMMENT = 'Full funnel campaign performance metrics'
AS
WITH impressions_agg AS (
    SELECT 
        campaign_id,
        date_key,
        COUNT(*) AS impressions,
        SUM(cost_usd) AS impression_cost
    FROM EXT_IMPRESSIONS
    GROUP BY campaign_id, date_key
),
clicks_agg AS (
    SELECT 
        campaign_id,
        date_key,
        COUNT(*) AS clicks,
        SUM(cost_usd) AS click_cost
    FROM EXT_CLICKS
    GROUP BY campaign_id, date_key
),
conversions_agg AS (
    SELECT 
        campaign_id,
        date_key,
        COUNT(*) AS conversions,
        SUM(revenue_usd) AS revenue
    FROM EXT_CONVERSIONS
    GROUP BY campaign_id, date_key
)
SELECT 
    c.campaign_id,
    c.campaign_name,
    c.channel,
    c.advertiser_name,
    COALESCE(i.date_key, cl.date_key, cv.date_key) AS date_key,
    COALESCE(i.impressions, 0) AS impressions,
    COALESCE(cl.clicks, 0) AS clicks,
    COALESCE(cv.conversions, 0) AS conversions,
    COALESCE(i.impression_cost, 0) + COALESCE(cl.click_cost, 0) AS total_cost_usd,
    COALESCE(cv.revenue, 0) AS total_revenue_usd,
    -- Calculated metrics
    ROUND(COALESCE(cl.clicks, 0) * 100.0 / NULLIF(i.impressions, 0), 4) AS ctr_pct,
    ROUND(COALESCE(cv.conversions, 0) * 100.0 / NULLIF(cl.clicks, 0), 4) AS conversion_rate_pct,
    ROUND((COALESCE(i.impression_cost, 0) + COALESCE(cl.click_cost, 0)) / NULLIF(cv.conversions, 0), 2) AS cost_per_acquisition,
    ROUND(COALESCE(cv.revenue, 0) / NULLIF(COALESCE(i.impression_cost, 0) + COALESCE(cl.click_cost, 0), 0), 2) AS roas
FROM EXT_CAMPAIGNS c
LEFT JOIN impressions_agg i ON c.campaign_id = i.campaign_id
LEFT JOIN clicks_agg cl ON c.campaign_id = cl.campaign_id AND i.date_key = cl.date_key
LEFT JOIN conversions_agg cv ON c.campaign_id = cv.campaign_id AND i.date_key = cv.date_key
WHERE i.date_key IS NOT NULL OR cl.date_key IS NOT NULL OR cv.date_key IS NOT NULL;

-- Verify
SHOW MATERIALIZED VIEWS LIKE 'MV_CAMPAIGN_PERFORMANCE%';

-- ============================================================================
-- SECTION 6: Grant Access to Materialized Views
-- ============================================================================

-- Grant SELECT on all MVs to analyst role
GRANT SELECT ON MATERIALIZED VIEW MV_CAMPAIGNS_SUMMARY TO ROLE ICEBERG_ANALYST;
GRANT SELECT ON MATERIALIZED VIEW MV_IMPRESSIONS_DAILY TO ROLE ICEBERG_ANALYST;
GRANT SELECT ON MATERIALIZED VIEW MV_CLICKS_DAILY TO ROLE ICEBERG_ANALYST;
GRANT SELECT ON MATERIALIZED VIEW MV_CONVERSIONS_DAILY TO ROLE ICEBERG_ANALYST;
GRANT SELECT ON MATERIALIZED VIEW MV_CAMPAIGN_PERFORMANCE TO ROLE ICEBERG_ANALYST;

-- ============================================================================
-- SECTION 7: Verification Queries
-- ============================================================================

-- Show all materialized views
SHOW MATERIALIZED VIEWS IN SCHEMA ADVERTISING;

-- Test queries
SELECT COUNT(*) AS campaign_count FROM MV_CAMPAIGNS_SUMMARY;
SELECT date_key, SUM(impression_count) AS total_impressions FROM MV_IMPRESSIONS_DAILY GROUP BY date_key ORDER BY date_key LIMIT 5;
SELECT date_key, SUM(click_count) AS total_clicks FROM MV_CLICKS_DAILY GROUP BY date_key ORDER BY date_key LIMIT 5;
SELECT date_key, SUM(conversion_count) AS total_conversions FROM MV_CONVERSIONS_DAILY GROUP BY date_key ORDER BY date_key LIMIT 5;

-- Full funnel summary
SELECT 
    channel,
    SUM(impressions) AS total_impressions,
    SUM(clicks) AS total_clicks,
    SUM(conversions) AS total_conversions,
    ROUND(AVG(ctr_pct), 4) AS avg_ctr,
    ROUND(AVG(roas), 2) AS avg_roas
FROM MV_CAMPAIGN_PERFORMANCE
GROUP BY channel
ORDER BY total_impressions DESC;

/*******************************************************************************
 * MATERIALIZED VIEW SUMMARY:
 *
 * Created 5 Materialized Views:
 *
 * 1. MV_CAMPAIGNS_SUMMARY
 *    - Full campaign details with calculated duration
 *    - Source: EXT_CAMPAIGNS
 *
 * 2. MV_IMPRESSIONS_DAILY  
 *    - Daily impression aggregates by campaign/region/device
 *    - Source: EXT_IMPRESSIONS
 *
 * 3. MV_CLICKS_DAILY
 *    - Daily click aggregates with engagement metrics
 *    - Source: EXT_CLICKS
 *
 * 4. MV_CONVERSIONS_DAILY
 *    - Daily conversion aggregates with revenue
 *    - Source: EXT_CONVERSIONS
 *
 * 5. MV_CAMPAIGN_PERFORMANCE
 *    - Full funnel metrics (impressions → clicks → conversions)
 *    - Joined view with CTR, conversion rate, CPA, ROAS
 *
 * BENEFITS:
 * - Faster query performance (pre-aggregated data)
 * - Auto-refresh when underlying Iceberg tables change
 * - Can be replicated via failover groups (unlike CLDs!)
 *
 * NEXT STEPS:
 * - Query the MVs for faster analytics
 * - MVs will be included in database replication
 ******************************************************************************/

