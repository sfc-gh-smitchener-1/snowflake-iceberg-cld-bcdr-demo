/*******************************************************************************
 * ICEBERG CLD BCDR DEMO
 * Script: 15_create_prod_database.sql
 * Purpose: Create a PROD database with views over CLD tables
 *
 * This script creates an abstraction layer that:
 * 1. Creates views over all CLD Iceberg tables (SELECT *)
 * 2. Recreates the EXT materialized view logic pointing to CLD tables
 * 3. Provides a single consistent interface for applications
 *
 * Benefits:
 * - Applications connect to PROD, not directly to CLD or EXT
 * - Easy to switch underlying data sources
 * - Consistent naming regardless of source
 * - Can add business logic in the view layer
 *
 * Prerequisites:
 *   - CLD created (script 11)
 *   - External tables created (script 10)
 *   - Materialized views created (script 12) - optional
 *
 * Run as: ICEBERG_ADMIN or ACCOUNTADMIN
 ******************************************************************************/

-- ============================================================================
-- SECTION 1: Create PROD Database and Schema
-- ============================================================================

USE ROLE ICEBERG_ADMIN;
USE WAREHOUSE ICEBERG_DEMO_WH;

-- Create the PROD database
CREATE DATABASE IF NOT EXISTS ICEBERG_PROD
    COMMENT = 'Production database with views over Iceberg CLD tables';

-- Create schema
CREATE SCHEMA IF NOT EXISTS ICEBERG_PROD.ADVERTISING
    COMMENT = 'Advertising data views backed by CLD Iceberg tables';

USE DATABASE ICEBERG_PROD;
USE SCHEMA ADVERTISING;

-- ============================================================================
-- SECTION 2: Create Views Over CLD Tables (SELECT *)
-- ============================================================================

/*
 * These views provide a simple passthrough to the CLD tables.
 * Applications query these views instead of the CLD directly.
 */

-- CAMPAIGNS view
CREATE OR REPLACE VIEW CAMPAIGNS
    COMMENT = 'Campaign dimension table - backed by CLD Iceberg table'
AS
SELECT *
FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CAMPAIGNS;

-- IMPRESSIONS view
CREATE OR REPLACE VIEW IMPRESSIONS
    COMMENT = 'Impression events - backed by CLD Iceberg table'
AS
SELECT *
FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.IMPRESSIONS;

-- CLICKS view
CREATE OR REPLACE VIEW CLICKS
    COMMENT = 'Click events - backed by CLD Iceberg table'
AS
SELECT *
FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CLICKS;

-- CONVERSIONS view
CREATE OR REPLACE VIEW CONVERSIONS
    COMMENT = 'Conversion events - backed by CLD Iceberg table'
AS
SELECT *
FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CONVERSIONS;

-- Verify base views
SHOW VIEWS IN SCHEMA ICEBERG_PROD.ADVERTISING;

-- ============================================================================
-- SECTION 3: Recreate Aggregated Views (Same Logic as MVs, Using CLD)
-- ============================================================================

/*
 * These views replicate the logic from the EXT materialized views,
 * but point to the CLD tables instead.
 */

-- V_CAMPAIGNS_SUMMARY: Campaign summary with calculated fields
CREATE OR REPLACE VIEW V_CAMPAIGNS_SUMMARY
    COMMENT = 'Campaign summary with budget metrics - CLD backed'
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
FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CAMPAIGNS;

-- V_IMPRESSIONS_DAILY: Daily impression aggregates
CREATE OR REPLACE VIEW V_IMPRESSIONS_DAILY
    COMMENT = 'Daily impression aggregates by campaign and region - CLD backed'
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
FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.IMPRESSIONS
GROUP BY 
    campaign_id,
    date_key,
    geo_region,
    device_type;

-- V_CLICKS_DAILY: Daily click aggregates
CREATE OR REPLACE VIEW V_CLICKS_DAILY
    COMMENT = 'Daily click aggregates with engagement metrics - CLD backed'
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
FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CLICKS
GROUP BY 
    campaign_id,
    date_key,
    geo_region,
    device_type;

-- V_CONVERSIONS_DAILY: Daily conversion aggregates
CREATE OR REPLACE VIEW V_CONVERSIONS_DAILY
    COMMENT = 'Daily conversion aggregates with revenue metrics - CLD backed'
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
FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CONVERSIONS
GROUP BY 
    campaign_id,
    date_key,
    conversion_type,
    geo_region,
    device_type,
    attribution_model;

-- V_CAMPAIGN_PERFORMANCE: Full funnel metrics
CREATE OR REPLACE VIEW V_CAMPAIGN_PERFORMANCE
    COMMENT = 'Full funnel campaign performance metrics - CLD backed'
AS
WITH impressions_agg AS (
    SELECT 
        campaign_id,
        date_key,
        COUNT(*) AS impressions,
        SUM(cost_usd) AS impression_cost
    FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.IMPRESSIONS
    GROUP BY campaign_id, date_key
),
clicks_agg AS (
    SELECT 
        campaign_id,
        date_key,
        COUNT(*) AS clicks,
        SUM(cost_usd) AS click_cost
    FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CLICKS
    GROUP BY campaign_id, date_key
),
conversions_agg AS (
    SELECT 
        campaign_id,
        date_key,
        COUNT(*) AS conversions,
        SUM(revenue_usd) AS revenue
    FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CONVERSIONS
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
FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CAMPAIGNS c
LEFT JOIN impressions_agg i ON c.campaign_id = i.campaign_id
LEFT JOIN clicks_agg cl ON c.campaign_id = cl.campaign_id AND i.date_key = cl.date_key
LEFT JOIN conversions_agg cv ON c.campaign_id = cv.campaign_id AND i.date_key = cv.date_key
WHERE i.date_key IS NOT NULL OR cl.date_key IS NOT NULL OR cv.date_key IS NOT NULL;

-- ============================================================================
-- SECTION 4: Grant Access
-- ============================================================================

-- Grant database access
GRANT USAGE ON DATABASE ICEBERG_PROD TO ROLE ICEBERG_ENGINEER;
GRANT USAGE ON DATABASE ICEBERG_PROD TO ROLE ICEBERG_ANALYST;

-- Grant schema access
GRANT USAGE ON SCHEMA ICEBERG_PROD.ADVERTISING TO ROLE ICEBERG_ENGINEER;
GRANT USAGE ON SCHEMA ICEBERG_PROD.ADVERTISING TO ROLE ICEBERG_ANALYST;

-- Grant SELECT on all views to analyst
GRANT SELECT ON ALL VIEWS IN SCHEMA ICEBERG_PROD.ADVERTISING TO ROLE ICEBERG_ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA ICEBERG_PROD.ADVERTISING TO ROLE ICEBERG_ENGINEER;

-- Future grants
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ICEBERG_PROD.ADVERTISING TO ROLE ICEBERG_ANALYST;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ICEBERG_PROD.ADVERTISING TO ROLE ICEBERG_ENGINEER;

-- ============================================================================
-- SECTION 5: Verification
-- ============================================================================

-- Show all views
SHOW VIEWS IN SCHEMA ICEBERG_PROD.ADVERTISING;

-- Test base table views
SELECT 'CAMPAIGNS' AS view_name, COUNT(*) AS row_count FROM CAMPAIGNS
UNION ALL
SELECT 'IMPRESSIONS', COUNT(*) FROM IMPRESSIONS
UNION ALL
SELECT 'CLICKS', COUNT(*) FROM CLICKS
UNION ALL
SELECT 'CONVERSIONS', COUNT(*) FROM CONVERSIONS;

-- Test aggregated views
SELECT 'V_CAMPAIGNS_SUMMARY' AS view_name, COUNT(*) AS row_count FROM V_CAMPAIGNS_SUMMARY
UNION ALL
SELECT 'V_IMPRESSIONS_DAILY', COUNT(*) FROM V_IMPRESSIONS_DAILY
UNION ALL
SELECT 'V_CLICKS_DAILY', COUNT(*) FROM V_CLICKS_DAILY
UNION ALL
SELECT 'V_CONVERSIONS_DAILY', COUNT(*) FROM V_CONVERSIONS_DAILY
UNION ALL
SELECT 'V_CAMPAIGN_PERFORMANCE', COUNT(*) FROM V_CAMPAIGN_PERFORMANCE;

-- Sample query on PROD views
SELECT 
    channel,
    COUNT(*) AS campaigns,
    SUM(budget_usd) AS total_budget
FROM V_CAMPAIGNS_SUMMARY
GROUP BY channel
ORDER BY total_budget DESC;

/*******************************************************************************
 * PROD DATABASE SUMMARY:
 *
 * Created ICEBERG_PROD.ADVERTISING with the following views:
 *
 * BASE TABLE VIEWS (SELECT * from CLD):
 * ┌───────────────────┬─────────────────────────────────────────────────┐
 * │ View              │ Source                                          │
 * ├───────────────────┼─────────────────────────────────────────────────┤
 * │ CAMPAIGNS         │ ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CAMPAIGNS│
 * │ IMPRESSIONS       │ ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.IMPRESSIONS│
 * │ CLICKS            │ ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CLICKS  │
 * │ CONVERSIONS       │ ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CONVERSIONS│
 * └───────────────────┴─────────────────────────────────────────────────┘
 *
 * AGGREGATED VIEWS (Same logic as EXT MVs, using CLD):
 * ┌───────────────────────────┬─────────────────────────────────────────┐
 * │ View                      │ Description                             │
 * ├───────────────────────────┼─────────────────────────────────────────┤
 * │ V_CAMPAIGNS_SUMMARY       │ Campaign details with duration          │
 * │ V_IMPRESSIONS_DAILY       │ Daily impression aggregates             │
 * │ V_CLICKS_DAILY            │ Daily click aggregates with bounce rate │
 * │ V_CONVERSIONS_DAILY       │ Daily conversion aggregates with revenue│
 * │ V_CAMPAIGN_PERFORMANCE    │ Full funnel: CTR, CVR, CPA, ROAS        │
 * └───────────────────────────┴─────────────────────────────────────────┘
 *
 * ARCHITECTURE:
 *
 *   ┌─────────────────────────────────────────────────────────────────┐
 *   │                    APPLICATION LAYER                            │
 *   │              (BI Tools, Apps, Dashboards)                       │
 *   └────────────────────────────┬────────────────────────────────────┘
 *                                │
 *                                ▼
 *   ┌─────────────────────────────────────────────────────────────────┐
 *   │                    ICEBERG_PROD                                 │
 *   │         (Views - Abstraction Layer)                             │
 *   │   CAMPAIGNS, IMPRESSIONS, CLICKS, CONVERSIONS                   │
 *   │   V_CAMPAIGNS_SUMMARY, V_IMPRESSIONS_DAILY, etc.                │
 *   └────────────────────────────┬────────────────────────────────────┘
 *                                │
 *                                ▼
 *   ┌─────────────────────────────────────────────────────────────────┐
 *   │                    ICEBERG_DEMO_CLD                             │
 *   │         (Catalog Linked Database)                               │
 *   │   Auto-synced from AWS Glue Catalog                             │
 *   └────────────────────────────┬────────────────────────────────────┘
 *                                │
 *                                ▼
 *   ┌─────────────────────────────────────────────────────────────────┐
 *   │                    AWS Glue + S3                                │
 *   │         (Iceberg Tables)                                        │
 *   └─────────────────────────────────────────────────────────────────┘
 *
 * BCDR NOTE:
 * - ICEBERG_PROD is now INDEPENDENT on both accounts (NOT replicated)
 * - This allows writable objects (tasks, procedures) on both accounts
 * - Both ICEBERG_PROD databases have identical structure with views to local CLD
 * - Schema drift is detected daily by script 33_schema_sync_task.sql
 * - Migration to independent ICEBERG_PROD: script 32_migrate_prod_db_independent.sql
 *
 * NEXT STEPS FOR SECONDARY:
 * 1. Run 32_migrate_prod_db_independent.sql to create independent ICEBERG_PROD
 * 2. Run 33_schema_sync_task.sql to set up daily drift detection
 *
 ******************************************************************************/

