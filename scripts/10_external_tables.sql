/*******************************************************************************
 * ICEBERG CLD BCDR DEMO
 * Script: 10_external_tables.sql
 * Purpose: Create Iceberg External Tables in Snowflake (traditional approach)
 *
 * This script creates external Iceberg tables that reference the Glue-managed
 * tables in AWS. This is the "traditional" approach where table definitions
 * are manually created in Snowflake.
 *
 * Naming Convention:
 *   Database: ICEBERG_DEMO_EXT
 *   Schema: ADVERTISING
 *   Tables: EXT_CAMPAIGNS, EXT_IMPRESSIONS, EXT_CLICKS, EXT_CONVERSIONS
 *
 * Prerequisites:
 *   - Storage integration created (AWS_ICEBERG_STORAGE_INT)
 *   - Catalog integration created (GLUE_CATALOG_INT)
 *   - External volume created (ICEBERG_EXT_VOLUME)
 *   - Data loaded into Glue-managed Iceberg tables
 *
 * Run as: ICEBERG_ENGINEER
 ******************************************************************************/

-- ============================================================================
-- SECTION 1: Setup
-- ============================================================================

USE ROLE ICEBERG_ENGINEER;
USE WAREHOUSE ICEBERG_DEMO_WH;
USE DATABASE ICEBERG_DEMO_EXT;
USE SCHEMA ADVERTISING;

-- ============================================================================
-- SECTION 2: Create External Iceberg Tables
-- ============================================================================

/*
 * NOTE: These tables reference the Glue catalog. The CATALOG_TABLE_NAME
 * must match the table name in the Glue database exactly.
 */

-- ----------------------------------------------------------------------------
-- EXT_CAMPAIGNS: Advertising campaign definitions
-- ----------------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE EXT_CAMPAIGNS
    EXTERNAL_VOLUME = 'ICEBERG_EXT_VOLUME'
    CATALOG = 'GLUE_CATALOG_INT'
    CATALOG_TABLE_NAME = 'campaigns'
    COMMENT = 'Advertising campaigns - External Iceberg table from Glue catalog';

-- Describe table to verify structure
DESCRIBE TABLE EXT_CAMPAIGNS;

-- ----------------------------------------------------------------------------
-- EXT_IMPRESSIONS: Ad impression events
-- ----------------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE EXT_IMPRESSIONS
    EXTERNAL_VOLUME = 'ICEBERG_EXT_VOLUME'
    CATALOG = 'GLUE_CATALOG_INT'
    CATALOG_TABLE_NAME = 'impressions'
    COMMENT = 'Ad impression events - External Iceberg table from Glue catalog';

-- Describe table to verify structure
DESCRIBE TABLE EXT_IMPRESSIONS;

-- ----------------------------------------------------------------------------
-- EXT_CLICKS: Click-through events
-- ----------------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE EXT_CLICKS
    EXTERNAL_VOLUME = 'ICEBERG_EXT_VOLUME'
    CATALOG = 'GLUE_CATALOG_INT'
    CATALOG_TABLE_NAME = 'clicks'
    COMMENT = 'Click-through events - External Iceberg table from Glue catalog';

-- Describe table to verify structure
DESCRIBE TABLE EXT_CLICKS;

-- ----------------------------------------------------------------------------
-- EXT_CONVERSIONS: Conversion/purchase events
-- ----------------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE EXT_CONVERSIONS
    EXTERNAL_VOLUME = 'ICEBERG_EXT_VOLUME'
    CATALOG = 'GLUE_CATALOG_INT'
    CATALOG_TABLE_NAME = 'conversions'
    COMMENT = 'Conversion events - External Iceberg table from Glue catalog';

-- Describe table to verify structure
DESCRIBE TABLE EXT_CONVERSIONS;

-- ============================================================================
-- SECTION 3: Grant Analyst Access
-- ============================================================================

USE ROLE ICEBERG_ADMIN;

-- Grant SELECT on all external tables to analyst role
GRANT SELECT ON TABLE ICEBERG_DEMO_EXT.ADVERTISING.EXT_CAMPAIGNS TO ROLE ICEBERG_ANALYST;
GRANT SELECT ON TABLE ICEBERG_DEMO_EXT.ADVERTISING.EXT_IMPRESSIONS TO ROLE ICEBERG_ANALYST;
GRANT SELECT ON TABLE ICEBERG_DEMO_EXT.ADVERTISING.EXT_CLICKS TO ROLE ICEBERG_ANALYST;
GRANT SELECT ON TABLE ICEBERG_DEMO_EXT.ADVERTISING.EXT_CONVERSIONS TO ROLE ICEBERG_ANALYST;

-- ============================================================================
-- SECTION 4: Create Convenience Views
-- ============================================================================

USE ROLE ICEBERG_ENGINEER;
USE DATABASE ICEBERG_DEMO_EXT;
USE SCHEMA ADVERTISING;

-- Campaign performance summary view
CREATE OR REPLACE VIEW V_CAMPAIGN_PERFORMANCE AS
SELECT 
    c.campaign_id,
    c.campaign_name,
    c.channel,
    c.ad_format,
    c.budget_usd,
    c.target_cpa_usd,
    c.target_region,
    c.status,
    COUNT(DISTINCT i.impression_id) AS total_impressions,
    COUNT(DISTINCT cl.click_id) AS total_clicks,
    COUNT(DISTINCT cv.conversion_id) AS total_conversions,
    SUM(i.cost_usd) AS impression_cost,
    SUM(cl.cost_usd) AS click_cost,
    SUM(cv.revenue_usd) AS total_revenue,
    ROUND(COUNT(DISTINCT cl.click_id) / NULLIF(COUNT(DISTINCT i.impression_id), 0) * 100, 2) AS ctr_pct,
    ROUND(COUNT(DISTINCT cv.conversion_id) / NULLIF(COUNT(DISTINCT cl.click_id), 0) * 100, 2) AS conversion_rate_pct,
    ROUND(SUM(cv.revenue_usd) / NULLIF(SUM(i.cost_usd) + SUM(cl.cost_usd), 0), 2) AS roas
FROM EXT_CAMPAIGNS c
LEFT JOIN EXT_IMPRESSIONS i ON c.campaign_id = i.campaign_id
LEFT JOIN EXT_CLICKS cl ON c.campaign_id = cl.campaign_id
LEFT JOIN EXT_CONVERSIONS cv ON c.campaign_id = cv.campaign_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;

COMMENT ON VIEW V_CAMPAIGN_PERFORMANCE IS 'Campaign performance metrics aggregated from external Iceberg tables';

-- Daily metrics view
CREATE OR REPLACE VIEW V_DAILY_METRICS AS
SELECT 
    i.date_key,
    i.geo_region,
    i.device_type,
    COUNT(DISTINCT i.impression_id) AS impressions,
    COUNT(DISTINCT cl.click_id) AS clicks,
    COUNT(DISTINCT cv.conversion_id) AS conversions,
    SUM(i.cost_usd) + COALESCE(SUM(cl.cost_usd), 0) AS total_cost,
    COALESCE(SUM(cv.revenue_usd), 0) AS total_revenue
FROM EXT_IMPRESSIONS i
LEFT JOIN EXT_CLICKS cl ON i.impression_id = cl.impression_id
LEFT JOIN EXT_CONVERSIONS cv ON cl.click_id = cv.click_id
GROUP BY 1, 2, 3;

COMMENT ON VIEW V_DAILY_METRICS IS 'Daily advertising metrics by region and device from external Iceberg tables';

-- Grant view access to analyst
USE ROLE ICEBERG_ADMIN;
GRANT SELECT ON VIEW ICEBERG_DEMO_EXT.ADVERTISING.V_CAMPAIGN_PERFORMANCE TO ROLE ICEBERG_ANALYST;
GRANT SELECT ON VIEW ICEBERG_DEMO_EXT.ADVERTISING.V_DAILY_METRICS TO ROLE ICEBERG_ANALYST;

-- ============================================================================
-- SECTION 5: Validation Queries
-- ============================================================================

USE ROLE ICEBERG_ANALYST;
USE WAREHOUSE ICEBERG_DEMO_WH;
USE DATABASE ICEBERG_DEMO_EXT;
USE SCHEMA ADVERTISING;

-- Show all tables in schema
SHOW TABLES IN SCHEMA ICEBERG_DEMO_EXT.ADVERTISING;

-- Row counts
SELECT 'EXT_CAMPAIGNS' AS table_name, COUNT(*) AS row_count FROM EXT_CAMPAIGNS
UNION ALL
SELECT 'EXT_IMPRESSIONS', COUNT(*) FROM EXT_IMPRESSIONS
UNION ALL
SELECT 'EXT_CLICKS', COUNT(*) FROM EXT_CLICKS
UNION ALL
SELECT 'EXT_CONVERSIONS', COUNT(*) FROM EXT_CONVERSIONS;

-- Sample campaign data
SELECT * FROM EXT_CAMPAIGNS LIMIT 5;

-- Sample performance view
SELECT * FROM V_CAMPAIGN_PERFORMANCE LIMIT 10;

-- Top campaigns by revenue
SELECT 
    campaign_name,
    channel,
    total_impressions,
    total_clicks,
    total_conversions,
    total_revenue,
    roas
FROM V_CAMPAIGN_PERFORMANCE
ORDER BY total_revenue DESC NULLS LAST
LIMIT 10;

/*******************************************************************************
 * NOTES:
 * 
 * External Iceberg Tables Characteristics:
 * - Schema is defined at table creation time (or inferred from catalog)
 * - Metadata sync is point-in-time (refresh required for updates)
 * - Read-only by default (write support requires specific configuration)
 * - Good for stable schemas where you want explicit control
 *
 * Comparison with CLD (script 11):
 * - External tables: Manual schema management, explicit refresh
 * - CLD: Automatic schema sync, continuous catalog integration
 *
 * NEXT STEPS:
 * 1. Run 11_catalog_linked_database.sql to create CLD approach
 * 2. Compare query patterns between EXT and CLD approaches
 ******************************************************************************/

