/*******************************************************************************
 * ICEBERG CLD BCDR DEMO
 * Script: 32_migrate_prod_db_independent.sql
 * Purpose: ONE-TIME migration to make ICEBERG_PROD independent on both accounts
 *
 * ╔═══════════════════════════════════════════════════════════════════════════╗
 * ║  THIS IS A ONE-TIME MIGRATION SCRIPT                                      ║
 * ║                                                                           ║
 * ║  After this migration, both PRIMARY and SECONDARY will have their own    ║
 * ║  independent ICEBERG_PROD database with identical structure, both        ║
 * ║  pointing to their local CLD (which connects to the same Glue catalog).  ║
 * ╚═══════════════════════════════════════════════════════════════════════════╝
 *
 * WHY THIS MIGRATION:
 * - Replicated databases are READ-ONLY on secondary
 * - We need writable ICEBERG_PROD on both accounts for tasks, procedures, etc.
 * - Both accounts need identical namespace (same database names)
 * - The CLD architecture already provides data consistency via shared Glue catalog
 *
 * MIGRATION OVERVIEW:
 * 1. [PRIMARY] Ensure ICEBERG_PROD is replicated to secondary (one-time sync)
 * 2. [PRIMARY] Remove ICEBERG_PROD from database failover group
 * 3. [SECONDARY] Promote DB failover group to make replica writable
 * 4. [SECONDARY] Update all view definitions to point to local CLD
 * 5. [BOTH] Verify independent databases work correctly
 *
 * Prerequisites:
 *   - Failover groups already configured (scripts 20, 21)
 *   - ICEBERG_PROD exists on primary with views pointing to CLD
 *   - CLD exists on both accounts pointing to same Glue catalog
 *
 ******************************************************************************/

-- ============================================================================
-- PART A: RUN ON PRIMARY ACCOUNT
-- ============================================================================

/*******************************************************************************
 * STEP 1: Verify current state on PRIMARY
 ******************************************************************************/

USE ROLE ACCOUNTADMIN;

-- Confirm we're on PRIMARY
SELECT 'PRIMARY CHECK' AS step, CURRENT_ACCOUNT() AS account, CURRENT_ORGANIZATION_NAME() AS org;

-- Show current failover groups
SHOW FAILOVER GROUPS;

-- Verify ICEBERG_PROD exists and has the expected objects
USE DATABASE ICEBERG_PROD;
SHOW SCHEMAS;
SHOW VIEWS IN DATABASE ICEBERG_PROD;
SHOW PROCEDURES IN DATABASE ICEBERG_PROD;
SHOW TASKS IN DATABASE ICEBERG_PROD;

/*******************************************************************************
 * STEP 2: Ensure ICEBERG_PROD is fully synced to secondary
 ******************************************************************************/

-- First, make sure ICEBERG_PROD is IN the failover group (temporarily if not already)
-- This ensures secondary has all the DDL definitions

-- Check if ICEBERG_PROD is in the DB failover group
DESCRIBE FAILOVER GROUP ICEBERG_BCDR_DB_FG;

-- If ICEBERG_PROD is NOT in the failover group, add it temporarily:
/*
ALTER FAILOVER GROUP ICEBERG_BCDR_DB_FG
    SET ALLOWED_DATABASES = ICEBERG_DEMO_EXT, ICEBERG_PROD;

-- Force a refresh to sync
ALTER FAILOVER GROUP ICEBERG_BCDR_DB_FG REFRESH;

-- Wait for sync to complete (check secondary to verify)
-- Give it 2-5 minutes for metadata to sync
*/

/*******************************************************************************
 * STEP 3: Remove ICEBERG_PROD from failover group
 ******************************************************************************/

-- After confirming secondary has ICEBERG_PROD, remove it from replication
-- This keeps only ICEBERG_DEMO_EXT in the database failover group

ALTER FAILOVER GROUP ICEBERG_BCDR_DB_FG
    SET ALLOWED_DATABASES = ICEBERG_DEMO_EXT;

-- Verify change
DESCRIBE FAILOVER GROUP ICEBERG_BCDR_DB_FG;

-- The failover group now only replicates ICEBERG_DEMO_EXT (external tables)

/*******************************************************************************
 * PRIMARY MIGRATION COMPLETE
 * 
 * Now proceed to PART B on the SECONDARY account.
 ******************************************************************************/


-- ============================================================================
-- PART B: RUN ON SECONDARY ACCOUNT
-- ============================================================================

/*******************************************************************************
 * STEP 4: Verify secondary state before promotion
 ******************************************************************************/

USE ROLE ACCOUNTADMIN;

-- Confirm we're on SECONDARY
SELECT 'SECONDARY CHECK' AS step, CURRENT_ACCOUNT() AS account, CURRENT_ORGANIZATION_NAME() AS org;

-- Check ICEBERG_PROD exists (should be a replica)
SHOW DATABASES LIKE 'ICEBERG_PROD';

-- It should show:
-- - is_current = N (it's a replica, not primary)
-- - type = REPLICA or similar

-- View the objects in the replica
USE DATABASE ICEBERG_PROD;
SHOW SCHEMAS;
SHOW VIEWS IN DATABASE ICEBERG_PROD;

/*******************************************************************************
 * STEP 5: Convert replica to independent database
 * 
 * Since we removed ICEBERG_PROD from the failover group on primary,
 * the secondary's replica is now "orphaned". We need to:
 * 1. Drop the replica
 * 2. Create a new independent database with the same structure
 ******************************************************************************/

-- Option A: If the replica can be promoted (preferred)
-- This might work if the database was just removed from the FG
/*
-- Try to unlink the database from replication
ALTER DATABASE ICEBERG_PROD SET IS_TRANSIENT = FALSE;  -- May help break the link
*/

-- Option B: Drop and recreate (safer approach)
-- First, capture the DDL we need to recreate

-- Get list of all views and their definitions
SELECT 
    table_schema,
    table_name,
    view_definition
FROM ICEBERG_PROD.INFORMATION_SCHEMA.VIEWS
WHERE table_schema != 'INFORMATION_SCHEMA'
ORDER BY table_schema, table_name;

-- Drop the replica database
DROP DATABASE IF EXISTS ICEBERG_PROD;

-- Create fresh independent database
CREATE DATABASE ICEBERG_PROD
    COMMENT = 'Production database - independent on secondary, views over local CLD';

/*******************************************************************************
 * STEP 6: Recreate schemas
 ******************************************************************************/

USE DATABASE ICEBERG_PROD;

CREATE SCHEMA IF NOT EXISTS ADVERTISING
    COMMENT = 'Advertising data views backed by CLD Iceberg tables';

CREATE SCHEMA IF NOT EXISTS MONITORING
    COMMENT = 'Monitoring and health check objects';

CREATE SCHEMA IF NOT EXISTS DR_MONITORING
    COMMENT = 'DR-specific monitoring for secondary operations';

/*******************************************************************************
 * STEP 7: Recreate base views pointing to LOCAL CLD
 * 
 * IMPORTANT: These point to ICEBERG_DEMO_CLD which exists on THIS account
 ******************************************************************************/

USE SCHEMA ADVERTISING;

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

/*******************************************************************************
 * STEP 8: Recreate aggregated views
 ******************************************************************************/

-- V_CAMPAIGNS_SUMMARY
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

-- V_IMPRESSIONS_DAILY
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

-- V_CLICKS_DAILY
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

-- V_CONVERSIONS_DAILY
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

-- V_CAMPAIGN_PERFORMANCE
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
    ROUND(COALESCE(cl.clicks, 0) * 100.0 / NULLIF(i.impressions, 0), 4) AS ctr_pct,
    ROUND(COALESCE(cv.conversions, 0) * 100.0 / NULLIF(cl.clicks, 0), 4) AS conversion_rate_pct,
    ROUND((COALESCE(i.impression_cost, 0) + COALESCE(cl.click_cost, 0)) / NULLIF(cv.conversions, 0), 2) AS cost_per_acquisition,
    ROUND(COALESCE(cv.revenue, 0) / NULLIF(COALESCE(i.impression_cost, 0) + COALESCE(cl.click_cost, 0), 0), 2) AS roas
FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CAMPAIGNS c
LEFT JOIN impressions_agg i ON c.campaign_id = i.campaign_id
LEFT JOIN clicks_agg cl ON c.campaign_id = cl.campaign_id AND i.date_key = cl.date_key
LEFT JOIN conversions_agg cv ON c.campaign_id = cv.campaign_id AND i.date_key = cv.date_key
WHERE i.date_key IS NOT NULL OR cl.date_key IS NOT NULL OR cv.date_key IS NOT NULL;

/*******************************************************************************
 * STEP 9: Recreate monitoring tables for DR heartbeat
 ******************************************************************************/

USE SCHEMA DR_MONITORING;

CREATE TABLE IF NOT EXISTS SECONDARY_HEARTBEAT_LOG (
    heartbeat_id NUMBER AUTOINCREMENT,
    heartbeat_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    check_type VARCHAR(50),
    status VARCHAR(20),
    details VARCHAR(4000),
    error_msg VARCHAR(4000),
    cld_table_count NUMBER,
    prod_view_count NUMBER,
    replication_lag_seconds NUMBER,
    PRIMARY KEY (heartbeat_id)
);

CREATE TABLE IF NOT EXISTS GRANT_AUDIT_LOG (
    audit_id NUMBER AUTOINCREMENT,
    audit_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    object_type VARCHAR(50),
    object_name VARCHAR(255),
    grantee_role VARCHAR(100),
    privilege VARCHAR(50),
    action VARCHAR(20),
    status VARCHAR(20),
    PRIMARY KEY (audit_id)
);

CREATE TABLE IF NOT EXISTS SCHEMA_DRIFT_LOG (
    drift_id NUMBER AUTOINCREMENT,
    check_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    object_type VARCHAR(50),
    object_name VARCHAR(255),
    drift_type VARCHAR(50),      -- 'MISSING_ON_SECONDARY', 'MISSING_ON_PRIMARY', 'DEFINITION_MISMATCH'
    primary_definition VARCHAR(10000),
    secondary_definition VARCHAR(10000),
    status VARCHAR(20),          -- 'DETECTED', 'RESOLVED', 'IGNORED'
    PRIMARY KEY (drift_id)
);

/*******************************************************************************
 * STEP 10: Grant access to roles (replicated from primary)
 ******************************************************************************/

USE ROLE ACCOUNTADMIN;

-- Database grants
GRANT USAGE ON DATABASE ICEBERG_PROD TO ROLE ICEBERG_ENGINEER;
GRANT USAGE ON DATABASE ICEBERG_PROD TO ROLE ICEBERG_ANALYST;
GRANT USAGE ON DATABASE ICEBERG_PROD TO ROLE ICEBERG_ADMIN;

-- Schema grants
GRANT USAGE ON ALL SCHEMAS IN DATABASE ICEBERG_PROD TO ROLE ICEBERG_ENGINEER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE ICEBERG_PROD TO ROLE ICEBERG_ANALYST;
GRANT USAGE ON ALL SCHEMAS IN DATABASE ICEBERG_PROD TO ROLE ICEBERG_ADMIN;

GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ICEBERG_PROD TO ROLE ICEBERG_ENGINEER;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ICEBERG_PROD TO ROLE ICEBERG_ANALYST;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ICEBERG_PROD TO ROLE ICEBERG_ADMIN;

-- View grants
GRANT SELECT ON ALL VIEWS IN DATABASE ICEBERG_PROD TO ROLE ICEBERG_ANALYST;
GRANT SELECT ON ALL VIEWS IN DATABASE ICEBERG_PROD TO ROLE ICEBERG_ENGINEER;

GRANT SELECT ON FUTURE VIEWS IN DATABASE ICEBERG_PROD TO ROLE ICEBERG_ANALYST;
GRANT SELECT ON FUTURE VIEWS IN DATABASE ICEBERG_PROD TO ROLE ICEBERG_ENGINEER;

-- Table grants (for monitoring tables)
GRANT SELECT ON ALL TABLES IN DATABASE ICEBERG_PROD TO ROLE ICEBERG_ADMIN;
GRANT INSERT ON ALL TABLES IN SCHEMA ICEBERG_PROD.DR_MONITORING TO ROLE ICEBERG_ADMIN;

/*******************************************************************************
 * STEP 11: Verify migration
 ******************************************************************************/

USE ROLE ICEBERG_ANALYST;
USE WAREHOUSE ICEBERG_DEMO_WH;
USE DATABASE ICEBERG_PROD;

-- Show all views
SHOW VIEWS IN DATABASE ICEBERG_PROD;

-- Test data access
SELECT 'CAMPAIGNS' AS view_name, COUNT(*) AS row_count FROM ADVERTISING.CAMPAIGNS
UNION ALL
SELECT 'IMPRESSIONS', COUNT(*) FROM ADVERTISING.IMPRESSIONS
UNION ALL
SELECT 'CLICKS', COUNT(*) FROM ADVERTISING.CLICKS
UNION ALL
SELECT 'CONVERSIONS', COUNT(*) FROM ADVERTISING.CONVERSIONS;

-- Verify CLD connection
SELECT * FROM ADVERTISING.CAMPAIGNS LIMIT 5;

/*******************************************************************************
 * MIGRATION COMPLETE!
 * 
 * NEW ARCHITECTURE:
 * 
 *   PRIMARY ACCOUNT                      SECONDARY ACCOUNT
 *   ┌──────────────────┐                ┌──────────────────┐
 *   │ ICEBERG_PROD     │                │ ICEBERG_PROD     │
 *   │ (INDEPENDENT)    │                │ (INDEPENDENT)    │
 *   │ └─ views → CLD   │                │ └─ views → CLD   │
 *   └────────┬─────────┘                └────────┬─────────┘
 *            │                                   │
 *            ▼                                   ▼
 *   ┌──────────────────┐                ┌──────────────────┐
 *   │ ICEBERG_DEMO_CLD │                │ ICEBERG_DEMO_CLD │
 *   └────────┬─────────┘                └────────┬─────────┘
 *            │                                   │
 *            └───────────────┬───────────────────┘
 *                            ▼
 *                   ┌────────────────┐
 *                   │   AWS GLUE     │
 *                   │   CATALOG      │
 *                   └────────┬───────┘
 *                            ▼
 *                   ┌────────────────┐
 *                   │   AMAZON S3    │
 *                   │ (Iceberg data) │
 *                   └────────────────┘
 * 
 * WHAT'S STILL REPLICATED:
 * - ICEBERG_BCDR_ACCOUNT_FG: Roles, storage integrations
 * - ICEBERG_BCDR_VOLUME_FG: External volumes
 * - ICEBERG_BCDR_DB_FG: ICEBERG_DEMO_EXT only (external tables)
 * 
 * WHAT'S NOW INDEPENDENT:
 * - ICEBERG_PROD: Both accounts have their own writable copy
 * - ICEBERG_DEMO_CLD: Already independent (CLDs can't be replicated)
 * 
 * NEXT STEPS:
 * 1. Run 33_schema_sync_task.sql to set up daily schema drift detection
 * 2. Update 31_sync_task_secondary.sql (remove old merge logic)
 * 3. Update any documentation/runbooks
 * 
 ******************************************************************************/
