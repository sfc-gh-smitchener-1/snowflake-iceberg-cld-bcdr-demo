/*******************************************************************************
 * ICEBERG CLD BCDR DEMO
 * Script: 11_catalog_linked_database.sql
 * Purpose: Create Catalog Linked Database (CLD) for Iceberg tables in AWS Glue
 *
 * A Catalog Linked Database (CLD) provides automatic synchronization with
 * an external catalog (AWS Glue). Tables are automatically discovered and 
 * their schemas are kept in sync.
 *
 * Prerequisites:
 *   - REST catalog integration created (REST_GLUE_CATALOG_INT)
 *   - Iceberg tables exist in Glue catalog
 *   - Lake Formation configured with external engine access enabled
 *   - IAM role trust policy includes Lake Formation service
 *
 * Run as: ACCOUNTADMIN
 ******************************************************************************/

-- ============================================================================
-- CONFIGURATION
-- ============================================================================

SET glue_database_name = 'iceberg_advertising_db';
SET catalog_integration_name = 'REST_GLUE_CATALOG_INT';

-- ============================================================================
-- SECTION 1: Verify Prerequisites
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ICEBERG_DEMO_WH;

-- Verify catalog integration exists
SHOW CATALOG INTEGRATIONS LIKE 'REST_GLUE%';

-- Verify it's configured correctly
DESCRIBE CATALOG INTEGRATION REST_GLUE_CATALOG_INT;

-- ============================================================================
-- SECTION 2: Create Catalog Linked Database
-- ============================================================================

/*
 * The CLD syntax for Glue REST API:
 * - CATALOG: Must be a REST catalog integration (not standard GLUE integration)
 * - ALLOWED_NAMESPACES: Glue databases to sync
 * - No EXTERNAL_VOLUME when using VENDED_CREDENTIALS mode
 */

CREATE OR REPLACE DATABASE ICEBERG_DEMO_CLD
  LINKED_CATALOG = (
    CATALOG = 'REST_GLUE_CATALOG_INT',
    ALLOWED_NAMESPACES = ('iceberg_advertising_db')
  )
  COMMENT = 'Catalog Linked Database for Iceberg tables from AWS Glue';

-- ============================================================================
-- SECTION 3: Verify CLD Creation
-- ============================================================================

-- Check database exists
SHOW DATABASES LIKE 'ICEBERG_DEMO_CLD';

-- Check schemas synced from Glue
SHOW SCHEMAS IN DATABASE ICEBERG_DEMO_CLD;

-- Check tables synced from Glue
SHOW ICEBERG TABLES IN DATABASE ICEBERG_DEMO_CLD;

-- Check link status
SELECT SYSTEM$CATALOG_LINK_STATUS('ICEBERG_DEMO_CLD');

-- ============================================================================
-- SECTION 4: Query CLD Tables
-- ============================================================================

-- Row counts
SELECT 'CAMPAIGNS' AS tbl, COUNT(*) AS rows FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CAMPAIGNS
UNION ALL SELECT 'IMPRESSIONS', COUNT(*) FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.IMPRESSIONS
UNION ALL SELECT 'CLICKS', COUNT(*) FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CLICKS
UNION ALL SELECT 'CONVERSIONS', COUNT(*) FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CONVERSIONS;

-- Sample data
SELECT * FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CAMPAIGNS LIMIT 5;

-- ============================================================================
-- SECTION 5: Grant Access to Roles
-- ============================================================================

-- Grant database access
GRANT USAGE ON DATABASE ICEBERG_DEMO_CLD TO ROLE ICEBERG_ENGINEER;
GRANT USAGE ON DATABASE ICEBERG_DEMO_CLD TO ROLE ICEBERG_ANALYST;

-- Grant schema access
GRANT USAGE ON ALL SCHEMAS IN DATABASE ICEBERG_DEMO_CLD TO ROLE ICEBERG_ENGINEER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE ICEBERG_DEMO_CLD TO ROLE ICEBERG_ANALYST;

-- Grant table access
GRANT SELECT ON ALL TABLES IN DATABASE ICEBERG_DEMO_CLD TO ROLE ICEBERG_ANALYST;
GRANT SELECT ON ALL ICEBERG TABLES IN DATABASE ICEBERG_DEMO_CLD TO ROLE ICEBERG_ANALYST;

-- Future grants
GRANT SELECT ON FUTURE TABLES IN DATABASE ICEBERG_DEMO_CLD TO ROLE ICEBERG_ANALYST;
GRANT SELECT ON FUTURE ICEBERG TABLES IN DATABASE ICEBERG_DEMO_CLD TO ROLE ICEBERG_ANALYST;

-- ============================================================================
-- SECTION 6: Compare EXT vs CLD Approaches
-- ============================================================================

/*
 * Both External Tables (ICEBERG_DEMO_EXT) and CLD (ICEBERG_DEMO_CLD) 
 * point to the same Iceberg data. Compare:
 */

-- Compare campaign counts
SELECT 'External Tables' AS approach, COUNT(*) AS campaigns 
FROM ICEBERG_DEMO_EXT.ADVERTISING.EXT_CAMPAIGNS
UNION ALL
SELECT 'Catalog Linked DB', COUNT(*) 
FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CAMPAIGNS;

-- ============================================================================
-- SECTION 7: Refresh CLD
-- ============================================================================

-- Manual refresh to sync latest changes from Glue
ALTER DATABASE ICEBERG_DEMO_CLD REFRESH;

-- Check refresh status
SELECT SYSTEM$CATALOG_LINK_STATUS('ICEBERG_DEMO_CLD');

/*******************************************************************************
 * TROUBLESHOOTING
 * 
 * If tables show as <invalid>:
 * 1. Check Lake Formation → Application integration settings
 *    - "Allow external engines to access data in Amazon S3 locations 
 *       with full table access" must be ENABLED
 *
 * 2. Verify IAM role trust policy includes Lake Formation:
 *    {
 *      "Effect": "Allow",
 *      "Principal": {"Service": "lakeformation.amazonaws.com"},
 *      "Action": "sts:AssumeRole"
 *    }
 *
 * 3. Check IAM role has lakeformation:GetDataAccess permission
 *
 * 4. Verify Lake Formation data permissions are granted on tables
 *
 * 5. Check S3 location is registered in Lake Formation
 *
 * Common Errors:
 * - "Forbidden: null" → Lake Formation Application integration not enabled
 * - "Unable to assume role" → Trust policy missing Lake Formation service
 * - "lakeformation:GetDataAccess" error → IAM policy missing permission
 * - "credential vending not enabled" → Wrong catalog integration type
 ******************************************************************************/

/*******************************************************************************
 * CLD vs EXTERNAL TABLES COMPARISON
 *
 * | Feature              | External Tables        | Catalog Linked DB      |
 * |----------------------|------------------------|------------------------|
 * | Table Discovery      | Manual creation        | Automatic sync         |
 * | Schema Updates       | Manual ALTER           | Automatic on refresh   |
 * | Naming               | Custom (EXT_ prefix)   | Matches Glue catalog   |
 * | Refresh              | On-demand              | Continuous/on-demand   |
 * | Lake Formation       | Not required           | Required (vending)     |
 * | Best For             | Stable schemas         | Dynamic catalogs       |
 *
 * NEXT STEPS:
 * 1. Run 20_failover_groups_primary.sql to configure BCDR
 ******************************************************************************/
