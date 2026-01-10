/*******************************************************************************
 * ICEBERG CLD BCDR DEMO
 * Script: 30_cld_secondary_setup.sql
 * Purpose: Create Catalog Linked Database on SECONDARY account
 *
 * ╔═══════════════════════════════════════════════════════════════════════════╗
 * ║  WHY THIS SCRIPT IS REQUIRED:                                             ║
 * ║                                                                           ║
 * ║  Catalog Linked Databases (CLD) CANNOT be replicated via failover groups.║
 * ║  Each Snowflake account must create its own CLD independently.           ║
 * ║                                                                           ║
 * ║  Both CLDs point to the SAME Glue catalog and S3 storage, ensuring       ║
 * ║  data consistency across accounts without replication.                   ║
 * ╚═══════════════════════════════════════════════════════════════════════════╝
 *
 * Prerequisites:
 *   - Failover groups configured and synced (scripts 20, 21)
 *   - Integrations replicated to secondary (REST_GLUE_CATALOG_INT)
 *   - AWS IAM role trust policy includes BOTH accounts' Snowflake users
 *   - Lake Formation permissions configured (same as primary)
 *
 * Run as: ACCOUNTADMIN (on SECONDARY account)
 ******************************************************************************/

-- ============================================================================
-- CONFIGURATION VARIABLES
-- These should match the primary account configuration
-- ============================================================================

SET glue_database_name = 'iceberg_advertising_db';         -- Glue database name

-- ============================================================================
-- SECTION 1: Verify Secondary Account Setup
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Confirm current account (should be SECONDARY)
SELECT CURRENT_ACCOUNT(), CURRENT_ORGANIZATION_NAME();

-- ============================================================================
-- SECTION 2: Verify Replicated Integrations
-- ============================================================================

/*
 * The REST catalog integration should have been replicated via failover groups.
 * Verify it exists before creating the CLD.
 */

-- Check if REST catalog integration was replicated
SHOW CATALOG INTEGRATIONS;
SHOW CATALOG INTEGRATIONS LIKE 'REST_GLUE%';

-- Get details of the replicated integration
DESCRIBE CATALOG INTEGRATION REST_GLUE_CATALOG_INT;

-- Note the API_AWS_IAM_USER_ARN - this MAY be different from primary!
-- If so, update your AWS IAM role trust policy to include BOTH user ARNs.

-- ============================================================================
-- SECTION 3: Verify External Volume
-- ============================================================================

-- Check if external volume was replicated
SHOW EXTERNAL VOLUMES;

-- Get details
DESCRIBE EXTERNAL VOLUME ICEBERG_EXT_VOLUME;

-- Note the STORAGE_AWS_IAM_USER_ARN - add to trust policy if different from primary

-- ============================================================================
-- SECTION 4: Update AWS IAM Trust Policy (If Needed)
-- ============================================================================

/*
 * ╔═══════════════════════════════════════════════════════════════════════════╗
 * ║  CRITICAL: AWS IAM TRUST POLICY CONFIGURATION                             ║
 * ║                                                                           ║
 * ║  The IAM role must trust BOTH Snowflake accounts:                        ║
 * ║  - Primary account's Snowflake IAM user                                  ║
 * ║  - Secondary account's Snowflake IAM user (may be different!)           ║
 * ║  - Lake Formation service principal                                      ║
 * ╚═══════════════════════════════════════════════════════════════════════════╝
 *
 * Example trust policy:
 * {
 *   "Version": "2012-10-17",
 *   "Statement": [
 *     {
 *       "Effect": "Allow",
 *       "Principal": {
 *         "AWS": [
 *           "<PRIMARY_SNOWFLAKE_IAM_USER_ARN>",
 *           "<SECONDARY_SNOWFLAKE_IAM_USER_ARN>"
 *         ]
 *       },
 *       "Action": "sts:AssumeRole"
 *     },
 *     {
 *       "Effect": "Allow",
 *       "Principal": {
 *         "Service": "lakeformation.amazonaws.com"
 *       },
 *       "Action": "sts:AssumeRole"
 *     }
 *   ]
 * }
 */

-- ============================================================================
-- SECTION 5: Create CLD on Secondary Account
-- ============================================================================

/*
 * Create the Catalog Linked Database using the REPLICATED catalog integration.
 * This CLD will connect to the SAME Glue catalog as the primary account's CLD.
 */

USE WAREHOUSE ICEBERG_DEMO_WH;

-- Create the CLD with the SAME name as primary for consistency
CREATE OR REPLACE DATABASE ICEBERG_DEMO_CLD
  LINKED_CATALOG = (
    CATALOG = 'REST_GLUE_CATALOG_INT',
    ALLOWED_NAMESPACES = ('iceberg_advertising_db')
  )
  COMMENT = 'Catalog Linked Database on Secondary - points to same Glue catalog as Primary';

-- Verify database creation
SHOW DATABASES LIKE 'ICEBERG_DEMO_CLD';

-- Check link status
SELECT SYSTEM$CATALOG_LINK_STATUS('ICEBERG_DEMO_CLD');

-- ============================================================================
-- SECTION 6: Wait for CLD Sync
-- ============================================================================

/*
 * The CLD needs time to sync table metadata from Glue.
 * Wait 30-60 seconds, then verify tables are visible.
 */

-- Check schemas (should show ICEBERG_ADVERTISING_DB)
SHOW SCHEMAS IN DATABASE ICEBERG_DEMO_CLD;

-- Check tables (should show campaigns, impressions, clicks, conversions)
SHOW ICEBERG TABLES IN DATABASE ICEBERG_DEMO_CLD;

-- If tables don't appear, check the link status for errors
SELECT SYSTEM$CATALOG_LINK_STATUS('ICEBERG_DEMO_CLD');

-- ============================================================================
-- SECTION 7: Grant Access to Roles
-- ============================================================================

-- Grant access to the CLD (roles were replicated from primary)
GRANT USAGE ON DATABASE ICEBERG_DEMO_CLD TO ROLE ICEBERG_ENGINEER;
GRANT USAGE ON DATABASE ICEBERG_DEMO_CLD TO ROLE ICEBERG_ANALYST;

GRANT USAGE ON ALL SCHEMAS IN DATABASE ICEBERG_DEMO_CLD TO ROLE ICEBERG_ENGINEER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE ICEBERG_DEMO_CLD TO ROLE ICEBERG_ANALYST;

GRANT SELECT ON ALL TABLES IN DATABASE ICEBERG_DEMO_CLD TO ROLE ICEBERG_ANALYST;
GRANT SELECT ON ALL ICEBERG TABLES IN DATABASE ICEBERG_DEMO_CLD TO ROLE ICEBERG_ANALYST;

-- Future grants for new tables synced from Glue
GRANT SELECT ON FUTURE TABLES IN DATABASE ICEBERG_DEMO_CLD TO ROLE ICEBERG_ANALYST;
GRANT SELECT ON FUTURE ICEBERG TABLES IN DATABASE ICEBERG_DEMO_CLD TO ROLE ICEBERG_ANALYST;

-- ============================================================================
-- SECTION 8: Validate CLD Access
-- ============================================================================

-- Switch to analyst role and test
USE ROLE ICEBERG_ANALYST;
USE WAREHOUSE ICEBERG_DEMO_WH;
USE DATABASE ICEBERG_DEMO_CLD;

-- Show schemas
SHOW SCHEMAS;

-- Show tables
SHOW ICEBERG TABLES IN SCHEMA ICEBERG_ADVERTISING_DB;

-- Query data (should return same data as primary account)
SELECT COUNT(*) AS campaigns FROM ICEBERG_ADVERTISING_DB.CAMPAIGNS;
SELECT COUNT(*) AS impressions FROM ICEBERG_ADVERTISING_DB.IMPRESSIONS;
SELECT COUNT(*) AS clicks FROM ICEBERG_ADVERTISING_DB.CLICKS;
SELECT COUNT(*) AS conversions FROM ICEBERG_ADVERTISING_DB.CONVERSIONS;

-- Sample data
SELECT * FROM ICEBERG_ADVERTISING_DB.CAMPAIGNS LIMIT 5;

-- ============================================================================
-- SECTION 9: Compare with External Tables
-- ============================================================================

/*
 * Verify that CLD and External Tables show the same data.
 * Both access methods point to the same Iceberg tables in S3.
 */

SELECT 'External Tables' AS access_method, COUNT(*) AS campaigns 
FROM ICEBERG_DEMO_EXT.ADVERTISING.EXT_CAMPAIGNS

UNION ALL

SELECT 'Catalog Linked DB', COUNT(*) 
FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CAMPAIGNS;

-- ============================================================================
-- SECTION 10: Troubleshooting
-- ============================================================================

USE ROLE ACCOUNTADMIN;

/*
 * If CLD creation fails or tables don't appear, check these common issues:
 *
 * 1. IAM TRUST POLICY
 *    Error: "sts:AssumeRole not authorized"
 *    Fix: Add SECONDARY account's Snowflake IAM user ARN to trust policy
 *
 * 2. LAKE FORMATION PERMISSIONS
 *    Error: "lakeformation:GetDataAccess not authorized" or "Forbidden: null"
 *    Fix: Ensure IAM role has lakeformation:GetDataAccess permission
 *    Fix: Enable "Allow external engines" in Lake Formation settings
 *
 * 3. CATALOG INTEGRATION NOT FOUND
 *    Error: "Catalog integration 'REST_GLUE_CATALOG_INT' does not exist"
 *    Fix: Wait for failover group replication to complete, then retry
 *
 * 4. TABLES NOT APPEARING
 *    Fix: Wait 30-60 seconds for sync, then run ALTER DATABASE ... REFRESH
 */

-- Force refresh if tables aren't appearing
ALTER DATABASE ICEBERG_DEMO_CLD REFRESH;

-- Check status again
SELECT SYSTEM$CATALOG_LINK_STATUS('ICEBERG_DEMO_CLD');

/*******************************************************************************
 * CLD BCDR ARCHITECTURE:
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │                                                                         │
 * │   PRIMARY ACCOUNT              SECONDARY ACCOUNT                       │
 * │   ┌──────────────────┐        ┌──────────────────┐                    │
 * │   │ ICEBERG_DEMO_CLD │        │ ICEBERG_DEMO_CLD │                    │
 * │   │ (created in      │        │ (created in      │                    │
 * │   │  script 11)      │        │  THIS script)    │                    │
 * │   └────────┬─────────┘        └────────┬─────────┘                    │
 * │            │                           │                               │
 * │            │  REST_GLUE_CATALOG_INT    │  (replicated via failover)   │
 * │            │                           │                               │
 * │            └───────────┬───────────────┘                               │
 * │                        │                                               │
 * │            ┌───────────▼───────────┐                                   │
 * │            │   AWS LAKE FORMATION  │                                   │
 * │            │  (Credential Vending) │                                   │
 * │            └───────────┬───────────┘                                   │
 * │                        │                                               │
 * │            ┌───────────▼───────────┐                                   │
 * │            │    AWS GLUE CATALOG   │                                   │
 * │            │ iceberg_advertising_db │                                   │
 * │            └───────────┬───────────┘                                   │
 * │                        │                                               │
 * │            ┌───────────▼───────────┐                                   │
 * │            │      AMAZON S3        │                                   │
 * │            │ s3://bucket/iceberg/  │                                   │
 * │            └───────────────────────┘                                   │
 * │                                                                         │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 * KEY POINTS:
 * 
 * 1. Each account has its OWN CLD (cannot be replicated)
 * 2. Both CLDs use the SAME replicated catalog integration
 * 3. Both CLDs connect to the SAME Glue catalog
 * 4. Both CLDs access the SAME S3 data
 * 5. No data duplication - true shared storage pattern
 *
 * FAILOVER BEHAVIOR:
 *
 * - During failover, the CLD on secondary is ALREADY WORKING
 * - No special action needed for CLD during failover
 * - Applications just switch to secondary account
 * - Data is immediately available through the CLD
 *
 * NEXT STEPS:
 * 1. Run 90_validation_queries.sql to verify complete setup
 * 2. Test failover procedure
 * 3. Document runbook for operations team
 ******************************************************************************/
